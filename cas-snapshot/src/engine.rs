//! 内容寻址快照库核心引擎。
//!
//! 不变量：
//! * 块以规范化字节 SHA-256 寻址；重复内容只产生收据，不重复落盘。
//! * 已发布快照引用的每个块都存在于 `blocks` 且文件可读、摘要一致。
//! * 冻结经 intent -> refs -> publish 三阶段，崩溃后重启自动提交或回滚。
//! * GC 使用发布序号作为代次屏障，并在删除事务内重查引用，扫描期间新
//!   冻结快照的块不会被误删。

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;
use std::sync::Mutex;

use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::events;

pub const CHUNK_SIZE: usize = 64 * 1024;
pub const DEFAULT_LEASE_MS: i64 = 15 * 60 * 1000;

#[derive(Debug)]
pub enum Error {
    Sql(rusqlite::Error),
    Io(String),
    Conflict(String),
    NotFound(String),
    BadInput(String),
    Corrupt(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Sql(e) => write!(f, "数据库错误: {e}"),
            Error::Io(e) => write!(f, "IO 错误: {e}"),
            Error::Conflict(e) => write!(f, "冲突: {e}"),
            Error::NotFound(e) => write!(f, "未找到: {e}"),
            Error::BadInput(e) => write!(f, "无效输入: {e}"),
            Error::Corrupt(e) => write!(f, "完整性损坏: {e}"),
        }
    }
}
impl std::error::Error for Error {}
impl From<rusqlite::Error> for Error {
    fn from(e: rusqlite::Error) -> Self {
        Error::Sql(e)
    }
}
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error::Io(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;

/// 崩溃注入点。测试把它设为“执行到该点则退出”，用于验证恢复协议。
pub trait CrashSink: Send + Sync {
    fn hit(&self, point: &str);
}

pub struct NoCrash;
impl CrashSink for NoCrash {
    fn hit(&self, _point: &str) {}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RefSource {
    pub ref_id: String,
    pub kind: String, // snapshot | session
    pub label: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct BlockInfo {
    pub hash: String,
    pub size: i64,
    pub created: i64,
    pub refs: Vec<RefSource>,
    pub file_present: bool,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct ReceiptInfo {
    pub seq: i64,
    pub block_hash: String,
    pub size: i64,
    pub dedup: bool,
    pub idem_key: Option<String>,
    pub created: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionInfo {
    pub id: String,
    pub label: String,
    pub fence: i64,
    pub created: i64,
    pub expires: i64,
    pub finalized: bool,
    pub receipts: Vec<ReceiptInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub position: i64,
    pub hash: String,
    pub size: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryInfo {
    pub name: String,
    pub size: i64,
    pub chunks: Vec<ChunkRef>,
}

#[derive(Debug, Clone, Serialize)]
pub struct SnapshotInfo {
    pub id: String,
    pub label: String,
    pub parent_id: Option<String>,
    pub manifest: Value,
    pub manifest_hash: String,
    pub publish_seq: Option<i64>,
    pub status: String,
    pub created: i64,
    pub deleted_at: Option<i64>,
    pub entries: Vec<EntryInfo>,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerificationItem {
    pub hash: String,
    pub ok: bool,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct VerificationReport {
    pub ok: bool,
    pub blocks_checked: usize,
    pub snapshots_checked: usize,
    pub chain_ok: bool,
    pub chain_head: String,
    pub items: Vec<VerificationItem>,
    pub affected_snapshots: Vec<String>,
    pub ts: i64,
    pub receipt_hash: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct GcReport {
    pub gc_id: String,
    pub barrier_seq: i64,
    pub candidates: Vec<String>,
    pub deleted: Vec<String>,
    pub spared_by_refs: Vec<String>,
    pub phase: String,
    pub start_event: String,
    pub end_event: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EventInfo {
    pub seq: i64,
    pub ts: i64,
    pub kind: String,
    pub payload: Value,
    pub prev_hash: String,
    pub hash: String,
}

pub struct Engine {
    pub data_dir: PathBuf,
    conn: Mutex<Connection>,
    crash: Box<dyn CrashSink>,
    /// 测试用固定时钟；None 时使用系统毫秒时间。
    clock: Option<Box<dyn Fn() -> i64 + Send + Sync>>,
}

impl Engine {
    /// 打开（必要时初始化）数据目录与元数据库，并完成崩溃恢复。
    pub fn open(data_dir: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with(data_dir, Box::new(NoCrash), None)
    }

    pub fn open_with(
        data_dir: impl Into<PathBuf>,
        crash: Box<dyn CrashSink>,
        clock: Option<Box<dyn Fn() -> i64 + Send + Sync>>,
    ) -> Result<Self> {
        let dir = data_dir.into();
        fs::create_dir_all(dir.join("blocks"))?;
        fs::create_dir_all(dir.join("tmp"))?;
        let db_path = dir.join("meta.db");
        let conn = Connection::open(db_path)?;
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", true)?;
        conn.pragma_update(None, "busy_timeout", 10_000)?;
        conn.execute_batch(crate::schema::MIGRATIONS)?;
        let mut engine = Engine {
            data_dir: dir,
            conn: Mutex::new(conn),
            crash,
            clock,
        };
        engine.recover()?;
        Ok(engine)
    }

    pub fn now_ms(&self) -> i64 {
        if let Some(clock) = &self.clock {
            clock()
        } else {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        }
    }

    fn block_path(&self, hash: &str) -> PathBuf {
        self.data_dir.join("blocks").join(&hash[0..2]).join(&hash[2..])
    }


    pub fn with_conn<R>(
        &self,
        f: impl FnOnce(&Connection) -> rusqlite::Result<R>,
    ) -> Result<R> {
        let conn = self.conn.lock().unwrap();
        Ok(f(&conn)?)
    }

    // ------------------------------------------------------------------
    // 块写入（内容寻址、去重、原子落盘）
    // ------------------------------------------------------------------

    /// 规范化字节即原始字节；先写临时文件再 rename，保证块文件完整出现。
    fn store_bytes(&self, bytes: &[u8]) -> Result<String> {
        let hash = crate::hash::sha256_hex(bytes);
        let target = self.block_path(&hash);
        if target.exists() {
            return Ok(hash);
        }
        fs::create_dir_all(target.parent().unwrap())?;
        let tmp = self
            .data_dir
            .join("tmp")
            .join(format!("{}-{}", hash, self.now_ms()));
        fs::write(&tmp, bytes)?;
        // 校验写入内容，防止静默错误后再发布摘要。
        let actual = crate::hash::sha256_hex(&fs::read(&tmp)?);
        if actual != hash {
            let _ = fs::remove_file(&tmp);
            return Err(Error::Corrupt("临时块写入后摘要不一致".into()));
        }
        fs::rename(&tmp, &target)?;
        Ok(hash)
    }


    // ------------------------------------------------------------------
    // 上传会话、租约与 fencing
    // ------------------------------------------------------------------

    pub fn create_session(&self, label: &str, lease_ms: Option<i64>) -> Result<SessionInfo> {
        let now = self.now_ms();
        let lease = lease_ms.unwrap_or(DEFAULT_LEASE_MS).max(1000);
        let conn = self.conn.lock().unwrap();
        let id = format!("s_{:016x}", events::next_seq(&conn, "session")?);
        let fence = events::next_seq(&conn, "fence")?;
        conn.execute(
            "INSERT INTO sessions (id, label, fence, created, expires)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![id, label, fence, now, now + lease],
        )?;
        events::append_autocommit(
            &conn,
            now,
            "session.created",
            json!({"session": id, "label": label, "fence": fence, "lease_ms": lease}),
        )?;
        Ok(SessionInfo {
            id,
            label: label.to_string(),
            fence,
            created: now,
            expires: now + lease,
            finalized: false,
            receipts: vec![],
        })
    }

    /// 续租并换发 fencing token；旧 token 立即失效。
    pub fn renew_session(&self, session_id: &str, token: i64, lease_ms: Option<i64>) -> Result<i64> {
        let now = self.now_ms();
        let lease = lease_ms.unwrap_or(DEFAULT_LEASE_MS).max(1000);
        let conn = self.conn.lock().unwrap();
        let row: Option<(i64, i64, i64)> = conn
            .query_row(
                "SELECT fence, expires, finalized FROM sessions WHERE id = ?1",
                [session_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let (current_fence, expires, finalized) =
            row.ok_or_else(|| Error::NotFound(format!("会话 {session_id} 不存在")))?;
        if finalized != 0 {
            return Err(Error::Conflict("会话已冻结".into()));
        }
        if token != current_fence {
            return Err(Error::Conflict(format!(
                "fencing token {token} 已过期（当前 {current_fence}），拒绝续租"
            )));
        }
        let new_fence = events::next_seq(&conn, "fence")?;
        let new_expires = now.max(expires) + lease;
        conn.execute(
            "UPDATE sessions SET fence = ?1, expires = ?2 WHERE id = ?3",
            params![new_fence, new_expires, session_id],
        )?;
        events::append_autocommit(
            &conn,
            now,
            "session.renewed",
            json!({"session": session_id, "old_fence": token, "fence": new_fence}),
        )?;
        Ok(new_fence)
    }

    fn check_token(&self, conn: &Connection, session_id: &str, token: i64) -> Result<()> {
        let now = self.now_ms();
        let row: Option<(i64, i64, i64)> = conn
            .query_row(
                "SELECT fence, expires, finalized FROM sessions WHERE id = ?1",
                [session_id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .optional()?;
        let (fence, expires, finalized) =
            row.ok_or_else(|| Error::NotFound(format!("会话 {session_id} 不存在")))?;
        if finalized != 0 {
            return Err(Error::Conflict("会话已冻结，不能再提交".into()));
        }
        if now > expires {
            return Err(Error::Conflict(format!(
                "会话租约已于 {expires} 过期（现在 {now}），请续租"
            )));
        }
        if token != fence {
            return Err(Error::Conflict(format!(
                "fencing token 过期：提交携带 {token}，当前 {fence}，拒绝旧会话晚提交"
            )));
        }
        Ok(())
    }

    /// 提交一个块（幂等键可选）。重复内容只增加收据；同幂等键内容不同则冲突。
    pub fn submit_block(
        &self,
        session_id: &str,
        token: i64,
        idem_key: Option<&str>,
        data: &[u8],
    ) -> Result<ReceiptInfo> {
        let now = self.now_ms();
        let conn = self.conn.lock().unwrap();
        self.check_token(&conn, session_id, token)?;

        // 幂等键先查：同会话内命中则必须内容一致。
        if let Some(key) = idem_key {
            if let Some((h,)) = conn
                .query_row(
                    "SELECT block_hash FROM receipts WHERE session_id = ?1 AND idem_key = ?2",
                    params![session_id, key],
                    |r| Ok((r.get::<_, String>(0)?,)),
                )
                .optional()?
            {
                let incoming = crate::hash::sha256_hex(data);
                if incoming == h {
                    let info = conn.query_row(
                        "SELECT seq, block_hash, size, dedup, idem_key, created
                         FROM receipts WHERE session_id = ?1 AND idem_key = ?2",
                        params![session_id, key],
                        receipt_row,
                    )?;
                    events::append_autocommit(
                        &conn,
                        now,
                        "block.idem_hit",
                        json!({"session": session_id, "idem_key": key, "block": h}),
                    )?;
                    return Ok(info);
                } else {
                    return Err(Error::Conflict(format!(
                        "幂等键 {key} 已绑定块 {h}，本次内容摘要 {incoming} 不同"
                    )));
                }
            }
        }

        // 落盘（去重发生在文件层）。
        let hash = self.store_bytes(data)?;
        let existed: bool = conn
            .query_row("SELECT 1 FROM blocks WHERE hash = ?1", [&hash], |_| Ok(()))
            .optional()?
            .is_some();
        if !existed {
            conn.execute(
                "INSERT INTO blocks (hash, size, created) VALUES (?1, ?2, ?3)",
                params![hash, data.len() as i64, now],
            )?;
        }

        // 会话持有引用：租约内未冻结的块不会被 GC。
        conn.execute(
            "INSERT OR IGNORE INTO block_refs (block_hash, ref_id) VALUES (?1, ?2)",
            params![hash, format!("session:{session_id}")],
        )?;

        let seq = conn.query_row(
            "SELECT COALESCE(MAX(seq), 0) + 1 FROM receipts WHERE session_id = ?1",
            [session_id],
            |r| r.get(0),
        )?;
        conn.execute(
            "INSERT INTO receipts (session_id, block_hash, idem_key, size, dedup, seq, created)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                session_id,
                hash,
                idem_key,
                data.len() as i64,
                existed,
                seq,
                now
            ],
        )?;
        events::append_autocommit(
            &conn,
            now,
            if existed { "block.dedup" } else { "block.stored" },
            json!({
                "session": session_id, "block": hash, "size": data.len(), "dedup": existed
            }),
        )?;
        Ok(ReceiptInfo {
            seq,
            block_hash: hash,
            size: data.len() as i64,
            dedup: existed,
            idem_key: idem_key.map(str::to_string),
            created: now,
        })
    }

    pub fn get_session(&self, session_id: &str) -> Result<SessionInfo> {
        let conn = self.conn.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT id, label, fence, created, expires, finalized FROM sessions WHERE id = ?1",
                [session_id],
                |r| {
                    Ok(SessionInfo {
                        id: r.get(0)?,
                        label: r.get(1)?,
                        fence: r.get(2)?,
                        created: r.get(3)?,
                        expires: r.get(4)?,
                        finalized: r.get::<_, i64>(5)? != 0,
                        receipts: vec![],
                    })
                },
            )
            .optional()?
            .ok_or_else(|| Error::NotFound(format!("会话 {session_id} 不存在")))?;
        let mut receipts = Vec::new();
        {
            let mut stmt = conn.prepare(
                "SELECT seq, block_hash, size, dedup, idem_key, created
                 FROM receipts WHERE session_id = ?1 ORDER BY seq",
            )?;
            for r in stmt.query_map([session_id], receipt_row)? {
                receipts.push(r?);
            }
        }
        Ok(SessionInfo {
            receipts,
            ..row
        })
    }

    pub fn list_sessions(&self) -> Result<Vec<SessionInfo>> {
        let conn = self.conn.lock().unwrap();
        let mut ids = Vec::new();
        {
            let mut stmt = conn.prepare("SELECT id FROM sessions ORDER BY created DESC, id")?;
            for r in stmt.query_map([], |r| r.get::<_, String>(0))? {
                ids.push(r?);
            }
        }
        drop(conn);
        ids.iter().map(|id| self.get_session(id)).collect()
    }
}

fn receipt_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<ReceiptInfo> {
    Ok(ReceiptInfo {
        seq: r.get(0)?,
        block_hash: r.get(1)?,
        size: r.get(2)?,
        dedup: r.get::<_, i64>(3)? != 0,
        idem_key: r.get(4)?,
        created: r.get(5)?,
    })
}

/// 冻结时保存的意图状态（JSON）。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct FreezeState {
    snapshot_id: String,
    label: String,
    parent_id: Option<String>,
    session_id: String,
    manifest: Value,
    entries: Vec<EntryInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GcState {
    gc_id: String,
    barrier_seq: i64,
    start_event: String,
    candidates: Vec<String>,
    deleted: Vec<String>,
    spared: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ImportCheck {
    pub ok: bool,
    pub manifest_hash: String,
    pub manifest_hash_matches: bool,
    pub freeze_receipt_matches: bool,
    pub detail: Vec<String>,
}

impl Engine {
    // ------------------------------------------------------------------
    // 条目组装与冻结（三阶段提交）
    // ------------------------------------------------------------------

    /// 把若干条目按固定块大小切片，全部必须由当前会话已上传收据覆盖。
    pub fn assemble_entries(
        &self,
        session_id: &str,
        entries: &[(String, Vec<u8>)],
    ) -> Result<Vec<EntryInfo>> {
        if entries.is_empty() {
            return Err(Error::BadInput("快照至少需要一个条目".into()));
        }
        let conn = self.conn.lock().unwrap();
        let mut owned: BTreeSet<String> = BTreeSet::new();
        {
            let mut stmt = conn
                .prepare("SELECT block_hash FROM receipts WHERE session_id = ?1")?;
            for r in stmt.query_map([session_id], |r| r.get::<_, String>(0))? {
                owned.insert(r?);
            }
        }
        if owned.is_empty() {
            return Err(Error::Conflict(format!("会话 {session_id} 还没有任何已提交块")));
        }

        let mut result = Vec::new();
        for (name, data) in entries {
            if name.is_empty() || name.contains(['\\', '\0']) {
                return Err(Error::BadInput(format!("非法条目名 {name}")));
            }
            let mut chunks = Vec::new();
            if data.is_empty() {
                // 空条目：单个零字节块，仍然内容寻址。
                let h = crate::hash::sha256_hex(b"");
                if !owned.contains(&h) {
                    return Err(Error::BadInput(format!(
                        "条目 {name} 使用空块 {h}，但会话未上传该块"
                    )));
                }
                chunks.push(ChunkRef {
                    position: 0,
                    hash: h,
                    size: 0,
                });
            } else {
                for (position, chunk) in data.chunks(CHUNK_SIZE).enumerate() {
                    let h = crate::hash::sha256_hex(chunk);
                    if !owned.contains(&h) {
                        return Err(Error::BadInput(format!(
                            "条目 {name} 的第 {position} 块 {h} 不在会话收据中"
                        )));
                    }
                    chunks.push(ChunkRef {
                        position: position as i64,
                        hash: h,
                        size: chunk.len() as i64,
                    });
                }
            }
            result.push(EntryInfo {
                name: name.clone(),
                size: data.len() as i64,
                chunks,
            });
        }
        Ok(result)
    }

    fn build_manifest(
        snapshot_id: &str,
        parent_id: Option<&str>,
        entries: &[EntryInfo],
    ) -> Value {
        json!({
            "format": "cas-snapshot/v1",
            "snapshot": snapshot_id,
            "parent": parent_id,
            "chunk_size": CHUNK_SIZE,
            "entries": entries.iter().map(|e| json!({
                "name": e.name,
                "size": e.size,
                "chunks": e.chunks.iter().map(|c| json!({
                    "position": c.position, "hash": c.hash, "size": c.size
                })).collect::<Vec<_>>()
            })).collect::<Vec<_>>(),
        })
    }

    /// 创建上传会话 -> 提交块 -> 组装条目 -> 冻结。
    ///
    /// 阶段：
    /// 1. `intent`   ：写 manifest 意图（快照尚不可见）。
    /// 2. `refs`     ：原子加入该快照对所有块的引用。
    /// 3. `publish`  ：发布快照、分配代次、移除会话引用并写收据。
    pub fn freeze(
        &self,
        session_id: &str,
        token: i64,
        label: &str,
        parent_id: Option<&str>,
        entries: Vec<EntryInfo>,
    ) -> Result<SnapshotInfo> {
        let now = self.now_ms();
        if entries.is_empty() {
            return Err(Error::BadInput("快照至少需要一个条目".into()));
        }
        let conn = self.conn.lock().unwrap();
        self.check_token(&conn, session_id, token)?;
        if let Some(parent) = parent_id {
            let status: String = conn.query_row(
                "SELECT status FROM snapshots WHERE id = ?1",
                [parent],
                |r| r.get(0),
            ).optional()?
                .ok_or_else(|| Error::NotFound(format!("父快照 {parent} 不存在")))?;
            if status != "published" {
                return Err(Error::Conflict(format!("父快照 {parent} 已删除，不能派生")));
            }
        }

        let snap_id = format!("v_{:016x}", events::next_seq(&conn, "snapshot")?);
        let manifest = Self::build_manifest(&snap_id, parent_id, &entries);
        let _manifest_h = crate::canon::canonical_hash(&manifest);
        let state = FreezeState {
            snapshot_id: snap_id.clone(),
            label: label.to_string(),
            parent_id: parent_id.map(str::to_string),
            session_id: session_id.to_string(),
            manifest: manifest.clone(),
            entries,
        };

        // 阶段 1：意图落盘。
        conn.execute(
            "INSERT INTO pending (kind, phase, state, updated) VALUES ('freeze', 'intent', ?1, ?2)
             ON CONFLICT(kind) DO UPDATE SET phase=excluded.phase, state=excluded.state,
               updated=excluded.updated",
            params![serde_json::to_string(&state).unwrap(), now],
        )?;
        self.crash.hit("freeze.after_intent");

        // 阶段 2：引用（在单个事务内完成，对外原子）。
        self.freeze_add_refs(&conn, &state, now)?;
        // 先把进度推进为 refs 并确保落盘，再允许崩溃，恢复即可从 refs 继续。
        {
            let tx = conn.unchecked_transaction()?;
            tx.execute(
                "UPDATE pending SET phase = 'refs', updated = ?1 WHERE kind = 'freeze'",
                [now],
            )?;
            tx.commit()?;
        }
        self.crash.hit("freeze.after_refs");

        // 阶段 3：发布快照并写入条目（pending 在同一事务清除）。
        self.freeze_publish(&conn, &state, now)?;
        drop(conn);
        self.get_snapshot(&snap_id)
    }

    fn freeze_add_refs(&self, conn: &Connection, state: &FreezeState, now: i64) -> Result<()> {
        let tx = conn.unchecked_transaction()?;
        let mut block_rows: BTreeSet<String> = BTreeSet::new();
        {
            let mut stmt = tx.prepare("SELECT hash FROM blocks")?;
            for r in stmt.query_map([], |r| r.get::<_, String>(0))? {
                block_rows.insert(r?);
            }
        }
        let mut needed: BTreeSet<String> = BTreeSet::new();
        for e in &state.entries {
            for c in &e.chunks {
                needed.insert(c.hash.clone());
            }
        }
        for h in &needed {
            if !block_rows.contains(h) {
                return Err(Error::Corrupt(format!(
                    "冻结前发现块 {h} 在元数据中缺失，拒绝冻结"
                )));
            }
            let present = self.block_path(h).exists();
            if !present {
                return Err(Error::Corrupt(format!(
                    "冻结前发现块 {h} 文件缺失，拒绝冻结"
                )));
            }
            tx.execute(
                "INSERT OR IGNORE INTO block_refs (block_hash, ref_id) VALUES (?1, ?2)",
                params![h, format!("snap:{}", state.snapshot_id)],
            )?;
        }
        let _ = now;
        tx.commit()?;
        Ok(())
    }

    fn freeze_publish(&self, conn: &Connection, state: &FreezeState, now: i64) -> Result<()> {
        // 幂等：若该快照已经发布过（崩溃后重入），直接成功返回。
        let already: bool = conn
            .query_row(
                "SELECT 1 FROM snapshots WHERE id = ?1",
                [&state.snapshot_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if already {
            return Ok(());
        }
        let tx = conn.unchecked_transaction()?;
        // 发布与清除 intent 在同一事务：要么快照对外可见且 pending 消失，要么都没有。
        let publish_seq = events::next_seq(&tx, "publish")?;
        tx.execute(
            "INSERT INTO snapshots
                (id, label, parent_id, manifest, manifest_h, publish_seq, status, created)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'published', ?7)",
            params![
                state.snapshot_id,
                state.label,
                state.parent_id,
                state.manifest.to_string(),
                crate::canon::canonical_hash(&state.manifest),
                publish_seq,
                now
            ],
        )?;
        for (ord, entry) in state.entries.iter().enumerate() {
            tx.execute(
                "INSERT INTO entries (snapshot_id, name, ord, size) VALUES (?1, ?2, ?3, ?4)",
                params![state.snapshot_id, entry.name, ord as i64, entry.size],
            )?;
            let entry_id: i64 = tx.last_insert_rowid();
            for c in &entry.chunks {
                tx.execute(
                    "INSERT INTO entry_chunks (entry_id, position, block_hash, chunk_size)
                     VALUES (?1, ?2, ?3, ?4)",
                    params![entry_id, c.position, c.hash, c.size],
                )?;
            }
        }
        // 快照已可见，会话不再需要替它持有引用。
        tx.execute(
            "DELETE FROM block_refs WHERE ref_id = ?1",
            [format!("session:{}", state.session_id)],
        )?;
        tx.execute(
            "UPDATE sessions SET finalized = 1 WHERE id = ?1",
            [state.session_id.clone()],
        )?;
        events::append(
            &tx,
            now,
            "snapshot.frozen",
            json!({
                "snapshot": state.snapshot_id,
                "label": state.label,
                "parent": state.parent_id,
                "manifest_hash": crate::canon::canonical_hash(&state.manifest),
                "publish_seq": publish_seq,
            }),
        )?;
        tx.execute("DELETE FROM pending WHERE kind = 'freeze'", [])?;
        tx.commit()?;
        self.crash.hit("freeze.after_publish");
        Ok(())
    }

    // ------------------------------------------------------------------
    // 崩溃恢复
    // ------------------------------------------------------------------

    /// 打开数据库后执行：完成未提交的冻结/GC，或回滚到“不可见”状态。
    fn recover(&mut self) -> Result<()> {
        self.recover_freeze()?;
        self.recover_gc()?;
        Ok(())
    }

    fn recover_freeze(&mut self) -> Result<()> {
        let row: Option<(String, String)> = {
            let conn = self.conn.lock().unwrap();
            conn.query_row(
                "SELECT phase, state FROM pending WHERE kind = 'freeze'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
        };
        let Some((phase, state_json)) = row else {
            return Ok(());
        };
        let state: FreezeState = serde_json::from_str(&state_json)
            .map_err(|e| Error::Corrupt(format!("freeze intent 无法解析: {e}")))?;

        match phase.as_str() {
            "intent" => {
                // 崩溃发生在引用加入之前：快照从未可见，安全回滚意图。
                let conn = self.conn.lock().unwrap();
                conn.execute("DELETE FROM pending WHERE kind = 'freeze'", [])?;
                events::append_autocommit(
                    &conn,
                    self.now_ms(),
                    "freeze.rolled_back",
                    json!({"snapshot": state.snapshot_id, "at": "intent"}),
                )?;
            }
            "refs" => {
                // 引用可能已加入但快照未发布。块齐全则继续发布；缺失则回滚引用。
                let missing = {
                    let conn = self.conn.lock().unwrap();
                    let mut missing = BTreeSet::new();
                    for e in &state.entries {
                        for c in &e.chunks {
                            let known: bool = conn
                                .query_row(
                                    "SELECT 1 FROM blocks WHERE hash = ?1",
                                    [&c.hash],
                                    |_| Ok(()),
                                )
                                .optional()?
                                .is_some();
                            if !known || !self.block_path(&c.hash).exists() {
                                missing.insert(c.hash.clone());
                            }
                        }
                    }
                    missing
                };
                let conn = self.conn.lock().unwrap();
                if missing.is_empty() {
                    let now = self.now_ms();
                    self.freeze_publish(&conn, &state, now)?;
                    events::append_autocommit(
                        &conn,
                        now,
                        "freeze.resumed",
                        json!({"snapshot": state.snapshot_id, "at": "refs"}),
                    )?;
                } else {
                    let tx = conn.unchecked_transaction()?;
                    tx.execute(
                        "DELETE FROM block_refs WHERE ref_id = ?1",
                        [format!("snap:{}", state.snapshot_id)],
                    )?;
                    // 清理悬空 blocks 行：缺失块若已无任何存活引用，则移除元数据，
                    // 避免留下“无文件的块”。其他快照/会话仍引用时保留（它们也会被验证标出）。
                    for h in &missing {
                        let remaining: i64 = tx.query_row(
                            "SELECT COUNT(*) FROM block_refs WHERE block_hash = ?1",
                            [h],
                            |r| r.get(0),
                        )?;
                        if remaining == 0 {
                            tx.execute("DELETE FROM blocks WHERE hash = ?1", [h])?;
                        }
                    }
                    tx.execute("DELETE FROM pending WHERE kind = 'freeze'", [])?;
                    events::append(
                        &tx,
                        self.now_ms(),
                        "freeze.rolled_back",
                        json!({"snapshot": state.snapshot_id, "at": "refs",
                               "missing": missing.iter().collect::<Vec<_>>()}),
                    )?;
                    tx.commit()?;
                }
            }
            other => {
                return Err(Error::Corrupt(format!("未知 freeze phase: {other}")));
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------------
    // 快照查询、派生与删除
    // ------------------------------------------------------------------

    pub fn get_snapshot(&self, id: &str) -> Result<SnapshotInfo> {
        let conn = self.conn.lock().unwrap();
        self.get_snapshot_locked(&conn, id)
    }

    fn get_snapshot_locked(&self, conn: &Connection, id: &str) -> Result<SnapshotInfo> {
        let row = conn
            .query_row(
                "SELECT id, label, parent_id, manifest, manifest_h, publish_seq, status,
                        created, deleted_at
                 FROM snapshots WHERE id = ?1",
                [id],
                |r| {
                    let manifest_text: String = r.get(3)?;
                    Ok(SnapshotInfo {
                        id: r.get(0)?,
                        label: r.get(1)?,
                        parent_id: r.get(2)?,
                        manifest: serde_json::from_str(&manifest_text).unwrap_or(Value::Null),
                        manifest_hash: r.get(4)?,
                        publish_seq: r.get(5)?,
                        status: r.get(6)?,
                        created: r.get(7)?,
                        deleted_at: r.get(8)?,
                        entries: vec![],
                    })
                },
            )
            .optional()?
            .ok_or_else(|| Error::NotFound(format!("快照 {id} 不存在")))?;

        let mut entry_ids: Vec<(i64, String, i64)> = Vec::new();
        {
            let mut stmt = conn.prepare(
                "SELECT id, name, size FROM entries WHERE snapshot_id = ?1 ORDER BY ord",
            )?;
            for r in stmt.query_map([id], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))? {
                entry_ids.push(r?);
            }
        }
        let mut entries = Vec::new();
        for (entry_id, name, size) in entry_ids {
            let mut chunks = Vec::new();
            {
                let mut stmt = conn.prepare(
                    "SELECT position, block_hash, chunk_size FROM entry_chunks
                     WHERE entry_id = ?1 ORDER BY position",
                )?;
                for r in stmt.query_map([entry_id], |r| {
                    Ok(ChunkRef {
                        position: r.get(0)?,
                        hash: r.get(1)?,
                        size: r.get(2)?,
                    })
                })? {
                    chunks.push(r?);
                }
            }
            entries.push(EntryInfo { name, size, chunks });
        }
        Ok(SnapshotInfo { entries, ..row })
    }

    pub fn list_snapshots(&self, include_deleted: bool) -> Result<Vec<SnapshotInfo>> {
        let conn = self.conn.lock().unwrap();
        self.list_snapshots_locked(&conn, include_deleted)
    }

    fn list_snapshots_locked(
        &self,
        conn: &Connection,
        include_deleted: bool,
    ) -> Result<Vec<SnapshotInfo>> {
        let mut ids: Vec<String> = Vec::new();
        {
            let sql = if include_deleted {
                "SELECT id FROM snapshots ORDER BY COALESCE(publish_seq, 0) DESC, id"
            } else {
                "SELECT id FROM snapshots WHERE status = 'published'
                 ORDER BY COALESCE(publish_seq, 0) DESC, id"
            };
            let mut stmt = conn.prepare(sql)?;
            for r in stmt.query_map([], |r| r.get::<_, String>(0))? {
                ids.push(r?);
            }
        }
        let mut out = Vec::new();
        for id in &ids {
            out.push(self.get_snapshot_locked(conn, id)?);
        }
        Ok(out)
    }

    /// 派生快照：以父快照条目为底，应用更新/删除，未变条目直接复用其块引用。
    pub fn derive(
        &self,
        session_id: &str,
        token: i64,
        label: &str,
        parent_id: &str,
        updates: Vec<(String, Vec<u8>)>,
        remove: Vec<String>,
    ) -> Result<SnapshotInfo> {
        let parent = self.get_snapshot(parent_id)?;
        if parent.status != "published" {
            return Err(Error::Conflict(format!("父快照 {parent_id} 已删除")));
        }
        // 若提供了更新，则用新数据校验并切出会话拥有的块；纯删除/纯复用无需新块。
        let new_entries = if updates.is_empty() {
            Vec::new()
        } else {
            self.assemble_entries(session_id, &updates)?
        };

        let mut merged: BTreeMap<String, EntryInfo> = BTreeMap::new();
        for e in parent.entries {
            merged.insert(e.name.clone(), e);
        }
        for e in new_entries {
            merged.insert(e.name.clone(), e);
        }
        for name in &remove {
            merged.remove(name);
        }
        if merged.is_empty() {
            return Err(Error::BadInput("派生结果为空，至少保留一个条目".into()));
        }
        let entries = merged.into_values().collect();
        self.freeze(session_id, token, label, Some(parent_id), entries)
    }

    /// 删除快照：只移除该快照自己的引用，不触碰共享块（其他快照仍引用它）。
    pub fn delete_snapshot(&self, id: &str) -> Result<EventInfo> {
        let now = self.now_ms();
        let conn = self.conn.lock().unwrap();
        let status: String = conn
            .query_row("SELECT status FROM snapshots WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .optional()?
            .ok_or_else(|| Error::NotFound(format!("快照 {id} 不存在")))?;
        if status != "published" {
            return Err(Error::Conflict(format!("快照 {id} 不是已发布状态")));
        }
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "UPDATE snapshots SET status = 'deleted', deleted_at = ?1 WHERE id = ?2",
            params![now, id],
        )?;
        // 删除该快照的条目与引用，但块行保留（可能被其他快照引用）。
        tx.execute(
            "DELETE FROM entry_chunks WHERE entry_id IN
                (SELECT id FROM entries WHERE snapshot_id = ?1)",
            [id],
        )?;
        tx.execute("DELETE FROM entries WHERE snapshot_id = ?1", [id])?;
        let removed = tx.execute(
            "DELETE FROM block_refs WHERE ref_id = ?1",
            [format!("snap:{id}")],
        )?;
        let event = events::append(
            &tx,
            now,
            "snapshot.deleted",
            json!({"snapshot": id, "refs_removed": removed}),
        )?;
        tx.commit()?;
        Ok(EventInfo {
            seq: event.seq,
            ts: event.ts,
            kind: event.kind,
            payload: event.payload,
            prev_hash: event.prev_hash,
            hash: event.hash,
        })
    }

    // ------------------------------------------------------------------
    // 块与引用来源
    // ------------------------------------------------------------------

    pub fn list_blocks(&self) -> Result<Vec<BlockInfo>> {
        let conn = self.conn.lock().unwrap();
        let mut hashes: Vec<String> = {
            let mut stmt = conn.prepare("SELECT hash FROM blocks ORDER BY hash")?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            let mut v = Vec::new();
            for r in rows { v.push(r?); }
            v
        };
        let mut out = Vec::new();
        for hash in hashes.drain(..) {
            let size: i64 = conn.query_row(
                "SELECT size, created FROM blocks WHERE hash = ?1",
                [&hash],
                |r| r.get(0),
            )?;
            let created: i64 = conn.query_row(
                "SELECT created FROM blocks WHERE hash = ?1",
                [&hash],
                |r| r.get(0),
            )?;
            let mut refs = Vec::new();
            {
                let mut stmt = conn.prepare(
                    "SELECT ref_id FROM block_refs WHERE block_hash = ?1 ORDER BY ref_id",
                )?;
                for r in stmt.query_map([&hash], |r| r.get::<_, String>(0))? {
                    let ref_id = r?;
                    let label = if let Some(sid) = ref_id.strip_prefix("snap:") {
                        conn.query_row(
                            "SELECT label FROM snapshots WHERE id = ?1",
                            [sid],
                            |r| r.get::<_, String>(0),
                        )
                        .unwrap_or_else(|_| sid.to_string())
                    } else if let Some(sid) = ref_id.strip_prefix("session:") {
                        conn.query_row(
                            "SELECT label FROM sessions WHERE id = ?1",
                            [sid],
                            |r| r.get::<_, String>(0),
                        )
                        .unwrap_or_else(|_| sid.to_string())
                    } else {
                        ref_id.clone()
                    };
                    let kind = if ref_id.starts_with("snap:") {
                        "snapshot"
                    } else {
                        "session"
                    };
                    refs.push(RefSource {
                        ref_id,
                        kind: kind.to_string(),
                        label,
                    });
                }
            }
            let file_present = self.block_path(&hash).exists();
            out.push(BlockInfo {
                hash,
                size,
                created,
                refs,
                file_present,
            });
        }
        Ok(out)
    }

    /// 返回每个被快照引用的块 -> 引用它的已发布快照（共享关系）。
    pub fn block_sharing(&self) -> Result<BTreeMap<String, Vec<String>>> {
        let conn = self.conn.lock().unwrap();
        let mut map: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut stmt = conn.prepare(
            "SELECT block_hash, ref_id FROM block_refs
             WHERE ref_id LIKE 'snap:%' ORDER BY block_hash, ref_id",
        )?;
        for r in stmt.query_map([], |r| {
            Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
        })? {
            let (hash, ref_id) = r?;
            map.entry(hash).or_default().push(ref_id);
        }
        Ok(map)
    }

    // ------------------------------------------------------------------
    // 完整性验证与修复
    // ------------------------------------------------------------------

    /// 验证：块文件重哈希、引用一致性、manifest 重算、事件链；并列出受影响快照。
    pub fn verify(&self) -> Result<VerificationReport> {
        let now = self.now_ms();
        let conn = self.conn.lock().unwrap();
        let mut items = Vec::new();
        let mut bad_blocks: BTreeSet<String> = BTreeSet::new();

        let blocks = self.list_blocks_locked(&conn)?;
        for (block_hash, block_size) in &blocks {
            let path = self.block_path(block_hash);
            let item = match fs::read(&path) {
                Ok(bytes) => {
                    let actual = crate::hash::sha256_hex(&bytes);
                    if &actual == block_hash && bytes.len() as i64 == *block_size {
                        VerificationItem {
                            hash: block_hash.clone(),
                            ok: true,
                            detail: format!("{} 字节，摘要一致", bytes.len()),
                        }
                    } else if actual != *block_hash {
                        bad_blocks.insert(block_hash.clone());
                        VerificationItem {
                            hash: block_hash.clone(),
                            ok: false,
                            detail: format!(
                                "篡改：路径名为 {block_hash}，实际内容摘要 {actual}"
                            ),
                        }
                    } else {
                        bad_blocks.insert(block_hash.clone());
                        VerificationItem {
                            hash: block_hash.clone(),
                            ok: false,
                            detail: format!("大小不符：登记 {block_size}，实际 {}", bytes.len()),
                        }
                    }
                }
                Err(e) => {
                    bad_blocks.insert(block_hash.clone());
                    VerificationItem {
                        hash: block_hash.clone(),
                        ok: false,
                        detail: format!("文件缺失/不可读: {e}"),
                    }
                }
            };
            items.push(item);
        }

        // manifest 重算与“引用不存在块”检查。
        let snapshots = self.list_snapshots_locked(&conn, true)?;
        let mut affected: BTreeSet<String> = BTreeSet::new();
        for s in &snapshots {
            let recomputed = crate::canon::canonical_hash(&s.manifest);
            if recomputed != s.manifest_hash {
                items.push(VerificationItem {
                    hash: s.manifest_hash.clone(),
                    ok: false,
                    detail: format!("快照 {} manifest 摘要被改写", s.id),
                });
                affected.insert(s.id.clone());
            }
            for e in &s.entries {
                for c in &e.chunks {
                    if bad_blocks.contains(&c.hash) {
                        affected.insert(s.id.clone());
                    }
                }
            }
            // 引用集合与条目块集合必须一致。
            let mut ref_set: BTreeSet<String> = BTreeSet::new();
            {
                let mut stmt = conn.prepare(
                    "SELECT block_hash FROM block_refs WHERE ref_id = ?1",
                )?;
                for r in stmt.query_map([format!("snap:{}", s.id)], |r| {
                    r.get::<_, String>(0)
                })? {
                    ref_set.insert(r?);
                }
            }
            for e in &s.entries {
                for c in &e.chunks {
                    if !ref_set.contains(&c.hash) {
                        items.push(VerificationItem {
                            hash: c.hash.clone(),
                            ok: false,
                            detail: format!(
                                "快照 {} 引用块 {} 但 block_refs 缺行",
                                s.id, c.hash
                            ),
                        });
                        affected.insert(s.id.clone());
                    }
                }
            }
        }

        let chain = events::verify_chain(&conn)?;
        let ok = items.iter().all(|i| i.ok) && chain.ok();
        let payload = json!({
            "ok": ok,
            "blocks_checked": blocks.len(),
            "snapshots_checked": snapshots.len(),
            "chain_ok": chain.ok(),
            "bad_blocks": bad_blocks.iter().collect::<Vec<_>>(),
            "affected_snapshots": affected.iter().collect::<Vec<_>>(),
        });
        let event = events::append(&conn, now, "verify", payload)?;
        Ok(VerificationReport {
            ok,
            blocks_checked: blocks.len(),
            snapshots_checked: snapshots.len(),
            chain_ok: chain.ok(),
            chain_head: chain.head_hash.unwrap_or_default(),
            items,
            affected_snapshots: affected.into_iter().collect(),
            ts: now,
            receipt_hash: Some(event.hash),
        })
    }

    fn list_blocks_locked(&self, conn: &Connection) -> Result<Vec<(String, i64)>> {
        let mut stmt = conn.prepare("SELECT hash, size FROM blocks ORDER BY hash")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    /// 修复：用重新上传的正确字节覆盖被篡改/缺失块。摘要必须匹配块名，
    /// 不允许“修复”成新摘要（那应走新块+新快照流程）。修复形成收据事件。
    pub fn repair_block(&self, expected_hash: &str, data: &[u8]) -> Result<EventInfo> {
        let now = self.now_ms();
        if !crate::hash::is_hash_hex(expected_hash) {
            return Err(Error::BadInput("不是合法摘要".into()));
        }
        let actual = crate::hash::sha256_hex(data);
        if actual != expected_hash {
            return Err(Error::Conflict(format!(
                "修复被拒：上传内容摘要 {actual} 与目标块 {expected_hash} 不同；\
                 不能悄悄改摘要，请以新内容冻结新快照"
            )));
        }
        let conn = self.conn.lock().unwrap();
        let known: bool = conn
            .query_row("SELECT 1 FROM blocks WHERE hash = ?1", [expected_hash], |_| {
                Ok(())
            })
            .optional()?
            .is_some();
        if !known {
            return Err(Error::NotFound(format!("块 {expected_hash} 不在元数据中")));
        }
        let before = self.block_path(expected_hash).exists();
        // 原子替换。
        let tmp = self
            .data_dir
            .join("tmp")
            .join(format!("repair-{expected_hash}-{now}"));
        fs::write(&tmp, data)?;
        let target = self.block_path(expected_hash);
        fs::create_dir_all(target.parent().unwrap())?;
        fs::rename(&tmp, &target)?;
        let event = events::append_autocommit(
            &conn,
            now,
            "block.repaired",
            json!({"block": expected_hash, "had_file": before, "size": data.len()}),
        )?;
        Ok(EventInfo {
            seq: event.seq,
            ts: event.ts,
            kind: event.kind,
            payload: event.payload,
            prev_hash: event.prev_hash,
            hash: event.hash,
        })
    }

    // ------------------------------------------------------------------
    // 垃圾回收：代次屏障
    // ------------------------------------------------------------------

    /// 执行一次 GC。
    ///
    /// `during_scan` 回调在“标记完成、删除开始之前”执行（测试用它插入新冻结
    /// 快照）。屏障保证：屏障点之后新发布的快照引用的块，即使当时无引用行，
    /// 也不会被本轮删除。
    pub fn gc(
        &self,
        during_scan: Option<&dyn Fn()>,
    ) -> Result<GcReport> {
        let now = self.now_ms();
        let conn = self.conn.lock().unwrap();

        // 1) 建立代次屏障：记录“当前已发布快照最大序号”。扫描集在屏障点固定。
        let barrier_seq: i64 = conn
            .query_row(
                "SELECT COALESCE(MAX(publish_seq), 0) FROM snapshots WHERE status = 'published'",
                [],
                |r| r.get(0),
            )
            .unwrap_or(0);
        let gc_id = format!("gc_{:016x}", events::next_seq(&conn, "gc")?);

        // 清理租约已过期会话持有的引用（它们已不能提交/冻结）。
        self.drop_expired_session_refs(&conn, now)?;

        // 2) 标记：屏障时刻所有引用行对应的块为存活；其余为候选。
        let mut live: BTreeSet<String> = BTreeSet::new();
        {
            let mut stmt = conn.prepare("SELECT DISTINCT block_hash FROM block_refs")?;
            for r in stmt.query_map([], |r| r.get::<_, String>(0))? {
                live.insert(r?);
            }
        }
        // 屏障点的根集合还包括“屏障序号及以前”的已发布快照引用（与 block_refs
        // 正常一致；此处显式重算，确保删除事务之外也有一致快照）。
        let mut all_blocks: Vec<String> = {
            let mut stmt = conn.prepare("SELECT hash FROM blocks")?;
            let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
            let mut v = Vec::new();
            for r in rows { v.push(r?); }
            v
        };
        let candidates: Vec<String> = all_blocks
            .iter()
            .filter(|h| !live.contains(*h))
            .cloned()
            .collect();

        let start_event = events::append_autocommit(
            &conn,
            now,
            "gc.started",
            json!({
                "gc": gc_id,
                "barrier_seq": barrier_seq,
                "candidates": candidates,
            }),
        )?;

        let state = GcState {
            gc_id: gc_id.clone(),
            barrier_seq,
            start_event: start_event.hash.clone(),
            candidates: candidates.clone(),
            deleted: vec![],
            spared: vec![],
        };
        conn.execute(
            "INSERT INTO pending (kind, phase, state, updated)
             VALUES ('gc', 'deleting', ?1, ?2)
             ON CONFLICT(kind) DO UPDATE SET phase=excluded.phase, state=excluded.state,
               updated=excluded.updated",
            params![serde_json::to_string(&state).unwrap(), now],
        )?;
        self.crash.hit("gc.after_intent");

        // 3) 扫描窗口：测试在此并发冻结新快照。新快照获得 publish_seq > barrier。
        drop(conn);
        if let Some(hook) = during_scan {
            hook();
        }
        let conn = self.conn.lock().unwrap();

        // 4) 逐块在删除事务内重查引用 + 代次屏障。任一保护成立则保留。
        let mut deleted = Vec::new();
        let mut spared = Vec::new();
        for hash in &candidates {
            let tx = conn.unchecked_transaction()?;
            let ref_count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM block_refs WHERE block_hash = ?1",
                [hash],
                |r| r.get(0),
            )?;
            // 屏障保护：扫描期间新冻结（序号 > barrier）的快照若引用该块，则保留。
            let new_root: i64 = tx.query_row(
                "SELECT COUNT(*) FROM entry_chunks ec
                 JOIN entries e ON e.id = ec.entry_id
                 JOIN snapshots s ON s.id = e.snapshot_id
                 WHERE ec.block_hash = ?1
                   AND s.status = 'published'
                   AND COALESCE(s.publish_seq, 0) > ?2",
                params![hash, barrier_seq],
                |r| r.get(0),
            )?;
            // 过期会话可能在窗口内被续租并重新持有：同样按 refs 计数保护（已含于 ref_count）。
            if ref_count > 0 || new_root > 0 {
                let reason = if new_root > 0 {
                    "generation-barrier"
                } else {
                    "refs-reappeared"
                };
                events::append(
                    &tx,
                    now,
                    "gc.spared",
                    json!({"gc": gc_id, "block": hash, "reason": reason}),
                )?;
                tx.commit()?;
                spared.push(hash.clone());
                continue;
            }
            tx.execute("DELETE FROM block_refs WHERE block_hash = ?1", [hash])?;
            tx.execute("DELETE FROM blocks WHERE hash = ?1", [hash])?;
            events::append(
                &tx,
                now,
                "gc.block_deleted",
                json!({"gc": gc_id, "block": hash}),
            )?;
            tx.commit()?;

            // 元数据删除提交后再删文件；文件缺失也视为删除完成（幂等）。
            match fs::remove_file(self.block_path(hash)) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(Error::Io(e.to_string())),
            }
            deleted.push(hash.clone());
        }

        self.crash.hit("gc.before_end");
        let end_event = events::append_autocommit(
            &conn,
            self.now_ms(),
            "gc.finished",
            json!({
                "gc": gc_id,
                "barrier_seq": barrier_seq,
                "deleted": deleted,
                "spared": spared,
                "start_event": start_event.hash,
            }),
        )?;
        conn.execute("DELETE FROM pending WHERE kind = 'gc'", [])?;
        let _ = all_blocks.drain(..);

        Ok(GcReport {
            gc_id,
            barrier_seq,
            candidates,
            deleted,
            spared_by_refs: spared,
            phase: "finished".to_string(),
            start_event: start_event.hash,
            end_event: Some(end_event.hash),
        })
    }

    fn drop_expired_session_refs(&self, conn: &Connection, now: i64) -> Result<()> {
        let expired: Vec<String> = {
            let mut stmt =
                conn.prepare("SELECT id FROM sessions WHERE finalized = 0 AND expires < ?1")?;
            let rows = stmt.query_map([now], |r| r.get::<_, String>(0))?;
            let mut v = Vec::new();
            for r in rows { v.push(r?); }
            v
        };
        for id in expired {
            conn.execute(
                "DELETE FROM block_refs WHERE ref_id = ?1",
                [format!("session:{id}")],
            )?;
        }
        Ok(())
    }

    fn recover_gc(&mut self) -> Result<()> {
        let row: Option<(String, String)> = {
            let conn = self.conn.lock().unwrap();
            conn.query_row(
                "SELECT phase, state FROM pending WHERE kind = 'gc'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?
        };
        let Some((_phase, state_json)) = row else {
            return Ok(());
        };
        let state: GcState = serde_json::from_str(&state_json)
            .map_err(|e| Error::Corrupt(format!("gc intent 无法解析: {e}")))?;

        // 恢复时重新评估每个候选：仍无引用且不被新代次保护才删。
        let now = self.now_ms();
        let conn = self.conn.lock().unwrap();
        let mut deleted = state.deleted.clone();
        let mut spared = state.spared.clone();
        for hash in &state.candidates {
            if deleted.contains(hash) {
                // 元数据可能已删而文件未删；尽力清理文件。
                let _ = fs::remove_file(self.block_path(hash));
                continue;
            }
            let tx = conn.unchecked_transaction()?;
            let known: bool = tx
                .query_row("SELECT 1 FROM blocks WHERE hash = ?1", [hash], |_| Ok(()))
                .optional()?
                .is_some();
            if !known {
                tx.commit()?;
                let _ = fs::remove_file(self.block_path(hash));
                deleted.push(hash.clone());
                continue;
            }
            let ref_count: i64 = tx.query_row(
                "SELECT COUNT(*) FROM block_refs WHERE block_hash = ?1",
                [hash],
                |r| r.get(0),
            )?;
            let new_root: i64 = tx.query_row(
                "SELECT COUNT(*) FROM entry_chunks ec
                 JOIN entries e ON e.id = ec.entry_id
                 JOIN snapshots s ON s.id = e.snapshot_id
                 WHERE ec.block_hash = ?1 AND s.status = 'published'
                   AND COALESCE(s.publish_seq, 0) > ?2",
                params![hash, state.barrier_seq],
                |r| r.get(0),
            )?;
            if ref_count > 0 || new_root > 0 {
                tx.commit()?;
                spared.push(hash.clone());
                continue;
            }
            tx.execute("DELETE FROM block_refs WHERE block_hash = ?1", [hash])?;
            tx.execute("DELETE FROM blocks WHERE hash = ?1", [hash])?;
            tx.commit()?;
            let _ = fs::remove_file(self.block_path(hash));
            deleted.push(hash.clone());
        }
        events::append_autocommit(
            &conn,
            now,
            "gc.resumed",
            json!({
                "gc": state.gc_id,
                "barrier_seq": state.barrier_seq,
                "deleted": deleted,
                "spared": spared,
                "start_event": state.start_event,
            }),
        )?;
        conn.execute("DELETE FROM pending WHERE kind = 'gc'", [])?;
        Ok(())
    }

    /// 读取最近一次验证事件（不产生新事件），用于页面展示。
    pub fn last_verification_event(&self) -> Option<EventInfo> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT seq, ts, kind, payload, prev_hash, hash FROM events
             WHERE kind = 'verify' ORDER BY seq DESC LIMIT 1",
            [],
            |r| {
                let payload_text: String = r.get(3)?;
                Ok(EventInfo {
                    seq: r.get(0)?,
                    ts: r.get(1)?,
                    kind: r.get(2)?,
                    payload: serde_json::from_str(&payload_text).unwrap_or(Value::Null),
                    prev_hash: r.get(4)?,
                    hash: r.get(5)?,
                })
            },
        )
        .optional()
        .ok()
        .flatten()
    }

    // ------------------------------------------------------------------
    // 事件日志
    // ------------------------------------------------------------------

    pub fn list_events(&self, limit: i64) -> Result<Vec<EventInfo>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT seq, ts, kind, payload, prev_hash, hash
             FROM events ORDER BY seq DESC LIMIT ?1",
        )?;
        let rows = stmt.query_map(params![limit], |r| {
            let payload_text: String = r.get(3)?;
            Ok(EventInfo {
                seq: r.get(0)?,
                ts: r.get(1)?,
                kind: r.get(2)?,
                payload: serde_json::from_str(&payload_text).unwrap_or(Value::Null),
                prev_hash: r.get(4)?,
                hash: r.get(5)?,
            })
        })?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r?);
        }
        Ok(out)
    }

    // ------------------------------------------------------------------
    // 导出 / 导入：只带 manifest 与验证证明，不带块数据
    // ------------------------------------------------------------------

    pub fn export_snapshot(&self, id: &str) -> Result<Value> {
        let snap = self.get_snapshot(id)?;
        if snap.status != "published" {
            return Err(Error::Conflict("只能导出已发布快照".into()));
        }
        let conn = self.conn.lock().unwrap();
        // 证明：冻结收据 + 当前链头。冻结收据的 hash 由哈希链覆盖，不可伪造。
        let freeze_event = conn
            .query_row(
                "SELECT seq, payload, hash FROM events
                 WHERE kind = 'snapshot.frozen'
                   AND json_extract(payload, '$.snapshot') = ?1",
                [id],
                |r| {
                    Ok(json!({
                        "seq": r.get::<_, i64>(0)?,
                        "payload": serde_json::from_str::<Value>(&r.get::<_, String>(1)?)
                            .unwrap_or(Value::Null),
                        "hash": r.get::<_, String>(2)?,
                    }))
                },
            )
            .optional()?
            .ok_or_else(|| Error::Corrupt("缺少冻结收据事件".into()))?;
        let head: (i64, String) = conn.query_row(
            "SELECT seq, hash FROM events ORDER BY seq DESC LIMIT 1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        let bundle = json!({
            "bundle": "cas-snapshot-export/v1",
            "exported_at": self.now_ms(),
            "snapshot_id": snap.id,
            "manifest": snap.manifest,
            "proof": {
                "manifest_hash": snap.manifest_hash,
                "freeze_event": freeze_event,
                "chain_head": {"seq": head.0, "hash": head.1},
                "note": "导出不含块数据；导入时仅核对 manifest 规范化摘要与冻结收据/链证明",
            },
        });
        Ok(bundle)
    }

    /// 核对导入包：manifest 规范化摘要是否一致；冻结收据内嵌 manifest_hash 是否一致。
    /// 不写入任何块，也不持久化快照（纯核对）。
    pub fn import_check(&self, bundle: &Value) -> Result<ImportCheck> {
        let mut detail = Vec::new();
        let manifest = bundle
            .get("manifest")
            .ok_or_else(|| Error::BadInput("导出包缺少 manifest".into()))?;
        let proof = bundle
            .get("proof")
            .ok_or_else(|| Error::BadInput("导出包缺少 proof".into()))?;
        let claimed = proof
            .get("manifest_hash")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let recomputed = crate::canon::canonical_hash(manifest);
        let m_match = recomputed == claimed;
        detail.push(format!(
            "manifest 规范化摘要：重算 {recomputed}，导出声明 {claimed}，{}",
            if m_match { "一致" } else { "不一致" }
        ));

        // manifest 中的 snapshot 字段应与顶层 snapshot_id 一致。
        let top_id = bundle.get("snapshot_id").and_then(|v| v.as_str());
        let man_id = manifest.get("snapshot").and_then(|v| v.as_str());
        let id_match = top_id.is_some() && top_id == man_id;
        detail.push(format!(
            "snapshot 标识：导出 {} / manifest {}，{}",
            top_id.unwrap_or("?"),
            man_id.unwrap_or("?"),
            if id_match { "一致" } else { "不一致" }
        ));

        let f_match = proof
            .get("freeze_event")
            .and_then(|e| e.get("payload"))
            .and_then(|p| p.get("manifest_hash"))
            .and_then(|v| v.as_str())
            .map(|h| {
                let same = h == recomputed;
                detail.push(format!(
                    "冻结收据内 manifest_hash：{h}，{}",
                    if same { "与重算一致" } else { "与重算不一致" }
                ));
                same
            })
            .unwrap_or(false);

        // 若本库恰好也有该冻结事件，则顺带核对收据哈希是否同一条。
        if let Some(sid) = man_id {
            let conn = self.conn.lock().unwrap();
            let local: Option<String> = conn
                .query_row(
                    "SELECT hash FROM events WHERE kind = 'snapshot.frozen'
                       AND json_extract(payload, '$.snapshot') = ?1",
                    [sid],
                    |r| r.get(0),
                )
                .optional()?;
            let exported_ev_hash = proof
                .get("freeze_event")
                .and_then(|e| e.get("hash"))
                .and_then(|v| v.as_str());
            match (local, exported_ev_hash) {
                (Some(local), Some(exp)) => detail.push(format!(
                    "本库冻结收据 {local} 与导出收据 {exp}：{}",
                    if local == exp { "同一条" } else { "不同（可能来自另一实例）" }
                )),
                _ => detail.push("本库无对应冻结收据，仅核对导出包自洽性".to_string()),
            }
        }

        let ok = m_match && id_match && f_match;
        Ok(ImportCheck {
            ok,
            manifest_hash: recomputed,
            manifest_hash_matches: m_match,
            freeze_receipt_matches: f_match,
            detail,
        })
    }

    /// 强制标记一个会话过期（演示/测试租约拒绝旧提交）。
    pub fn expire_session_for_demo(&self, session_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        let n = conn.execute(
            "UPDATE sessions SET expires = -1 WHERE id = ?1",
            [session_id],
        )?;
        if n == 0 {
            return Err(Error::NotFound(format!("会话 {session_id} 不存在")));
        }
        Ok(())
    }
}
