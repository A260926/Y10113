//! 只追加、哈希串联的收据日志。每条事件摘要覆盖其前一条摘要，
//! 任何删除/改写都会破坏后续链，可独立核验。

use rusqlite::{Connection, OptionalExtension};
use serde_json::{json, Value};

use crate::canon::canonical_hash;

pub const GENESIS: &str = "0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, Clone)]
pub struct Event {
    pub seq: i64,
    pub ts: i64,
    pub kind: String,
    pub payload: Value,
    pub prev_hash: String,
    pub hash: String,
}

fn compute_hash(seq: i64, ts: i64, kind: &str, payload: &Value, prev_hash: &str) -> String {
    canonical_hash(&json!({
        "seq": seq,
        "ts": ts,
        "kind": kind,
        "payload": payload,
        "prev": prev_hash,
    }))
}

fn insert(conn: &Connection, ts: i64, kind: &str, payload: Value) -> rusqlite::Result<Event> {
    let seq = next_seq(conn, "event")?;
    let prev: String = conn
        .query_row(
            "SELECT hash FROM events ORDER BY seq DESC LIMIT 1",
            [],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or_else(|| GENESIS.to_string());
    let hash = compute_hash(seq, ts, kind, &payload, &prev);
    conn.execute(
        "INSERT INTO events (seq, ts, kind, payload, prev_hash, hash)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params![
            seq,
            ts,
            kind,
            serde_json::to_string(&payload).unwrap(),
            prev,
            hash
        ],
    )?;
    Ok(Event {
        seq,
        ts,
        kind: kind.to_string(),
        payload,
        prev_hash: prev,
        hash,
    })
}

/// 追加一条事件（参与调用方事务）。调用方需保证在事务中或接受自动提交。
pub fn append(conn: &Connection, ts: i64, kind: &str, payload: Value) -> rusqlite::Result<Event> {
    insert(conn, ts, kind, payload)
}

/// 独立调用：包一层短事务，避免事件插入半完成。
pub fn append_autocommit(
    conn: &Connection,
    ts: i64,
    kind: &str,
    payload: Value,
) -> rusqlite::Result<Event> {
    let tx = conn.unchecked_transaction()?;
    let event = insert(&tx, ts, kind, payload)?;
    tx.commit()?;
    Ok(event)
}

pub fn next_seq(conn: &Connection, name: &str) -> rusqlite::Result<i64> {
    conn.execute(
        "INSERT INTO seq (name, value) VALUES (?1, 1)
         ON CONFLICT(name) DO UPDATE SET value = value + 1",
        [name],
    )?;
    conn.query_row("SELECT value FROM seq WHERE name = ?1", [name], |r| r.get(0))
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ChainCheck {
    pub count: i64,
    pub head_seq: Option<i64>,
    pub head_hash: Option<String>,
    pub broken: Option<String>,
}

impl ChainCheck {
    pub fn ok(&self) -> bool {
        self.broken.is_none()
    }
}

/// 重放整条事件链，校验序号连续、prev 链接与摘要重算一致。
pub fn verify_chain(conn: &Connection) -> rusqlite::Result<ChainCheck> {
    let mut stmt = conn.prepare(
        "SELECT seq, ts, kind, payload, prev_hash, hash FROM events ORDER BY seq ASC",
    )?;
    let rows = stmt.query_map([], |r| {
        let payload_text: String = r.get(3)?;
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, i64>(1)?,
            r.get::<_, String>(2)?,
            payload_text,
            r.get::<_, String>(4)?,
            r.get::<_, String>(5)?,
        ))
    })?;

    let mut check = ChainCheck::default();
    let mut expected_prev = GENESIS.to_string();
    let mut expected_seq: i64 = 1;
    for row in rows {
        let (seq, ts, kind, payload_text, prev, hash) = row?;
        let payload: Value = serde_json::from_str(&payload_text).unwrap_or(Value::Null);
        if seq != expected_seq {
            check.broken = Some(format!("seq 不连续：期望 {expected_seq}，实际 {seq}"));
            return Ok(check);
        }
        if prev != expected_prev {
            check.broken = Some(format!("事件 {seq} 的前驱链接不匹配"));
            return Ok(check);
        }
        let recomputed = compute_hash(seq, ts, &kind, &payload, &prev);
        if recomputed != hash {
            check.broken = Some(format!("事件 {seq} 摘要重算不一致"));
            return Ok(check);
        }
        expected_prev = hash.clone();
        check.head_seq = Some(seq);
        check.head_hash = Some(hash);
        check.count += 1;
        expected_seq += 1;
    }
    Ok(check)
}
