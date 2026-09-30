//! SQLite 元数据结构与迁移。所有引用计数由 `block_refs` 行集合推导，
//! 不存在可漂移的计数字段。

pub const SCHEMA_VERSION: u32 = 1;

pub const MIGRATIONS: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 单调递增分配器：fencing token、快照发布序号（=GC 代次）、事件序号。
CREATE TABLE IF NOT EXISTS seq (
    name  TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);

-- 内容寻址块。文件路径由摘要派生：blocks/ab/cdef...
CREATE TABLE IF NOT EXISTS blocks (
    hash    TEXT PRIMARY KEY,
    size    INTEGER NOT NULL,
    created INTEGER NOT NULL
);

-- 上传会话。fence 单调递增，旧 token 的提交被拒；expires 为租约到期时间(ms)。
CREATE TABLE IF NOT EXISTS sessions (
    id        TEXT PRIMARY KEY,
    label     TEXT NOT NULL,
    fence     INTEGER NOT NULL,
    created   INTEGER NOT NULL,
    expires   INTEGER NOT NULL,
    finalized INTEGER NOT NULL DEFAULT 0
);

-- 块上传收据。重复块只新增收据；idem_key 在会话内幂等。
CREATE TABLE IF NOT EXISTS receipts (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id TEXT NOT NULL REFERENCES sessions(id),
    block_hash TEXT NOT NULL,
    idem_key   TEXT,
    size       INTEGER NOT NULL,
    dedup      INTEGER NOT NULL,
    seq        INTEGER NOT NULL,
    created    INTEGER NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_receipts_idem
    ON receipts(session_id, idem_key) WHERE idem_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_receipts_block ON receipts(block_hash);

-- 已冻结快照。publish_seq 即 GC 代次，发布时单调分配。
CREATE TABLE IF NOT EXISTS snapshots (
    id          TEXT PRIMARY KEY,
    label       TEXT NOT NULL,
    parent_id   TEXT,
    manifest    TEXT NOT NULL,
    manifest_h  TEXT NOT NULL,
    publish_seq INTEGER,
    status      TEXT NOT NULL,            -- published | deleted
    created     INTEGER NOT NULL,
    deleted_at  INTEGER
);
CREATE INDEX IF NOT EXISTS idx_snap_seq ON snapshots(publish_seq);

CREATE TABLE IF NOT EXISTS entries (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    snapshot_id TEXT NOT NULL REFERENCES snapshots(id),
    name        TEXT NOT NULL,
    ord         INTEGER NOT NULL,
    size        INTEGER NOT NULL,
    UNIQUE(snapshot_id, name)
);
CREATE INDEX IF NOT EXISTS idx_entries_snap ON entries(snapshot_id);

CREATE TABLE IF NOT EXISTS entry_chunks (
    entry_id     INTEGER NOT NULL REFERENCES entries(id),
    position     INTEGER NOT NULL,
    block_hash   TEXT NOT NULL REFERENCES blocks(hash),
    chunk_size   INTEGER NOT NULL,
    PRIMARY KEY (entry_id, position)
);
CREATE INDEX IF NOT EXISTS idx_chunks_block ON entry_chunks(block_hash);

-- 引用集合：snap:<snapshot_id> 或 session:<session_id>。引用数=行数。
CREATE TABLE IF NOT EXISTS block_refs (
    block_hash TEXT NOT NULL REFERENCES blocks(hash),
    ref_id     TEXT NOT NULL,
    PRIMARY KEY (block_hash, ref_id)
);
CREATE INDEX IF NOT EXISTS idx_refs_ref ON block_refs(ref_id);

-- 崩溃恢复意图：kind = freeze | gc，phase 记录提交进度，state 为 JSON。
CREATE TABLE IF NOT EXISTS pending (
    kind    TEXT PRIMARY KEY,
    phase   TEXT NOT NULL,
    state   TEXT NOT NULL,
    updated INTEGER NOT NULL
);

-- 只追加、哈希串联的事件/收据日志。
CREATE TABLE IF NOT EXISTS events (
    seq       INTEGER PRIMARY KEY,
    ts        INTEGER NOT NULL,
    kind      TEXT NOT NULL,
    payload   TEXT NOT NULL,
    prev_hash TEXT NOT NULL,
    hash      TEXT NOT NULL UNIQUE
);
"#;
