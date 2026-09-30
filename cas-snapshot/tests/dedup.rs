#[path = "common/mod.rs"]
mod common;

use cas_snapshot::engine::Engine;

#[test]
fn duplicate_block_only_adds_receipt_not_space() {
    let dir = common::temp_dir("dedup");
    let e = Engine::open(&dir).unwrap();
    let payload = b"hello content-addressed world";

    let s1 = e.create_session("s1", Some(600_000)).unwrap();
    let r1 = e.submit_block(&s1.id, s1.fence, Some("k1"), payload).unwrap();
    assert!(!r1.dedup, "首次上传不是去重");

    // 同会话同内容重复上传：收据 +1，标记 dedup。
    let r2 = e.submit_block(&s1.id, s1.fence, Some("k2"), payload).unwrap();
    assert!(r2.dedup);
    assert_eq!(r1.block_hash, r2.block_hash);

    // 不同会话同内容同样只产生收据。
    let s2 = e.create_session("s2", Some(600_000)).unwrap();
    let r3 = e.submit_block(&s2.id, s2.fence, Some("k1"), payload).unwrap();
    assert!(r3.dedup);
    assert_eq!(r3.block_hash, r1.block_hash);

    // blocks 元数据只有一行，物理文件只有一份。
    assert_eq!(common::block_count(&e), 1);
    assert_eq!(common::physical_block_files(&dir).len(), 1);

    // 两个会话都在引用来源中（直到冻结）。
    let blocks = e.list_blocks().unwrap();
    let refs: Vec<_> = blocks[0].refs.iter().map(|r| r.ref_id.clone()).collect();
    assert!(refs.contains(&format!("session:{}", s1.id)));
    assert!(refs.contains(&format!("session:{}", s2.id)));
}

#[test]
fn idempotency_key_same_content_returns_original_block() {
    let dir = common::temp_dir("idem-same");
    let e = Engine::open(&dir).unwrap();
    let s = e.create_session("s", Some(600_000)).unwrap();
    let r1 = e.submit_block(&s.id, s.fence, Some("key-42"), b"same").unwrap();
    let r2 = e.submit_block(&s.id, s.fence, Some("key-42"), b"same").unwrap();
    assert_eq!(r1.block_hash, r2.block_hash);
    assert_eq!(r1.seq, r2.seq, "幂等命中返回原收据（同 seq）");
}

#[test]
fn idempotency_key_different_content_conflicts() {
    let dir = common::temp_dir("idem-diff");
    let e = Engine::open(&dir).unwrap();
    let s = e.create_session("s", Some(600_000)).unwrap();
    e.submit_block(&s.id, s.fence, Some("key"), b"first content").unwrap();
    let err = e
        .submit_block(&s.id, s.fence, Some("key"), b"second content different")
        .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("冲突"), "应返回冲突: {msg}");
    // 不同内容仍可使用不同幂等键上传。
    e.submit_block(&s.id, s.fence, Some("other"), b"second content different")
        .unwrap();
}

#[test]
fn large_entry_is_split_into_64k_chunks_and_shared() {
    let dir = common::temp_dir("chunks");
    let e = Engine::open(&dir).unwrap();
    // 130KiB，可预测模式以便共享中间块。
    let big: Vec<u8> = (0..130_000u32).map(|i| (i % 251) as u8).collect();
    let snap1 = common::freeze_blob(&e, "big-1", None, &[("blob.bin", &big)]);
    let before = common::block_count(&e);
    assert_eq!(before, 2, "130000 字节应切成 2 块");

    // 另一个快照上传完全相同的大块：全部块去重，blocks 行数不增加。
    let snap2 = common::freeze_blob(&e, "big-2", None, &[("blob.bin", &big)]);
    assert_eq!(common::block_count(&e), before);

    let sharing = e.block_sharing().unwrap();
    let shared: Vec<_> = sharing.values().filter(|v| v.len() > 1).collect();
    assert_eq!(shared.len(), 2, "两个块都应被两个快照共享");

    // 两个快照各自引用计数为 2，删除其一不影响另一个。
    e.delete_snapshot(&snap1.id).unwrap();
    e.gc(None).unwrap();
    assert_eq!(common::block_count(&e), 2, "共享块不应被回收");
    let report = e.verify().unwrap();
    assert!(report.ok);
    let still = e.get_snapshot(&snap2.id).unwrap();
    assert_eq!(still.entries[0].chunks.len(), 2);
}
