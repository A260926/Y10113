#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeSet;

use cas_snapshot::engine::Engine;

fn block_hashes(e: &Engine) -> BTreeSet<String> {
    e.list_blocks().unwrap().into_iter().map(|b| b.hash).collect()
}

#[test]
fn generation_barrier_protects_block_frozen_during_gc_scan() {
    let dir = common::temp_dir("gc-barrier");
    let e = Engine::open(&dir).unwrap();

    // 1) 一个“孤儿”块：由会话提交但会话租约过期、未冻结，理应成为 GC 候选。
    let s = e.create_session("orphan-session", Some(60_000)).unwrap();
    let orphan_data = b"this block will be reborn during gc scan".to_vec();
    let orphan_hash = {
        use cas_snapshot::engine::CHUNK_SIZE;
        let mut h = None;
        for chunk in orphan_data.chunks(CHUNK_SIZE) {
            let r = e
                .submit_block(&s.id, s.fence, Some("orphan"), chunk)
                .unwrap();
            h = Some(r.block_hash);
        }
        h.unwrap()
    };
    e.expire_session_for_demo(&s.id).unwrap();

    // 先跑一次“无害”GC 让已发布快照基线稳定，不影响：孤儿会被删除——
    // 为了精确测试扫描窗口，我们不提前删，而是在 during_scan 中重新提交+冻结。
    // 但过期会话不能再提交，因此在回调里新建一个会话并重新上传相同字节，
    // 在 GC 删除事务之前完成冻结（新代次 > barrier）。
    let engine_ref = &e;
    let orphan_for_closure = orphan_data.clone();
    let report = engine_ref
        .gc(Some(&|| {
            // 扫描窗口回调：新建会话，重新上传相同字节（同摘要），立即冻结。
            let snap = common::freeze_blob(
                engine_ref,
                "reborn-during-scan",
                None,
                &[("reborn.txt", &orphan_for_closure)],
            );
            assert!(snap.publish_seq.unwrap() > 0);
        }))
        .unwrap();

    // 该块必须因代次屏障被保留，而不是被误删。
    let blocks = block_hashes(&e);
    assert!(
        blocks.contains(&orphan_hash),
        "扫描期间新冻结快照引用的块被代次屏障保护；deleted={:?}",
        report.deleted
    );
    let report_v = e.verify().unwrap();
    assert!(report_v.ok, "屏障保护后所有已发布快照必须可验证");

    // 再跑一次 GC（此时新快照已稳定持有引用），块依旧保留。
    let report2 = e.gc(None).unwrap();
    assert!(!report2.deleted.contains(&orphan_hash));
    assert!(block_hashes(&e).contains(&orphan_hash));

    // 删除新快照后，孤儿块才真正成为垃圾被回收。
    let snaps = e.list_snapshots(false).unwrap();
    let reborn = snaps
        .iter()
        .find(|x| x.label == "reborn-during-scan")
        .unwrap();
    e.delete_snapshot(&reborn.id).unwrap();
    let report3 = e.gc(None).unwrap();
    assert!(report3.deleted.contains(&orphan_hash), "失去全部引用后应回收");
    assert!(!block_hashes(&e).contains(&orphan_hash));
}

#[test]
fn gc_deletes_only_truly_orphaned_blocks_and_leaves_receipts() {
    let dir = common::temp_dir("gc-basic");
    let e = Engine::open(&dir).unwrap();
    let keep = common::freeze_blob(&e, "keep", None, &[("k", b"keep")]);

    // 另一个上传后不冻结、且租约过期的块。
    let s = e.create_session("abandoned", Some(1_000)).unwrap();
    let junk = e.submit_block(&s.id, s.fence, Some("junk"), b"junk data").unwrap();
    e.expire_session_for_demo(&s.id).unwrap();

    let report = e.gc(None).unwrap();
    assert!(report.deleted.contains(&junk.block_hash));
    let hashes = block_hashes(&e);
    assert!(!hashes.contains(&junk.block_hash));

    // 物理文件已删；保留快照仍完整。
    let files = common::physical_block_files(&dir);
    assert!(files.iter().all(|p| {
        let name = p.file_name().unwrap().to_string_lossy().to_string();
        let shard = p
            .parent()
            .unwrap()
            .file_name()
            .unwrap()
            .to_string_lossy()
            .to_string();
        format!("{shard}{name}") != junk.block_hash
    }));
    assert!(e.verify().unwrap().ok);
    let _ = keep;
}

#[test]
fn gc_emits_start_and_end_receipts_forming_chain() {
    let dir = common::temp_dir("gc-receipts");
    let e = Engine::open(&dir).unwrap();
    e.gc(None).unwrap();
    let events = e.list_events(100).unwrap();
    assert!(events.iter().any(|x| x.kind == "gc.started"));
    assert!(events.iter().any(|x| x.kind == "gc.finished"));
    assert!(e.verify().unwrap().chain_ok);
}


