#[path = "common/mod.rs"]
mod common;

use std::collections::BTreeSet;

use cas_snapshot::engine::Engine;

fn block_path(dir: &std::path::Path, hash: &str) -> std::path::PathBuf {
    dir.join("blocks").join(&hash[0..2]).join(&hash[2..])
}

#[test]
fn tampered_block_is_located_and_lists_all_affected_snapshots() {
    let dir = common::temp_dir("tamper");
    let e = Engine::open(&dir).unwrap();
    let common = b"shared common bytes";
    let a_only = b"only in alpha";
    let b_only = b"only in beta";

    let alpha = common::freeze_blob(&e, "alpha", None, &[
        ("common.txt", common), ("a.txt", a_only),
    ]);
    let beta = common::freeze_blob(&e, "beta", None, &[
        ("common.txt", common), ("b.txt", b_only),
    ]);

    // 找到共享块摘要。
    let shared_hash = {
        let snap = e.get_snapshot(&alpha.id).unwrap();
        snap.entries
            .iter()
            .find(|x| x.name == "common.txt")
            .unwrap()
            .chunks[0]
            .hash
            .clone()
    };

    // 手工篡改块文件字节（不改文件名/摘要）。
    let path = block_path(&dir, &shared_hash);
    std::fs::write(&path, b"TAMPERED CONTENT NOT EQUAL TO HASH").unwrap();

    let report = e.verify().unwrap();
    assert!(!report.ok, "篡改后验证必须失败");
    let affected: BTreeSet<_> = report.affected_snapshots.iter().cloned().collect();
    assert!(affected.contains(&alpha.id), "必须定位到 alpha");
    assert!(affected.contains(&beta.id), "共享块被 beta 引用，也必须列出");

    // 不能静默改摘要：用不同字节“修复”应被拒绝。
    let err = e.repair_block(&shared_hash, b"some other bytes").unwrap_err();
    assert!(err.to_string().contains("不能悄悄改摘要") || err.to_string().contains("修复被拒"));

    // 正确做法：重新上传原始字节，摘要与块名一致。
    e.repair_block(&shared_hash, common).unwrap();
    let report2 = e.verify().unwrap();
    assert!(report2.ok, "重新上传后恢复: {:?}", report2.affected_snapshots);

    // 修复留下 block.repaired 事件。
    assert!(e
        .list_events(100)
        .unwrap()
        .iter()
        .any(|x| x.kind == "block.repaired"));
}

#[test]
fn missing_block_file_is_detected_and_repairable() {
    let dir = common::temp_dir("missing");
    let e = Engine::open(&dir).unwrap();
    let data = b"some unique bytes for missing test";
    let snap = common::freeze_blob(&e, "v", None, &[("f", data)]);
    let hash = e.get_snapshot(&snap.id).unwrap().entries[0].chunks[0]
        .hash
        .clone();
    let path = block_path(&dir, &hash);
    std::fs::remove_file(&path).unwrap();
    let report = e.verify().unwrap();
    assert!(!report.ok);
    assert!(report.affected_snapshots.contains(&snap.id));
    e.repair_block(&hash, data).unwrap();
    assert!(e.verify().unwrap().ok);
}

#[test]
fn event_chain_detects_deleted_or_modified_event() {
    let dir = common::temp_dir("chain");
    let e = Engine::open(&dir).unwrap();
    common::freeze_blob(&e, "v", None, &[("f", b"x")]);
    // 正常验证链完好。
    assert!(e.verify().unwrap().chain_ok);
    // 直接改库删除一条事件（模拟外部破坏），链必须断裂。
    e.with_conn(|c| {
        c.execute("DELETE FROM events WHERE seq = (SELECT MIN(seq) FROM events)", [])
    })
    .unwrap();
    let report = e.verify().unwrap();
    assert!(!report.chain_ok);
}
