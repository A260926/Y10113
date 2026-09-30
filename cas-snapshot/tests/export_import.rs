#[path = "common/mod.rs"]
mod common;

use cas_snapshot::engine::Engine;

#[test]
fn export_contains_manifest_and_proof_but_no_blocks_and_import_verifies() {
    let dir = common::temp_dir("export");
    let e = Engine::open(&dir).unwrap();
    let snap = common::freeze_blob(&e, "v", None, &[
        ("readme.txt", b"readme content"),
        ("note.txt", b"another entry"),
    ]);

    let bundle = e.export_snapshot(&snap.id).unwrap();
    // 不含块数据：导出包大小应远小于“manifest+证明”，且无 blocks 字段。
    let text = serde_json::to_string(&bundle).unwrap();
    assert!(!text.contains("readme content"), "导出不得携带块字节");
    assert_eq!(bundle["bundle"], "cas-snapshot-export/v1");
    assert!(bundle["proof"]["manifest_hash"].is_string());
    assert!(bundle["proof"]["freeze_event"]["hash"].is_string());
    assert!(bundle["proof"]["chain_head"]["hash"].is_string());

    // 重新导入核对通过。
    let check = e.import_check(&bundle).unwrap();
    assert!(check.ok, "自洽导入应通过: {:?}", check.detail);
    assert!(check.manifest_hash_matches);
    assert!(check.freeze_receipt_matches);

    // 篡改 manifest 后导入核对失败。
    let mut tampered = bundle.clone();
    tampered["manifest"]["entries"][0]["name"] = serde_json::json!("hacked.txt");
    let bad = e.import_check(&tampered).unwrap();
    assert!(!bad.ok);
    assert!(!bad.manifest_hash_matches);

    // 伪造 manifest_hash 也无法通过（以重算为准）。
    let mut forged = bundle.clone();
    forged["proof"]["manifest_hash"] = serde_json::json!("0".repeat(64));
    let bad2 = e.import_check(&forged).unwrap();
    assert!(!bad2.ok);

    // 导入核对不写入任何块/快照（库内快照数不变）。
    assert_eq!(e.list_snapshots(false).unwrap().len(), 1);
}

#[test]
fn export_roundtrip_into_second_instance() {
    let dir1 = common::temp_dir("export-a");
    let dir2 = common::temp_dir("export-b");
    let e1 = Engine::open(&dir1).unwrap();
    let snap = common::freeze_blob(&e1, "v", None, &[("f", b"portable manifest")]);
    let bundle = e1.export_snapshot(&snap.id).unwrap();

    // 另一个空实例也能独立核对导出包自洽性（不依赖本地块）。
    let e2 = Engine::open(&dir2).unwrap();
    let check = e2.import_check(&bundle).unwrap();
    assert!(check.ok, "{:?}", check.detail);
}
