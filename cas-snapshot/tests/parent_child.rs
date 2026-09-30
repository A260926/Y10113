#[path = "common/mod.rs"]
mod common;

use cas_snapshot::engine::Engine;

#[test]
fn derive_reuses_unchanged_entries_and_delete_parent_is_safe() {
    let dir = common::temp_dir("derive");
    let e = Engine::open(&dir).unwrap();

    let parent = common::freeze_blob(
        &e,
        "parent",
        None,
        &[
            ("a.txt", b"alpha content"),
            ("common.txt", b"shared across generations"),
        ],
    );

    // 子会话：只上传一个修改块；common.txt 不在子会话收据中，但派生会复用父条目。
    let cs = e.create_session("child-session", Some(600_000)).unwrap();
    let new_a = b"alpha content changed in child";
    e.submit_block(&cs.id, cs.fence, Some("new-a"), new_a).unwrap();
    let updates = vec![("a.txt".to_string(), new_a.to_vec())];

    let child = e
        .derive(&cs.id, cs.fence, "child", &parent.id, updates, vec![])
        .unwrap();
    assert_eq!(child.parent_id.as_deref(), Some(parent.id.as_str()));

    // 子快照包含两个条目；common.txt 的块哈希与父一致。
    let p = e.get_snapshot(&parent.id).unwrap();
    let c = e.get_snapshot(&child.id).unwrap();
    let p_common = p
        .entries
        .iter()
        .find(|x| x.name == "common.txt")
        .unwrap()
        .chunks[0]
        .hash
        .clone();
    let c_common = c
        .entries
        .iter()
        .find(|x| x.name == "common.txt")
        .unwrap()
        .chunks[0]
        .hash
        .clone();
    assert_eq!(p_common, c_common, "未变条目复用同一内容块");
    let c_a = c
        .entries
        .iter()
        .find(|x| x.name == "a.txt")
        .unwrap()
        .chunks[0]
        .hash
        .clone();
    let p_a = p
        .entries
        .iter()
        .find(|x| x.name == "a.txt")
        .unwrap()
        .chunks[0]
        .hash
        .clone();
    assert_ne!(p_a, c_a, "修改条目使用新块");

    // 删除父快照：只减少父自己的引用；子仍完整、common 块仍存活。
    e.delete_snapshot(&parent.id).unwrap();
    e.gc(None).unwrap();
    let report = e.verify().unwrap();
    assert!(report.ok, "删除父快照后验证应通过: {:?}", report.affected_snapshots);
    let blocks: std::collections::HashSet<_> =
        e.list_blocks().unwrap().into_iter().map(|b| b.hash).collect();
    assert!(blocks.contains(&c_common), "复用块仍被子快照引用");
    assert!(blocks.contains(&c_a), "子快照新块仍在");
    assert!(!blocks.contains(&p_a), "父快照私有旧块应被回收");

    let child_reloaded = e.get_snapshot(&child.id).unwrap();
    assert_eq!(child_reloaded.status, "published");
    assert_eq!(child_reloaded.entries.len(), 2);
}

#[test]
fn derive_can_remove_entries_without_touching_parent() {
    let dir = common::temp_dir("derive-remove");
    let e = Engine::open(&dir).unwrap();
    let parent = common::freeze_blob(
        &e,
        "p",
        None,
        &[("keep.txt", b"keep me"), ("drop.txt", b"drop me")],
    );
    let cs = e.create_session("c", Some(600_000)).unwrap();
    // 派生仅删除，不上传新块也可以（复用全部保留条目）。
    let child = e
        .derive(&cs.id, cs.fence, "c", &parent.id, vec![], vec!["drop.txt".to_string()])
        .unwrap();
    let names: Vec<_> = child.entries.iter().map(|x| x.name.clone()).collect();
    assert_eq!(names, vec!["keep.txt".to_string()]);

    // 父快照原封不动。
    let p = e.get_snapshot(&parent.id).unwrap();
    let mut pnames: Vec<_> = p.entries.iter().map(|x| x.name.clone()).collect();
    pnames.sort();
    assert_eq!(pnames, vec!["drop.txt".to_string(), "keep.txt".to_string()]);
}

#[test]
fn cannot_delete_then_derive_from_parent() {
    let dir = common::temp_dir("derive-deleted");
    let e = Engine::open(&dir).unwrap();
    let p = common::freeze_blob(&e, "p", None, &[("x", b"x")]);
    e.delete_snapshot(&p.id).unwrap();
    let cs = e.create_session("c", Some(600_000)).unwrap();
    let err = e
        .derive(&cs.id, cs.fence, "c", &p.id, vec![], vec![])
        .unwrap_err();
    assert!(err.to_string().contains("冲突"));
}
