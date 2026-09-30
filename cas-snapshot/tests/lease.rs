#[path = "common/mod.rs"]
mod common;

use cas_snapshot::engine::Engine;

#[test]
fn stale_fencing_token_is_rejected() {
    let dir = common::temp_dir("fence");
    let e = Engine::open(&dir).unwrap();
    let s = e.create_session("s", Some(600_000)).unwrap();
    let old = s.fence;

    // 续租换发新 token。
    let new = e.renew_session(&s.id, old, Some(600_000)).unwrap();
    assert_ne!(old, new);

    // 旧 token 提交被拒（旧会话晚提交）。
    let err = e.submit_block(&s.id, old, Some("late"), b"late block").unwrap_err();
    assert!(err.to_string().contains("fencing token 过期"), "{}", err);

    // 旧 token 续租也被拒。
    let err2 = e.renew_session(&s.id, old, Some(600_000)).unwrap_err();
    assert!(err2.to_string().contains("fencing token"));

    // 新 token 正常提交。
    e.submit_block(&s.id, new, Some("ok"), b"fresh").unwrap();
}

#[test]
fn expired_lease_rejects_submit_and_freeze() {
    let dir = common::temp_dir("lease");
    let (e, _clock) = common::clocked_engine(&dir);
    let s = e.create_session("s", Some(10_000)).unwrap();
    let token = s.fence;
    e.submit_block(&s.id, token, Some("b"), b"b").unwrap();
    e.expire_session_for_demo(&s.id).unwrap();

    let err = e.submit_block(&s.id, token, Some("late"), b"late").unwrap_err();
    assert!(err.to_string().contains("租约"), "{}", err);

    let entries = e.assemble_entries(&s.id, &[("f".to_string(), b"b".to_vec())]).unwrap();
    let err2 = e.freeze(&s.id, token, "f", None, entries).unwrap_err();
    assert!(err2.to_string().contains("租约"), "{}", err2);
}

#[test]
fn finalized_session_cannot_submit_again() {
    let dir = common::temp_dir("finalized");
    let e = Engine::open(&dir).unwrap();
    let snap = common::freeze_blob(&e, "v", None, &[("f", b"data")]);
    // freeze_blob 内部会话已 finalized；再提交即冲突。
    let s = e.list_sessions().unwrap().into_iter().next().unwrap();
    let err = e
        .submit_block(&s.id, s.fence, Some("x"), b"x")
        .unwrap_err();
    assert!(err.to_string().contains("已冻结"), "{}", err);
    let _ = snap;
}
