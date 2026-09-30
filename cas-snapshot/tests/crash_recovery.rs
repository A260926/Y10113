#[path = "common/mod.rs"]
mod common;

use std::path::PathBuf;
use std::process::Command;

use cas_snapshot::engine::Engine;

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_cas-snapshot"))
}

struct Prepared {
    dir: PathBuf,
    session_id: String,
    token: i64,
    data: Vec<u8>,
}

fn prepare_session(tag: &str) -> Prepared {
    let dir = common::temp_dir(tag);
    let e = Engine::open(&dir).unwrap();
    let s = e.create_session("crash-me", Some(600_000)).unwrap();
    let data = b"crash recovery payload bytes".to_vec();
    for (i, chunk) in data.chunks(cas_snapshot::engine::CHUNK_SIZE).enumerate() {
        e.submit_block(&s.id, s.fence, Some(&format!("c{i}")), chunk)
            .unwrap();
    }
    let f = dir.join("entry.bin");
    std::fs::write(&f, &data).unwrap();
    drop(e);
    Prepared {
        dir,
        session_id: s.id,
        token: s.fence,
        data,
    }
}

fn run_crash_freeze(p: &Prepared, point: &str) {
    let f = p.dir.join("entry.bin");
    let output = Command::new(bin())
        .arg("--data-dir").arg(&p.dir)
        .arg("--crash-point").arg(point)
        .arg("crash-freeze")
        .arg("--session").arg(&p.session_id)
        .arg("--token").arg(p.token.to_string())
        .arg("--label").arg("crash-snap")
        .arg("--entry").arg("entry.bin").arg(&f)
        .output()
        .expect("运行崩溃子进程失败");
    assert_eq!(
        output.status.code(),
        Some(7),
        "应在 {point} 以 7 退出: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn run_resume(dir: &PathBuf) -> (bool, String) {
    let output = Command::new(bin())
        .arg("--data-dir").arg(dir)
        .arg("resume")
        .output()
        .expect("resume 启动失败");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    (output.status.success(), stdout)
}

fn snapshot_count(dir: &PathBuf) -> usize {
    Engine::open(dir).unwrap().list_snapshots(false).unwrap().len()
}

fn scalar(dir: &PathBuf, sql: &str) -> i64 {
    let conn = rusqlite::Connection::open(dir.join("meta.db")).unwrap();
    conn.query_row(sql, [], |r| r.get(0)).unwrap()
}

#[test]
fn crash_after_intent_rolls_back_no_visible_snapshot() {
    let p = prepare_session("crash-intent");
    run_crash_freeze(&p, "freeze.after_intent");

    let (ok, stdout) = run_resume(&p.dir);
    assert!(ok, "{stdout}");
    assert!(stdout.contains("verify after resume: ok=true"));
    assert_eq!(snapshot_count(&p.dir), 0, "intent 阶段崩溃应回滚");
    assert!(scalar(
        &p.dir,
        "SELECT COUNT(*) FROM events WHERE kind='freeze.rolled_back'"
    ) >= 1);
}

#[test]
fn crash_after_refs_resumes_and_publishes_atomically() {
    let p = prepare_session("crash-refs");
    run_crash_freeze(&p, "freeze.after_refs");

    let (ok, stdout) = run_resume(&p.dir);
    assert!(ok, "{stdout}");
    assert!(stdout.contains("verify after resume: ok=true"), "{stdout}");
    assert_eq!(snapshot_count(&p.dir), 1, "refs 阶段崩溃应继续提交发布");
    let e = Engine::open(&p.dir).unwrap();
    assert!(e.list_events(200).unwrap().iter().any(|x| x.kind == "freeze.resumed"));
    let snap = e.list_snapshots(false).unwrap().remove(0);
    assert_eq!(snap.label, "crash-snap");
    assert_eq!(snap.entries[0].chunks[0].size as usize, p.data.len());
}

#[test]
fn crash_after_publish_reopens_cleanly() {
    let p = prepare_session("crash-publish");
    run_crash_freeze(&p, "freeze.after_publish");

    let (ok, stdout) = run_resume(&p.dir);
    assert!(ok, "{stdout}");
    assert!(stdout.contains("verify after resume: ok=true"), "{stdout}");
    assert_eq!(snapshot_count(&p.dir), 1);
}

#[test]
fn crash_after_refs_with_missing_block_rolls_back_instead_of_publishing() {
    let p = prepare_session("crash-refs-missing");
    run_crash_freeze(&p, "freeze.after_refs");

    // 不触发恢复地删除块文件（refs 已加入、块丢失）。
    let hash = {
        let conn = rusqlite::Connection::open(p.dir.join("meta.db")).unwrap();
        conn.query_row("SELECT hash FROM blocks LIMIT 1", [], |r| r.get::<_, String>(0))
            .unwrap()
    };
    std::fs::remove_file(
        p.dir.join("blocks").join(&hash[0..2]).join(&hash[2..]),
    )
    .unwrap();

    let (ok, stdout) = run_resume(&p.dir);
    // 恢复动作本身成功；整库验证可能因“会话仍持有的块缺文件”而为 false，
    // 但 resume 程序退出码体现的是整库验证——这里要求“没有可见快照引用缺块”，
    // 因此检查快照数与引用，而不是整库 verify。
    let _ = ok;
    assert!(stdout.contains("resume ok"), "{stdout}");
    assert_eq!(snapshot_count(&p.dir), 0, "块缺失时绝不能发布快照");
    assert_eq!(
        scalar(&p.dir, "SELECT COUNT(*) FROM block_refs WHERE ref_id LIKE 'snap:%'"),
        0,
        "回滚后不应残留快照引用"
    );
    assert!(scalar(
        &p.dir,
        "SELECT COUNT(*) FROM events WHERE kind='freeze.rolled_back'"
    ) >= 1);

    // 会话重新上传同一字节（相同摘要）后，可再次冻结并验证通过。
    let e = Engine::open(&p.dir).unwrap();
    let s = e.get_session(&p.session_id).unwrap();
    assert!(!s.finalized, "回滚后会话不应被标记冻结");
    e.repair_block(&hash, &p.data).unwrap();
    let entries = vec![("entry.bin".to_string(), p.data.clone())];
    let built = e.assemble_entries(&p.session_id, &entries).unwrap();
    let snap = e
        .freeze(&p.session_id, p.token, "recovered", None, built)
        .unwrap();
    assert!(e.verify().unwrap().ok);
    assert_eq!(e.get_snapshot(&snap.id).unwrap().status, "published");
}
