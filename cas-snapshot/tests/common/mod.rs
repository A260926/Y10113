#![allow(dead_code)]
use std::path::PathBuf;
use std::sync::atomic::{AtomicI64, Ordering};

use cas_snapshot::engine::Engine;

pub fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "cas-test-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// 可预测、单调推进的虚拟时钟。
pub fn clocked_engine(dir: &std::path::Path) -> (Engine, std::sync::Arc<AtomicI64>) {
    let counter = std::sync::Arc::new(AtomicI64::new(1_000_000));
    let c2 = counter.clone();
    let engine = Engine::open_with(
        dir,
        Box::new(cas_snapshot::engine::NoCrash),
        Some(Box::new(move || c2.fetch_add(1, Ordering::SeqCst))),
    )
    .unwrap();
    (engine, counter)
}

pub fn engine(dir: &std::path::Path) -> Engine {
    Engine::open(dir).unwrap()
}

/// 提交一份完整快照（自动按 64KiB 分块）。
pub fn freeze_blob(
    e: &Engine,
    label: &str,
    parent: Option<&str>,
    entries: &[(&str, &[u8])],
) -> cas_snapshot::engine::SnapshotInfo {
    let s = e.create_session(label, Some(600_000)).unwrap();
    for (idx, (_, data)) in entries.iter().enumerate() {
        for (i, chunk) in data.chunks(cas_snapshot::engine::CHUNK_SIZE).enumerate() {
            e.submit_block(
                &s.id,
                s.fence,
                Some(&format!("{label}-{idx}-{i}")),
                chunk,
            )
            .unwrap();
        }
    }
    let owned: Vec<(String, Vec<u8>)> = entries
        .iter()
        .map(|(n, d)| (n.to_string(), d.to_vec()))
        .collect();
    let built = e.assemble_entries(&s.id, &owned).unwrap();
    e.freeze(&s.id, s.fence, label, parent, built).unwrap()
}

pub fn block_count(e: &Engine) -> usize {
    e.list_blocks().unwrap().len()
}

pub fn physical_block_files(dir: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let root = dir.join("blocks");
    if let Ok(rd) = std::fs::read_dir(&root) {
        for shard in rd.flatten() {
            if let Ok(files) = std::fs::read_dir(shard.path()) {
                for f in files.flatten() {
                    out.push(f.path());
                }
            }
        }
    }
    out
}
