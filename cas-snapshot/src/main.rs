//! 内容寻址快照库可执行入口。
//!
//! 正常服务：
//!   cas-snapshot --listen 127.0.0.1:5243 [--data-dir DIR]
//!
//! 崩溃测试（测试程序使用，执行到指定阶段立即以退出码 7 终止）：
//!   cas-snapshot --data-dir DIR --crash-point freeze.after_intent|freeze.after_refs|...
//!                crash-freeze   --session S --token T --label L [--parent P]
//!                                --entry NAME FILE [--entry NAME FILE ...]
//!   cas-snapshot --data-dir DIR resume

use std::process::exit;
use std::sync::Arc;

use cas_snapshot::engine::{CrashSink, Engine, DEFAULT_LEASE_MS};

struct ExitOnPoint(&'static str);
impl CrashSink for ExitOnPoint {
    fn hit(&self, point: &str) {
        if point == self.0 {
            eprintln!("[crash-inject] 到达 {point}，立即退出（模拟进程崩溃）");
            exit(7);
        }
    }
}

struct Args {
    listen: String,
    data_dir: String,
    crash_point: Option<String>,
    seed: bool,
}

fn parse_args(argv: Vec<String>) -> Args {
    let mut listen = "127.0.0.1:5243".to_string();
    let mut data_dir = std::env::var("CAS_DATA_DIR").unwrap_or_else(|_| "cas-data".to_string());
    let mut crash_point = None;
    let mut seed = true;
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--listen" => {
                listen = argv[i + 1].clone();
                i += 2;
            }
            "--data-dir" => {
                data_dir = argv[i + 1].clone();
                i += 2;
            }
            "--crash-point" => {
                crash_point = Some(argv[i + 1].clone());
                i += 2;
            }
            "--no-seed" => {
                seed = false;
                i += 1;
            }
            _ => i += 1,
        }
    }
    Args {
        listen,
        data_dir,
        crash_point,
        seed,
    }
}

fn open_engine(args: &Args) -> Engine {
    let sink: Box<dyn CrashSink> = match &args.crash_point {
        Some(p) => {
            let leaked: &'static str = Box::leak(p.clone().into_boxed_str());
            Box::new(ExitOnPoint(leaked))
        }
        None => Box::new(cas_snapshot::engine::NoCrash),
    };
    Engine::open_with(&args.data_dir, sink, None).expect("打开引擎失败（恢复阶段）")
}

fn main() {
    let argv: Vec<String> = std::env::args().collect();
    let sub = argv
        .iter()
        .position(|a| a == "crash-freeze" || a == "resume" || a == "gc-now" || a == "verify-now")
        .map(|idx| argv[idx].clone());
    let global: Vec<String> = if let Some(idx) =
        argv.iter().position(|a| a == "crash-freeze" || a == "resume" || a == "gc-now" || a == "verify-now")
    {
        argv[..idx].to_vec()
    } else {
        argv.clone()
    };
    let args = parse_args(global);

    match sub.as_deref() {
        Some("resume") => {
            // 仅打开引擎即触发恢复，随后输出状态并退出。
            let engine = open_engine(&args);
            let snaps = engine.list_snapshots(true).unwrap();
            println!(
                "resume ok: {} 个快照（含墓碑）",
                snaps.len()
            );
            for s in snaps {
                println!(
                    "  {} status={} seq={:?} entries={}",
                    s.id,
                    s.status,
                    s.publish_seq,
                    s.entries.len()
                );
            }
            let report = engine.verify().unwrap();
            println!("verify after resume: ok={}", report.ok);
            if !report.ok {
                for it in report.items.iter().filter(|i| !i.ok) {
                    println!("  bad {} :: {}", it.hash, it.detail);
                }
                exit(2);
            }
        }
        Some("crash-freeze") => run_crash_freeze(&args, &argv),
        Some("gc-now") => {
            let engine = open_engine(&args);
            let report = engine.gc(None).unwrap();
            println!(
                "gc {}: deleted={} spared={}",
                report.gc_id,
                report.deleted.len(),
                report.spared_by_refs.len()
            );
        }
        Some("verify-now") => {
            let engine = open_engine(&args);
            let report = engine.verify().unwrap();
            println!("verify ok={} receipt={:?}", report.ok, report.receipt_hash);
            if !report.ok {
                exit(2);
            }
        }
        _ => {
            let engine = open_engine(&args);
            if args.seed {
                cas_snapshot::seed::seed_if_empty(&engine)
                    .expect("预置数据失败");
            }
            let engine = Arc::new(engine);
            cas_snapshot::web::serve(engine, &args.listen).expect("HTTP 服务失败");
        }
    }
}

/// 解析 crash-freeze 子命令参数：
/// --session ID --token N --label L [--parent ID] --entry NAME FILE ...
fn run_crash_freeze(args: &Args, argv: &[String]) {
    let mut session = String::new();
    let mut token: i64 = 0;
    let mut label = "crash-snapshot".to_string();
    let mut parent: Option<String> = None;
    let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
    let start = argv
        .iter()
        .position(|a| a == "crash-freeze")
        .map(|i| i + 1)
        .unwrap_or(argv.len());
    let mut i = start;
    while i < argv.len() {
        match argv[i].as_str() {
            "--session" => {
                session = argv[i + 1].clone();
                i += 2;
            }
            "--token" => {
                token = argv[i + 1].parse().unwrap();
                i += 2;
            }
            "--label" => {
                label = argv[i + 1].clone();
                i += 2;
            }
            "--parent" => {
                parent = Some(argv[i + 1].clone());
                i += 2;
            }
            "--entry" => {
                let name = argv[i + 1].clone();
                let file = argv[i + 2].clone();
                let data = std::fs::read(&file).unwrap_or_else(|e| panic!("读 {file}: {e}"));
                entries.push((name, data));
                i += 3;
            }
            other => panic!("未知 crash-freeze 参数: {other}"),
        }
    }
    let _ = DEFAULT_LEASE_MS;
    let engine = open_engine(args);
    let built = engine
        .assemble_entries(&session, &entries)
        .expect("组装条目失败（块收据不足）");
    let snap = engine
        .freeze(&session, token, &label, parent.as_deref(), built)
        .expect("冻结失败");
    println!("frozen {}", snap.id);
}
