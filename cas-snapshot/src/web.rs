//! 内嵌 HTTP 服务与服务器渲染页面。所有状态来自 [`Engine`]，
//! 页面展示 manifest、块共享、引用来源、验证结果、GC/恢复事件。

use std::io::Read;
use std::sync::Arc;

use serde_json::json;
use tiny_http::{Header, Method, Request, Response, Server};

use crate::engine::{Engine, Error};

pub type Resp = Response<Box<dyn Read + Send>>;

pub struct AppState {
    pub engine: Arc<Engine>,
}

pub fn serve(engine: Arc<Engine>, listen: &str) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let server = Server::http(listen)?;
    let state = Arc::new(AppState { engine });
    eprintln!("内容寻址快照库已监听 http://{listen}");
    for request in server.incoming_requests() {
        let state = state.clone();
        // 每个连接一个线程；引擎内部用 Mutex 串行化写事务。
        std::thread::spawn(move || {
            if let Err(e) = handle(state, request) {
                eprintln!("请求处理错误: {e}");
            }
        });
    }
    Ok(())
}

fn handle(
    state: Arc<AppState>,
    mut request: Request,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = request.url().to_string();
    let (path, query) = url.split_once('?').unwrap_or((&url, ""));
    let method = request.method().clone();

    let mut body_bytes = Vec::new();
    if method == Method::Post || method == Method::Put {
        let len = request
            .headers()
            .iter()
            .find(|h| h.field.equiv("Content-Length"))
            .and_then(|h| h.value.as_str().parse::<usize>().ok())
            .unwrap_or(0);
        let reader = request.as_reader();
        reader.take(len.min(16 * 1024 * 1024) as u64).read_to_end(&mut body_bytes)?;
    }

    let response = match route(&state, method, path, query, &body_bytes) {
        Ok(resp) => resp,
        Err(Error::NotFound(msg)) => plain(404, format!("未找到: {msg}")),
        Err(Error::Conflict(msg)) => plain(409, format!("冲突: {msg}")),
        Err(Error::BadInput(msg)) => plain(400, format!("无效输入: {msg}")),
        Err(Error::Corrupt(msg)) => plain(500, format!("完整性错误: {msg}")),
        Err(e) => plain(500, format!("服务器错误: {e}")),
    };
    request.respond(response)?;
    Ok(())
}

fn plain(status: u16, body: String) -> Resp {
    Response::from_string(body).with_status_code(status).boxed()
}

fn html(status: u16, body: String) -> Resp {
    let header = Header::from_bytes(&b"Content-Type"[..], &b"text/html; charset=utf-8"[..]).unwrap();
    Response::from_string(body)
        .with_status_code(status)
        .with_header(header)
        .boxed()
}

fn json_response(status: u16, value: serde_json::Value) -> Resp {
    let header =
        Header::from_bytes(&b"Content-Type"[..], &b"application/json; charset=utf-8"[..]).unwrap();
    Response::from_data(serde_json::to_vec_pretty(&value).unwrap())
        .with_status_code(status)
        .with_header(header)
        .boxed()
}

fn redirect(to: &str) -> Resp {
    let header = Header::from_bytes(&b"Location"[..], to.as_bytes()).unwrap();
    Response::empty(303).with_header(header).boxed()
}
// ---------------------------------------------------------------------------
// 表单 / 查询参数解析
// ---------------------------------------------------------------------------

fn parse_form(body: &[u8]) -> Vec<(String, String)> {
    let text = String::from_utf8_lossy(body);
    parse_kv(&text)
}

fn parse_query(q: &str) -> Vec<(String, String)> {
    parse_kv(q)
}

fn parse_kv(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for pair in text.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
        out.push((percent_decode(k), percent_decode(v)));
    }
    out
}

fn get<'a>(pairs: &'a [(String, String)], key: &str) -> Option<&'a str> {
    pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v.as_str())
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                let h = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or("");
                if let Ok(n) = u8::from_str_radix(h, 16) {
                    out.push(n);
                    i += 3;
                } else {
                    out.push(bytes[i]);
                    i += 1;
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn short(hash: &str) -> String {
    if hash.len() > 16 {
        format!("{}…{}", &hash[..10], &hash[hash.len() - 6..])
} else {
        hash.to_string()
    }
}

fn route(
    state: &AppState,
    method: Method,
    path: &str,
    query: &str,
    body: &[u8],
) -> Result<Resp, Error> {
    let form = parse_form(body);
    let q = parse_query(query);
    let e = &state.engine;

    // ---------------- POST 动作（处理后 PRG 重定向回页面） ----------------
    if method == Method::Post {
        return Ok(match path {
            "/sessions" => {
                let label = get(&form, "label").unwrap_or("未命名会话").to_string();
                let lease: i64 = get(&form, "lease_ms")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(crate::engine::DEFAULT_LEASE_MS);
                let s = e.create_session(&label, Some(lease))?;
                redirect(&format!("/?tab=sessions&new={}", s.id))
            }
            "/sessions/renew" => {
                let sid = get(&form, "session_id").unwrap_or_default();
                let token: i64 = get(&form, "token").and_then(|s| s.parse().ok()).unwrap_or(0);
                let lease: i64 = get(&form, "lease_ms")
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(crate::engine::DEFAULT_LEASE_MS);
                let new_token = e.renew_session(sid, token, Some(lease))?;
                redirect(&format!("/?tab=sessions&sid={sid}&new_token={new_token}"))
            }
            "/sessions/expire" => {
                let sid = get(&form, "session_id").unwrap_or_default();
                e.expire_session_for_demo(sid)?;
                redirect("/?tab=sessions&expired=1")
            }
            "/sessions/submit" => {
                let sid = get(&form, "session_id").unwrap_or_default();
                let token: i64 = get(&form, "token").and_then(|s| s.parse().ok()).unwrap_or(0);
                let idem = get(&form, "idem_key").map(str::to_string);
                let idem = idem.filter(|s| !s.is_empty());
                let data = get(&form, "data")
                    .ok_or_else(|| Error::BadInput("请填写块内容 data".into()))?
                    .as_bytes()
                    .to_vec();
                let r = e.submit_block(sid, token, idem.as_deref(), &data)?;
                redirect(&format!(
                    "/?tab=sessions&sid={sid}&receipt={}",
                    r.block_hash
                ))
            }
            "/sessions/freeze" => {
                let sid = get(&form, "session_id").unwrap_or_default();
                let token: i64 = get(&form, "token").and_then(|s| s.parse().ok()).unwrap_or(0);
                let label = get(&form, "label").unwrap_or("未命名快照").to_string();
                let parent = get(&form, "parent_id").map(str::to_string).filter(|s| !s.is_empty());
                let entries = collect_entries(&form);
                let built = e.assemble_entries(
                    sid,
                    &entries
                        .iter()
                        .map(|(n, d)| (n.clone(), d.clone()))
                        .collect::<Vec<_>>(),
                )?;
                let snap = e.freeze(sid, token, &label, parent.as_deref(), built)?;
                redirect(&format!("/snapshot/{}", snap.id))
            }
            "/snapshots/derive" => {
                let sid = get(&form, "session_id").unwrap_or_default();
                let token: i64 = get(&form, "token").and_then(|s| s.parse().ok()).unwrap_or(0);
                let label = get(&form, "label").unwrap_or("派生快照").to_string();
                let parent = get(&form, "parent_id").unwrap_or_default();
                let updates = collect_entries(&form);
                let remove: Vec<String> = get(&form, "remove")
                    .unwrap_or("")
                    .lines()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                let snap = e.derive(sid, token, &label, parent, updates, remove)?;
                redirect(&format!("/snapshot/{}", snap.id))
            }
            "/snapshots/delete" => {
                let id = get(&form, "snapshot_id").unwrap_or_default();
                e.delete_snapshot(id)?;
                redirect("/?tab=snapshots&deleted=1")
            }
            "/verify" => {
                e.verify()?;
                redirect("/?tab=verify&ran=1")
            }
            "/repair" => {
                let hash = get(&form, "hash").unwrap_or_default();
                let data = get(&form, "data").unwrap_or("").as_bytes().to_vec();
                e.repair_block(hash, &data)?;
                redirect("/?tab=verify&repaired=1")
            }
            "/gc" => {
                e.gc(None)?;
                redirect("/?tab=gc")
            }
            "/import" => {
                let raw = get(&form, "bundle").unwrap_or("");
                let value: serde_json::Value = serde_json::from_str(raw)
                    .map_err(|err| Error::BadInput(format!("导出包不是合法 JSON: {err}")))?;
                let result = e.import_check(&value)?;
                redirect(&format!(
                    "/?tab=export&import_ok={}",
                    if result.ok { 1 } else { 0 }
                ))
            }
            _ => return Ok(plain(404, format!("无此 POST 路由: {path}"))),
        });
    }

    // ---------------- GET 页面 / JSON ----------------
    if path == "/" {
        let tab = get(&q, "tab").unwrap_or("snapshots");
        return Ok(html(200, render_dashboard(state, tab, &q)));
    }
    if let Some(id) = path.strip_prefix("/snapshot/") {
        let s = e.get_snapshot(id)?;
        return Ok(html(200, render_snapshot_page(state, &s)));
    }
    if path == "/api/snapshots" {
        return Ok(json_response(200, json!(e.list_snapshots(false)?)));
    }
    if let Some(sid) = path.strip_prefix("/api/session/") {
        return Ok(json_response(200, json!(e.get_session(sid)?)));
    }
    if path == "/api/blocks" {
        return Ok(json_response(200, json!(e.list_blocks()?)));
    }
    if path == "/api/verify" {
        return Ok(json_response(200, json!(e.verify()?)));
    }
    if path == "/api/events" {
        return Ok(json_response(200, json!(e.list_events(200)?)));
    }
    if let Some(id) = path.strip_prefix("/api/export/") {
        return Ok(json_response(200, e.export_snapshot(id)?));
    }
    Ok(plain(404, format!("无此路由: {path}")))
}

/// 从表单中收集 entryN / dataN 成对字段。
fn collect_entries(form: &[(String, String)]) -> Vec<(String, Vec<u8>)> {
    let mut map: std::collections::BTreeMap<usize, (String, String)> =
        std::collections::BTreeMap::new();
    for (k, v) in form {
        if let Some(rest) = k.strip_prefix("entry") {
            if let Ok(idx) = rest.parse::<usize>() {
                map.entry(idx).or_insert((String::new(), String::new())).0 = v.clone();
            }
        } else if let Some(rest) = k.strip_prefix("data") {
            if let Ok(idx) = rest.parse::<usize>() {
                map.entry(idx).or_insert((String::new(), String::new())).1 = v.clone();
            }
        }
    }
    map.into_values()
        .filter(|(n, d)| !n.is_empty() && !d.is_empty())
        .map(|(n, d)| (n, d.into_bytes()))
        .collect()
}
// ---------------------------------------------------------------------------
// 页面渲染
// ---------------------------------------------------------------------------


fn page(title: &str, active: &str, body: String) -> String {
    let tabs = [
        ("snapshots", "快照与 Manifest"),
        ("blocks", "块 / 共享 / 引用来源"),
        ("sessions", "上传会话 / 租约"),
        ("verify", "验证与修复"),
        ("gc", "垃圾回收"),
        ("export", "导出 / 导入核对"),
        ("events", "事件收据"),
    ];
    let nav: String = tabs
        .iter()
        .map(|(id, label)| {
            let cls = if *id == active { "tab active" } else { "tab" };
            format!(r#"<a class="{cls}" href="/?tab={id}">{label}</a>"#)
        })
        .collect();
    format!(
        r#"<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
:root {{ --bg:#0f1420; --panel:#161d2e; --line:#26314a; --fg:#e6ebf5;
       --muted:#8b97b3; --accent:#5b8cff; --good:#3ecf8e; --bad:#ff6b6b; --warn:#f6b26b; }}
* {{ box-sizing:border-box; }}
body {{ margin:0; font:14px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
       background:var(--bg); color:var(--fg); }}
header {{ padding:18px 24px; border-bottom:1px solid var(--line);
          background:linear-gradient(120deg,#14213d,#101726); }}
h1 {{ margin:0 0 4px; font-size:20px; }}
.sub {{ color:var(--muted); font-size:13px; }}
nav {{ display:flex; flex-wrap:wrap; gap:4px; padding:10px 24px;
       border-bottom:1px solid var(--line); background:var(--panel); position:sticky; top:0; }}
.tab {{ padding:6px 12px; border-radius:8px; color:var(--muted); text-decoration:none; }}
.tab:hover {{ color:var(--fg); background:#1d2740; }}
.tab.active {{ color:#fff; background:var(--accent); }}
main {{ padding:20px 24px; max-width:1180px; margin:0 auto; }}
.grid {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
.card {{ background:var(--panel); border:1px solid var(--line); border-radius:12px;
         padding:16px; margin-bottom:16px; }}
.card h2 {{ margin:0 0 10px; font-size:15px; }}
.card h3 {{ margin:14px 0 6px; font-size:13px; color:var(--muted); }}
table {{ border-collapse:collapse; width:100%; font-size:12.5px; }}
th,td {{ text-align:left; padding:6px 8px; border-bottom:1px solid var(--line);
         vertical-align:top; word-break:break-all; }}
th {{ color:var(--muted); font-weight:600; }}
code,pre {{ font-family:ui-monospace,SFMono-Regular,Menlo,monospace; }}
pre {{ background:#0b1020; border:1px solid var(--line); border-radius:8px;
       padding:10px; overflow:auto; font-size:12px; max-height:340px; }}
.hash {{ color:#9fc0ff; font-size:12px; }}
.tag {{ display:inline-block; padding:1px 8px; border-radius:999px; font-size:11px; }}
.good {{ background:rgba(62,207,142,.15); color:var(--good); }}
.bad {{ background:rgba(255,107,107,.15); color:var(--bad); }}
.warn {{ background:rgba(246,178,107,.15); color:var(--warn); }}
.muted {{ color:var(--muted); }}
input,textarea,select {{ width:100%; background:#0b1020; border:1px solid var(--line);
       color:var(--fg); border-radius:8px; padding:8px; font:inherit; }}
textarea {{ min-height:70px; }}
label {{ display:block; font-size:12px; color:var(--muted); margin:8px 0 3px; }}
button {{ background:var(--accent); border:0; color:#fff; padding:8px 14px;
       border-radius:8px; font:inherit; cursor:pointer; margin-top:10px; }}
button.ghost {{ background:#26314a; }}
button.danger {{ background:#a33; }}
.row {{ display:flex; gap:10px; flex-wrap:wrap; }}
.row > div {{ flex:1; min-width:160px; }}
a {{ color:var(--accent); }}
.banner {{ padding:10px 14px; border-radius:8px; margin-bottom:14px; }}
.banner.good {{ background:rgba(62,207,142,.12); border:1px solid rgba(62,207,142,.4); }}
.banner.bad {{ background:rgba(255,107,107,.12); border:1px solid rgba(255,107,107,.4); }}
</style></head>
<body>
<header>
  <h1>内容寻址快照库</h1>
  <div class="sub">本地内容寻址 · 分块去重 · 引用计数 · 三阶段冻结/崩溃恢复 · 代次屏障 GC · 可验证 manifest</div>
</header>
<nav>{nav}</nav>
<main>
{body}
</main></body></html>"#
    )
}

fn banner(q: &[(String, String)], key: &str, good: &str, bad: Option<&str>) -> String {
    use crate::web::get;
    let Some(v) = get(q, key) else { return String::new() };
    if v == "1" {
        format!(r#"<div class="banner good">✅ {good}</div>"#)
    } else if let Some(b) = bad {
        format!(r#"<div class="banner bad">⛔ {b}</div>"#)
    } else {
        String::new()
    }
}

pub fn render_dashboard(state: &AppState, tab: &str, q: &[(String, String)]) -> String {
    let body = match tab {
        "blocks" => render_blocks(state, q),
        "sessions" => render_sessions(state, q),
        "verify" => render_verify(state, q),
        "gc" => render_gc(state, q),
        "export" => render_export(state, q),
        "events" => render_events(state, q),
        _ => render_snapshots(state, q),
    };
    let body = body.unwrap_or_else(|e| format!(r#"<div class="banner bad">渲染失败: {}</div>"#, escape(&e.to_string())));
    page("内容寻址快照库", tab, body)
}

fn render_snapshots(state: &AppState, q: &[(String, String)]) -> Result<String, crate::engine::Error> {
    let e = &state.engine;
    let snaps = e.list_snapshots(false)?;
    let all = e.list_snapshots(true)?;
    let sharing = e.block_sharing()?;
    let mut out = banner(q, "deleted", "快照已删除（仅移除自身引用，共享块仍受其他快照保护）", None);

    out.push_str(r#"<div class="card"><h2>已发布快照（manifest）</h2>"#);
    if snaps.is_empty() {
        out.push_str(r#"<p class="muted">暂无已发布快照。</p>"#);
    } else {
        out.push_str("<table><tr><th>快照</th><th>标签</th><th>父快照</th><th>代次</th><th>manifest 摘要</th><th>条目/块</th><th></th></tr>");
        for s in &snaps {
            let n_chunks: usize = s.entries.iter().map(|en| en.chunks.len()).sum();
            let parent = s.parent_id.as_deref().map(short_p).unwrap_or_default();
            out.push_str(&format!(
                r#"<tr><td><a href="/snapshot/{id}">{id}</a></td><td>{label}</td>
<td class="hash">{parent}</td><td>{seq}</td>
<td class="hash">{mh}</td><td>{ne} 条目 / {nc} 块</td>
<td><form method="post" action="/snapshots/delete" onsubmit="return confirm('删除快照 {id}？')">
<input type="hidden" name="snapshot_id" value="{id}">
<button class="danger">删除</button></form></td></tr>"#,
                id = escape(&s.id),
                label = escape(&s.label),
                parent = parent,
                seq = s.publish_seq.unwrap_or(0),
                mh = short(&s.manifest_hash),
                ne = s.entries.len(),
                nc = n_chunks,
            ));
        }
        out.push_str("</table>");
    }
    out.push_str("</div>");

    // 派生表单
    out.push_str(r#"<div class="card"><h2>派生快照（复用未变条目）</h2>
<p class="muted">先在“上传会话”页创建会话并提交新/修改条目使用的块；这里选择父快照，
填入新条目内容与要删除的条目名。未列出的条目直接复用父快照的块引用。</p>
<form method="post" action="/snapshots/derive">
<div class="row"><div><label>已有会话 ID</label><input name="session_id" placeholder="s_..."></div>
<div><label>fencing token</label><input name="token" placeholder="会话页显示的当前 token"></div></div>
<div class="row"><div><label>新快照标签</label><input name="label" value="派生快照"></div>
<div><label>父快照 ID</label><select name="parent_id">"#);
    for s in &snaps {
        out.push_str(&format!(r#"<option value="{}">{} · {}</option>"#, escape(&s.id), escape(&s.id), escape(&s.label)));
    }
    out.push_str(r#"</select></div></div>
<div class="row"><div><label>更新条目名 1</label><input name="entry1" placeholder="docs/readme.txt"></div>
<div><label>内容 1（不超过 64KiB 即单块）</label><textarea name="data1"></textarea></div></div>
<div class="row"><div><label>更新条目名 2（可选）</label><input name="entry2"></div>
<div><label>内容 2</label><textarea name="data2"></textarea></div></div>
<label>删除条目名（每行一个，按父快照中的名字）</label>
<textarea name="remove" placeholder="old/note.txt"></textarea>
<button>派生并冻结</button></form></div>"#);

    // 共享关系
    out.push_str(r#"<div class="card"><h2>块共享关系</h2><p class="muted">
    被多个快照引用的块即共享块——删除其中任一快照不会减少其他快照的可用性。</p>"#);
    let shared: Vec<_> = sharing.iter().filter(|(_, v)| v.len() > 1).collect();
    if shared.is_empty() {
        out.push_str(r#"<p class="muted">当前没有被多个快照共享的块。</p>"#);
    } else {
        out.push_str("<table><tr><th>块摘要</th><th>引用它的快照</th></tr>");
        for (h, refs) in shared {
            let refs: String = refs
                .iter()
                .map(|r| escape(r))
                .collect::<Vec<_>>()
                .join("<br>");
            out.push_str(&format!(r#"<tr><td class="hash">{}</td><td>{}</td></tr>"#, short(h), refs));
        }
        out.push_str("</table>");
    }
    // 已删除快照（墓碑）
    let deleted: Vec<_> = all.iter().filter(|s| s.status != "published").collect();
    if !deleted.is_empty() {
        out.push_str(r#"<h3>已删除快照（墓碑，引用已移除）</h3><table><tr><th>ID</th><th>标签</th><th>删除时间(ms)</th></tr>"#);
        for s in deleted {
            out.push_str(&format!(r#"<tr><td>{}</td><td>{}</td><td>{}</td></tr>"#,
                escape(&s.id), escape(&s.label), s.deleted_at.unwrap_or(0)));
        }
        out.push_str("</table>");
    }
    out.push_str("</div>");
    Ok(out)
}

fn short_p(s: &str) -> String {
    short(s)
}

fn render_blocks(state: &AppState, _q: &[(String, String)]) -> Result<String, crate::engine::Error> {
    let e = &state.engine;
    let blocks = e.list_blocks()?;
    let total: i64 = blocks.iter().map(|b| b.size).sum();
    let ref_total: usize = blocks.iter().map(|b| b.refs.len()).sum();
    let mut out = format!(
        r#"<div class="card"><h2>块存储（去重后物理占用）</h2>
<p class="muted">{} 个唯一块，合计 {} 字节；引用行共 {} 条（引用计数来源见每行）。
块文件位于应用数据目录 <code>blocks/&lt;ab&gt;/&lt;cdef…&gt;</code>。</p>"#,
        blocks.len(),
        total,
        ref_total
    );
    out.push_str(r#"<table><tr><th>摘要（=路径名）</th><th>字节</th><th>文件</th><th>引用计数</th><th>引用来源</th></tr>"#);
    for b in &blocks {
        let file_tag = if b.file_present {
            r#"<span class="tag good">存在</span>"#
        } else {
            r#"<span class="tag bad">缺失</span>"#
        };
        let refs: String = b
            .refs
            .iter()
            .map(|r| {
                let cls = if r.kind == "snapshot" { "good" } else { "warn" };
                format!(r#"<span class="tag {cls}">{}: {}</span>"#, r.kind, escape(&r.label))
            })
            .collect::<Vec<_>>()
            .join(" ");
        out.push_str(&format!(
            r#"<tr><td class="hash">{h}</td><td>{size}</td><td>{file}</td>
<td>{n}</td><td>{refs}</td></tr>"#,
            h = b.hash,
            size = b.size,
            file = file_tag,
            n = b.refs.len(),
            refs = refs
        ));
    }
    out.push_str("</table></div>");
    Ok(out)
}

fn render_sessions(state: &AppState, q: &[(String, String)]) -> Result<String, crate::engine::Error> {
    use crate::web::get;
    let e = &state.engine;
    let sessions = e.list_sessions()?;
    let focus = get(q, "sid").unwrap_or("");
    let new_id = get(q, "new").unwrap_or("");
    let new_token = get(q, "new_token").unwrap_or("");
    let mut out = String::new();
    if !new_id.is_empty() {
        out.push_str(&format!(r#"<div class="banner good">已创建会话 <code>{}</code>，请保存 fencing token。</div>"#, escape(new_id)));
    }
    if !new_token.is_empty() {
        out.push_str(&format!(r#"<div class="banner good">已续租并换发新 token：<code>{}</code>，旧 token 立即失效。</div>"#, escape(new_token)));
    }
    if get(q, "expired").is_some() {
        out.push_str(r#"<div class="banner bad">会话租约已被置为过期；再用旧 token 提交会被拒绝。</div>"#);
    }
    if let Some(r) = get(q, "receipt") {
        out.push_str(&format!(r#"<div class="banner good">收到块收据：<code>{}</code>（重复内容会标记 dedup，不重复占空间）。</div>"#, escape(&short(r))));
    }

    // 创建会话
    out.push_str(r#"<div class="card"><h2>创建上传会话</h2>
<form method="post" action="/sessions">
<div class="row"><div><label>标签</label><input name="label" placeholder="release-2026-09"></div>
<div><label>租约时长（毫秒）</label><input name="lease_ms" value="900000"></div></div>
<button>创建（返回 fencing token）</button></form></div>"#);

    // 会话列表 + 详情
    out.push_str(r#"<div class="card"><h2>会话与租约 / fencing token</h2>"#);
    if sessions.is_empty() {
        out.push_str(r#"<p class="muted">还没有会话。</p>"#);
    }
    for s in &sessions {
        let now = e.now_ms();
        let state_tag = if s.finalized {
            r#"<span class="tag good">已冻结</span>"#
        } else if now > s.expires {
            r#"<span class="tag bad">租约过期</span>"#
        } else {
            r#"<span class="tag warn">活跃</span>"#
        };
        let open = if s.id == focus { " open" } else { "" };
        out.push_str(&format!(
            r#"<details{open} style="margin:8px 0;border:1px solid var(--line);border-radius:8px;padding:8px 12px">
<summary><code>{id}</code> {tag} <span class="muted">{label}</span></summary>
<p class="muted">fencing token（当前唯一有效）：<code>{fence}</code>　
创建 {created}　到期 {expires}（当前 {now}）</p>
<h3>块收据（{n}）</h3><table><tr><th>#</th><th>块</th><th>字节</th><th>去重</th><th>幂等键</th></tr>"#,
            open = open, id = escape(&s.id), tag = state_tag, label = escape(&s.label),
            fence = s.fence, created = s.created, expires = s.expires, now = now, n = s.receipts.len()
        ));
        for r in &s.receipts {
            let tag = if r.dedup {
                r#"<span class="tag good">dedup</span>"#
            } else {
                r#"<span class="tag warn">new</span>"#
            };
            out.push_str(&format!(
                r#"<tr><td>{seq}</td><td class="hash">{h}</td><td>{size}</td><td>{tag}</td><td>{idem}</td></tr>"#,
                seq = r.seq,
                h = r.block_hash,
                size = r.size,
                tag = tag,
                idem = escape(r.idem_key.as_deref().unwrap_or("—"))
            ));
        }
        out.push_str("</table>");

        if !s.finalized {
            // 提交块
            out.push_str(r#"<h3>提交块（携带 token）</h3>
<form method="post" action="/sessions/submit">
<input type="hidden" name="session_id" value="#);
            out.push_str(&escape(&s.id));
            out.push_str(r#"">
<div class="row"><div><label>fencing token</label><input name="token" value="#);
            out.push_str(&s.fence.to_string());
            out.push_str(r#""></div>
<div><label>幂等键（可选）</label><input name="idem_key" placeholder="idem-1"></div></div>
<label>规范化字节内容（本表单按 UTF-8 文本；相同内容→原收据，不同内容同键→409）</label>
<textarea name="data"></textarea>
<button>提交块</button>
<button class="ghost" formaction="/sessions/renew">续租换发 token</button>
<button class="danger" formaction="/sessions/expire">（演示）令租约过期</button>
</form>"#);

            // 冻结
            let snaps = e.list_snapshots(false)?;
            out.push_str(r#"<h3>组装条目并冻结（manifest intent → refs → publish）</h3>
<form method="post" action="/sessions/freeze">
<input type="hidden" name="session_id" value="#);
            out.push_str(&escape(&s.id));
            out.push_str(r#"">
<div class="row"><div><label>fencing token</label><input name="token" value="#);
            out.push_str(&s.fence.to_string());
            out.push_str(r#""></div>
<div><label>快照标签</label><input name="label" value="新快照"></div>
<div><label>父快照（可空，派生请用专门表单）</label><select name="parent_id"><option value="">（无）</option>"#);
            for sp in &snaps {
                out.push_str(&format!(r#"<option value="{}">{} · {}</option>"#, escape(&sp.id), escape(&sp.id), escape(&sp.label)));
            }
            out.push_str(r#"</select></div></div>
<div class="row"><div><label>条目名 1</label><input name="entry1" value="readme.txt"></div>
<div><label>内容 1</label><textarea name="data1"></textarea></div></div>
<div class="row"><div><label>条目名 2（可选）</label><input name="entry2"></div>
<div><label>内容 2</label><textarea name="data2"></textarea></div></div>
<button>冻结快照</button></form>"#);
        }
        out.push_str("</details>");
    }
    out.push_str("</div>");
    Ok(out)
}

fn render_verify(state: &AppState, q: &[(String, String)]) -> Result<String, crate::engine::Error> {
    use crate::web::get;
    let e = &state.engine;
    let mut out = String::new();
    if get(q, "repaired").is_some() {
        out.push_str(r#"<div class="banner good">已用重新上传的原始字节修复（摘要与块名一致），并产生 <code>block.repaired</code> 收据；摘要保持不变。</div>"#);
    }

    // 操作按钮
    out.push_str(r#"<div class="card"><h2>完整性验证</h2>
<p class="muted">重算每个块文件摘要、核对 manifest 规范化摘要、引用一致性与事件哈希链。
篡改块后会列出所有受影响快照。</p>
<form method="post" action="/verify"><button>立即验证</button></form>
<h3>按正确字节修复块（重新上传形成事件，禁止改摘要）</h3>
<form method="post" action="/repair">
<div class="row"><div><label>目标块摘要（64 位 hex）</label><input name="hash" placeholder="ab12…"></div></div>
<label>该块原始内容（必须重新算出相同摘要，否则 409）</label>
<textarea name="data"></textarea>
<button>重新上传修复</button></form></div>"#);

    // 展示最近一次验证（不会因刷新页面产生新事件）；点击按钮才触发 e.verify()。
    let fresh = get(q, "ran").is_some();
    let report = if fresh { Some(e.verify()?) } else { None };
    let last_event = e.last_verification_event();
    let show = report.as_ref().map(|r| (r.ok, r.blocks_checked, r.snapshots_checked,
        r.chain_ok.clone(), r.chain_head.clone(),
        r.receipt_hash.clone().unwrap_or_default(),
        r.affected_snapshots.clone()));
    let summary = show.or_else(|| last_event.as_ref().map(|ev| {
        let p = &ev.payload;
        (p.get("ok").and_then(|v| v.as_bool()).unwrap_or(false),
         p.get("blocks_checked").and_then(|v| v.as_i64()).unwrap_or(0) as usize,
         p.get("snapshots_checked").and_then(|v| v.as_i64()).unwrap_or(0) as usize,
         p.get("chain_ok").and_then(|v| v.as_bool()).unwrap_or(false),
         String::new(),
         ev.hash.clone(),
         p.get("affected_snapshots").and_then(|v| v.as_array())
             .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
             .unwrap_or_default())
    }));
    if let Some((ok, bc, sc, chain_ok, head, receipt_hash, affected)) = summary {
        let cls = if ok { "good" } else { "bad" };
        let icon = if ok { "✅" } else { "⛔" };
        out.push_str(&format!(
            r#"<div class="card"><h2>最近验证结果 <span class="tag {cls}">{icon} {status}</span></h2>
<p class="muted">检查块 {bc} 个、快照 {sc} 个；事件链 {chain}；收据事件 <code>{receipt}</code>。</p>"#,
            status = if ok { "通过" } else { "发现损坏" },
            bc = bc,
            sc = sc,
            chain = if chain_ok { "完好" } else { "断裂" },
            receipt = short(&receipt_hash)
        ));
        let _ = head;
        if !affected.is_empty() {
            let aff = affected.iter().map(|s| escape(s)).collect::<Vec<_>>().join(", ");
            out.push_str(&format!(
                r#"<div class="banner bad">受影响快照（引用了被篡改/缺失块或 manifest 异常）：{aff}</div>"#,
                aff = aff
            ));
        }
        // 刚跑的验证带逐项详情；历史事件只显示摘要。
        if let Some(r) = &report {
            out.push_str(r#"<table><tr><th>对象摘要</th><th>结果</th><th>详情</th></tr>"#);
            for item in r.items.iter().filter(|i| !i.ok).chain(r.items.iter().filter(|i| i.ok)) {
                let (tag, cls) = if item.ok { ("OK", "good") } else { ("损坏", "bad") };
                out.push_str(&format!(
                    r#"<tr><td class="hash">{h}</td><td><span class="tag {cls}">{tag}</span></td><td>{d}</td></tr>"#,
                    h = short(&item.hash), d = escape(&item.detail)
                ));
            }
            out.push_str("</table>");
        } else {
            out.push_str(r#"<p class="muted">以上为最近一次验证收据摘要；点击“立即验证”可重算并生成新收据。</p>"#);
        }
        out.push_str("</div>");
    } else {
        out.push_str(r#"<div class="card"><h2>最近验证结果</h2>
<p class="muted">尚未运行验证。</p></div>"#);
    }
    Ok(out)
}

fn render_gc(state: &AppState, _q: &[(String, String)]) -> Result<String, crate::engine::Error> {
    let e = &state.engine;
    let mut out = r#"<div class="card"><h2>垃圾回收（代次屏障）</h2>
<p class="muted">GC 开始时记录当前最大快照发布序号作为屏障；扫描窗口中若有新快照冻结
（序号大于屏障），其引用的块即使当时无引用行也不会被删除。删除在每个块的事务内
重查引用与屏障，再删元数据行和文件，并留下 <code>gc.started / gc.block_deleted / gc.finished</code> 收据。</p>
<form method="post" action="/gc"><button>执行 GC</button></form></div>"#.to_string();

    // 最近 GC 事件
    let events = e.list_events(500)?;
    let gc_events: Vec<_> = events
        .iter()
        .filter(|ev| ev.kind.starts_with("gc."))
        .collect();
    out.push_str(r#"<div class="card"><h2>回收事件（可核验收据）</h2>"#);
    if gc_events.is_empty() {
        out.push_str(r#"<p class="muted">暂无 GC 事件（预置数据在首次启动时已执行过一次）。</p>"#);
    } else {
        out.push_str(r#"<table><tr><th>#</th><th>事件</th><th>摘要</th><th>事件哈希（链内）</th></tr>"#);
        for ev in gc_events.iter().take(40) {
            let summary = match ev.kind.as_str() {
                "gc.started" => format!(
                    "gc={} 屏障代次={} 候选块 {}",
                    ev.payload.get("gc").and_then(|v| v.as_str()).unwrap_or(""),
                    ev.payload.get("barrier_seq").and_then(|v| v.as_i64()).unwrap_or(0),
                    ev.payload.get("candidates").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0)
                ),
                "gc.block_deleted" => format!("删除块 {}", ev.payload.get("block").and_then(|v| v.as_str()).unwrap_or("")),
                "gc.spared" => format!("保留块 {}（{}）",
                    ev.payload.get("block").and_then(|v| v.as_str()).unwrap_or(""),
                    ev.payload.get("reason").and_then(|v| v.as_str()).unwrap_or("")),
                "gc.finished" => format!(
                    "gc={} 完成，删除 {} 个，保留 {} 个",
                    ev.payload.get("gc").and_then(|v| v.as_str()).unwrap_or(""),
                    ev.payload.get("deleted").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0),
                    ev.payload.get("spared").and_then(|v| v.as_array()).map(|a| a.len()).unwrap_or(0)
                ),
                "gc.resumed" => format!("崩溃后恢复完成 gc={}", ev.payload.get("gc").and_then(|v| v.as_str()).unwrap_or("")),
                other => other.to_string(),
            };
            out.push_str(&format!(
                r#"<tr><td>{seq}</td><td>{kind}</td><td>{sum}</td><td class="hash">{h}</td></tr>"#,
                seq = ev.seq,
                kind = escape(&ev.kind),
                sum = escape(&summary),
                h = short(&ev.hash)
            ));
        }
        out.push_str("</table>");
    }
    out.push_str("</div>");
    Ok(out)
}

fn render_export(state: &AppState, q: &[(String, String)]) -> Result<String, crate::engine::Error> {
    use crate::web::get;
    let e = &state.engine;
    let snaps = e.list_snapshots(false)?;
    let mut out = String::new();
    if let Some(v) = get(q, "import_ok") {
        if v == "1" {
            out.push_str(r#"<div class="banner good">导入包核对通过：manifest 规范化摘要与冻结收据一致（未写入任何块）。</div>"#);
        } else {
            out.push_str(r#"<div class="banner bad">导入包核对失败：摘要或冻结收据不一致。</div>"#);
        }
    }

    out.push_str(r#"<div class="card"><h2>导出（只含 manifest 与验证证明，不含块数据）</h2>
<p class="muted">导出包为 JSON：规范化 manifest、manifest 摘要、冻结收据事件（位于哈希链中）
与导出时链头。可在本库或另一实例“导入核对”。</p>"#);
    if snaps.is_empty() {
        out.push_str(r#"<p class="muted">没有可导出的已发布快照。</p>"#);
    } else {
        out.push_str("<ul>");
        for s in &snaps {
            out.push_str(&format!(
                r#"<li><a href="/api/export/{id}">{id}</a> · {label} · manifest <code>{mh}</code></li>"#,
                id = escape(&s.id),
                label = escape(&s.label),
                mh = short(&s.manifest_hash)
            ));
        }
        out.push_str("</ul>");
    }
    out.push_str(r#"</div>
<div class="card"><h2>导入核对（重新计算摘要 + 冻结收据证明）</h2>
<form method="post" action="/import">
<label>粘贴导出包 JSON</label><textarea name="bundle" style="min-height:200px"></textarea>
<button>核对（不写块、不建快照）</button></form></div>"#);
    Ok(out)
}

fn render_events(state: &AppState, _q: &[(String, String)]) -> Result<String, crate::engine::Error> {
    let e = &state.engine;
    let events = e.list_events(300)?;
    let mut out = r#"<div class="card"><h2>事件收据日志（哈希链）</h2>
<p class="muted">每条事件覆盖前一条摘要：prev 链 + 摘要重算可在“验证”页一键核验。
冻结、去重、删除、GC、修复、崩溃恢复都会留痕。</p>
<table><tr><th>#</th><th>时间(ms)</th><th>事件</th><th>载荷</th><th>prev</th><th>hash</th></tr>"#.to_string();
    for ev in &events {
        out.push_str(&format!(
            r#"<tr><td>{seq}</td><td>{ts}</td><td>{kind}</td>
<td><pre style="max-height:120px">{payload}</pre></td>
<td class="hash">{prev}</td><td class="hash">{hash}</td></tr>"#,
            seq = ev.seq,
            ts = ev.ts,
            kind = escape(&ev.kind),
            payload = escape(&serde_json::to_string_pretty(&ev.payload).unwrap_or_default()),
            prev = short(&ev.prev_hash),
            hash = short(&ev.hash)
        ));
    }
    out.push_str("</table></div>");
    Ok(out)
}

pub fn render_snapshot_page(
    state: &AppState,
    s: &crate::engine::SnapshotInfo,
) -> String {
    let status_tag = if s.status == "published" {
        r#"<span class="tag good">published</span>"#
    } else {
        r#"<span class="tag bad">deleted</span>"#
    };
    let mut body = format!(
        r#"<p><a href="/?tab=snapshots">← 返回快照列表</a></p>
<div class="card"><h2>{id} {tag}</h2>
<p class="muted">标签 {label}　父快照 <code>{parent}</code>　发布代次 {seq}　创建 {created}</p>
<p>manifest 摘要（规范化 JSON）：<code>{mh}</code></p>
<h3>条目与块映射</h3>
<table><tr><th>条目</th><th>字节</th><th>块序列（position: hash@size）</th></tr>"#,
        id = escape(&s.id),
        tag = status_tag,
        label = escape(&s.label),
        parent = s.parent_id.as_deref().unwrap_or("—"),
        seq = s.publish_seq.unwrap_or(0),
        created = s.created,
        mh = s.manifest_hash
    );
    for en in &s.entries {
        let chunks = en
            .chunks
            .iter()
            .map(|c| format!("{}:{}", c.position, short(&c.hash)))
            .collect::<Vec<_>>()
            .join("<br>");
        body.push_str(&format!(
            r#"<tr><td>{name}</td><td>{size}</td><td class="hash">{chunks}</td></tr>"#,
            name = escape(&en.name),
            size = en.size,
            chunks = chunks
        ));
    }
    body.push_str("</table></div>");
    body.push_str(&format!(
        r#"<div class="card"><h2>规范化 manifest</h2><pre>{m}</pre>
<p><a href="/api/export/{id}">导出该快照（manifest + 验证证明）</a></p></div>"#,
        m = escape(&serde_json::to_string_pretty(&s.manifest).unwrap_or_default()),
        id = escape(&s.id)
    ));
    let _ = state;
    let _ = json!({});
    page("快照 manifest · 内容寻址快照库", "snapshots", body)
}
