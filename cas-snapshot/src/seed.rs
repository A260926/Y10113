//! 首次启动预置演示数据：
//! 快照 `snapshot-alpha` 与 `snapshot-beta` 共享一个公共块；
//! 随后删除 alpha 并执行 GC，证明共享块仍被 beta 引用，beta 完整可验证。
//! 幂等：依据 meta.seeded 标记，只执行一次。

use std::collections::BTreeMap;

use rusqlite::params;

use crate::engine::Engine;

pub fn seed_if_empty(engine: &Engine) -> crate::engine::Result<()> {
    let already: bool = {
        // 通过快照数量判断，避免直接触碰连接锁外状态。
        !engine.list_snapshots(true)?.is_empty()
    };
    if already {
        return Ok(());
    }

    // 三个内容：common 被两个快照共享；alpha/beta 各有私有内容。
    let common = "=== 共享条目：团队手册 / common chapter ===
内容寻址意味着同样的字节只占一份空间。
".as_bytes().to_vec();
    let alpha_only = "alpha 私有：路线图 A（将被删除）
".as_bytes().to_vec();
    let beta_only = "beta 私有：路线图 B（删除 alpha 后仍保留）
".as_bytes().to_vec();

    // alpha 会话。
    let s1 = engine.create_session("seed-alpha", Some(60 * 60 * 1000))?;
    let _r_common_1 = engine.submit_block(&s1.id, s1.fence, Some("common"), &common)?;
    let r_alpha = engine.submit_block(&s1.id, s1.fence, Some("alpha-only"), &alpha_only)?;
    let entries_a = engine.assemble_entries(
        &s1.id,
        &[
            ("common.txt".to_string(), common.clone()),
            ("roadmap-a.txt".to_string(), alpha_only),
        ],
    )?;
    let alpha = engine.freeze(
        &s1.id,
        s1.fence,
        "snapshot-alpha",
        None,
        entries_a,
    )?;

    // beta 会话：common 再次提交（不同会话），只产生收据不重复落盘。
    let s2 = engine.create_session("seed-beta", Some(60 * 60 * 1000))?;
    let r_common_2 = engine.submit_block(&s2.id, s2.fence, Some("common"), &common)?;
    let _r_beta = engine.submit_block(&s2.id, s2.fence, Some("beta-only"), &beta_only)?;
    let entries_b = engine.assemble_entries(
        &s2.id,
        &[
            ("common.txt".to_string(), common),
            ("roadmap-b.txt".to_string(), beta_only),
        ],
    )?;
    let beta = engine.freeze(
        &s2.id,
        s2.fence,
        "snapshot-beta",
        None,
        entries_b,
    )?;

    // 删除 alpha 并回收：common 必须保留（beta 引用），alpha-only 应被回收。
    engine.delete_snapshot(&alpha.id)?;
    let gc = engine.gc(None)?;

    // beta 必须仍完整；common 块仍在，alpha-only 已删除。
    let report = engine.verify()?;
    if !report.ok {
        return Err(crate::engine::Error::Corrupt(format!(
            "预置后验证失败：{:?}",
            report.items.iter().filter(|i| !i.ok).map(|i| &i.detail).collect::<Vec<_>>()
        )));
    }
    let blocks: BTreeMap<String, _> = engine
        .list_blocks()?
        .into_iter()
        .map(|b| (b.hash.clone(), b))
        .collect();
    if !blocks.contains_key(&r_common_2.block_hash) {
        return Err(crate::engine::Error::Corrupt(
            "预置失败：共享块在删除 alpha + GC 后丢失".into(),
        ));
    }
    if blocks.contains_key(&r_alpha.block_hash) {
        return Err(crate::engine::Error::Corrupt(
            "预置失败：alpha 私有块未被回收".into(),
        ));
    }

    // 记录一条预置完成收据（直接写 meta 供页面展示说明）。
    engine.with_conn(|conn| -> rusqlite::Result<()> {
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('seeded', '1')",
            [],
        )?;
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('seed.alpha', ?1)",
            params![alpha.id],
        )?;
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('seed.beta', ?1)",
            params![beta.id],
        )?;
        conn.execute(
            "INSERT OR REPLACE INTO meta (key, value) VALUES ('seed.gc', ?1)",
            params![gc.gc_id],
        )?;
        Ok(())
    })?;
    Ok(())
}
