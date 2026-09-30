//! 本地内容寻址快照库：分块、去重、引用计数、冻结/恢复、代次 GC、验证与导出。

pub mod canon;
pub mod engine;
pub mod events;
pub mod hash;
pub mod schema;
pub mod seed;
pub mod web;
