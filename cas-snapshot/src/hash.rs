//! SHA-256 内容寻址与十六进制编码。

use sha2::{Digest, Sha256};

pub const HASH_HEX_LEN: usize = 64;

/// 计算规范化字节的 SHA-256，返回小写十六进制字符串。
pub fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex::encode(hasher.finalize())
}

/// 校验字符串是否为 64 位小写十六进制摘要。
pub fn is_hash_hex(s: &str) -> bool {
    s.len() == HASH_HEX_LEN
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
