//! 规范化 JSON：对象键排序、无多余空白，保证 manifest 字节确定。

use serde_json::Value;

/// 将任意 JSON 转换为规范化字节（键按字典序、紧凑分隔）。
pub fn canonical_bytes(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    write_canon(value, &mut out);
    out
}

fn write_canon(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.extend_from_slice(b"null"),
        Value::Bool(b) => out.extend_from_slice(if *b { b"true" } else { b"false" }),
        Value::Number(n) => out.extend_from_slice(n.to_string().as_bytes()),
        Value::String(s) => out.extend_from_slice(serde_json::to_vec(s).unwrap().as_slice()),
        Value::Array(items) => {
            out.push(b'[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                write_canon(item, out);
            }
            out.push(b']');
        }
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();
            out.push(b'{');
            for (i, key) in keys.iter().enumerate() {
                if i > 0 {
                    out.push(b',');
                }
                out.extend_from_slice(serde_json::to_vec(key).unwrap().as_slice());
                out.push(b':');
                write_canon(&map[*key], out);
            }
            out.push(b'}');
        }
    }
}

/// 计算任意 JSON 的规范化摘要。
pub fn canonical_hash(value: &Value) -> String {
    crate::hash::sha256_hex(&canonical_bytes(value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn key_order_is_ignored() {
        let a = json!({"b": 1, "a": [1, {"y": 2, "x": 3}]});
        let b = json!({"a": [1, {"x": 3, "y": 2}], "b": 1});
        assert_eq!(canonical_bytes(&a), canonical_bytes(&b));
        assert_eq!(
            std::str::from_utf8(&canonical_bytes(&a)).unwrap(),
            r#"{"a":[1,{"x":3,"y":2}],"b":1}"#
        );
    }
}
