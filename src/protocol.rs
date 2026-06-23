use serde::{Deserialize, Serialize};
use serde_json::Value;

// ── standard JSON-RPC error codes ──
pub const PARSE_ERROR: i32 = -32700;
pub const METHOD_NOT_FOUND: i32 = -32601;
pub const INVALID_PARAMS: i32 = -32602;
#[allow(dead_code)]
pub const INTERNAL_ERROR: i32 = -32603;

// ── custom error codes ──
pub const UNAUTHORIZED: i32 = -32001;
pub const SESSION_NOT_FOUND: i32 = -32002;
#[allow(dead_code)]
pub const EXECUTION_ERROR: i32 = -32003;
pub const TIMEOUT_ERROR: i32 = -32004;

// ── inbound ──
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest {
    #[allow(dead_code)]
    pub jsonrpc: String,
    #[serde(default)]
    pub id: Option<Value>,
    pub method: String,
    #[serde(default)]
    pub params: Option<Value>,
}

// ── outbound success ──
#[derive(Debug, Serialize)]
pub struct JsonRpcSuccess {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    pub result: Value,
}

// ── outbound error ──
#[derive(Debug, Serialize)]
pub struct JsonRpcErrorResponse {
    pub jsonrpc: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Value>,
    pub error: RpcErrorDetail,
}

#[derive(Debug, Serialize)]
pub struct RpcErrorDetail {
    pub code: i32,
    pub message: String,
}

impl JsonRpcSuccess {
    pub fn new(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            result,
        }
    }
}

impl JsonRpcErrorResponse {
    pub fn new(id: Option<Value>, code: i32, message: String) -> Self {
        Self {
            jsonrpc: "2.0",
            id,
            error: RpcErrorDetail { code, message },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    // ── constants (from A) ──
    #[test]
    fn error_code_values_are_stable() {
        assert_eq!(PARSE_ERROR, -32700);
        assert_eq!(METHOD_NOT_FOUND, -32601);
        assert_eq!(INVALID_PARAMS, -32602);
        assert_eq!(INTERNAL_ERROR, -32603);
        assert_eq!(UNAUTHORIZED, -32001);
        assert_eq!(SESSION_NOT_FOUND, -32002);
        assert_eq!(EXECUTION_ERROR, -32003);
        assert_eq!(TIMEOUT_ERROR, -32004);
    }

    // ── JsonRpcRequest deserialization (from A, with C additions) ──

    #[test]
    fn deserialize_full_request() {
        let body = r#"{
            "jsonrpc": "2.0",
            "id": 7,
            "method": "tools/call",
            "params": {"name": "execute_command", "arguments": {"command": "echo hi"}}
        }"#;
        let req: JsonRpcRequest = serde_json::from_str(body).unwrap();
        assert_eq!(req.jsonrpc, "2.0");
        assert_eq!(req.method, "tools/call");
        assert!(req.id.is_some());
        assert!(req.params.is_some());
    }

    #[test]
    fn deserialize_request_without_id() {
        let body = r#"{"jsonrpc": "2.0", "method": "ping"}"#;
        let req: JsonRpcRequest = serde_json::from_str(body).unwrap();
        assert_eq!(req.method, "ping");
        assert!(req.id.is_none());
        assert!(req.params.is_none());
    }

    #[test]
    fn deserialize_null_id() {
        // serde treats JSON null as Rust None for Option<T>, so a null id
        // is indistinguishable from a missing id at this deserialization layer.
        // Note: this differs from parse_id_from_body() in handler.rs which
        // works on raw serde_json::Value and would return Some(Value::Null).
        let json = r#"{"jsonrpc":"2.0","id":null,"method":"ping"}"#;
        let req: JsonRpcRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.id, None);
    }

    #[test]
    fn deserialize_numeric_id() {
        let json = r#"{"jsonrpc":"2.0","id":123,"method":"ping"}"#;
        let req: JsonRpcRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.id, Some(serde_json::json!(123)));
    }

    #[test]
    fn deserialize_request_missing_params() {
        let body = r#"{"jsonrpc": "2.0", "id": 1, "method": "initialize"}"#;
        let req: JsonRpcRequest = serde_json::from_str(body).unwrap();
        assert!(req.params.is_none());
    }

    // ── deserialization failure paths (from C) ──

    #[test]
    fn deserialize_missing_method() {
        let json = r#"{"jsonrpc":"2.0","id":"req-1"}"#;
        let result = serde_json::from_str::<JsonRpcRequest>(json);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_empty_object() {
        let result = serde_json::from_str::<JsonRpcRequest>(r#"{}"#);
        assert!(result.is_err());
    }

    #[test]
    fn deserialize_malformed_json() {
        let result = serde_json::from_str::<JsonRpcRequest>(r#"not json"#);
        assert!(result.is_err());
    }

    // ── JsonRpcSuccess serialization (from A) ──

    #[test]
    fn serialize_success_with_id() {
        let resp = JsonRpcSuccess::new(Some(json!(42)), json!({"ok": true}));
        let json = serde_json::to_string(&resp).unwrap();
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 42);
        assert_eq!(v["result"]["ok"], true);
    }

    #[test]
    fn serialize_success_without_id_skips_field() {
        let resp = JsonRpcSuccess::new(None, json!({"hello": "world"}));
        let json = serde_json::to_string(&resp).unwrap();
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert!(v.get("id").is_none());
        assert_eq!(v["result"]["hello"], "world");
    }

    // ── JsonRpcErrorResponse serialization (from A) ──

    #[test]
    fn serialize_error_with_id() {
        let resp = JsonRpcErrorResponse::new(
            Some(json!(1)),
            METHOD_NOT_FOUND,
            "method not found".to_string(),
        );
        let json = serde_json::to_string(&resp).unwrap();
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert_eq!(v["id"], 1);
        assert_eq!(v["error"]["code"], METHOD_NOT_FOUND);
        assert_eq!(v["error"]["message"], "method not found");
    }

    #[test]
    fn serialize_error_without_id_skips_field() {
        let resp = JsonRpcErrorResponse::new(None, PARSE_ERROR, "parse error".to_string());
        let json = serde_json::to_string(&resp).unwrap();
        let v: Value = serde_json::from_str(&json).unwrap();
        assert_eq!(v["jsonrpc"], "2.0");
        assert!(v.get("id").is_none());
        assert_eq!(v["error"]["code"], PARSE_ERROR);
    }

    // ── jsonrpc field consistency (from A) ──

    #[test]
    fn success_has_required_jsonrpc_field() {
        let resp = JsonRpcSuccess::new(Some(json!("x")), json!(null));
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
    }

    #[test]
    fn error_has_required_jsonrpc_field() {
        let resp =
            JsonRpcErrorResponse::new(Some(json!("x")), SESSION_NOT_FOUND, "gone".to_string());
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"jsonrpc\":\"2.0\""));
    }
}
