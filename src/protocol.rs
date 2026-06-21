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
