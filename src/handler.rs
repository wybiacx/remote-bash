use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event, Sse},
};
use futures::stream::Stream;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use uuid::Uuid;

use crate::executor;
use crate::protocol::{
    JsonRpcErrorResponse, JsonRpcRequest, JsonRpcSuccess, INVALID_PARAMS, METHOD_NOT_FOUND,
    PARSE_ERROR, SESSION_NOT_FOUND, TIMEOUT_ERROR, UNAUTHORIZED,
};
use crate::session;

use crate::AppState;

// ── helpers ──

fn extract_bearer_token(headers: &HeaderMap) -> Option<String> {
    let auth = headers.get("authorization")?.to_str().ok()?;
    let stripped = auth.strip_prefix("Bearer ")?;
    Some(stripped.to_string())
}

fn verify_token(state: &AppState, headers: &HeaderMap) -> bool {
    match extract_bearer_token(headers) {
        Some(t) => t == state.token,
        None => false,
    }
}

/// Try to pull an `id` out of a raw JSON body, even if the whole request is malformed.
fn parse_id_from_body(body: &str) -> Option<Value> {
    serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|v| v.get("id").cloned())
}

fn push_error(
    sessions: &session::SessionMap,
    sid: Option<Uuid>,
    id: Option<Value>,
    code: i32,
    message: &str,
) {
    let Some(sid) = sid else { return };
    let resp =
        JsonRpcErrorResponse::new(Some(id.unwrap_or(Value::Null)), code, message.to_string());
    if let Ok(json) = serde_json::to_string(&resp) {
        // fire-and-forget: if session is gone the send just fails silently
        let sessions = sessions.clone();
        let msg = json;
        tokio::spawn(async move {
            send_or_close_on_full(&sessions, &sid, msg).await;
        });
    }
}

async fn send_or_close_on_full(sessions: &session::SessionMap, sid: &Uuid, msg: String) -> bool {
    match session::send_to_session(sessions, sid, msg).await {
        session::SendResult::Sent => true,
        session::SendResult::Full => {
            tracing::warn!(
                %sid,
                "SSE session send queue is full, closing session to avoid dropping JSON-RPC response"
            );
            session::remove_session(sessions, sid).await;
            false
        }
        session::SendResult::Closed => {
            tracing::debug!(%sid, "SSE session receiver is closed");
            false
        }
        session::SendResult::Missing => {
            tracing::debug!(%sid, "SSE session is missing");
            false
        }
    }
}

// ── SSE handler ──

pub async fn sse_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let authenticated = verify_token(&state, &headers);
    let sessions = state.sessions.clone();

    let stream = async_stream::stream! {
        // 1. auth
        if !authenticated {
            let err = json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": { "code": UNAUTHORIZED, "message": "Unauthorized: invalid token" }
            })
            .to_string();
            yield Ok(Event::default().event("error").data(err));
            return;
        }

        // 2. create session
        let (session_id, rx) = session::create_session(&sessions).await;
        tracing::info!(%session_id, "SSE session created");

        let endpoint_url = format!("/messages?session_id={}", session_id);

        // send endpoint event first
        yield Ok(Event::default().event("endpoint").data(endpoint_url));

        let mut rx_stream = ReceiverStream::new(rx);
        while let Some(msg) = rx_stream.next().await {
            yield Ok(Event::default().event("message").data(msg));
        }

        // receiver dropped → client disconnected or session removed
        tracing::info!(%session_id, "SSE stream ended, removing session");
        session::remove_session(&sessions, &session_id).await;
    };

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(std::time::Duration::from_secs(15))
            .text("keep-alive"),
    )
}

// ── POST /messages handler ──

pub async fn message_handler(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<HashMap<String, String>>,
    body: String,
) -> StatusCode {
    let session_id_str = params.get("session_id");

    // 1. verify token
    if !verify_token(&state, &headers) {
        let id = parse_id_from_body(&body);
        let sid = session_id_str.and_then(|s| Uuid::parse_str(s).ok());
        push_error(
            &state.sessions,
            sid,
            id,
            UNAUTHORIZED,
            "Unauthorized: invalid token",
        );
        return StatusCode::ACCEPTED;
    }

    // 2. parse session_id
    let Some(sid) = session_id_str.and_then(|s| Uuid::parse_str(s).ok()) else {
        tracing::warn!("POST /messages missing or invalid session_id");
        return StatusCode::BAD_REQUEST;
    };

    // 3. verify session exists
    if !session::session_exists(&state.sessions, &sid).await {
        let id = parse_id_from_body(&body);
        push_error(
            &state.sessions,
            Some(sid),
            id,
            SESSION_NOT_FOUND,
            "Session not found",
        );
        return StatusCode::ACCEPTED;
    }

    // 4. parse JSON-RPC
    let request: JsonRpcRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(%body, "JSON-RPC parse error: {}", e);
            let id = parse_id_from_body(&body);
            push_error(
                &state.sessions,
                Some(sid),
                id,
                PARSE_ERROR,
                &format!("Parse error: {}", e),
            );
            return StatusCode::ACCEPTED;
        }
    };

    tracing::debug!(method = %request.method, id = ?request.id, "received request");

    // 5. dispatch
    let sessions = state.sessions.clone();
    let sid2 = sid;
    tokio::spawn(async move {
        let result = handle_method(&request).await;
        match result {
            Ok(resp) => {
                if let Some(ref id) = request.id {
                    // only respond if there's an id (not a notification)
                    let success = JsonRpcSuccess::new(Some(id.clone()), resp);
                    if let Ok(json) = serde_json::to_string(&success) {
                        send_or_close_on_full(&sessions, &sid2, json).await;
                    }
                }
            }
            Err(e) => {
                if let Some(ref id) = request.id {
                    let err_resp = JsonRpcErrorResponse::new(Some(id.clone()), e.code, e.message);
                    if let Ok(json) = serde_json::to_string(&err_resp) {
                        send_or_close_on_full(&sessions, &sid2, json).await;
                    }
                }
            }
        }
    });

    StatusCode::ACCEPTED
}

// ── method dispatch ──

#[derive(Debug)]
struct HandlerError {
    code: i32,
    message: String,
}

async fn handle_method(req: &JsonRpcRequest) -> Result<Value, HandlerError> {
    match req.method.as_str() {
        "initialize" => handle_initialize(req).await,
        "notifications/initialized" => {
            // notification — no response needed
            Ok(Value::Null)
        }
        "tools/list" => handle_tools_list(req).await,
        "tools/call" => handle_tools_call(req).await,
        "ping" => Ok(json!({})),
        _ => Err(HandlerError {
            code: METHOD_NOT_FOUND,
            message: format!("Method not found: {}", req.method),
        }),
    }
}

async fn handle_initialize(_req: &JsonRpcRequest) -> Result<Value, HandlerError> {
    Ok(json!({
        "protocolVersion": "2024-11-05",
        "capabilities": {
            "tools": {}
        },
        "serverInfo": {
            "name": "remote-bash",
            "version": "0.1.0"
        }
    }))
}

async fn handle_tools_list(_req: &JsonRpcRequest) -> Result<Value, HandlerError> {
    Ok(json!({
        "tools": [
            {
                "name": "execute_command",
                "description": "Execute an arbitrary bash command on the remote server.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "string",
                            "description": "The bash command to execute."
                        },
                        "timeout": {
                            "type": "integer",
                            "description": "Timeout in seconds (default 30).",
                            "default": executor::DEFAULT_TIMEOUT_SECS,
                            "minimum": 1,
                            "maximum": executor::MAX_TIMEOUT_SECS
                        }
                    },
                    "required": ["command"]
                }
            }
        ]
    }))
}

async fn handle_tools_call(req: &JsonRpcRequest) -> Result<Value, HandlerError> {
    let params = req.params.as_ref().ok_or_else(|| HandlerError {
        code: INVALID_PARAMS,
        message: "Missing params".into(),
    })?;

    let tool_name = params
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| HandlerError {
            code: INVALID_PARAMS,
            message: "Missing tool name".into(),
        })?;

    if tool_name != "execute_command" {
        return Err(HandlerError {
            code: METHOD_NOT_FOUND,
            message: format!("Unknown tool: {}", tool_name),
        });
    }

    let args = params.get("arguments").ok_or_else(|| HandlerError {
        code: INVALID_PARAMS,
        message: "Missing arguments".into(),
    })?;

    let command = args
        .get("command")
        .and_then(|v| v.as_str())
        .ok_or_else(|| HandlerError {
            code: INVALID_PARAMS,
            message: "Missing command argument".into(),
        })?;

    let timeout_secs = match args.get("timeout") {
        None => executor::DEFAULT_TIMEOUT_SECS,
        Some(value) => {
            let Some(timeout) = value.as_u64() else {
                return Err(HandlerError {
                    code: INVALID_PARAMS,
                    message: "Timeout must be a positive integer".into(),
                });
            };

            if timeout == 0 || timeout > executor::MAX_TIMEOUT_SECS {
                return Err(HandlerError {
                    code: INVALID_PARAMS,
                    message: format!(
                        "Timeout must be between 1 and {} seconds",
                        executor::MAX_TIMEOUT_SECS
                    ),
                });
            }

            timeout
        }
    };

    if command.trim().is_empty() {
        return Err(HandlerError {
            code: INVALID_PARAMS,
            message: "Command must not be empty".into(),
        });
    }

    tracing::info!(command, timeout_secs, "executing command");

    let result = executor::execute_bash(command, timeout_secs).await;
    let content = executor::to_mcp_content(&result);

    if result.timed_out {
        Err(HandlerError {
            code: TIMEOUT_ERROR,
            message: format!("Command timed out after {} seconds", timeout_secs),
        })
    } else if result.exit_code.is_some_and(|c| c != 0) {
        // Non-zero exit — still return content so the agent can see output
        Ok(json!({
            "content": content,
            "isError": true
        }))
    } else {
        Ok(json!({
            "content": content
        }))
    }
}

// ── protocol tests ──
#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{header, HeaderMap, HeaderValue};
    use serde_json::json;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    // ── helpers for B's protocol tests ──
    fn req(method: &str, id: Option<Value>, params: Option<Value>) -> JsonRpcRequest {
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            id,
            method: method.into(),
            params,
        }
    }

    // ── helpers for C's security tests ──
    fn make_state(token: &str) -> AppState {
        AppState {
            sessions: Arc::new(Mutex::new(std::collections::HashMap::new())),
            token: token.to_string(),
        }
    }

    fn bearer_headers(token: Option<&str>) -> HeaderMap {
        let mut h = HeaderMap::new();
        if let Some(t) = token {
            h.insert(
                header::AUTHORIZATION,
                HeaderValue::from_str(&format!("Bearer {}", t)).unwrap(),
            );
        }
        h
    }

    #[tokio::test]
    async fn send_or_close_on_full_removes_overloaded_session() {
        let sessions: session::SessionMap = Arc::new(Mutex::new(std::collections::HashMap::new()));
        let (sid, _rx) = session::create_session(&sessions).await;

        for i in 0..64 {
            assert_eq!(
                session::send_to_session(&sessions, &sid, format!("msg-{i}")).await,
                session::SendResult::Sent
            );
        }

        let sent = send_or_close_on_full(&sessions, &sid, "overflow".to_string()).await;
        assert!(!sent);
        assert!(!session::session_exists(&sessions, &sid).await);
    }

    // ══════════════════════════════════════════════════════════════
    // B: initialize
    // ══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn initialize_returns_protocol_version() {
        let r = req("initialize", Some(json!(1)), None);
        let result = handle_method(&r).await.unwrap();
        assert_eq!(result["protocolVersion"], "2024-11-05");
    }

    #[tokio::test]
    async fn initialize_returns_capabilities() {
        let r = req("initialize", Some(json!(1)), None);
        let result = handle_method(&r).await.unwrap();
        let caps = &result["capabilities"];
        assert!(caps["tools"].is_object());
    }

    #[tokio::test]
    async fn initialize_returns_server_info() {
        let r = req("initialize", Some(json!(1)), None);
        let result = handle_method(&r).await.unwrap();
        assert_eq!(result["serverInfo"]["name"], "remote-bash");
        assert_eq!(result["serverInfo"]["version"], "0.1.0");
    }

    // ══════════════════════════════════════════════════════════════
    // B: tools/list
    // ══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn tools_list_returns_execute_command() {
        let r = req("tools/list", Some(json!(1)), None);
        let result = handle_method(&r).await.unwrap();
        let tools = result["tools"].as_array().unwrap();
        assert_eq!(tools.len(), 1);
        assert_eq!(tools[0]["name"], "execute_command");
    }

    #[tokio::test]
    async fn tools_list_includes_input_schema() {
        let r = req("tools/list", Some(json!(1)), None);
        let result = handle_method(&r).await.unwrap();
        let tools = result["tools"].as_array().unwrap();
        let schema = &tools[0]["inputSchema"];
        assert_eq!(schema["type"], "object");
        assert!(schema["properties"]["command"].is_object());
        assert_eq!(
            schema["properties"]["timeout"]["default"],
            executor::DEFAULT_TIMEOUT_SECS
        );
        assert_eq!(schema["properties"]["timeout"]["minimum"], 1);
        assert_eq!(
            schema["properties"]["timeout"]["maximum"],
            executor::MAX_TIMEOUT_SECS
        );
        assert!(schema["required"]
            .as_array()
            .unwrap()
            .contains(&json!("command")));
    }

    // ══════════════════════════════════════════════════════════════
    // B: unknown method
    // ══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn unknown_method_returns_method_not_found() {
        let r = req("nonexistent/method", Some(json!(1)), None);
        let err = handle_method(&r).await.unwrap_err();
        assert_eq!(err.code, METHOD_NOT_FOUND);
        assert!(err.message.contains("nonexistent/method"));
    }

    // ══════════════════════════════════════════════════════════════
    // B: tools/call params validation
    // ══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn tools_call_missing_params_returns_invalid_params() {
        let r = req("tools/call", Some(json!(1)), None);
        let err = handle_method(&r).await.unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
        assert!(err.message.contains("Missing params"));
    }

    #[tokio::test]
    async fn tools_call_missing_name_returns_invalid_params() {
        let r = req("tools/call", Some(json!(1)), Some(json!({})));
        let err = handle_method(&r).await.unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
        assert!(err.message.contains("Missing tool name"));
    }

    #[tokio::test]
    async fn tools_call_missing_arguments_returns_invalid_params() {
        let r = req(
            "tools/call",
            Some(json!(1)),
            Some(json!({"name": "execute_command"})),
        );
        let err = handle_method(&r).await.unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
        assert!(err.message.contains("Missing arguments"));
    }

    #[tokio::test]
    async fn tools_call_missing_command_returns_invalid_params() {
        let r = req(
            "tools/call",
            Some(json!(1)),
            Some(json!({"name": "execute_command", "arguments": {}})),
        );
        let err = handle_method(&r).await.unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
        assert!(err.message.contains("Missing command argument"));
    }

    #[tokio::test]
    async fn tools_call_empty_command_returns_invalid_params() {
        let r = req(
            "tools/call",
            Some(json!(1)),
            Some(json!({
                "name": "execute_command",
                "arguments": {"command": "   "}
            })),
        );
        let err = handle_method(&r).await.unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
        assert!(err.message.contains("Command must not be empty"));
    }

    #[tokio::test]
    async fn tools_call_zero_timeout_returns_invalid_params() {
        let r = req(
            "tools/call",
            Some(json!(1)),
            Some(json!({
                "name": "execute_command",
                "arguments": {"command": "echo hello", "timeout": 0}
            })),
        );
        let err = handle_method(&r).await.unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
        assert!(err.message.contains("Timeout must be between"));
    }

    #[tokio::test]
    async fn tools_call_large_timeout_returns_invalid_params() {
        let r = req(
            "tools/call",
            Some(json!(1)),
            Some(json!({
                "name": "execute_command",
                "arguments": {
                    "command": "echo hello",
                    "timeout": executor::MAX_TIMEOUT_SECS + 1
                }
            })),
        );
        let err = handle_method(&r).await.unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
        assert!(err.message.contains("Timeout must be between"));
    }

    #[tokio::test]
    async fn tools_call_non_integer_timeout_returns_invalid_params() {
        let r = req(
            "tools/call",
            Some(json!(1)),
            Some(json!({
                "name": "execute_command",
                "arguments": {"command": "echo hello", "timeout": "slow"}
            })),
        );
        let err = handle_method(&r).await.unwrap_err();
        assert_eq!(err.code, INVALID_PARAMS);
        assert!(err.message.contains("Timeout must be a positive integer"));
    }

    #[tokio::test]
    async fn tools_call_unknown_tool_returns_method_not_found() {
        let r = req(
            "tools/call",
            Some(json!(1)),
            Some(json!({"name": "unknown_tool"})),
        );
        let err = handle_method(&r).await.unwrap_err();
        assert_eq!(err.code, METHOD_NOT_FOUND);
        assert!(err.message.contains("Unknown tool"));
    }

    // ══════════════════════════════════════════════════════════════
    // B: tools/call integration (safe echo)
    // ══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn tools_call_echo_returns_content() {
        let r = req(
            "tools/call",
            Some(json!(1)),
            Some(json!({
                "name": "execute_command",
                "arguments": {"command": "echo hello"}
            })),
        );
        let result = handle_method(&r).await.unwrap();
        let content = result["content"].as_array().unwrap();
        let has_hello = content.iter().any(|c| {
            c["type"] == "text" && c["text"].as_str().is_some_and(|t| t.contains("hello"))
        });
        assert!(
            has_hello,
            "Expected content containing 'hello', got: {}",
            result
        );
    }

    // ══════════════════════════════════════════════════════════════
    // B: notification / ping
    // ══════════════════════════════════════════════════════════════

    #[tokio::test]
    async fn notification_initialized_returns_null() {
        let r = req("notifications/initialized", None, None);
        let result = handle_method(&r).await.unwrap();
        assert!(result.is_null());
    }

    #[tokio::test]
    async fn ping_returns_empty_object() {
        let r = req("ping", Some(json!(1)), None);
        let result = handle_method(&r).await.unwrap();
        assert_eq!(result, json!({}));
    }

    #[tokio::test]
    async fn notification_without_id_handler_still_processes() {
        // handle_method always returns a result regardless of id presence.
        // The message_handler caller is responsible for suppressing the
        // response when request.id is None.
        let r = req("ping", None, None);
        let result = handle_method(&r).await.unwrap();
        assert_eq!(result, json!({}));
    }

    // ══════════════════════════════════════════════════════════════
    // C: extract_bearer_token
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn extract_valid_bearer() {
        let h = bearer_headers(Some("my-secret-token"));
        assert_eq!(
            extract_bearer_token(&h),
            Some("my-secret-token".to_string())
        );
    }

    #[test]
    fn extract_missing_header() {
        let h = HeaderMap::new();
        assert_eq!(extract_bearer_token(&h), None);
    }

    #[test]
    fn extract_no_bearer_prefix() {
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Basic abc123"),
        );
        assert_eq!(extract_bearer_token(&h), None);
    }

    #[test]
    fn extract_bearer_empty_token() {
        let mut h = HeaderMap::new();
        h.insert(header::AUTHORIZATION, HeaderValue::from_static("Bearer "));
        assert_eq!(extract_bearer_token(&h), Some("".to_string()));
    }

    #[test]
    fn extract_bearer_case_sensitive() {
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("bearer token123"),
        );
        assert_eq!(extract_bearer_token(&h), None);
    }

    #[test]
    fn extract_bearer_leading_whitespace_in_value() {
        let mut h = HeaderMap::new();
        h.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer  token"), // double space
        );
        assert_eq!(extract_bearer_token(&h), Some(" token".to_string()));
    }

    // ══════════════════════════════════════════════════════════════
    // C: verify_token
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn verify_correct_token() {
        let state = make_state("secret");
        let h = bearer_headers(Some("secret"));
        assert!(verify_token(&state, &h));
    }

    #[test]
    fn verify_wrong_token() {
        let state = make_state("secret");
        let h = bearer_headers(Some("wrong"));
        assert!(!verify_token(&state, &h));
    }

    #[test]
    fn verify_no_header() {
        let state = make_state("secret");
        let h = HeaderMap::new();
        assert!(!verify_token(&state, &h));
    }

    // ══════════════════════════════════════════════════════════════
    // C: parse_id_from_body
    // ══════════════════════════════════════════════════════════════

    #[test]
    fn parse_id_string() {
        let body = r#"{"jsonrpc":"2.0","id":"req-1","method":"ping"}"#;
        assert_eq!(
            parse_id_from_body(body),
            Some(Value::String("req-1".to_string()))
        );
    }

    #[test]
    fn parse_id_number() {
        let body = r#"{"jsonrpc":"2.0","id":42,"method":"ping"}"#;
        assert_eq!(parse_id_from_body(body), Some(serde_json::json!(42)));
    }

    #[test]
    fn parse_id_null() {
        // JSON-RPC permits null ids, so parse_id_from_body preserves "id": null.
        // JsonRpcRequest deserialization follows the same behavior.
        let body = r#"{"jsonrpc":"2.0","id":null,"method":"ping"}"#;
        assert_eq!(parse_id_from_body(body), Some(Value::Null));
    }

    #[test]
    fn parse_id_missing() {
        let body = r#"{"jsonrpc":"2.0","method":"ping"}"#;
        assert_eq!(parse_id_from_body(body), None);
    }

    #[test]
    fn parse_id_malformed_json() {
        let body = r#"not json at all"#;
        assert_eq!(parse_id_from_body(body), None);
    }

    #[test]
    fn parse_id_empty_body() {
        assert_eq!(parse_id_from_body(""), None);
    }

    #[test]
    fn parse_id_truncated_json() {
        let body = r#"{"jsonrpc":"2.0","id":"#;
        assert_eq!(parse_id_from_body(body), None);
    }

    #[test]
    fn parse_id_array_body() {
        // JSON array, not object — .get("id") on array returns None
        let body = r#"[1,2,3]"#;
        assert_eq!(parse_id_from_body(body), None);
    }

    #[test]
    fn parse_id_id_is_object() {
        let body = r#"{"jsonrpc":"2.0","id":{"nested":"val"},"method":"ping"}"#;
        assert_eq!(
            parse_id_from_body(body),
            Some(serde_json::json!({"nested": "val"}))
        );
    }
}
