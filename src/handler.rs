use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event, Sse},
};
use futures::stream::Stream;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::convert::Infallible;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_stream::StreamExt;
use tracing;
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
    let Some(ref id) = id else { return };
    let resp = JsonRpcErrorResponse::new(Some(id.clone()), code, message.to_string());
    if let Ok(json) = serde_json::to_string(&resp) {
        // fire-and-forget: if session is gone the send just fails silently
        let sessions = sessions.clone();
        let msg = json;
        tokio::spawn(async move {
            session::send_to_session(&sessions, &sid, msg).await;
        });
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

        let mut rx_stream = UnboundedReceiverStream::new(rx);
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
        push_error(&state.sessions, sid, id, UNAUTHORIZED, "Unauthorized: invalid token");
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
        push_error(&state.sessions, Some(sid), id, SESSION_NOT_FOUND, "Session not found");
        return StatusCode::ACCEPTED;
    }

    // 4. parse JSON-RPC
    let request: JsonRpcRequest = match serde_json::from_str(&body) {
        Ok(r) => r,
        Err(e) => {
            tracing::warn!(%body, "JSON-RPC parse error: {}", e);
            let id = parse_id_from_body(&body);
            push_error(&state.sessions, Some(sid), id, PARSE_ERROR, &format!("Parse error: {}", e));
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
                        session::send_to_session(&sessions, &sid2, json).await;
                    }
                }
            }
            Err(e) => {
                if let Some(ref id) = request.id {
                    let err_resp = JsonRpcErrorResponse::new(
                        Some(id.clone()),
                        e.code,
                        e.message,
                    );
                    if let Ok(json) = serde_json::to_string(&err_resp) {
                        session::send_to_session(&sessions, &sid2, json).await;
                    }
                }
            }
        }
    });

    StatusCode::ACCEPTED
}

// ── method dispatch ──

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
                            "default": 30
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

    let args = params
        .get("arguments")
        .ok_or_else(|| HandlerError {
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

    let timeout_secs = args
        .get("timeout")
        .and_then(|v| v.as_u64())
        .unwrap_or(30);

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
    } else if result.exit_code.map_or(false, |c| c != 0) {
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
