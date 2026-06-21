use serde_json::{json, Value};
use std::time::Duration;
use tokio::process::Command;
use tokio::time::timeout;

/// Execution result used as MCP tool call content.
#[derive(Debug)]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
}

/// Execute a bash command with an optional timeout (default 30 s).
pub async fn execute_bash(cmd: &str, timeout_secs: u64) -> ExecResult {
    let fut = Command::new("/bin/bash")
        .arg("-c")
        .arg(cmd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .output();

    match timeout(Duration::from_secs(timeout_secs), fut).await {
        Ok(Ok(output)) => ExecResult {
            stdout: String::from_utf8_lossy(&output.stdout).to_string(),
            stderr: String::from_utf8_lossy(&output.stderr).to_string(),
            exit_code: output.status.code(),
            timed_out: false,
        },
        Ok(Err(e)) => ExecResult {
            stdout: String::new(),
            stderr: format!("failed to spawn bash: {}", e),
            exit_code: None,
            timed_out: false,
        },
        Err(_elapsed) => ExecResult {
            stdout: String::new(),
            stderr: format!("command timed out after {} seconds", timeout_secs),
            exit_code: None,
            timed_out: true,
        },
    }
}

/// Convert ExecResult into the MCP tool-result content array.
pub fn to_mcp_content(r: &ExecResult) -> Value {
    let mut content = Vec::new();

    if !r.stdout.is_empty() {
        content.push(json!({
            "type": "text",
            "text": r.stdout
        }));
    }

    if !r.stderr.is_empty() {
        content.push(json!({
            "type": "text",
            "text": format!("[stderr]\n{}", r.stderr)
        }));
    }

    if r.timed_out {
        content.push(json!({
            "type": "text",
            "text": format!("[timed out after {}s]", r.stderr)
        }));
    }

    if let Some(code) = r.exit_code {
        content.push(json!({
            "type": "text",
            "text": format!("[exit code: {}]", code)
        }));
    }

    json!(content)
}
