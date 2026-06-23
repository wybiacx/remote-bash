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
            "text": "[timed out]"
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

#[cfg(test)]
mod tests {
    use super::*;

    fn result_for(
        stdout: &str,
        stderr: &str,
        exit_code: Option<i32>,
        timed_out: bool,
    ) -> ExecResult {
        ExecResult {
            stdout: stdout.to_string(),
            stderr: stderr.to_string(),
            exit_code,
            timed_out,
        }
    }

    // ── stdout-only ──
    #[test]
    fn stdout_only_produces_single_text_block() {
        let r = result_for("hello", "", Some(0), false);
        let v = to_mcp_content(&r);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 2); // stdout + exit code
        assert_eq!(arr[0]["type"], "text");
        assert_eq!(arr[0]["text"], "hello");
        assert_eq!(arr[1]["text"], "[exit code: 0]");
    }

    // ── stderr-only ──
    #[test]
    fn stderr_only_produces_tagged_block() {
        let r = result_for("", "oops", Some(2), false);
        let v = to_mcp_content(&r);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 2); // stderr + exit code
        assert_eq!(arr[0]["type"], "text");
        assert!(arr[0]["text"].as_str().unwrap().starts_with("[stderr]\n"));
        assert!(arr[0]["text"].as_str().unwrap().contains("oops"));
        assert_eq!(arr[1]["text"], "[exit code: 2]");
    }

    // ── both stdout and stderr ──
    #[test]
    fn stdout_and_stderr_both_included() {
        let r = result_for("A", "B", None, false);
        let v = to_mcp_content(&r);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 2); // stdout + stderr (no exit code)
        assert_eq!(arr[0]["text"], "A");
        assert!(arr[1]["text"].as_str().unwrap().contains("B"));
    }

    // ── timed_out ──
    #[test]
    fn timed_out_includes_tag_and_no_exit_code() {
        let r = result_for("", "command timed out after 5 seconds", None, true);
        let v = to_mcp_content(&r);
        let arr = v.as_array().unwrap();
        // stderr + timed_out tag, no exit code
        assert_eq!(arr.len(), 2);
        assert!(arr[0]["text"].as_str().unwrap().contains("timed out"));
        assert_eq!(arr[1]["text"], "[timed out]");
    }

    #[test]
    fn timed_out_with_stdout_produces_all_three() {
        let r = result_for(
            "partial output",
            "command timed out after 5 seconds",
            None,
            true,
        );
        let v = to_mcp_content(&r);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 3); // stdout + stderr + timed_out
        assert_eq!(arr[0]["text"], "partial output");
        assert_eq!(arr[2]["text"], "[timed out]");
    }

    // ── exit code cases ──
    #[test]
    fn exit_code_zero_shown() {
        let r = result_for("ok", "", Some(0), false);
        let v = to_mcp_content(&r);
        let arr = v.as_array().unwrap();
        let last = arr.last().unwrap();
        assert_eq!(last["text"], "[exit code: 0]");
    }

    #[test]
    fn exit_code_nonzero_shown() {
        let r = result_for("", "fail", Some(127), false);
        let v = to_mcp_content(&r);
        let arr = v.as_array().unwrap();
        let last = arr.last().unwrap();
        assert_eq!(last["text"], "[exit code: 127]");
    }

    #[test]
    fn no_exit_code_omits_exit_code_block() {
        let r = result_for("out", "", None, false);
        let v = to_mcp_content(&r);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 1);
        for item in arr {
            assert!(!item["text"].as_str().unwrap().contains("exit code"));
        }
    }

    // ── empty result ──
    #[test]
    fn all_empty_produces_empty_array() {
        let r = result_for("", "", None, false);
        let v = to_mcp_content(&r);
        let arr = v.as_array().unwrap();
        assert!(arr.is_empty());
    }
}
