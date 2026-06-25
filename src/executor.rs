use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;
use tokio::task::JoinHandle;
use tokio::time::timeout;

pub const DEFAULT_TIMEOUT_SECS: u64 = 30;
pub const MAX_TIMEOUT_SECS: u64 = 300;
pub const MAX_OUTPUT_BYTES: usize = 1024 * 1024;
const OUTPUT_DRAIN_TIMEOUT: Duration = Duration::from_secs(2);

#[cfg(unix)]
const SIGKILL: i32 = 9;

#[cfg(unix)]
unsafe extern "C" {
    fn setpgid(pid: i32, pgid: i32) -> i32;
    fn kill(pid: i32, sig: i32) -> i32;
}

/// Execution result used as MCP tool call content.
#[derive(Debug)]
pub struct ExecResult {
    pub stdout: String,
    pub stderr: String,
    pub warnings: Vec<String>,
    pub exit_code: Option<i32>,
    pub timed_out: bool,
    pub stdout_truncated: bool,
    pub stderr_truncated: bool,
}

#[derive(Default)]
struct OutputBuffer {
    bytes: Vec<u8>,
    truncated: bool,
}

type SharedOutput = Arc<Mutex<OutputBuffer>>;

enum OutputIssue {
    Warning(String),
    Error(String),
}

/// Execute a bash command with an optional timeout (default 30 s).
pub async fn execute_bash(cmd: &str, timeout_secs: u64) -> ExecResult {
    let timeout_secs = timeout_secs.clamp(1, MAX_TIMEOUT_SECS);
    let mut command = Command::new("/bin/bash");
    command
        .arg("-c")
        .arg(cmd)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    configure_process_group(&mut command);

    let mut child = match command.spawn() {
        Ok(child) => child,
        Err(e) => {
            return ExecResult {
                stdout: String::new(),
                stderr: format!("failed to spawn bash: {}", e),
                warnings: Vec::new(),
                exit_code: None,
                timed_out: false,
                stdout_truncated: false,
                stderr_truncated: false,
            }
        }
    };
    let child_pid = child.id();

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();
    let stdout_buffer = Arc::new(Mutex::new(OutputBuffer::default()));
    let stderr_buffer = Arc::new(Mutex::new(OutputBuffer::default()));
    let stdout_reader_buffer = stdout_buffer.clone();
    let stderr_reader_buffer = stderr_buffer.clone();
    let stdout_task = tokio::spawn(async move {
        match stdout {
            Some(stdout) => read_capped(stdout, MAX_OUTPUT_BYTES, stdout_reader_buffer).await,
            None => Ok(()),
        }
    });
    let stderr_task = tokio::spawn(async move {
        match stderr {
            Some(stderr) => read_capped(stderr, MAX_OUTPUT_BYTES, stderr_reader_buffer).await,
            None => Ok(()),
        }
    });

    match timeout(Duration::from_secs(timeout_secs), child.wait()).await {
        Ok(Ok(status)) => {
            let (stdout, stdout_truncated, stdout_error) =
                collect_output(stdout_task, stdout_buffer, OUTPUT_DRAIN_TIMEOUT).await;
            let (mut stderr, stderr_truncated, stderr_error) =
                collect_output(stderr_task, stderr_buffer, OUTPUT_DRAIN_TIMEOUT).await;
            let mut warnings = Vec::new();
            append_output_issue(&mut stderr, &mut warnings, stdout_error);
            append_output_issue(&mut stderr, &mut warnings, stderr_error);
            ExecResult {
                stdout,
                stderr,
                warnings,
                exit_code: status.code(),
                timed_out: false,
                stdout_truncated,
                stderr_truncated,
            }
        }
        Ok(Err(e)) => {
            kill_process_group(child_pid);
            let _ = child.kill().await;
            let (stdout, stdout_truncated, stdout_error) =
                collect_output(stdout_task, stdout_buffer, OUTPUT_DRAIN_TIMEOUT).await;
            let (mut stderr, stderr_truncated, stderr_error) =
                collect_output(stderr_task, stderr_buffer, OUTPUT_DRAIN_TIMEOUT).await;
            let mut warnings = Vec::new();
            append_output_issue(&mut stderr, &mut warnings, stdout_error);
            append_output_issue(&mut stderr, &mut warnings, stderr_error);
            if !stderr.is_empty() {
                stderr.push('\n');
            }
            stderr.push_str(&format!("failed to wait for bash: {}", e));
            ExecResult {
                stdout,
                stderr,
                warnings,
                exit_code: None,
                timed_out: false,
                stdout_truncated,
                stderr_truncated,
            }
        }
        Err(_elapsed) => {
            kill_process_group(child_pid);
            let _ = child.kill().await;
            let _ = child.wait().await;
            let (stdout, stdout_truncated, stdout_error) =
                collect_output(stdout_task, stdout_buffer, OUTPUT_DRAIN_TIMEOUT).await;
            let (mut stderr, stderr_truncated, stderr_error) =
                collect_output(stderr_task, stderr_buffer, OUTPUT_DRAIN_TIMEOUT).await;
            let mut warnings = Vec::new();
            append_output_issue(&mut stderr, &mut warnings, stdout_error);
            append_output_issue(&mut stderr, &mut warnings, stderr_error);
            if !stderr.is_empty() {
                stderr.push('\n');
            }
            stderr.push_str(&format!("command timed out after {} seconds", timeout_secs));
            ExecResult {
                stdout,
                stderr,
                warnings,
                exit_code: None,
                timed_out: true,
                stdout_truncated,
                stderr_truncated,
            }
        }
    }
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    unsafe {
        command.pre_exec(|| {
            if setpgid(0, 0) == -1 {
                return Err(std::io::Error::last_os_error());
            }
            Ok(())
        });
    }
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn kill_process_group(child_pid: Option<u32>) {
    if let Some(pid) = child_pid.and_then(|pid| i32::try_from(pid).ok()) {
        unsafe {
            kill(-pid, SIGKILL);
        }
    }
}

#[cfg(not(unix))]
fn kill_process_group(_child_pid: Option<u32>) {}

async fn read_capped<R>(mut reader: R, limit: usize, output: SharedOutput) -> std::io::Result<()>
where
    R: AsyncRead + Unpin,
{
    let mut buf = [0_u8; 8192];

    loop {
        let n = reader.read(&mut buf).await?;
        if n == 0 {
            break;
        }

        let mut output = output.lock().unwrap();
        let remaining = limit.saturating_sub(output.bytes.len());
        if remaining == 0 {
            output.truncated = true;
            continue;
        }

        let take = remaining.min(n);
        output.bytes.extend_from_slice(&buf[..take]);
        if take < n {
            output.truncated = true;
        }
    }

    Ok(())
}

async fn collect_output(
    mut task: JoinHandle<std::io::Result<()>>,
    output: SharedOutput,
    drain_timeout: Duration,
) -> (String, bool, Option<OutputIssue>) {
    let result = match timeout(drain_timeout, &mut task).await {
        Ok(result) => result,
        Err(_) => {
            task.abort();
            let (text, truncated) = snapshot_output(&output);
            return (
                text,
                truncated,
                Some(OutputIssue::Warning(format!(
                    "process output did not close within {} seconds",
                    drain_timeout.as_secs()
                ))),
            );
        }
    };

    match result {
        Ok(Ok(())) => {
            let (text, truncated) = snapshot_output(&output);
            (text, truncated, None)
        }
        Ok(Err(e)) => {
            let (text, truncated) = snapshot_output(&output);
            (text, truncated, Some(OutputIssue::Error(e.to_string())))
        }
        Err(e) => {
            let (text, truncated) = snapshot_output(&output);
            (text, truncated, Some(OutputIssue::Error(e.to_string())))
        }
    }
}

fn snapshot_output(output: &SharedOutput) -> (String, bool) {
    let output = output.lock().unwrap();
    (
        String::from_utf8_lossy(&output.bytes).to_string(),
        output.truncated,
    )
}

fn append_output_issue(
    stderr: &mut String,
    warnings: &mut Vec<String>,
    issue: Option<OutputIssue>,
) {
    if let Some(issue) = issue {
        match issue {
            OutputIssue::Warning(message) => warnings.push(message),
            OutputIssue::Error(message) => {
                if !stderr.is_empty() {
                    stderr.push('\n');
                }
                stderr.push_str(&format!("failed to read process output: {}", message));
            }
        }
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

    if r.stdout_truncated {
        content.push(json!({
            "type": "text",
            "text": format!("[stdout truncated after {} bytes]", MAX_OUTPUT_BYTES)
        }));
    }

    if !r.stderr.is_empty() {
        content.push(json!({
            "type": "text",
            "text": format!("[stderr]\n{}", r.stderr)
        }));
    }

    if r.stderr_truncated {
        content.push(json!({
            "type": "text",
            "text": format!("[stderr truncated after {} bytes]", MAX_OUTPUT_BYTES)
        }));
    }

    for warning in &r.warnings {
        content.push(json!({
            "type": "text",
            "text": format!("[warning]\n{}", warning)
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
            warnings: Vec::new(),
            exit_code,
            timed_out,
            stdout_truncated: false,
            stderr_truncated: false,
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

    #[test]
    fn truncated_output_adds_marker() {
        let mut r = result_for("partial", "", None, false);
        r.stdout_truncated = true;
        let v = to_mcp_content(&r);
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0]["text"], "partial");
        assert!(arr[1]["text"]
            .as_str()
            .unwrap()
            .contains("stdout truncated"));
    }

    #[tokio::test]
    async fn background_process_holding_pipe_does_not_block_result() {
        let start = std::time::Instant::now();
        let result = execute_bash("sleep 30 & echo done", 1).await;

        assert!(
            start.elapsed() < std::time::Duration::from_secs(5),
            "command should not wait for background process to close inherited pipes"
        );
        assert!(!result.timed_out);
        assert!(result.stdout.contains("done"));
        assert!(result.stderr.is_empty());
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("process output did not close")));
    }

    #[tokio::test]
    async fn detached_background_process_survives_normal_shell_exit() {
        let marker =
            std::env::temp_dir().join(format!("remote-bash-detached-{}.txt", std::process::id()));
        let _ = std::fs::remove_file(&marker);

        let command = format!(
            "(sleep 1; echo survived > {}) >/dev/null 2>&1 & echo started",
            marker.display()
        );
        let result = execute_bash(&command, 5).await;
        assert!(!result.timed_out);
        assert!(result.stdout.contains("started"));

        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        let marker_contents = std::fs::read_to_string(&marker).unwrap_or_default();
        let _ = std::fs::remove_file(&marker);

        assert_eq!(marker_contents.trim(), "survived");
    }
}
