//! Process-level lifecycle checks for `serve --stdio` over real pipes (no
//! PTY): offline handshake, protocol shutdown, stdin EOF, and SIGINT/SIGTERM
//! on Unix. The device endpoint is unreachable on purpose — startup and
//! shutdown must not depend on connectivity.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_recorder-for-jetkvm");
const DEADLINE: Duration = Duration::from_secs(30);

struct Server {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Receiver<std::io::Result<String>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn read_output(stdout: ChildStdout) -> Receiver<std::io::Result<String>> {
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => return,
                Ok(_) => {
                    if tx.send(Ok(line)).is_err() {
                        return;
                    }
                }
                Err(error) => {
                    let _ = tx.send(Err(error));
                    return;
                }
            }
        }
    });
    rx
}

fn spawn_server() -> Server {
    let mut child = Command::new(BIN)
        .args(["serve", "--stdio", "--host", "http://127.0.0.1:1"])
        .env("JETKVM_PASSWORD", "test-password")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn recorder-for-jetkvm");
    let stdin = child.stdin.take().expect("stdin pipe");
    let stdout = read_output(child.stdout.take().expect("stdout pipe"));
    let mut server = Server {
        child,
        stdin: Some(stdin),
        stdout,
    };
    request(
        &mut server,
        1,
        "hello",
        serde_json::json!({ "protocol_version": 2 }),
    );
    let hello = response(&mut server, 1);
    assert!(
        hello.get("result").is_some(),
        "hello must succeed while the device is offline: {hello}"
    );
    let status_state = hello["result"]["status"]["state"].as_str().unwrap_or("");
    assert!(
        status_state == "connecting" || status_state == "reconnecting",
        "unexpected offline startup state: {status_state}"
    );
    server
}

fn request(server: &mut Server, id: u64, method: &str, params: serde_json::Value) {
    let line = serde_json::json!({ "id": id, "method": method, "params": params });
    let stdin = server.stdin.as_mut().expect("stdin remains open");
    writeln!(stdin, "{line}").expect("write request");
    stdin.flush().expect("flush request");
}

/// Reads until the response for `id` arrives; asserts every stdout line is
/// valid NDJSON (events are skipped).
fn response(server: &mut Server, id: u64) -> serde_json::Value {
    let started = std::time::Instant::now();
    loop {
        let remaining = DEADLINE
            .checked_sub(started.elapsed())
            .unwrap_or(Duration::ZERO);
        let line = server
            .stdout
            .recv_timeout(remaining)
            .unwrap_or_else(|error| panic!("response {id} timed out or stdout closed: {error}"))
            .expect("read stdout");
        let value: serde_json::Value =
            serde_json::from_str(line.trim()).expect("stdout line is not valid NDJSON");
        if value.get("event").is_some() {
            continue;
        }
        if value.get("id") == Some(&serde_json::json!(id)) {
            return value;
        }
    }
}

fn expect_clean_exit(mut server: Server, context: &str) {
    drop(server.stdin.take());
    let started = std::time::Instant::now();
    loop {
        match server.child.try_wait().expect("wait on child") {
            Some(status) => {
                assert!(status.success(), "{context}: process exited with {status}");
                return;
            }
            None => {
                assert!(
                    started.elapsed() < DEADLINE,
                    "{context}: process did not exit within {DEADLINE:?}"
                );
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

#[test]
fn recorder_worker_failure_exits_nonzero_promptly() {
    let mut child = Command::new(BIN)
        .args([
            "--host",
            "http://127.0.0.1:1",
            "--output-dir",
            "/proc/recorder-for-jetkvm-test",
        ])
        .env("JETKVM_PASSWORD", "test-password")
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn recorder");
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("wait on recorder") {
            assert!(!status.success(), "worker failure must exit nonzero");
            break;
        }
        if started.elapsed() >= DEADLINE {
            let _ = child.kill();
            let _ = child.wait();
            panic!("recorder did not exit after its worker failed");
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn unsupported_protocol_version_is_rejected_cleanly() {
    let mut child = Command::new(BIN)
        .args(["serve", "--stdio", "--host", "http://127.0.0.1:1"])
        .env("JETKVM_PASSWORD", "test-password")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn recorder-for-jetkvm");
    let stdin = child.stdin.take().expect("stdin pipe");
    let stdout = read_output(child.stdout.take().expect("stdout pipe"));
    let mut server = Server {
        stdin: Some(stdin),
        stdout,
        child,
    };
    request(
        &mut server,
        1,
        "hello",
        serde_json::json!({ "protocol_version": 1 }),
    );
    let rejected = response(&mut server, 1);
    assert_eq!(
        rejected["error"]["code"].as_str(),
        Some("unsupported_protocol"),
        "protocol v1 must be rejected: {rejected}"
    );
    // A rejection is not fatal: the correct version still handshakes.
    request(
        &mut server,
        2,
        "hello",
        serde_json::json!({ "protocol_version": 2 }),
    );
    assert!(
        response(&mut server, 2).get("result").is_some(),
        "v2 hello must succeed after a rejected v1 attempt"
    );
    expect_clean_exit(server, "version rejection");
}

#[test]
fn protocol_shutdown_exits_cleanly_while_offline() {
    let mut server = spawn_server();
    request(&mut server, 2, "shutdown", serde_json::json!({}));
    let shutdown = response(&mut server, 2);
    assert!(
        shutdown.get("result").is_some(),
        "shutdown failed: {shutdown}"
    );
    expect_clean_exit(server, "protocol shutdown");
}
#[test]
fn protocol_shutdown_preempts_a_pending_connect() {
    let mut server = spawn_server();
    request(&mut server, 2, "connect", serde_json::json!({}));
    request(&mut server, 3, "shutdown", serde_json::json!({}));
    let shutdown = response(&mut server, 3);
    assert!(
        shutdown.get("result").is_some(),
        "shutdown failed behind a pending connect: {shutdown}"
    );
    expect_clean_exit(server, "preemptive protocol shutdown");
}

#[test]
fn stdin_eof_exits_cleanly_while_offline() {
    let server = spawn_server();
    expect_clean_exit(server, "stdin EOF");
}

#[cfg(unix)]
#[test]
fn sigterm_exits_cleanly_while_offline() {
    let server = spawn_server();
    let pid = server.child.id().to_string();
    let status = Command::new("kill")
        .args(["-TERM", &pid])
        .status()
        .expect("run kill");
    assert!(status.success(), "kill -TERM failed");
    expect_clean_exit(server, "SIGTERM");
}

#[cfg(unix)]
#[test]
fn sigint_exits_cleanly_while_offline() {
    let server = spawn_server();
    let pid = server.child.id().to_string();
    let status = Command::new("kill")
        .args(["-INT", &pid])
        .status()
        .expect("run kill");
    assert!(status.success(), "kill -INT failed");
    expect_clean_exit(server, "SIGINT");
}

#[cfg(unix)]
#[test]
fn repeated_signals_remain_idempotent() {
    let server = spawn_server();
    let pid = server.child.id().to_string();
    for _ in 0..2 {
        let _ = Command::new("kill").args(["-TERM", &pid]).status();
        std::thread::sleep(Duration::from_millis(100));
    }
    expect_clean_exit(server, "repeated SIGTERM");
}
