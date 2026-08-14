//! Process-level lifecycle checks for `serve --stdio` over real pipes (no
//! PTY): offline handshake, protocol shutdown, stdin EOF, and SIGINT/SIGTERM
//! on Unix. The device endpoint is unreachable on purpose — startup and
//! shutdown must not depend on connectivity.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_recorder-for-jetkvm");
const DEADLINE: Duration = Duration::from_secs(30);

struct Server {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<std::process::ChildStdout>,
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
    let stdout = BufReader::new(child.stdout.take().expect("stdout pipe"));
    let mut server = Server {
        child,
        stdin,
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
    writeln!(server.stdin, "{line}").expect("write request");
    server.stdin.flush().expect("flush request");
}

/// Reads until the response for `id` arrives; asserts every stdout line is
/// valid NDJSON (events are skipped).
fn response(server: &mut Server, id: u64) -> serde_json::Value {
    let started = Instant::now();
    loop {
        assert!(started.elapsed() < DEADLINE, "response {id} timed out");
        let mut line = String::new();
        let read = server.stdout.read_line(&mut line).expect("read stdout");
        assert!(read > 0, "stdout closed before response {id}");
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
    drop(server.stdin);
    let started = Instant::now();
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
fn unsupported_protocol_version_is_rejected_cleanly() {
    let mut child = Command::new(BIN)
        .args(["serve", "--stdio", "--host", "http://127.0.0.1:1"])
        .env("JETKVM_PASSWORD", "test-password")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn recorder-for-jetkvm");
    let mut server = Server {
        stdin: child.stdin.take().expect("stdin pipe"),
        stdout: BufReader::new(child.stdout.take().expect("stdout pipe")),
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
