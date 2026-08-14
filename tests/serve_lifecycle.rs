//! Process-level lifecycle checks for `serve --stdio` over real pipes (no
//! PTY): offline handshake, protocol shutdown, stdin EOF, and SIGINT/SIGTERM
//! on Unix. The device endpoint is unreachable on purpose — startup and
//! shutdown must not depend on connectivity.

use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver};
use std::time::Duration;

const BIN: &str = env!("CARGO_BIN_EXE_recorder-for-jetkvm");
const DEADLINE: Duration = Duration::from_secs(30);

struct Server {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: Receiver<std::io::Result<String>>,
    stderr: Receiver<std::io::Result<String>>,
}

struct HangingHttpServer {
    host: String,
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

impl HangingHttpServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind hanging HTTP endpoint");
        listener
            .set_nonblocking(true)
            .expect("make hanging endpoint nonblocking");
        let host = format!(
            "http://{}",
            listener.local_addr().expect("listener address")
        );
        let stop = Arc::new(AtomicBool::new(false));
        let worker_stop = Arc::clone(&stop);
        let worker = std::thread::spawn(move || {
            let mut connections = Vec::new();
            while !worker_stop.load(Ordering::Acquire) {
                match listener.accept() {
                    Ok((connection, _)) => connections.push(connection),
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => return,
                }
            }
        });
        Self {
            host,
            stop,
            worker: Some(worker),
        }
    }
}

impl Drop for HangingHttpServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().expect("join hanging endpoint");
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
    }
}

fn read_output<R>(output: R) -> Receiver<std::io::Result<String>>
where
    R: Read + Send + 'static,
{
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let mut reader = BufReader::new(output);
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
    spawn_server_at("http://127.0.0.1:1")
}

fn spawn_server_at(host: &str) -> Server {
    let mut child = Command::new(BIN)
        .args(["serve", "--stdio", "--host", host])
        .env("JETKVM_PASSWORD", "test-password")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn recorder-for-jetkvm");
    let stdin = child.stdin.take().expect("stdin pipe");
    let stdout = read_output(child.stdout.take().expect("stdout pipe"));
    let stderr = read_output(child.stderr.take().expect("stderr pipe"));
    let mut server = Server {
        child,
        stdin: Some(stdin),
        stdout,
        stderr,
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
        "hello must succeed while authentication is unresolved: {hello}"
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

fn wait_for_exit(child: &mut Child, context: &str) -> std::process::ExitStatus {
    let started = std::time::Instant::now();
    loop {
        if let Some(status) = child.try_wait().expect("wait on child") {
            return status;
        }
        assert!(
            started.elapsed() < DEADLINE,
            "{context}: process did not exit within {DEADLINE:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn expect_clean_exit(mut server: Server, context: &str) -> String {
    drop(server.stdin.take());
    let status = wait_for_exit(&mut server.child, context);
    assert!(status.success(), "{context}: process exited with {status}");

    while let Ok(line) = server.stdout.recv_timeout(Duration::from_millis(50)) {
        let line = line.expect("read remaining stdout");
        serde_json::from_str::<serde_json::Value>(line.trim())
            .expect("remaining stdout line is not valid NDJSON");
    }
    let mut stderr = String::new();
    while let Ok(line) = server.stderr.recv_timeout(Duration::from_millis(50)) {
        stderr.push_str(&line.expect("read stderr"));
    }
    stderr
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
        .stderr(Stdio::piped())
        .spawn()
        .expect("failed to spawn recorder-for-jetkvm");
    let stdin = child.stdin.take().expect("stdin pipe");
    let stdout = read_output(child.stdout.take().expect("stdout pipe"));
    let stderr = read_output(child.stderr.take().expect("stderr pipe"));
    let mut server = Server {
        stdin: Some(stdin),
        stdout,
        stderr,
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
fn closed_stdout_stops_server_promptly() {
    let mut child = Command::new(BIN)
        .args(["serve", "--stdio", "--host", "http://127.0.0.1:1"])
        .env("JETKVM_PASSWORD", "test-password")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn server with closed stdout");
    let mut stdin = child.stdin.take().expect("stdin pipe");
    drop(child.stdout.take());

    writeln!(
        stdin,
        "{}",
        serde_json::json!({
            "id": 1,
            "method": "hello",
            "params": { "protocol_version": 2 },
        })
    )
    .expect("write hello");
    stdin.flush().expect("flush hello");

    let status = wait_for_exit(&mut child, "closed stdout");
    assert!(
        !status.success(),
        "protocol output failure must produce a nonzero exit"
    );
}

#[test]
fn repeated_hello_obeys_duplicate_id_protection() {
    let endpoint = HangingHttpServer::start();
    let mut server = spawn_server_at(&endpoint.host);
    request(&mut server, 2, "connect", serde_json::json!({}));
    request(
        &mut server,
        2,
        "hello",
        serde_json::json!({ "protocol_version": 2 }),
    );

    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    let duplicate = loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let line = server
            .stdout
            .recv_timeout(remaining)
            .expect("duplicate response deadline")
            .expect("read duplicate response");
        let value: serde_json::Value =
            serde_json::from_str(line.trim()).expect("stdout line is valid NDJSON");
        if value.get("event").is_none() {
            break value;
        }
    };
    assert_eq!(duplicate["id"], serde_json::Value::Null);
    assert_eq!(duplicate["error"]["code"], "duplicate_request_id");

    request(&mut server, 3, "shutdown", serde_json::json!({}));
    assert!(response(&mut server, 3).get("result").is_some());
    expect_clean_exit(server, "repeated hello");
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
fn saturated_ordinary_queue_reserves_status_and_shutdown_admission() {
    let endpoint = HangingHttpServer::start();
    let mut server = spawn_server_at(&endpoint.host);

    for id in 100..164 {
        request(&mut server, id, "connect", serde_json::json!({}));
    }
    request(&mut server, 100, "status", serde_json::json!({}));
    let status_started = std::time::Instant::now();
    request(&mut server, 1_000, "status", serde_json::json!({}));
    request(&mut server, 2_000, "connect", serde_json::json!({}));

    let mut duplicate = None;
    let mut status = None;
    let mut status_latency = None;
    let mut busy = None;
    let deadline = std::time::Instant::now() + Duration::from_secs(2);
    while duplicate.is_none() || status.is_none() || busy.is_none() {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let line = server
            .stdout
            .recv_timeout(remaining)
            .expect("saturated control response deadline")
            .expect("read saturated stdout");
        let value: serde_json::Value =
            serde_json::from_str(line.trim()).expect("stdout line is valid NDJSON");
        if value.get("event").is_some() {
            continue;
        }
        match value.get("id") {
            Some(serde_json::Value::Null) if value["error"]["code"] == "duplicate_request_id" => {
                duplicate = Some(value);
            }
            Some(id) if id == &serde_json::json!(1_000) => {
                let elapsed = status_started.elapsed();
                assert!(
                    elapsed < Duration::from_millis(250),
                    "status at ordinary capacity took {elapsed:?}"
                );
                status_latency = Some(elapsed);
                status = Some(value);
            }
            Some(id) if id == &serde_json::json!(2_000) => busy = Some(value),
            _ => {}
        }
    }
    println!(
        "saturated_status_latency_us={}",
        status_latency.expect("status latency").as_micros()
    );

    assert!(
        status.expect("status response").get("result").is_some(),
        "status must succeed at ordinary capacity"
    );
    assert_eq!(
        busy.expect("busy response")["error"]["code"],
        "server_busy",
        "a sixty-fifth ordinary request must be rejected"
    );

    request(&mut server, 3_000, "shutdown", serde_json::json!({}));
    let shutdown = response(&mut server, 3_000);
    assert!(
        shutdown.get("result").is_some(),
        "shutdown must be admitted at ordinary capacity: {shutdown}"
    );
    let stderr = expect_clean_exit(server, "saturated protocol shutdown");
    assert!(
        !stderr.contains("test-password"),
        "stderr leaked the seeded password"
    );
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
fn send_signal(child: &Child, signal: libc::c_int) -> std::io::Result<()> {
    // SAFETY: `child.id()` is a live child-process PID and `signal` is one of
    // the platform signal constants used below.
    if unsafe { libc::kill(child.id() as libc::pid_t, signal) } == 0 {
        Ok(())
    } else {
        Err(std::io::Error::last_os_error())
    }
}

#[cfg(unix)]
#[test]
fn sigterm_exits_cleanly_while_offline() {
    let server = spawn_server();
    send_signal(&server.child, libc::SIGTERM).expect("send SIGTERM");
    expect_clean_exit(server, "SIGTERM");
}

#[cfg(unix)]
#[test]
fn sigint_exits_cleanly_while_offline() {
    let server = spawn_server();
    send_signal(&server.child, libc::SIGINT).expect("send SIGINT");
    expect_clean_exit(server, "SIGINT");
}

#[cfg(unix)]
#[test]
fn repeated_signals_remain_idempotent() {
    let server = spawn_server();
    for _ in 0..2 {
        let _ = send_signal(&server.child, libc::SIGTERM);
        std::thread::sleep(Duration::from_millis(100));
    }
    expect_clean_exit(server, "repeated SIGTERM");
}
