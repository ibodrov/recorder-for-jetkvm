use std::collections::HashMap;
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{Mutex, mpsc};
use tokio_util::sync::CancellationToken;

use crate::controller::{FrameCursor, JetKvmController, ScrollEvent, TypeTextRequest};
use crate::error::{CodedError, codes, error_code};
use crate::hid::{AbsoluteMouseEvent, RelativeMouseEvent};
use crate::rpc::VirtualMediaMode;
use crate::virtual_media::Approval;

pub const PROTOCOL_VERSION: u32 = 2;
const MAX_LINE_BYTES: usize = 1024 * 1024;
const OUTPUT_BUFFER: usize = 128;

const MAX_ACTIVE_REQUESTS: usize = 64;
const DISPATCH_BUFFER: usize = 64;
const SHUTDOWN_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);

#[derive(Debug, Deserialize)]
struct Request {
    id: Value,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct SuccessResponse {
    id: Value,
    result: Value,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    id: Value,
    error: ProtocolError,
}

#[derive(Debug, Serialize)]
struct ProtocolError {
    code: &'static str,
    message: String,
}

#[derive(Debug, Deserialize)]
struct HelloParams {
    protocol_version: u32,
}

#[derive(Debug, Deserialize)]
struct SnapshotParams {
    #[serde(default)]
    path: Option<PathBuf>,
    #[serde(default)]
    approved: bool,
    /// When set, wait for a frame strictly newer than this cursor.
    #[serde(default)]
    after: Option<FrameCursor>,
}

#[derive(Debug, Deserialize)]
struct UrlParams {
    url: String,
    #[serde(default)]
    approved: bool,
}

#[derive(Debug, Deserialize)]
struct MountUrlParams {
    url: String,
    mode: VirtualMediaMode,
    approved: bool,
}

#[derive(Debug, Deserialize)]
struct MountLocalParams {
    path: PathBuf,
    mode: VirtualMediaMode,
    approved: bool,
}

#[derive(Debug, Deserialize)]
struct ApprovalParams {
    approved: bool,
}

#[derive(Debug, Deserialize)]
struct UploadParams {
    path: PathBuf,
    filename: String,
    approved: bool,
}

#[derive(Debug, Deserialize)]
struct StorageFileParams {
    filename: String,
    #[serde(default)]
    mode: Option<VirtualMediaMode>,
    approved: bool,
}

#[derive(Debug, Deserialize)]
struct CancelParams {
    id: Value,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum MouseMoveParams {
    Absolute(AbsoluteMouseEvent),
    Relative(RelativeMouseEvent),
}

enum ReadLine {
    Line(Vec<u8>),
    Oversized,
    Eof,
}

/// What the protocol layer needs to know about an admitted request. Control
/// requests retain duplicate-ID protection but do not consume ordinary
/// capacity; the reader executes them inline, bounding them to one entry.
#[derive(Clone)]
enum ActiveKind {
    Upload(CancellationToken),
    Ordinary,
    Control,
}

impl ActiveKind {
    fn consumes_ordinary_capacity(&self) -> bool {
        matches!(self, Self::Upload(_) | Self::Ordinary)
    }
}

type ActiveMap = Arc<Mutex<HashMap<String, ActiveKind>>>;

async fn admit_request(
    active: &ActiveMap,
    key: String,
    kind: ActiveKind,
) -> Option<(&'static str, String)> {
    let mut active = active.lock().await;
    if active.contains_key(&key) {
        Some((
            "duplicate_request_id",
            "request id is already active".to_owned(),
        ))
    } else if kind.consumes_ordinary_capacity()
        && active
            .values()
            .filter(|active| active.consumes_ordinary_capacity())
            .count()
            >= MAX_ACTIVE_REQUESTS
    {
        Some((
            codes::SERVER_BUSY,
            format!("at most {MAX_ACTIVE_REQUESTS} ordinary requests may be active"),
        ))
    } else {
        active.insert(key, kind);
        None
    }
}

/// A state-changing request queued for the ordered dispatcher.
struct QueuedRequest {
    id: Value,
    method: String,
    params: Value,
    upload_cancellation: Option<CancellationToken>,
}

pub async fn serve_stdio(controller: JetKvmController) -> Result<()> {
    let (output_tx, mut output_rx) = mpsc::channel::<Value>(OUTPUT_BUFFER);
    let writer = tokio::spawn(async move {
        let stdout = tokio::io::stdout();
        let mut writer = BufWriter::new(stdout);
        while let Some(value) = output_rx.recv().await {
            let mut encoded =
                serde_json::to_vec(&value).context("failed to encode protocol output")?;
            encoded.push(b'\n');
            writer.write_all(&encoded).await?;
            writer.flush().await?;
        }
        Result::<()>::Ok(())
    });

    // Controller events are suppressed until the hello handshake completes,
    // so a client never observes an event before the handshake response.
    let handshake_done = Arc::new(AtomicBool::new(false));
    let event_handshake = Arc::clone(&handshake_done);
    let event_output = output_tx.clone();
    let mut events = controller.subscribe_events();
    let event_task = tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => {
                    if !event_handshake.load(Ordering::Acquire) {
                        continue;
                    }
                    if let Ok(value) = serde_json::to_value(event)
                        && event_output.send(value).await.is_err()
                    {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => continue,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return,
            }
        }
    });

    let (input_tx, mut input_rx) = mpsc::channel(8);
    std::thread::spawn(move || {
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();
        loop {
            let line = read_ndjson_line(&mut reader, MAX_LINE_BYTES);
            let finished = matches!(&line, Ok(ReadLine::Eof) | Err(_));
            if input_tx.blocking_send(line).is_err() || finished {
                return;
            }
        }
    });

    let active: ActiveMap = Arc::new(Mutex::new(HashMap::new()));
    let server_shutdown = CancellationToken::new();

    // SIGINT/SIGTERM take the same shutdown path as protocol shutdown and
    // stdin EOF: stop admitting work, cancel uploads, clean up the session.
    {
        let signal_shutdown = server_shutdown.clone();
        tokio::spawn(async move {
            wait_for_termination_signal().await;
            signal_shutdown.cancel();
        });
    }

    let (dispatch_tx, dispatch_rx) = mpsc::channel::<QueuedRequest>(DISPATCH_BUFFER);
    let dispatcher = {
        let dispatch_controller = controller.clone();
        let dispatch_output = output_tx.clone();
        let dispatch_active = Arc::clone(&active);
        tokio::spawn(async move {
            run_dispatcher(
                dispatch_rx,
                dispatch_output,
                dispatch_active,
                move |method, params, upload_cancellation| {
                    let controller = dispatch_controller.clone();
                    async move { dispatch(&controller, &method, params, upload_cancellation).await }
                },
            )
            .await;
        })
    };

    let mut handshake_complete = false;
    let mut shutdown_response: Option<(Value, String)> = None;

    loop {
        tokio::select! {
            _ = server_shutdown.cancelled() => break,
            line = input_rx.recv() => {
                let Some(line) = line else {
                    break;
                };
                match line? {
                    ReadLine::Eof => break,
                    ReadLine::Oversized => {
                        send_error(
                            &output_tx,
                            Value::Null,
                            "line_too_large",
                            format!("request line exceeds {MAX_LINE_BYTES} bytes"),
                        ).await?;
                    }
                    ReadLine::Line(line) => {
                        let request = match serde_json::from_slice::<Request>(&line) {
                            Ok(request) => request,
                            Err(_) => {
                                send_error(
                                    &output_tx,
                                    Value::Null,
                                    "malformed_json",
                                    "request is not valid JSON".to_owned(),
                                ).await?;
                                continue;
                            }
                        };
                        if !valid_id(&request.id) {
                            send_error(
                                &output_tx,
                                Value::Null,
                                "invalid_request_id",
                                "request id must be a string or integer".to_owned(),
                            ).await?;
                            continue;
                        }

                        if !handshake_complete {
                            if request.method != "hello" {
                                send_error(
                                    &output_tx,
                                    request.id,
                                    "handshake_required",
                                    "hello must be the first request".to_owned(),
                                ).await?;
                                continue;
                            }
                            let params = match serde_json::from_value::<HelloParams>(request.params) {
                                Ok(params) => params,
                                Err(_) => {
                                    send_error(
                                        &output_tx,
                                        request.id,
                                        "invalid_params",
                                        "hello requires protocol_version".to_owned(),
                                    ).await?;
                                    continue;
                                }
                            };
                            if params.protocol_version != PROTOCOL_VERSION {
                                send_error(
                                    &output_tx,
                                    request.id,
                                    "unsupported_protocol",
                                    format!("supported protocol version is {PROTOCOL_VERSION}"),
                                ).await?;
                                continue;
                            }
                            handshake_complete = true;
                            let status = controller.status().await?;
                            let warnings = match status.device_capabilities.check_mount_url {
                                Some(false) => vec![
                                    "JetKVM firmware does not support checkMountUrl; \
                                     preflight URL checks are disabled",
                                ],
                                Some(true) | None => Vec::new(),
                            };
                            send_success(
                                &output_tx,
                                request.id,
                                serde_json::json!({
                                    "protocol_version": PROTOCOL_VERSION,
                                    "capabilities": capability_names(),
                                    "warnings": warnings,
                                    "status": status,
                                }),
                            ).await?;
                            handshake_done.store(true, Ordering::Release);
                            continue;
                        }

                        if request.method == "hello" {
                            send_error(
                                &output_tx,
                                request.id,
                                "invalid_params",
                                "handshake is already complete".to_owned(),
                            ).await?;
                            continue;
                        }

                        let key = request_key(&request.id);
                        let upload_cancellation =
                            (request.method == "upload").then(CancellationToken::new);
                        let kind = if let Some(cancellation) = upload_cancellation.clone() {
                            ActiveKind::Upload(cancellation)
                        } else if matches!(request.method.as_str(), "status" | "cancel" | "shutdown")
                        {
                            ActiveKind::Control
                        } else {
                            ActiveKind::Ordinary
                        };
                        let admission_error = admit_request(&active, key.clone(), kind).await;
                        if let Some((code, message)) = admission_error {
                            let response_id = if code == "duplicate_request_id" {
                                Value::Null
                            } else {
                                request.id
                            };
                            send_error(&output_tx, response_id, code, message).await?;
                            continue;
                        }

                        if request.method == "shutdown" {
                            shutdown_response = Some((request.id, key));
                            server_shutdown.cancel();
                            break;
                        }

                        // Cancellation is truthful: uploads only, routed
                        // inline so it stays responsive while the ordered
                        // worker is busy.
                        if request.method == "cancel" {
                            match serde_json::from_value::<CancelParams>(request.params) {
                                Ok(params) => {
                                    let target_key = request_key(&params.id);
                                    match cancel_active(&active, &target_key).await {
                                        CancelOutcome::Cancelled => {
                                            send_success(
                                                &output_tx,
                                                request.id,
                                                serde_json::json!({ "cancelled": true }),
                                            ).await?;
                                        }
                                        CancelOutcome::NoActiveRequest => {
                                            send_error(
                                                &output_tx,
                                                request.id,
                                                codes::INVALID_PARAMS,
                                                "no active request with that id".to_owned(),
                                            ).await?;
                                        }
                                        CancelOutcome::NotCancellable => {
                                            send_error(
                                                &output_tx,
                                                request.id,
                                                codes::NOT_CANCELLABLE,
                                                "only upload requests may be cancelled".to_owned(),
                                            ).await?;
                                        }
                                    }
                                }
                                Err(_) => {
                                    send_error(
                                        &output_tx,
                                        request.id,
                                        codes::INVALID_PARAMS,
                                        "cancel requires a request id".to_owned(),
                                    ).await?;
                                }
                            }
                            active.lock().await.remove(&key);
                            continue;
                        }

                        // status stays concurrent: it is a read-only query
                        // the controller answers promptly in every state, so
                        // it cannot overtake or delay state-changing work.
                        if request.method == "status" {
                            match controller.status().await {
                                Ok(status) => {
                                    send_success(&output_tx, request.id, to_value(status)?).await?;
                                }
                                Err(error) => {
                                    let error = public_error(&error);
                                    send_error(&output_tx, request.id, error.code, error.message)
                                        .await?;
                                }
                            }
                            active.lock().await.remove(&key);
                            continue;
                        }

                        dispatch_tx
                            .send(QueuedRequest {
                                id: request.id,
                                method: request.method,
                                params: request.params,
                                upload_cancellation,
                            })
                            .await
                            .context("ordered dispatcher stopped")?;
                    }
                }
            }
        }
    }

    // Stop admission, cancel uploads, and trigger lifecycle cleanup before
    // draining ordered work. Controller shutdown interrupts a blocked device
    // operation, allowing the dispatcher to complete promptly.
    cancel_uploads(&active).await;
    drop(dispatch_tx);
    let controller_result = controller.shutdown().await;
    let mut dispatcher = dispatcher;
    let drain_result = match tokio::time::timeout(SHUTDOWN_DRAIN_TIMEOUT, &mut dispatcher).await {
        Ok(result) => result.context("ordered dispatcher task failed"),
        Err(_) => {
            dispatcher.abort();
            Err(anyhow::anyhow!(
                "ordered dispatcher did not stop within the shutdown deadline"
            ))
        }
    };
    let cleanup_result = controller_result.and(drain_result);

    if let Some((id, key)) = shutdown_response {
        active.lock().await.remove(&key);
        match &cleanup_result {
            Ok(()) => send_success(&output_tx, id, Value::Null).await?,
            Err(error) => {
                let error = public_error(error);
                send_error(&output_tx, id, error.code, error.message).await?;
            }
        }
    }

    event_task.abort();
    drop(output_tx);
    writer.await.context("protocol writer task failed")??;
    cleanup_result
}

/// Waits for Ctrl+C on every platform and SIGTERM on Unix.
async fn wait_for_termination_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        match signal(SignalKind::terminate()) {
            Ok(mut sigterm) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = sigterm.recv() => {}
                }
            }
            Err(_) => {
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Runs queued state-changing requests strictly in input order; each result
/// is awaited before the next request starts.
async fn run_dispatcher<H, F>(
    mut rx: mpsc::Receiver<QueuedRequest>,
    output: mpsc::Sender<Value>,
    active: ActiveMap,
    handler: H,
) where
    H: Fn(String, Value, Option<CancellationToken>) -> F,
    F: Future<Output = Result<Value>>,
{
    while let Some(queued) = rx.recv().await {
        let key = request_key(&queued.id);
        let result = handler(queued.method, queued.params, queued.upload_cancellation).await;
        match result {
            Ok(value) => {
                let _ = send_success(&output, queued.id, value).await;
            }
            Err(error) => {
                let error = public_error(&error);
                let _ = send_error(&output, queued.id, error.code, error.message).await;
            }
        }
        active.lock().await.remove(&key);
    }
}

enum CancelOutcome {
    Cancelled,
    NoActiveRequest,
    NotCancellable,
}

async fn cancel_active(active: &ActiveMap, key: &str) -> CancelOutcome {
    let active = active.lock().await;
    match active.get(key) {
        None => CancelOutcome::NoActiveRequest,
        Some(ActiveKind::Ordinary | ActiveKind::Control) => CancelOutcome::NotCancellable,
        Some(ActiveKind::Upload(token)) => {
            token.cancel();
            CancelOutcome::Cancelled
        }
    }
}

async fn cancel_uploads(active: &ActiveMap) {
    let active = active.lock().await;
    for kind in active.values() {
        if let ActiveKind::Upload(token) = kind {
            token.cancel();
        }
    }
}

async fn dispatch(
    controller: &JetKvmController,
    method: &str,
    params: Value,
    upload_cancellation: Option<CancellationToken>,
) -> Result<Value> {
    match method {
        "connect" => to_value(controller.reconnect().await?),
        "disconnect" => {
            controller.disconnect().await?;
            Ok(Value::Null)
        }
        "snapshot" => {
            let params = parse_params::<SnapshotParams>(params)?;
            let snapshot = match params.path {
                Some(path) => {
                    controller
                        .snapshot_to(
                            path,
                            Approval {
                                approved: params.approved,
                            },
                            params.after,
                        )
                        .await?
                }
                None => controller.snapshot(params.after).await?,
            };
            to_value(snapshot)
        }
        "key" => to_value(controller.key(parse_params(params)?).await?),
        "type_text" => to_value(
            controller
                .type_text(parse_params::<TypeTextRequest>(params)?)
                .await?,
        ),
        "mouse_move" | "mouse_button" => {
            let receipt = match parse_params::<MouseMoveParams>(params)? {
                MouseMoveParams::Absolute(event) => controller.absolute_mouse(event).await?,
                MouseMoveParams::Relative(event) => controller.relative_mouse(event).await?,
            };
            to_value(receipt)
        }
        "mouse_scroll" => to_value(
            controller
                .scroll(parse_params::<ScrollEvent>(params)?)
                .await?,
        ),
        "media_state" => to_value(controller.media_state().await?),
        "check_mount_url" => {
            let params = parse_params::<UrlParams>(params)?;
            to_value(
                controller
                    .check_mount_url(
                        params.url,
                        Approval {
                            approved: params.approved,
                        },
                    )
                    .await?,
            )
        }
        "mount_url" => {
            let params = parse_params::<MountUrlParams>(params)?;
            to_value(
                controller
                    .mount_url(
                        params.url,
                        params.mode,
                        Approval {
                            approved: params.approved,
                        },
                    )
                    .await?,
            )
        }
        "mount_local" => {
            let params = parse_params::<MountLocalParams>(params)?;
            to_value(
                controller
                    .mount_local(
                        params.path,
                        params.mode,
                        Approval {
                            approved: params.approved,
                        },
                    )
                    .await?,
            )
        }
        "unmount" => {
            let params = parse_params::<ApprovalParams>(params)?;
            controller
                .unmount(Approval {
                    approved: params.approved,
                })
                .await?;
            Ok(Value::Null)
        }
        "storage_space" => to_value(controller.storage_space().await?),
        "storage_files" => to_value(controller.storage_files().await?),
        "upload" => {
            let params = parse_params::<UploadParams>(params)?;
            let cancellation =
                upload_cancellation.context("upload request is missing its cancellation token")?;
            to_value(
                controller
                    .upload(
                        params.path,
                        params.filename,
                        Approval {
                            approved: params.approved,
                        },
                        cancellation,
                    )
                    .await?,
            )
        }
        "mount_storage" => {
            let params = parse_params::<StorageFileParams>(params)?;
            let mode = params.mode.ok_or_else(|| {
                CodedError::new(codes::INVALID_PARAMS, "mount_storage requires mode")
            })?;
            to_value(
                controller
                    .mount_storage(
                        params.filename,
                        mode,
                        Approval {
                            approved: params.approved,
                        },
                    )
                    .await?,
            )
        }
        "delete_storage" => {
            let params = parse_params::<StorageFileParams>(params)?;
            controller
                .delete_storage(
                    params.filename,
                    Approval {
                        approved: params.approved,
                    },
                )
                .await?;
            Ok(Value::Null)
        }
        "shutdown" => unreachable!("shutdown is handled by the protocol control plane"),
        other => {
            Err(CodedError::new(codes::UNSUPPORTED, format!("unknown method: {other}")).into())
        }
    }
}

fn capability_names() -> Vec<&'static str> {
    vec![
        "connect",
        "disconnect",
        "status",
        "snapshot",
        "key",
        "type_text",
        "mouse_move",
        "mouse_button",
        "mouse_scroll",
        "media_state",
        "check_mount_url",
        "mount_url",
        "mount_local",
        "unmount",
        "storage_space",
        "storage_files",
        "upload",
        "mount_storage",
        "delete_storage",
        "cancel",
        "shutdown",
    ]
}

fn parse_params<T: for<'de> Deserialize<'de>>(params: Value) -> Result<T> {
    serde_json::from_value(params)
        .map_err(|error| CodedError::new(codes::INVALID_PARAMS, format!("invalid params: {error}")))
        .map_err(Into::into)
}

fn to_value(value: impl Serialize) -> Result<Value> {
    serde_json::to_value(value).context("failed to encode method result")
}

/// Maps an internal error to a stable protocol code from its typed
/// [`CodedError`] origin (no string classification); the message is always
/// redacted as defense in depth.
fn public_error(error: &anyhow::Error) -> ProtocolError {
    ProtocolError {
        code: error_code(error),
        message: redact(format!("{error:#}")),
    }
}

pub(crate) fn redact(message: String) -> String {
    let mut message = redact_urls(&message);
    let lowercase = message.to_ascii_lowercase();
    if let Some(index) = [
        "authorization:",
        "proxy-authorization:",
        "bearer ",
        "cookie:",
        "cookie=",
        "set-cookie:",
        "password=",
        "password:",
        "\"password\"",
        "\"token\"",
    ]
    .into_iter()
    .filter_map(|marker| lowercase.find(marker))
    .min()
    {
        message.truncate(index);
        message.push_str("<redacted>");
    }
    let lowercase = message.to_ascii_lowercase();
    let route = "/jetkvm-controller/media/";
    if let Some(index) = lowercase.find(route) {
        message.truncate(index + route.len());
        message.push_str("<redacted>");
    }
    let lowercase = message.to_ascii_lowercase();
    let upload_id = "uploadid=";
    if let Some(index) = lowercase.find(upload_id) {
        message.truncate(index + upload_id.len());
        message.push_str("<redacted>");
    }
    message
}

/// Replaces URL-shaped error fragments wholesale. Public errors need the
/// operation and failure class, not caller-supplied paths, userinfo, queries,
/// or fragments that may carry credentials.
fn redact_urls(message: &str) -> String {
    const SCHEMES: [&str; 3] = ["http://", "https://", "file://"];

    let lowercase = message.to_ascii_lowercase();
    let mut output = String::with_capacity(message.len());
    let mut cursor = 0;
    while cursor < message.len() {
        let Some(start) = SCHEMES
            .into_iter()
            .filter_map(|scheme| lowercase[cursor..].find(scheme))
            .map(|relative| cursor + relative)
            .min()
        else {
            output.push_str(&message[cursor..]);
            break;
        };
        output.push_str(&message[cursor..start]);
        let end = message[start..]
            .char_indices()
            .find_map(|(offset, character)| {
                (offset > 0
                    && (character.is_whitespace()
                        || matches!(
                            character,
                            '"' | '\'' | '<' | '>' | '(' | ')' | '[' | ']' | '{' | '}'
                        )))
                .then_some(start + offset)
            })
            .unwrap_or(message.len());
        output.push_str("<redacted-url>");
        cursor = end;
    }
    output
}

fn valid_id(id: &Value) -> bool {
    id.is_string() || id.as_i64().is_some() || id.as_u64().is_some()
}

fn request_key(id: &Value) -> String {
    serde_json::to_string(id).unwrap_or_else(|_| "null".to_owned())
}

async fn send_success(output: &mpsc::Sender<Value>, id: Value, result: Value) -> Result<()> {
    output
        .send(serde_json::to_value(SuccessResponse { id, result })?)
        .await
        .context("protocol output closed")
}

async fn send_error(
    output: &mpsc::Sender<Value>,
    id: Value,
    code: &'static str,
    message: String,
) -> Result<()> {
    output
        .send(serde_json::to_value(ErrorResponse {
            id,
            error: ProtocolError { code, message },
        })?)
        .await
        .context("protocol output closed")
}

fn read_ndjson_line<R: BufRead>(reader: &mut R, maximum: usize) -> std::io::Result<ReadLine> {
    let mut line = Vec::new();
    let mut oversized = false;
    loop {
        let buffer = reader.fill_buf()?;
        if buffer.is_empty() {
            return if line.is_empty() {
                Ok(ReadLine::Eof)
            } else if oversized {
                Ok(ReadLine::Oversized)
            } else {
                Ok(ReadLine::Line(line))
            };
        }
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let consumed = newline.map_or(buffer.len(), |index| index + 1);
        let content_length = newline.unwrap_or(buffer.len());
        if !oversized {
            if line.len() + content_length > maximum {
                oversized = true;
                line.clear();
            } else {
                line.extend_from_slice(&buffer[..content_length]);
            }
        }
        reader.consume(consumed);
        if newline.is_some() {
            if oversized {
                return Ok(ReadLine::Oversized);
            }
            if line.last() == Some(&b'\r') {
                line.pop();
            }
            return Ok(ReadLine::Line(line));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn framing_recovers_after_oversized_line() {
        let input = b"123456\n{}\n";
        let mut reader = std::io::BufReader::new(&input[..]);
        assert!(matches!(
            read_ndjson_line(&mut reader, 4).unwrap(),
            ReadLine::Oversized
        ));
        match read_ndjson_line(&mut reader, 4).unwrap() {
            ReadLine::Line(line) => assert_eq!(line, b"{}"),
            _ => panic!("expected recovered line"),
        }
    }

    #[test]
    fn output_redacts_urls_tokens_and_credentials() {
        for message in [
            "failed http://host/jetkvm-controller/media/secret-token",
            "failed https://user:password@example.invalid/image.iso?token=secret#fragment",
            "failed FILE:///home/operator/private.iso",
            "request Cookie: session=secret",
            "request cookie=session-secret",
            "request authorization: Bearer secret",
            "request Proxy-Authorization: Basic secret",
            "request bearer secret",
            "response Set-Cookie: session=secret",
            r#"request {"password":"secret"}"#,
            r#"request {"token":"secret"}"#,
            "upload uploadId=secret-token",
        ] {
            let redacted = redact(message.to_owned());
            for secret in ["secret", "password@example", "/home/operator", "image.iso"] {
                assert!(
                    !redacted.contains(secret),
                    "{secret:?} leaked from {message:?} as {redacted:?}"
                );
            }
            assert!(
                redacted.contains("<redacted"),
                "{message:?} was not redacted"
            );
        }

        assert_eq!(
            redact(
                "first https://user:secret@one.invalid/a?token=x then \
                 http://two.invalid/private.iso?signature=y failed"
                    .to_owned()
            ),
            "first <redacted-url> then <redacted-url> failed"
        );
    }

    #[test]
    fn request_ids_are_limited_to_strings_and_integers() {
        assert!(valid_id(&serde_json::json!(1)));
        assert!(valid_id(&serde_json::json!("one")));
        assert!(!valid_id(&Value::Null));
        assert!(!valid_id(&serde_json::json!(1.5)));
    }

    #[test]
    fn snapshot_paths_require_explicit_opt_in() {
        let owned: SnapshotParams =
            serde_json::from_value(serde_json::json!({})).expect("default snapshot params");
        assert!(owned.path.is_none());
        assert!(!owned.approved);
        assert!(owned.after.is_none());

        let selected: SnapshotParams = serde_json::from_value(serde_json::json!({
            "path": "/tmp/selected.png",
            "approved": true,
        }))
        .expect("selected snapshot params");
        assert_eq!(selected.path, Some(PathBuf::from("/tmp/selected.png")));
        assert!(selected.approved);
    }

    #[test]
    fn snapshot_after_cursor_parses() {
        let params: SnapshotParams = serde_json::from_value(serde_json::json!({
            "after": { "generation": 3, "frame_id": 41 },
        }))
        .expect("snapshot params with cursor");
        assert_eq!(
            params.after,
            Some(FrameCursor {
                generation: 3,
                frame_id: 41,
            })
        );
    }

    #[test]
    fn capabilities_always_report_sidecar_methods() {
        assert!(capability_names().contains(&"check_mount_url"));
        assert!(capability_names().contains(&"shutdown"));
    }

    #[test]
    fn public_error_maps_typed_codes_and_redacts() {
        let coded = anyhow::Error::new(CodedError::new(
            codes::APPROVAL_REQUIRED,
            "explicit approval is required to mount media",
        ))
        .context("mount failed");
        let error = public_error(&coded);
        assert_eq!(error.code, codes::APPROVAL_REQUIRED);

        let stale = anyhow::Error::new(CodedError::new(
            codes::STALE_GENERATION,
            "frame cursor generation 1 is stale",
        ));
        assert_eq!(public_error(&stale).code, codes::STALE_GENERATION);

        let uncoded = anyhow::anyhow!("unexpected disk state");
        assert_eq!(public_error(&uncoded).code, codes::OPERATION_FAILED);

        let secret = anyhow::Error::new(CodedError::new(
            codes::OPERATION_FAILED,
            "mount failed for http://host/jetkvm-controller/media/secret-token",
        ));
        let error = public_error(&secret);
        assert!(!error.message.contains("secret-token"));
    }

    #[tokio::test]
    async fn cancel_only_applies_to_active_uploads() {
        let active: ActiveMap = Arc::new(Mutex::new(HashMap::new()));
        assert!(matches!(
            cancel_active(&active, "\"missing\"").await,
            CancelOutcome::NoActiveRequest
        ));

        active
            .lock()
            .await
            .insert("\"plain\"".to_owned(), ActiveKind::Ordinary);
        assert!(matches!(
            cancel_active(&active, "\"plain\"").await,
            CancelOutcome::NotCancellable
        ));

        let token = CancellationToken::new();
        active
            .lock()
            .await
            .insert("\"up\"".to_owned(), ActiveKind::Upload(token.clone()));
        assert!(matches!(
            cancel_active(&active, "\"up\"").await,
            CancelOutcome::Cancelled
        ));
        assert!(token.is_cancelled());
        assert!(
            active.lock().await.contains_key("\"up\""),
            "cancellation must retain the upload ID until its terminal response"
        );
    }

    #[tokio::test]
    async fn duplicate_admission_preserves_the_original_active_request() {
        let active: ActiveMap = Arc::new(Mutex::new(HashMap::new()));
        let key = request_key(&serde_json::json!("shared"));
        let upload = CancellationToken::new();
        assert!(
            admit_request(&active, key.clone(), ActiveKind::Upload(upload.clone()),)
                .await
                .is_none()
        );

        let duplicate = admit_request(&active, key.clone(), ActiveKind::Ordinary)
            .await
            .expect("duplicate is rejected");
        assert_eq!(duplicate.0, "duplicate_request_id");
        assert!(matches!(
            cancel_active(&active, &key).await,
            CancelOutcome::Cancelled
        ));
        assert!(upload.is_cancelled());

        active.lock().await.remove(&key);
        assert!(
            admit_request(&active, key, ActiveKind::Ordinary)
                .await
                .is_none(),
            "an ID is reusable after its terminal response"
        );
    }

    #[tokio::test]
    async fn control_admission_survives_saturated_ordinary_capacity() {
        let active: ActiveMap = Arc::new(Mutex::new(HashMap::new()));
        for id in 0..MAX_ACTIVE_REQUESTS {
            assert!(
                admit_request(&active, id.to_string(), ActiveKind::Ordinary)
                    .await
                    .is_none()
            );
        }

        let status_key = "status".to_owned();
        assert!(
            admit_request(&active, status_key.clone(), ActiveKind::Control)
                .await
                .is_none(),
            "control request must not consume ordinary capacity"
        );
        let duplicate = admit_request(&active, status_key, ActiveKind::Control)
            .await
            .expect("duplicate control ID is rejected");
        assert_eq!(duplicate.0, "duplicate_request_id");

        let busy = admit_request(&active, "ordinary-65".to_owned(), ActiveKind::Ordinary)
            .await
            .expect("sixty-fifth ordinary request is rejected");
        assert_eq!(busy.0, codes::SERVER_BUSY);
    }

    #[tokio::test]
    async fn cancel_reaches_upload_at_saturated_ordinary_capacity() {
        let active: ActiveMap = Arc::new(Mutex::new(HashMap::new()));
        for id in 0..(MAX_ACTIVE_REQUESTS - 1) {
            assert!(
                admit_request(&active, id.to_string(), ActiveKind::Ordinary)
                    .await
                    .is_none()
            );
        }
        let upload = CancellationToken::new();
        assert!(
            admit_request(
                &active,
                "upload".to_owned(),
                ActiveKind::Upload(upload.clone()),
            )
            .await
            .is_none()
        );
        assert!(
            admit_request(&active, "cancel".to_owned(), ActiveKind::Control)
                .await
                .is_none()
        );

        assert!(matches!(
            cancel_active(&active, "upload").await,
            CancelOutcome::Cancelled
        ));
        assert!(upload.is_cancelled());
        assert!(
            active.lock().await.contains_key("upload"),
            "upload ID remains active until its terminal response"
        );
    }

    #[tokio::test]
    async fn blocked_upload_allows_status_then_cancel_with_terminal_response() {
        let (output_tx, mut output_rx) = mpsc::channel(4);
        let active: ActiveMap = Arc::new(Mutex::new(HashMap::new()));
        let (dispatch_tx, dispatch_rx) = mpsc::channel(1);
        let upload_id = Value::String("upload".to_owned());
        let upload_key = request_key(&upload_id);
        let cancellation = CancellationToken::new();
        assert!(
            admit_request(
                &active,
                upload_key.clone(),
                ActiveKind::Upload(cancellation.clone()),
            )
            .await
            .is_none()
        );
        let started = Arc::new(tokio::sync::Notify::new());
        let handler_started = Arc::clone(&started);
        let dispatcher_active = Arc::clone(&active);
        let dispatcher = tokio::spawn(async move {
            run_dispatcher(
                dispatch_rx,
                output_tx,
                dispatcher_active,
                move |_method, _params, cancellation| {
                    let started = Arc::clone(&handler_started);
                    async move {
                        let cancellation = cancellation.expect("upload cancellation token");
                        started.notify_one();
                        cancellation.cancelled().await;
                        Err::<Value, _>(anyhow::Error::new(CodedError::new(
                            codes::CANCELLED,
                            "upload cancelled",
                        )))
                    }
                },
            )
            .await;
        });
        dispatch_tx
            .send(QueuedRequest {
                id: upload_id,
                method: "upload".to_owned(),
                params: Value::Null,
                upload_cancellation: Some(cancellation),
            })
            .await
            .expect("admit blocked upload to dispatcher");
        started.notified().await;

        let status_started = std::time::Instant::now();
        assert!(
            admit_request(&active, "status".to_owned(), ActiveKind::Control)
                .await
                .is_none()
        );
        assert!(status_started.elapsed() < std::time::Duration::from_millis(250));
        active.lock().await.remove("status");

        assert!(
            admit_request(&active, "cancel".to_owned(), ActiveKind::Control)
                .await
                .is_none()
        );
        assert!(matches!(
            cancel_active(&active, &upload_key).await,
            CancelOutcome::Cancelled
        ));
        assert!(
            active.lock().await.contains_key(&upload_key),
            "upload ID remains active until its terminal error is sent"
        );
        active.lock().await.remove("cancel");

        let response = tokio::time::timeout(std::time::Duration::from_secs(1), output_rx.recv())
            .await
            .expect("cancelled upload terminal deadline")
            .expect("cancelled upload response");
        assert_eq!(response["error"]["code"], codes::CANCELLED);
        drop(dispatch_tx);
        dispatcher.await.expect("dispatcher joins");
        assert!(!active.lock().await.contains_key(&upload_key));
    }

    #[tokio::test]
    async fn shutdown_cancels_active_and_queued_uploads_before_dispatch_drain() {
        let (output_tx, mut output_rx) = mpsc::channel(4);
        let active: ActiveMap = Arc::new(Mutex::new(HashMap::new()));
        let (dispatch_tx, dispatch_rx) = mpsc::channel(2);
        let first_token = CancellationToken::new();
        let second_token = CancellationToken::new();
        for (id, token) in [(1, first_token.clone()), (2, second_token.clone())] {
            let value = Value::from(id);
            assert!(
                admit_request(
                    &active,
                    request_key(&value),
                    ActiveKind::Upload(token.clone()),
                )
                .await
                .is_none()
            );
            dispatch_tx
                .send(QueuedRequest {
                    id: value,
                    method: "upload".to_owned(),
                    params: Value::Null,
                    upload_cancellation: Some(token),
                })
                .await
                .expect("queue upload");
        }

        let remote_starts = Arc::new(AtomicUsize::new(0));
        let first_started = Arc::new(tokio::sync::Notify::new());
        let handler_starts = Arc::clone(&remote_starts);
        let handler_first_started = Arc::clone(&first_started);
        let dispatcher_active = Arc::clone(&active);
        let dispatcher = tokio::spawn(async move {
            run_dispatcher(
                dispatch_rx,
                output_tx,
                dispatcher_active,
                move |_method, _params, cancellation| {
                    let remote_starts = Arc::clone(&handler_starts);
                    let first_started = Arc::clone(&handler_first_started);
                    async move {
                        let cancellation = cancellation.expect("upload cancellation token");
                        if !cancellation.is_cancelled() {
                            remote_starts.fetch_add(1, Ordering::SeqCst);
                            first_started.notify_one();
                        }
                        cancellation.cancelled().await;
                        Err::<Value, _>(anyhow::Error::new(CodedError::new(
                            codes::CANCELLED,
                            "upload cancelled by shutdown",
                        )))
                    }
                },
            )
            .await;
        });
        first_started.notified().await;

        cancel_uploads(&active).await;
        assert!(first_token.is_cancelled());
        assert!(second_token.is_cancelled());
        drop(dispatch_tx);
        let first = output_rx.recv().await.expect("first terminal response");
        let second = output_rx.recv().await.expect("second terminal response");
        dispatcher.await.expect("dispatcher joins after shutdown");
        assert_eq!(first["error"]["code"], codes::CANCELLED);
        assert_eq!(second["error"]["code"], codes::CANCELLED);
        assert_eq!(
            remote_starts.load(Ordering::SeqCst),
            1,
            "queued upload observes pre-cancellation and never starts remotely"
        );
        assert!(active.lock().await.is_empty());
    }

    #[tokio::test]
    async fn cancel_racing_shutdown_is_idempotent_and_retains_active_id() {
        let active: ActiveMap = Arc::new(Mutex::new(HashMap::new()));
        let token = CancellationToken::new();
        active
            .lock()
            .await
            .insert("upload".to_owned(), ActiveKind::Upload(token.clone()));
        let (cancel, ()) = tokio::join!(cancel_active(&active, "upload"), cancel_uploads(&active),);
        assert!(matches!(cancel, CancelOutcome::Cancelled));
        assert!(token.is_cancelled());
        assert!(active.lock().await.contains_key("upload"));
    }

    #[tokio::test]
    async fn eof_and_sigterm_racing_shutdown_cancel_uploads_idempotently() {
        for termination in ["eof", "sigterm"] {
            let active: ActiveMap = Arc::new(Mutex::new(HashMap::new()));
            let token = CancellationToken::new();
            active
                .lock()
                .await
                .insert(termination.to_owned(), ActiveKind::Upload(token.clone()));

            tokio::join!(cancel_uploads(&active), cancel_uploads(&active));
            assert!(token.is_cancelled(), "{termination} race cancels upload");
            assert!(
                active.lock().await.contains_key(termination),
                "{termination} race retains ID until the terminal response"
            );
            active.lock().await.remove(termination);
            assert!(active.lock().await.is_empty());
        }
    }

    #[tokio::test]
    async fn upload_completion_races_have_deterministic_terminal_outcomes() {
        let active: ActiveMap = Arc::new(Mutex::new(HashMap::new()));

        let completed_before_cancel = CancellationToken::new();
        active.lock().await.insert(
            "complete-first".to_owned(),
            ActiveKind::Upload(completed_before_cancel.clone()),
        );
        active.lock().await.remove("complete-first");
        assert!(matches!(
            cancel_active(&active, "complete-first").await,
            CancelOutcome::NoActiveRequest
        ));
        assert!(!completed_before_cancel.is_cancelled());

        let cancelled_before_complete = CancellationToken::new();
        active.lock().await.insert(
            "cancel-first".to_owned(),
            ActiveKind::Upload(cancelled_before_complete.clone()),
        );
        assert!(matches!(
            cancel_active(&active, "cancel-first").await,
            CancelOutcome::Cancelled
        ));
        active.lock().await.remove("cancel-first");
        assert!(cancelled_before_complete.is_cancelled());

        let completed_before_shutdown = CancellationToken::new();
        active.lock().await.insert(
            "shutdown-complete-first".to_owned(),
            ActiveKind::Upload(completed_before_shutdown.clone()),
        );
        active.lock().await.remove("shutdown-complete-first");
        cancel_uploads(&active).await;
        assert!(!completed_before_shutdown.is_cancelled());

        let shutdown_before_complete = CancellationToken::new();
        active.lock().await.insert(
            "shutdown-first".to_owned(),
            ActiveKind::Upload(shutdown_before_complete.clone()),
        );
        cancel_uploads(&active).await;
        active.lock().await.remove("shutdown-first");
        assert!(shutdown_before_complete.is_cancelled());
        assert!(active.lock().await.is_empty());
    }

    #[tokio::test]
    async fn dispatcher_executes_requests_in_input_order() {
        let (output_tx, mut output_rx) = mpsc::channel(16);
        let active: ActiveMap = Arc::new(Mutex::new(HashMap::new()));
        let (dispatch_tx, dispatch_rx) = mpsc::channel(16);

        // The first request is artificially slow; completion order must
        // still match input order.
        let slow_first = Arc::new(std::sync::Mutex::new(VecDeque::from(vec![
            std::time::Duration::from_millis(150),
            std::time::Duration::ZERO,
            std::time::Duration::ZERO,
        ])));
        let handler_active = Arc::clone(&active);
        let dispatcher = tokio::spawn(async move {
            run_dispatcher(
                dispatch_rx,
                output_tx,
                handler_active,
                move |method, _params, _cancel| {
                    let slow = slow_first
                        .lock()
                        .expect("delay queue")
                        .pop_front()
                        .unwrap_or(std::time::Duration::ZERO);
                    async move {
                        tokio::time::sleep(slow).await;
                        Ok(Value::String(method))
                    }
                },
            )
            .await;
        });

        for (id, method) in ["first", "second", "third"].into_iter().enumerate() {
            let id = Value::from(id);
            active
                .lock()
                .await
                .insert(request_key(&id), ActiveKind::Ordinary);
            dispatch_tx
                .send(QueuedRequest {
                    id,
                    method: method.to_owned(),
                    params: Value::Null,
                    upload_cancellation: None,
                })
                .await
                .expect("dispatcher accepts request");
        }
        drop(dispatch_tx);

        let mut results = Vec::new();
        while let Some(value) = output_rx.recv().await {
            results.push(value);
        }
        dispatcher.await.expect("dispatcher completes");
        let results: Vec<String> = results
            .into_iter()
            .map(|value| value["result"].as_str().expect("result").to_owned())
            .collect();
        assert_eq!(results, vec!["first", "second", "third"]);
        assert!(active.lock().await.is_empty());
    }
}
