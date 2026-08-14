use std::collections::HashMap;
use std::io::BufRead;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::io::{AsyncWriteExt, BufWriter};
use tokio::sync::{Mutex, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::controller::{JetKvmController, ScrollEvent, TypeTextRequest};
use crate::hid::{AbsoluteMouseEvent, RelativeMouseEvent};
use crate::rpc::VirtualMediaMode;
use crate::virtual_media::Approval;

pub const PROTOCOL_VERSION: u32 = 1;
const MAX_LINE_BYTES: usize = 1024 * 1024;
const OUTPUT_BUFFER: usize = 128;

const MAX_ACTIVE_REQUESTS: usize = 64;
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
    path: PathBuf,
}

#[derive(Debug, Deserialize)]
struct UrlParams {
    url: String,
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

    let event_output = output_tx.clone();
    let mut events = controller.subscribe_events();
    let event_task = tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(event) => {
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
    let active = Arc::new(Mutex::new(HashMap::<String, CancellationToken>::new()));
    let server_shutdown = CancellationToken::new();
    let mut requests = JoinSet::new();
    let mut handshake_complete = false;

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
                            send_success(
                                &output_tx,
                                request.id,
                                serde_json::json!({
                                    "protocol_version": PROTOCOL_VERSION,
                                    "capabilities": [
                                        "connect", "disconnect", "status", "snapshot", "key",
                                        "type_text", "mouse_move", "mouse_button", "mouse_scroll",
                                        "media_state", "check_mount_url", "mount_url", "mount_local",
                                        "unmount", "storage_space", "storage_files", "upload",
                                        "mount_storage", "delete_storage", "cancel", "shutdown"
                                    ]
                                }),
                            ).await?;
                            continue;
                        }

                        if request.method == "cancel" {
                            let result = serde_json::from_value::<CancelParams>(request.params);
                            match result {
                                Ok(params) => {
                                    let key = request_key(&params.id);
                                    let cancelled = active
                                        .lock()
                                        .await
                                        .get(&key)
                                        .is_some_and(|token| {
                                            token.cancel();
                                            true
                                        });
                                    send_success(
                                        &output_tx,
                                        request.id,
                                        serde_json::json!({ "cancelled": cancelled }),
                                    ).await?;
                                }
                                Err(_) => {
                                    send_error(
                                        &output_tx,
                                        request.id,
                                        "invalid_params",
                                        "cancel requires a request id".to_owned(),
                                    ).await?;
                                }
                            }
                            continue;
                        }

                        let key = request_key(&request.id);
                        let cancellation = CancellationToken::new();
                        let admission_error = {
                            let mut active = active.lock().await;
                            if active.contains_key(&key) {
                                Some((
                                    "duplicate_request_id",
                                    "request id is already active".to_owned(),
                                ))
                            } else if active.len() >= MAX_ACTIVE_REQUESTS {
                                Some((
                                    "server_busy",
                                    format!("at most {MAX_ACTIVE_REQUESTS} requests may be active"),
                                ))
                            } else {
                                active.insert(key.clone(), cancellation.clone());
                                None
                            }
                        };
                        if let Some((code, message)) = admission_error {
                            send_error(&output_tx, request.id, code, message).await?;
                            continue;
                        }
                        let request_controller = controller.clone();
                        let request_output = output_tx.clone();
                        let request_active = Arc::clone(&active);
                        let request_shutdown = server_shutdown.clone();
                        requests.spawn(async move {
                            let id = request.id.clone();
                            let method = request.method;
                            let dispatch_cancellation = cancellation.clone();
                            let operation = dispatch(
                                &request_controller,
                                &method,
                                request.params,
                                dispatch_cancellation,
                            );
                            let result = if method == "upload" {
                                operation.await
                            } else {
                                tokio::select! {
                                    result = operation => result,
                                    _ = cancellation.cancelled() => {
                                        Err(anyhow::anyhow!("request cancelled"))
                                    }
                                }
                            };
                            match result {
                                Ok(value) => {
                                    let _ = send_success(&request_output, id, value).await;
                                    if method == "shutdown" {
                                        request_shutdown.cancel();
                                    }
                                }
                                Err(error) => {
                                    let error = public_error(&error);
                                    let _ = send_error(
                                        &request_output,
                                        id,
                                        error.code,
                                        error.message,
                                    )
                                    .await;
                                }
                            }
                            request_active.lock().await.remove(&key);
                        });
                    }
                }
            }
        }
    }

    for token in active.lock().await.values() {
        token.cancel();
    }
    while requests.join_next().await.is_some() {}
    if !server_shutdown.is_cancelled() {
        controller.shutdown().await?;
    }
    event_task.abort();
    drop(output_tx);
    writer.await.context("protocol writer task failed")??;
    Ok(())
}

async fn dispatch(
    controller: &JetKvmController,
    method: &str,
    params: Value,
    cancellation: CancellationToken,
) -> Result<Value> {
    match method {
        "connect" => to_value(controller.reconnect().await?),
        "disconnect" => {
            controller.disconnect().await?;
            Ok(Value::Null)
        }
        "status" => to_value(controller.status().await?),
        "snapshot" => {
            let params = parse_params::<SnapshotParams>(params)?;
            to_value(controller.snapshot(params.path).await?)
        }
        "key" => {
            controller.key(parse_params(params)?).await?;
            Ok(Value::Null)
        }
        "type_text" => {
            controller
                .type_text(parse_params::<TypeTextRequest>(params)?)
                .await?;
            Ok(Value::Null)
        }
        "mouse_move" | "mouse_button" => {
            match parse_params::<MouseMoveParams>(params)? {
                MouseMoveParams::Absolute(event) => controller.absolute_mouse(event).await?,
                MouseMoveParams::Relative(event) => controller.relative_mouse(event).await?,
            }
            Ok(Value::Null)
        }
        "mouse_scroll" => {
            controller
                .scroll(parse_params::<ScrollEvent>(params)?)
                .await?;
            Ok(Value::Null)
        }
        "media_state" => to_value(controller.media_state().await?),
        "check_mount_url" => {
            let params = parse_params::<UrlParams>(params)?;
            to_value(controller.check_mount_url(params.url).await?)
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
            let mode = params.mode.context("mount_storage requires mode")?;
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
        "shutdown" => {
            controller.shutdown().await?;
            Ok(Value::Null)
        }
        _ => anyhow::bail!("unknown control method"),
    }
}

fn parse_params<T: for<'de> Deserialize<'de>>(params: Value) -> Result<T> {
    serde_json::from_value(params).context("invalid method parameters")
}

fn to_value(value: impl Serialize) -> Result<Value> {
    serde_json::to_value(value).context("failed to encode method result")
}

fn public_error(error: &anyhow::Error) -> ProtocolError {
    let message = redact(format!("{error:#}"));
    let code = if message.contains("approval") {
        "approval_required"
    } else if message.contains("not ready")
        || message.contains("not connected")
        || message.contains("stopped")
    {
        "not_connected"
    } else if message.contains("timed out") {
        "timeout"
    } else if message.contains("cancelled") {
        "cancelled"
    } else if message.contains("not implemented") || message.contains("unsupported") {
        "unsupported"
    } else if message.contains("invalid") || message.contains("unknown control method") {
        "invalid_request"
    } else {
        "operation_failed"
    };
    ProtocolError { code, message }
}

fn redact(mut message: String) -> String {
    let lowercase = message.to_ascii_lowercase();
    if let Some(index) = ["cookie:", "authorization:", "password=", "\"password\""]
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
    fn output_redacts_local_tokens_and_credentials() {
        assert_eq!(
            redact("failed http://host/jetkvm-controller/media/secret-token".to_owned()),
            "failed http://host/jetkvm-controller/media/<redacted>"
        );
        assert_eq!(
            redact("request Cookie: session=secret".to_owned()),
            "request <redacted>"
        );
        assert_eq!(
            redact("upload uploadId=secret-token".to_owned()),
            "upload uploadId=<redacted>"
        );
        for message in [
            "request authorization: Bearer secret",
            r#"request {"password":"secret"}"#,
        ] {
            let redacted = redact(message.to_owned());
            assert!(!redacted.contains("secret"));
            assert!(redacted.ends_with("<redacted>"));
        }
    }

    #[test]
    fn request_ids_are_limited_to_strings_and_integers() {
        assert!(valid_id(&serde_json::json!(1)));
        assert!(valid_id(&serde_json::json!("one")));
        assert!(!valid_id(&Value::Null));
        assert!(!valid_id(&serde_json::json!(1.5)));
    }
}
