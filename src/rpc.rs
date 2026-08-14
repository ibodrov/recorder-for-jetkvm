use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{broadcast, oneshot};
use webrtc::data_channel::RTCDataChannel;
use webrtc::data_channel::data_channel_message::DataChannelMessage;

const JSON_RPC_VERSION: &str = "2.0";
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

type PendingResult = std::result::Result<Value, RpcError>;
type PendingMap = Arc<Mutex<HashMap<u64, oneshot::Sender<PendingResult>>>>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl RpcError {
    fn connection_changed() -> Self {
        Self {
            code: -32001,
            message: "connection generation changed".to_owned(),
            data: None,
        }
    }

    fn malformed(message: impl Into<String>) -> Self {
        Self {
            code: -32700,
            message: message.into(),
            data: None,
        }
    }
}

impl fmt::Display for RpcError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "remote RPC error {}", self.code)?;
        if self.message.eq_ignore_ascii_case("not implemented")
            || self.data.as_ref().and_then(Value::as_str) == Some("not implemented")
        {
            write!(f, ": not implemented")?;
        }
        Ok(())
    }
}

impl std::error::Error for RpcError {}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RpcNotification {
    pub method: String,
    #[serde(default)]
    pub params: Value,
    pub id: Option<Value>,
}

#[derive(Debug, Serialize)]
struct Request<'a> {
    jsonrpc: &'static str,
    method: &'a str,
    params: Value,
    id: u64,
}

#[derive(Debug, Deserialize)]
struct Response {
    #[allow(dead_code)]
    jsonrpc: Option<String>,
    id: Option<u64>,
    result: Option<Value>,
    error: Option<RpcError>,
    method: Option<String>,
    #[serde(default)]
    params: Value,
}

#[derive(Clone)]
pub(crate) struct RpcClient {
    channel: Arc<RTCDataChannel>,
    pending: PendingMap,
    next_id: Arc<AtomicU64>,
    notifications: broadcast::Sender<RpcNotification>,
    timeout: Duration,
}

impl RpcClient {
    pub fn new(channel: Arc<RTCDataChannel>) -> Self {
        Self::with_timeout(channel, DEFAULT_TIMEOUT)
    }

    fn with_timeout(channel: Arc<RTCDataChannel>, timeout: Duration) -> Self {
        let pending = PendingMap::default();
        let (notifications, _) = broadcast::channel(64);
        install_message_handler(
            &channel,
            Arc::clone(&pending),
            notifications.clone(),
            Arc::clone(&channel),
        );

        Self {
            channel,
            pending,
            next_id: Arc::new(AtomicU64::new(1)),
            notifications,
            timeout,
        }
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, async {
            while self.channel.ready_state()
                != webrtc::data_channel::data_channel_state::RTCDataChannelState::Open
            {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("RPC data channel did not open")?;
        Ok(())
    }

    pub fn subscribe_notifications(&self) -> broadcast::Receiver<RpcNotification> {
        self.notifications.subscribe()
    }

    pub async fn call<P, R>(&self, method: &str, params: P) -> Result<R>
    where
        P: Serialize,
        R: DeserializeOwned,
    {
        let params = serde_json::to_value(params).context("failed to encode RPC parameters")?;
        let value = self.call_value(method, params).await?;
        serde_json::from_value(value).with_context(|| format!("invalid {method} RPC result"))
    }

    pub async fn call_value(&self, method: &str, params: Value) -> Result<Value> {
        self.call_value_with_timeout(method, params, self.timeout)
            .await
    }

    async fn call_value_with_timeout(
        &self,
        method: &str,
        params: Value,
        timeout: Duration,
    ) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let payload = serde_json::to_string(&Request {
            jsonrpc: JSON_RPC_VERSION,
            method,
            params,
            id,
        })
        .context("failed to encode RPC request")?;

        let (tx, rx) = oneshot::channel();
        self.pending.lock().insert(id, tx);

        if let Err(err) = self.channel.send_text(payload).await {
            self.pending.lock().remove(&id);
            return Err(err).context("failed to send RPC request");
        }

        wait_for_response(&self.pending, id, method, rx, timeout).await
    }

    pub fn cancel_generation(&self) {
        cancel_pending(&self.pending);
    }

    pub async fn media_state(&self) -> Result<Option<VirtualMediaState>> {
        self.call("getVirtualMediaState", serde_json::json!({}))
            .await
    }

    pub async fn check_mount_url(&self, url: &str) -> Result<MountUrlInfo> {
        self.call("checkMountUrl", serde_json::json!({ "url": url }))
            .await
    }

    pub async fn mount_http(&self, url: &str, mode: VirtualMediaMode) -> Result<()> {
        self.call::<_, Value>(
            "mountWithHTTP",
            serde_json::json!({ "url": url, "mode": mode }),
        )
        .await
        .map(|_| ())
    }

    pub async fn unmount(&self) -> Result<()> {
        self.call_value_with_timeout(
            "unmountImage",
            serde_json::json!({}),
            Duration::from_secs(30),
        )
        .await
        .map(|_| ())
    }

    pub async fn storage_space(&self) -> Result<StorageSpace> {
        self.call("getStorageSpace", serde_json::json!({})).await
    }

    pub async fn storage_files(&self) -> Result<StorageFiles> {
        self.call("listStorageFiles", serde_json::json!({})).await
    }

    pub async fn start_upload(&self, filename: &str, size: u64) -> Result<StorageUpload> {
        self.call(
            "startStorageFileUpload",
            serde_json::json!({ "filename": filename, "size": size }),
        )
        .await
    }

    pub async fn mount_storage(&self, filename: &str, mode: VirtualMediaMode) -> Result<()> {
        self.call::<_, Value>(
            "mountWithStorage",
            serde_json::json!({ "filename": filename, "mode": mode }),
        )
        .await
        .map(|_| ())
    }

    pub async fn delete_storage_file(&self, filename: &str) -> Result<()> {
        self.call::<_, Value>(
            "deleteStorageFile",
            serde_json::json!({ "filename": filename }),
        )
        .await
        .map(|_| ())
    }
    pub async fn scroll(&self, wheel_x: i8, wheel_y: i8) -> Result<()> {
        self.call::<_, Value>(
            "wheelReport",
            serde_json::json!({ "wheelX": wheel_x, "wheelY": wheel_y }),
        )
        .await
        .map(|_| ())
    }
}

impl Drop for RpcClient {
    fn drop(&mut self) {
        if Arc::strong_count(&self.pending) == 1 {
            self.cancel_generation();
        }
    }
}
async fn wait_for_response(
    pending: &PendingMap,
    id: u64,
    method: &str,
    receiver: oneshot::Receiver<PendingResult>,
    timeout: Duration,
) -> Result<Value> {
    match tokio::time::timeout(timeout, receiver).await {
        Ok(Ok(Ok(value))) => Ok(value),
        Ok(Ok(Err(error))) => Err(error.into()),
        Ok(Err(_)) => anyhow::bail!("RPC response channel closed"),
        Err(_) => {
            pending.lock().remove(&id);
            anyhow::bail!("RPC request timed out: {method}")
        }
    }
}

fn cancel_pending(pending: &PendingMap) {
    let pending = std::mem::take(&mut *pending.lock());
    for (_, response) in pending {
        let _ = response.send(Err(RpcError::connection_changed()));
    }
}

fn parse_response(data: &[u8], pending: &PendingMap) -> Option<(Response, bool)> {
    let raw = serde_json::from_slice::<Value>(data).ok()?;
    let id = raw.get("id").and_then(Value::as_u64);
    let has_result = raw.get("result").is_some();
    match serde_json::from_value::<Response>(raw) {
        Ok(response) => Some((response, has_result)),
        Err(error) => {
            if let Some(id) = id
                && let Some(sender) = pending.lock().remove(&id)
            {
                let _ = sender.send(Err(RpcError::malformed(format!(
                    "malformed RPC response: {error}"
                ))));
            }
            None
        }
    }
}

fn complete_response(pending: &PendingMap, response: Response, has_result: bool) {
    let Some(id) = response.id else {
        return;
    };
    let Some(sender) = pending.lock().remove(&id) else {
        return;
    };
    let result = if let Some(error) = response.error {
        Err(error)
    } else if has_result {
        Ok(response.result.unwrap_or(Value::Null))
    } else {
        Ok(Value::Null)
    };
    let _ = sender.send(result);
}

fn install_message_handler(
    channel: &Arc<RTCDataChannel>,
    pending: PendingMap,
    notifications: broadcast::Sender<RpcNotification>,
    response_channel: Arc<RTCDataChannel>,
) {
    channel.on_message(Box::new(move |message: DataChannelMessage| {
        let pending = Arc::clone(&pending);
        let notifications = notifications.clone();
        let response_channel = Arc::clone(&response_channel);
        Box::pin(async move {
            let Some((response, has_result)) = parse_response(&message.data, &pending) else {
                return;
            };
            if let Some(method) = response.method {
                let notification = RpcNotification {
                    method,
                    params: response.params,
                    id: response.id.map(Value::from),
                };
                let _ = notifications.send(notification);

                if let Some(id) = response.id {
                    let payload = serde_json::json!({
                        "jsonrpc": JSON_RPC_VERSION,
                        "id": id,
                        "error": {
                            "code": -32601,
                            "message": "client method not supported"
                        }
                    });
                    let _ = response_channel.send_text(payload.to_string()).await;
                }
                return;
            }

            complete_response(&pending, response, has_result);
        })
    }));
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum VirtualMediaMode {
    #[serde(rename = "CDROM")]
    Cdrom,
    #[serde(rename = "Disk")]
    Disk,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VirtualMediaState {
    pub source: String,
    pub mode: VirtualMediaMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MountUrlInfo {
    pub usable: bool,
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub size: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageSpace {
    #[serde(rename = "bytesUsed")]
    pub bytes_used: u64,
    #[serde(rename = "bytesFree")]
    pub bytes_free: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageFile {
    pub filename: String,
    pub size: u64,
    #[serde(rename = "createdAt")]
    pub created_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageFiles {
    pub files: Vec<StorageFile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StorageUpload {
    #[serde(rename = "alreadyUploadedBytes")]
    pub already_uploaded_bytes: u64,
    #[serde(rename = "dataChannel")]
    pub upload_id: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_media_state_wire_shape() {
        let state: VirtualMediaState = serde_json::from_value(serde_json::json!({
            "source": "HTTP",
            "mode": "CDROM",
            "url": "http://example.invalid/image.iso",
            "size": 4096
        }))
        .expect("media state should decode");
        assert_eq!(state.mode, VirtualMediaMode::Cdrom);
        assert_eq!(state.size, 4096);
    }

    #[test]
    fn decodes_structured_remote_error() {
        let response: Response = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 7,
            "error": { "code": -32000, "message": "not available" }
        }))
        .expect("response should decode");
        assert_eq!(response.error.expect("error expected").code, -32000);
    }

    #[tokio::test]
    async fn correlates_out_of_order_and_empty_success_responses() {
        let pending = PendingMap::default();
        let (first_tx, mut first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        pending.lock().insert(1, first_tx);
        pending.lock().insert(2, second_tx);

        let response: Response =
            serde_json::from_value(serde_json::json!({"jsonrpc": "2.0", "id": 2}))
                .expect("empty success response");
        complete_response(&pending, response, false);

        assert_eq!(second_rx.await.unwrap().unwrap(), Value::Null);
        assert!(matches!(
            first_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(pending.lock().contains_key(&1));
    }

    #[tokio::test]
    async fn timeout_removes_pending_request() {
        let pending = PendingMap::default();
        let (sender, receiver) = oneshot::channel();
        pending.lock().insert(7, sender);

        let error = wait_for_response(
            &pending,
            7,
            "slowMethod",
            receiver,
            Duration::from_millis(1),
        )
        .await
        .expect_err("request should time out");

        assert!(error.to_string().contains("slowMethod"));
        assert!(pending.lock().is_empty());
    }

    #[tokio::test]
    async fn generation_cancellation_fails_every_pending_request() {
        let pending = PendingMap::default();
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        pending.lock().insert(1, first_tx);
        pending.lock().insert(2, second_tx);

        cancel_pending(&pending);

        for receiver in [first_rx, second_rx] {
            let error = receiver
                .await
                .unwrap()
                .expect_err("request should be cancelled");
            assert_eq!(error.code, -32001);
        }
        assert!(pending.lock().is_empty());
    }

    #[tokio::test]
    async fn malformed_correlated_response_fails_request() {
        let pending = PendingMap::default();
        let (sender, receiver) = oneshot::channel();
        pending.lock().insert(9, sender);

        assert!(parse_response(br#"{"id":9,"error":"invalid"}"#, &pending).is_none());

        let error = receiver
            .await
            .unwrap()
            .expect_err("malformed response should fail");
        assert_eq!(error.code, -32700);
        assert!(pending.lock().is_empty());
    }
}
