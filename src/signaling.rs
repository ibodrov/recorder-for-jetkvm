use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use base64::prelude::*;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio_tungstenite::Connector;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tracing::{debug, warn};
use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use crate::auth::AuthenticatedClient;

const SIGNALING_TIMEOUT: Duration = Duration::from_secs(15);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(15);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalingMode {
    WebSocket,
    LegacyHttp,
}

pub struct EstablishedSignaling {
    pub mode: SignalingMode,
    pub device_version: Option<String>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl EstablishedSignaling {
    pub async fn shutdown(mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
            let _ = task.await;
        }
    }
}

impl Drop for EstablishedSignaling {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

#[derive(Serialize, Deserialize)]
struct SdpMessage {
    #[serde(rename = "type")]
    sdp_type: String,
    sdp: String,
}

#[derive(Serialize)]
struct SessionRequest {
    sd: String,
}

#[derive(Deserialize)]
struct SessionResponse {
    sd: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct SignalMessage {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default)]
    data: Value,
}

fn encode_sdp(desc: &RTCSessionDescription) -> Result<String> {
    let msg = SdpMessage {
        sdp_type: desc.sdp_type.to_string().to_lowercase(),
        sdp: desc.sdp.clone(),
    };
    let json = serde_json::to_string(&msg).context("failed to serialize SDP")?;
    Ok(BASE64_STANDARD.encode(json.as_bytes()))
}

fn decode_sdp(encoded: &str) -> Result<RTCSessionDescription> {
    let json_bytes = BASE64_STANDARD
        .decode(encoded)
        .context("failed to decode base64 SDP")?;
    let msg: SdpMessage =
        serde_json::from_slice(&json_bytes).context("failed to parse SDP JSON")?;
    debug!(sdp_type = %msg.sdp_type, "decoded remote SDP");
    match msg.sdp_type.as_str() {
        "answer" => RTCSessionDescription::answer(msg.sdp).context("failed to parse SDP answer"),
        "offer" => RTCSessionDescription::offer(msg.sdp).context("failed to parse SDP offer"),
        other => anyhow::bail!("unknown SDP type: {other}"),
    }
}

pub async fn establish(
    auth: &AuthenticatedClient,
    peer_connection: Arc<RTCPeerConnection>,
    offer: &RTCSessionDescription,
    local_candidates: mpsc::Receiver<RTCIceCandidateInit>,
    mut gathering_complete: mpsc::Receiver<()>,
) -> Result<(RTCSessionDescription, EstablishedSignaling)> {
    match establish_websocket(auth, Arc::clone(&peer_connection), offer, local_candidates).await {
        Ok(established) => Ok(established),
        Err(_) => {
            warn!("WebSocket signaling unavailable; using legacy HTTP fallback");
            tokio::time::timeout(SIGNALING_TIMEOUT, gathering_complete.recv())
                .await
                .context("ICE gathering timed out for legacy signaling")?;
            let complete_offer = peer_connection
                .local_description()
                .await
                .context("no local description after ICE gathering")?;
            let answer = exchange_sdp(auth.client(), auth.base_url(), &complete_offer).await?;
            Ok((
                answer,
                EstablishedSignaling {
                    mode: SignalingMode::LegacyHttp,
                    device_version: None,
                    task: None,
                },
            ))
        }
    }
}

async fn establish_websocket(
    auth: &AuthenticatedClient,
    peer_connection: Arc<RTCPeerConnection>,
    offer: &RTCSessionDescription,
    mut local_candidates: mpsc::Receiver<RTCIceCandidateInit>,
) -> Result<(RTCSessionDescription, EstablishedSignaling)> {
    let (mut socket, device_version) = open_websocket(auth).await?;
    let encoded_offer = encode_sdp(offer)?;
    let message = SignalMessage {
        kind: "offer".to_owned(),
        data: serde_json::json!({ "sd": encoded_offer }),
    };
    socket
        .send(Message::Text(serde_json::to_string(&message)?.into()))
        .await
        .context("failed to send WebSocket SDP offer")?;

    let answer = tokio::time::timeout(SIGNALING_TIMEOUT, async {
        loop {
            tokio::select! {
                candidate = local_candidates.recv() => {
                    if let Some(candidate) = candidate {
                        send_candidate(&mut socket, &candidate).await?;
                    }
                }
                incoming = socket.next() => {
                    let message = incoming
                        .context("WebSocket signaling ended before SDP answer")?
                        .context("failed to read WebSocket signaling message")?;
                    if let Some(answer) = process_signal(message, &peer_connection).await? {
                        break Ok::<_, anyhow::Error>(answer);
                    }
                }
            }
        }
    })
    .await
    .context("WebSocket signaling timed out")??;

    let task_peer = Arc::clone(&peer_connection);
    let task = tokio::spawn(async move {
        let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
        loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    if socket.send(Message::Ping(Vec::new().into())).await.is_err() {
                        return;
                    }
                }
                candidate = local_candidates.recv() => {
                    let Some(candidate) = candidate else {
                        return;
                    };
                    if send_candidate(&mut socket, &candidate).await.is_err() {
                        return;
                    }
                }
                incoming = socket.next() => {
                    let Some(Ok(message)) = incoming else {
                        return;
                    };
                    if process_signal(message, &task_peer).await.is_err() {
                        return;
                    }
                }
            }
        }
    });
    Ok((
        answer,
        EstablishedSignaling {
            mode: SignalingMode::WebSocket,
            device_version,
            task: Some(task),
        },
    ))
}

async fn open_websocket(
    auth: &AuthenticatedClient,
) -> Result<(
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    Option<String>,
)> {
    let mut url = reqwest::Url::parse(auth.base_url()).context("invalid JetKVM URL")?;
    url.set_scheme(if url.scheme() == "https" { "wss" } else { "ws" })
        .map_err(|_| anyhow::anyhow!("unsupported JetKVM URL scheme"))?;
    url.set_path("/webrtc/signaling/client");
    url.set_query(None);
    let mut request = url
        .as_str()
        .into_client_request()
        .context("failed to build WebSocket request")?;
    request.headers_mut().insert(
        "Origin",
        auth.base_url()
            .parse()
            .context("invalid WebSocket Origin header")?,
    );
    if let Some(cookie) = auth.cookie_header()? {
        request.headers_mut().insert(
            "Cookie",
            cookie
                .to_str()
                .context("invalid authentication cookie")?
                .parse()
                .context("invalid WebSocket Cookie header")?,
        );
    }
    let connector = if auth.no_tls_verify() && url.scheme() == "wss" {
        Some(Connector::NativeTls(
            native_tls::TlsConnector::builder()
                .danger_accept_invalid_certs(true)
                .build()
                .context("failed to build insecure TLS connector")?,
        ))
    } else {
        None
    };
    let (mut socket, _) =
        tokio_tungstenite::connect_async_tls_with_config(request, None, false, connector)
            .await
            .context("failed to open WebSocket signaling")?;

    let device_version = tokio::time::timeout(SIGNALING_TIMEOUT, async {
        loop {
            let message = socket
                .next()
                .await
                .context("WebSocket closed before device metadata")?
                .context("failed to read device metadata")?;
            if let Message::Text(text) = message {
                let signal: SignalMessage =
                    serde_json::from_str(&text).context("invalid signaling metadata")?;
                if signal.kind == "device-metadata" {
                    return Ok::<_, anyhow::Error>(
                        signal
                            .data
                            .get("deviceVersion")
                            .and_then(Value::as_str)
                            .map(str::to_owned),
                    );
                }
            }
        }
    })
    .await
    .context("timed out waiting for device metadata")??;
    Ok((socket, device_version))
}

async fn send_candidate<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    candidate: &RTCIceCandidateInit,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let message = SignalMessage {
        kind: "new-ice-candidate".to_owned(),
        data: serde_json::to_value(candidate)?,
    };
    socket
        .send(Message::Text(serde_json::to_string(&message)?.into()))
        .await
        .context("failed to send ICE candidate")?;
    Ok(())
}

async fn process_signal(
    message: Message,
    peer_connection: &Arc<RTCPeerConnection>,
) -> Result<Option<RTCSessionDescription>> {
    let Message::Text(text) = message else {
        return Ok(None);
    };
    if text == "pong" {
        return Ok(None);
    }
    let signal: SignalMessage =
        serde_json::from_str(&text).context("invalid WebSocket signaling message")?;
    match signal.kind.as_str() {
        "answer" => {
            let encoded = signal
                .data
                .as_str()
                .context("WebSocket answer did not contain encoded SDP")?;
            Ok(Some(decode_sdp(encoded)?))
        }
        "new-ice-candidate" => {
            let candidate: RTCIceCandidateInit =
                serde_json::from_value(signal.data).context("invalid remote ICE candidate")?;
            if !candidate.candidate.is_empty() {
                peer_connection
                    .add_ice_candidate(candidate)
                    .await
                    .context("failed to add remote ICE candidate")?;
            }
            Ok(None)
        }
        _ => Ok(None),
    }
}

pub async fn exchange_sdp(
    client: &reqwest::Client,
    host: &str,
    offer: &RTCSessionDescription,
) -> Result<RTCSessionDescription> {
    let encoded_offer = encode_sdp(offer)?;
    let base = crate::auth::base_url(host);
    let url = format!("{base}/webrtc/session");

    debug!("sending legacy SDP offer");
    let resp = client
        .post(&url)
        .json(&SessionRequest { sd: encoded_offer })
        .send()
        .await
        .context("failed to send legacy SDP offer")?;
    let status = resp.status();
    if !status.is_success() {
        anyhow::bail!("legacy signaling failed (HTTP {status})");
    }
    let session_resp: SessionResponse = resp
        .json()
        .await
        .context("failed to parse signaling response")?;
    decode_sdp(&session_resp.sd)
}
