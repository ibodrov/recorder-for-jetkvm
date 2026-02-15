use anyhow::{Context, Result};
use base64::prelude::*;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tracing::debug;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

use crate::auth;

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

pub async fn exchange_sdp(
    client: &Client,
    host: &str,
    offer: &RTCSessionDescription,
) -> Result<RTCSessionDescription> {
    let encoded_offer = encode_sdp(offer)?;
    let base = auth::base_url(host);
    let url = format!("{base}/webrtc/session");

    debug!("sending SDP offer to {url}");
    let resp = client
        .post(&url)
        .json(&SessionRequest { sd: encoded_offer })
        .send()
        .await
        .context("failed to send SDP offer")?;

    let status = resp.status();
    if !status.is_success() {
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("signaling failed (HTTP {status}): {body}");
    }

    let session_resp: SessionResponse = resp
        .json()
        .await
        .context("failed to parse signaling response")?;

    let answer = decode_sdp(&session_resp.sd)?;
    Ok(answer)
}
