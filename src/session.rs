use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};
use webrtc::api::APIBuilder;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::{MIME_TYPE_H264, MediaEngine};
use webrtc::data_channel::RTCDataChannel;
use webrtc::data_channel::data_channel_init::RTCDataChannelInit;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::rtp::packet::Packet;
use webrtc::rtp_transceiver::RTCPFeedback;
use webrtc::rtp_transceiver::rtp_codec::{
    RTCRtpCodecCapability, RTCRtpCodecParameters, RTPCodecType,
};
use webrtc::track::track_remote::TrackRemote;

use crate::auth::AuthenticatedClient;
use crate::h264::{self, NalUnit};
use crate::hid::HidClient;
use crate::rpc::RpcClient;
use crate::signaling::{self, EstablishedSignaling, SignalingMode};

const CONNECTION_TIMEOUT: Duration = Duration::from_secs(20);
const H264_PROFILES: &[(u8, &str)] = &[
    (
        102,
        "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42001f",
    ),
    (
        127,
        "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42001f",
    ),
    (
        125,
        "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=42e01f",
    ),
    (
        108,
        "level-asymmetry-allowed=1;packetization-mode=0;profile-level-id=42e01f",
    ),
    (
        123,
        "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640032",
    ),
    (
        118,
        "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640028",
    ),
    (
        119,
        "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640029",
    ),
    (
        120,
        "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=64002a",
    ),
    (
        121,
        "level-asymmetry-allowed=1;packetization-mode=1;profile-level-id=640033",
    ),
];

pub(crate) struct SessionConnection {
    peer_connection: Arc<RTCPeerConnection>,
    rpc: RpcClient,
    hid: HidClient,
    signaling: EstablishedSignaling,
    device_version: Option<String>,
    state_rx: mpsc::Receiver<RTCPeerConnectionState>,
    cancellation: CancellationToken,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl SessionConnection {
    pub async fn connect(
        auth: AuthenticatedClient,
        generation: u64,
        nal_tx: broadcast::Sender<NalUnit>,
        pli_interval: Duration,
        keyframe_tx: mpsc::Sender<()>,
        mut keyframe_rx: mpsc::Receiver<()>,
    ) -> Result<Self> {
        let mut media_engine = MediaEngine::default();
        register_h264_codecs(&mut media_engine)?;
        let mut registry = Registry::new();
        registry = register_default_interceptors(registry, &mut media_engine)?;
        let api = APIBuilder::new()
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .build();
        let peer_connection = Arc::new(api.new_peer_connection(RTCConfiguration::default()).await?);
        peer_connection
            .add_transceiver_from_kind(RTPCodecType::Video, None)
            .await
            .context("failed to add video transceiver")?;

        let rpc_channel = peer_connection
            .create_data_channel("rpc", None)
            .await
            .context("failed to create RPC data channel")?;
        let reliable_hid = peer_connection
            .create_data_channel("hidrpc", None)
            .await
            .context("failed to create reliable HID data channel")?;
        let unreliable_ordered = peer_connection
            .create_data_channel(
                "hidrpc-unreliable-ordered",
                Some(RTCDataChannelInit {
                    ordered: Some(true),
                    max_retransmits: Some(0),
                    ..Default::default()
                }),
            )
            .await
            .context("failed to create ordered unreliable HID data channel")?;
        let unreliable_unordered = peer_connection
            .create_data_channel(
                "hidrpc-unreliable-nonordered",
                Some(RTCDataChannelInit {
                    ordered: Some(false),
                    max_retransmits: Some(0),
                    ..Default::default()
                }),
            )
            .await
            .context("failed to create unordered unreliable HID data channel")?;

        let rpc = RpcClient::new(Arc::clone(&rpc_channel));
        let hid = HidClient::new(
            Arc::clone(&reliable_hid),
            Arc::clone(&unreliable_ordered),
            Arc::clone(&unreliable_unordered),
        );
        let cancellation = CancellationToken::new();
        let (state_tx, mut state_rx) = mpsc::channel(16);
        peer_connection.on_peer_connection_state_change(Box::new(move |state| {
            let state_tx = state_tx.clone();
            Box::pin(async move {
                let _ = state_tx.send(state).await;
            })
        }));

        let (rtp_tx, rtp_rx) = mpsc::channel::<Packet>(1024);
        let track_cancellation = cancellation.clone();
        let track_keyframe = keyframe_tx.clone();
        peer_connection.on_track(Box::new(
            move |track: Arc<TrackRemote>, _receiver, _transceiver| {
                let rtp_tx = rtp_tx.clone();
                let cancellation = track_cancellation.clone();
                let keyframe_tx = track_keyframe.clone();
                let codec = track.codec();
                if !codec.capability.mime_type.eq_ignore_ascii_case(MIME_TYPE_H264) {
                    warn!(mime = %codec.capability.mime_type, "ignoring unsupported video track");
                    return Box::pin(async {});
                }
                Box::pin(async move {
                    let mut buffer = vec![0_u8; 65_535];
                    let mut expected_sequence = None;
                    loop {
                        tokio::select! {
                            _ = cancellation.cancelled() => return,
                            result = track.read(&mut buffer) => match result {
                                Ok((packet, _)) => {
                                    if expected_sequence.is_some_and(|expected| expected != packet.header.sequence_number) {
                                        let _ = keyframe_tx.try_send(());
                                    }
                                    expected_sequence = Some(packet.header.sequence_number.wrapping_add(1));
                                    if rtp_tx.send(packet).await.is_err() {
                                        return;
                                    }
                                }
                                Err(error) => {
                                    debug!(%error, "video track read ended");
                                    return;
                                }
                            }
                        }
                    }
                })
            },
        ));

        let (candidate_tx, candidate_rx) = mpsc::channel(64);
        peer_connection.on_ice_candidate(Box::new(move |candidate| {
            let candidate_tx = candidate_tx.clone();
            Box::pin(async move {
                if let Some(candidate) = candidate
                    && let Ok(candidate) = candidate.to_json()
                {
                    let _ = candidate_tx.send(candidate).await;
                }
            })
        }));

        let depacketizer = tokio::spawn(async move {
            h264::depacketize(rtp_rx, nal_tx).await;
        });
        let offer = peer_connection.create_offer(None).await?;
        peer_connection.set_local_description(offer).await?;
        let mut gathered = peer_connection.gathering_complete_promise().await;
        let _ = gathered.recv().await;
        let local_description = peer_connection
            .local_description()
            .await
            .context("no local description after ICE gathering")?;
        let (answer, signaling) = signaling::establish(
            &auth,
            Arc::clone(&peer_connection),
            &local_description,
            candidate_rx,
        )
        .await?;
        let device_version = signaling.device_version.clone();
        peer_connection.set_remote_description(answer).await?;
        info!(?generation, ?device_version, mode = ?signaling.mode, "WebRTC session established");

        tokio::time::timeout(CONNECTION_TIMEOUT, async {
            loop {
                match state_rx.recv().await {
                    Some(RTCPeerConnectionState::Connected) => return Ok::<_, anyhow::Error>(()),
                    Some(RTCPeerConnectionState::Failed | RTCPeerConnectionState::Closed) => {
                        anyhow::bail!("WebRTC connection failed during setup")
                    }
                    Some(state) => debug!(?state, "peer connection state during setup"),
                    None => anyhow::bail!("peer connection state channel closed"),
                }
            }
        })
        .await
        .context("timed out connecting WebRTC")??;
        rpc.wait_ready(Duration::from_secs(10)).await?;
        hid.wait_ready(Duration::from_secs(10)).await?;

        let pli_peer = Arc::clone(&peer_connection);
        let pli_cancellation = cancellation.clone();
        let pli_task = tokio::spawn(async move {
            let mut interval = tokio::time::interval(pli_interval);
            loop {
                tokio::select! {
                    _ = pli_cancellation.cancelled() => return,
                    _ = interval.tick() => request_keyframe(&pli_peer).await,
                    request = keyframe_rx.recv() => {
                        if request.is_none() {
                            return;
                        }
                        request_keyframe(&pli_peer).await;
                    }
                }
            }
        });

        Ok(Self {
            peer_connection,
            rpc,
            hid,
            signaling,
            device_version,
            state_rx,
            cancellation,
            tasks: vec![depacketizer, pli_task],
        })
    }

    pub fn rpc(&self) -> &RpcClient {
        &self.rpc
    }

    pub fn hid(&self) -> &HidClient {
        &self.hid
    }

    pub fn device_version(&self) -> Option<&str> {
        self.device_version.as_deref()
    }

    pub fn signaling_mode(&self) -> SignalingMode {
        self.signaling.mode
    }

    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }

    pub async fn wait_for_end(&mut self) -> RTCPeerConnectionState {
        loop {
            match self.state_rx.recv().await {
                Some(
                    state @ (RTCPeerConnectionState::Failed
                    | RTCPeerConnectionState::Disconnected
                    | RTCPeerConnectionState::Closed),
                ) => return state,
                Some(state) => debug!(?state, "peer connection state"),
                None => return RTCPeerConnectionState::Closed,
            }
        }
    }

    pub async fn shutdown(mut self) -> Result<()> {
        self.cancellation.cancel();
        let _ = self.hid.reset().await;
        self.rpc.cancel_generation();
        self.peer_connection
            .close()
            .await
            .context("failed to close peer connection")?;
        self.signaling.shutdown().await;
        for task in self.tasks.drain(..) {
            task.abort();
            let _ = task.await;
        }
        Ok(())
    }
}

async fn request_keyframe(peer_connection: &Arc<RTCPeerConnection>) {
    for receiver in peer_connection.get_receivers().await {
        for track in receiver.tracks().await {
            let request =
                webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                    sender_ssrc: 0,
                    media_ssrc: track.ssrc(),
                };
            if let Err(error) = peer_connection.write_rtcp(&[Box::new(request)]).await {
                debug!(%error, "failed to request video keyframe");
            }
        }
    }
}

fn register_h264_codecs(media_engine: &mut MediaEngine) -> Result<()> {
    let feedback = vec![
        RTCPFeedback {
            typ: "goog-remb".to_owned(),
            parameter: String::new(),
        },
        RTCPFeedback {
            typ: "ccm".to_owned(),
            parameter: "fir".to_owned(),
        },
        RTCPFeedback {
            typ: "nack".to_owned(),
            parameter: String::new(),
        },
        RTCPFeedback {
            typ: "nack".to_owned(),
            parameter: "pli".to_owned(),
        },
    ];
    for &(payload_type, format) in H264_PROFILES {
        media_engine.register_codec(
            RTCRtpCodecParameters {
                capability: RTCRtpCodecCapability {
                    mime_type: MIME_TYPE_H264.to_owned(),
                    clock_rate: 90_000,
                    channels: 0,
                    sdp_fmtp_line: format.to_owned(),
                    rtcp_feedback: feedback.clone(),
                },
                payload_type,
                ..Default::default()
            },
            RTPCodecType::Video,
        )?;
    }
    Ok(())
}

pub async fn wait_data_channel_open(
    channel: &Arc<RTCDataChannel>,
    timeout: Duration,
) -> Result<()> {
    tokio::time::timeout(timeout, async {
        while channel.ready_state()
            != webrtc::data_channel::data_channel_state::RTCDataChannelState::Open
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .context("data channel did not open")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn offer_advertises_data_transport_and_only_h264_video() {
        let mut media_engine = MediaEngine::default();
        register_h264_codecs(&mut media_engine).expect("H.264 codecs should register");
        let mut registry = Registry::new();
        registry = register_default_interceptors(registry, &mut media_engine)
            .expect("interceptors should register");
        let api = APIBuilder::new()
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .build();
        let peer = api
            .new_peer_connection(RTCConfiguration::default())
            .await
            .expect("peer connection");
        peer.add_transceiver_from_kind(RTPCodecType::Video, None)
            .await
            .expect("video transceiver");
        for label in [
            "rpc",
            "hidrpc",
            "hidrpc-unreliable-ordered",
            "hidrpc-unreliable-nonordered",
        ] {
            peer.create_data_channel(label, None)
                .await
                .expect("data channel");
        }
        let offer = peer.create_offer(None).await.expect("SDP offer");
        assert!(offer.sdp.contains("H264/90000"));
        for unsupported in ["VP8/90000", "VP9/90000", "AV1/90000", "H265/90000"] {
            assert!(!offer.sdp.contains(unsupported), "advertised {unsupported}");
        }
        assert!(offer.sdp.contains("m=application"));
        assert!(offer.sdp.contains("webrtc-datachannel"));
        peer.close().await.expect("peer close");
    }
}
