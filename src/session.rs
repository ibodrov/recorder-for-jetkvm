use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use reqwest::Client;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};
use webrtc::api::APIBuilder;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::rtp::packet::Packet;
use webrtc::rtp_transceiver::rtp_codec::RTPCodecType;
use webrtc::track::track_remote::TrackRemote;

use crate::signaling;

#[derive(Debug)]
pub struct ConnectionLostError;

impl std::fmt::Display for ConnectionLostError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WebRTC connection lost")
    }
}

impl std::error::Error for ConnectionLostError {}

pub fn is_connection_lost_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<ConnectionLostError>().is_some()
}

pub async fn run(
    client: &Client,
    host: &str,
    rtp_tx: mpsc::Sender<Packet>,
    pli_interval: Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;

    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut media_engine)?;

    let api = APIBuilder::new()
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .build();

    let config = RTCConfiguration::default();
    let peer_connection = Arc::new(api.new_peer_connection(config).await?);

    peer_connection
        .add_transceiver_from_kind(RTPCodecType::Video, None)
        .await
        .context("failed to add video transceiver")?;

    // Channel to notify when the connection fails
    let (conn_fail_tx, mut conn_fail_rx) = mpsc::channel::<()>(1);

    peer_connection.on_peer_connection_state_change(Box::new(
        move |state: RTCPeerConnectionState| {
            match state {
                RTCPeerConnectionState::Connected => info!("WebRTC connected"),
                RTCPeerConnectionState::Failed | RTCPeerConnectionState::Disconnected => {
                    warn!("WebRTC connection lost: {state}");
                    let _ = conn_fail_tx.try_send(());
                }
                _ => debug!("peer connection state: {state}"),
            }
            Box::pin(async {})
        },
    ));

    let rtp_tx_clone = rtp_tx.clone();
    let shutdown_clone = shutdown.clone();
    peer_connection.on_track(Box::new(
        move |track: Arc<TrackRemote>, _receiver, _transceiver| {
            let rtp_tx = rtp_tx_clone.clone();
            let mut shutdown = shutdown_clone.clone();
            let codec = track.codec();
            debug!(
                mime = %codec.capability.mime_type,
                pt = codec.payload_type,
                "received track"
            );

            if !codec.capability.mime_type.to_lowercase().contains("h264") {
                warn!("ignoring non-H264 track: {}", codec.capability.mime_type);
                return Box::pin(async {});
            }

            Box::pin(async move {
                debug!("starting H.264 video track read loop");
                let mut buf = vec![0u8; 65535];
                loop {
                    tokio::select! {
                        result = track.read(&mut buf) => {
                            match result {
                                Ok((pkt, _)) => {
                                    if rtp_tx.send(pkt).await.is_err() {
                                        debug!("RTP channel closed, stopping read loop");
                                        return;
                                    }
                                }
                                Err(e) => {
                                    error!("track read error: {e}");
                                    return;
                                }
                            }
                        }
                        _ = shutdown.changed() => {
                            info!("shutdown signal received, stopping track read loop");
                            return;
                        }
                    }
                }
            })
        },
    ));

    let offer = peer_connection.create_offer(None).await?;
    peer_connection.set_local_description(offer.clone()).await?;

    // Wait for ICE gathering to complete
    let mut gather_complete = peer_connection.gathering_complete_promise().await;
    let _ = gather_complete.recv().await;

    let local_desc = peer_connection
        .local_description()
        .await
        .context("no local description after ICE gathering")?;

    debug!("exchanging SDP with JetKVM");
    let answer = signaling::exchange_sdp(client, host, &local_desc).await?;
    peer_connection.set_remote_description(answer).await?;
    info!("WebRTC session established");

    // Periodic PLI requests to get keyframes
    let pc_pli = Arc::clone(&peer_connection);
    let mut shutdown_pli = shutdown.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(pli_interval);
        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let receivers = pc_pli.get_receivers().await;
                    for receiver in &receivers {
                        let tracks = receiver.tracks().await;
                        for track in &tracks {
                            let ssrc = track.ssrc();
                            if let Err(e) = pc_pli
                                .write_rtcp(&[Box::new(
                                    webrtc::rtcp::payload_feedbacks::picture_loss_indication::PictureLossIndication {
                                        sender_ssrc: 0,
                                        media_ssrc: ssrc,
                                    },
                                )])
                                .await
                            {
                                debug!("failed to send PLI: {e}");
                            }
                        }
                    }
                }
                _ = shutdown_pli.changed() => {
                    debug!("PLI sender shutting down");
                    return;
                }
            }
        }
    });

    // Wait for shutdown or connection failure
    let mut shutdown_wait = shutdown.clone();
    tokio::select! {
        _ = shutdown_wait.changed() => {
            info!("closing peer connection");
            peer_connection.close().await?;
            Ok(())
        }
        _ = conn_fail_rx.recv() => {
            warn!("WebRTC connection failed, closing peer connection");
            peer_connection.close().await?;
            anyhow::bail!(ConnectionLostError)
        }
    }
}
