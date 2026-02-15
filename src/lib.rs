pub mod auth;
pub mod config;
pub mod detector;
pub mod h264;
pub mod paths;
pub mod recorder;
pub mod screenshot;
pub mod session;
pub mod signaling;

use anyhow::Result;
use h264::NalUnit;
use tokio::sync::{broadcast, watch};

/// A source of H.264 NAL units.
///
/// Implementations produce NAL units from different transports (WebRTC, RTSP,
/// file, etc.) and send them on a broadcast channel until the source is
/// exhausted or a shutdown signal is received.
///
/// # Example
///
/// The JetKVM WebRTC session implements this trait by authenticating with the
/// device, establishing a WebRTC peer connection, depacketizing RTP packets
/// into NAL units, and forwarding them to `nal_tx`.
pub trait VideoSource {
    /// Start producing NAL units.
    ///
    /// Implementations should send [`NalUnit`]s on `nal_tx` until:
    /// - The underlying transport closes (return `Ok(())`),
    /// - A shutdown signal is received on `shutdown` (return `Ok(())`), or
    /// - An unrecoverable error occurs (return `Err(...)`).
    fn run(
        &self,
        nal_tx: broadcast::Sender<NalUnit>,
        shutdown: watch::Receiver<bool>,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}
