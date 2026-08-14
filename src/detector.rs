use std::time::{Duration, Instant};

use anyhow::{Result, anyhow};
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{debug, info, warn};

use crate::h264::{self, NalUnit};

#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub timestamp: Instant,
    pub changed_fraction: f64,
}

/// Detect screen changes by analyzing compressed H.264 frame sizes.
///
/// Instead of decoding frames and comparing pixels, this uses the observation
/// that P-frame size correlates with the amount of visual change:
/// - Static screen: P-frames are tiny (encoder sends mostly skip macroblocks)
/// - Screen change: P-frames grow large (encoder must encode the differences)
///
/// The IDR (keyframe) size serves as reference for "full frame worth of data".
/// A change is detected when the largest P-frame in a check interval exceeds
/// `sensitivity × IDR_size`.
pub async fn run(
    mut nal_rx: broadcast::Receiver<NalUnit>,
    change_tx: mpsc::Sender<ChangeEvent>,
    check_interval: Duration,
    sensitivity: f64,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut last_check = Instant::now();
    let mut ref_size: Option<usize> = None;
    let mut max_p_size: usize = 0;

    debug!("change detector initialized (frame-size mode, sensitivity={sensitivity})");

    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() && !*shutdown.borrow() {
                    return Err(anyhow!("detector shutdown channel closed unexpectedly"));
                }
                info!("shutdown signal received, detector exiting");
                return Ok(());
            }
            recv_result = nal_rx.recv() => match recv_result {
            Ok(nal) => {

                // Use IDR frame size as the reference for "full screen of data"
                if let Some(nal_type) = nal.nal_type()
                    && nal_type == h264::NAL_TYPE_IDR
                {
                    let size = nal.data.len();
                    if ref_size.is_none() {
                        debug!("IDR reference size: {size} bytes");
                    }
                    ref_size = Some(size);
                    continue;
                }

                // Skip SPS/PPS — small metadata NALs, not picture content
                if nal.is_keyframe {
                    continue;
                }

                // Track the largest P-frame in the current check interval
                max_p_size = max_p_size.max(nal.data.len());

                // Only evaluate at the configured interval
                if last_check.elapsed() < check_interval {
                    continue;
                }
                last_check = Instant::now();

                if let Some(ref_sz) = ref_size {
                    let threshold = (sensitivity * ref_sz as f64) as usize;

                    debug!(
                        max_p_frame = max_p_size,
                        ref_size = ref_sz,
                        threshold,
                        "frame size check"
                    );

                    if max_p_size > threshold {
                        let fraction = (max_p_size as f64 / ref_sz as f64).min(1.0);
                        info!(
                            max_p_frame = max_p_size,
                            threshold,
                            changed = format!("{:.2}%", fraction * 100.0),
                            "change detected (frame size)"
                        );
                        let _ = change_tx.try_send(ChangeEvent {
                            timestamp: Instant::now(),
                            changed_fraction: fraction,
                        });
                    }
                }

                max_p_size = 0;
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                warn!("detector lagged, missed {n} NAL units");
            }
            Err(broadcast::error::RecvError::Closed) => {
                return Err(anyhow!("detector NAL stream closed unexpectedly"));
            }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[tokio::test]
    async fn test_detector_exits_on_shutdown_signal() {
        let (_nal_tx, nal_rx) = broadcast::channel(8);
        let (change_tx, mut change_rx) = mpsc::channel(8);
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let handle = tokio::spawn(run(
            nal_rx,
            change_tx,
            Duration::from_millis(10),
            0.02,
            shutdown_rx,
        ));

        shutdown_tx
            .send(true)
            .expect("failed to send shutdown signal");

        timeout(Duration::from_secs(1), handle)
            .await
            .expect("detector task did not exit on shutdown")
            .expect("detector task panicked")
            .expect("detector returned an error during shutdown");

        assert!(matches!(
            change_rx.try_recv(),
            Err(mpsc::error::TryRecvError::Empty) | Err(mpsc::error::TryRecvError::Disconnected)
        ));
    }
}
