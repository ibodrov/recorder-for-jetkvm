use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing::warn;

use crate::h264::{NAL_TYPE_IDR, NalUnit};
use crate::screenshot;

struct DecodedFrame {
    frame: ffmpeg_the_third::frame::Video,
    received_at: Instant,
    captured_at: SystemTime,
    generation: u64,
}

#[derive(Clone)]
pub struct LatestFrameCache {
    sender: watch::Sender<Option<Arc<DecodedFrame>>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FrameInfo {
    pub generation: u64,
    pub age: Duration,
    pub width: u32,
    pub height: u32,
    pub captured_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotFile {
    pub path: PathBuf,
    pub mime_type: &'static str,
    pub width: u32,
    pub height: u32,
    pub generation: u64,
    pub frame_age: Duration,
    pub captured_at: SystemTime,
}

impl LatestFrameCache {
    pub fn new() -> Self {
        let (sender, _) = watch::channel(None);
        Self { sender }
    }

    pub fn clear(&self) {
        self.sender.send_replace(None);
    }

    pub fn info(&self) -> Option<FrameInfo> {
        self.sender.borrow().as_ref().map(|frame| FrameInfo {
            generation: frame.generation,
            age: frame.received_at.elapsed(),
            width: frame.frame.width(),
            height: frame.frame.height(),
            captured_at: frame.captured_at,
        })
    }

    pub async fn snapshot(
        &self,
        output: &Path,
        generation: u64,
        timeout: Duration,
    ) -> Result<SnapshotFile> {
        let mut receiver = self.sender.subscribe();
        let frame = tokio::time::timeout(timeout, async {
            loop {
                if let Some(frame) = receiver.borrow().clone()
                    && frame.generation == generation
                {
                    return Ok::<_, anyhow::Error>(frame);
                }
                receiver
                    .changed()
                    .await
                    .context("video decoder stopped before producing a frame")?;
            }
        })
        .await
        .context("timed out waiting for a decoded video frame")??;

        let width = frame.frame.width();
        let height = frame.frame.height();
        let age = frame.received_at.elapsed();
        let captured_at = frame.captured_at;
        let output = output.to_owned();
        let output_for_worker = output.clone();
        let decoded = frame.frame.clone();
        tokio::task::spawn_blocking(move || {
            screenshot::encode_png_atomic(&decoded, &output_for_worker)
        })
        .await
        .context("snapshot encoder task failed")??;

        Ok(SnapshotFile {
            path: output,
            mime_type: "image/png",
            width,
            height,
            generation,
            frame_age: age,
            captured_at,
        })
    }

    fn replace(&self, frame: DecodedFrame) {
        self.sender.send_replace(Some(Arc::new(frame)));
    }
}

impl Default for LatestFrameCache {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
struct AccessUnit {
    rtp_timestamp: u32,
    data: Vec<u8>,
    contains_idr: bool,
    contains_sps: bool,
    contains_pps: bool,
    received_at: Option<Instant>,
}

impl AccessUnit {
    fn new(rtp_timestamp: u32) -> Self {
        Self {
            rtp_timestamp,
            ..Self::default()
        }
    }

    fn push(&mut self, nal: &NalUnit) {
        self.data.extend_from_slice(&nal.data);
        self.received_at = Some(nal.timestamp);
        match nal.nal_type() {
            Some(NAL_TYPE_IDR) => self.contains_idr = true,
            Some(7) => self.contains_sps = true,
            Some(8) => self.contains_pps = true,
            _ => {}
        }
    }
}

pub fn spawn_decoder(
    mut nal_rx: broadcast::Receiver<NalUnit>,
    cache: LatestFrameCache,
    generation: u64,
    keyframe_tx: mpsc::Sender<()>,
    cancellation: CancellationToken,
) -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let codec = ffmpeg_the_third::decoder::find(ffmpeg_the_third::codec::Id::H264)
            .context("H.264 decoder not found in linked FFmpeg")?;
        let mut decoder = ffmpeg_the_third::codec::Context::new()
            .decoder()
            .open_as(codec)
            .context("failed to open H.264 decoder")?
            .video()
            .context("linked H.264 decoder is not a video decoder")?;

        let mut current_sps: Option<Vec<u8>> = None;
        let mut current_pps: Option<Vec<u8>> = None;
        let mut pending: Option<AccessUnit> = None;
        let mut started = false;
        let _ = keyframe_tx.try_send(());

        loop {
            tokio::select! {
                _ = cancellation.cancelled() => return Ok(()),
                received = nal_rx.recv() => match received {
                    Ok(nal) => {
                        if pending.as_ref().is_some_and(|unit| unit.rtp_timestamp != nal.rtp_timestamp) {
                            let unit = pending.take().expect("pending access unit checked above");
                            decode_access_unit(
                                &mut decoder,
                                &unit,
                                current_sps.as_deref(),
                                current_pps.as_deref(),
                                &mut started,
                                &cache,
                                generation,
                                &keyframe_tx,
                            )?;
                        }
                        match nal.nal_type() {
                            Some(7) => current_sps = Some(nal.data.to_vec()),
                            Some(8) => current_pps = Some(nal.data.to_vec()),
                            _ => {}
                        }
                        pending
                            .get_or_insert_with(|| AccessUnit::new(nal.rtp_timestamp))
                            .push(&nal);
                    }
                    Err(broadcast::error::RecvError::Lagged(count)) => {
                        warn!(count, "video decoder lagged; resetting at next keyframe");
                        pending = None;
                        started = false;
                        decoder.flush();
                        cache.clear();
                        let _ = keyframe_tx.try_send(());
                    }
                    Err(broadcast::error::RecvError::Closed) => return Ok(()),
                }
            }
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn decode_access_unit(
    decoder: &mut ffmpeg_the_third::decoder::Video,
    unit: &AccessUnit,
    current_sps: Option<&[u8]>,
    current_pps: Option<&[u8]>,
    started: &mut bool,
    cache: &LatestFrameCache,
    generation: u64,
    keyframe_tx: &mpsc::Sender<()>,
) -> Result<()> {
    if unit.data.is_empty() {
        return Ok(());
    }
    let first_keyframe = !*started;
    if first_keyframe && !unit.contains_idr {
        return Ok(());
    }
    if first_keyframe
        && ((!unit.contains_sps && current_sps.is_none())
            || (!unit.contains_pps && current_pps.is_none()))
    {
        return Ok(());
    }

    let mut packet_data = Vec::with_capacity(
        unit.data.len() + current_sps.map_or(0, <[u8]>::len) + current_pps.map_or(0, <[u8]>::len),
    );
    if first_keyframe {
        if !unit.contains_sps
            && let Some(sps) = current_sps
        {
            packet_data.extend_from_slice(sps);
        }
        if !unit.contains_pps
            && let Some(pps) = current_pps
        {
            packet_data.extend_from_slice(pps);
        }
    }
    packet_data.extend_from_slice(&unit.data);

    if let Err(error) = decoder.send_packet(&ffmpeg_the_third::Packet::copy(&packet_data)) {
        warn!(%error, "failed to decode video access unit; resetting decoder");
        decoder.flush();
        *started = false;
        cache.clear();
        let _ = keyframe_tx.try_send(());
        return Ok(());
    }
    *started = true;

    loop {
        let mut frame = ffmpeg_the_third::frame::Video::empty();
        match decoder.receive_frame(&mut frame) {
            Ok(()) => cache.replace(DecodedFrame {
                frame,
                received_at: unit.received_at.unwrap_or_else(Instant::now),
                captured_at: SystemTime::now(),
                generation,
            }),
            Err(error)
                if is_would_block(&error) || matches!(error, ffmpeg_the_third::Error::Eof) =>
            {
                return Ok(());
            }
            Err(error) => {
                warn!(%error, "video decoder failed; resetting at next keyframe");
                decoder.flush();
                *started = false;
                cache.clear();
                let _ = keyframe_tx.try_send(());
                return Ok(());
            }
        }
    }
}

fn is_would_block(error: &ffmpeg_the_third::Error) -> bool {
    match error {
        ffmpeg_the_third::Error::Other { errno } => {
            std::io::Error::from_raw_os_error(*errno).kind() == std::io::ErrorKind::WouldBlock
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latest_frame_cache_starts_empty_and_clears() {
        let cache = LatestFrameCache::new();
        assert!(cache.info().is_none());
        cache.clear();
        assert!(cache.info().is_none());
    }

    #[test]
    fn access_unit_tracks_parameter_sets_and_timestamp() {
        let now = Instant::now();
        let mut unit = AccessUnit::new(10);
        unit.push(&NalUnit {
            data: bytes::Bytes::from_static(&[0, 0, 0, 1, 0x67]),
            is_keyframe: true,
            timestamp: now,
            rtp_timestamp: 10,
        });
        assert!(unit.contains_sps);
        assert_eq!(unit.received_at, Some(now));
    }

    #[tokio::test]
    async fn decodes_recorded_rtp_fixture_and_resets_by_generation() {
        let fixture = include_bytes!("../tests/fixtures/black-16x16.h264");
        let (rtp_tx, rtp_rx) = mpsc::channel(32);
        let (nal_tx, _) = broadcast::channel(32);
        let depacketizer = tokio::spawn(crate::h264::depacketize(rtp_rx, nal_tx.clone()));
        let cache = LatestFrameCache::new();
        let cancellation = CancellationToken::new();
        let (keyframe_tx, _keyframe_rx) = mpsc::channel(4);
        let decoder = spawn_decoder(
            nal_tx.subscribe(),
            cache.clone(),
            42,
            keyframe_tx,
            cancellation.clone(),
        );

        for nal in crate::recorder::split_annexb_nals(fixture) {
            let mut packet = webrtc::rtp::packet::Packet::default();
            packet.header.timestamp = 90_000;
            packet.payload = bytes::Bytes::copy_from_slice(nal);
            rtp_tx.send(packet).await.unwrap();
        }
        let mut boundary = webrtc::rtp::packet::Packet::default();
        boundary.header.timestamp = 180_000;
        boundary.payload = bytes::Bytes::from_static(&[0x09, 0xf0]);
        rtp_tx.send(boundary).await.unwrap();
        drop(rtp_tx);

        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Some(info) = cache.info() {
                    assert_eq!(info.generation, 42);
                    assert_eq!((info.width, info.height), (16, 16));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fixture should decode");

        cancellation.cancel();
        decoder.await.unwrap().unwrap();
        depacketizer.await.unwrap();
        cache.clear();
        assert!(cache.info().is_none());
    }
}
