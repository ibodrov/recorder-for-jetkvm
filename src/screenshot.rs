use std::io::ErrorKind;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use tokio::sync::{broadcast, watch};
use tracing::{info, warn};

use crate::h264::{NAL_TYPE_IDR, NalUnit};

#[derive(Debug, Default)]
struct AccessUnit {
    rtp_timestamp: u32,
    data: Vec<u8>,
    contains_idr: bool,
    contains_sps: bool,
    contains_pps: bool,
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

        match nal.nal_type() {
            Some(NAL_TYPE_IDR) => self.contains_idr = true,
            Some(7) => self.contains_sps = true,
            Some(8) => self.contains_pps = true,
            _ => {}
        }
    }
}

pub async fn capture(
    mut nal_rx: broadcast::Receiver<NalUnit>,
    output_path: &Path,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
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
    let mut pending_access_unit: Option<AccessUnit> = None;
    let mut started = false;

    loop {
        tokio::select! {
            _ = shutdown.changed() => {
                bail!("shutdown requested before screenshot was captured");
            }
            recv_result = nal_rx.recv() => {
                match recv_result {
                    Ok(nal) => {
                        if pending_access_unit
                            .as_ref()
                            .is_some_and(|au| au.rtp_timestamp != nal.rtp_timestamp)
                        {
                            let access_unit = pending_access_unit
                                .take()
                                .expect("checked pending access unit above");
                            if maybe_capture_access_unit(
                                &mut decoder,
                                &access_unit,
                                current_sps.as_deref(),
                                current_pps.as_deref(),
                                &mut started,
                                output_path,
                            )? {
                                return Ok(());
                            }
                        }

                        match nal.nal_type() {
                            Some(7) => current_sps = Some(nal.data.to_vec()),
                            Some(8) => current_pps = Some(nal.data.to_vec()),
                            _ => {}
                        }

                        pending_access_unit
                            .get_or_insert_with(|| AccessUnit::new(nal.rtp_timestamp))
                            .push(&nal);
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("screenshot capture lagged, missed {n} NAL units");
                        pending_access_unit = None;
                        started = false;
                        decoder.flush();
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        if let Some(access_unit) = pending_access_unit.take()
                            && maybe_capture_access_unit(
                                &mut decoder,
                                &access_unit,
                                current_sps.as_deref(),
                                current_pps.as_deref(),
                                &mut started,
                                output_path,
                            )?
                        {
                            return Ok(());
                        }

                        bail!("video stream ended before a screenshot could be captured");
                    }
                }
            }
        }
    }
}

fn maybe_capture_access_unit(
    decoder: &mut ffmpeg_the_third::decoder::Video,
    access_unit: &AccessUnit,
    current_sps: Option<&[u8]>,
    current_pps: Option<&[u8]>,
    started: &mut bool,
    output_path: &Path,
) -> Result<bool> {
    if access_unit.data.is_empty() {
        return Ok(false);
    }

    let first_keyframe = !*started;
    if first_keyframe && !access_unit.contains_idr {
        return Ok(false);
    }

    if first_keyframe
        && ((!access_unit.contains_sps && current_sps.is_none())
            || (!access_unit.contains_pps && current_pps.is_none()))
    {
        warn!("skipping keyframe without cached SPS/PPS data");
        return Ok(false);
    }

    *started = true;

    let packet_data = build_decoder_packet(access_unit, current_sps, current_pps, first_keyframe);
    let packet = ffmpeg_the_third::Packet::copy(&packet_data);

    if let Err(err) = decoder.send_packet(&packet) {
        warn!("failed to send access unit to decoder, waiting for next keyframe: {err}");
        decoder.flush();
        *started = false;
        return Ok(false);
    }

    loop {
        let mut decoded = ffmpeg_the_third::frame::Video::empty();
        match decoder.receive_frame(&mut decoded) {
            Ok(()) => {
                encode_png(&decoded, output_path)?;
                info!(
                    path = %output_path.display(),
                    width = decoded.width(),
                    height = decoded.height(),
                    "saved screenshot"
                );
                return Ok(true);
            }
            Err(err) if is_would_block(&err) || matches!(err, ffmpeg_the_third::Error::Eof) => {
                return Ok(false);
            }
            Err(err) => {
                warn!("failed to decode access unit, waiting for next keyframe: {err}");
                decoder.flush();
                *started = false;
                return Ok(false);
            }
        }
    }
}

fn build_decoder_packet(
    access_unit: &AccessUnit,
    current_sps: Option<&[u8]>,
    current_pps: Option<&[u8]>,
    prepend_parameter_sets: bool,
) -> Vec<u8> {
    let mut packet = Vec::with_capacity(
        access_unit.data.len()
            + current_sps.map_or(0, |data| data.len())
            + current_pps.map_or(0, |data| data.len()),
    );

    if prepend_parameter_sets {
        if !access_unit.contains_sps
            && let Some(sps) = current_sps
        {
            packet.extend_from_slice(sps);
        }
        if !access_unit.contains_pps
            && let Some(pps) = current_pps
        {
            packet.extend_from_slice(pps);
        }
    }

    packet.extend_from_slice(&access_unit.data);
    packet
}

fn encode_png(frame: &ffmpeg_the_third::frame::Video, output_path: &Path) -> Result<()> {
    let mut rgb_frame = ffmpeg_the_third::frame::Video::new(
        ffmpeg_the_third::format::Pixel::RGB24,
        frame.width(),
        frame.height(),
    );
    let mut scaler = frame
        .converter(ffmpeg_the_third::format::Pixel::RGB24)
        .context("failed to create RGB frame converter")?;
    scaler
        .run(frame, &mut rgb_frame)
        .context("failed to convert decoded frame to RGB24")?;

    let codec = ffmpeg_the_third::encoder::find(ffmpeg_the_third::codec::Id::PNG)
        .context("PNG encoder not found in linked FFmpeg")?;
    let mut encoder = ffmpeg_the_third::codec::Context::new()
        .encoder()
        .video()
        .context("failed to create PNG encoder context")?;
    encoder.set_width(rgb_frame.width());
    encoder.set_height(rgb_frame.height());
    encoder.set_format(ffmpeg_the_third::format::Pixel::RGB24);
    encoder.set_time_base(ffmpeg_the_third::Rational(1, 1));

    let mut encoder = encoder.open_as(codec).context("failed to open PNG encoder")?;
    encoder
        .send_frame(&rgb_frame)
        .context("failed to send RGB frame to PNG encoder")?;
    encoder
        .send_eof()
        .context("failed to flush PNG encoder")?;

    let mut packet = ffmpeg_the_third::Packet::empty();
    loop {
        match encoder.receive_packet(&mut packet) {
            Ok(()) => {
                let data = packet
                    .data()
                    .ok_or_else(|| anyhow!("PNG encoder returned an empty packet"))?;

                if let Some(parent) = output_path.parent() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!("failed to create screenshot directory: {}", parent.display())
                    })?;
                }

                std::fs::write(output_path, data).with_context(|| {
                    format!("failed to write screenshot: {}", output_path.display())
                })?;
                return Ok(());
            }
            Err(err) if is_would_block(&err) => continue,
            Err(ffmpeg_the_third::Error::Eof) => {
                bail!("PNG encoder produced no output packet");
            }
            Err(err) => {
                return Err(err).context("failed to receive PNG packet from encoder");
            }
        }
    }
}

fn is_would_block(err: &ffmpeg_the_third::Error) -> bool {
    match err {
        ffmpeg_the_third::Error::Other { errno } => {
            std::io::Error::from_raw_os_error(*errno).kind() == ErrorKind::WouldBlock
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use std::time::Instant;

    fn nal(rtp_timestamp: u32, bytes: &[u8]) -> NalUnit {
        NalUnit {
            data: Bytes::copy_from_slice(bytes),
            is_keyframe: matches!(bytes.get(4).map(|b| b & 0x1f), Some(5 | 7 | 8)),
            timestamp: Instant::now(),
            rtp_timestamp,
        }
    }

    #[test]
    fn test_build_decoder_packet_prefixes_cached_parameter_sets_for_first_keyframe() {
        let mut access_unit = AccessUnit::new(42);
        access_unit.push(&nal(42, &[0, 0, 0, 1, 0x65, 0xaa]));

        let packet = build_decoder_packet(
            &access_unit,
            Some(&[0, 0, 0, 1, 0x67, 0x64]),
            Some(&[0, 0, 0, 1, 0x68, 0xee]),
            true,
        );

        assert_eq!(
            packet,
            vec![
                0, 0, 0, 1, 0x67, 0x64, 0, 0, 0, 1, 0x68, 0xee, 0, 0, 0, 1, 0x65, 0xaa
            ]
        );
    }

    #[test]
    fn test_build_decoder_packet_does_not_duplicate_parameter_sets() {
        let mut access_unit = AccessUnit::new(42);
        access_unit.push(&nal(42, &[0, 0, 0, 1, 0x67, 0x64]));
        access_unit.push(&nal(42, &[0, 0, 0, 1, 0x68, 0xee]));
        access_unit.push(&nal(42, &[0, 0, 0, 1, 0x65, 0xaa]));

        let packet = build_decoder_packet(
            &access_unit,
            Some(&[0, 0, 0, 1, 0x67, 0x64]),
            Some(&[0, 0, 0, 1, 0x68, 0xee]),
            true,
        );

        assert_eq!(
            packet,
            vec![
                0, 0, 0, 1, 0x67, 0x64, 0, 0, 0, 1, 0x68, 0xee, 0, 0, 0, 1, 0x65, 0xaa
            ]
        );
    }
}
