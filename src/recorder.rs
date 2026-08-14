use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info, warn};

use crate::detector::ChangeEvent;
use crate::h264::{self, NalUnit};

/// How long after the last change event before transitioning from Recording to Cooldown.
const RECORDING_TO_COOLDOWN_SECS: u64 = 1;

#[derive(Debug, Clone, Copy, PartialEq)]
enum State {
    Idle,
    Recording,
    Cooldown,
}

/// Extract raw NAL units from Annex-B data (strips start codes).
pub fn split_annexb_nals(annexb: &[u8]) -> Vec<&[u8]> {
    let mut nals = Vec::new();
    let mut i = 0;

    while i < annexb.len() {
        // Find start code
        if i + 4 <= annexb.len() && annexb[i..i + 4] == [0, 0, 0, 1] {
            i += 4;
        } else if i + 3 <= annexb.len() && annexb[i..i + 3] == [0, 0, 1] {
            i += 3;
        } else {
            i += 1;
            continue;
        }

        // Find end (next start code or end of data)
        let nal_start = i;
        while i < annexb.len() {
            if i + 4 <= annexb.len() && annexb[i..i + 4] == [0, 0, 0, 1] {
                break;
            }
            if i + 3 <= annexb.len() && annexb[i..i + 3] == [0, 0, 1] {
                break;
            }
            i += 1;
        }

        if i > nal_start {
            nals.push(&annexb[nal_start..i]);
        }
    }

    nals
}

/// Build an avcC (AVCDecoderConfigurationRecord) from raw SPS and PPS NAL data.
#[must_use]
pub fn build_avcc_extradata(sps: &[u8], pps: &[u8]) -> Vec<u8> {
    let mut avcc = Vec::with_capacity(11 + sps.len() + pps.len());

    // configurationVersion
    avcc.push(1);
    // AVCProfileIndication, profile_compatibility, AVCLevelIndication from SPS
    if sps.len() >= 4 {
        avcc.push(sps[1]); // profile_idc
        avcc.push(sps[2]); // constraint flags
        avcc.push(sps[3]); // level_idc
    } else {
        avcc.extend_from_slice(&[0x64, 0x00, 0x1F]); // High profile, level 3.1 fallback
    }
    // lengthSizeMinusOne = 3 (4-byte length prefixes) | reserved bits
    avcc.push(0xFF);
    // numOfSequenceParameterSets = 1 | reserved bits
    avcc.push(0xE1);
    // SPS length + data
    avcc.extend_from_slice(&(sps.len() as u16).to_be_bytes());
    avcc.extend_from_slice(sps);
    // numOfPictureParameterSets = 1
    avcc.push(1);
    // PPS length + data
    avcc.extend_from_slice(&(pps.len() as u16).to_be_bytes());
    avcc.extend_from_slice(pps);

    avcc
}

fn append_nal_to_avcc(output: &mut Vec<u8>, nal_data: &NalUnit) {
    for nal in split_annexb_nals(&nal_data.data) {
        if nal.is_empty() {
            continue;
        }
        let nal_type = nal[0] & 0x1F;
        // SPS/PPS belong in stream extradata, not media samples.
        if nal_type == 7 || nal_type == 8 {
            continue;
        }
        output.extend_from_slice(&(nal.len() as u32).to_be_bytes());
        output.extend_from_slice(nal);
    }
}

/// Convert a single Annex-B NAL unit to AVCC format (4-byte length prefix).
#[must_use]
pub fn nal_to_avcc(nal_data: &NalUnit) -> Vec<u8> {
    let mut avcc = Vec::with_capacity(nal_data.data.len());
    append_nal_to_avcc(&mut avcc, nal_data);
    avcc
}

/// Configure the H.264 stream parameters on an FFmpeg output stream.
///
/// # Safety
/// Accesses raw FFmpeg pointers to set codec parameters and extradata.
unsafe fn configure_h264_stream(
    stream: &mut ffmpeg_the_third::format::stream::StreamMut,
    sps_pps_annexb: &[u8],
) -> Result<()> {
    let nals = split_annexb_nals(sps_pps_annexb);
    let sps = nals.iter().find(|n| !n.is_empty() && (n[0] & 0x1F) == 7);
    let pps = nals.iter().find(|n| !n.is_empty() && (n[0] & 0x1F) == 8);

    // Try to parse actual resolution from SPS
    let (width, height) = sps
        .and_then(|s| h264::parse_sps_dimensions(s))
        .unwrap_or_else(|| {
            warn!("could not parse SPS dimensions, falling back to 1920x1080");
            (1920, 1080)
        });

    info!(width, height, "configured stream resolution");

    unsafe {
        let par = &mut *stream.parameters_mut().as_mut_ptr();
        par.codec_type = ffmpeg_the_third::ffi::AVMediaType::AVMEDIA_TYPE_VIDEO;
        par.codec_id = ffmpeg_the_third::ffi::AVCodecID::AV_CODEC_ID_H264;
        par.width = width as i32;
        par.height = height as i32;

        if let (Some(sps), Some(pps)) = (sps, pps) {
            let avcc = build_avcc_extradata(sps, pps);
            let extradata = ffmpeg_the_third::ffi::av_malloc(
                avcc.len() + ffmpeg_the_third::ffi::AV_INPUT_BUFFER_PADDING_SIZE as usize,
            ) as *mut u8;
            if !extradata.is_null() {
                std::ptr::copy_nonoverlapping(avcc.as_ptr(), extradata, avcc.len());
                std::ptr::write_bytes(
                    extradata.add(avcc.len()),
                    0,
                    ffmpeg_the_third::ffi::AV_INPUT_BUFFER_PADDING_SIZE as usize,
                );
                par.extradata = extradata;
                par.extradata_size = avcc.len() as i32;
            }
        } else {
            warn!("no SPS/PPS available, MP4 may not be playable");
        }
    }

    Ok(())
}

#[derive(Debug)]
struct AccessUnit {
    rtp_timestamp: u32,
    data: Vec<u8>,
    is_keyframe: bool,
}

fn collect_access_unit(pending: &mut Option<AccessUnit>, nal: &NalUnit) -> Option<AccessUnit> {
    if let Some(unit) = pending.as_mut()
        && unit.rtp_timestamp == nal.rtp_timestamp
    {
        let previous_len = unit.data.len();
        unit.data.reserve(nal.data.len());
        append_nal_to_avcc(&mut unit.data, nal);
        if unit.data.len() > previous_len {
            unit.is_keyframe |= nal.is_keyframe;
        }
        return None;
    }

    let mut data = Vec::with_capacity(nal.data.len());
    append_nal_to_avcc(&mut data, nal);
    if data.is_empty() {
        return None;
    }
    let completed = pending.take();
    *pending = Some(AccessUnit {
        rtp_timestamp: nal.rtp_timestamp,
        data,
        is_keyframe: nal.is_keyframe,
    });
    completed
}

fn buffered_keyframe_start(ring_buffer: &VecDeque<NalUnit>) -> usize {
    let Some(keyframe_timestamp) = ring_buffer
        .iter()
        .rev()
        .find(|nal| nal.nal_type() == Some(h264::NAL_TYPE_IDR))
        .map(|nal| nal.rtp_timestamp)
    else {
        return 0;
    };

    ring_buffer
        .iter()
        .position(|nal| nal.rtp_timestamp == keyframe_timestamp)
        .unwrap_or(0)
}

struct Mp4Writer {
    output_ctx: ffmpeg_the_third::format::context::Output,
    stream_index: usize,
    stream_time_base: ffmpeg_the_third::Rational,
    first_rtp_ts: Option<u32>,
    pending: Option<AccessUnit>,
    packet_count: u64,
    error_count: u64,
    finalized: bool,
}

impl Mp4Writer {
    fn new(path: &Path, sps_pps_annexb: &[u8]) -> Result<Self> {
        let mut output_ctx = ffmpeg_the_third::format::output(path)
            .with_context(|| format!("failed to create output file: {}", path.display()))?;

        let codec = ffmpeg_the_third::encoder::find(ffmpeg_the_third::codec::Id::H264)
            .context("H.264 encoder not found")?;

        let stream_index;
        {
            let mut stream = output_ctx.add_stream(codec)?;
            stream_index = stream.index();

            stream.set_time_base(ffmpeg_the_third::Rational(1, 90000));

            unsafe {
                configure_h264_stream(&mut stream, sps_pps_annexb)?;
            }
        }

        let mut opts = ffmpeg_the_third::Dictionary::new();
        opts.set("movflags", "frag_keyframe+empty_moov");

        output_ctx
            .write_header_with(opts)
            .context("failed to write MP4 header")?;

        // Read back the effective time_base — the muxer may have changed it
        let stream_time_base = output_ctx
            .stream(stream_index)
            .map(|s| s.time_base())
            .unwrap_or(ffmpeg_the_third::Rational(1, 90000));

        info!(
            "opened MP4 file: {} (time_base: {}/{})",
            path.display(),
            stream_time_base.numerator(),
            stream_time_base.denominator(),
        );

        Ok(Self {
            output_ctx,
            stream_index,
            stream_time_base,
            first_rtp_ts: None,
            pending: None,
            packet_count: 0,
            error_count: 0,
            finalized: false,
        })
    }

    fn write_nal(&mut self, nal: &NalUnit) -> Result<()> {
        if let Some(unit) = collect_access_unit(&mut self.pending, nal) {
            self.write_access_unit(unit)?;
        }
        Ok(())
    }

    fn write_access_unit(&mut self, unit: AccessUnit) -> Result<()> {
        if self.packet_count == 0 && !unit.is_keyframe {
            warn!("first packet written is not a keyframe, video may have initial artifacts");
        }
        if self.first_rtp_ts.is_none() {
            self.first_rtp_ts = Some(unit.rtp_timestamp);
        }

        let first_ts = self
            .first_rtp_ts
            .expect("first RTP timestamp is initialized");
        let rtp_offset = unit.rtp_timestamp.wrapping_sub(first_ts) as i64;
        let tb = self.stream_time_base;
        let pts = if tb.numerator() > 0 && tb.denominator() > 0 {
            let num = rtp_offset as i128 * tb.denominator() as i128;
            let den = 90000i128 * tb.numerator() as i128;
            ((num + den / 2) / den) as i64
        } else {
            rtp_offset
        };

        let mut packet = ffmpeg_the_third::codec::packet::Packet::copy(&unit.data);
        packet.set_pts(Some(pts));
        packet.set_dts(Some(pts));
        packet.set_stream(self.stream_index);
        if unit.is_keyframe {
            packet.set_flags(ffmpeg_the_third::codec::packet::Flags::KEY);
        }
        packet
            .write_interleaved(&mut self.output_ctx)
            .with_context(|| {
                format!(
                    "write_interleaved failed: rtp_timestamp={} pts={pts} size={} keyframe={}",
                    unit.rtp_timestamp,
                    unit.data.len(),
                    unit.is_keyframe
                )
            })?;

        self.packet_count += 1;
        Ok(())
    }

    fn finalize(&mut self) {
        if self.finalized {
            return;
        }
        self.finalized = true;
        if let Some(unit) = self.pending.take()
            && let Err(error) = self.write_access_unit(unit)
        {
            self.error_count += 1;
            error!(%error, "failed to flush final H.264 access unit");
        }

        if self.packet_count == 0 {
            warn!("no video packets were written, skipping MP4 trailer");
            return;
        }
        // With fragmented MP4 (frag_keyframe+empty_moov) the file is already
        // playable up to the last completed fragment. The trailer is best-effort.
        match self.output_ctx.write_trailer() {
            Ok(()) => info!("finalized MP4 ({} packets written)", self.packet_count),
            Err(e) => warn!(
                "MP4 trailer write failed (file should still be playable): {e}; {} packets written",
                self.packet_count
            ),
        }
    }
}

impl Drop for Mp4Writer {
    fn drop(&mut self) {
        self.finalize();
    }
}

pub async fn run(
    mut nal_rx: broadcast::Receiver<NalUnit>,
    mut change_rx: mpsc::Receiver<ChangeEvent>,
    output_dir: PathBuf,
    pre_buffer_secs: u64,
    cooldown_secs: u64,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    tokio::fs::create_dir_all(&output_dir)
        .await
        .with_context(|| {
            format!(
                "failed to create output directory: {}",
                output_dir.display()
            )
        })?;

    let pre_buffer_duration = Duration::from_secs(pre_buffer_secs);
    let cooldown_duration = Duration::from_secs(cooldown_secs);

    let mut state = State::Idle;
    let mut ring_buffer: VecDeque<NalUnit> = VecDeque::new();
    let mut writer: Option<Mp4Writer> = None;
    let mut last_change: Option<Instant> = None;
    let mut current_sps: Option<Vec<u8>> = None;
    let mut current_pps: Option<Vec<u8>> = None;
    let mut sps_pps: Vec<u8> = Vec::new();

    loop {
        tokio::select! {
            nal_result = nal_rx.recv() => {
                match nal_result {
                    Ok(nal) => {
                        // Track SPS/PPS separately for correct MP4 extradata
                        if let Some(nal_type) = nal.nal_type() {
                            let mut sps_pps_changed = false;
                            if nal_type == 7 {
                                if current_sps.as_deref() != Some(&nal.data) {
                                    current_sps = Some(nal.data.to_vec());
                                    sps_pps_changed = true;
                                }
                            } else if nal_type == 8
                                && current_pps.as_deref() != Some(&nal.data) {
                                    current_pps = Some(nal.data.to_vec());
                                    sps_pps_changed = true;
                                }
                            // Only rebuild when both are available and something changed
                            if sps_pps_changed && current_sps.is_some() && current_pps.is_some() {
                                sps_pps.clear();
                                if let Some(ref s) = current_sps {
                                    sps_pps.extend_from_slice(s);
                                }
                                if let Some(ref p) = current_pps {
                                    sps_pps.extend_from_slice(p);
                                }
                            }
                        }

                        match state {
                            State::Idle => {
                                ring_buffer.push_back(nal);
                                while let Some(front) = ring_buffer.front() {
                                    if front.timestamp.elapsed() > pre_buffer_duration {
                                        ring_buffer.pop_front();
                                    } else {
                                        break;
                                    }
                                }
                            }
                            State::Recording | State::Cooldown => {
                                if let Some(ref mut w) = writer
                                    && let Err(e) = w.write_nal(&nal)
                                {
                                    w.error_count += 1;
                                    if w.error_count <= 3 {
                                        error!("failed to write NAL to MP4: {e:#}");
                                    } else if w.error_count == 4 {
                                        error!("suppressing further write errors");
                                    }
                                }

                                if state == State::Cooldown
                                    && let Some(lc) = last_change
                                    && lc.elapsed() >= cooldown_duration
                                {
                                    info!("cooldown expired, stopping recording");
                                    if let Some(mut w) = writer.take() {
                                        w.finalize();
                                    }
                                    state = State::Idle;
                                    last_change = None;
                                }
                            }
                        }
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!("recorder lagged, missed {n} NAL units");
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        anyhow::bail!("recorder NAL stream closed unexpectedly");
                    }
                }
            }

            Some(change) = change_rx.recv() => {
                last_change = Some(change.timestamp);

                match state {
                    State::Idle => {
                        info!(
                            changed = format!("{:.2}%", change.changed_fraction * 100.0),
                            "change detected, starting recording"
                        );
                        state = State::Recording;

                        let timestamp = chrono::Local::now().format("%Y-%m-%d_%H-%M-%S");
                        let filename = format!("{timestamp}_event.mp4");
                        let path = output_dir.join(&filename);

                        match Mp4Writer::new(&path, &sps_pps) {
                            Ok(mut w) => {
                                let keyframe_pos = buffered_keyframe_start(&ring_buffer);

                                let mut flushed = 0;
                                for nal in ring_buffer.iter().skip(keyframe_pos) {
                                    if let Err(e) = w.write_nal(nal) {
                                        error!("failed to write buffered NAL: {e:#}");
                                        break;
                                    }
                                    flushed += 1;
                                }
                                debug!("flushed {flushed} NALs from ring buffer");
                                ring_buffer.clear();
                                writer = Some(w);
                            }
                            Err(e) => {
                                error!("failed to create MP4 writer: {e}");
                                state = State::Idle;
                            }
                        }
                    }
                    State::Recording => {
                        debug!("change continues during recording");
                    }
                    State::Cooldown => {
                        info!("change detected during cooldown, continuing recording");
                        state = State::Recording;
                    }
                }
            }

            changed = shutdown.changed() => {
                if changed.is_err() && !*shutdown.borrow() {
                    anyhow::bail!("recorder shutdown channel closed unexpectedly");
                }
                info!("shutdown signal, finalizing recording");
                if let Some(mut w) = writer.take() {
                    w.finalize();
                }
                return Ok(());
            }
        }

        if state == State::Recording
            && let Some(lc) = last_change
            && lc.elapsed() >= Duration::from_secs(RECORDING_TO_COOLDOWN_SECS)
        {
            state = State::Cooldown;
            debug!("entering cooldown");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_split_annexb_nals_single() {
        let data = [0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0x00, 0x1E];
        let nals = split_annexb_nals(&data);
        assert_eq!(nals.len(), 1);
        assert_eq!(nals[0], &[0x67, 0x42, 0x00, 0x1E]);
    }

    #[test]
    fn test_split_annexb_nals_multiple() {
        let data = [
            0x00, 0x00, 0x00, 0x01, 0x67, 0x42, // SPS
            0x00, 0x00, 0x00, 0x01, 0x68, 0xCE, // PPS
        ];
        let nals = split_annexb_nals(&data);
        assert_eq!(nals.len(), 2);
        assert_eq!(nals[0], &[0x67, 0x42]);
        assert_eq!(nals[1], &[0x68, 0xCE]);
    }

    #[test]
    fn test_split_annexb_nals_3byte_start_code() {
        let data = [0x00, 0x00, 0x01, 0x65, 0xAB, 0xCD];
        let nals = split_annexb_nals(&data);
        assert_eq!(nals.len(), 1);
        assert_eq!(nals[0], &[0x65, 0xAB, 0xCD]);
    }

    #[test]
    fn test_split_annexb_nals_empty() {
        let nals = split_annexb_nals(&[]);
        assert!(nals.is_empty());
    }

    #[test]
    fn test_build_avcc_extradata_structure() {
        let sps = vec![0x67, 0x64, 0x00, 0x28, 0xAC, 0xD9];
        let pps = vec![0x68, 0xEE, 0x3C, 0x80];
        let avcc = build_avcc_extradata(&sps, &pps);

        assert_eq!(avcc[0], 1); // configurationVersion
        assert_eq!(avcc[1], 0x64); // profile_idc
        assert_eq!(avcc[2], 0x00); // constraint flags
        assert_eq!(avcc[3], 0x28); // level_idc
        assert_eq!(avcc[4], 0xFF); // lengthSizeMinusOne | reserved
        assert_eq!(avcc[5], 0xE1); // numSPS | reserved

        // SPS length (2 bytes big-endian)
        let sps_len = u16::from_be_bytes([avcc[6], avcc[7]]) as usize;
        assert_eq!(sps_len, sps.len());
        assert_eq!(&avcc[8..8 + sps_len], &sps[..]);

        // numPPS
        assert_eq!(avcc[8 + sps_len], 1);

        // PPS length
        let pps_offset = 8 + sps_len + 1;
        let pps_len = u16::from_be_bytes([avcc[pps_offset], avcc[pps_offset + 1]]) as usize;
        assert_eq!(pps_len, pps.len());
        assert_eq!(&avcc[pps_offset + 2..pps_offset + 2 + pps_len], &pps[..]);
    }

    #[test]
    fn test_nal_to_avcc_skips_sps_pps() {
        // SPS NAL (type 7) should be skipped
        let sps_nal = NalUnit {
            data: vec![0x00, 0x00, 0x00, 0x01, 0x67, 0x64, 0x00, 0x28].into(),
            is_keyframe: true,
            timestamp: Instant::now(),
            rtp_timestamp: 0,
        };
        assert!(nal_to_avcc(&sps_nal).is_empty());

        // PPS NAL (type 8) should be skipped
        let pps_nal = NalUnit {
            data: vec![0x00, 0x00, 0x00, 0x01, 0x68, 0xEE, 0x3C, 0x80].into(),
            is_keyframe: true,
            timestamp: Instant::now(),
            rtp_timestamp: 0,
        };
        assert!(nal_to_avcc(&pps_nal).is_empty());
    }

    #[test]
    fn test_nal_to_avcc_converts_idr() {
        // IDR NAL (type 5) should be converted with 4-byte length prefix
        let idr_data = vec![0x65, 0xAB, 0xCD, 0xEF];
        let mut annexb = vec![0x00, 0x00, 0x00, 0x01];
        annexb.extend_from_slice(&idr_data);

        let nal = NalUnit {
            data: annexb.into(),
            is_keyframe: true,
            timestamp: Instant::now(),
            rtp_timestamp: 1000,
        };

        let avcc = nal_to_avcc(&nal);
        assert!(!avcc.is_empty());

        // First 4 bytes should be the length in big-endian
        let len = u32::from_be_bytes([avcc[0], avcc[1], avcc[2], avcc[3]]) as usize;
        assert_eq!(len, idr_data.len());
        assert_eq!(&avcc[4..], &idr_data[..]);
    }

    #[test]
    fn groups_all_nals_with_one_rtp_timestamp_into_one_access_unit() {
        fn nal(payload: &[u8], rtp_timestamp: u32, is_keyframe: bool) -> NalUnit {
            let mut data = vec![0, 0, 0, 1];
            data.extend_from_slice(payload);
            NalUnit {
                data: data.into(),
                is_keyframe,
                timestamp: Instant::now(),
                rtp_timestamp,
            }
        }

        let mut pending = None;
        assert!(collect_access_unit(&mut pending, &nal(&[0x65, 0xAA], 90_000, true)).is_none());
        assert!(collect_access_unit(&mut pending, &nal(&[0x06, 0xBB], 90_000, false)).is_none());
        let completed = collect_access_unit(&mut pending, &nal(&[0x41, 0xCC], 93_000, false))
            .expect("timestamp change completes an access unit");

        assert_eq!(completed.rtp_timestamp, 90_000);
        assert!(completed.is_keyframe);
        assert_eq!(
            completed.data,
            vec![0, 0, 0, 2, 0x65, 0xAA, 0, 0, 0, 2, 0x06, 0xBB,]
        );
        assert_eq!(pending.expect("next access unit").rtp_timestamp, 93_000);
    }

    #[test]
    fn buffered_keyframe_starts_at_first_nal_in_access_unit() {
        fn nal(nal_type: u8, rtp_timestamp: u32) -> NalUnit {
            NalUnit {
                data: vec![0, 0, 0, 1, nal_type].into(),
                is_keyframe: nal_type & 0x1f == h264::NAL_TYPE_IDR,
                timestamp: Instant::now(),
                rtp_timestamp,
            }
        }

        let ring_buffer = VecDeque::from([
            nal(0x41, 90_000),
            nal(0x09, 180_000),
            nal(0x65, 180_000),
            nal(0x65, 180_000),
            nal(0x41, 270_000),
        ]);

        assert_eq!(buffered_keyframe_start(&ring_buffer), 1);
    }

    #[test]
    fn buffered_stream_without_idr_keeps_all_nals() {
        let ring_buffer = VecDeque::from([NalUnit {
            data: vec![0, 0, 0, 1, 0x41].into(),
            is_keyframe: false,
            timestamp: Instant::now(),
            rtp_timestamp: 90_000,
        }]);

        assert_eq!(buffered_keyframe_start(&ring_buffer), 0);
    }

    #[test]
    #[ignore = "requires ffmpeg and ffprobe executables"]
    fn multi_slice_fixture_produces_decodable_mp4_access_units() {
        ffmpeg_the_third::init().expect("initialize FFmpeg");
        let source = include_bytes!("../tests/fixtures/multi_slice.h264");
        let nals = split_annexb_nals(source);
        let mut parameter_sets = Vec::new();
        for nal in &nals {
            if matches!(nal.first().map(|byte| byte & 0x1F), Some(7 | 8)) {
                parameter_sets.extend_from_slice(&[0, 0, 0, 1]);
                parameter_sets.extend_from_slice(nal);
            }
        }

        let directory = tempfile::tempdir().expect("temporary directory");
        let output = directory.path().join("multi-slice.mp4");
        let mut writer = Mp4Writer::new(&output, &parameter_sets).expect("create MP4 writer");
        let mut rtp_timestamp = 0_u32;
        let mut saw_picture = false;
        for nal in nals {
            let nal_type = nal[0] & 0x1F;
            if nal_type == 9 && saw_picture {
                rtp_timestamp = rtp_timestamp.wrapping_add(45_000);
                saw_picture = false;
            }
            saw_picture |= matches!(nal_type, 1 | 5);
            let mut data = vec![0, 0, 0, 1];
            data.extend_from_slice(nal);
            writer
                .write_nal(&NalUnit {
                    data: data.into(),
                    is_keyframe: nal_type == 5,
                    timestamp: Instant::now(),
                    rtp_timestamp,
                })
                .expect("write fixture NAL");
        }
        writer.finalize();
        assert_eq!(writer.packet_count, 4, "one MP4 sample per access unit");
        drop(writer);

        let probe = std::process::Command::new("ffprobe")
            .args([
                "-v",
                "error",
                "-count_frames",
                "-select_streams",
                "v:0",
                "-show_entries",
                "stream=nb_read_frames",
                "-of",
                "default=nokey=1:noprint_wrappers=1",
            ])
            .arg(&output)
            .output()
            .expect("run ffprobe");
        assert!(
            probe.status.success(),
            "ffprobe failed: {}",
            String::from_utf8_lossy(&probe.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&probe.stdout).trim(), "4");

        let decode = std::process::Command::new("ffmpeg")
            .args(["-v", "error", "-i"])
            .arg(&output)
            .args(["-f", "null", "-"])
            .output()
            .expect("run ffmpeg decoder");
        assert!(
            decode.status.success(),
            "fixture MP4 failed to decode: {}",
            String::from_utf8_lossy(&decode.stderr)
        );
    }
}
