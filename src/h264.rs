use std::time::Instant;

use bytes::Bytes;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, info, trace, warn};
use webrtc::rtp::packet::Packet;

const START_CODE: [u8; 4] = [0x00, 0x00, 0x00, 0x01];

pub const NAL_TYPE_IDR: u8 = 5;
const NAL_TYPE_SPS: u8 = 7;
const NAL_TYPE_PPS: u8 = 8;
const NAL_TYPE_STAP_A: u8 = 24;
const NAL_TYPE_FU_A: u8 = 28;

#[derive(Clone, Debug)]
pub struct NalUnit {
    pub data: Bytes,
    pub is_keyframe: bool,
    pub timestamp: Instant,
    pub rtp_timestamp: u32,
}

impl NalUnit {
    /// Returns the NAL unit type, skipping the 4-byte Annex-B start code prefix.
    /// Returns `None` if the data is too short.
    pub fn nal_type(&self) -> Option<u8> {
        self.data.get(4).map(|b| b & 0x1F)
    }
}

pub async fn depacketize(mut rtp_rx: mpsc::Receiver<Packet>, nal_tx: broadcast::Sender<NalUnit>) {
    let mut fu_a_buffer: Vec<u8> = Vec::new();
    let mut fu_a_started = false;
    let mut nal_count: u64 = 0;
    let mut prev_nal_count: u64 = 0;
    let mut keyframe_count: u64 = 0;
    let mut rtp_count: u64 = 0;

    while let Some(pkt) = rtp_rx.recv().await {
        rtp_count += 1;
        let payload = &pkt.payload;
        if payload.is_empty() {
            continue;
        }

        let now = Instant::now();
        let rtp_ts = pkt.header.timestamp;
        let nal_type = payload[0] & 0x1F;

        match nal_type {
            // Single NAL unit (types 1-23)
            1..=23 => {
                let is_keyframe = nal_type == NAL_TYPE_IDR
                    || nal_type == NAL_TYPE_SPS
                    || nal_type == NAL_TYPE_PPS;
                let mut data = Vec::with_capacity(START_CODE.len() + payload.len());
                data.extend_from_slice(&START_CODE);
                data.extend_from_slice(payload);

                if is_keyframe {
                    keyframe_count += 1;
                }
                nal_count += 1;
                let nal = NalUnit {
                    data: data.into(),
                    is_keyframe,
                    timestamp: now,
                    rtp_timestamp: rtp_ts,
                };
                if nal_tx.send(nal).is_err() {
                    debug!("NAL channel has no receivers");
                    return;
                }
            }

            // STAP-A: aggregation packet
            NAL_TYPE_STAP_A => {
                let mut offset = 1; // skip STAP-A header
                while offset + 2 <= payload.len() {
                    let nalu_size =
                        u16::from_be_bytes([payload[offset], payload[offset + 1]]) as usize;
                    offset += 2;
                    if nalu_size == 0 {
                        warn!("STAP-A: empty NAL unit");
                        continue;
                    }
                    if offset + nalu_size > payload.len() {
                        warn!("STAP-A: NAL size exceeds payload");
                        break;
                    }
                    let nalu_data = &payload[offset..offset + nalu_size];
                    offset += nalu_size;
                    if nalu_data.is_empty() {
                        warn!("STAP-A: empty NAL payload");
                        continue;
                    }

                    let inner_nal_type = nalu_data[0] & 0x1F;
                    let is_keyframe = inner_nal_type == NAL_TYPE_IDR
                        || inner_nal_type == NAL_TYPE_SPS
                        || inner_nal_type == NAL_TYPE_PPS;

                    let mut data = Vec::with_capacity(START_CODE.len() + nalu_size);
                    data.extend_from_slice(&START_CODE);
                    data.extend_from_slice(nalu_data);

                    if is_keyframe {
                        keyframe_count += 1;
                    }
                    nal_count += 1;
                    let nal = NalUnit {
                        data: data.into(),
                        is_keyframe,
                        timestamp: now,
                        rtp_timestamp: rtp_ts,
                    };
                    if nal_tx.send(nal).is_err() {
                        debug!("NAL channel has no receivers");
                        return;
                    }
                }
            }

            // FU-A: fragmentation unit
            NAL_TYPE_FU_A => {
                if payload.len() < 2 {
                    warn!("FU-A packet too short");
                    continue;
                }
                let fu_header = payload[1];
                let start = (fu_header & 0x80) != 0;
                let end = (fu_header & 0x40) != 0;
                let inner_nal_type = fu_header & 0x1F;

                if start {
                    fu_a_buffer.clear();
                    // Reconstruct NAL header: forbidden_zero_bit | NRI from FU indicator | type from FU header
                    let nal_header = (payload[0] & 0xE0) | inner_nal_type;
                    fu_a_buffer.extend_from_slice(&START_CODE);
                    fu_a_buffer.push(nal_header);
                    fu_a_buffer.extend_from_slice(&payload[2..]);
                    fu_a_started = true;
                } else if fu_a_started {
                    fu_a_buffer.extend_from_slice(&payload[2..]);
                } else {
                    // Fragment without start; skip
                    continue;
                }

                if end && fu_a_started {
                    let is_keyframe = inner_nal_type == NAL_TYPE_IDR;

                    if is_keyframe {
                        keyframe_count += 1;
                    }
                    nal_count += 1;
                    let data: Bytes = std::mem::take(&mut fu_a_buffer).into();
                    let nal = NalUnit {
                        data,
                        is_keyframe,
                        timestamp: now,
                        rtp_timestamp: rtp_ts,
                    };
                    if nal_tx.send(nal).is_err() {
                        debug!("NAL channel has no receivers");
                        return;
                    }
                    fu_a_started = false;
                }
            }

            _ => {
                trace!("ignoring RTP NAL type {nal_type}");
            }
        }

        if nal_count / 500 != prev_nal_count / 500 {
            debug!(
                rtp_packets = rtp_count,
                nal_units = nal_count,
                keyframes = keyframe_count,
                "depacketizer stats"
            );
        }
        prev_nal_count = nal_count;
    }

    info!(
        "RTP receiver closed, depacketizer exiting (total: {nal_count} NALs, {keyframe_count} keyframes)"
    );
}

/// Bit reader for parsing H.264 SPS NAL units (Exp-Golomb coded).
pub struct BitReader<'a> {
    data: &'a [u8],
    byte_offset: usize,
    bit_offset: u8,
}

impl<'a> BitReader<'a> {
    pub fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            byte_offset: 0,
            bit_offset: 0,
        }
    }

    pub fn read_bits(&mut self, n: u8) -> Option<u32> {
        let mut value: u32 = 0;
        for _ in 0..n {
            if self.byte_offset >= self.data.len() {
                return None;
            }
            let bit = (self.data[self.byte_offset] >> (7 - self.bit_offset)) & 1;
            value = (value << 1) | bit as u32;
            self.bit_offset += 1;
            if self.bit_offset == 8 {
                self.bit_offset = 0;
                self.byte_offset += 1;
            }
        }
        Some(value)
    }

    /// Read unsigned Exp-Golomb coded value.
    pub fn read_ue(&mut self) -> Option<u32> {
        let mut leading_zeros: u32 = 0;
        loop {
            let bit = self.read_bits(1)?;
            if bit == 1 {
                break;
            }
            leading_zeros += 1;
            if leading_zeros > 31 {
                return None;
            }
        }
        if leading_zeros == 0 {
            return Some(0);
        }
        let suffix = self.read_bits(leading_zeros as u8)?;
        Some((1 << leading_zeros) - 1 + suffix)
    }

    /// Read signed Exp-Golomb coded value.
    pub fn read_se(&mut self) -> Option<i32> {
        let code = self.read_ue()?;
        let value = code.div_ceil(2) as i32;
        if code % 2 == 0 {
            Some(-value)
        } else {
            Some(value)
        }
    }
}

/// Parse width and height from a raw SPS NAL unit (without start code).
/// Returns `None` on parse failure — caller should fall back to defaults.
pub fn parse_sps_dimensions(sps: &[u8]) -> Option<(u32, u32)> {
    let sps = strip_annexb_start_code(sps);
    if sps.len() < 4 {
        return None;
    }

    // Convert SPS payload (without NAL header) from EBSP to RBSP before bit parsing.
    let rbsp = ebsp_to_rbsp(&sps[1..]);
    if rbsp.len() < 3 {
        return None;
    }

    let profile_idc = rbsp[0];
    // rbsp[1] = constraint flags, rbsp[2] = level_idc
    let mut reader = BitReader::new(&rbsp[3..]);
    let mut chroma_format_idc = 1;
    let mut separate_colour_plane_flag = 0;

    // seq_parameter_set_id
    reader.read_ue()?;

    // High profile and related profiles need extra parsing
    if matches!(
        profile_idc,
        100 | 110 | 122 | 244 | 44 | 83 | 86 | 118 | 128 | 138 | 139 | 134 | 135
    ) {
        chroma_format_idc = reader.read_ue()?;
        if chroma_format_idc > 3 {
            return None;
        }
        if chroma_format_idc == 3 {
            // separate_colour_plane_flag
            separate_colour_plane_flag = reader.read_bits(1)?;
        }
        // bit_depth_luma_minus8
        reader.read_ue()?;
        // bit_depth_chroma_minus8
        reader.read_ue()?;
        // qpprime_y_zero_transform_bypass_flag
        reader.read_bits(1)?;
        // seq_scaling_matrix_present_flag
        let scaling_matrix_present = reader.read_bits(1)?;
        if scaling_matrix_present == 1 {
            let count = if chroma_format_idc != 3 { 8 } else { 12 };
            for i in 0..count {
                let seq_scaling_list_present = reader.read_bits(1)?;
                if seq_scaling_list_present == 1 {
                    let size = if i < 6 { 16 } else { 64 };
                    skip_scaling_list(&mut reader, size)?;
                }
            }
        }
    }

    // log2_max_frame_num_minus4
    reader.read_ue()?;

    let pic_order_cnt_type = reader.read_ue()?;
    match pic_order_cnt_type {
        0 => {
            // log2_max_pic_order_cnt_lsb_minus4
            reader.read_ue()?;
        }
        1 => {
            // delta_pic_order_always_zero_flag
            reader.read_bits(1)?;
            // offset_for_non_ref_pic
            reader.read_se()?;
            // offset_for_top_to_bottom_field
            reader.read_se()?;
            let num_ref_frames_in_pic_order_cnt_cycle = reader.read_ue()?;
            for _ in 0..num_ref_frames_in_pic_order_cnt_cycle {
                reader.read_se()?;
            }
        }
        _ => {}
    }

    // max_num_ref_frames
    reader.read_ue()?;
    // gaps_in_frame_num_value_allowed_flag
    reader.read_bits(1)?;

    let pic_width_in_mbs_minus1 = reader.read_ue()?;
    let pic_height_in_map_units_minus1 = reader.read_ue()?;

    let frame_mbs_only_flag = reader.read_bits(1)?;
    if frame_mbs_only_flag == 0 {
        // mb_adaptive_frame_field_flag
        reader.read_bits(1)?;
    }

    // direct_8x8_inference_flag
    reader.read_bits(1)?;

    let frame_cropping_flag = reader.read_bits(1)?;
    let (crop_left, crop_right, crop_top, crop_bottom) = if frame_cropping_flag == 1 {
        let l = reader.read_ue()?;
        let r = reader.read_ue()?;
        let t = reader.read_ue()?;
        let b = reader.read_ue()?;
        (l, r, t, b)
    } else {
        (0, 0, 0, 0)
    };

    let width = pic_width_in_mbs_minus1.checked_add(1)?.checked_mul(16)?;
    let height = 2_u32
        .checked_sub(frame_mbs_only_flag)?
        .checked_mul(pic_height_in_map_units_minus1.checked_add(1)?)?
        .checked_mul(16)?;

    // ITU-T H.264 7.4.2.1.1: crop units depend on chroma array type and frame structure.
    let chroma_array_type = if separate_colour_plane_flag == 1 {
        0
    } else {
        chroma_format_idc
    };
    let (crop_unit_x, crop_unit_y) = match chroma_array_type {
        0 => (1, 2_u32.checked_sub(frame_mbs_only_flag)?),
        1 => (2, 2 * 2_u32.checked_sub(frame_mbs_only_flag)?),
        2 => (2, 2_u32.checked_sub(frame_mbs_only_flag)?),
        3 => (1, 2_u32.checked_sub(frame_mbs_only_flag)?),
        _ => return None,
    };

    let cropped_width = width.checked_sub(
        crop_left
            .checked_add(crop_right)?
            .checked_mul(crop_unit_x)?,
    )?;
    let cropped_height = height.checked_sub(
        crop_top
            .checked_add(crop_bottom)?
            .checked_mul(crop_unit_y)?,
    )?;

    Some((cropped_width, cropped_height))
}

fn strip_annexb_start_code(nal: &[u8]) -> &[u8] {
    if nal.len() >= 4 && nal[..4] == [0, 0, 0, 1] {
        &nal[4..]
    } else if nal.len() >= 3 && nal[..3] == [0, 0, 1] {
        &nal[3..]
    } else {
        nal
    }
}

fn ebsp_to_rbsp(ebsp: &[u8]) -> Vec<u8> {
    let mut rbsp = Vec::with_capacity(ebsp.len());
    let mut i = 0;

    while i < ebsp.len() {
        if i + 2 < ebsp.len()
            && ebsp[i] == 0x00
            && ebsp[i + 1] == 0x00
            && ebsp[i + 2] == 0x03
            && i + 3 < ebsp.len()
            && ebsp[i + 3] <= 0x03
        {
            rbsp.extend_from_slice(&ebsp[i..=i + 1]);
            i += 3;
            continue;
        }

        rbsp.push(ebsp[i]);
        i += 1;
    }

    rbsp
}

fn skip_scaling_list(reader: &mut BitReader, size: usize) -> Option<()> {
    let mut last_scale: i32 = 8;
    let mut next_scale: i32 = 8;
    for _ in 0..size {
        if next_scale != 0 {
            let delta_scale = reader.read_se()?;
            next_scale = (last_scale + delta_scale + 256) % 256;
        }
        last_scale = if next_scale == 0 {
            last_scale
        } else {
            next_scale
        };
    }
    Some(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, timeout};

    #[test]
    fn test_bit_reader_read_bits() {
        let data = [0b1010_0110, 0b1100_0001];
        let mut reader = BitReader::new(&data);

        assert_eq!(reader.read_bits(1), Some(1));
        assert_eq!(reader.read_bits(1), Some(0));
        assert_eq!(reader.read_bits(4), Some(0b1001));
        assert_eq!(reader.read_bits(2), Some(0b10));
        // Now at byte 1
        assert_eq!(reader.read_bits(8), Some(0b1100_0001));
        // Past end
        assert_eq!(reader.read_bits(1), None);
    }

    #[test]
    fn test_read_ue_zero() {
        // ue(0) = bit sequence "1" => value 0
        let data = [0b1000_0000];
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_ue(), Some(0));
    }

    #[test]
    fn test_read_ue_small_values() {
        // ue(1) = "010" => value 1
        // ue(2) = "011" => value 2
        // ue(3) = "00100" => value 3
        let data = [0b0100_1100, 0b1000_0000];
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_ue(), Some(1));
        assert_eq!(reader.read_ue(), Some(2));
        assert_eq!(reader.read_ue(), Some(3));
    }

    #[test]
    fn test_read_se() {
        // se uses ue mapping: ue=0 -> se=0, ue=1 -> se=1, ue=2 -> se=-1, ue=3 -> se=2, ue=4 -> se=-2
        let data = [0b1010_0110, 0b0100_0000];
        let mut reader = BitReader::new(&data);
        assert_eq!(reader.read_se(), Some(0)); // ue=0
        assert_eq!(reader.read_se(), Some(1)); // ue=1
        assert_eq!(reader.read_se(), Some(-1)); // ue=2
        assert_eq!(reader.read_se(), Some(2)); // ue=3
    }

    struct BitWriter {
        bytes: Vec<u8>,
        current_byte: u8,
        bits_filled: u8,
    }

    impl BitWriter {
        fn new() -> Self {
            Self {
                bytes: Vec::new(),
                current_byte: 0,
                bits_filled: 0,
            }
        }

        fn write_bit(&mut self, bit: u8) {
            self.current_byte = (self.current_byte << 1) | (bit & 1);
            self.bits_filled += 1;
            if self.bits_filled == 8 {
                self.bytes.push(self.current_byte);
                self.current_byte = 0;
                self.bits_filled = 0;
            }
        }

        fn write_bits(&mut self, value: u32, bit_count: u8) {
            for i in (0..bit_count).rev() {
                self.write_bit(((value >> i) & 1) as u8);
            }
        }

        fn write_ue(&mut self, value: u32) {
            let code_num = value + 1;
            let bit_len = (u32::BITS - code_num.leading_zeros()) as u8;
            for _ in 0..(bit_len - 1) {
                self.write_bit(0);
            }
            self.write_bits(code_num, bit_len);
        }

        fn write_rbsp_trailing_bits(&mut self) {
            self.write_bit(1);
            while self.bits_filled != 0 {
                self.write_bit(0);
            }
        }

        fn into_bytes(mut self) -> Vec<u8> {
            if self.bits_filled != 0 {
                self.current_byte <<= 8 - self.bits_filled;
                self.bytes.push(self.current_byte);
            }
            self.bytes
        }
    }

    fn build_high_profile_sps(chroma_format_idc: u32, crop: (u32, u32, u32, u32)) -> Vec<u8> {
        let mut writer = BitWriter::new();

        writer.write_ue(0); // seq_parameter_set_id
        writer.write_ue(chroma_format_idc);
        if chroma_format_idc == 3 {
            writer.write_bits(0, 1); // separate_colour_plane_flag
        }
        writer.write_ue(0); // bit_depth_luma_minus8
        writer.write_ue(0); // bit_depth_chroma_minus8
        writer.write_bits(0, 1); // qpprime_y_zero_transform_bypass_flag
        writer.write_bits(0, 1); // seq_scaling_matrix_present_flag
        writer.write_ue(0); // log2_max_frame_num_minus4
        writer.write_ue(0); // pic_order_cnt_type
        writer.write_ue(0); // log2_max_pic_order_cnt_lsb_minus4
        writer.write_ue(1); // max_num_ref_frames
        writer.write_bits(0, 1); // gaps_in_frame_num_value_allowed_flag
        writer.write_ue(39); // pic_width_in_mbs_minus1 (640px)
        writer.write_ue(29); // pic_height_in_map_units_minus1 (480px progressive)
        writer.write_bits(1, 1); // frame_mbs_only_flag
        writer.write_bits(1, 1); // direct_8x8_inference_flag

        let (crop_left, crop_right, crop_top, crop_bottom) = crop;
        let has_crop = crop_left != 0 || crop_right != 0 || crop_top != 0 || crop_bottom != 0;
        writer.write_bits(has_crop as u32, 1); // frame_cropping_flag
        if has_crop {
            writer.write_ue(crop_left);
            writer.write_ue(crop_right);
            writer.write_ue(crop_top);
            writer.write_ue(crop_bottom);
        }
        writer.write_bits(0, 1); // vui_parameters_present_flag
        writer.write_rbsp_trailing_bits();

        let mut sps = vec![
            0x67, // NAL header
            0x64, // profile_idc = 100 (High)
            0x00, // constraint flags
            0x1F, // level_idc
        ];
        sps.extend_from_slice(&writer.into_bytes());
        sps
    }

    #[test]
    fn test_parse_sps_dimensions_1080p_annexb_with_emulation_prevention_bytes() {
        let mut sps_annexb: Vec<u8> = vec![0x00, 0x00, 0x00, 0x01];
        sps_annexb.extend_from_slice(&[
            0x67, // NAL header (SPS)
            0x64, // profile_idc = 100 (High)
            0x00, // constraint flags
            0x28, // level_idc = 40
            0xAC, 0xD9, 0x40, 0x78, 0x02, 0x27, 0xE5, 0xC0, 0x44, 0x00, 0x00, 0x03, 0x00, 0x04,
            0x00, 0x00, 0x03, 0x00, 0xC8, 0x3C, 0x48, 0x96, 0x58,
        ]);

        let result = parse_sps_dimensions(&sps_annexb);
        assert_eq!(result, Some((1920, 1080)));
    }

    #[test]
    fn test_parse_sps_dimensions_chroma_444_uses_correct_crop_units() {
        // For 4:4:4, crop units are 1x1 (progressive). Fixed *2 logic would under-report.
        let sps = build_high_profile_sps(3, (1, 1, 1, 1));
        let result = parse_sps_dimensions(&sps);
        assert_eq!(result, Some((638, 478)));
    }

    #[test]
    fn test_parse_sps_dimensions_1080p() {
        // Real SPS NAL for 1920x1080 (High profile, level 4.0)
        // NAL header (0x67 = SPS), profile_idc=100, constraint_flags=0x00, level_idc=40
        // pic_width_in_mbs_minus1=119 (=> 120*16=1920)
        // pic_height_in_map_units_minus1=67 (=> 68*16=1088, with crop of 8 => 1080)
        let sps: Vec<u8> = vec![
            0x67, // NAL header (SPS)
            0x64, // profile_idc = 100 (High)
            0x00, // constraint flags
            0x28, // level_idc = 40
            0xAC, 0xD9, 0x40, 0x78, 0x02, 0x27, 0xE5, 0xC0, 0x44, 0x00, 0x00, 0x03, 0x00, 0x04,
            0x00, 0x00, 0x03, 0x00, 0xC8, 0x3C, 0x48, 0x96, 0x58,
        ];

        let result = parse_sps_dimensions(&sps);
        assert!(result.is_some(), "should parse SPS successfully");
        let (w, h) = result.unwrap();
        assert_eq!(w, 1920, "width should be 1920");
        assert_eq!(h, 1080, "height should be 1080");
    }

    #[test]
    fn test_parse_sps_dimensions_720p() {
        // SPS NAL for 1280x720 (High profile)
        // pic_width_in_mbs_minus1=79 (=> 80*16=1280)
        // pic_height_in_map_units_minus1=44 (=> 45*16=720, no crop needed)
        let sps: Vec<u8> = vec![
            0x67, // NAL header
            0x64, // profile_idc = 100
            0x00, // constraint flags
            0x1F, // level_idc = 31
            0xAC, 0xD9, 0x40, 0x50, 0x05, 0xBB, 0x01, 0x10, 0x00, 0x00, 0x03, 0x00, 0x10, 0x00,
            0x00, 0x03, 0x03, 0xC0, 0xF1, 0x62, 0xE4, 0x80,
        ];

        let result = parse_sps_dimensions(&sps);
        assert!(result.is_some(), "should parse SPS successfully");
        let (w, h) = result.unwrap();
        assert_eq!(w, 1280, "width should be 1280");
        assert_eq!(h, 720, "height should be 720");
    }

    #[test]
    fn test_parse_sps_too_short() {
        assert_eq!(parse_sps_dimensions(&[0x67, 0x64, 0x00]), None);
    }

    #[test]
    fn test_parse_sps_empty() {
        assert_eq!(parse_sps_dimensions(&[]), None);
    }

    #[test]
    fn test_parse_sps_baseline_profile() {
        // Baseline profile (66) — no High profile extra fields
        // Hand-encoded: sps_id=0, log2_max_frame_num_minus4=0, poc_type=0,
        // log2_max_poc_lsb_minus4=0, max_ref_frames=1, gaps=0,
        // pic_width_in_mbs_minus1=39 (40*16=640), pic_height_in_map_units_minus1=29 (30*16=480),
        // frame_mbs_only=1, direct_8x8=0, crop=0
        let sps: Vec<u8> = vec![
            0x67, // NAL header
            0x42, // profile_idc = 66 (Baseline)
            0xC0, // constraint flags
            0x1E, // level_idc = 30
            0xF4, 0x05, 0x01, 0xE8,
        ];

        let result = parse_sps_dimensions(&sps);
        assert!(result.is_some(), "should parse baseline SPS");
        let (w, h) = result.unwrap();
        assert_eq!(w, 640);
        assert_eq!(h, 480);
    }

    fn packet_with_payload(timestamp: u32, payload: &[u8]) -> Packet {
        let mut pkt = Packet::default();
        pkt.header.timestamp = timestamp;
        pkt.payload = Bytes::copy_from_slice(payload);
        pkt
    }

    #[tokio::test]
    async fn test_depacketize_stap_a_zero_length_sub_nal_is_skipped() {
        let (rtp_tx, rtp_rx) = mpsc::channel(4);
        let (nal_tx, mut nal_rx) = broadcast::channel(8);
        let handle = tokio::spawn(async move {
            depacketize(rtp_rx, nal_tx).await;
        });

        // STAP-A with one empty sub-NAL followed by one valid IDR NAL (0x65, 0xAA)
        let stap_a = [NAL_TYPE_STAP_A, 0x00, 0x00, 0x00, 0x02, 0x65, 0xAA];
        rtp_tx
            .send(packet_with_payload(1234, &stap_a))
            .await
            .expect("failed to send test RTP packet");
        drop(rtp_tx);

        timeout(Duration::from_secs(1), handle)
            .await
            .expect("depacketize task did not exit")
            .expect("depacketize task panicked");

        let nal = nal_rx
            .recv()
            .await
            .expect("expected one valid NAL from STAP-A");
        assert_eq!(nal.nal_type(), Some(NAL_TYPE_IDR));
        assert_eq!(&nal.data[..], &[0, 0, 0, 1, 0x65, 0xAA]);
    }

    #[tokio::test]
    async fn test_depacketize_stap_a_truncated_sub_nal_is_ignored() {
        let (rtp_tx, rtp_rx) = mpsc::channel(4);
        let (nal_tx, mut nal_rx) = broadcast::channel(8);
        let handle = tokio::spawn(async move {
            depacketize(rtp_rx, nal_tx).await;
        });

        // STAP-A with declared sub-NAL size 4, but only 2 payload bytes available.
        let stap_a = [NAL_TYPE_STAP_A, 0x00, 0x04, 0x65, 0xAA];
        rtp_tx
            .send(packet_with_payload(5678, &stap_a))
            .await
            .expect("failed to send test RTP packet");
        drop(rtp_tx);

        timeout(Duration::from_secs(1), handle)
            .await
            .expect("depacketize task did not exit")
            .expect("depacketize task panicked");

        match nal_rx.try_recv() {
            Err(broadcast::error::TryRecvError::Empty)
            | Err(broadcast::error::TryRecvError::Closed) => {}
            Ok(_) => panic!("unexpected NAL emitted for truncated STAP-A payload"),
            Err(e) => panic!("unexpected receive error: {e}"),
        }
    }
}
