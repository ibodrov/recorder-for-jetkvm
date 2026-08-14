use std::io::ErrorKind;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};

pub(crate) fn encode_png_atomic(
    frame: &ffmpeg_the_third::frame::Video,
    output_path: &Path,
) -> Result<()> {
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

    let mut encoder = encoder
        .open_as(codec)
        .context("failed to open PNG encoder")?;
    encoder
        .send_frame(&rgb_frame)
        .context("failed to send RGB frame to PNG encoder")?;
    encoder.send_eof().context("failed to flush PNG encoder")?;

    let mut packet = ffmpeg_the_third::Packet::empty();
    loop {
        match encoder.receive_packet(&mut packet) {
            Ok(()) => {
                let data = packet
                    .data()
                    .ok_or_else(|| anyhow!("PNG encoder returned an empty packet"))?;

                let parent = output_path
                    .parent()
                    .filter(|path| !path.as_os_str().is_empty())
                    .unwrap_or_else(|| Path::new("."));
                std::fs::create_dir_all(parent).with_context(|| {
                    format!(
                        "failed to create screenshot directory: {}",
                        parent.display()
                    )
                })?;
                let temporary = tempfile::NamedTempFile::new_in(parent)
                    .context("failed to create temporary screenshot")?;
                std::fs::write(temporary.path(), data)
                    .context("failed to write temporary screenshot")?;
                temporary.persist(output_path).map_err(|err| {
                    anyhow!(
                        "failed to atomically publish screenshot {}: {}",
                        output_path.display(),
                        err.error
                    )
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
