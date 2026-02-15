use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use tokio::sync::{broadcast, mpsc, watch};
use tracing::{error, info, warn};

use recorder_for_jetkvm::config::Config;
use recorder_for_jetkvm::{auth, detector, h264, recorder, screenshot, session};

/// Result of a single recording session.
enum SessionResult {
    /// User requested a clean shutdown (e.g. Ctrl+C).
    Shutdown,
    /// The WebRTC connection failed and we should reconnect.
    ConnectionFailed,
}

/// Run a single recording session: authenticate, set up the pipeline, and
/// stream until the connection drops or the user requests shutdown.
async fn run_session(config: &Config, password: &str) -> Result<SessionResult> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    // Authenticate with JetKVM
    let client = auth::login(&config.host, password, config.no_tls_verify).await?;

    // Channels
    let (rtp_tx, rtp_rx) = mpsc::channel(1024);
    let (nal_tx, _) = broadcast::channel(512);
    let (change_tx, change_rx) = mpsc::channel(64);

    // Start WebRTC session — returns Err on connection failure
    let session_shutdown = shutdown_rx.clone();
    let session_client = client.clone();
    let session_host = config.host.clone();
    let session_pli_interval = Duration::from_secs(config.pli_interval);
    let mut session_handle = tokio::spawn(async move {
        session::run(
            &session_client,
            &session_host,
            rtp_tx,
            session_pli_interval,
            session_shutdown,
        )
        .await
    });

    // Start H.264 depacketizer
    let depack_nal_tx = nal_tx.clone();
    let mut depack_handle = tokio::spawn(async move {
        h264::depacketize(rtp_rx, depack_nal_tx).await;
    });

    // Start change detector
    let detect_nal_rx = nal_tx.subscribe();
    let detect_shutdown = shutdown_rx.clone();
    let detect_interval = Duration::from_millis(config.check_interval);
    let detect_sensitivity = config.sensitivity;
    let mut detect_handle = tokio::spawn(async move {
        detector::run(
            detect_nal_rx,
            change_tx,
            detect_interval,
            detect_sensitivity,
            detect_shutdown,
        )
        .await;
    });

    // Start recorder
    let rec_nal_rx = nal_tx.subscribe();
    let rec_shutdown = shutdown_rx.clone();
    let rec_output_dir = config.recordings_dir();
    let rec_pre_buffer = config.pre_buffer;
    let rec_cooldown = config.cooldown;
    let mut rec_handle = tokio::spawn(async move {
        recorder::run(
            rec_nal_rx,
            change_rx,
            rec_output_dir,
            rec_pre_buffer,
            rec_cooldown,
            rec_shutdown,
        )
        .await;
    });

    // Wait for either Ctrl+C or session failure
    let session_failed;
    let mut session_joined = false;
    tokio::select! {
        result = &mut session_handle => {
            session_joined = true;
            match result {
                Ok(Ok(())) => {
                    session_failed = false;
                }
                Ok(Err(e)) => {
                    if session::is_connection_lost_error(&e) {
                        warn!("WebRTC session ended, reconnecting: {e}");
                    } else {
                        error!("WebRTC session error: {e}");
                    }
                    session_failed = true;
                }
                Err(e) => {
                    error!("session task panicked: {e}");
                    session_failed = true;
                }
            }
        }
        _ = tokio::signal::ctrl_c() => {
            info!("received shutdown signal");
            session_failed = false;
        }
    }

    // Signal all tasks to shut down
    let _ = shutdown_tx.send(true);
    drop(nal_tx);

    // Wait for all tasks to finish (with timeout), then abort any stragglers.
    let shutdown_result = tokio::time::timeout(Duration::from_secs(5), async {
        if !session_joined && let Err(e) = (&mut session_handle).await {
            error!("session task join error during shutdown: {e}");
        }
        if let Err(e) = (&mut depack_handle).await {
            error!("depacketizer task join error during shutdown: {e}");
        }
        if let Err(e) = (&mut detect_handle).await {
            error!("detector task join error during shutdown: {e}");
        }
        if let Err(e) = (&mut rec_handle).await {
            error!("recorder task join error during shutdown: {e}");
        }
    })
    .await;

    if shutdown_result.is_err() {
        warn!("timeout waiting for pipeline shutdown, aborting remaining tasks");
        if !session_handle.is_finished() {
            session_handle.abort();
        }
        if !depack_handle.is_finished() {
            depack_handle.abort();
        }
        if !detect_handle.is_finished() {
            detect_handle.abort();
        }
        if !rec_handle.is_finished() {
            rec_handle.abort();
        }
    }

    if session_failed {
        Ok(SessionResult::ConnectionFailed)
    } else {
        Ok(SessionResult::Shutdown)
    }
}

async fn run_screenshot_session(config: &Config, password: &str, output_path: &std::path::Path) -> Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);

    let client = auth::login(&config.host, password, config.no_tls_verify).await?;

    let (rtp_tx, rtp_rx) = mpsc::channel(1024);
    let (nal_tx, _) = broadcast::channel(512);

    let session_shutdown = shutdown_rx.clone();
    let session_client = client.clone();
    let session_host = config.host.clone();
    let session_pli_interval = Duration::from_secs(config.pli_interval);
    let mut session_handle = tokio::spawn(async move {
        session::run(
            &session_client,
            &session_host,
            rtp_tx,
            session_pli_interval,
            session_shutdown,
        )
        .await
    });

    let depack_nal_tx = nal_tx.clone();
    let mut depack_handle = tokio::spawn(async move {
        h264::depacketize(rtp_rx, depack_nal_tx).await;
    });

    let screenshot_nal_rx = nal_tx.subscribe();
    let screenshot_shutdown = shutdown_rx.clone();
    let screenshot_path = output_path.to_path_buf();
    let mut screenshot_handle = tokio::spawn(async move {
        screenshot::capture(screenshot_nal_rx, &screenshot_path, screenshot_shutdown).await
    });

    let mut session_joined = false;
    let mut screenshot_joined = false;
    let outcome = tokio::select! {
        result = &mut screenshot_handle => {
            screenshot_joined = true;
            match result {
                Ok(result) => result,
                Err(e) => Err(anyhow!("screenshot task panicked: {e}")),
            }
        }
        result = &mut session_handle => {
            session_joined = true;
            match result {
                Ok(Ok(())) => Err(anyhow!("WebRTC session ended before the screenshot was captured")),
                Ok(Err(e)) => Err(e).context("WebRTC session failed before the screenshot was captured"),
                Err(e) => Err(anyhow!("session task panicked: {e}")),
            }
        }
        _ = tokio::signal::ctrl_c() => {
            Err(anyhow!("received shutdown signal before the screenshot was captured"))
        }
    };

    let _ = shutdown_tx.send(true);
    drop(nal_tx);

    let shutdown_result = tokio::time::timeout(Duration::from_secs(5), async {
        if !session_joined && let Err(e) = (&mut session_handle).await {
            error!("session task join error during shutdown: {e}");
        }
        if let Err(e) = (&mut depack_handle).await {
            error!("depacketizer task join error during shutdown: {e}");
        }
        if !screenshot_joined && let Err(e) = (&mut screenshot_handle).await {
            error!("screenshot task join error during shutdown: {e}");
        }
    })
    .await;

    if shutdown_result.is_err() {
        warn!("timeout waiting for screenshot pipeline shutdown, aborting remaining tasks");
        if !session_handle.is_finished() {
            session_handle.abort();
        }
        if !depack_handle.is_finished() {
            depack_handle.abort();
        }
        if !screenshot_handle.is_finished() {
            screenshot_handle.abort();
        }
    }

    outcome
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(
                    "info,webrtc=error,webrtc_ice=error,webrtc_mdns=error,webrtc_sctp=error,webrtc_srtp=error,webrtc_util=error,dtls=error,rtcp=error,stun=error,turn=error"
                )),
        )
        .init();

    let config = Config::parse();

    // Suppress noisy ffmpeg log messages (they bypass tracing and go to stderr)
    ffmpeg_the_third::init().context("failed to initialize FFmpeg")?;
    unsafe {
        ffmpeg_the_third::ffi::av_log_set_level(ffmpeg_the_third::ffi::AV_LOG_FATAL);
    }

    // Resolve password from CLI, env var, or file
    let password = config.resolve_password()?;

    if config.screenshot {
        let screenshot_output = config.screenshot_output_path();
        info!(
            host = %config.host,
            output = %screenshot_output.display(),
            pli_interval = config.pli_interval,
            "starting single screenshot capture"
        );
        run_screenshot_session(&config, &password, &screenshot_output).await?;
        info!(output = %screenshot_output.display(), "screenshot capture complete");
        return Ok(());
    }

    let recordings_dir = config.recordings_dir();
    info!(
        host = %config.host,
        output_dir = %recordings_dir.display(),
        sensitivity = config.sensitivity,
        pre_buffer = config.pre_buffer,
        cooldown = config.cooldown,
        check_interval = config.check_interval,
        pli_interval = config.pli_interval,
        "starting recorder-for-jetkvm"
    );

    // Reconnection loop with exponential backoff
    let mut backoff = Duration::from_secs(1);
    let max_backoff = Duration::from_secs(60);

    loop {
        match run_session(&config, &password).await {
            Ok(SessionResult::Shutdown) => {
                info!("shutdown complete");
                return Ok(());
            }
            Ok(SessionResult::ConnectionFailed) => {
                // fall through to backoff
            }
            Err(e) => {
                warn!("session failed (will retry): {e}");
                // fall through to backoff
            }
        }

        warn!("reconnecting in {backoff:?}");
        tokio::select! {
            _ = tokio::time::sleep(backoff) => {}
            _ = tokio::signal::ctrl_c() => {
                info!("received shutdown signal during backoff");
                return Ok(());
            }
        }
        backoff = (backoff * 2).min(max_backoff);
    }
}
