use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use clap::Parser;
use tokio::sync::{mpsc, watch};
use tracing::{error, info, warn};

use recorder_for_jetkvm::config::{CliCommand, Config};
use recorder_for_jetkvm::controller::{ConnectionConfig, JetKvmController};
use recorder_for_jetkvm::{control_protocol, detector, recorder};

fn connection_config(config: &Config, password: String) -> ConnectionConfig {
    ConnectionConfig {
        host: config.host.clone(),
        password,
        no_tls_verify: config.no_tls_verify,
        pli_interval: Duration::from_secs(config.pli_interval),
        reconnect_min: Duration::from_secs(1),
        reconnect_max: Duration::from_secs(60),
    }
}

async fn run_recording(config: &Config, controller: JetKvmController) -> Result<()> {
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let (change_tx, change_rx) = mpsc::channel(64);

    let detector_nals = controller.subscribe_nals();
    let detector_shutdown = shutdown_rx.clone();
    let detector_interval = Duration::from_millis(config.check_interval);
    let detector_sensitivity = config.sensitivity;
    let mut detector_task = tokio::spawn(async move {
        detector::run(
            detector_nals,
            change_tx,
            detector_interval,
            detector_sensitivity,
            detector_shutdown,
        )
        .await;
    });

    let recorder_nals = controller.subscribe_nals();
    let recorder_shutdown = shutdown_rx.clone();
    let output_directory = config.recordings_dir();
    let pre_buffer = config.pre_buffer;
    let cooldown = config.cooldown;
    let mut recorder_task = tokio::spawn(async move {
        recorder::run(
            recorder_nals,
            change_rx,
            output_directory,
            pre_buffer,
            cooldown,
            recorder_shutdown,
        )
        .await;
    });

    tokio::signal::ctrl_c().await?;
    info!("received shutdown signal");
    let _ = shutdown_tx.send(true);
    controller.shutdown().await?;

    let joined = tokio::time::timeout(Duration::from_secs(5), async {
        if let Err(error) = (&mut detector_task).await {
            error!(%error, "detector task failed during shutdown");
        }
        if let Err(error) = (&mut recorder_task).await {
            error!(%error, "recorder task failed during shutdown");
        }
    })
    .await;
    if joined.is_err() {
        warn!("recording pipeline shutdown timed out");
        detector_task.abort();
        recorder_task.abort();
    }
    Ok(())
}

async fn run_screenshot(controller: JetKvmController, output_path: &Path) -> Result<()> {
    let snapshot = tokio::select! {
        result = controller.snapshot(output_path.to_owned()) => result?,
        _ = tokio::signal::ctrl_c() => {
            controller.shutdown().await?;
            return Err(anyhow!("received shutdown signal before screenshot capture"));
        }
    };
    controller.shutdown().await?;
    info!(
        output = %output_path.display(),
        width = snapshot.width,
        height = snapshot.height,
        generation = snapshot.generation,
        "screenshot capture complete"
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                tracing_subscriber::EnvFilter::new(
                    "info,webrtc=error,webrtc_ice=error,webrtc_mdns=error,webrtc_sctp=error,webrtc_srtp=error,webrtc_util=error,dtls=error,rtcp=error,stun=error,turn=error",
                )
            }),
        )
        .init();

    let config = Config::parse();
    config.validate()?;
    ffmpeg_the_third::init().context("failed to initialize FFmpeg")?;
    unsafe {
        ffmpeg_the_third::ffi::av_log_set_level(ffmpeg_the_third::ffi::AV_LOG_FATAL);
    }
    let password = config.resolve_password()?;
    let controller = JetKvmController::connect(connection_config(&config, password)).await?;

    match &config.command {
        Some(CliCommand::Serve { stdio: true }) => {
            info!(host = %config.host, "starting persistent stdio controller");
            control_protocol::serve_stdio(controller).await
        }
        Some(CliCommand::Serve { stdio: false }) => {
            controller.shutdown().await?;
            anyhow::bail!("serve currently requires --stdio")
        }
        None if config.screenshot => {
            let output = config.screenshot_output_path();
            info!(host = %config.host, output = %output.display(), "starting screenshot capture");
            run_screenshot(controller, &output).await
        }
        None => {
            let output = config.recordings_dir();
            info!(
                host = %config.host,
                output_dir = %output.display(),
                sensitivity = config.sensitivity,
                pre_buffer = config.pre_buffer,
                cooldown = config.cooldown,
                check_interval = config.check_interval,
                "starting recorder-for-jetkvm"
            );
            run_recording(&config, controller).await
        }
    }
}
