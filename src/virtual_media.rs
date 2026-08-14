use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures_util::stream;
use reqwest::Body;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Notify, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::peer_connection::RTCPeerConnection;

use crate::auth::AuthenticatedClient;
use crate::error::{CodedError, codes};
use crate::hid::HidClient;
use crate::range_server::{REDACTED_LOCAL_MEDIA_URL, RangeServer, is_controller_owned_url};
use crate::rpc::{
    MountUrlInfo, RpcClient, StorageFile, StorageSpace, VirtualMediaMode, VirtualMediaState,
};

const PROGRESS_INTERVAL: Duration = Duration::from_millis(200);
const UPLOAD_CHUNK_SIZE: usize = 16 * 1024;
const DATA_CHANNEL_BUFFER_LOW: usize = 512 * 1024;
const DATA_CHANNEL_BUFFER_HIGH: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Approval {
    pub approved: bool,
}

impl Approval {
    pub fn require(self, operation: &str) -> Result<()> {
        if !self.approved {
            return Err(CodedError::new(
                codes::APPROVAL_REQUIRED,
                format!("explicit approval is required to {operation}"),
            )
            .into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum MediaEvent {
    UploadProgress {
        filename: String,
        uploaded_bytes: u64,
        total_bytes: u64,
    },
    StaleControllerMount {
        redacted_url: String,
    },
}

pub(crate) struct VirtualMediaManager {
    rpc: RpcClient,
    auth: AuthenticatedClient,
    hid: HidClient,
    peer_connection: Arc<RTCPeerConnection>,
    supports_check_mount_url: bool,
    events: broadcast::Sender<MediaEvent>,
    range_server: Option<RangeServer>,
    stale_controller_mount: bool,
    /// Origin proofs for uploads started by this manager, keyed by device
    /// filename; survives reconnects so interrupted uploads can resume.
    upload_origins: UploadOrigins,
}

impl VirtualMediaManager {
    pub fn new(
        rpc: RpcClient,
        auth: AuthenticatedClient,
        hid: HidClient,
        peer_connection: Arc<RTCPeerConnection>,
        supports_check_mount_url: bool,
        events: broadcast::Sender<MediaEvent>,
    ) -> Self {
        Self {
            rpc,
            auth,
            hid,
            peer_connection,
            supports_check_mount_url,
            events,
            range_server: None,
            stale_controller_mount: false,
            upload_origins: UploadOrigins::default(),
        }
    }

    pub fn rebind(
        &mut self,
        rpc: RpcClient,
        hid: HidClient,
        peer_connection: Arc<RTCPeerConnection>,
        auth: AuthenticatedClient,
        supports_check_mount_url: bool,
    ) {
        self.rpc.cancel_generation();
        self.rpc = rpc;
        self.hid = hid;
        self.peer_connection = peer_connection;
        self.auth = auth;
        self.supports_check_mount_url = supports_check_mount_url;
    }

    pub fn supports_check_mount_url(&self) -> bool {
        self.supports_check_mount_url
    }

    pub fn has_stale_controller_mount(&self) -> bool {
        self.stale_controller_mount
    }

    pub async fn refresh_state(&mut self) -> Result<Option<VirtualMediaState>> {
        let state = self.rpc.media_state().await?;
        self.stale_controller_mount = state
            .as_ref()
            .and_then(|state| state.url.as_deref())
            .filter(|url| is_controller_owned_url(url))
            .is_some_and(|url| {
                self.range_server
                    .as_ref()
                    .is_none_or(|server| server.mount_url() != url || !server.is_healthy())
            });
        if self.stale_controller_mount {
            let _ = self.events.send(MediaEvent::StaleControllerMount {
                redacted_url: REDACTED_LOCAL_MEDIA_URL.to_owned(),
            });
        }
        Ok(state.map(redact_state))
    }

    pub async fn check_url(&mut self, url: &str) -> Result<MountUrlInfo> {
        if !self.supports_check_mount_url {
            bail!("check_mount_url is unsupported by this JetKVM firmware");
        }
        validate_http_url(url)?;
        match self.rpc.check_mount_url(url).await {
            Ok(info) => Ok(info),
            Err(error) if is_check_mount_url_unimplemented(&error) => {
                self.supports_check_mount_url = false;
                Err(error).context("check_mount_url is unsupported by this JetKVM firmware")
            }
            Err(error) => Err(error),
        }
    }

    pub async fn mount_url(
        &mut self,
        url: &str,
        mode: VirtualMediaMode,
        approval: Approval,
    ) -> Result<VirtualMediaState> {
        approval.require("mount a remote URL")?;
        validate_http_url(url)?;
        self.ensure_unmounted().await?;
        let expected_size = if self.supports_check_mount_url {
            match self.rpc.check_mount_url(url).await {
                Ok(checked) => {
                    if !checked.usable {
                        bail!("mount URL is unusable: {}", checked.reason);
                    }
                    Some(checked.size)
                }
                Err(error) if is_check_mount_url_unimplemented(&error) => {
                    warn!(
                        "checkMountUrl is not implemented by this firmware build; \
                         disabling preflight URL checks"
                    );
                    self.supports_check_mount_url = false;
                    None
                }
                Err(error) => return Err(error),
            }
        } else {
            None
        };
        self.with_hid_recovery(self.rpc.mount_http(url, mode))
            .await?;
        let state = self
            .verified_state(|state| {
                state.source == "HTTP"
                    && state.mode == mode
                    && state.url.as_deref() == Some(url)
                    && expected_size.is_none_or(|size| state.size == size)
            })
            .await
            .context("JetKVM did not report the requested URL mount")?;
        Ok(redact_state(state))
    }

    pub async fn mount_local(
        &mut self,
        image: &Path,
        mode: VirtualMediaMode,
        approval: Approval,
    ) -> Result<VirtualMediaState> {
        approval.require("disclose and mount a local image")?;
        self.ensure_unmounted().await?;
        if self.range_server.is_some() {
            bail!("a controller-hosted image is already active");
        }
        let server = RangeServer::start(image, self.auth.base_url()).await?;
        server.ensure_healthy()?;
        let url = server.mount_url().to_owned();
        let expected_size = if self.supports_check_mount_url {
            match self.rpc.check_mount_url(&url).await {
                Ok(checked) if checked.usable => Some(checked.size),
                Ok(checked) => {
                    server.shutdown().await?;
                    bail!("JetKVM cannot read the local image: {}", checked.reason);
                }
                Err(error) if is_check_mount_url_unimplemented(&error) => {
                    warn!(
                        "checkMountUrl is not implemented by this firmware build; \
                         disabling preflight URL checks"
                    );
                    self.supports_check_mount_url = false;
                    None
                }
                Err(error) => {
                    server.shutdown().await?;
                    return Err(error).context("failed to validate controller-hosted image");
                }
            }
        } else {
            None
        };
        server.ensure_healthy()?;
        if let Err(error) = self
            .with_hid_recovery(self.rpc.mount_http(&url, mode))
            .await
        {
            server.shutdown().await?;
            return Err(error);
        }
        self.range_server = Some(server);
        let state = match self
            .verified_state(|state| {
                state.source == "HTTP"
                    && state.mode == mode
                    && state.url.as_deref() == Some(url.as_str())
                    && expected_size.is_none_or(|size| state.size == size)
            })
            .await
        {
            Ok(state) => state,
            Err(verification_error) => {
                let rollback_result = async {
                    self.rpc.unmount().await?;
                    self.verified_unmounted().await
                }
                .await;
                return self
                    .complete_failed_local_mount(verification_error, rollback_result)
                    .await;
            }
        };
        self.range_server
            .as_ref()
            .expect("range server retained after mount")
            .ensure_healthy()?;
        self.stale_controller_mount = false;
        Ok(redact_state(state))
    }
    async fn complete_failed_local_mount(
        &mut self,
        verification_error: anyhow::Error,
        rollback_result: Result<()>,
    ) -> Result<VirtualMediaState> {
        if let Err(rollback_error) = rollback_result {
            self.stale_controller_mount = true;
            let _ = self.events.send(MediaEvent::StaleControllerMount {
                redacted_url: REDACTED_LOCAL_MEDIA_URL.to_owned(),
            });
            return Err(anyhow::anyhow!(
                "JetKVM did not report the controller-hosted mount: \
                 {verification_error:#}; rollback failed while the range server was \
                 kept alive: {rollback_error:#}"
            ));
        }
        if let Some(server) = self.range_server.take() {
            server
                .shutdown()
                .await
                .context("failed to stop the range server after mount rollback")?;
        }
        Err(verification_error).context("JetKVM did not report the controller-hosted mount")
    }

    pub async fn unmount(&mut self, approval: Approval) -> Result<()> {
        approval.require("unmount virtual media")?;
        self.with_hid_recovery(self.rpc.unmount()).await?;
        self.verified_unmounted().await?;
        if let Some(server) = self.range_server.take() {
            server.shutdown().await?;
        }
        self.stale_controller_mount = false;
        Ok(())
    }

    pub async fn storage_space(&self) -> Result<StorageSpace> {
        self.rpc.storage_space().await
    }

    pub async fn storage_files(&self) -> Result<Vec<StorageFile>> {
        Ok(self.rpc.storage_files().await?.files)
    }

    /// Uploads a local image to device storage, resuming an interrupted
    /// upload when the device holds a partial with a matching origin.
    ///
    /// Admission order: validate inputs, hash the complete source, obtain and
    /// validate the resume offset, verify the recorded source identity, check
    /// free space against the remaining bytes, transfer that range, re-hash
    /// the source, and verify the completed device file. Partials are never
    /// deleted implicitly; unknown or different origins are rejected.
    pub async fn upload(
        &mut self,
        image: &Path,
        filename: &str,
        approval: Approval,
        cancellation: CancellationToken,
    ) -> Result<StorageFile> {
        approval.require("upload a local image")?;
        if cancellation.is_cancelled() {
            return upload_cancelled("before it started");
        }
        validate_filename(filename)?;
        let canonical = tokio::fs::canonicalize(image)
            .await
            .with_context(|| format!("failed to canonicalize upload image: {}", image.display()))?;
        let metadata = tokio::fs::metadata(&canonical)
            .await
            .context("failed to inspect upload image")?;
        if !metadata.is_file() || metadata.len() == 0 {
            bail!("upload image must be a non-empty regular file");
        }
        let size = metadata.len();
        if cancellation.is_cancelled() {
            return upload_cancelled("before disclosing the local image");
        }
        let origin = read_upload_origin(&canonical, &cancellation).await?;

        if cancellation.is_cancelled() {
            return upload_cancelled("before creating the upload");
        }
        // A previous attempt in this process may have been cancelled while
        // its device-side handler still drains buffered bytes into the same
        // `.incomplete` file. Wait for the partial to stop growing so the
        // resume offset is stable.
        if self.upload_origins.0.contains_key(filename) {
            self.settle_partial_upload(filename).await;
        }
        let upload = self.rpc.start_upload(filename, size).await?;
        let mut uploaded = upload.already_uploaded_bytes;
        validate_upload_offset(uploaded, size)?;
        debug!(
            filename,
            size,
            already_uploaded = uploaded,
            "storage upload registered with JetKVM"
        );
        self.verify_upload_origin(filename, &origin, uploaded)?;

        let remaining = size - uploaded;
        if remaining > 0 {
            let space = self.rpc.storage_space().await?;
            if remaining > space.bytes_free {
                return Err(CodedError::new(
                    codes::OPERATION_FAILED,
                    format!(
                        "JetKVM storage has {} bytes free but the upload needs {remaining} more; \
                         the partial upload is kept for resume",
                        space.bytes_free
                    ),
                )
                .into());
            }
            let http_result = upload_over_http(
                &self.auth,
                &upload.upload_id,
                UploadTransfer {
                    path: &canonical,
                    filename,
                    total: size,
                    uploaded,
                    events: &self.events,
                    cancellation: &cancellation,
                },
            )
            .await;
            if let Err(http_error) = http_result {
                if cancellation.is_cancelled() {
                    return upload_cancelled("during HTTP transfer");
                }
                warn!(%http_error, "direct HTTP storage upload failed; falling back to WebRTC");
                if cancellation.is_cancelled() {
                    return upload_cancelled("before the WebRTC fallback");
                }
                // Re-read the resume offset: the HTTP attempt may have
                // advanced the device's partial upload.
                let upload = self.rpc.start_upload(filename, size).await?;
                uploaded = upload.already_uploaded_bytes;
                validate_upload_offset(uploaded, size)?;
                upload_over_data_channel(
                    &self.peer_connection,
                    &upload.upload_id,
                    UploadTransfer {
                        path: &canonical,
                        filename,
                        total: size,
                        uploaded,
                        events: &self.events,
                        cancellation: &cancellation,
                    },
                )
                .await?;
            }
        }

        let completed_origin = read_upload_origin(&canonical, &cancellation).await?;
        if completed_origin != origin {
            return Err(CodedError::new(
                codes::OPERATION_FAILED,
                "upload source changed while it was being transferred; \
                 the device partial is kept but cannot be resumed from this source",
            )
            .into());
        }

        wait_for_storage_file(&self.rpc, filename, size, &cancellation).await
    }

    /// Polls until the device-side partial stops growing across two polls,
    /// so a cancelled transfer's handler has fully drained before we ask
    /// for a resume offset.
    async fn settle_partial_upload(&self, filename: &str) {
        let partial = format!("{filename}.incomplete");
        let mut last: Option<u64> = None;
        for _ in 0..16 {
            let size = self.rpc.storage_files().await.ok().and_then(|files| {
                files
                    .files
                    .into_iter()
                    .find(|file| file.filename == partial)
                    .map(|file| file.size)
            });
            if size == last && size.is_some() {
                return;
            }
            last = size;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        warn!(filename, "partial upload did not settle before resume");
    }

    /// Origin check for resumed uploads. Fresh uploads (offset 0) record
    /// their origin; resumes must match the recorded full-source digest.
    fn verify_upload_origin(
        &mut self,
        filename: &str,
        origin: &UploadOrigin,
        uploaded: u64,
    ) -> Result<()> {
        self.upload_origins.verify(filename, origin, uploaded)
    }

    pub async fn mount_storage(
        &mut self,
        filename: &str,
        mode: VirtualMediaMode,
        approval: Approval,
    ) -> Result<VirtualMediaState> {
        approval.require("mount a stored image")?;
        validate_filename(filename)?;
        self.ensure_unmounted().await?;
        let expected_size = self
            .rpc
            .storage_files()
            .await?
            .files
            .into_iter()
            .find(|file| file.filename == filename)
            .context("stored image does not exist")?
            .size;
        self.with_hid_recovery(self.rpc.mount_storage(filename, mode))
            .await?;
        let state = self
            .verified_state(|state| {
                state.source == "Storage"
                    && state.mode == mode
                    && state.filename.as_deref() == Some(filename)
                    && state.size == expected_size
            })
            .await
            .context("JetKVM did not report the requested storage mount")?;
        Ok(state)
    }

    pub async fn delete_storage_file(&self, filename: &str, approval: Approval) -> Result<()> {
        approval.require("delete a stored image")?;
        validate_filename(filename)?;
        if self
            .rpc
            .media_state()
            .await?
            .is_some_and(|state| state.filename.as_deref() == Some(filename))
        {
            bail!("cannot delete a mounted image");
        }
        self.rpc.delete_storage_file(filename).await?;
        if self
            .rpc
            .storage_files()
            .await?
            .files
            .iter()
            .any(|file| file.filename == filename)
        {
            bail!("JetKVM still reports the deleted storage file");
        }
        Ok(())
    }

    pub async fn clean_shutdown(mut self) -> Result<()> {
        if self.range_server.is_some() {
            self.rpc.unmount().await?;
            self.verified_unmounted().await?;
            if let Some(server) = self.range_server.take() {
                server.shutdown().await?;
            }
        }
        Ok(())
    }

    async fn ensure_unmounted(&self) -> Result<()> {
        if self.rpc.media_state().await?.is_some() {
            bail!("virtual media is already mounted; unmount it explicitly first");
        }
        Ok(())
    }

    async fn with_hid_recovery<T>(&self, operation: impl Future<Output = Result<T>>) -> Result<T> {
        self.hid.reset().await?;
        let result = operation.await;
        self.hid.wait_ready(Duration::from_secs(15)).await?;
        self.hid.reset().await?;
        result
    }

    async fn verified_state(
        &self,
        predicate: impl Fn(&VirtualMediaState) -> bool,
    ) -> Result<VirtualMediaState> {
        for _ in 0..20 {
            if let Some(state) = self.rpc.media_state().await?
                && predicate(&state)
            {
                return Ok(state);
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        bail!("virtual media state verification timed out")
    }

    async fn verified_unmounted(&self) -> Result<()> {
        for _ in 0..40 {
            if self.rpc.media_state().await?.is_none() {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        bail!("virtual media remained mounted after unmount")
    }
}

fn upload_timeout(remaining_bytes: u64) -> Duration {
    const ASSUMED_BYTES_PER_SECOND: u64 = 256 * 1024;

    const MINIMUM_SECONDS: u64 = 60;
    const MAXIMUM_SECONDS: u64 = 2 * 60 * 60;

    let transfer_seconds = remaining_bytes.div_ceil(ASSUMED_BYTES_PER_SECOND);
    Duration::from_secs(
        MINIMUM_SECONDS
            .saturating_add(transfer_seconds)
            .min(MAXIMUM_SECONDS),
    )
}
fn validate_upload_offset(uploaded: u64, total: u64) -> Result<()> {
    if uploaded > total {
        bail!("JetKVM reported an invalid upload resume offset");
    }
    Ok(())
}

/// Cryptographic identity for a resumable upload. The full source is hashed
/// before transfer, and the same identity is checked again after transfer so
/// equal-sized images with a shared prefix cannot be spliced.
#[derive(Debug, Clone, PartialEq, Eq)]
struct UploadOrigin {
    size: u64,
    sha256: [u8; 32],
}

/// Per-filename origin proofs for uploads started by this controller. A
/// device-side partial upload with no matching proof is rejected so two
/// different images can never be spliced across a resume.
#[derive(Debug, Default)]
struct UploadOrigins(std::collections::HashMap<String, UploadOrigin>);

impl UploadOrigins {
    fn verify(&mut self, filename: &str, origin: &UploadOrigin, uploaded: u64) -> Result<()> {
        if uploaded == 0 {
            self.0.insert(filename.to_owned(), origin.clone());
            return Ok(());
        }
        match self.0.get(filename) {
            Some(recorded) if recorded == origin => Ok(()),
            Some(_) => Err(CodedError::new(
                codes::OPERATION_FAILED,
                format!(
                    "a partial upload named '{filename}' belongs to a different local image; \
                     refusing to splice — delete the partial with delete_storage first"
                ),
            )
            .into()),
            None => Err(CodedError::new(
                codes::OPERATION_FAILED,
                format!(
                    "a partial upload named '{filename}' has no recorded origin in this \
                     controller; refusing to resume — delete it with delete_storage first \
                     or resume from the controller session that started it"
                ),
            )
            .into()),
        }
    }
}

async fn read_upload_origin(path: &Path, cancellation: &CancellationToken) -> Result<UploadOrigin> {
    let mut file = tokio::fs::File::open(path)
        .await
        .context("failed to open upload image")?;
    let before = file
        .metadata()
        .await
        .context("failed to inspect upload image")?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];
    let mut hashed = 0_u64;
    loop {
        let count = tokio::select! {
            _ = cancellation.cancelled() => return upload_cancelled("while identifying the source"),
            result = file.read(&mut buffer) => result.context("failed to read upload image")?,
        };
        if count == 0 {
            break;
        }
        hasher.update(&buffer[..count]);
        hashed = hashed.saturating_add(count as u64);
    }
    let after = file
        .metadata()
        .await
        .context("failed to re-inspect upload image")?;
    if before.len() != after.len() || hashed != before.len() {
        bail!("upload source changed while its identity was being computed");
    }
    Ok(UploadOrigin {
        size: before.len(),
        sha256: hasher.finalize().into(),
    })
}

fn upload_cancelled<T>(stage: &str) -> Result<T> {
    Err(CodedError::new(codes::CANCELLED, format!("upload cancelled {stage}")).into())
}

struct UploadTransfer<'a> {
    path: &'a Path,
    filename: &'a str,
    total: u64,
    uploaded: u64,
    events: &'a broadcast::Sender<MediaEvent>,
    cancellation: &'a CancellationToken,
}

async fn upload_over_http(
    auth: &AuthenticatedClient,
    upload_id: &str,
    transfer: UploadTransfer<'_>,
) -> Result<()> {
    let UploadTransfer {
        path,
        filename,
        total,
        uploaded,
        events,
        cancellation,
    } = transfer;
    let mut file = tokio::fs::File::open(path)
        .await
        .context("failed to open upload image")?;
    file.seek(std::io::SeekFrom::Start(uploaded))
        .await
        .context("failed to seek upload image")?;
    let stream = upload_stream(file, uploaded, total, filename.to_owned(), events.clone());
    let upload_url = format!("{}/storage/upload?uploadId={upload_id}", auth.base_url());
    let request = auth
        .client()
        .post(upload_url)
        .body(Body::wrap_stream(stream))
        .timeout(upload_timeout(total - uploaded))
        .send();
    let response = tokio::select! {
        _ = cancellation.cancelled() => return upload_cancelled("during HTTP transfer"),
        response = request => response.context("storage upload request failed")?,
    };
    if !response.status().is_success() {
        bail!("storage upload failed (HTTP {})", response.status());
    }
    Ok(())
}

async fn upload_over_data_channel(
    peer_connection: &Arc<RTCPeerConnection>,
    upload_id: &str,
    transfer: UploadTransfer<'_>,
) -> Result<()> {
    let UploadTransfer {
        path,
        filename,
        total,
        uploaded,
        events,
        cancellation,
    } = transfer;
    let channel = peer_connection
        .create_data_channel(upload_id, None)
        .await
        .context("failed to create WebRTC upload channel")?;
    channel
        .set_buffered_amount_low_threshold(DATA_CHANNEL_BUFFER_LOW)
        .await;

    let buffer_low = Arc::new(Notify::new());
    let buffer_low_callback = Arc::clone(&buffer_low);
    channel
        .on_buffered_amount_low(Box::new(move || {
            let buffer_low = Arc::clone(&buffer_low_callback);
            Box::pin(async move {
                buffer_low.notify_one();
            })
        }))
        .await;
    let closed = Arc::new(Notify::new());
    let closed_callback = Arc::clone(&closed);
    channel.on_close(Box::new(move || {
        let closed = Arc::clone(&closed_callback);
        Box::pin(async move {
            closed.notify_one();
        })
    }));

    if let Err(error) =
        crate::session::wait_data_channel_open(&channel, Duration::from_secs(15), cancellation)
            .await
    {
        let _ = channel.close().await;
        if cancellation.is_cancelled() {
            return upload_cancelled("while opening the upload channel");
        }
        return Err(error).context("failed to open WebRTC upload channel");
    }

    // Every error exit after the channel opened must close it: the device
    // keeps the pending upload open until channel closure.
    let transfer = async {
        let mut file = tokio::fs::File::open(path)
            .await
            .context("failed to open upload image")?;
        file.seek(std::io::SeekFrom::Start(uploaded))
            .await
            .context("failed to seek upload image")?;
        let mut sent = uploaded;
        let mut last_progress = Instant::now() - PROGRESS_INTERVAL;
        while sent < total {
            while channel.buffered_amount().await > DATA_CHANNEL_BUFFER_HIGH {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return upload_cancelled("during transfer");
                    }
                    _ = buffer_low.notified() => {}
                    _ = closed.notified() => {
                        bail!("WebRTC upload channel closed before upload completed");
                    }
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {
                        bail!("WebRTC upload channel backpressure timed out");
                    }
                }
            }
            if channel.ready_state() != RTCDataChannelState::Open {
                bail!("WebRTC upload channel closed before upload completed");
            }
            let length = usize::try_from((total - sent).min(UPLOAD_CHUNK_SIZE as u64))
                .expect("bounded upload chunk fits usize");
            let mut buffer = vec![0_u8; length];
            tokio::select! {
                _ = cancellation.cancelled() => {
                    return upload_cancelled("during transfer");
                }
                result = file.read_exact(&mut buffer) => {
                    result.context("failed to read upload image")?;
                }
            }
            channel
                .send(&Bytes::from(buffer))
                .await
                .context("failed to send WebRTC upload data")?;
            sent += length as u64;
            if last_progress.elapsed() >= PROGRESS_INTERVAL || sent == total {
                let _ = events.send(MediaEvent::UploadProgress {
                    filename: filename.to_owned(),
                    uploaded_bytes: sent,
                    total_bytes: total,
                });
                last_progress = Instant::now();
            }
        }

        tokio::time::timeout(upload_timeout(total - uploaded), async {
            loop {
                if channel.ready_state() == RTCDataChannelState::Closed {
                    return Ok(());
                }
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return upload_cancelled("while waiting for completion");
                    }
                    _ = closed.notified() => {}
                }
            }
        })
        .await
        .context("timed out waiting for WebRTC upload completion")?
    }
    .await;
    if transfer.is_err() {
        let _ = channel.close().await;
    }
    transfer
}

async fn wait_for_storage_file(
    rpc: &RpcClient,
    filename: &str,
    size: u64,
    cancellation: &CancellationToken,
) -> Result<StorageFile> {
    for _ in 0..120 {
        let files = rpc.storage_files().await?.files;
        if let Some(file) = files
            .into_iter()
            .find(|file| file.filename == filename && file.size == size)
        {
            return Ok(file);
        }
        tokio::select! {
            _ = cancellation.cancelled() => return upload_cancelled("while waiting for the completed file"),
            _ = tokio::time::sleep(Duration::from_millis(250)) => {}
        }
    }
    bail!("uploaded file was not reported with the expected name and size")
}

fn upload_stream(
    file: tokio::fs::File,
    uploaded: u64,
    total: u64,
    filename: String,
    events: broadcast::Sender<MediaEvent>,
) -> impl futures_util::Stream<Item = std::io::Result<Bytes>> + Send + 'static {
    stream::unfold(
        (file, uploaded, Instant::now() - PROGRESS_INTERVAL),
        move |(mut file, mut uploaded, mut last_progress)| {
            let events = events.clone();
            let filename = filename.clone();
            async move {
                if uploaded >= total {
                    return None;
                }
                let length = usize::try_from((total - uploaded).min(UPLOAD_CHUNK_SIZE as u64))
                    .expect("bounded upload chunk fits usize");
                let mut buffer = vec![0_u8; length];
                match file.read_exact(&mut buffer).await {
                    Ok(_) => {
                        uploaded += length as u64;
                        if last_progress.elapsed() >= PROGRESS_INTERVAL || uploaded == total {
                            let _ = events.send(MediaEvent::UploadProgress {
                                filename,
                                uploaded_bytes: uploaded,
                                total_bytes: total,
                            });
                            last_progress = Instant::now();
                        }
                        Some((Ok(Bytes::from(buffer)), (file, uploaded, last_progress)))
                    }
                    Err(error) => Some((Err(error), (file, uploaded, last_progress))),
                }
            }
        },
    )
}

/// True when the device rejected `checkMountUrl` as unknown or unimplemented
/// (firmware builds that register the method as a stub or not at all).
fn is_check_mount_url_unimplemented(error: &anyhow::Error) -> bool {
    let message = format!("{error:#}").to_ascii_lowercase();
    message.contains("not implemented")
        || message.contains("method not found")
        || message.contains("unknown method")
        || message.contains("remote rpc error -32601")
}

fn validate_http_url(url: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(url).context("invalid media URL")?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        bail!("media URL must use HTTP or HTTPS and include a host");
    }
    Ok(())
}

fn validate_filename(filename: &str) -> Result<()> {
    let path = PathBuf::from(filename);
    if filename.is_empty()
        || path.is_absolute()
        || path.components().count() != 1
        || matches!(filename, "." | "..")
    {
        bail!("storage filename must be a single non-empty file name");
    }
    Ok(())
}

fn redact_state(mut state: VirtualMediaState) -> VirtualMediaState {
    if let Some(url) = state.url.take() {
        state.url = Some(if is_controller_owned_url(&url) {
            REDACTED_LOCAL_MEDIA_URL.to_owned()
        } else if let Ok(mut parsed) = reqwest::Url::parse(&url) {
            let _ = parsed.set_username("");
            let _ = parsed.set_password(None);
            parsed.set_path("/<redacted>");
            parsed.set_query(None);
            parsed.set_fragment(None);
            parsed.to_string()
        } else {
            "<redacted-media-url>".to_owned()
        });
    }
    state
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, VecDeque};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

    use serde_json::{Value, json};
    use webrtc::api::APIBuilder;
    use webrtc::api::media_engine::MediaEngine;
    use webrtc::data_channel::RTCDataChannel;
    use webrtc::peer_connection::configuration::RTCConfiguration;

    #[derive(Clone, Debug)]
    struct ObservedTransfer {
        upload_id: String,
        bytes: Vec<u8>,
    }

    struct MockStorage {
        bytes_free: AtomicU64,
        filename: String,
        size: u64,
        offsets: tokio::sync::Mutex<VecDeque<u64>>,
        expected: tokio::sync::Mutex<HashMap<String, usize>>,
        transfers: tokio::sync::Mutex<Vec<ObservedTransfer>>,
        files: tokio::sync::Mutex<Vec<StorageFile>>,
        start_calls: AtomicUsize,
        delete_calls: AtomicUsize,
    }

    impl MockStorage {
        fn new(bytes_free: u64, filename: &str, size: u64, offsets: &[u64]) -> Arc<Self> {
            Arc::new(Self {
                bytes_free: AtomicU64::new(bytes_free),
                filename: filename.to_owned(),
                size,
                offsets: tokio::sync::Mutex::new(offsets.iter().copied().collect()),
                expected: tokio::sync::Mutex::new(HashMap::new()),
                transfers: tokio::sync::Mutex::new(Vec::new()),
                files: tokio::sync::Mutex::new(Vec::new()),
                start_calls: AtomicUsize::new(0),
                delete_calls: AtomicUsize::new(0),
            })
        }

        async fn respond(&self, request: Value) -> Value {
            match request["method"].as_str().expect("RPC method") {
                "getStorageSpace" => json!({
                    "bytesUsed": 0,
                    "bytesFree": self.bytes_free.load(Ordering::SeqCst),
                }),
                "startStorageFileUpload" => {
                    let call = self.start_calls.fetch_add(1, Ordering::SeqCst);
                    let offset = self
                        .offsets
                        .lock()
                        .await
                        .pop_front()
                        .expect("scripted startStorageFileUpload response");
                    let upload_id = format!("upload-{call}");
                    self.expected
                        .lock()
                        .await
                        .insert(upload_id.clone(), self.size.saturating_sub(offset) as usize);
                    json!({
                        "alreadyUploadedBytes": offset,
                        "dataChannel": upload_id,
                    })
                }
                "listStorageFiles" => json!({
                    "files": self.files.lock().await.clone(),
                }),
                "deleteStorageFile" => {
                    self.delete_calls.fetch_add(1, Ordering::SeqCst);
                    json!({})
                }
                method => panic!("unexpected mock storage RPC: {method}"),
            }
        }

        async fn observe_upload_channel(self: Arc<Self>, channel: Arc<RTCDataChannel>) {
            let upload_id = channel.label().to_owned();
            self.transfers.lock().await.push(ObservedTransfer {
                upload_id: upload_id.clone(),
                bytes: Vec::new(),
            });
            let storage = Arc::clone(&self);
            let message_channel = Arc::clone(&channel);
            channel.on_message(Box::new(move |message| {
                let storage = Arc::clone(&storage);
                let channel = Arc::clone(&message_channel);
                let upload_id = upload_id.clone();
                Box::pin(async move {
                    let expected = *storage
                        .expected
                        .lock()
                        .await
                        .get(&upload_id)
                        .expect("unexpected upload data channel");
                    let complete = {
                        let mut transfers = storage.transfers.lock().await;
                        let transfer = transfers
                            .iter_mut()
                            .find(|transfer| transfer.upload_id == upload_id)
                            .expect("recorded upload data channel");
                        transfer.bytes.extend_from_slice(&message.data);
                        assert!(
                            transfer.bytes.len() <= expected,
                            "upload channel sent more bytes than the resume range"
                        );
                        transfer.bytes.len() == expected
                    };
                    if complete {
                        let mut files = storage.files.lock().await;
                        if !files.iter().any(|file| file.filename == storage.filename) {
                            files.push(StorageFile {
                                filename: storage.filename.clone(),
                                size: storage.size,
                                created_at: "now".to_owned(),
                            });
                        }
                        drop(files);
                        channel
                            .close()
                            .await
                            .expect("close completed upload channel");
                    }
                })
            }));
        }

        async fn transfers(&self) -> Vec<ObservedTransfer> {
            self.transfers.lock().await.clone()
        }
    }

    struct UploadHarness {
        manager: VirtualMediaManager,
        storage: Arc<MockStorage>,
        answer_peer: Arc<RTCPeerConnection>,
    }

    impl UploadHarness {
        async fn close(self) {
            self.answer_peer.close().await.expect("close mock peer");
        }
    }

    async fn upload_harness(
        bytes_free: u64,
        filename: &str,
        size: u64,
        offsets: &[u64],
    ) -> UploadHarness {
        let storage = MockStorage::new(bytes_free, filename, size, offsets);
        let mut media_engine = MediaEngine::default();
        media_engine
            .register_default_codecs()
            .expect("default codecs");
        let api = APIBuilder::new().with_media_engine(media_engine).build();
        let offer_peer = Arc::new(
            api.new_peer_connection(RTCConfiguration::default())
                .await
                .expect("offer peer"),
        );
        let answer_peer = Arc::new(
            api.new_peer_connection(RTCConfiguration::default())
                .await
                .expect("answer peer"),
        );
        let rpc_channel = offer_peer
            .create_data_channel("rpc", None)
            .await
            .expect("RPC data channel");
        let hid_reliable = offer_peer
            .create_data_channel("hid-reliable", None)
            .await
            .expect("reliable HID data channel");
        let hid_ordered = offer_peer
            .create_data_channel("hid-ordered", None)
            .await
            .expect("ordered HID data channel");
        let hid_unordered = offer_peer
            .create_data_channel("hid-unordered", None)
            .await
            .expect("unordered HID data channel");
        let storage_for_channels = Arc::clone(&storage);
        answer_peer.on_data_channel(Box::new(move |channel| {
            let storage = Arc::clone(&storage_for_channels);
            Box::pin(async move {
                if channel.label() == "rpc" {
                    let response_channel = Arc::clone(&channel);
                    channel.on_message(Box::new(move |message| {
                        let storage = Arc::clone(&storage);
                        let response_channel = Arc::clone(&response_channel);
                        Box::pin(async move {
                            let request: Value =
                                serde_json::from_slice(&message.data).expect("parse RPC request");
                            let id = request["id"].clone();
                            let response = if request["method"] == "checkMountUrl" {
                                json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "error": {
                                        "code": -32601,
                                        "message": "not implemented",
                                    },
                                })
                            } else {
                                let result = storage.respond(request).await;
                                json!({
                                    "jsonrpc": "2.0",
                                    "id": id,
                                    "result": result,
                                })
                            };
                            response_channel
                                .send_text(
                                    serde_json::to_string(&response)
                                        .expect("serialize RPC response"),
                                )
                                .await
                                .expect("send RPC response");
                        })
                    }));
                } else if channel.label().starts_with("upload-") {
                    storage.observe_upload_channel(channel).await;
                }
            })
        }));

        connect_loopback(&offer_peer, &answer_peer).await;
        let rpc = RpcClient::new(rpc_channel);
        rpc.wait_ready(Duration::from_secs(5))
            .await
            .expect("RPC channel open");
        let hid = HidClient::new(hid_reliable, hid_ordered, hid_unordered);
        let (events, _) = broadcast::channel(4);
        UploadHarness {
            manager: VirtualMediaManager::new(
                rpc,
                AuthenticatedClient::test_client(),
                hid,
                offer_peer,
                false,
                events,
            ),
            storage,
            answer_peer,
        }
    }

    async fn connect_loopback(
        offer_peer: &Arc<RTCPeerConnection>,
        answer_peer: &Arc<RTCPeerConnection>,
    ) {
        let offer = offer_peer.create_offer(None).await.expect("offer");
        let mut offer_gathering = offer_peer.gathering_complete_promise().await;
        offer_peer
            .set_local_description(offer)
            .await
            .expect("set offer");
        offer_gathering.recv().await;
        answer_peer
            .set_remote_description(
                offer_peer
                    .local_description()
                    .await
                    .expect("gathered offer"),
            )
            .await
            .expect("apply offer");
        let answer = answer_peer.create_answer(None).await.expect("answer");
        let mut answer_gathering = answer_peer.gathering_complete_promise().await;
        answer_peer
            .set_local_description(answer)
            .await
            .expect("set answer");
        answer_gathering.recv().await;
        offer_peer
            .set_remote_description(
                answer_peer
                    .local_description()
                    .await
                    .expect("gathered answer"),
            )
            .await
            .expect("apply answer");
    }

    fn upload_fixture(size: usize) -> (tempfile::NamedTempFile, Vec<u8>) {
        let bytes = (37_u8..=255).cycle().take(size).collect::<Vec<_>>();
        let file = tempfile::NamedTempFile::new().expect("temporary upload file");
        std::fs::write(file.path(), &bytes).expect("write upload fixture");
        (file, bytes)
    }

    async fn establish_upload_origin(harness: &mut UploadHarness, image: &Path, filename: &str) {
        harness
            .manager
            .upload(
                image,
                filename,
                Approval { approved: true },
                CancellationToken::new(),
            )
            .await
            .expect("fresh upload establishes its origin");
    }

    #[test]
    fn upload_origins_bind_resume_to_the_same_source() {
        let mut origins = UploadOrigins::default();
        let image_a = UploadOrigin {
            size: 10_000,
            sha256: [0xAA; 32],
        };
        // Fresh upload records its origin.
        origins
            .verify("disk.iso", &image_a, 0)
            .expect("fresh upload records its origin");
        // Resume with the same source is admitted.
        origins
            .verify("disk.iso", &image_a, 4096)
            .expect("same-origin resume is admitted");
        // Same name and size but different content is rejected (no splice).
        let image_b = UploadOrigin {
            size: 10_000,
            sha256: [0xBB; 32],
        };
        let error = origins
            .verify("disk.iso", &image_b, 4096)
            .expect_err("different source must not resume");
        assert_eq!(crate::error::error_code(&error), codes::OPERATION_FAILED);
        assert!(error.to_string().contains("different local image"));
        // A partial from an unknown origin is rejected as well.
        let error = origins
            .verify("other.iso", &image_a, 2048)
            .expect_err("unknown-origin partial must not resume");
        assert!(error.to_string().contains("no recorded origin"));
        // Identity check only applies to actual resumes.
        origins
            .verify("other.iso", &image_a, 0)
            .expect("fresh upload for another file records identity");
    }

    #[tokio::test]
    async fn upload_origin_hashes_bytes_beyond_a_shared_prefix() {
        let directory = tempfile::tempdir().expect("temporary directory");
        let first = directory.path().join("first.iso");
        let second = directory.path().join("second.iso");
        let mut first_bytes = vec![0_u8; 8192];
        let mut second_bytes = first_bytes.clone();
        first_bytes[7000] = 1;
        second_bytes[7000] = 2;
        tokio::fs::write(&first, first_bytes)
            .await
            .expect("write first image");
        tokio::fs::write(&second, second_bytes)
            .await
            .expect("write second image");
        let cancellation = CancellationToken::new();

        let first_origin = read_upload_origin(&first, &cancellation)
            .await
            .expect("hash first image");
        let second_origin = read_upload_origin(&second, &cancellation)
            .await
            .expect("hash second image");
        assert_eq!(first_origin.size, second_origin.size);
        assert_ne!(first_origin.sha256, second_origin.sha256);
    }

    #[tokio::test]
    async fn failed_mount_rollback_keeps_the_range_server_alive() {
        let mut harness = upload_harness(1, "unused.iso", 1, &[0]).await;
        let directory = tempfile::tempdir().expect("temporary directory");
        let image = directory.path().join("local.iso");
        tokio::fs::write(&image, [0xAA])
            .await
            .expect("write local image");
        let server = RangeServer::start(&image, harness.manager.auth.base_url())
            .await
            .expect("start range server");
        harness.manager.range_server = Some(server);

        let error = harness
            .manager
            .complete_failed_local_mount(
                anyhow::anyhow!("mount verification failed"),
                Err(anyhow::anyhow!("unmount failed")),
            )
            .await
            .expect_err("rollback failure must be reported");
        assert!(error.to_string().contains("rollback failed"));
        assert!(harness.manager.stale_controller_mount);
        assert!(
            harness
                .manager
                .range_server
                .as_ref()
                .is_some_and(RangeServer::is_healthy),
            "the mounted URL must remain serviceable after rollback failure"
        );

        harness
            .manager
            .range_server
            .take()
            .expect("retained range server")
            .shutdown()
            .await
            .expect("stop range server");
        harness.close().await;
    }
    #[test]
    fn check_mount_url_unimplemented_is_classified() {
        let stub = anyhow::anyhow!("remote RPC error -32601: not implemented");
        assert!(is_check_mount_url_unimplemented(&stub));
        let method_missing = anyhow::anyhow!("remote RPC error -32601: method not found");
        assert!(is_check_mount_url_unimplemented(&method_missing));
        let other = anyhow::anyhow!("remote RPC error -32000: storage full");
        assert!(!is_check_mount_url_unimplemented(&other));
    }

    #[tokio::test]
    async fn runtime_rpc_fallback_disables_check_mount_url_capability() {
        let mut harness = upload_harness(1, "unused.iso", 1, &[0]).await;
        harness.manager.supports_check_mount_url = true;
        assert!(harness.manager.supports_check_mount_url());

        let error = harness
            .manager
            .check_url("http://example.invalid/image.iso")
            .await
            .expect_err("stub RPC must trigger capability fallback");
        assert!(error.to_string().contains("unsupported"));
        assert!(!harness.manager.supports_check_mount_url());
        harness.close().await;
    }

    #[test]
    fn approval_is_required_for_mutation() {
        assert!(Approval { approved: false }.require("mount media").is_err());
        assert!(Approval { approved: true }.require("mount media").is_ok());
    }

    #[test]
    fn filenames_are_constrained_to_one_component() {
        assert!(validate_filename("image.iso").is_ok());
        for invalid in [
            "",
            ".",
            "..",
            "../image.iso",
            "/tmp/image.iso",
            "dir/image.iso",
        ] {
            assert!(validate_filename(invalid).is_err(), "accepted {invalid:?}");
        }
    }

    #[test]
    fn controller_tokens_are_redacted_from_state() {
        let state = VirtualMediaState {
            source: "HTTP".to_owned(),
            mode: VirtualMediaMode::Cdrom,
            filename: None,
            url: Some("http://127.0.0.1/jetkvm-controller/media/secret".to_owned()),
            size: 10,
        };
        assert_eq!(
            redact_state(state).url.as_deref(),
            Some(REDACTED_LOCAL_MEDIA_URL)
        );
    }

    #[test]
    fn remote_media_credentials_and_tokens_are_redacted_from_state() {
        let state = VirtualMediaState {
            source: "HTTP".to_owned(),
            mode: VirtualMediaMode::Cdrom,
            filename: None,
            url: Some(
                "https://user:password@example.com/private/token.iso?signature=secret".to_owned(),
            ),
            size: 10,
        };
        let redacted = redact_state(state).url.unwrap();
        assert!(redacted.starts_with("https://example.com/"));
        for secret in [
            "user",
            "password",
            "private",
            "token.iso",
            "signature",
            "secret",
        ] {
            assert!(!redacted.contains(secret), "leaked {secret}");
        }
    }

    #[tokio::test]
    async fn upload_resume_admits_remaining_bytes_that_fit_and_sends_no_prefix() {
        let filename = "resume.iso";
        let (file, bytes) = upload_fixture(24 * 1024);
        let offset = 8 * 1024_u64;
        let mut harness = upload_harness(
            bytes.len() as u64,
            filename,
            bytes.len() as u64,
            &[0, 0, offset, offset],
        )
        .await;
        establish_upload_origin(&mut harness, file.path(), filename).await;
        harness
            .storage
            .bytes_free
            .store((bytes.len() as u64) - offset, Ordering::SeqCst);

        harness
            .manager
            .upload(
                file.path(),
                filename,
                Approval { approved: true },
                CancellationToken::new(),
            )
            .await
            .expect("resume admitted when only the remaining range fits");

        let transfers = harness.storage.transfers().await;
        assert_eq!(
            transfers.len(),
            2,
            "each completed upload uses one fallback channel"
        );
        assert_eq!(transfers[1].bytes, bytes[offset as usize..]);
        assert_eq!(harness.storage.start_calls.load(Ordering::SeqCst), 4);
        harness.close().await;
    }

    #[tokio::test]
    async fn upload_rejects_when_remaining_bytes_exceed_free_space_without_transfer_or_delete() {
        let filename = "too-large.iso";
        let (file, bytes) = upload_fixture(24 * 1024);
        let offset = 8 * 1024_u64;
        let mut harness = upload_harness(
            bytes.len() as u64,
            filename,
            bytes.len() as u64,
            &[0, 0, offset],
        )
        .await;
        establish_upload_origin(&mut harness, file.path(), filename).await;
        let transfers_before = harness.storage.transfers().await;
        let remaining = (bytes.len() as u64) - offset;
        harness
            .storage
            .bytes_free
            .store(remaining - 1, Ordering::SeqCst);
        let error = harness
            .manager
            .upload(
                file.path(),
                filename,
                Approval { approved: true },
                CancellationToken::new(),
            )
            .await
            .expect_err("remaining range must be rejected before transfer");

        assert!(error.to_string().contains("bytes free"));
        assert!(error.to_string().contains("needs"));
        assert_eq!(
            harness.storage.transfers().await.len(),
            transfers_before.len()
        );
        assert_eq!(harness.storage.delete_calls.load(Ordering::SeqCst), 0);
        harness.close().await;
    }

    #[tokio::test]
    async fn upload_at_completed_resume_offset_verifies_without_another_transfer() {
        let filename = "complete.iso";
        let (file, bytes) = upload_fixture(24 * 1024);
        let mut harness = upload_harness(
            bytes.len() as u64,
            filename,
            bytes.len() as u64,
            &[0, 0, bytes.len() as u64],
        )
        .await;
        establish_upload_origin(&mut harness, file.path(), filename).await;
        let transfers_before = harness.storage.transfers().await;

        let stored = harness
            .manager
            .upload(
                file.path(),
                filename,
                Approval { approved: true },
                CancellationToken::new(),
            )
            .await
            .expect("completed resume offset verifies stored file");

        assert_eq!(stored.filename, filename);
        assert_eq!(stored.size, bytes.len() as u64);
        assert_eq!(
            harness.storage.transfers().await.len(),
            transfers_before.len()
        );
        assert_eq!(harness.storage.start_calls.load(Ordering::SeqCst), 3);
        harness.close().await;
    }

    #[tokio::test]
    async fn upload_rejects_resume_offset_past_end_without_transfer() {
        let filename = "invalid-offset.iso";
        let (file, bytes) = upload_fixture(24 * 1024);
        let mut harness = upload_harness(
            bytes.len() as u64,
            filename,
            bytes.len() as u64,
            &[(bytes.len() as u64) + 1],
        )
        .await;

        let error = harness
            .manager
            .upload(
                file.path(),
                filename,
                Approval { approved: true },
                CancellationToken::new(),
            )
            .await
            .expect_err("past-end resume offset must fail");

        assert!(error.to_string().contains("invalid upload resume offset"));
        assert!(harness.storage.transfers().await.is_empty());
        assert_eq!(harness.storage.start_calls.load(Ordering::SeqCst), 1);
        harness.close().await;
    }

    #[tokio::test]
    async fn upload_fallback_re_reads_the_advanced_resume_offset() {
        let filename = "fallback.iso";
        let (file, bytes) = upload_fixture(24 * 1024);
        let advanced = 12 * 1024_u64;
        let mut harness = upload_harness(
            bytes.len() as u64,
            filename,
            bytes.len() as u64,
            &[0, advanced],
        )
        .await;

        harness
            .manager
            .upload(
                file.path(),
                filename,
                Approval { approved: true },
                CancellationToken::new(),
            )
            .await
            .expect("HTTP failure falls back to the advanced resume offset");

        let transfers = harness.storage.transfers().await;
        assert_eq!(transfers.len(), 1);
        assert_eq!(transfers[0].upload_id, "upload-1");
        assert_eq!(transfers[0].bytes, bytes[advanced as usize..]);
        assert_eq!(harness.storage.start_calls.load(Ordering::SeqCst), 2);
        harness.close().await;
    }

    #[tokio::test]
    async fn upload_pre_cancelled_token_never_starts_remote_upload() {
        let filename = "cancelled.iso";
        let (file, bytes) = upload_fixture(24 * 1024);
        let mut harness =
            upload_harness(bytes.len() as u64, filename, bytes.len() as u64, &[0]).await;
        let cancellation = CancellationToken::new();
        cancellation.cancel();

        let error = harness
            .manager
            .upload(
                file.path(),
                filename,
                Approval { approved: true },
                cancellation,
            )
            .await
            .expect_err("pre-cancelled upload must fail");

        assert_eq!(crate::error::error_code(&error), codes::CANCELLED);
        assert_eq!(harness.storage.start_calls.load(Ordering::SeqCst), 0);
        assert!(harness.storage.transfers().await.is_empty());
        harness.close().await;
    }

    #[tokio::test]
    async fn webrtc_upload_channel_resumes_and_streams_exact_remaining_bytes() {
        let mut media_engine = MediaEngine::default();
        media_engine
            .register_default_codecs()
            .expect("default codecs");
        let api = APIBuilder::new().with_media_engine(media_engine).build();
        let offer_peer = api
            .new_peer_connection(RTCConfiguration::default())
            .await
            .expect("offer peer");
        let answer_peer = api
            .new_peer_connection(RTCConfiguration::default())
            .await
            .expect("answer peer");
        offer_peer
            .create_data_channel("rpc", None)
            .await
            .expect("initial data channel");

        let expected = Arc::new((37_u8..=255).cycle().take(96 * 1024).collect::<Vec<_>>());
        let resume_offset = 4096_u64;
        let received = Arc::new(tokio::sync::Mutex::new(Vec::new()));
        let received_done = Arc::new(Notify::new());
        let channel_seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let channel_seen_for_callback = Arc::clone(&channel_seen);
        let received_done_for_channel = Arc::clone(&received_done);
        let received_for_channel = Arc::clone(&received);
        let expected_remaining = expected.len() - resume_offset as usize;
        answer_peer.on_data_channel(Box::new(move |channel| {
            let received = Arc::clone(&received_for_channel);
            let received_done = Arc::clone(&received_done_for_channel);
            let channel_seen = Arc::clone(&channel_seen_for_callback);
            Box::pin(async move {
                if channel.label() != "upload-test" {
                    return;
                }
                channel_seen.store(true, std::sync::atomic::Ordering::SeqCst);
                let channel_for_message = Arc::clone(&channel);
                channel.on_message(Box::new(move |message| {
                    let received = Arc::clone(&received);
                    let channel = Arc::clone(&channel_for_message);
                    let received_done = Arc::clone(&received_done);
                    Box::pin(async move {
                        let mut received = received.lock().await;
                        received.extend_from_slice(&message.data);
                        if received.len() == expected_remaining {
                            drop(received);
                            channel.close().await.expect("close upload channel");
                            received_done.notify_one();
                        }
                    })
                }));
            })
        }));

        let offer = offer_peer.create_offer(None).await.expect("offer");
        let mut offer_gathering = offer_peer.gathering_complete_promise().await;
        offer_peer
            .set_local_description(offer)
            .await
            .expect("set offer");
        offer_gathering.recv().await;
        answer_peer
            .set_remote_description(
                offer_peer
                    .local_description()
                    .await
                    .expect("gathered offer"),
            )
            .await
            .expect("apply offer");
        let answer = answer_peer.create_answer(None).await.expect("answer");
        let mut answer_gathering = answer_peer.gathering_complete_promise().await;
        answer_peer
            .set_local_description(answer)
            .await
            .expect("set answer");
        answer_gathering.recv().await;
        offer_peer
            .set_remote_description(
                answer_peer
                    .local_description()
                    .await
                    .expect("gathered answer"),
            )
            .await
            .expect("apply answer");

        let file = tempfile::NamedTempFile::new().expect("temporary upload file");
        std::fs::write(file.path(), expected.as_slice()).expect("write upload fixture");
        let (events, _) = broadcast::channel(4);
        let cancellation = CancellationToken::new();
        upload_over_data_channel(
            &Arc::new(offer_peer),
            "upload-test",
            UploadTransfer {
                path: file.path(),
                filename: "fixture.iso",
                total: expected.len() as u64,
                uploaded: resume_offset,
                events: &events,
                cancellation: &cancellation,
            },
        )
        .await
        .expect("WebRTC upload");
        assert!(
            channel_seen.load(std::sync::atomic::Ordering::SeqCst),
            "remote peer did not receive dynamic upload channel"
        );
        if tokio::time::timeout(Duration::from_secs(5), received_done.notified())
            .await
            .is_err()
        {
            panic!(
                "remote peer received {} of {expected_remaining} upload bytes",
                received.lock().await.len()
            );
        }

        assert_eq!(
            received.lock().await.as_slice(),
            &expected[resume_offset as usize..]
        );
        answer_peer.close().await.expect("close answer peer");
    }
}
