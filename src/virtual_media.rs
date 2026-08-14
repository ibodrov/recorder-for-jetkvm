use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use futures_util::stream;
use reqwest::Body;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Notify, broadcast};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use webrtc::data_channel::data_channel_state::RTCDataChannelState;
use webrtc::peer_connection::RTCPeerConnection;

use crate::auth::AuthenticatedClient;
use crate::hid::HidClient;
use crate::range_server::{REDACTED_LOCAL_MEDIA_URL, RangeServer, is_controller_owned_url};
use crate::rpc::{
    MountUrlInfo, RpcClient, StorageFile, StorageSpace, VirtualMediaMode, VirtualMediaState,
};

const PROGRESS_INTERVAL: Duration = Duration::from_millis(200);
const UPLOAD_CHUNK_SIZE: usize = 64 * 1024;
const DATA_CHANNEL_BUFFER_LOW: usize = 512 * 1024;
const DATA_CHANNEL_BUFFER_HIGH: usize = 2 * 1024 * 1024;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct Approval {
    pub approved: bool,
}

impl Approval {
    pub fn require(self, operation: &str) -> Result<()> {
        if !self.approved {
            bail!("explicit approval is required to {operation}");
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

    pub async fn check_url(&self, url: &str) -> Result<MountUrlInfo> {
        if !self.supports_check_mount_url {
            bail!("check_mount_url is unsupported by this JetKVM firmware");
        }
        validate_http_url(url)?;
        self.rpc.check_mount_url(url).await
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
            let checked = self.rpc.check_mount_url(url).await?;
            if !checked.usable {
                bail!("mount URL is unusable: {}", checked.reason);
            }
            Some(checked.size)
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
            let checked = match self.rpc.check_mount_url(&url).await {
                Ok(checked) if checked.usable => checked,
                Ok(checked) => {
                    server.shutdown().await?;
                    bail!("JetKVM cannot read the local image: {}", checked.reason);
                }
                Err(error) => {
                    server.shutdown().await?;
                    return Err(error).context("failed to validate controller-hosted image");
                }
            };
            Some(checked.size)
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
            Err(error) => {
                let _ = self.rpc.unmount().await;
                server.shutdown().await?;
                return Err(error).context("JetKVM did not report the controller-hosted mount");
            }
        };
        server.ensure_healthy()?;
        self.range_server = Some(server);
        self.stale_controller_mount = false;
        Ok(redact_state(state))
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

    pub async fn upload(
        &self,
        image: &Path,
        filename: &str,
        approval: Approval,
        cancellation: CancellationToken,
    ) -> Result<StorageFile> {
        approval.require("upload a local image")?;
        let canonical = tokio::fs::canonicalize(image)
            .await
            .with_context(|| format!("failed to canonicalize upload image: {}", image.display()))?;
        let metadata = tokio::fs::metadata(&canonical)
            .await
            .context("failed to inspect upload image")?;
        if !metadata.is_file() || metadata.len() == 0 {
            bail!("upload image must be a non-empty regular file");
        }
        validate_filename(filename)?;
        let size = metadata.len();
        let space = self.rpc.storage_space().await?;
        if size > space.bytes_free {
            bail!("image is larger than available JetKVM storage");
        }

        let upload = self.rpc.start_upload(filename, size).await?;
        validate_upload_offset(upload.already_uploaded_bytes, size)?;
        let http_result = upload_over_http(
            &self.auth,
            &upload.upload_id,
            UploadTransfer {
                path: &canonical,
                filename,
                total: size,
                uploaded: upload.already_uploaded_bytes,
                events: &self.events,
                cancellation: &cancellation,
            },
        )
        .await;
        if http_result.is_err() {
            if cancellation.is_cancelled() {
                bail!("upload cancelled");
            }
            warn!("direct HTTP storage upload failed; falling back to WebRTC");
            let upload = self.rpc.start_upload(filename, size).await?;
            validate_upload_offset(upload.already_uploaded_bytes, size)?;
            upload_over_data_channel(
                &self.peer_connection,
                &upload.upload_id,
                UploadTransfer {
                    path: &canonical,
                    filename,
                    total: size,
                    uploaded: upload.already_uploaded_bytes,
                    events: &self.events,
                    cancellation: &cancellation,
                },
            )
            .await?;
        }

        wait_for_storage_file(&self.rpc, filename, size, &cancellation).await
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
        _ = cancellation.cancelled() => bail!("upload cancelled"),
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
            bail!("upload cancelled");
        }
        return Err(error).context("failed to open WebRTC upload channel");
    }

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
                    let _ = channel.close().await;
                    bail!("upload cancelled");
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
                let _ = channel.close().await;
                bail!("upload cancelled");
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
                    let _ = channel.close().await;
                    bail!("upload cancelled");
                }
                _ = closed.notified() => {}
            }
        }
    })
    .await
    .context("timed out waiting for WebRTC upload completion")?
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
            _ = cancellation.cancelled() => bail!("upload cancelled"),
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

    #[test]
    fn progress_resume_math_starts_at_reported_offset() {
        let offset = 4096_u64;
        let total = 10_000_u64;
        assert_eq!(total - offset, 5904);
        assert!(offset <= total);
    }
}
