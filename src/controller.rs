use std::fmt;
use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};

use crate::auth::{self, AuthenticatedClient};
use crate::h264::NalUnit;
use crate::hid::{AbsoluteMouseEvent, HidStatus, KeyEvent, RelativeMouseEvent};
use crate::keyboard;
use crate::rpc::{MountUrlInfo, StorageFile, StorageSpace, VirtualMediaMode, VirtualMediaState};
use crate::session::SessionConnection;
use crate::signaling::SignalingMode;
use crate::video::{LatestFrameCache, SnapshotFile};
use crate::virtual_media::{Approval, MediaEvent, VirtualMediaManager};

const COMMAND_BUFFER: usize = 64;
const EVENT_BUFFER: usize = 128;
const NAL_BUFFER: usize = 512;
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct ConnectionConfig {
    pub host: String,
    pub password: String,
    pub no_tls_verify: bool,
    pub pli_interval: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
}

impl fmt::Debug for ConnectionConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectionConfig")
            .field("host", &self.host)
            .field("password", &"<redacted>")
            .field("no_tls_verify", &self.no_tls_verify)
            .field("pli_interval", &self.pli_interval)
            .field("reconnect_min", &self.reconnect_min)
            .field("reconnect_max", &self.reconnect_max)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ConnectionState {
    Connecting,
    Connected,
    Reconnecting,
    Disconnected,
    TakenOver,
    ShuttingDown,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FrameStatus {
    pub age_ms: u64,
    pub width: u32,
    pub height: u32,
    pub generation: u64,
    pub captured_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControllerStatus {
    pub connected: bool,
    pub state: ConnectionState,
    pub generation: u64,
    pub device_version: Option<String>,
    pub signaling: Option<String>,
    pub frame: Option<FrameStatus>,
    pub hid: Option<HidStatus>,
    pub stale_controller_mount: bool,
}

impl ControllerStatus {
    fn disconnected(state: ConnectionState, generation: u64) -> Self {
        Self {
            connected: false,
            state,
            generation,
            device_version: None,
            signaling: None,
            frame: None,
            hid: None,
            stale_controller_mount: false,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    pub path: PathBuf,
    pub mime_type: String,
    pub width: u32,
    pub height: u32,
    pub captured_at: String,
    pub frame_age_ms: u64,
    pub generation: u64,
}

impl From<SnapshotFile> for Snapshot {
    fn from(value: SnapshotFile) -> Self {
        Self {
            path: value.path,
            mime_type: value.mime_type.to_owned(),
            width: value.width,
            height: value.height,
            captured_at: format_system_time(value.captured_at),
            frame_age_ms: duration_millis(value.frame_age),
            generation: value.generation,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
pub enum ControllerEvent {
    ConnectionState {
        state: ConnectionState,
        generation: u64,
    },
    TakenOver {
        generation: u64,
    },
    UploadProgress {
        filename: String,
        uploaded_bytes: u64,
        total_bytes: u64,
    },
    StaleControllerMount {
        redacted_url: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TypeTextRequest {
    pub text: String,
    #[serde(default)]
    pub is_paste: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ScrollEvent {
    pub wheel_x: i8,
    pub wheel_y: i8,
}

#[derive(Clone)]
pub struct JetKvmController {
    command_tx: mpsc::Sender<Command>,
    event_tx: broadcast::Sender<ControllerEvent>,
    nal_tx: broadcast::Sender<NalUnit>,
}

enum Command {
    Status(oneshot::Sender<Result<ControllerStatus>>),
    Connect(oneshot::Sender<Result<ControllerStatus>>),
    Disconnect(oneshot::Sender<Result<()>>),
    Snapshot(PathBuf, oneshot::Sender<Result<Snapshot>>),
    Key(KeyEvent, oneshot::Sender<Result<()>>),
    TypeText(TypeTextRequest, oneshot::Sender<Result<()>>),
    AbsoluteMouse(AbsoluteMouseEvent, oneshot::Sender<Result<()>>),
    RelativeMouse(RelativeMouseEvent, oneshot::Sender<Result<()>>),
    Scroll(ScrollEvent, oneshot::Sender<Result<()>>),
    MediaState(oneshot::Sender<Result<Option<VirtualMediaState>>>),
    CheckMountUrl(String, oneshot::Sender<Result<MountUrlInfo>>),
    MountUrl {
        url: String,
        mode: VirtualMediaMode,
        approval: Approval,
        response: oneshot::Sender<Result<VirtualMediaState>>,
    },
    MountLocal {
        path: PathBuf,
        mode: VirtualMediaMode,
        approval: Approval,
        response: oneshot::Sender<Result<VirtualMediaState>>,
    },
    Unmount(Approval, oneshot::Sender<Result<()>>),
    StorageSpace(oneshot::Sender<Result<StorageSpace>>),
    StorageFiles(oneshot::Sender<Result<Vec<StorageFile>>>),
    Upload {
        path: PathBuf,
        filename: String,
        approval: Approval,
        cancellation: CancellationToken,
        response: oneshot::Sender<Result<StorageFile>>,
    },
    MountStorage {
        filename: String,
        mode: VirtualMediaMode,
        approval: Approval,
        response: oneshot::Sender<Result<VirtualMediaState>>,
    },
    DeleteStorage {
        filename: String,
        approval: Approval,
        response: oneshot::Sender<Result<()>>,
    },
    Shutdown(oneshot::Sender<Result<()>>),
}

enum ActorDirective {
    Continue,
    Disconnect(Option<oneshot::Sender<Result<()>>>),
    Shutdown(oneshot::Sender<Result<()>>),
}

impl JetKvmController {
    pub async fn connect(config: ConnectionConfig) -> Result<Self> {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_BUFFER);
        let (event_tx, _) = broadcast::channel(EVENT_BUFFER);
        let (nal_tx, _) = broadcast::channel(NAL_BUFFER);
        let (media_event_tx, mut media_event_rx) = broadcast::channel(EVENT_BUFFER);
        let (ready_tx, ready_rx) = oneshot::channel();

        let actor_event_tx = event_tx.clone();
        let actor_nal_tx = nal_tx.clone();
        tokio::spawn(run_actor(
            config,
            command_rx,
            actor_event_tx.clone(),
            actor_nal_tx,
            media_event_tx,
            ready_tx,
        ));
        tokio::spawn(async move {
            loop {
                match media_event_rx.recv().await {
                    Ok(MediaEvent::UploadProgress {
                        filename,
                        uploaded_bytes,
                        total_bytes,
                    }) => {
                        let _ = actor_event_tx.send(ControllerEvent::UploadProgress {
                            filename,
                            uploaded_bytes,
                            total_bytes,
                        });
                    }
                    Ok(MediaEvent::StaleControllerMount { redacted_url }) => {
                        let _ = actor_event_tx
                            .send(ControllerEvent::StaleControllerMount { redacted_url });
                    }
                    Err(broadcast::error::RecvError::Lagged(_)) => continue,
                    Err(broadcast::error::RecvError::Closed) => return,
                }
            }
        });

        ready_rx
            .await
            .context("controller actor stopped during startup")??;
        Ok(Self {
            command_tx,
            event_tx,
            nal_tx,
        })
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<ControllerEvent> {
        self.event_tx.subscribe()
    }

    pub fn subscribe_nals(&self) -> broadcast::Receiver<NalUnit> {
        self.nal_tx.subscribe()
    }

    pub async fn status(&self) -> Result<ControllerStatus> {
        request(&self.command_tx, Command::Status).await
    }

    pub async fn reconnect(&self) -> Result<ControllerStatus> {
        request(&self.command_tx, Command::Connect).await
    }

    pub async fn disconnect(&self) -> Result<()> {
        request(&self.command_tx, Command::Disconnect).await
    }

    pub async fn snapshot(&self, path: PathBuf) -> Result<Snapshot> {
        request_with(&self.command_tx, |response| {
            Command::Snapshot(path, response)
        })
        .await
    }

    pub async fn key(&self, event: KeyEvent) -> Result<()> {
        request_with(&self.command_tx, |response| Command::Key(event, response)).await
    }

    pub async fn type_text(&self, request_value: TypeTextRequest) -> Result<()> {
        request_with(&self.command_tx, |response| {
            Command::TypeText(request_value, response)
        })
        .await
    }

    pub async fn absolute_mouse(&self, event: AbsoluteMouseEvent) -> Result<()> {
        request_with(&self.command_tx, |response| {
            Command::AbsoluteMouse(event, response)
        })
        .await
    }

    pub async fn relative_mouse(&self, event: RelativeMouseEvent) -> Result<()> {
        request_with(&self.command_tx, |response| {
            Command::RelativeMouse(event, response)
        })
        .await
    }

    pub async fn scroll(&self, event: ScrollEvent) -> Result<()> {
        request_with(&self.command_tx, |response| {
            Command::Scroll(event, response)
        })
        .await
    }

    pub async fn media_state(&self) -> Result<Option<VirtualMediaState>> {
        request(&self.command_tx, Command::MediaState).await
    }

    pub async fn check_mount_url(&self, url: String) -> Result<MountUrlInfo> {
        request_with(&self.command_tx, |response| {
            Command::CheckMountUrl(url, response)
        })
        .await
    }

    pub async fn mount_url(
        &self,
        url: String,
        mode: VirtualMediaMode,
        approval: Approval,
    ) -> Result<VirtualMediaState> {
        request_with(&self.command_tx, |response| Command::MountUrl {
            url,
            mode,
            approval,
            response,
        })
        .await
    }

    pub async fn mount_local(
        &self,
        path: PathBuf,
        mode: VirtualMediaMode,
        approval: Approval,
    ) -> Result<VirtualMediaState> {
        request_with(&self.command_tx, |response| Command::MountLocal {
            path,
            mode,
            approval,
            response,
        })
        .await
    }

    pub async fn unmount(&self, approval: Approval) -> Result<()> {
        request_with(&self.command_tx, |response| {
            Command::Unmount(approval, response)
        })
        .await
    }

    pub async fn storage_space(&self) -> Result<StorageSpace> {
        request(&self.command_tx, Command::StorageSpace).await
    }

    pub async fn storage_files(&self) -> Result<Vec<StorageFile>> {
        request(&self.command_tx, Command::StorageFiles).await
    }

    pub async fn upload(
        &self,
        path: PathBuf,
        filename: String,
        approval: Approval,
        cancellation: CancellationToken,
    ) -> Result<StorageFile> {
        request_with(&self.command_tx, |response| Command::Upload {
            path,
            filename,
            approval,
            cancellation,
            response,
        })
        .await
    }

    pub async fn mount_storage(
        &self,
        filename: String,
        mode: VirtualMediaMode,
        approval: Approval,
    ) -> Result<VirtualMediaState> {
        request_with(&self.command_tx, |response| Command::MountStorage {
            filename,
            mode,
            approval,
            response,
        })
        .await
    }

    pub async fn delete_storage(&self, filename: String, approval: Approval) -> Result<()> {
        request_with(&self.command_tx, |response| Command::DeleteStorage {
            filename,
            approval,
            response,
        })
        .await
    }

    pub async fn shutdown(&self) -> Result<()> {
        request(&self.command_tx, Command::Shutdown).await
    }
}

async fn request<T>(
    sender: &mpsc::Sender<Command>,
    constructor: fn(oneshot::Sender<Result<T>>) -> Command,
) -> Result<T> {
    request_with(sender, constructor).await
}

async fn request_with<T>(
    sender: &mpsc::Sender<Command>,
    constructor: impl FnOnce(oneshot::Sender<Result<T>>) -> Command,
) -> Result<T> {
    let (response_tx, response_rx) = oneshot::channel();
    sender
        .send(constructor(response_tx))
        .await
        .context("controller actor stopped")?;
    response_rx
        .await
        .context("controller response was dropped")?
}

async fn run_actor(
    config: ConnectionConfig,
    mut command_rx: mpsc::Receiver<Command>,
    event_tx: broadcast::Sender<ControllerEvent>,
    nal_tx: broadcast::Sender<NalUnit>,
    media_event_tx: broadcast::Sender<MediaEvent>,
    ready_tx: oneshot::Sender<Result<()>>,
) {
    let cache = LatestFrameCache::new();
    let mut generation = 1_u64;
    let initial = connect_once(&config, generation, nal_tx.clone()).await;
    let (mut session, auth, keyframe_tx) = match initial {
        Ok(connection) => connection,
        Err(error) => {
            let _ = ready_tx.send(Err(error));
            return;
        }
    };
    let _ = ready_tx.send(Ok(()));
    let mut media = VirtualMediaManager::new(
        session.rpc().clone(),
        auth,
        session.hid().clone(),
        media_event_tx.clone(),
    );
    let _ = media.refresh_state().await;
    let mut decoder = crate::video::spawn_decoder(
        nal_tx.subscribe(),
        cache.clone(),
        generation,
        keyframe_tx,
        session.cancellation(),
    );
    let mut keepalive = session.hid().start_keepalive(session.cancellation());
    let mut reconnect_backoff = config.reconnect_min;

    'actor: loop {
        let status = connected_status(&session, &cache, generation, &media);
        let _ = event_tx.send(ControllerEvent::ConnectionState {
            state: ConnectionState::Connected,
            generation,
        });
        let mut notifications = session.rpc().subscribe_notifications();
        let mut taken_over = false;
        let directive = loop {
            tokio::select! {
                command = command_rx.recv() => {
                    let Some(command) = command else {
                        break ActorDirective::Shutdown(closed_response());
                    };
                    match handle_connected_command(
                        command,
                        &status,
                        &session,
                        &mut media,
                        &cache,
                        generation,
                    ).await {
                        ActorDirective::Continue => {}
                        directive => break directive,
                    }
                }
                state = session.wait_for_end() => {
                    debug!(?state, generation, "controller connection ended");
                    break if taken_over {
                        ActorDirective::Disconnect(None)
                    } else {
                        ActorDirective::Continue
                    };
                }
                notification = notifications.recv() => {
                    if let Ok(notification) = notification
                        && notification.method == "otherSessionConnected"
                    {
                        taken_over = true;
                        let _ = session.hid().reset().await;
                        let _ = event_tx.send(ControllerEvent::TakenOver { generation });
                        let _ = event_tx.send(ControllerEvent::ConnectionState {
                            state: ConnectionState::TakenOver,
                            generation,
                        });
                    }
                }
            }
        };

        match directive {
            ActorDirective::Shutdown(response) => {
                let _ = event_tx.send(ControllerEvent::ConnectionState {
                    state: ConnectionState::ShuttingDown,
                    generation,
                });
                let cleanup = async {
                    media.clean_shutdown().await?;
                    session.shutdown().await?;
                    Result::<()>::Ok(())
                };
                let result = tokio::time::timeout(SHUTDOWN_TIMEOUT, cleanup)
                    .await
                    .context("controller shutdown timed out")
                    .and_then(|result| result);
                decoder.abort();
                keepalive.abort();
                let _ = response.send(result);
                break 'actor;
            }
            ActorDirective::Disconnect(response) => {
                let result = async {
                    media.clean_shutdown().await?;
                    session.shutdown().await
                }
                .await;
                decoder.abort();
                keepalive.abort();
                cache.clear();
                let failed = result.is_err();
                if let Some(response) = response {
                    let _ = response.send(result);
                }
                if failed {
                    break 'actor;
                }
                let _ = event_tx.send(ControllerEvent::ConnectionState {
                    state: ConnectionState::Disconnected,
                    generation,
                });
                loop {
                    let Some(command) = command_rx.recv().await else {
                        break 'actor;
                    };
                    match command {
                        Command::Status(response) => {
                            let _ = response.send(Ok(ControllerStatus::disconnected(
                                ConnectionState::Disconnected,
                                generation,
                            )));
                        }
                        Command::Connect(response) => {
                            generation = generation.saturating_add(1);
                            let _ = event_tx.send(ControllerEvent::ConnectionState {
                                state: ConnectionState::Connecting,
                                generation,
                            });
                            match connect_once(&config, generation, nal_tx.clone()).await {
                                Ok((new_session, auth, keyframe_tx)) => {
                                    session = new_session;
                                    media = VirtualMediaManager::new(
                                        session.rpc().clone(),
                                        auth,
                                        session.hid().clone(),
                                        media_event_tx.clone(),
                                    );
                                    let _ = media.refresh_state().await;
                                    decoder = crate::video::spawn_decoder(
                                        nal_tx.subscribe(),
                                        cache.clone(),
                                        generation,
                                        keyframe_tx,
                                        session.cancellation(),
                                    );
                                    keepalive =
                                        session.hid().start_keepalive(session.cancellation());
                                    let _ = response.send(Ok(connected_status(
                                        &session, &cache, generation, &media,
                                    )));
                                    continue 'actor;
                                }
                                Err(error) => {
                                    let _ = response.send(Err(error));
                                }
                            }
                        }
                        Command::Disconnect(response) => {
                            let _ = response.send(Ok(()));
                        }
                        Command::Shutdown(response) => {
                            let _ = response.send(Ok(()));
                            break 'actor;
                        }
                        command => reject_not_connected(command),
                    }
                }
            }
            ActorDirective::Continue => {
                session.rpc().cancel_generation();
                let _ = session.hid().reset().await;
                let _ = session.shutdown().await;
                decoder.abort();
                keepalive.abort();
                cache.clear();
                if taken_over {
                    warn!(generation, "JetKVM controller session was taken over");
                }
            }
        }

        let (new_session, auth, keyframe_tx) = loop {
            generation = generation.saturating_add(1);
            let _ = event_tx.send(ControllerEvent::ConnectionState {
                state: ConnectionState::Reconnecting,
                generation,
            });
            tokio::time::sleep(reconnect_backoff).await;
            match connect_once(&config, generation, nal_tx.clone()).await {
                Ok(connection) => break connection,
                Err(error) => {
                    warn!(%error, generation, "controller reconnect failed");
                    reconnect_backoff = (reconnect_backoff * 2).min(config.reconnect_max);
                }
            }
        };
        session = new_session;
        media.rebind(session.rpc().clone(), session.hid().clone(), auth);
        let _ = media.refresh_state().await;
        decoder = crate::video::spawn_decoder(
            nal_tx.subscribe(),
            cache.clone(),
            generation,
            keyframe_tx,
            session.cancellation(),
        );
        keepalive = session.hid().start_keepalive(session.cancellation());
        reconnect_backoff = config.reconnect_min;
    }
}

async fn connect_once(
    config: &ConnectionConfig,
    generation: u64,
    nal_tx: broadcast::Sender<NalUnit>,
) -> Result<(SessionConnection, AuthenticatedClient, mpsc::Sender<()>)> {
    let auth = auth::authenticate(&config.host, &config.password, config.no_tls_verify).await?;
    let (keyframe_tx, keyframe_rx) = mpsc::channel(8);
    let session = SessionConnection::connect(
        auth.clone(),
        generation,
        nal_tx,
        config.pli_interval,
        keyframe_tx.clone(),
        keyframe_rx,
    )
    .await?;
    Ok((session, auth, keyframe_tx))
}

fn connected_status(
    session: &SessionConnection,
    cache: &LatestFrameCache,
    generation: u64,
    media: &VirtualMediaManager,
) -> ControllerStatus {
    ControllerStatus {
        connected: true,
        state: ConnectionState::Connected,
        generation,
        device_version: session.device_version().map(str::to_owned),
        signaling: Some(
            match session.signaling_mode() {
                SignalingMode::WebSocket => "websocket",
                SignalingMode::LegacyHttp => "legacy_http",
            }
            .to_owned(),
        ),
        frame: cache.info().map(|frame| FrameStatus {
            age_ms: duration_millis(frame.age),
            width: frame.width,
            height: frame.height,
            generation: frame.generation,
            captured_at: format_system_time(frame.captured_at),
        }),
        hid: Some(session.hid().status()),
        stale_controller_mount: media.has_stale_controller_mount(),
    }
}

async fn handle_connected_command(
    command: Command,
    initial_status: &ControllerStatus,
    session: &SessionConnection,
    media: &mut VirtualMediaManager,
    cache: &LatestFrameCache,
    generation: u64,
) -> ActorDirective {
    match command {
        Command::Status(response) | Command::Connect(response) => {
            let mut status = initial_status.clone();
            status.frame = cache.info().map(|frame| FrameStatus {
                age_ms: duration_millis(frame.age),
                width: frame.width,
                height: frame.height,
                generation: frame.generation,
                captured_at: format_system_time(frame.captured_at),
            });
            status.hid = Some(session.hid().status());
            status.stale_controller_mount = media.has_stale_controller_mount();
            let _ = response.send(Ok(status));
        }
        Command::Disconnect(response) => {
            return ActorDirective::Disconnect(Some(response));
        }
        Command::Snapshot(path, response) => {
            let result = cache
                .snapshot(&path, generation, SNAPSHOT_TIMEOUT)
                .await
                .map(Snapshot::from);
            let _ = response.send(result);
        }
        Command::Key(event, response) => {
            let _ = response.send(session.hid().key(event).await);
        }
        Command::TypeText(request, response) => {
            let result = match keyboard::text_to_macro(&request.text) {
                Ok(steps) => session.hid().type_macro(&steps, request.is_paste).await,
                Err(error) => Err(error),
            };
            let _ = response.send(result);
        }
        Command::AbsoluteMouse(event, response) => {
            let _ = response.send(session.hid().absolute_mouse(event).await);
        }
        Command::RelativeMouse(event, response) => {
            let _ = response.send(session.hid().relative_mouse(event).await);
        }
        Command::Scroll(event, response) => {
            let _ = response.send(session.rpc().scroll(event.wheel_x, event.wheel_y).await);
        }
        Command::MediaState(response) => {
            let _ = response.send(media.refresh_state().await);
        }
        Command::CheckMountUrl(url, response) => {
            let _ = response.send(media.check_url(&url).await);
        }
        Command::MountUrl {
            url,
            mode,
            approval,
            response,
        } => {
            let _ = response.send(media.mount_url(&url, mode, approval).await);
        }
        Command::MountLocal {
            path,
            mode,
            approval,
            response,
        } => {
            let _ = response.send(media.mount_local(&path, mode, approval).await);
        }
        Command::Unmount(approval, response) => {
            let _ = response.send(media.unmount(approval).await);
        }
        Command::StorageSpace(response) => {
            let _ = response.send(media.storage_space().await);
        }
        Command::StorageFiles(response) => {
            let _ = response.send(media.storage_files().await);
        }
        Command::Upload {
            path,
            filename,
            approval,
            cancellation,
            response,
        } => {
            let _ = response.send(media.upload(&path, &filename, approval, cancellation).await);
        }
        Command::MountStorage {
            filename,
            mode,
            approval,
            response,
        } => {
            let _ = response.send(media.mount_storage(&filename, mode, approval).await);
        }
        Command::DeleteStorage {
            filename,
            approval,
            response,
        } => {
            let _ = response.send(media.delete_storage_file(&filename, approval).await);
        }
        Command::Shutdown(response) => return ActorDirective::Shutdown(response),
    }
    ActorDirective::Continue
}

fn reject_not_connected(command: Command) {
    let message = "JetKVM is not connected";
    match command {
        Command::Status(response) | Command::Connect(response) => {
            let _ = response.send(Err(anyhow!(message)));
        }
        Command::Disconnect(response)
        | Command::Key(_, response)
        | Command::TypeText(_, response)
        | Command::AbsoluteMouse(_, response)
        | Command::RelativeMouse(_, response)
        | Command::Scroll(_, response)
        | Command::Unmount(_, response)
        | Command::DeleteStorage { response, .. }
        | Command::Shutdown(response) => {
            let _ = response.send(Err(anyhow!(message)));
        }
        Command::Snapshot(_, response) => {
            let _ = response.send(Err(anyhow!(message)));
        }
        Command::MediaState(response) => {
            let _ = response.send(Err(anyhow!(message)));
        }
        Command::CheckMountUrl(_, response) => {
            let _ = response.send(Err(anyhow!(message)));
        }
        Command::MountUrl { response, .. }
        | Command::MountLocal { response, .. }
        | Command::MountStorage { response, .. } => {
            let _ = response.send(Err(anyhow!(message)));
        }
        Command::StorageSpace(response) => {
            let _ = response.send(Err(anyhow!(message)));
        }
        Command::StorageFiles(response) => {
            let _ = response.send(Err(anyhow!(message)));
        }
        Command::Upload { response, .. } => {
            let _ = response.send(Err(anyhow!(message)));
        }
    }
}

fn closed_response() -> oneshot::Sender<Result<()>> {
    let (sender, receiver) = oneshot::channel();
    drop(receiver);
    sender
}

fn duration_millis(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn format_system_time(time: SystemTime) -> String {
    DateTime::<Utc>::from(time).to_rfc3339()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_config_debug_redacts_password() {
        let config = ConnectionConfig {
            host: "http://jetkvm.invalid".to_owned(),
            password: "secret-value".to_owned(),
            no_tls_verify: false,
            pli_interval: Duration::from_secs(3),
            reconnect_min: Duration::from_secs(1),
            reconnect_max: Duration::from_secs(10),
        };
        let output = format!("{config:?}");
        assert!(!output.contains("secret-value"));
        assert!(output.contains("<redacted>"));
    }

    #[test]
    fn disconnected_status_has_no_stale_generation_data() {
        let status = ControllerStatus::disconnected(ConnectionState::Disconnected, 4);
        assert!(!status.connected);
        assert_eq!(status.generation, 4);
        assert!(status.frame.is_none());
        assert!(status.hid.is_none());
    }
}
