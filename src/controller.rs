use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result, anyhow};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;
use tracing::{debug, warn};
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;

use crate::auth::{self, AuthenticatedClient};
use crate::error::{CodedError, codes};
use crate::h264::NalUnit;
use crate::hid::{AbsoluteMouseEvent, HidStatus, KeyEvent, RelativeMouseEvent};
use crate::keyboard;
use crate::rpc::{
    MountUrlInfo, RpcNotification, StorageFile, StorageSpace, VirtualMediaMode, VirtualMediaState,
};
use crate::session::{SessionConnection, is_terminal_state};
use crate::signaling::SignalingMode;
use crate::video::{LatestFrameCache, ParameterSets, SnapshotFile};
use crate::virtual_media::{Approval, MediaEvent, VirtualMediaManager};

pub use crate::video::FrameCursor;

const COMMAND_BUFFER: usize = 64;
const EVENT_BUFFER: usize = 128;
const NAL_BUFFER: usize = 512;
const SNAPSHOT_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(10);
const MEDIA_CLEANUP_TIMEOUT: Duration = Duration::from_secs(4);
const SESSION_CLEANUP_TIMEOUT: Duration = Duration::from_secs(5);

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
    pub frame_id: u64,
    pub captured_at: String,
}

/// Result of a successful input action: the connection generation and the
/// latest frame cursor captured after the device-facing send/completion
/// boundary. Pass `cursor` as `after` to `snapshot` for a strictly newer
/// frame. This is a transport freshness guarantee, not proof that an
/// arbitrary UI operation completed on the controlled machine.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct ActionReceipt {
    pub generation: u64,
    pub cursor: FrameCursor,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DeviceCapabilities {
    /// `null` while disconnected or before firmware support is known.
    pub check_mount_url: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ControllerStatus {
    pub connected: bool,
    pub state: ConnectionState,
    pub generation: u64,
    pub device_version: Option<String>,
    pub signaling: Option<String>,
    pub device_capabilities: DeviceCapabilities,
    pub frame: Option<FrameStatus>,
    pub hid: Option<HidStatus>,
    pub stale_controller_mount: bool,
    /// Sanitized description of the most recent connection failure.
    /// Never contains credentials, cookies, tokens, or caller secrets.
    pub last_error: Option<String>,
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
    pub cursor: FrameCursor,
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
            cursor: FrameCursor {
                generation: value.generation,
                frame_id: value.frame_id,
            },
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "event", content = "data", rename_all = "snake_case")]
pub enum ControllerEvent {
    ConnectionState {
        state: ConnectionState,
        generation: u64,
        last_error: Option<String>,
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
    lifecycle: Arc<ControllerLifecycle>,
    status: Arc<parking_lot::RwLock<ControllerStatus>>,
    cache: LatestFrameCache,
}

struct ControllerLifecycle {
    shutdown: CancellationToken,
    done: CancellationToken,
    error: Arc<parking_lot::Mutex<Option<String>>>,
}

impl Drop for ControllerLifecycle {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

enum Command {
    Connect(oneshot::Sender<Result<ControllerStatus>>),
    Disconnect(oneshot::Sender<Result<()>>),
    Snapshot {
        after: Option<FrameCursor>,
        response: oneshot::Sender<Result<Snapshot>>,
    },
    SnapshotTo {
        path: PathBuf,
        approval: Approval,
        after: Option<FrameCursor>,
        response: oneshot::Sender<Result<Snapshot>>,
    },
    Key(KeyEvent, oneshot::Sender<Result<ActionReceipt>>),
    TypeText(TypeTextRequest, oneshot::Sender<Result<ActionReceipt>>),
    AbsoluteMouse(AbsoluteMouseEvent, oneshot::Sender<Result<ActionReceipt>>),
    RelativeMouse(RelativeMouseEvent, oneshot::Sender<Result<ActionReceipt>>),
    Scroll(ScrollEvent, oneshot::Sender<Result<ActionReceipt>>),
    MediaState(oneshot::Sender<Result<Option<VirtualMediaState>>>),
    CheckMountUrl {
        url: String,
        approval: Approval,
        response: oneshot::Sender<Result<MountUrlInfo>>,
    },
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
}

/// A successfully established session plus the bits the actor needs.
struct Established {
    session: SessionConnection,
    auth: AuthenticatedClient,
    keyframe_tx: mpsc::Sender<()>,
}

type ConnectFuture = Pin<Box<dyn Future<Output = Result<Established>> + Send>>;

/// Connection attempts are produced behind this trait so lifecycle behavior
/// is testable without real network endpoints. The production implementation
/// is the only connector used outside tests.
trait Connector: Send + Sync + 'static {
    fn connect(&self, generation: u64, nal_tx: broadcast::Sender<NalUnit>) -> ConnectFuture;
}

struct ProductionConnector {
    config: ConnectionConfig,
}

impl Connector for ProductionConnector {
    fn connect(&self, generation: u64, nal_tx: broadcast::Sender<NalUnit>) -> ConnectFuture {
        let config = self.config.clone();
        Box::pin(async move {
            let auth =
                auth::authenticate(&config.host, &config.password, config.no_tls_verify).await?;
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
            Ok(Established {
                session,
                auth,
                keyframe_tx,
            })
        })
    }
}

/// Per-session state held while in [`Phase::Connected`].
struct Connected {
    session: SessionConnection,
    keyframe_tx: mpsc::Sender<()>,
    keepalive: tokio::task::JoinHandle<()>,
    decoder: Option<tokio::task::JoinHandle<Result<()>>>,
    notifications: broadcast::Receiver<RpcNotification>,
    taken_over: bool,
}

/// Explicit connection state machine. All states live inside one
/// command-servicing loop, so `status`, `disconnect`, and shutdown stay
/// responsive regardless of connectivity.
enum Phase {
    Disconnected,
    Connecting {
        attempt: tokio::task::JoinHandle<Result<Established>>,
        waiters: Vec<oneshot::Sender<Result<ControllerStatus>>>,
    },
    Reconnecting {
        wake: Pin<Box<tokio::time::Sleep>>,
    },
    TakenOver,
    Connected(Box<Connected>),
    Shutdown(Result<()>),
}

/// How an interrupted connected-command execution should transition.
enum Interrupt {
    None,
    Shutdown,
    SessionEnd,
    TakenOver,
}

enum RaceOutcome<T> {
    Completed(T),
    Shutdown,
    SessionEnd,
    TakenOver,
}

struct Actor {
    connector: Arc<dyn Connector>,
    config: ConnectionConfig,
    command_rx: mpsc::Receiver<Command>,
    event_tx: broadcast::Sender<ControllerEvent>,
    nal_tx: broadcast::Sender<NalUnit>,
    media_event_tx: broadcast::Sender<MediaEvent>,
    shutdown: CancellationToken,
    error: Arc<parking_lot::Mutex<Option<String>>>,
    status: Arc<parking_lot::RwLock<ControllerStatus>>,
    snapshot_directory: tempfile::TempDir,
    cache: LatestFrameCache,
    media: Option<VirtualMediaManager>,
    /// Subscribed before each connection attempt so the device's initial
    /// SPS/PPS burst is captured; replaced per attempt so stale NALs from a
    /// previous generation are never observed.
    parameter_rx: broadcast::Receiver<NalUnit>,
    parameter_sets: ParameterSets,
    next_snapshot_id: u64,
    generation: u64,
    backoff: Duration,
    last_error: Option<String>,
}

/// Channel ends the actor owns.
struct ActorChannels {
    command_rx: mpsc::Receiver<Command>,
    event_tx: broadcast::Sender<ControllerEvent>,
    nal_tx: broadcast::Sender<NalUnit>,
    media_event_tx: broadcast::Sender<MediaEvent>,
}

impl Actor {
    fn new(
        config: ConnectionConfig,
        connector: Arc<dyn Connector>,
        channels: ActorChannels,
        shutdown: CancellationToken,
        error: Arc<parking_lot::Mutex<Option<String>>>,
        status: Arc<parking_lot::RwLock<ControllerStatus>>,
        cache: LatestFrameCache,
    ) -> Result<Self> {
        let snapshot_directory = tempfile::Builder::new()
            .prefix("recorder-for-jetkvm-")
            .tempdir()
            .context("failed to create snapshot directory")?;
        let ActorChannels {
            command_rx,
            event_tx,
            nal_tx,
            media_event_tx,
        } = channels;
        let parameter_rx = nal_tx.subscribe();
        Ok(Self {
            backoff: config.reconnect_min,
            connector,
            config,
            command_rx,
            event_tx,
            nal_tx,
            media_event_tx,
            shutdown,
            error,
            status,
            snapshot_directory,
            cache,
            media: None,
            parameter_rx,
            parameter_sets: ParameterSets::default(),
            next_snapshot_id: 1,
            generation: 0,
            last_error: None,
        })
    }

    async fn run(mut self) {
        let actor_error = Arc::clone(&self.error);
        let mut phase = self.start_attempt(Vec::new());
        let result = loop {
            phase = match phase {
                Phase::Connecting { attempt, waiters } => {
                    self.serve_connecting(attempt, waiters).await
                }
                Phase::Reconnecting { wake } => self.serve_reconnecting(wake).await,
                Phase::Disconnected => self.serve_offline(ConnectionState::Disconnected).await,
                Phase::TakenOver => self.serve_offline(ConnectionState::TakenOver).await,
                Phase::Connected(connected) => self.serve_connected(connected).await,
                Phase::Shutdown(result) => break result,
            };
        };
        if let Err(error) = result {
            *actor_error.lock() = Some(format!("{error:#}"));
        }
    }

    fn publish_status(&self, status: ControllerStatus) {
        *self.status.write() = status;
    }

    fn transition_event(&self, state: ConnectionState) {
        if state != ConnectionState::Connected {
            self.publish_status(self.offline_status(state));
        }
        let _ = self.event_tx.send(ControllerEvent::ConnectionState {
            state,
            generation: self.generation,
            last_error: self.last_error.clone(),
        });
    }

    /// Starts a connection attempt immediately, incrementing the generation
    /// at this single defined boundary.
    fn start_attempt(&mut self, waiters: Vec<oneshot::Sender<Result<ControllerStatus>>>) -> Phase {
        self.generation = self.generation.saturating_add(1);
        // Fresh subscription before the attempt so the new session's initial
        // parameter-set burst is observed and nothing stale can be buffered.
        self.parameter_rx = self.nal_tx.subscribe();
        self.parameter_sets.clear();
        let attempt = tokio::spawn(self.connector.connect(self.generation, self.nal_tx.clone()));
        self.transition_event(ConnectionState::Connecting);
        Phase::Connecting { attempt, waiters }
    }

    fn start_reconnect(&mut self) -> Phase {
        self.transition_event(ConnectionState::Reconnecting);
        let wake = Box::pin(tokio::time::sleep(self.backoff));
        self.backoff = (self.backoff * 2).min(self.config.reconnect_max);
        Phase::Reconnecting { wake }
    }

    fn media(&mut self) -> &mut VirtualMediaManager {
        self.media
            .as_mut()
            .expect("media manager exists while connected")
    }

    async fn serve_connecting(
        &mut self,
        mut attempt: tokio::task::JoinHandle<Result<Established>>,
        mut waiters: Vec<oneshot::Sender<Result<ControllerStatus>>>,
    ) -> Phase {
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    discard_attempt(attempt).await;
                    fail_waiters(waiters, "controller is shutting down");
                    return self.shutdown_cleanup(None).await;
                }
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        discard_attempt(attempt).await;
                        fail_waiters(waiters, "controller client disconnected");
                        return self.shutdown_cleanup(None).await;
                    };
                    match command {
                        Command::Connect(response) => waiters.push(response),
                        Command::Disconnect(response) => {
                            discard_attempt(attempt).await;
                            fail_waiters(waiters, "connection attempt cancelled by disconnect");
                            let _ = response.send(Ok(()));
                            self.last_error = None;
                            self.transition_event(ConnectionState::Disconnected);
                            return Phase::Disconnected;
                        }
                        command => reject_not_connected(command),
                    }
                }
                result = &mut attempt => {
                    return match result {
                        Ok(Ok(established)) => self.establish(established, waiters).await,
                        Ok(Err(error)) => {
                            let message = sanitize_error(&error, &self.config.password);
                            warn!(%message, generation = self.generation, "JetKVM connection attempt failed");
                            self.last_error = Some(message.clone());
                            fail_waiters(waiters, &message);
                            self.start_reconnect()
                        }
                        Err(join_error) => {
                            let message = format!("connection attempt task failed: {join_error}");
                            self.last_error = Some(message.clone());
                            fail_waiters(waiters, &message);
                            self.start_reconnect()
                        }
                    };
                }
            }
        }
    }

    async fn serve_reconnecting(&mut self, wake: Pin<Box<tokio::time::Sleep>>) -> Phase {
        let mut wake = wake;
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => return self.shutdown_cleanup(None).await,
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        return self.shutdown_cleanup(None).await;
                    };
                    match command {
                        Command::Connect(response) => {
                            return self.start_attempt(vec![response]);
                        }
                        Command::Disconnect(response) => {
                            let _ = response.send(Ok(()));
                            self.last_error = None;
                            self.transition_event(ConnectionState::Disconnected);
                            return Phase::Disconnected;
                        }
                        command => reject_not_connected(command),
                    }
                }
                _ = &mut wake => return self.start_attempt(Vec::new()),
            }
        }
    }

    /// Serves `Disconnected` and `TakenOver`: no automatic reconnect; an
    /// explicit `connect` starts an attempt immediately.
    async fn serve_offline(&mut self, _state: ConnectionState) -> Phase {
        loop {
            tokio::select! {
                _ = self.shutdown.cancelled() => return self.shutdown_cleanup(None).await,
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        return self.shutdown_cleanup(None).await;
                    };
                    match command {
                        Command::Connect(response) => {
                            return self.start_attempt(vec![response]);
                        }
                        Command::Disconnect(response) => {
                            let _ = response.send(Ok(()));
                        }
                        command => reject_not_connected(command),
                    }
                }
            }
        }
    }

    async fn serve_connected(&mut self, mut connected: Box<Connected>) -> Phase {
        let mut end_watch = connected.session.end_watch();
        loop {
            if is_terminal_state(*end_watch.borrow()) {
                return self.session_lost(connected).await;
            }
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    return self.shutdown_cleanup(Some(connected)).await;
                }
                command = self.command_rx.recv() => {
                    let Some(command) = command else {
                        return self.shutdown_cleanup(Some(connected)).await;
                    };
                    match command {
                        Command::Connect(response) => {
                            let _ = response.send(Ok(self.connected_status(&connected)));
                        }
                        Command::Disconnect(response) => {
                            let result = self.teardown_full(connected).await;
                            let failed = result.is_err();
                            let _ = response.send(result);
                            if failed {
                                return Phase::Shutdown(Err(anyhow!(
                                    "failed to clean up the disconnected session"
                                )));
                            }
                            self.last_error = None;
                            self.transition_event(ConnectionState::Disconnected);
                            return Phase::Disconnected;
                        }
                        command => {
                            match self.execute_connected(&mut connected, &mut end_watch, command).await {
                                Interrupt::None => {
                                    self.publish_status(self.connected_status(&connected));
                                }
                                Interrupt::Shutdown => {
                                    return self.shutdown_cleanup(Some(connected)).await;
                                }
                                Interrupt::SessionEnd => {
                                    return self.session_lost(connected).await;
                                }
                                Interrupt::TakenOver => {
                                    connected.taken_over = true;
                                    let _ = connected.session.hid().reset().await;
                                    let _ = self.event_tx.send(ControllerEvent::TakenOver {
                                        generation: self.generation,
                                    });
                                    return self.takeover(connected).await;
                                }
                            }
                        }
                    }
                }
                changed = end_watch.changed() => {
                    if changed.is_err() || is_terminal_state(*end_watch.borrow()) {
                        return self.session_lost(connected).await;
                    }
                    debug!(state = ?*end_watch.borrow(), generation = self.generation, "peer connection state");
                }
                notification = connected.notifications.recv() => {
                    match notification {
                        Ok(notification) if notification.method == "otherSessionConnected" => {
                            connected.taken_over = true;
                            let _ = connected.session.hid().reset().await;
                            let _ = self.event_tx.send(ControllerEvent::TakenOver {
                                generation: self.generation,
                            });
                            return self.takeover(connected).await;
                        }
                        Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                        Err(broadcast::error::RecvError::Closed) => {
                            return self.session_lost(connected).await;
                        }
                    }
                }
                parameter = self.parameter_rx.recv() => {
                    match parameter {
                        Ok(nal) => self.parameter_sets.observe(&nal),
                        Err(broadcast::error::RecvError::Lagged(_)) => {
                            self.parameter_sets.clear();
                        }
                        Err(broadcast::error::RecvError::Closed) => {}
                    }
                }
            }
        }
    }

    /// Runs one connected command while lifecycle signals stay live: global
    /// shutdown, session end, and takeover all interrupt the operation.
    async fn execute_connected(
        &mut self,
        connected: &mut Connected,
        end_watch: &mut watch::Receiver<RTCPeerConnectionState>,
        command: Command,
    ) -> Interrupt {
        let shutdown = self.shutdown.clone();
        macro_rules! run {
            ($future:expr, $response:expr) => {{
                let outcome =
                    race_interrupts(&shutdown, end_watch, &mut connected.notifications, $future)
                        .await;
                complete(outcome, $response)
            }};
        }
        match command {
            Command::Connect(_) | Command::Disconnect(_) => {
                unreachable!("lifecycle commands are intercepted before execute_connected")
            }
            Command::Snapshot { after, response } => {
                let generation = self.generation;
                let path = self.snapshot_directory.path().join(format!(
                    "snapshot-{generation}-{}.png",
                    self.next_snapshot_id
                ));
                self.next_snapshot_id = self.next_snapshot_id.saturating_add(1);
                let cache = self.cache.clone();
                let nal_tx = self.nal_tx.clone();
                let keyframe_tx = connected.keyframe_tx.clone();
                let cancellation = connected.session.cancellation();
                let parameter_sets = self.parameter_sets.clone();
                let decoder = &mut connected.decoder;
                let future = async move {
                    ensure_decoder(
                        decoder,
                        &nal_tx,
                        &cache,
                        generation,
                        keyframe_tx,
                        cancellation,
                        parameter_sets,
                    )
                    .await?;
                    cache
                        .snapshot(&path, generation, after, SNAPSHOT_TIMEOUT)
                        .await
                        .map(Snapshot::from)
                };
                run!(future, response)
            }
            Command::SnapshotTo {
                path,
                approval,
                after,
                response,
            } => {
                let generation = self.generation;
                let cache = self.cache.clone();
                let nal_tx = self.nal_tx.clone();
                let keyframe_tx = connected.keyframe_tx.clone();
                let cancellation = connected.session.cancellation();
                let parameter_sets = self.parameter_sets.clone();
                let decoder = &mut connected.decoder;
                let future = async move {
                    approval.require("write a snapshot to a caller-selected path")?;
                    ensure_decoder(
                        decoder,
                        &nal_tx,
                        &cache,
                        generation,
                        keyframe_tx,
                        cancellation,
                        parameter_sets,
                    )
                    .await?;
                    cache
                        .snapshot(&path, generation, after, SNAPSHOT_TIMEOUT)
                        .await
                        .map(Snapshot::from)
                };
                run!(future, response)
            }
            Command::Key(event, response) => {
                let future = connected.session.hid().key(event);
                let future = async {
                    future.await?;
                    Ok(self.receipt())
                };
                run!(future, response)
            }
            Command::TypeText(request, response) => match keyboard::text_to_macro(&request.text) {
                Ok(steps) => {
                    let send = connected.session.hid().type_macro(&steps, request.is_paste);
                    let future = async {
                        send.await?;
                        Ok(self.receipt())
                    };
                    run!(future, response)
                }
                Err(error) => {
                    let _ = response.send(Err(error));
                    Interrupt::None
                }
            },
            Command::AbsoluteMouse(event, response) => {
                let send = connected.session.hid().absolute_mouse(event);
                let future = async {
                    send.await?;
                    Ok(self.receipt())
                };
                run!(future, response)
            }
            Command::RelativeMouse(event, response) => {
                let send = connected.session.hid().relative_mouse(event);
                let future = async {
                    send.await?;
                    Ok(self.receipt())
                };
                run!(future, response)
            }
            Command::Scroll(event, response) => {
                let send = connected.session.rpc().scroll(event.wheel_x, event.wheel_y);
                let future = async {
                    send.await?;
                    Ok(self.receipt())
                };
                run!(future, response)
            }
            Command::MediaState(response) => {
                run!(self.media().refresh_state(), response)
            }
            Command::CheckMountUrl {
                url,
                approval,
                response,
            } => {
                let validated = approval
                    .require("ask JetKVM to fetch a URL")
                    .and_then(|_| validate_mount_check_url(&url));
                match validated {
                    Ok(()) => run!(self.media().check_url(&url), response),
                    Err(error) => {
                        let _ = response.send(Err(error));
                        Interrupt::None
                    }
                }
            }
            Command::MountUrl {
                url,
                mode,
                approval,
                response,
            } => {
                run!(self.media().mount_url(&url, mode, approval), response)
            }
            Command::MountLocal {
                path,
                mode,
                approval,
                response,
            } => {
                run!(self.media().mount_local(&path, mode, approval), response)
            }
            Command::Unmount(approval, response) => {
                run!(self.media().unmount(approval), response)
            }
            Command::StorageSpace(response) => {
                run!(self.media().storage_space(), response)
            }
            Command::StorageFiles(response) => {
                run!(self.media().storage_files(), response)
            }
            Command::Upload {
                path,
                filename,
                approval,
                cancellation,
                response,
            } => {
                if cancellation.is_cancelled() {
                    let _ = response.send(Err(CodedError::new(
                        codes::CANCELLED,
                        "upload was cancelled before it started",
                    )
                    .into()));
                    return Interrupt::None;
                }
                let token = cancellation.clone();
                let future = self
                    .media()
                    .upload(&path, &filename, approval, cancellation);
                let outcome =
                    race_interrupts(&shutdown, end_watch, &mut connected.notifications, future)
                        .await;
                if !matches!(outcome, RaceOutcome::Completed(_)) {
                    token.cancel();
                }
                complete(outcome, response)
            }
            Command::MountStorage {
                filename,
                mode,
                approval,
                response,
            } => {
                run!(
                    self.media().mount_storage(&filename, mode, approval),
                    response
                )
            }
            Command::DeleteStorage {
                filename,
                approval,
                response,
            } => {
                run!(
                    self.media().delete_storage_file(&filename, approval),
                    response
                )
            }
        }
    }

    /// Installs a fresh session: builds or rebinds the media manager, starts
    /// HID keepalive, resets per-generation caches, answers connect waiters.
    async fn establish(
        &mut self,
        established: Established,
        waiters: Vec<oneshot::Sender<Result<ControllerStatus>>>,
    ) -> Phase {
        let Established {
            session,
            auth,
            keyframe_tx,
        } = established;
        let supports = supports_check_mount_url(session.device_version());
        if !supports {
            warn!(
                device_version = ?session.device_version(),
                "JetKVM firmware does not support checkMountUrl; preflight URL checks are disabled"
            );
        }
        match self.media.as_mut() {
            Some(media) => media.rebind(
                session.rpc().clone(),
                session.hid().clone(),
                session.peer_connection(),
                auth,
                supports,
            ),
            None => {
                self.media = Some(VirtualMediaManager::new(
                    session.rpc().clone(),
                    auth,
                    session.hid().clone(),
                    session.peer_connection(),
                    supports,
                    self.media_event_tx.clone(),
                ));
            }
        }
        let _ = self.media().refresh_state().await;
        let keepalive = session.hid().start_keepalive(session.cancellation());
        let connected = Connected {
            notifications: session.rpc().subscribe_notifications(),
            session,
            keyframe_tx,
            keepalive,
            decoder: None,
            taken_over: false,
        };
        self.backoff = self.config.reconnect_min;
        self.last_error = None;
        let status = self.connected_status(&connected);
        self.publish_status(status.clone());
        for waiter in waiters {
            let _ = waiter.send(Ok(status.clone()));
        }
        self.transition_event(ConnectionState::Connected);
        Phase::Connected(Box::new(connected))
    }

    /// The session ended on its own: light teardown, then auto-reconnect
    /// unless another client took the session over.
    async fn session_lost(&mut self, mut connected: Box<Connected>) -> Phase {
        debug!(generation = self.generation, "controller connection ended");
        // A takeover notification can race the terminal peer state; prefer
        // the takeover explanation to avoid a reconnect fight.
        while let Ok(notification) = connected.notifications.try_recv() {
            if notification.method == "otherSessionConnected" {
                connected.taken_over = true;
            }
        }
        let taken_over = connected.taken_over;
        self.teardown_light(connected).await;
        if taken_over {
            warn!(
                generation = self.generation,
                "JetKVM controller session was taken over"
            );
            let _ = self.event_tx.send(ControllerEvent::TakenOver {
                generation: self.generation,
            });
            self.transition_event(ConnectionState::TakenOver);
            Phase::TakenOver
        } else {
            self.start_reconnect()
        }
    }

    async fn takeover(&mut self, connected: Box<Connected>) -> Phase {
        warn!(
            generation = self.generation,
            "JetKVM controller session was taken over"
        );
        self.teardown_light(connected).await;
        self.transition_event(ConnectionState::TakenOver);
        Phase::TakenOver
    }

    /// Light teardown after transport loss: pending RPCs fail, local HID
    /// intent is cleared, per-generation tasks stop. The media manager (and
    /// any controller-hosted range server) survives for a later rebind.
    async fn teardown_light(&mut self, connected: Box<Connected>) {
        let Connected {
            session,
            keepalive,
            mut decoder,
            ..
        } = *connected;
        session.rpc().cancel_generation();
        session.hid().connection_lost();
        keepalive.abort();
        if let Some(decoder) = decoder.take() {
            decoder.abort();
        }
        self.cache.clear();
        let _ = tokio::time::timeout(SESSION_CLEANUP_TIMEOUT, session.shutdown()).await;
    }

    /// Full teardown for explicit disconnect/shutdown: controller-owned media
    /// cleanup (unmount + verify) runs before the session and any range
    /// server go away.
    async fn teardown_full(&mut self, connected: Box<Connected>) -> Result<()> {
        let Connected {
            session,
            keepalive,
            mut decoder,
            ..
        } = *connected;
        session.cancellation().cancel();
        session.rpc().cancel_generation();
        keepalive.abort();
        if let Some(decoder) = decoder.take() {
            decoder.abort();
        }
        self.cache.clear();
        let media_result = match self.media.take() {
            Some(media) => tokio::time::timeout(MEDIA_CLEANUP_TIMEOUT, media.clean_shutdown())
                .await
                .context("controller-owned media cleanup timed out")
                .and_then(|result| result),
            None => Ok(()),
        };
        let session_result = tokio::time::timeout(SESSION_CLEANUP_TIMEOUT, session.shutdown())
            .await
            .context("session shutdown timed out")
            .and_then(|result| result);
        media_result.and(session_result)
    }

    async fn shutdown_cleanup(&mut self, connected: Option<Box<Connected>>) -> Phase {
        self.transition_event(ConnectionState::ShuttingDown);
        let result = match connected {
            Some(connected) => self.teardown_full(connected).await,
            None => match self.media.take() {
                Some(media) => tokio::time::timeout(SHUTDOWN_TIMEOUT, media.clean_shutdown())
                    .await
                    .context("controller-owned media cleanup timed out")
                    .and_then(|result| result),
                None => Ok(()),
            },
        };
        Phase::Shutdown(result)
    }

    fn connected_status(&self, connected: &Connected) -> ControllerStatus {
        let session = &connected.session;
        ControllerStatus {
            connected: true,
            state: ConnectionState::Connected,
            generation: self.generation,
            device_version: session.device_version().map(str::to_owned),
            device_capabilities: DeviceCapabilities {
                check_mount_url: self
                    .media
                    .as_ref()
                    .map(VirtualMediaManager::supports_check_mount_url),
            },
            signaling: Some(
                match session.signaling_mode() {
                    SignalingMode::WebSocket => "websocket",
                    SignalingMode::LegacyHttp => "legacy_http",
                }
                .to_owned(),
            ),
            frame: self.cache.info().map(|frame| FrameStatus {
                age_ms: duration_millis(frame.age),
                width: frame.width,
                height: frame.height,
                generation: frame.generation,
                frame_id: frame.frame_id,
                captured_at: format_system_time(frame.captured_at),
            }),
            hid: Some(session.hid().status()),
            stale_controller_mount: self
                .media
                .as_ref()
                .is_some_and(|media| media.has_stale_controller_mount()),
            last_error: None,
        }
    }

    /// Captured immediately after the device-facing send/completion
    /// boundary of an input action.
    fn receipt(&self) -> ActionReceipt {
        let cursor = self.cache.info().map_or(
            FrameCursor {
                generation: self.generation,
                frame_id: 0,
            },
            |info| FrameCursor {
                generation: info.generation,
                frame_id: info.frame_id,
            },
        );
        ActionReceipt {
            generation: self.generation,
            cursor,
        }
    }

    fn offline_status(&self, state: ConnectionState) -> ControllerStatus {
        ControllerStatus {
            connected: false,
            state,
            generation: self.generation,
            device_version: None,
            signaling: None,
            device_capabilities: DeviceCapabilities {
                check_mount_url: None,
            },
            frame: None,
            hid: None,
            stale_controller_mount: self
                .media
                .as_ref()
                .is_some_and(|media| media.has_stale_controller_mount()),
            last_error: self.last_error.clone(),
        }
    }
}

/// Aborts a connection attempt; if it completed concurrently, the fresh
/// session is shut down instead of leaking.
async fn discard_attempt(attempt: tokio::task::JoinHandle<Result<Established>>) {
    attempt.abort();
    if let Ok(Ok(established)) = attempt.await {
        let _ = tokio::time::timeout(SESSION_CLEANUP_TIMEOUT, established.session.shutdown()).await;
    }
}

fn fail_waiters(waiters: Vec<oneshot::Sender<Result<ControllerStatus>>>, message: &str) {
    for waiter in waiters {
        let _ = waiter.send(Err(anyhow!(message.to_owned())));
    }
}

/// Races a connected command against lifecycle signals so shutdown, session
/// end, and takeover are observed promptly even during long operations.
async fn race_interrupts<F, T>(
    shutdown: &CancellationToken,
    end_watch: &mut watch::Receiver<RTCPeerConnectionState>,
    notifications: &mut broadcast::Receiver<RpcNotification>,
    future: F,
) -> RaceOutcome<T>
where
    F: Future<Output = T>,
{
    tokio::pin!(future);
    loop {
        if is_terminal_state(*end_watch.borrow()) {
            return RaceOutcome::SessionEnd;
        }
        tokio::select! {
            result = &mut future => return RaceOutcome::Completed(result),
            _ = shutdown.cancelled() => return RaceOutcome::Shutdown,
            changed = end_watch.changed() => {
                if changed.is_err() || is_terminal_state(*end_watch.borrow()) {
                    return RaceOutcome::SessionEnd;
                }
            }
            notification = notifications.recv() => {
                match notification {
                    Ok(notification) if notification.method == "otherSessionConnected" => {
                        return RaceOutcome::TakenOver;
                    }
                    Ok(_) | Err(broadcast::error::RecvError::Lagged(_)) => {}
                    Err(broadcast::error::RecvError::Closed) => return RaceOutcome::SessionEnd,
                }
            }
        }
    }
}

fn complete<T>(outcome: RaceOutcome<Result<T>>, response: oneshot::Sender<Result<T>>) -> Interrupt {
    match outcome {
        RaceOutcome::Completed(result) => {
            let _ = response.send(result);
            Interrupt::None
        }
        RaceOutcome::Shutdown => {
            let _ = response.send(Err(anyhow!("controller is shutting down")));
            Interrupt::Shutdown
        }
        RaceOutcome::SessionEnd => {
            let _ = response.send(Err(anyhow!("JetKVM connection ended")));
            Interrupt::SessionEnd
        }
        RaceOutcome::TakenOver => {
            let _ = response.send(Err(anyhow!("JetKVM session was taken over")));
            Interrupt::TakenOver
        }
    }
}

/// Removes secrets from an error message destined for status/events:
/// the configured password, local media tokens, and URL credentials.
fn sanitize_error(error: &anyhow::Error, password: &str) -> String {
    let mut message = format!("{error:#}");
    if !password.is_empty() {
        message = message.replace(password, "<redacted>");
    }
    crate::control_protocol::redact(message)
}

async fn ensure_decoder(
    decoder: &mut Option<tokio::task::JoinHandle<Result<()>>>,
    nal_tx: &broadcast::Sender<NalUnit>,
    cache: &LatestFrameCache,
    generation: u64,
    keyframe_tx: mpsc::Sender<()>,
    cancellation: CancellationToken,
    parameter_sets: ParameterSets,
) -> Result<()> {
    if decoder.as_ref().is_some_and(|task| task.is_finished()) {
        let finished = decoder.take().expect("finished decoder task is present");
        finished.await.context("video decoder task failed")??;
    }
    if decoder.is_none() {
        *decoder = Some(crate::video::spawn_decoder(
            nal_tx.subscribe(),
            cache.clone(),
            generation,
            keyframe_tx,
            cancellation,
            parameter_sets,
        ));
    }
    Ok(())
}

impl JetKvmController {
    /// Starts the controller actor and returns as soon as it can serve
    /// commands. A remote session is NOT required: the initial connection
    /// attempt runs as cancellable state-machine work, so the stdio protocol
    /// handshake and `status` work while the device is offline.
    pub async fn connect(config: ConnectionConfig) -> Result<Self> {
        let connector: Arc<dyn Connector> = Arc::new(ProductionConnector {
            config: config.clone(),
        });
        Self::spawn(config, connector).await
    }

    async fn spawn(config: ConnectionConfig, connector: Arc<dyn Connector>) -> Result<Self> {
        let (command_tx, command_rx) = mpsc::channel(COMMAND_BUFFER);
        let (event_tx, _) = broadcast::channel(EVENT_BUFFER);
        let (nal_tx, _) = broadcast::channel(NAL_BUFFER);
        let (media_event_tx, mut media_event_rx) = broadcast::channel(EVENT_BUFFER);
        let (ready_tx, ready_rx) = oneshot::channel();
        let shutdown = CancellationToken::new();
        let done = CancellationToken::new();
        let error = Arc::new(parking_lot::Mutex::new(None));
        let status = Arc::new(parking_lot::RwLock::new(ControllerStatus {
            connected: false,
            state: ConnectionState::Connecting,
            generation: 0,
            device_version: None,
            signaling: None,
            device_capabilities: DeviceCapabilities {
                check_mount_url: None,
            },
            frame: None,
            hid: None,
            stale_controller_mount: false,
            last_error: None,
        }));
        let cache = LatestFrameCache::new();
        let lifecycle = Arc::new(ControllerLifecycle {
            shutdown: shutdown.clone(),
            done: done.clone(),
            error: Arc::clone(&error),
        });

        let actor_event_tx = event_tx.clone();
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

        let spawn_event_tx = event_tx.clone();
        let spawn_nal_tx = nal_tx.clone();
        let actor_status = Arc::clone(&status);
        let actor_cache = cache.clone();
        tokio::spawn(async move {
            match Actor::new(
                config,
                connector,
                ActorChannels {
                    command_rx,
                    event_tx: spawn_event_tx,
                    nal_tx: spawn_nal_tx,
                    media_event_tx,
                },
                shutdown,
                error,
                actor_status,
                actor_cache,
            ) {
                Ok(actor) => {
                    let _ = ready_tx.send(Ok(()));
                    actor.run().await;
                }
                Err(error) => {
                    let _ = ready_tx.send(Err(error));
                }
            }
            done.cancel();
        });

        ready_rx
            .await
            .context("controller actor stopped during startup")??;
        Ok(Self {
            command_tx,
            event_tx,
            nal_tx,
            lifecycle,
            status,
            cache,
        })
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<ControllerEvent> {
        self.event_tx.subscribe()
    }

    pub fn subscribe_nals(&self) -> broadcast::Receiver<NalUnit> {
        self.nal_tx.subscribe()
    }

    pub async fn status(&self) -> Result<ControllerStatus> {
        let mut status = self.status.read().clone();
        if status.connected {
            status.frame = self.cache.info().map(|frame| FrameStatus {
                age_ms: duration_millis(frame.age),
                width: frame.width,
                height: frame.height,
                generation: frame.generation,
                frame_id: frame.frame_id,
                captured_at: format_system_time(frame.captured_at),
            });
        }
        Ok(status)
    }

    pub async fn reconnect(&self) -> Result<ControllerStatus> {
        request(&self.command_tx, Command::Connect).await
    }

    pub async fn disconnect(&self) -> Result<()> {
        request(&self.command_tx, Command::Disconnect).await
    }

    pub async fn snapshot(&self, after: Option<FrameCursor>) -> Result<Snapshot> {
        request_with(&self.command_tx, |response| Command::Snapshot {
            after,
            response,
        })
        .await
    }

    pub async fn snapshot_to(
        &self,
        path: PathBuf,
        approval: Approval,
        after: Option<FrameCursor>,
    ) -> Result<Snapshot> {
        request_with(&self.command_tx, |response| Command::SnapshotTo {
            path,
            approval,
            after,
            response,
        })
        .await
    }

    pub async fn key(&self, event: KeyEvent) -> Result<ActionReceipt> {
        request_with(&self.command_tx, |response| Command::Key(event, response)).await
    }

    pub async fn type_text(&self, request_value: TypeTextRequest) -> Result<ActionReceipt> {
        request_with(&self.command_tx, |response| {
            Command::TypeText(request_value, response)
        })
        .await
    }

    pub async fn absolute_mouse(&self, event: AbsoluteMouseEvent) -> Result<ActionReceipt> {
        request_with(&self.command_tx, |response| {
            Command::AbsoluteMouse(event, response)
        })
        .await
    }

    pub async fn relative_mouse(&self, event: RelativeMouseEvent) -> Result<ActionReceipt> {
        request_with(&self.command_tx, |response| {
            Command::RelativeMouse(event, response)
        })
        .await
    }

    pub async fn scroll(&self, event: ScrollEvent) -> Result<ActionReceipt> {
        request_with(&self.command_tx, |response| {
            Command::Scroll(event, response)
        })
        .await
    }

    pub async fn media_state(&self) -> Result<Option<VirtualMediaState>> {
        request(&self.command_tx, Command::MediaState).await
    }

    pub async fn check_mount_url(&self, url: String, approval: Approval) -> Result<MountUrlInfo> {
        request_with(&self.command_tx, |response| Command::CheckMountUrl {
            url,
            approval,
            response,
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
        self.lifecycle.shutdown.cancel();
        tokio::time::timeout(
            SHUTDOWN_TIMEOUT + Duration::from_secs(2),
            self.lifecycle.done.cancelled(),
        )
        .await
        .context("controller actor did not stop within the shutdown deadline")?;
        if let Some(error) = self.lifecycle.error.lock().clone() {
            return Err(anyhow!(error));
        }
        Ok(())
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

/// Conservative firmware gate for the `checkMountUrl` RPC. Upstream
/// `release/0.5.9` dev builds through `202606220836` implement it as
/// `not implemented`; build `202606301105` is the first working one.
/// Release 0.5.9 and newer are assumed capable; the virtual media manager
/// additionally falls back at runtime if the RPC reports `not implemented`.
pub(crate) fn supports_check_mount_url(device_version: Option<&str>) -> bool {
    const FIRST_WORKING_DEV_BUILD: u64 = 202606301105;
    let Some(version) = device_version else {
        return false;
    };
    let version = version.trim().trim_start_matches('v');
    let mut parts = version.splitn(2, '-');
    let release = parts.next().unwrap_or_default();
    let suffix = parts.next();
    let mut numbers = release.split('.');
    let (Some(major), Some(minor), Some(patch)) = (
        numbers.next().and_then(|part| part.parse::<u64>().ok()),
        numbers.next().and_then(|part| part.parse::<u64>().ok()),
        numbers.next().and_then(|part| part.parse::<u64>().ok()),
    ) else {
        return false;
    };
    match (major, minor, patch) {
        version if version > (0, 5, 9) => true,
        (0, 5, 9) => match suffix {
            None => true,
            Some(suffix) => suffix
                .strip_prefix("dev")
                .and_then(|timestamp| timestamp.parse::<u64>().ok())
                .is_some_and(|timestamp| timestamp >= FIRST_WORKING_DEV_BUILD),
        },
        _ => false,
    }
}

fn validate_mount_check_url(url: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(url).context("invalid media URL")?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        anyhow::bail!("media URL must use HTTP or HTTPS and include a host");
    }
    Ok(())
}

fn reject_not_connected(command: Command) {
    fn not_connected<T>() -> Result<T> {
        Err(CodedError::new(codes::NOT_CONNECTED, "JetKVM is not connected").into())
    }
    match command {
        Command::Connect(response) => {
            let _ = response.send(not_connected());
        }
        Command::Disconnect(response)
        | Command::Unmount(_, response)
        | Command::DeleteStorage { response, .. } => {
            let _ = response.send(not_connected());
        }
        Command::Key(_, response)
        | Command::TypeText(_, response)
        | Command::AbsoluteMouse(_, response)
        | Command::RelativeMouse(_, response)
        | Command::Scroll(_, response) => {
            let _ = response.send(not_connected());
        }
        Command::Snapshot { response, .. } | Command::SnapshotTo { response, .. } => {
            let _ = response.send(not_connected());
        }
        Command::MediaState(response) => {
            let _ = response.send(not_connected());
        }
        Command::CheckMountUrl { response, .. } => {
            let _ = response.send(not_connected());
        }
        Command::MountUrl { response, .. }
        | Command::MountLocal { response, .. }
        | Command::MountStorage { response, .. } => {
            let _ = response.send(not_connected());
        }
        Command::StorageSpace(response) => {
            let _ = response.send(not_connected());
        }
        Command::StorageFiles(response) => {
            let _ = response.send(not_connected());
        }
        Command::Upload { response, .. } => {
            let _ = response.send(not_connected());
        }
    }
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
    use std::collections::VecDeque;
    use std::sync::Mutex as StdMutex;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn test_config() -> ConnectionConfig {
        ConnectionConfig {
            host: "http://jetkvm.invalid".to_owned(),
            password: "test-password".to_owned(),
            no_tls_verify: false,
            pli_interval: Duration::from_secs(3),
            reconnect_min: Duration::from_millis(50),
            reconnect_max: Duration::from_millis(200),
        }
    }

    enum Script {
        Pending,
        Fail(String),
        FailAfter(Duration, String),
    }

    struct ScriptedConnector {
        scripts: StdMutex<VecDeque<Script>>,
        attempts: Arc<AtomicU64>,
    }

    impl ScriptedConnector {
        fn new(scripts: Vec<Script>) -> (Arc<Self>, Arc<AtomicU64>) {
            let attempts = Arc::new(AtomicU64::new(0));
            let connector = Arc::new(Self {
                scripts: StdMutex::new(scripts.into()),
                attempts: Arc::clone(&attempts),
            });
            (connector, attempts)
        }
    }

    impl Connector for ScriptedConnector {
        fn connect(&self, _generation: u64, _nal_tx: broadcast::Sender<NalUnit>) -> ConnectFuture {
            self.attempts.fetch_add(1, Ordering::SeqCst);
            let script = self
                .scripts
                .lock()
                .expect("scripts lock")
                .pop_front()
                .unwrap_or(Script::Pending);
            Box::pin(async move {
                match script {
                    Script::Pending => std::future::pending().await,
                    Script::Fail(message) => Err(anyhow!(message)),
                    Script::FailAfter(delay, message) => {
                        tokio::time::sleep(delay).await;
                        Err(anyhow!(message))
                    }
                }
            })
        }
    }

    async fn wait_for_state(
        controller: &JetKvmController,
        state: ConnectionState,
    ) -> ControllerStatus {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let status = controller.status().await.expect("status succeeds");
                if status.state == state {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("state reached within deadline")
    }

    async fn wait_for_attempts(attempts: &AtomicU64, expected: u64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while attempts.load(Ordering::SeqCst) < expected {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("attempt count reached within deadline");
    }

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
    fn offline_status_has_no_stale_generation_data() {
        let status = ControllerStatus {
            connected: false,
            state: ConnectionState::Disconnected,
            generation: 4,
            device_version: None,
            signaling: None,
            device_capabilities: DeviceCapabilities {
                check_mount_url: None,
            },
            frame: None,
            hid: None,
            stale_controller_mount: false,
            last_error: Some("boom".to_owned()),
        };
        assert!(!status.connected);
        assert_eq!(status.generation, 4);
        assert!(status.frame.is_none());
        assert!(status.hid.is_none());
        assert_eq!(status.last_error.as_deref(), Some("boom"));
    }

    #[test]
    fn check_mount_url_capability_follows_firmware_build() {
        assert!(!supports_check_mount_url(None));
        assert!(!supports_check_mount_url(Some("0.5.8")));
        assert!(!supports_check_mount_url(Some("0.5.9-dev202606220836")));
        assert!(supports_check_mount_url(Some("0.5.9-dev202606301105")));
        assert!(supports_check_mount_url(Some("0.5.9")));
        assert!(supports_check_mount_url(Some("0.6.0")));
        assert!(!supports_check_mount_url(Some("unknown")));
    }

    #[test]
    fn sanitize_error_strips_password_and_credentials() {
        let error = anyhow!("authentication failed for test-password");
        let message = sanitize_error(&error, "test-password");
        assert!(!message.contains("test-password"));
        assert!(message.contains("<redacted>"));
    }

    #[tokio::test]
    async fn starts_and_serves_status_while_device_is_unavailable() {
        let (connector, attempts) = ScriptedConnector::new(vec![Script::Pending]);
        let controller = JetKvmController::spawn(test_config(), connector)
            .await
            .expect("controller starts without a remote session");
        let status = tokio::time::timeout(Duration::from_secs(2), controller.status())
            .await
            .expect("status is prompt")
            .expect("status succeeds");
        assert_eq!(status.state, ConnectionState::Connecting);
        assert!(!status.connected);
        assert_eq!(status.device_capabilities.check_mount_url, None);
        wait_for_attempts(&attempts, 1).await;
        controller.shutdown().await.expect("clean shutdown");
    }
    #[tokio::test]
    async fn status_bypasses_a_pending_controller_request() {
        let (connector, _attempts) = ScriptedConnector::new(vec![Script::Pending]);
        let controller = JetKvmController::spawn(test_config(), connector)
            .await
            .expect("controller starts");
        let reconnect_controller = controller.clone();
        let reconnect = tokio::spawn(async move { reconnect_controller.reconnect().await });
        tokio::task::yield_now().await;

        let status = tokio::time::timeout(Duration::from_millis(100), controller.status())
            .await
            .expect("status stays prompt while reconnect is pending")
            .expect("status succeeds");
        assert_eq!(status.state, ConnectionState::Connecting);

        controller.shutdown().await.expect("clean shutdown");
        reconnect
            .await
            .expect("reconnect task joins")
            .expect_err("pending reconnect is interrupted by shutdown");
    }

    #[tokio::test]
    async fn failed_attempt_enters_backoff_with_sanitized_last_error() {
        let (connector, _attempts) = ScriptedConnector::new(vec![Script::Fail(
            "authentication failed for test-password".to_owned(),
        )]);
        let controller = JetKvmController::spawn(test_config(), connector)
            .await
            .expect("controller starts");
        let status = wait_for_state(&controller, ConnectionState::Reconnecting).await;
        let last_error = status.last_error.expect("last error is reported");
        assert!(!last_error.contains("test-password"));
        assert!(last_error.contains("<redacted>"));
        controller.shutdown().await.expect("clean shutdown");
    }

    #[tokio::test]
    async fn disconnect_cancels_attempt_and_prevents_retry() {
        let (connector, attempts) = ScriptedConnector::new(vec![Script::Pending]);
        let controller = JetKvmController::spawn(test_config(), connector)
            .await
            .expect("controller starts");
        wait_for_attempts(&attempts, 1).await;
        controller.disconnect().await.expect("disconnect succeeds");
        let status = controller.status().await.expect("status succeeds");
        assert_eq!(status.state, ConnectionState::Disconnected);
        assert!(status.last_error.is_none());
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(
            attempts.load(Ordering::SeqCst),
            1,
            "no retry after explicit disconnect"
        );
        controller.shutdown().await.expect("clean shutdown");
    }

    #[tokio::test]
    async fn connect_from_disconnected_starts_attempt_immediately() {
        let (connector, attempts) = ScriptedConnector::new(vec![
            Script::Pending,
            Script::Fail("still offline".to_owned()),
        ]);
        let controller = JetKvmController::spawn(test_config(), connector)
            .await
            .expect("controller starts");
        let mut events = controller.subscribe_events();
        wait_for_attempts(&attempts, 1).await;
        controller.disconnect().await.expect("disconnect succeeds");
        let result = tokio::time::timeout(Duration::from_secs(2), controller.reconnect())
            .await
            .expect("connect resolves promptly");
        let error = result.expect_err("scripted attempt fails");
        assert!(error.to_string().contains("still offline"));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        let mut saw_connecting = false;
        while let Ok(event) = events.try_recv() {
            if let ControllerEvent::ConnectionState {
                state: ConnectionState::Connecting,
                generation,
                ..
            } = event
            {
                assert_eq!(generation, 2);
                saw_connecting = true;
            }
        }
        assert!(saw_connecting, "manual connect emitted a Connecting event");
        controller.shutdown().await.expect("clean shutdown");
    }

    #[tokio::test]
    async fn stale_attempt_failure_does_not_disturb_disconnect() {
        let (connector, attempts) = ScriptedConnector::new(vec![Script::FailAfter(
            Duration::from_millis(200),
            "late failure".to_owned(),
        )]);
        let controller = JetKvmController::spawn(test_config(), connector)
            .await
            .expect("controller starts");
        wait_for_attempts(&attempts, 1).await;
        controller.disconnect().await.expect("disconnect succeeds");
        tokio::time::sleep(Duration::from_millis(500)).await;
        let status = controller.status().await.expect("status succeeds");
        assert_eq!(status.state, ConnectionState::Disconnected);
        assert!(status.last_error.is_none());
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        controller.shutdown().await.expect("clean shutdown");
    }

    #[tokio::test]
    async fn shutdown_interrupts_in_flight_attempt() {
        let (connector, attempts) = ScriptedConnector::new(vec![Script::Pending]);
        let controller = JetKvmController::spawn(test_config(), connector)
            .await
            .expect("controller starts");
        wait_for_attempts(&attempts, 1).await;
        tokio::time::timeout(Duration::from_secs(3), controller.shutdown())
            .await
            .expect("shutdown completes within deadline")
            .expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn shutdown_interrupts_reconnect_backoff() {
        let (connector, _attempts) =
            ScriptedConnector::new(vec![Script::Fail("offline".to_owned())]);
        let controller = JetKvmController::spawn(test_config(), connector)
            .await
            .expect("controller starts");
        wait_for_state(&controller, ConnectionState::Reconnecting).await;
        tokio::time::timeout(Duration::from_secs(3), controller.shutdown())
            .await
            .expect("shutdown completes within deadline")
            .expect("shutdown succeeds");
    }

    #[tokio::test]
    async fn generation_increments_once_per_attempt() {
        let (connector, attempts) = ScriptedConnector::new(vec![
            Script::Fail("one".to_owned()),
            Script::Fail("two".to_owned()),
            Script::Fail("three".to_owned()),
            Script::Pending,
        ]);
        let controller = JetKvmController::spawn(test_config(), connector)
            .await
            .expect("controller starts");
        let mut events = controller.subscribe_events();
        wait_for_attempts(&attempts, 4).await;
        // Let the actor publish the fourth Connecting event.
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut connecting_generations = Vec::new();
        while let Ok(event) = events.try_recv() {
            if let ControllerEvent::ConnectionState {
                state: ConnectionState::Connecting,
                generation,
                ..
            } = event
            {
                connecting_generations.push(generation);
            }
        }
        // The first event can precede the subscription; what matters is one
        // generation per attempt, consecutive, ending at the fourth attempt.
        let first = *connecting_generations
            .first()
            .expect("connecting events observed");
        let expected: Vec<u64> = (first..=4).collect();
        assert_eq!(connecting_generations, expected);
        assert!(connecting_generations.len() >= 3);
        controller.shutdown().await.expect("clean shutdown");
    }
}
