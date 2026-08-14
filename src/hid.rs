use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex as AsyncMutex, watch};
use tokio_util::sync::CancellationToken;
use webrtc::data_channel::RTCDataChannel;
use webrtc::data_channel::data_channel_message::DataChannelMessage;

use crate::keyboard::MacroStep;

pub const PROTOCOL_VERSION: u8 = 1;
const TYPE_HANDSHAKE: u8 = 0x01;
const TYPE_KEYBOARD_REPORT: u8 = 0x02;
const TYPE_POINTER_REPORT: u8 = 0x03;
const TYPE_KEYPRESS_REPORT: u8 = 0x05;
const TYPE_MOUSE_REPORT: u8 = 0x06;
const TYPE_KEYBOARD_MACRO_REPORT: u8 = 0x07;
const TYPE_CANCEL_KEYBOARD_MACRO: u8 = 0x08;
const TYPE_KEYPRESS_KEEPALIVE: u8 = 0x09;
const TYPE_KEYBOARD_LED_STATE: u8 = 0x32;
const TYPE_KEYS_DOWN_STATE: u8 = 0x33;
const TYPE_KEYBOARD_MACRO_STATE: u8 = 0x34;
const KEY_SLOTS: usize = 6;
const KEEPALIVE_INTERVAL: Duration = Duration::from_millis(50);
const MACRO_COMPLETION_MARGIN: Duration = Duration::from_secs(1);
const MACRO_COMPLETION_MAX: Duration = Duration::from_secs(120);
const HID_ABSOLUTE_MAX: i32 = 32_767;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct KeyEvent {
    pub usage: u8,
    pub pressed: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct AbsoluteMouseEvent {
    pub x: i64,
    pub y: i64,
    pub width: u32,
    pub height: u32,
    #[serde(default)]
    pub buttons: u8,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct RelativeMouseEvent {
    pub dx: i16,
    pub dy: i16,
    #[serde(default)]
    pub buttons: u8,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct HidStatus {
    pub ready: bool,
    pub protocol_version: Option<u8>,
    pub keyboard_leds: u8,
    pub held_key_count: usize,
    pub local_held_key_count: usize,
    pub local_non_modifier_key_count: usize,
    pub observed_held_key_count: usize,
    pub observed_modifier_mask: u8,
    pub mouse_buttons: u8,
}

#[derive(Debug, Default)]
struct HidState {
    local_held_keys: BTreeSet<u8>,
    observed_held_keys: BTreeSet<u8>,
    observed_modifiers: u8,
    mouse_buttons: u8,
    last_x: i32,
    last_y: i32,
}

impl HidState {
    fn update_local_key(&mut self, event: KeyEvent) -> bool {
        let needed_keepalive = self.needs_keepalive();
        if event.pressed {
            self.local_held_keys.insert(event.usage);
        } else {
            self.local_held_keys.remove(&event.usage);
        }
        needed_keepalive != self.needs_keepalive()
    }

    fn observe_keys_down(&mut self, payload: &[u8]) {
        self.observed_modifiers = payload[0];
        self.observed_held_keys.clear();
        self.observed_held_keys.extend(
            payload[1..]
                .iter()
                .take(KEY_SLOTS)
                .copied()
                .filter(|key| *key != 0),
        );
    }

    fn clear_input(&mut self) -> bool {
        let needed_keepalive = self.needs_keepalive();
        self.local_held_keys.clear();
        self.observed_held_keys.clear();
        self.observed_modifiers = 0;
        self.mouse_buttons = 0;
        needed_keepalive
    }

    fn needs_keepalive(&self) -> bool {
        self.local_held_keys
            .iter()
            .any(|key| !is_modifier_usage(*key))
    }
}

#[derive(Debug, Clone, Copy, Default)]
struct MacroState {
    started_sequence: u64,
    completed_sequence: u64,
    active: bool,
}

#[derive(Debug)]
struct MacroTracker {
    state: Mutex<MacroState>,
    updates: watch::Sender<MacroState>,
    lifecycle: Mutex<CancellationToken>,
}

#[derive(Debug)]
struct MacroTicket {
    expected_sequence: u64,
    lifecycle: CancellationToken,
}

impl MacroTracker {
    fn new() -> Self {
        let (updates, _) = watch::channel(MacroState::default());
        Self {
            state: Mutex::new(MacroState::default()),
            updates,
            lifecycle: Mutex::new(CancellationToken::new()),
        }
    }

    fn lifecycle(&self) -> CancellationToken {
        self.lifecycle.lock().clone()
    }

    fn arm(
        &self,
        lifecycle: CancellationToken,
    ) -> Result<(MacroTicket, watch::Receiver<MacroState>)> {
        if lifecycle.is_cancelled() {
            bail!("keyboard macro cancelled by reset");
        }
        let expected_sequence = self.state.lock().started_sequence.saturating_add(1);
        Ok((
            MacroTicket {
                expected_sequence,
                lifecycle,
            },
            self.updates.subscribe(),
        ))
    }

    fn observe(&self, active: bool) {
        let mut state = self.state.lock();
        let changed = if active && !state.active {
            state.started_sequence = state.started_sequence.saturating_add(1);
            state.active = true;
            true
        } else if !active && state.active {
            state.completed_sequence = state.started_sequence;
            state.active = false;
            true
        } else {
            false
        };
        if changed {
            self.updates.send_replace(*state);
        }
    }

    fn cancel(&self) {
        let mut lifecycle = self.lifecycle.lock();
        lifecycle.cancel();
        *lifecycle = CancellationToken::new();

        let mut state = self.state.lock();
        if state.active {
            state.active = false;
            self.updates.send_replace(*state);
        }
    }
}

fn is_modifier_usage(usage: u8) -> bool {
    (0xe0..=0xe7).contains(&usage)
}

#[derive(Clone)]
pub(crate) struct HidClient {
    reliable: Arc<RTCDataChannel>,
    unreliable_ordered: Arc<RTCDataChannel>,
    unreliable_unordered: Arc<RTCDataChannel>,
    ready: Arc<AtomicBool>,
    version: Arc<AtomicU8>,
    keyboard_leds: Arc<AtomicU8>,
    state: Arc<Mutex<HidState>>,
    macro_tracker: Arc<MacroTracker>,
    macro_serialization: Arc<AsyncMutex<()>>,
    keepalive_active: watch::Sender<bool>,
}

impl HidClient {
    pub fn new(
        reliable: Arc<RTCDataChannel>,
        unreliable_ordered: Arc<RTCDataChannel>,
        unreliable_unordered: Arc<RTCDataChannel>,
    ) -> Self {
        let (keepalive_active, _) = watch::channel(false);
        let client = Self {
            reliable,
            unreliable_ordered,
            unreliable_unordered,
            ready: Arc::new(AtomicBool::new(false)),
            version: Arc::new(AtomicU8::new(0)),
            keyboard_leds: Arc::new(AtomicU8::new(0)),
            state: Arc::new(Mutex::new(HidState::default())),
            macro_tracker: Arc::new(MacroTracker::new()),
            macro_serialization: Arc::new(AsyncMutex::new(())),
            keepalive_active,
        };
        client.install_handlers();
        client
    }

    fn install_handlers(&self) {
        let channel = Arc::clone(&self.reliable);
        let ready = Arc::clone(&self.ready);
        self.reliable.on_open(Box::new(move || {
            let channel = Arc::clone(&channel);
            let ready = Arc::clone(&ready);
            Box::pin(async move {
                ready.store(false, Ordering::Release);
                let _ = channel
                    .send(&Bytes::from_static(&[TYPE_HANDSHAKE, PROTOCOL_VERSION]))
                    .await;
            })
        }));

        let ready = Arc::clone(&self.ready);
        let version = Arc::clone(&self.version);
        let leds = Arc::clone(&self.keyboard_leds);
        let state = Arc::clone(&self.state);
        let macro_tracker = Arc::clone(&self.macro_tracker);
        self.reliable
            .on_message(Box::new(move |message: DataChannelMessage| {
                let ready = Arc::clone(&ready);
                let version = Arc::clone(&version);
                let leds = Arc::clone(&leds);
                let state = Arc::clone(&state);
                let macro_tracker = Arc::clone(&macro_tracker);
                Box::pin(async move {
                    let data = message.data;
                    let Some((&message_type, payload)) = data.split_first() else {
                        return;
                    };
                    match message_type {
                        TYPE_HANDSHAKE
                            if payload
                                .first()
                                .copied()
                                .is_some_and(|v| v > 0 && v <= PROTOCOL_VERSION) =>
                        {
                            version.store(payload[0], Ordering::Release);
                            ready.store(true, Ordering::Release);
                        }
                        TYPE_KEYBOARD_LED_STATE if !payload.is_empty() => {
                            leds.store(payload[0], Ordering::Release);
                        }
                        TYPE_KEYS_DOWN_STATE if !payload.is_empty() => {
                            state.lock().observe_keys_down(payload);
                        }
                        TYPE_KEYBOARD_MACRO_STATE if payload.len() == 2 => {
                            macro_tracker.observe(payload[0] == 1);
                        }
                        _ => {}
                    }
                })
            }));

        let ready = Arc::clone(&self.ready);
        let version = Arc::clone(&self.version);
        let leds = Arc::clone(&self.keyboard_leds);
        let state = Arc::clone(&self.state);
        let macro_tracker = Arc::clone(&self.macro_tracker);
        let keepalive_active = self.keepalive_active.clone();
        self.reliable.on_close(Box::new(move || {
            ready.store(false, Ordering::Release);
            version.store(0, Ordering::Release);
            leds.store(0, Ordering::Release);
            if state.lock().clear_input() {
                keepalive_active.send_replace(false);
            }
            macro_tracker.cancel();
            Box::pin(async {})
        }));
    }

    pub async fn wait_ready(&self, timeout: Duration) -> Result<()> {
        tokio::time::timeout(timeout, async {
            while !self.ready.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        })
        .await
        .context("HID handshake timed out")?;
        Ok(())
    }

    pub fn status(&self) -> HidStatus {
        let state = self.state.lock();
        let version = self.version.load(Ordering::Acquire);
        let local_held_key_count = state.local_held_keys.len();
        HidStatus {
            ready: self.ready.load(Ordering::Acquire),
            protocol_version: (version != 0).then_some(version),
            keyboard_leds: self.keyboard_leds.load(Ordering::Acquire),
            held_key_count: local_held_key_count,
            local_held_key_count,
            local_non_modifier_key_count: state
                .local_held_keys
                .iter()
                .filter(|key| !is_modifier_usage(**key))
                .count(),
            observed_held_key_count: state.observed_held_keys.len(),
            observed_modifier_mask: state.observed_modifiers,
            mouse_buttons: state.mouse_buttons,
        }
    }

    fn ensure_ready(&self) -> Result<()> {
        if !self.ready.load(Ordering::Acquire) {
            bail!("HID channel is not ready");
        }
        Ok(())
    }

    pub async fn key(&self, event: KeyEvent) -> Result<()> {
        if let Err(error) = self.ensure_ready() {
            self.connection_lost();
            return Err(error);
        }
        let message = Bytes::from(vec![
            TYPE_KEYPRESS_REPORT,
            event.usage,
            u8::from(event.pressed),
        ]);
        if let Err(error) = self
            .reliable
            .send(&message)
            .await
            .context("failed to send key event")
        {
            return self.handle_send_failure(error).await;
        }
        self.update_local_key(event);
        Ok(())
    }

    pub async fn type_macro(&self, steps: &[MacroStep], is_paste: bool) -> Result<()> {
        let message = marshal_keyboard_macro(steps, is_paste)?;
        let lifecycle = self.macro_tracker.lifecycle();
        let _serialization = tokio::select! {
            guard = self.macro_serialization.lock() => guard,
            _ = lifecycle.cancelled() => bail!("keyboard macro cancelled by reset"),
        };
        let (ticket, mut macro_updates) = self.macro_tracker.arm(lifecycle)?;
        if let Err(error) = self.ensure_ready() {
            self.connection_lost();
            return Err(error);
        }
        if let Err(error) = self
            .reliable
            .send(&Bytes::from(message))
            .await
            .context("failed to send keyboard macro")
        {
            return self.handle_send_failure(error).await;
        }
        let timeout = macro_completion_timeout(steps);
        if let Err(error) = wait_for_macro_completion(&mut macro_updates, ticket, timeout).await {
            let _ = self.reset().await;
            return Err(error);
        }
        Ok(())
    }

    pub async fn absolute_mouse(&self, event: AbsoluteMouseEvent) -> Result<()> {
        if let Err(error) = self.ensure_ready() {
            self.connection_lost();
            return Err(error);
        }
        let (x, y) = map_absolute_coordinates(event.x, event.y, event.width, event.height);
        let buttons_changed = {
            let state = self.state.lock();
            state.mouse_buttons != event.buttons
        };
        let message = marshal_pointer_report(x, y, event.buttons);
        let channel = if buttons_changed {
            &self.reliable
        } else {
            &self.unreliable_ordered
        };
        if let Err(error) = channel
            .send(&Bytes::from(message))
            .await
            .context("failed to send absolute mouse event")
        {
            return self.handle_send_failure(error).await;
        }

        let mut state = self.state.lock();
        state.mouse_buttons = event.buttons;
        state.last_x = x;
        state.last_y = y;
        Ok(())
    }

    pub async fn relative_mouse(&self, event: RelativeMouseEvent) -> Result<()> {
        if let Err(error) = self.ensure_ready() {
            self.connection_lost();
            return Err(error);
        }
        let buttons_changed = {
            let state = self.state.lock();
            state.mouse_buttons != event.buttons
        };
        let dx = event.dx.clamp(i8::MIN as i16, i8::MAX as i16) as i8;
        let dy = event.dy.clamp(i8::MIN as i16, i8::MAX as i16) as i8;
        let message = Bytes::from(vec![TYPE_MOUSE_REPORT, dx as u8, dy as u8, event.buttons]);
        let channel = if buttons_changed {
            &self.reliable
        } else {
            &self.unreliable_unordered
        };
        if let Err(error) = channel
            .send(&message)
            .await
            .context("failed to send relative mouse event")
        {
            return self.handle_send_failure(error).await;
        }
        self.state.lock().mouse_buttons = event.buttons;
        Ok(())
    }

    pub async fn reset(&self) -> Result<()> {
        self.macro_tracker.cancel();
        let (x, y) = self.clear_input_state();
        if self.reliable.ready_state()
            != webrtc::data_channel::data_channel_state::RTCDataChannelState::Open
        {
            return Ok(());
        }

        let mut failure = None;
        if let Err(error) = self
            .reliable
            .send(&Bytes::from_static(&[TYPE_CANCEL_KEYBOARD_MACRO]))
            .await
            .context("failed to cancel keyboard macro")
        {
            failure = Some(error);
        }
        let mut keyboard = vec![TYPE_KEYBOARD_REPORT, 0];
        keyboard.extend_from_slice(&[0; KEY_SLOTS]);
        if let Err(error) = self
            .reliable
            .send(&Bytes::from(keyboard))
            .await
            .context("failed to reset keyboard state")
        {
            if failure.is_none() {
                failure = Some(error);
            }
        }
        if let Err(error) = self
            .reliable
            .send(&Bytes::from(marshal_pointer_report(x, y, 0)))
            .await
            .context("failed to release mouse buttons")
        {
            if failure.is_none() {
                failure = Some(error);
            }
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    pub fn connection_lost(&self) {
        self.macro_tracker.cancel();
        self.ready.store(false, Ordering::Release);
        self.version.store(0, Ordering::Release);
        self.keyboard_leds.store(0, Ordering::Release);
        self.clear_input_state();
    }

    fn update_local_key(&self, event: KeyEvent) {
        let mut state = self.state.lock();
        if state.update_local_key(event) {
            self.keepalive_active.send_replace(state.needs_keepalive());
        }
    }

    fn clear_input_state(&self) -> (i32, i32) {
        let mut state = self.state.lock();
        let position = (state.last_x, state.last_y);
        let keepalive_was_needed = state.clear_input();
        if keepalive_was_needed {
            self.keepalive_active.send_replace(false);
        }
        position
    }

    async fn handle_send_failure(&self, error: anyhow::Error) -> Result<()> {
        let _ = self.reset().await;
        Err(error)
    }

    pub fn start_keepalive(&self, cancellation: CancellationToken) -> tokio::task::JoinHandle<()> {
        let client = self.clone();
        let mut keepalive_active = client.keepalive_active.subscribe();
        tokio::spawn(async move {
            while wait_for_keepalive(&mut keepalive_active, &cancellation).await {
                if let Err(error) = client
                    .reliable
                    .send(&Bytes::from_static(&[TYPE_KEYPRESS_KEEPALIVE]))
                    .await
                    .context("failed to send keypress keepalive")
                {
                    let _ = client.handle_send_failure(error).await;
                    return;
                }
            }
            let _ = client.reset().await;
        })
    }
}

fn macro_completion_timeout(steps: &[MacroStep]) -> Duration {
    steps
        .iter()
        .fold(Duration::ZERO, |total, step| {
            total.saturating_add(Duration::from_millis(u64::from(step.delay_ms)))
        })
        .saturating_add(MACRO_COMPLETION_MARGIN)
        .min(MACRO_COMPLETION_MAX)
}

async fn wait_for_macro_completion(
    updates: &mut watch::Receiver<MacroState>,
    ticket: MacroTicket,
    timeout: Duration,
) -> Result<()> {
    let wait = async {
        loop {
            if updates.borrow_and_update().completed_sequence >= ticket.expected_sequence {
                return Ok(());
            }
            tokio::select! {
                _ = ticket.lifecycle.cancelled() => bail!("keyboard macro cancelled by reset"),
                changed = updates.changed() => {
                    if changed.is_err() {
                        bail!("keyboard macro completion tracking stopped");
                    }
                }
            }
        }
    };
    match tokio::time::timeout(timeout, wait).await {
        Ok(result) => result,
        Err(_) => bail!("keyboard macro completion timed out after {timeout:?}"),
    }
}

async fn wait_for_keepalive(
    keepalive_active: &mut watch::Receiver<bool>,
    cancellation: &CancellationToken,
) -> bool {
    loop {
        if !*keepalive_active.borrow() {
            tokio::select! {
                _ = cancellation.cancelled() => return false,
                changed = keepalive_active.changed() => {
                    if changed.is_err() {
                        return false;
                    }
                }
            }
            continue;
        }
        tokio::select! {
            _ = cancellation.cancelled() => return false,
            changed = keepalive_active.changed() => {
                if changed.is_err() {
                    return false;
                }
            }
            _ = tokio::time::sleep(KEEPALIVE_INTERVAL) => {
                if *keepalive_active.borrow() {
                    return true;
                }
            }
        }
    }
}

pub fn map_absolute_coordinates(x: i64, y: i64, width: u32, height: u32) -> (i32, i32) {
    fn map(value: i64, extent: u32) -> i32 {
        if extent <= 1 {
            return 0;
        }
        let max_pixel = i64::from(extent - 1);
        let clamped = value.clamp(0, max_pixel);
        ((clamped * i64::from(HID_ABSOLUTE_MAX) + max_pixel / 2) / max_pixel) as i32
    }
    (map(x, width), map(y, height))
}

fn marshal_pointer_report(x: i32, y: i32, buttons: u8) -> Vec<u8> {
    let mut message = Vec::with_capacity(10);
    message.push(TYPE_POINTER_REPORT);
    message.extend_from_slice(&x.to_be_bytes());
    message.extend_from_slice(&y.to_be_bytes());
    message.push(buttons);
    message
}

pub fn marshal_keyboard_macro(steps: &[MacroStep], is_paste: bool) -> Result<Vec<u8>> {
    let step_count = u32::try_from(steps.len()).context("too many keyboard macro steps")?;
    let mut message = Vec::with_capacity(6 + steps.len() * 9);
    message.push(TYPE_KEYBOARD_MACRO_REPORT);
    message.push(u8::from(is_paste));
    message.extend_from_slice(&step_count.to_be_bytes());
    for step in steps {
        message.push(step.modifier);
        message.extend_from_slice(&step.keys);
        message.extend_from_slice(&step.delay_ms.to_be_bytes());
    }
    Ok(message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_hid_wire_vectors() {
        assert_eq!(
            marshal_pointer_report(0x1020, 0x3040, 3),
            vec![3, 0, 0, 0x10, 0x20, 0, 0, 0x30, 0x40, 3]
        );
        let steps = [MacroStep {
            modifier: 2,
            keys: [4, 0, 0, 0, 0, 0],
            delay_ms: 25,
        }];
        assert_eq!(
            marshal_keyboard_macro(&steps, true).expect("macro should encode"),
            vec![7, 1, 0, 0, 0, 1, 2, 4, 0, 0, 0, 0, 0, 0, 25]
        );
    }

    #[test]
    fn local_intent_and_observed_state_are_independent() {
        let mut state = HidState::default();
        state.update_local_key(KeyEvent {
            usage: 4,
            pressed: true,
        });
        state.observe_keys_down(&[0x02, 5, 0, 0, 0, 0, 0]);

        assert!(state.local_held_keys.contains(&4));
        assert!(state.needs_keepalive());
        assert_eq!(state.observed_modifiers, 0x02);
        assert_eq!(state.observed_held_keys, BTreeSet::from([5]));
    }

    #[test]
    fn modifier_state_neither_triggers_nor_suppresses_keepalive() {
        let mut state = HidState::default();
        state.update_local_key(KeyEvent {
            usage: 0xe1,
            pressed: true,
        });
        state.observe_keys_down(&[0x02, 0, 0, 0, 0, 0, 0]);
        assert!(!state.needs_keepalive());

        state.update_local_key(KeyEvent {
            usage: 4,
            pressed: true,
        });
        state.observe_keys_down(&[0x02, 0, 0, 0, 0, 0, 0]);
        assert!(state.needs_keepalive());

        state.update_local_key(KeyEvent {
            usage: 4,
            pressed: false,
        });
        assert!(!state.needs_keepalive());
    }

    #[test]
    fn reset_state_clears_local_and_observed_input() {
        let mut state = HidState::default();
        state.update_local_key(KeyEvent {
            usage: 4,
            pressed: true,
        });
        state.observe_keys_down(&[0x03, 5, 0, 0, 0, 0, 0]);
        state.mouse_buttons = 3;

        assert!(state.clear_input());
        assert!(state.local_held_keys.is_empty());
        assert!(state.observed_held_keys.is_empty());
        assert_eq!(state.observed_modifiers, 0);
        assert_eq!(state.mouse_buttons, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn held_non_modifier_gets_50ms_keepalives_and_release_stops_them() {
        let state = Arc::new(Mutex::new(HidState::default()));
        state.lock().update_local_key(KeyEvent {
            usage: 4,
            pressed: true,
        });
        let (keepalive_active, mut keepalive_changes) = watch::channel(true);
        let cancellation = CancellationToken::new();
        let sent = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let worker = {
            let cancellation = cancellation.clone();
            let sent = Arc::clone(&sent);
            tokio::spawn(async move {
                while wait_for_keepalive(&mut keepalive_changes, &cancellation).await {
                    sent.fetch_add(1, Ordering::Relaxed);
                }
            })
        };

        tokio::task::yield_now().await;
        tokio::time::advance(KEEPALIVE_INTERVAL).await;
        tokio::task::yield_now().await;
        assert_eq!(sent.load(Ordering::Relaxed), 1);
        for _ in 1..10 {
            tokio::time::advance(KEEPALIVE_INTERVAL).await;
            tokio::task::yield_now().await;
        }
        assert!(
            sent.load(Ordering::Relaxed) >= 5,
            "a 500ms hold must not use the former 1s cadence"
        );

        assert!(state.lock().update_local_key(KeyEvent {
            usage: 4,
            pressed: false,
        }));
        keepalive_active.send_replace(false);
        tokio::task::yield_now().await;
        let sent_before_release = sent.load(Ordering::Relaxed);
        for _ in 0..10 {
            tokio::time::advance(KEEPALIVE_INTERVAL).await;
            tokio::task::yield_now().await;
        }
        assert_eq!(sent.load(Ordering::Relaxed), sent_before_release);

        cancellation.cancel();
        worker.await.expect("keepalive worker should exit");
    }

    #[test]
    fn absolute_coordinates_map_boundaries_and_center() {
        assert_eq!(map_absolute_coordinates(-1, -1, 1920, 1080), (0, 0));
        assert_eq!(
            map_absolute_coordinates(1919, 1079, 1920, 1080),
            (32767, 32767)
        );
        let (x, y) = map_absolute_coordinates(960, 540, 1921, 1081);
        assert_eq!((x, y), (16384, 16384));
        assert_eq!(map_absolute_coordinates(999, 999, 1, 1), (0, 0));
    }

    #[tokio::test]
    async fn macro_completion_requires_a_new_start_and_matching_completion() {
        let tracker = Arc::new(MacroTracker::new());
        tracker.observe(true);
        tracker.observe(false);

        let lifecycle = tracker.lifecycle();
        let (ticket, mut updates) = tracker.arm(lifecycle).expect("macro should arm");
        let waiter = {
            let tracker = Arc::clone(&tracker);
            tokio::spawn(async move {
                let result =
                    wait_for_macro_completion(&mut updates, ticket, Duration::from_secs(10)).await;
                (tracker, result)
            })
        };

        tokio::task::yield_now().await;
        tracker.observe(false);
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "a stale completion must not finish the new macro"
        );

        tracker.observe(true);
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "macro completion requires the completion transition"
        );
        tracker.observe(false);
        let (_, result) = waiter.await.expect("macro waiter should not panic");
        result.expect("matching completion should finish the macro");
    }

    #[tokio::test]
    async fn macro_after_active_cancellation_observes_a_fresh_transition() {
        let tracker = Arc::new(MacroTracker::new());
        let old_lifecycle = tracker.lifecycle();
        let (_old_ticket, _old_updates) = tracker.arm(old_lifecycle).expect("old macro should arm");
        tracker.observe(true);

        tracker.cancel();
        assert!(!tracker.state.lock().active);

        let lifecycle = tracker.lifecycle();
        let (ticket, mut updates) = tracker.arm(lifecycle).expect("new macro should arm");
        let waiter = tokio::spawn(async move {
            wait_for_macro_completion(&mut updates, ticket, Duration::from_secs(10)).await
        });

        tracker.observe(false);
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "the cancelled macro's completion must not finish the new macro"
        );

        tracker.observe(true);
        tracker.observe(false);
        waiter
            .await
            .expect("macro waiter should not panic")
            .expect("fresh transition should finish the new macro");
    }

    #[tokio::test(start_paused = true)]
    async fn macro_completion_times_out_without_completion() {
        let tracker = MacroTracker::new();
        let lifecycle = tracker.lifecycle();
        let (ticket, mut updates) = tracker.arm(lifecycle).expect("macro should arm");
        let waiter = tokio::spawn(async move {
            wait_for_macro_completion(&mut updates, ticket, Duration::from_secs(2)).await
        });

        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_secs(2)).await;
        let error = waiter
            .await
            .expect("macro waiter should not panic")
            .expect_err("macro should time out without completion");
        assert!(
            error
                .to_string()
                .contains("keyboard macro completion timed out")
        );
    }

    #[tokio::test]
    async fn macro_cancellation_wakes_active_and_queued_waiters() {
        let tracker = Arc::new(MacroTracker::new());
        let lifecycle = tracker.lifecycle();
        let (ticket, mut updates) = tracker.arm(lifecycle).expect("macro should arm");
        let waiter = tokio::spawn(async move {
            wait_for_macro_completion(&mut updates, ticket, Duration::from_secs(10)).await
        });

        tokio::task::yield_now().await;
        tracker.cancel();
        let error = waiter
            .await
            .expect("macro waiter should not panic")
            .expect_err("cancelled macro should fail");
        assert!(
            error
                .to_string()
                .contains("keyboard macro cancelled by reset")
        );

        let lifecycle = tracker.lifecycle();
        let serialization = Arc::new(AsyncMutex::new(()));
        let first = serialization.lock().await;
        let queued = {
            let serialization = Arc::clone(&serialization);
            tokio::spawn(async move {
                let _guard = tokio::select! {
                    guard = serialization.lock() => guard,
                    _ = lifecycle.cancelled() => bail!("keyboard macro cancelled by reset"),
                };
                if lifecycle.is_cancelled() {
                    bail!("keyboard macro cancelled by reset");
                }
                Ok(())
            })
        };
        tokio::task::yield_now().await;
        tracker.cancel();
        drop(first);
        let error = queued
            .await
            .expect("queued macro should not panic")
            .expect_err("queued macro should be cancelled");
        assert!(
            error
                .to_string()
                .contains("keyboard macro cancelled by reset")
        );
    }

    #[test]
    fn macro_timeout_covers_every_accepted_encoded_delay() {
        assert_eq!(
            macro_completion_timeout(&[MacroStep {
                modifier: 0,
                keys: [0; KEY_SLOTS],
                delay_ms: 250,
            }]),
            Duration::from_millis(1_250)
        );
        let maximum_text = "a".repeat(4096);
        let maximum_steps =
            crate::keyboard::text_to_macro(&maximum_text).expect("maximum text is accepted");
        assert_eq!(
            macro_completion_timeout(&maximum_steps),
            Duration::from_millis(99_304)
        );
        assert!(macro_completion_timeout(&maximum_steps) < MACRO_COMPLETION_MAX);
        assert_eq!(
            macro_completion_timeout(&vec![
                MacroStep {
                    modifier: 0,
                    keys: [0; KEY_SLOTS],
                    delay_ms: u16::MAX,
                };
                10_000
            ]),
            MACRO_COMPLETION_MAX
        );
    }

    #[tokio::test]
    async fn type_macro_waits_for_its_transition_and_serializes_wire_messages() {
        let (reliable, remote, offer_peer, answer_peer) = connected_hid_channel_pair().await;
        let client = HidClient::new(
            Arc::clone(&reliable),
            Arc::clone(&reliable),
            Arc::clone(&reliable),
        );
        let (sent_tx, mut sent_rx) = tokio::sync::mpsc::channel(8);
        remote.on_message(Box::new(move |message| {
            let sent_tx = sent_tx.clone();
            Box::pin(async move {
                let _ = sent_tx.send(message.data).await;
            })
        }));
        remote
            .send(&Bytes::from_static(&[TYPE_HANDSHAKE, PROTOCOL_VERSION]))
            .await
            .expect("remote handshake should send");
        client
            .wait_ready(Duration::from_secs(2))
            .await
            .expect("client should become ready");

        remote
            .send(&Bytes::from_static(&[TYPE_KEYBOARD_MACRO_STATE, 1, 0]))
            .await
            .expect("old macro start should send");
        remote
            .send(&Bytes::from_static(&[TYPE_KEYBOARD_MACRO_STATE, 0, 0]))
            .await
            .expect("old macro completion should send");
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if client.macro_tracker.state.lock().completed_sequence == 1 {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("old transition should be observed before new macro");

        let first = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .type_macro(
                        &[MacroStep {
                            modifier: 0,
                            keys: [4, 0, 0, 0, 0, 0],
                            delay_ms: 1,
                        }],
                        false,
                    )
                    .await
            })
        };
        let first_message = next_macro_message(&mut sent_rx).await;
        assert_eq!(first_message[0], TYPE_KEYBOARD_MACRO_REPORT);

        remote
            .send(&Bytes::from_static(&[TYPE_KEYBOARD_MACRO_STATE, 0, 0]))
            .await
            .expect("stale completion should send");
        tokio::task::yield_now().await;
        assert!(
            !first.is_finished(),
            "a pre-existing completion must not finish a new macro"
        );
        remote
            .send(&Bytes::from_static(&[TYPE_KEYBOARD_MACRO_STATE, 1, 0]))
            .await
            .expect("macro start should send");
        remote
            .send(&Bytes::from_static(&[TYPE_KEYBOARD_MACRO_STATE, 0, 0]))
            .await
            .expect("macro completion should send");
        first
            .await
            .expect("first macro task should not panic")
            .expect("matching macro completion should succeed");

        let second = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .type_macro(
                        &[MacroStep {
                            modifier: 0,
                            keys: [5, 0, 0, 0, 0, 0],
                            delay_ms: 1,
                        }],
                        false,
                    )
                    .await
            })
        };
        let third = {
            let client = client.clone();
            tokio::spawn(async move {
                client
                    .type_macro(
                        &[MacroStep {
                            modifier: 0,
                            keys: [6, 0, 0, 0, 0, 0],
                            delay_ms: 1,
                        }],
                        false,
                    )
                    .await
            })
        };
        let second_message = next_macro_message(&mut sent_rx).await;
        tokio::task::yield_now().await;
        assert!(
            sent_rx.try_recv().is_err(),
            "a second macro must not be sent before the first completes"
        );
        remote
            .send(&Bytes::from_static(&[TYPE_KEYBOARD_MACRO_STATE, 1, 0]))
            .await
            .expect("second macro start should send");
        remote
            .send(&Bytes::from_static(&[TYPE_KEYBOARD_MACRO_STATE, 0, 0]))
            .await
            .expect("second macro completion should send");
        let third_message = next_macro_message(&mut sent_rx).await;
        assert_ne!(
            second_message[7], third_message[7],
            "serialized macros must remain distinct on the wire"
        );
        remote
            .send(&Bytes::from_static(&[TYPE_KEYBOARD_MACRO_STATE, 1, 0]))
            .await
            .expect("third macro start should send");
        remote
            .send(&Bytes::from_static(&[TYPE_KEYBOARD_MACRO_STATE, 0, 0]))
            .await
            .expect("third macro completion should send");
        second
            .await
            .expect("second macro task should not panic")
            .expect("second macro should finish");
        third
            .await
            .expect("third macro task should not panic")
            .expect("third macro should finish");

        offer_peer.close().await.expect("offer peer should close");
        answer_peer.close().await.expect("answer peer should close");
    }

    #[tokio::test]
    async fn status_tracks_local_and_device_observed_changes_without_another_action() {
        let (reliable, remote, offer_peer, answer_peer) = connected_hid_channel_pair().await;
        let client = HidClient::new(
            Arc::clone(&reliable),
            Arc::clone(&reliable),
            Arc::clone(&reliable),
        );
        remote
            .send(&Bytes::from_static(&[TYPE_HANDSHAKE, PROTOCOL_VERSION]))
            .await
            .expect("remote handshake should send");
        client
            .wait_ready(Duration::from_secs(2))
            .await
            .expect("client should become ready");

        client
            .key(KeyEvent {
                usage: 0x68,
                pressed: true,
            })
            .await
            .expect("F13 press should send");
        assert_eq!(client.status().local_held_key_count, 1);
        assert_eq!(client.status().observed_held_key_count, 0);

        remote
            .send(&Bytes::from_static(&[
                TYPE_KEYS_DOWN_STATE,
                0,
                0x68,
                0,
                0,
                0,
                0,
                0,
            ]))
            .await
            .expect("observed press should send");
        wait_for_hid_status(&client, |status| status.observed_held_key_count == 1).await;

        remote
            .send(&Bytes::from_static(&[TYPE_KEYBOARD_LED_STATE, 0x05]))
            .await
            .expect("LED update should send");
        wait_for_hid_status(&client, |status| status.keyboard_leds == 0x05).await;

        client
            .key(KeyEvent {
                usage: 0x68,
                pressed: false,
            })
            .await
            .expect("F13 release should send");
        assert_eq!(client.status().local_held_key_count, 0);
        assert_eq!(client.status().observed_held_key_count, 1);

        remote
            .send(&Bytes::from_static(&[
                TYPE_KEYS_DOWN_STATE,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
            ]))
            .await
            .expect("observed release should send");
        wait_for_hid_status(&client, |status| status.observed_held_key_count == 0).await;

        remote
            .close()
            .await
            .expect("remote HID channel should close");
        wait_for_hid_status(&client, |status| !status.ready).await;
        let status = client.status();
        assert_eq!(status.protocol_version, None);
        assert_eq!(status.keyboard_leds, 0);
        assert_eq!(status.local_held_key_count, 0);
        assert_eq!(status.observed_held_key_count, 0);
        assert_eq!(status.mouse_buttons, 0);

        offer_peer.close().await.expect("offer peer should close");
        answer_peer.close().await.expect("answer peer should close");
    }

    async fn wait_for_hid_status(client: &HidClient, predicate: impl Fn(HidStatus) -> bool) {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if predicate(client.status()) {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("HID status should update within deadline");
    }

    #[tokio::test]
    async fn reset_and_connection_loss_cancel_macro_waiters() {
        let (reliable, remote, offer_peer, answer_peer) = connected_hid_channel_pair().await;
        let client = HidClient::new(
            Arc::clone(&reliable),
            Arc::clone(&reliable),
            Arc::clone(&reliable),
        );
        let (sent_tx, mut sent_rx) = tokio::sync::mpsc::channel(8);
        remote.on_message(Box::new(move |message| {
            let sent_tx = sent_tx.clone();
            Box::pin(async move {
                let _ = sent_tx.send(message.data).await;
            })
        }));
        remote
            .send(&Bytes::from_static(&[TYPE_HANDSHAKE, PROTOCOL_VERSION]))
            .await
            .expect("remote handshake should send");
        client
            .wait_ready(Duration::from_secs(2))
            .await
            .expect("client should become ready");

        let reset_waiter = spawn_macro(&client);
        next_macro_message(&mut sent_rx).await;
        client.reset().await.expect("reset should send");
        let reset_error = reset_waiter
            .await
            .expect("reset waiter should not panic")
            .expect_err("reset should cancel the macro");
        assert!(
            reset_error
                .to_string()
                .contains("keyboard macro cancelled by reset")
        );

        let loss_waiter = spawn_macro(&client);
        next_macro_message(&mut sent_rx).await;
        client.connection_lost();
        let loss_error = loss_waiter
            .await
            .expect("connection-loss waiter should not panic")
            .expect_err("connection loss should cancel the macro");
        assert!(
            loss_error
                .to_string()
                .contains("keyboard macro cancelled by reset")
        );

        offer_peer.close().await.expect("offer peer should close");
        answer_peer.close().await.expect("answer peer should close");
    }

    fn spawn_macro(client: &HidClient) -> tokio::task::JoinHandle<Result<()>> {
        let client = client.clone();
        tokio::spawn(async move {
            client
                .type_macro(
                    &[MacroStep {
                        modifier: 0,
                        keys: [4, 0, 0, 0, 0, 0],
                        delay_ms: 1,
                    }],
                    false,
                )
                .await
        })
    }

    async fn next_macro_message(sent_rx: &mut tokio::sync::mpsc::Receiver<Bytes>) -> Bytes {
        loop {
            let message = tokio::time::timeout(Duration::from_secs(2), sent_rx.recv())
                .await
                .expect("remote should receive a HID message")
                .expect("HID sender should remain alive");
            if message.first() == Some(&TYPE_KEYBOARD_MACRO_REPORT) {
                return message;
            }
        }
    }

    async fn connected_hid_channel_pair() -> (
        Arc<RTCDataChannel>,
        Arc<RTCDataChannel>,
        Arc<webrtc::peer_connection::RTCPeerConnection>,
        Arc<webrtc::peer_connection::RTCPeerConnection>,
    ) {
        let api = webrtc::api::APIBuilder::new().build();
        let offer_peer = Arc::new(
            api.new_peer_connection(
                webrtc::peer_connection::configuration::RTCConfiguration::default(),
            )
            .await
            .expect("offer peer connection"),
        );
        let answer_peer = Arc::new(
            api.new_peer_connection(
                webrtc::peer_connection::configuration::RTCConfiguration::default(),
            )
            .await
            .expect("answer peer connection"),
        );
        let (remote_tx, mut remote_rx) = tokio::sync::mpsc::channel(1);
        answer_peer.on_data_channel(Box::new(move |channel| {
            let remote_tx = remote_tx.clone();
            Box::pin(async move {
                let _ = remote_tx.send(channel).await;
            })
        }));
        let channel = offer_peer
            .create_data_channel("hidrpc", None)
            .await
            .expect("offer data channel");
        let offer = offer_peer.create_offer(None).await.expect("SDP offer");
        let mut offer_gathered = offer_peer.gathering_complete_promise().await;
        offer_peer
            .set_local_description(offer)
            .await
            .expect("offer local description");
        tokio::time::timeout(Duration::from_secs(5), offer_gathered.recv())
            .await
            .expect("offer ICE gathering should finish");
        answer_peer
            .set_remote_description(
                offer_peer
                    .local_description()
                    .await
                    .expect("offer local description should be present"),
            )
            .await
            .expect("answer remote description");
        let answer = answer_peer.create_answer(None).await.expect("SDP answer");
        let mut answer_gathered = answer_peer.gathering_complete_promise().await;
        answer_peer
            .set_local_description(answer)
            .await
            .expect("answer local description");
        tokio::time::timeout(Duration::from_secs(5), answer_gathered.recv())
            .await
            .expect("answer ICE gathering should finish");
        offer_peer
            .set_remote_description(
                answer_peer
                    .local_description()
                    .await
                    .expect("answer local description should be present"),
            )
            .await
            .expect("offer remote description");
        let remote_channel = tokio::time::timeout(Duration::from_secs(5), remote_rx.recv())
            .await
            .expect("remote data channel should arrive")
            .expect("remote sender should remain alive");
        (channel, remote_channel, offer_peer, answer_peer)
    }
}
