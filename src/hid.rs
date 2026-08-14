use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
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
const KEY_SLOTS: usize = 6;
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
    pub mouse_buttons: u8,
}

#[derive(Debug, Default)]
struct HidState {
    held_keys: BTreeSet<u8>,
    mouse_buttons: u8,
    last_x: i32,
    last_y: i32,
}

impl HidState {
    fn update_key(&mut self, event: KeyEvent) {
        if event.pressed {
            self.held_keys.insert(event.usage);
        } else {
            self.held_keys.remove(&event.usage);
        }
    }

    fn clear_input(&mut self) {
        self.held_keys.clear();
        self.mouse_buttons = 0;
    }

    fn needs_keepalive(&self) -> bool {
        !self.held_keys.is_empty()
    }
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
}

impl HidClient {
    pub fn new(
        reliable: Arc<RTCDataChannel>,
        unreliable_ordered: Arc<RTCDataChannel>,
        unreliable_unordered: Arc<RTCDataChannel>,
    ) -> Self {
        let client = Self {
            reliable,
            unreliable_ordered,
            unreliable_unordered,
            ready: Arc::new(AtomicBool::new(false)),
            version: Arc::new(AtomicU8::new(0)),
            keyboard_leds: Arc::new(AtomicU8::new(0)),
            state: Arc::new(Mutex::new(HidState::default())),
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
        self.reliable
            .on_message(Box::new(move |message: DataChannelMessage| {
                let ready = Arc::clone(&ready);
                let version = Arc::clone(&version);
                let leds = Arc::clone(&leds);
                let state = Arc::clone(&state);
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
                            let mut current = state.lock();
                            current.held_keys.clear();
                            current
                                .held_keys
                                .extend(payload[1..].iter().copied().filter(|key| *key != 0));
                        }
                        _ => {}
                    }
                })
            }));

        let ready = Arc::clone(&self.ready);
        self.reliable.on_close(Box::new(move || {
            ready.store(false, Ordering::Release);
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
        HidStatus {
            ready: self.ready.load(Ordering::Acquire),
            protocol_version: (version != 0).then_some(version),
            keyboard_leds: self.keyboard_leds.load(Ordering::Acquire),
            held_key_count: state.held_keys.len(),
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
        self.ensure_ready()?;
        let message = Bytes::from(vec![
            TYPE_KEYPRESS_REPORT,
            event.usage,
            u8::from(event.pressed),
        ]);
        self.reliable
            .send(&message)
            .await
            .context("failed to send key event")?;
        self.state.lock().update_key(event);
        Ok(())
    }

    pub async fn type_macro(&self, steps: &[MacroStep], is_paste: bool) -> Result<()> {
        self.ensure_ready()?;
        let message = marshal_keyboard_macro(steps, is_paste)?;
        self.reliable
            .send(&Bytes::from(message))
            .await
            .context("failed to send keyboard macro")?;
        Ok(())
    }

    pub async fn absolute_mouse(&self, event: AbsoluteMouseEvent) -> Result<()> {
        self.ensure_ready()?;
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
        channel
            .send(&Bytes::from(message))
            .await
            .context("failed to send absolute mouse event")?;

        let mut state = self.state.lock();
        state.mouse_buttons = event.buttons;
        state.last_x = x;
        state.last_y = y;
        Ok(())
    }

    pub async fn relative_mouse(&self, event: RelativeMouseEvent) -> Result<()> {
        self.ensure_ready()?;
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
        channel
            .send(&message)
            .await
            .context("failed to send relative mouse event")?;
        self.state.lock().mouse_buttons = event.buttons;
        Ok(())
    }

    pub async fn reset(&self) -> Result<()> {
        if self.reliable.ready_state()
            != webrtc::data_channel::data_channel_state::RTCDataChannelState::Open
        {
            self.clear_local_state();
            return Ok(());
        }

        let result = async {
            self.reliable
                .send(&Bytes::from_static(&[TYPE_CANCEL_KEYBOARD_MACRO]))
                .await
                .context("failed to cancel keyboard macro")?;
            let mut keyboard = vec![TYPE_KEYBOARD_REPORT, 0];
            keyboard.extend_from_slice(&[0; KEY_SLOTS]);
            self.reliable
                .send(&Bytes::from(keyboard))
                .await
                .context("failed to reset keyboard state")?;

            let (x, y) = {
                let state = self.state.lock();
                (state.last_x, state.last_y)
            };
            self.reliable
                .send(&Bytes::from(marshal_pointer_report(x, y, 0)))
                .await
                .context("failed to release mouse buttons")?;
            Ok(())
        }
        .await;
        self.clear_local_state();
        result
    }

    fn clear_local_state(&self) {
        self.state.lock().clear_input();
    }

    pub fn start_keepalive(&self, cancellation: CancellationToken) -> tokio::task::JoinHandle<()> {
        let client = self.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                tokio::select! {
                    _ = cancellation.cancelled() => return,
                    _ = interval.tick() => {
                        let has_held_keys = client.state.lock().needs_keepalive();
                        if has_held_keys {
                            let _ = client
                                .reliable
                                .send(&Bytes::from_static(&[TYPE_KEYPRESS_KEEPALIVE]))
                                .await;
                        }
                    }
                }
            }
        })
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
    fn key_transitions_drive_keepalive_and_emergency_reset_state() {
        let mut state = HidState::default();
        state.update_key(KeyEvent {
            usage: 4,
            pressed: true,
        });
        state.update_key(KeyEvent {
            usage: 4,
            pressed: true,
        });
        assert_eq!(state.held_keys.len(), 1);
        assert!(state.needs_keepalive());

        state.update_key(KeyEvent {
            usage: 4,
            pressed: false,
        });
        assert!(!state.needs_keepalive());

        state.update_key(KeyEvent {
            usage: 5,
            pressed: true,
        });
        state.mouse_buttons = 3;
        state.clear_input();
        assert!(!state.needs_keepalive());
        assert_eq!(state.mouse_buttons, 0);
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
}
