# recorder-for-jetkvm

Command-line recorder and persistent controller for JetKVM devices.

## Features

- Record screen changes as MP4 clips, with a configurable pre-event buffer.
- Capture PNG screenshots.
- Keep one WebRTC session alive for repeated commands.
- Send keyboard, text, mouse, and device RPC input.
- Mount remote URLs, local images, or uploaded images as virtual media.
- Resume and cancel storage uploads.
- Reconnect automatically while isolating stale frames and RPC responses.

## Install

Rust 1.88 or newer is required.

```bash
cargo install recorder-for-jetkvm
```

Or build from source:

```bash
cargo build --release
```

## Record and capture

Provide the password through `JETKVM_PASSWORD` or `--password-file`:

```bash
export JETKVM_PASSWORD='your-password'

# Record changed regions to MP4 files.
recorder-for-jetkvm --host 192.168.1.130

# Capture one screenshot and exit.
recorder-for-jetkvm --host 192.168.1.130 --screenshot

# Choose output paths.
recorder-for-jetkvm \
  --host 192.168.1.130 \
  --output-dir ./recordings \
  --screenshot-output ./screen.png
```

Recordings default to the OS video directory. Screenshots default to the OS
pictures directory.

## Persistent controller

`serve --stdio` exposes a versioned newline-delimited JSON protocol (currently
**version 2**). The process starts and answers the handshake even while the
JetKVM is offline; connection work continues in the background with
exponential backoff.

```bash
recorder-for-jetkvm serve --stdio \
  --host 192.168.1.130 \
  --password-file ~/.config/jetkvm-password
```

The first request must be `hello`. Its result includes protocol version 2, the
static sidecar method list, firmware warnings, and the current controller
status — no separate `status` round trip is needed. Device-dependent support
is reported separately in `status.device_capabilities`: `check_mount_url` is
`null` while disconnected or unknown, then `true` or `false` for the active
connection generation. No events are emitted before the handshake response.
While connected, `status.hid` is read live from the active generation's HID
client, so device-observed held keys, modifier state, keyboard LEDs, channel
readiness, local key intent, and mouse buttons do not wait for another
controller command. Frame and HID fields are omitted unless their generation
matches the reported connection; teardown clears both generation-scoped
sources.

```json
{"id":1,"method":"hello","params":{"protocol_version":2}}
{"id":2,"method":"status","params":{}}
{"id":3,"method":"type_text","params":{"text":"hostname\n"}}
{"id":4,"method":"snapshot","params":{}}
{"id":5,"method":"mount_url","params":{"url":"https://example.com/image.iso","mode":"CDROM","approved":true}}
{"id":6,"method":"unmount","params":{"approved":true}}
{"id":7,"method":"shutdown","params":{}}
```

Responses reuse the request `id`; connection, takeover, media, and upload
updates use an `event` field. Logs go to standard error only.
Request IDs must be unique while a request is active. A duplicate is rejected
with `duplicate_request_id` and a `null` response ID so the original request
retains exactly one correlated terminal response. An ID may be reused after
that response.

Ordinary admission is limited to 64 active requests. Those slots do not
include the bounded control plane: `status`, `cancel`, and `shutdown` remain
reachable when all ordinary slots are active. Control requests still
participate in duplicate-ID checking. They execute inline in the single
protocol reader, so reserved control admission does not create an unbounded
queue.

### Ordering and cancellation

- State-changing operations form one ordered queue and execute in input order.
  `status`, upload-only `cancel`, and `shutdown` use the control plane instead:
  status reads a current snapshot, cancel remains reachable during an upload,
  and shutdown preempts blocked work.
- `cancel` targets **uploads only**: `{"method":"cancel","params":{"id":<upload request id>}}`.
  Cancelling any other request returns `not_cancellable`; a completed or
  unknown ID returns `invalid_params`. A cancelled upload stops at a transfer
  boundary and leaves a partial file on the device. Resume is allowed only in
  the controller process that recorded the upload's full-source SHA-256
  identity, and only when the complete local source still matches.

### Action receipts and fresh frames

Successful `key`, `type_text`, `mouse_move`, `mouse_button`, and
`mouse_scroll` requests return an action receipt:

```json
{"id":8,"result":{"generation":1,"cursor":{"generation":1,"frame_id":42}}}
```

The receipt cursor identifies the newest decoded frame captured after the
device-facing operation completed. Pass it back as `after` to wait for a
strictly newer frame:

```json
{"id":9,"method":"snapshot","params":{"after":{"generation":1,"frame_id":42}}}
```

A cursor from an older connection generation fails immediately with
`stale_generation`. `type_text` accepts at most 4,096 characters in the
supported US keyboard layout and resolves after the device reports the entire
macro complete; its completion deadline covers the maximum encoded duration.
A newer frame proves only that the frame is fresher than the action boundary,
not that an arbitrary UI operation completed.

### Approvals

Operations that expose local paths or mutate virtual media require
`"approved":true`: `snapshot` with `path`, `mount_url`, `mount_local`,
`unmount`, `upload`, `mount_storage`, `delete_storage`, and `check_mount_url`.
Without it they fail with `approval_required`.

### Shutdown

Protocol `shutdown`, stdin EOF, SIGINT, and SIGTERM all run the same bounded
cleanup. They stop admission, cancel active uploads, interrupt blocked device
work, reset HID, and unmount and verify controller-owned media **before** the
local range server stops. Signaling, data channels, and the peer connection
then close. A protocol shutdown response is emitted only after cleanup; the
process exits successfully only when cleanup succeeds.

If the controller actor terminates unexpectedly, it first publishes a
disconnected `shutting_down` snapshot with frame, HID, and device fields
cleared. Once termination completes, `status` and subsequent commands return
the stable typed `operation_failed` error rather than presenting the terminal
snapshot as a recoverable connection state. Terminal messages are sanitized
before entering shared state or protocol output.

Run `recorder-for-jetkvm --help` or
`recorder-for-jetkvm serve --help` for all options.

## Runtime requirements

The binary dynamically links the system FFmpeg libraries used for codec,
container, and image-scaling support (`libavcodec`, `libavformat`, `libavutil`,
and `libswscale`). Unused device, filter, and software-resampling crate
features are disabled. Install the distribution FFmpeg packages (FFmpeg
6.x–8.x are supported by the crate's bindings); on a minimal system, verify
with `ldd $(which recorder-for-jetkvm)` that every shared library resolves.
TLS is provided by system OpenSSL (or the platform equivalent) through
`native-tls`.

## Security

- Prefer HTTPS. Plain HTTP exposes the password and session cookie on the LAN.
- Use `--no-tls-verify` only for a certificate verified by another channel.
- Treat the stdio client as trusted: it can control HID and approve local file
  and virtual-media operations.
- The binary links the system FFmpeg libraries; distributors must comply with
  the licenses of their FFmpeg build and enabled codecs.

This codebase is 100% AI-coded.

## License

Apache-2.0. See [`LICENSE`](./LICENSE).
