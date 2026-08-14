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

The first request must be `hello`. Its result includes the protocol version, a
machine-consumable capability list, firmware warnings, and the current
controller status — no separate `status` round trip is needed. No events are
emitted before the handshake response.

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

### Ordering and cancellation

- Requests from one client are dispatched independently **except** HID
  operations, media operations, storage operations, and snapshots, which form
  a single ordered queue executed strictly in input order. `hello`, `status`,
  and `cancel` bypass the queue and stay responsive while an upload runs.
- `cancel` targets **uploads only**: `{"method":"cancel","params":{"id":<upload request id>}}`.
  Cancelling any other request returns `not_cancellable`; a completed or
  unknown id returns `invalid_params`. A cancelled upload stops at a real
  transfer boundary; interrupting an upload leaves a resumable partial file on
  the device (visible via `storage_files`), which a later upload of the same
  image resumes automatically.

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
`stale_generation`. `type_text` resolves after the device reports the
keyboard macro completed. Note: a newer frame proves the frame is fresher than
the action boundary — it is not proof that an arbitrary UI operation
completed.

### Approvals

Operations that expose local paths or mutate virtual media require
`"approved":true`: `snapshot` with `path`, `mount_url`, `mount_local`,
`unmount`, `upload`, `mount_storage`, `delete_storage`, and `check_mount_url`.
Without it they fail with `approval_required`.

### Shutdown

Protocol `shutdown`, stdin EOF, SIGINT, and SIGTERM all run the same bounded
cleanup: stop admitting work, cancel active uploads, reset HID, unmount and
verify controller-owned media **before** the local range server stops, then
close signaling, data channels, and the peer connection. The exit code is zero
when cleanup succeeds.

Run `recorder-for-jetkvm --help` or
`recorder-for-jetkvm serve --help` for all options.

## Runtime requirements

The binary dynamically links the system FFmpeg libraries (libavcodec,
libavformat, libavutil, and their codec dependencies) via `ffmpeg-the-third`
with no optional crate features enabled. Install the distribution FFmpeg
packages (FFmpeg 6.x–8.x are supported by the crate's bindings); on a minimal
system, verify with `ldd $(which recorder-for-jetkvm)` that all shared
libraries resolve. TLS is provided by the system OpenSSL (or platform
equivalent) through `native-tls`.

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
