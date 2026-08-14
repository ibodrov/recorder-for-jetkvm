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

`serve --stdio` exposes a versioned newline-delimited JSON protocol:

```bash
recorder-for-jetkvm serve --stdio \
  --host 192.168.1.130 \
  --password-file ~/.config/jetkvm-password
```

The first request must be `hello`. Requests may then query status, capture
frames, control HID, and manage virtual media:

```json
{"id":1,"method":"hello","params":{"protocol_version":1}}
{"id":2,"method":"status","params":{}}
{"id":3,"method":"type_text","params":{"text":"hostname\n"}}
{"id":4,"method":"snapshot","params":{}}
{"id":5,"method":"mount_url","params":{"url":"https://example.com/image.iso","mode":"CDROM","approved":true}}
{"id":6,"method":"unmount","params":{"approved":true}}
{"id":7,"method":"shutdown","params":{}}
```

Responses reuse the request `id`; connection, takeover, media, and upload
updates use an `event` field. Logs go to standard error. Operations that expose
local paths or mutate virtual media require `"approved":true`.

Run `recorder-for-jetkvm --help` or
`recorder-for-jetkvm serve --help` for all options.

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
