# recorder-for-jetkvm

`recorder-for-jetkvm` is a small command-line utility for JetKVM devices.
It connects to the JetKVM video stream, detects visible screen changes, and writes MP4 clips only when something changes. It can also capture a single PNG screenshot and exit.

By default:

- recordings are saved in your operating system's video folder
- screenshots are saved in your operating system's pictures folder

## Install

Install from crates.io:

```bash
cargo install recorder-for-jetkvm
```

After that, the `recorder-for-jetkvm` binary should be available in your Cargo bin directory.

## Build From Source

Rust 1.88 or newer is required.

```bash
cargo build --release
```

The binary will be available at `target/release/recorder-for-jetkvm`.

## Usage

Record screen changes to MP4 files:

```bash
export JETKVM_PASSWORD='your-password'
recorder-for-jetkvm --host 192.168.1.130
```

Capture a single screenshot:

```bash
export JETKVM_PASSWORD='your-password'
recorder-for-jetkvm --host 192.168.1.130 --screenshot
```

Override the default output locations:

```bash
recorder-for-jetkvm \
  --host 192.168.1.130 \
  --output-dir /path/to/recordings \
  --screenshot-output /path/to/capture.png
```

Use a password file instead of an environment variable:

```bash
recorder-for-jetkvm \
  --host 192.168.1.130 \
  --password-file ~/.config/jetkvm-password
```

For the full option list:

```bash
recorder-for-jetkvm --help
```

## Persistent controller protocol

`serve --stdio` keeps one WebRTC session alive for repeated observation, HID input,
typed device RPC, and virtual-media operations:

```bash
recorder-for-jetkvm serve --stdio \
  --host 192.168.1.130 \
  --password-file ~/.config/jetkvm-password
```

The protocol is newline-delimited JSON on standard input and output. Logs go to
standard error. The first request must negotiate protocol version 1:

```json
{"id":1,"method":"hello","params":{"protocol_version":1}}
{"id":2,"method":"status","params":{}}
{"id":3,"method":"snapshot","params":{}}
{"id":4,"method":"snapshot","params":{"path":"/tmp/jetkvm.png","approved":true}}
{"id":5,"method":"shutdown","params":{}}
```

Responses carry the matching `id`; asynchronous connection, takeover, and upload
progress messages carry an `event` field. Request IDs are strings or integers.
Use `cancel` with the target request ID to cancel an in-flight operation.
`hello` returns the capabilities supported by the connected firmware plus any
compatibility warnings. A snapshot without `path` is written to
controller-owned temporary storage and remains available until controller
shutdown. Caller-selected snapshot paths and all virtual-media operations that
disclose local paths, mutate device state, or cause the JetKVM to fetch a URL
require `"approved":true`; set it only after explicit user approval.

The controller uses WebSocket signaling with trickle ICE and falls back to the
legacy HTTP offer/answer endpoint if the WebSocket handshake or exchange fails.
Each replacement connection has a monotonically increasing generation. Cached
frames and pending RPC responses never cross generations.

Local image mounting starts a tokenized, single-file HTTP range server bound only
to the interface used to reach the JetKVM. Device-storage uploads are resumable,
cancellable, and validated against free space before transfer. Uploads prefer
authenticated HTTP and fall back to a bounded, backpressured WebRTC data channel
when direct HTTP is unavailable.

## Security

- Plain HTTP sends the JetKVM password and session cookie without transport
  encryption. Use it only on a trusted LAN; prefer HTTPS.
- `--no-tls-verify` disables certificate authentication. Use it only for a
  device certificate you have verified by another channel.
- The stdio peer can request HID actions and approved reads or writes of explicitly
  named local paths. Run the controller only as the intended desktop user and
  treat the parent process as trusted.
- Controller output redacts authentication headers, upload IDs, and local range
  tokens. Do not forward device or FFmpeg debug logs to untrusted consumers.
- `ffmpeg-the-third` links the system FFmpeg libraries. Binary distributors must
  comply with the licenses of the exact FFmpeg build and enabled codecs they
  ship; GPL-enabled FFmpeg builds can impose GPL distribution obligations.

## Notes

- Screenshot filenames default to `recorder-for-jetkvm_YYYY-MM-DD_HH-MM-SS.png`.

## AI Note

This codebase is 100% AI-coded.

## License

Apache License 2.0. See [`LICENSE`](./LICENSE).
