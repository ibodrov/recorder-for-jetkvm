# recorder-for-jetkvm

`recorder-for-jetkvm` is a small command-line utility for JetKVM devices.
It connects to the JetKVM video stream, detects visible screen changes, and writes MP4 clips only when something changes. It can also capture a single PNG screenshot and exit.

By default:

- recordings are saved in your operating system's video folder
- screenshots are saved in your operating system's pictures folder

## Build

```bash
cargo build --release
```

The binary will be available at `target/release/recorder-for-jetkvm`.

## Usage

Record screen changes to MP4 files:

```bash
export JETKVM_PASSWORD='your-password'
./target/release/recorder-for-jetkvm --host 192.168.1.130
```

Capture a single screenshot:

```bash
export JETKVM_PASSWORD='your-password'
./target/release/recorder-for-jetkvm --host 192.168.1.130 --screenshot
```

Override the default output locations:

```bash
./target/release/recorder-for-jetkvm \
  --host 192.168.1.130 \
  --output-dir /path/to/recordings \
  --screenshot-output /path/to/capture.png
```

Use a password file instead of an environment variable:

```bash
./target/release/recorder-for-jetkvm \
  --host 192.168.1.130 \
  --password-file ~/.config/jetkvm-password
```

For the full option list:

```bash
recorder-for-jetkvm --help
```

## Notes

- Use `--no-tls-verify` if your JetKVM is using a self-signed certificate.
- Screenshot filenames default to `recorder-for-jetkvm_YYYY-MM-DD_HH-MM-SS.png`.

## AI Note

This codebase is 100% AI-coded.

## License

Apache License 2.0. See [`LICENSE`](./LICENSE).
