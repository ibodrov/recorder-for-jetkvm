# Branch Review

> Resolution: all findings below have been addressed. The descriptions retain
> the original pre-fix evidence.

## Findings

1. **[P1] Broken stdout leaves `serve --stdio` running indefinitely and can bypass cleanup.**  
   `serve_stdio` spawns the writer, but the main `select!` only observes shutdown and stdin; writer termination is checked only after the input loop exits (`src/control_protocol.rs:183-196`, `src/control_protocol.rs:271-278`). Closing the child’s stdout, sending `hello`, and leaving stdin open kept the process alive beyond three seconds. If a later `send_*` detects the closed channel, `?` unwinds before the cleanup epilogue at `src/control_protocol.rs:482-514`. That can skip upload cancellation, media unmount, and awaited controller shutdown. Monitor the writer in the main `select!`, then structure the function so every loop exit—success or error—runs one cleanup path.

2. **[P1] A repeated `hello` bypasses duplicate-ID protection.**  
   Post-handshake `hello` is rejected before `admit_request` runs (`src/control_protocol.rs:365-386`). If another request with that ID is active, the server emits the `hello` error using the duplicate ID and later emits the original request’s response using the same ID. Reproduction produced two terminal responses for ID `2`: `invalid_params` followed by `operation_failed`. This violates the documented correlation invariant. Duplicate admission must happen before the repeated-`hello` branch, with the duplicate response using a null ID.

3. **[P1] Pre-buffered multi-slice keyframes start at the last IDR slice, producing an incomplete first access unit.**  
   Recording starts at `rposition` of an IDR NAL (`src/recorder.rs:464-472`). With the included two-slice fixture, both IDR slices share an RTP timestamp, so this selects the second slice and discards the first. `Mp4Writer` now correctly groups NALs by timestamp, but receives an incomplete initial keyframe from the real ring-buffer path. Find the most recent IDR timestamp, then rewind to the first NAL of that access unit. The fixture test writes the complete stream directly and therefore misses this path.

4. **[P2] Macro cancellation leaves stale active state that can stall the next `type_text`.**  
   `MacroTracker::cancel` rotates only the cancellation token; it does not clear or epoch `MacroState` (`src/hid.rs:155-194`). If a macro was observed active, reset cancels it, and another macro starts before the device’s inactive notification arrives, the new active notification is ignored because `state.active` remains true. Its ticket then waits for a sequence that was never started, potentially until the 120-second deadline. Existing reset coverage never sends the initial active notification, so it cannot catch this race.

5. **[P2] An actor panic never marks the lifecycle complete.**  
   Both completion calls occur after `actor.run().await` (`src/controller.rs:1414-1441` and inside `run_from_phase`). A panic skips both. `status()` can therefore keep returning stale connected state, while `shutdown()` waits for `done` and times out. The test named `unexpected_actor_failure...` injects `Phase::Shutdown(Err(...))`; it does not exercise an actual task panic. Supervise the actor `JoinHandle` or use a panic-safe lifecycle guard so every task exit sets a stable terminal state.

6. **[P2] Public connection configuration accepts backoff values that hot-loop or panic.**  
   `JetKvmController::connect` does not validate `ConnectionConfig` (`src/controller.rs:1341-1350`). A zero `reconnect_min` or `reconnect_max` can create immediate retry loops. Large durations can panic at `self.backoff * 2` (`src/controller.rs:533-537`). Validate nonzero PLI/backoff durations and `reconnect_min <= reconnect_max` before spawning; use checked or saturating backoff arithmetic.

7. **[P2] Missing approval fields return the wrong stable protocol error.**  
   `MountUrlParams`, `MountLocalParams`, `ApprovalParams`, `UploadParams`, and `StorageFileParams` omit `#[serde(default)]` on `approved` (`src/control_protocol.rs:78-109`). Omitting the field therefore returns `invalid_params`, while `snapshot` and `check_mount_url` default it to false and return `approval_required`. Reproduction with `unmount` and `{}` returned:
   ```json
   {"code":"invalid_params","message":"invalid params: missing field `approved`"}
   ```
   Default every approval field to false so the documented authorization contract and error code are consistent.

## Test-only and refactoring residue

- `LiveHidStatus` stores `Arc<dyn Fn() -> HidStatus>` (`src/controller.rs:191-207`) although production always wraps a `HidClient`. The allocation and virtual call exist to let tests inject a closure. Store `HidClient` directly and test status composition separately.
- `RpcClient::drop` is effectively dead (`src/rpc.rs:260-265`): data-channel callbacks each retain `pending`, so its strong count cannot be one while the client still owns the channel. Normal behavior already depends on explicit `cancel_generation`; remove the misleading `Drop` path or introduce a real ownership token.
- The multi-slice unit test directly shells out to `ffprobe` and `ffmpeg` (`src/recorder.rs:700-727`), while signal tests shell out to `kill` (`tests/serve_lifecycle.rs:389-425`). These are environment-dependent tests rather than hermetic Rust tests. At minimum, isolate and explicitly gate the external-tool coverage.

## Verification

- `cargo test --all-targets`: **141 passed**, **1 explicitly gated external-tool test ignored**
- `cargo test recorder::tests::multi_slice_fixture_produces_decodable_mp4_access_units -- --ignored`: **passed**
- `cargo clippy --all-targets -- -D warnings`: **passed**
- `cargo fmt --all -- --check`: **passed**
- `cargo test --doc`: **passed**, no doctests
- Targeted regressions cover closed stdout, duplicate `hello` IDs, denied approval defaults, complete buffered IDR access units, macro reset, actor panic, and invalid connection timing.
- Rust-analyzer diagnostics were unavailable because the configured server exited during startup.
