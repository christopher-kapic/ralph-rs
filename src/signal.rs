// Signal handling for graceful shutdown.
//
// Implements a two-stage Ctrl+C / SIGTERM handler:
// - First signal: sets a shutdown flag, lets the current harness finish its
//   lifecycle (tests, commit/rollback) before the run loop exits.
// - Second signal during the grace period: force-kills the harness subprocess
//   and exits immediately.
//
// The shutdown flag is communicated via a `tokio::sync::watch` channel that
// the executor and runner already consume as `abort_rx`.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use tokio::sync::watch;

// ---------------------------------------------------------------------------
// CancelReason
// ---------------------------------------------------------------------------

/// Why the harness cancel channel was tripped.
///
/// The abort/cancel watch channel carries `Option<CancelReason>`: `None` while
/// the run is healthy, `Some(reason)` once something asked the in-flight
/// harness child to die. Both reasons drive the *same* SIGTERM→SIGKILL ladder
/// in [`crate::executor`]; the reason only changes what the executor does
/// *after* the child is dead:
///
/// - [`CancelReason::Aborted`] — operator Ctrl+C / SIGTERM. Terminates the
///   **whole run** (the existing two-stage shutdown behavior).
/// - [`CancelReason::Skipped`] — operator ran `ralph skip` (or the TUI skip
///   binding) against the step that is currently executing in *this* process.
///   Only the current step is dropped; the run advances to the next step.
///
/// They are deliberately distinct: conflating them would make a skip kill the
/// entire run, which is the opposite of what the user asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelReason {
    /// Ctrl+C / SIGTERM — abort the whole run.
    Aborted,
    /// `ralph skip` against the in-flight step — drop this step, keep running.
    Skipped,
}

/// Payload carried by the cancel watch channel. `None` == not cancelled.
pub type CancelState = Option<CancelReason>;

// ---------------------------------------------------------------------------
// In-process cancel registry (for `ralph skip` of the in-flight step)
// ---------------------------------------------------------------------------

/// The cancel `Sender` for the run active in *this* process, if any.
///
/// `signal::install_and_spawn*` registers the sender it hands the runner here
/// so a same-process `ralph skip` (a free function with no channel handle of
/// its own) can inject [`CancelReason::Skipped`] into the exact channel the
/// executor's wait loop is listening on — reusing the existing kill ladder
/// rather than spawning a parallel one. Cleared on a fresh install.
static ACTIVE_CANCEL_TX: Mutex<Option<watch::Sender<CancelState>>> = Mutex::new(None);

/// The change-handling strategy the most recent in-flight `ralph skip`
/// requested via `--changes`. `request_skip_in_flight` stashes it here just
/// before tripping the cancel channel so the executor's skip-finalize path
/// (a different call frame, reached via the kill ladder) knows whether to
/// stash / commit / discard the harness's uncommitted work. Read-once:
/// [`take_requested_park_kind`] consumes it so a stale value can't leak into
/// a later, unrelated skip.
static REQUESTED_PARK_KIND: Mutex<Option<crate::git::ParkStrategyKind>> = Mutex::new(None);

/// `true` while the runner in this process is inside `execute_step` for a
/// step. `skip_step` consults this to decide whether to route through the
/// cancel ladder (a step is in-flight here) or just flip the DB status (no
/// in-flight step, or the runner is a different process entirely).
static STEP_IN_FLIGHT: AtomicBool = AtomicBool::new(false);

fn register_active_cancel_tx(tx: watch::Sender<CancelState>) {
    *ACTIVE_CANCEL_TX.lock().unwrap() = Some(tx);
}

/// Test-only: publish a caller-owned cancel `Sender` into the process-global
/// registry so executor code that reaches for `ACTIVE_CANCEL_TX`
/// (`clear_cancel_state`, `clear_pending_skip_state`) operates on the very
/// channel the test handed `execute_step`. Mirrors what
/// `spawn_signal_listener` does for a real run, without spawning the signal
/// listener task.
#[cfg(test)]
pub fn install_skip_channel_for_test(tx: watch::Sender<CancelState>) {
    register_active_cancel_tx(tx);
}

/// Test-only: seed the park-kind slot exactly as `request_skip_in_flight`
/// would, so a test can assert it is consumed/cleared on the skip and
/// non-skip terminal paths.
#[cfg(test)]
pub fn set_requested_park_kind_for_test(kind: crate::git::ParkStrategyKind) {
    *REQUESTED_PARK_KIND.lock().unwrap() = Some(kind);
}

/// Mark a step as in-flight (or not) for the in-process runner. Returns a
/// guard-free toggle — the runner sets `true` immediately before
/// `execute_step` and `false` immediately after.
pub fn set_step_in_flight(in_flight: bool) {
    STEP_IN_FLIGHT.store(in_flight, Ordering::SeqCst);
}

/// Whether a step is currently executing in this process's runner.
pub fn step_in_flight() -> bool {
    STEP_IN_FLIGHT.load(Ordering::SeqCst)
}

/// RAII guard: sets the in-flight flag on construction, clears it on drop
/// (including the `?`-early-return / panic paths in the runner loop). While
/// it's alive, a same-process `ralph skip` routes through the cancel ladder.
pub struct StepInFlightGuard {
    _private: (),
}

impl StepInFlightGuard {
    pub fn enter() -> Self {
        set_step_in_flight(true);
        Self { _private: () }
    }
}

impl Drop for StepInFlightGuard {
    fn drop(&mut self) {
        set_step_in_flight(false);
    }
}

/// Request that the in-flight step be skipped: record the requested
/// change-handling strategy, then inject [`CancelReason::Skipped`] into this
/// process's cancel channel, kicking the existing SIGTERM→SIGKILL ladder
/// against the harness child.
///
/// `park_kind` is the user's `--changes` choice; it's stashed in a
/// process-global slot the executor consumes via
/// [`take_requested_park_kind`] when it reaches the skip-finalize path
/// (a separate call frame reached only through the kill ladder, so it can't
/// be threaded as a normal argument).
///
/// Returns `true` if a cancel sender was registered and a step is in-flight
/// (so the caller should expect the executor to mark the step `Skipped`),
/// `false` if there's nothing running here to interrupt (the caller should
/// fall back to a plain DB status flip).
pub fn request_skip_in_flight(park_kind: crate::git::ParkStrategyKind) -> bool {
    if !step_in_flight() {
        return false;
    }
    let guard = ACTIVE_CANCEL_TX.lock().unwrap();
    match guard.as_ref() {
        Some(tx) => {
            *REQUESTED_PARK_KIND.lock().unwrap() = Some(park_kind);
            let _ = tx.send(Some(CancelReason::Skipped));
            true
        }
        None => false,
    }
}

/// Drive a skip into *this* process's own cancel channel.
///
/// This is the cross-process bridge's funnel point. `request_skip_in_flight`
/// is for the *same-process* path (a `ralph skip` that shares a process with
/// the blocking runner — only unit tests, in practice). In production the
/// runner is its own process: it polls `plans.skip_requested_step_id` (see
/// [`crate::storage::take_skip_request`]) mid-attempt and, when the pending
/// request targets the step it currently has in-flight, calls this to record
/// the park kind and inject [`CancelReason::Skipped`] into the very channel
/// its own executor wait loop is listening on. From that point on, *all* of
/// the existing, tested `WaitResult::Skipped` →
/// `finalize_skipped`/`cancel_skipped_attempt` handling runs unchanged.
///
/// Unlike [`request_skip_in_flight`] there is no `step_in_flight()` gate: the
/// caller is the runner itself, which by construction is mid-attempt for the
/// step it just matched. Returns `true` if a cancel sender was registered
/// (always the case for a real run), `false` otherwise.
pub fn inject_skip_with_kind(park_kind: crate::git::ParkStrategyKind) -> bool {
    let guard = ACTIVE_CANCEL_TX.lock().unwrap();
    match guard.as_ref() {
        Some(tx) => {
            *REQUESTED_PARK_KIND.lock().unwrap() = Some(park_kind);
            let _ = tx.send(Some(CancelReason::Skipped));
            true
        }
        None => false,
    }
}

/// Clear a stale `Skipped` cancel reason **and** the park-kind slot, but
/// only when a skip is what's latched — never disturb a pending
/// `Aborted` (whole-run shutdown must survive). Defensive cleanup used by
/// the executor's non-skip terminal arms so a `Skipped` that raced and lost
/// can't leak into the next attempt/step (Fix 3).
pub fn clear_pending_skip_state() {
    let is_skip = {
        let guard = ACTIVE_CANCEL_TX.lock().unwrap();
        matches!(guard.as_ref(), Some(tx) if *tx.borrow() == Some(CancelReason::Skipped))
    };
    if is_skip {
        // Drop any recorded park kind first so even if the channel reset
        // races a fresh request, the slot doesn't carry a stale value.
        let _ = REQUESTED_PARK_KIND.lock().unwrap().take();
        clear_cancel_state();
    }
}

/// Consume the park strategy a prior [`request_skip_in_flight`] recorded.
///
/// Returns `None` when no in-flight skip set one (e.g. the skip arrived as
/// a cross-process SIGTERM, or this is an `Aborted` cancel, not a skip) —
/// the executor then falls back to its default (stash) so a skip never
/// silently loses the harness's work. Taking it (rather than peeking) keeps
/// a value from one skip from leaking into a later unrelated one.
pub fn take_requested_park_kind() -> Option<crate::git::ParkStrategyKind> {
    REQUESTED_PARK_KIND.lock().unwrap().take()
}

// NOTE: an earlier design `peek`ed the park-kind slot in the executor's
// `WaitResult::Skipped` arm and then `take`-d it again inside
// `finalize_skipped`. That second, independent read could race the
// `request_skip_in_flight` store under load (silently defaulting to `Stash`).
// The executor now does a single authoritative `take` at the `Skipped` arm
// and threads the kind down, so a separate peek accessor is no longer needed.

/// Clear (reset to `None`) the process's cancel watch channel.
///
/// Used only by the executor's TUI-skip *cancel* path (step 18): after a
/// cancelled attempt is rolled back, the channel still holds
/// `Some(CancelReason::Skipped)`. Without clearing it, the retry loop's
/// pre-attempt cancel check would immediately route the re-entered attempt
/// through `finalize_precancel` (marking the step Skipped) — defeating the
/// "re-enter at the same attempt" guarantee. Resetting it to `None` lets the
/// loop genuinely re-run the attempt. Sends through the registered sender so
/// every cloned receiver sees the reset.
///
/// Returns `true` if a cancel sender was registered (the reset was applied),
/// `false` otherwise (no in-process runner — nothing to reset).
pub fn clear_cancel_state() -> bool {
    let guard = ACTIVE_CANCEL_TX.lock().unwrap();
    match guard.as_ref() {
        Some(tx) => {
            let _ = tx.send(None);
            true
        }
        None => false,
    }
}

// ---------------------------------------------------------------------------
// Forced-exit cleanup registry
// ---------------------------------------------------------------------------

/// A cleanup closure executed just before `std::process::exit(130)` on a
/// second Ctrl+C. Since `exit` skips every Drop impl, any RAII guard whose
/// release is load-bearing (e.g. the per-project run lock) registers itself
/// here so the row still gets cleaned up on a forced exit.
type ExitCleanup = Box<dyn FnOnce() + Send>;

static EXIT_CLEANUP: Mutex<Option<ExitCleanup>> = Mutex::new(None);

/// Serializes tests that touch `EXIT_CLEANUP` so parallel test threads in the
/// same binary don't race on the global slot.
#[cfg(test)]
pub(crate) static EXIT_CLEANUP_TEST_LOCK: Mutex<()> = Mutex::new(());

/// Acquire [`EXIT_CLEANUP_TEST_LOCK`], tolerating poisoning.
///
/// The guard protects nothing but *execution ordering* — there is no shared
/// invariant a panicking test could leave half-updated. Without this, one
/// asserting test that panics while holding the guard poisons the mutex, and
/// every subsequently-serialized signal test then fails with `PoisonError`
/// instead of running — a cascade of false failures whose appearance depends
/// on cross-test scheduling (i.e. flaky under full parallel `cargo test`).
/// Recovering the poisoned guard is always safe here.
#[cfg(test)]
pub(crate) fn lock_exit_cleanup_test()
-> std::sync::MutexGuard<'static, ()> {
    EXIT_CLEANUP_TEST_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Register a cleanup to run before `exit(130)` on forced shutdown. Replaces
/// any previously-registered cleanup.
pub fn set_exit_cleanup(f: ExitCleanup) {
    *EXIT_CLEANUP.lock().unwrap() = Some(f);
}

/// Clear the registered exit cleanup. Called when the guard whose cleanup
/// this represents was dropped normally, so no forced-exit release is needed.
pub fn clear_exit_cleanup() {
    *EXIT_CLEANUP.lock().unwrap() = None;
}

/// Take and run the registered exit cleanup (if any). Idempotent.
pub(crate) fn run_exit_cleanup() {
    let f = EXIT_CLEANUP.lock().unwrap().take();
    if let Some(f) = f {
        f();
    }
}

// ---------------------------------------------------------------------------
// Shutdown controller
// ---------------------------------------------------------------------------

/// Handle returned from [`ShutdownController::spawn_signal_listener`] that
/// lets application code trigger a graceful shutdown programmatically — the
/// same effect as receiving a first Ctrl+C. Cheap to clone.
#[derive(Clone)]
pub struct ShutdownHandle {
    abort_tx: watch::Sender<CancelState>,
}

impl ShutdownHandle {
    /// Request graceful shutdown. Sets the abort flag so the current step
    /// finishes its lifecycle, then the runner exits. Idempotent.
    #[allow(dead_code)]
    pub fn shutdown(&self) {
        let _ = self.abort_tx.send(Some(CancelReason::Aborted));
    }

    /// Whether shutdown has already been requested (by signal or by a prior
    /// [`shutdown`](Self::shutdown) call).
    #[allow(dead_code)]
    pub fn is_shutdown_requested(&self) -> bool {
        self.abort_tx.borrow().is_some()
    }
}

/// Manages the two-stage shutdown lifecycle.
///
/// Create one per run via [`ShutdownController::new`], then call
/// [`ShutdownController::spawn_signal_listener`] before entering the run loop.
/// Pass [`ShutdownController::abort_rx`] to the runner/executor.
#[allow(dead_code)]
pub struct ShutdownController {
    /// Sends `Some(CancelReason::Aborted)` on first signal to request a
    /// graceful abort of the whole run.
    abort_tx: watch::Sender<CancelState>,
    /// Receivers cloned from here are handed to runner/executor.
    abort_rx: watch::Receiver<CancelState>,
    /// `true` once the first signal has been received. Per-instance so
    /// concurrent tests don't race on a shared global slot.
    first_signal_received: Arc<AtomicBool>,
}

impl ShutdownController {
    /// Create a new shutdown controller.
    ///
    /// Each controller owns its own first-signal flag, so creating multiple
    /// controllers in parallel (e.g. across test threads) does not contend on
    /// shared state.
    pub fn new() -> Self {
        let (abort_tx, abort_rx) = watch::channel(None);
        Self {
            abort_tx,
            abort_rx,
            first_signal_received: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Obtain a cloneable receiver for the abort flag.
    ///
    /// Hand this to [`runner::run_plan`] / [`executor::execute_step`].
    pub fn abort_rx(&self) -> watch::Receiver<CancelState> {
        self.abort_rx.clone()
    }

    /// Spawn a tokio task that listens for SIGINT / SIGTERM.
    ///
    /// - **First signal**: sets the abort flag (watch channel → `true`), prints
    ///   a message, and allows the current step to finish gracefully.
    /// - **Second signal**: prints a force-exit message and calls
    ///   [`std::process::exit(130)`] (128 + SIGINT) to terminate immediately.
    ///
    /// Returns a [`ShutdownHandle`] for triggering shutdown programmatically
    /// and a receiver for the abort flag.
    pub fn spawn_signal_listener(self) -> (ShutdownHandle, watch::Receiver<CancelState>) {
        let rx = self.abort_rx.clone();
        let handle = ShutdownHandle {
            abort_tx: self.abort_tx.clone(),
        };
        // Publish this run's cancel sender so a same-process `ralph skip`
        // can inject `Skipped` into the very channel the executor's wait
        // loop is listening on (reusing the existing kill ladder).
        register_active_cancel_tx(self.abort_tx.clone());
        tokio::spawn(async move {
            Self::listen(self.abort_tx, self.first_signal_received).await;
        });
        (handle, rx)
    }

    /// Internal listener loop.
    async fn listen(abort_tx: watch::Sender<CancelState>, first_received: Arc<AtomicBool>) {
        loop {
            // Wait for either SIGINT (Ctrl+C) or SIGTERM (`ralph cancel`
            // delivers the latter, and external process supervisors often
            // prefer it over SIGINT). Both route through the same two-stage
            // logic so the UX is consistent regardless of how shutdown was
            // requested.
            let signal_name = next_signal().await;

            if !first_received.swap(true, Ordering::SeqCst) {
                // --- First signal ---
                eprintln!(
                    "\n{signal_name} received — finishing current step. \
                     Send again to force-quit."
                );
                // Tell the executor to abort after the current lifecycle
                // phase. `Aborted` (distinct from `Skipped`) terminates the
                // whole run; it also overrides any pending skip request so a
                // Ctrl+C after a skip still tears the run down.
                let _ = abort_tx.send(Some(CancelReason::Aborted));
            } else {
                // --- Second signal (grace period active) ---
                eprintln!("\nForce-quit — killing harness and exiting.");
                // exit(130) skips Drop, so give registered guards (e.g. the
                // run lock) a chance to release before the process dies.
                run_exit_cleanup();
                std::process::exit(130);
            }
        }
    }

    /// Check whether the shutdown flag is currently set.
    #[allow(dead_code)]
    pub fn is_shutdown_requested(&self) -> bool {
        self.abort_rx.borrow().is_some()
    }
}

// ---------------------------------------------------------------------------
// Cross-signal listener
// ---------------------------------------------------------------------------

/// Wait for the next shutdown-class signal and return its human-readable name.
///
/// On unix, races SIGINT against SIGTERM; either one resolves and drives the
/// two-stage shutdown. On non-unix only Ctrl+C is available.
///
/// SIGTERM registration happens on the very first call inside the listener
/// task — before that call returns, any SIGTERM delivered to the process
/// would take the default action (terminate). That's fine for ralph: signals
/// arriving during startup (before the runner is in place) have nothing
/// useful to interrupt anyway, and callers install this listener before the
/// run loop begins.
#[cfg(unix)]
async fn next_signal() -> &'static str {
    use tokio::signal::unix::{SignalKind, signal};
    let mut sigterm = match signal(SignalKind::terminate()) {
        Ok(s) => s,
        Err(_) => {
            // Registration failed — fall back to ctrl_c only.
            let _ = tokio::signal::ctrl_c().await;
            return "SIGINT";
        }
    };
    tokio::select! {
        res = tokio::signal::ctrl_c() => {
            if res.is_err() {
                // ctrl_c failed but sigterm is live — wait on it.
                let _ = sigterm.recv().await;
                "SIGTERM"
            } else {
                "SIGINT"
            }
        }
        _ = sigterm.recv() => "SIGTERM",
    }
}

#[cfg(not(unix))]
async fn next_signal() -> &'static str {
    let _ = tokio::signal::ctrl_c().await;
    "SIGINT"
}

// ---------------------------------------------------------------------------
// Convenience function
// ---------------------------------------------------------------------------

/// Set up signal handling and return the abort receiver.
///
/// This is the primary entry-point used by `main.rs`:
///
/// ```ignore
/// let abort_rx = signal::install()?;
/// rt.block_on(runner::run_plan(&conn, &plan, &cfg, workdir, &opts, abort_rx))?;
/// ```
#[allow(dead_code)]
pub fn install() -> Result<(ShutdownController, watch::Receiver<CancelState>)> {
    let controller = ShutdownController::new();
    let rx = controller.abort_rx();
    Ok((controller, rx))
}

/// Install signal handlers and spawn the listener task.
///
/// Must be called from within an active tokio runtime.
pub fn install_and_spawn() -> watch::Receiver<CancelState> {
    let (_handle, rx) = install_and_spawn_with_handle();
    rx
}

/// Install signal handlers and spawn the listener task, returning both a
/// [`ShutdownHandle`] (for programmatic shutdown) and the abort receiver.
///
/// Must be called from within an active tokio runtime.
#[allow(dead_code)]
pub fn install_and_spawn_with_handle() -> (ShutdownHandle, watch::Receiver<CancelState>) {
    ShutdownController::new().spawn_signal_listener()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shutdown_controller_initial_state() {
        let controller = ShutdownController::new();
        assert!(!controller.is_shutdown_requested());
        assert!(controller.abort_rx().borrow().is_none());
    }

    #[test]
    fn test_shutdown_controller_instances_are_independent() {
        // Each controller owns its own flag; flipping one must not be
        // visible from another. This is the regression test for L35.
        let a = ShutdownController::new();
        let b = ShutdownController::new();
        a.first_signal_received.store(true, Ordering::SeqCst);
        assert!(a.first_signal_received.load(Ordering::SeqCst));
        assert!(!b.first_signal_received.load(Ordering::SeqCst));
    }

    #[test]
    fn test_abort_tx_propagates() {
        let controller = ShutdownController::new();
        let rx = controller.abort_rx();
        assert!(rx.borrow().is_none());

        // Simulate first signal: send the abort reason.
        controller
            .abort_tx
            .send(Some(CancelReason::Aborted))
            .unwrap();
        assert_eq!(*rx.borrow(), Some(CancelReason::Aborted));
        assert!(controller.is_shutdown_requested());
    }

    #[test]
    fn test_multiple_receivers() {
        let controller = ShutdownController::new();
        let rx1 = controller.abort_rx();
        let rx2 = controller.abort_rx();

        controller
            .abort_tx
            .send(Some(CancelReason::Aborted))
            .unwrap();
        assert_eq!(*rx1.borrow(), Some(CancelReason::Aborted));
        assert_eq!(*rx2.borrow(), Some(CancelReason::Aborted));
    }

    #[test]
    fn test_first_signal_flag() {
        let controller = ShutdownController::new();

        // Initially not received.
        assert!(!controller.first_signal_received.load(Ordering::SeqCst));

        // Simulate first signal.
        let was_set = controller
            .first_signal_received
            .swap(true, Ordering::SeqCst);
        assert!(!was_set); // First time → was false.

        // Second swap should indicate already set.
        let was_set = controller
            .first_signal_received
            .swap(true, Ordering::SeqCst);
        assert!(was_set); // Already true.
    }

    #[tokio::test]
    async fn test_spawn_signal_listener_returns_handle_and_rx() {
        let controller = ShutdownController::new();
        let (handle, rx) = controller.spawn_signal_listener();
        // Initially not cancelled.
        assert!(rx.borrow().is_none());
        assert!(!handle.is_shutdown_requested());
    }

    #[tokio::test]
    async fn test_shutdown_handle_triggers_abort() {
        // Regression for L36: application code can trigger graceful shutdown
        // via the handle returned from spawn_signal_listener, even though
        // spawn_signal_listener itself consumes the controller.
        let controller = ShutdownController::new();
        let (handle, mut rx) = controller.spawn_signal_listener();
        assert!(rx.borrow().is_none());

        handle.shutdown();

        // Wait for the value to propagate through the watch channel.
        rx.changed().await.unwrap();
        assert_eq!(*rx.borrow(), Some(CancelReason::Aborted));
        assert!(handle.is_shutdown_requested());
    }

    #[tokio::test]
    async fn test_install_and_spawn_with_handle() {
        let (handle, rx) = install_and_spawn_with_handle();
        assert!(rx.borrow().is_none());
        assert!(!handle.is_shutdown_requested());
    }

    #[tokio::test]
    async fn test_install_and_spawn() {
        let rx = install_and_spawn();
        assert!(rx.borrow().is_none());
    }

    #[test]
    fn test_install_returns_controller_and_rx() {
        let (controller, rx) = install().unwrap();
        assert!(rx.borrow().is_none());
        assert!(!controller.is_shutdown_requested());
    }

    #[test]
    fn test_exit_cleanup_runs_once_and_is_cleared() {
        let _guard = lock_exit_cleanup_test();
        clear_exit_cleanup();

        let ran = std::sync::Arc::new(AtomicBool::new(false));
        let ran_clone = std::sync::Arc::clone(&ran);
        set_exit_cleanup(Box::new(move || {
            ran_clone.store(true, Ordering::SeqCst);
        }));

        run_exit_cleanup();
        assert!(ran.load(Ordering::SeqCst), "cleanup should have run");

        // A second call is a no-op because the cleanup was taken.
        ran.store(false, Ordering::SeqCst);
        run_exit_cleanup();
        assert!(!ran.load(Ordering::SeqCst), "cleanup should not run twice");
    }

    /// Regression: a SIGTERM delivered to the process (which is how
    /// `ralph cancel` signals its sibling) must flip the abort flag via
    /// the same two-stage path that Ctrl+C uses.
    ///
    /// Holds `EXIT_CLEANUP_TEST_LOCK` to serialize with other tests that
    /// mutate process-wide state (signal handlers, exit cleanup slot), so
    /// parallel cargo test threads can't race on the SIGTERM disposition.
    #[cfg(unix)]
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn test_sigterm_triggers_graceful_shutdown() {
        // Holding the std::Mutex guard across .await is intentional here:
        // the whole point is to serialize the full SIGTERM-delivery window
        // (listener setup + raise + flag check) against other tests that
        // mutate process-wide state. The test runs on a current_thread
        // runtime, so there's no risk of cross-thread guard transfer.
        let _guard = lock_exit_cleanup_test();
        let controller = ShutdownController::new();
        let (_handle, mut rx) = controller.spawn_signal_listener();
        assert!(rx.borrow().is_none());

        // Give the listener a moment to register its SIGTERM handler
        // before we deliver the signal. Without this wait, we'd race the
        // default disposition and the test process would terminate.
        tokio::task::yield_now().await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // SAFETY: raise is async-signal-safe and just posts a signal to
        // the current process.
        let rc = unsafe { libc::raise(libc::SIGTERM) };
        assert_eq!(rc, 0, "libc::raise(SIGTERM) failed");

        // The watch channel must flip to true within a short window.
        tokio::time::timeout(std::time::Duration::from_millis(500), rx.changed())
            .await
            .expect("abort flag never flipped after SIGTERM")
            .expect("watch sender dropped");
        assert_eq!(
            *rx.borrow(),
            Some(CancelReason::Aborted),
            "SIGTERM must set the cancel reason to Aborted (whole-run abort)"
        );
    }

    #[test]
    fn test_clear_exit_cleanup_prevents_run() {
        let _guard = lock_exit_cleanup_test();
        clear_exit_cleanup();

        let ran = std::sync::Arc::new(AtomicBool::new(false));
        let ran_clone = std::sync::Arc::clone(&ran);
        set_exit_cleanup(Box::new(move || {
            ran_clone.store(true, Ordering::SeqCst);
        }));
        clear_exit_cleanup();
        run_exit_cleanup();
        assert!(!ran.load(Ordering::SeqCst));
    }

    /// `CancelReason::Skipped` must be a distinct value from
    /// `CancelReason::Aborted` — the executor branches on this to decide
    /// "drop one step" vs "tear the whole run down".
    #[test]
    fn test_cancel_reasons_are_distinct() {
        assert_ne!(CancelReason::Aborted, CancelReason::Skipped);
    }

    /// With no step in-flight, `request_skip_in_flight` is a no-op and
    /// reports `false` so the caller falls back to a plain DB status flip.
    #[test]
    fn test_request_skip_no_step_in_flight_is_noop() {
        let _guard = lock_exit_cleanup_test();
        set_step_in_flight(false);
        assert!(!request_skip_in_flight(crate::git::ParkStrategyKind::Stash));
    }

    /// When a step is in-flight, `request_skip_in_flight` injects exactly
    /// `Some(CancelReason::Skipped)` into the registered cancel channel —
    /// the same channel the executor's wait loop listens on — and returns
    /// `true`. Distinct from the `Aborted` value the signal listener sends.
    #[tokio::test(flavor = "current_thread")]
    #[allow(clippy::await_holding_lock)]
    async fn test_request_skip_in_flight_signals_skipped() {
        // Holding the std::Mutex guard across .await is intentional and
        // safe: this serializes the process-wide cancel registry +
        // in-flight flag against other tests that mutate the same globals,
        // and the current_thread runtime rules out cross-thread guard
        // transfer (same rationale as test_sigterm_triggers_graceful_shutdown).
        let _guard = lock_exit_cleanup_test();
        let controller = ShutdownController::new();
        let (_handle, mut rx) = controller.spawn_signal_listener();
        assert!(rx.borrow().is_none());

        set_step_in_flight(true);
        assert!(
            request_skip_in_flight(crate::git::ParkStrategyKind::Commit),
            "should signal when in-flight"
        );

        rx.changed().await.unwrap();
        assert_eq!(
            *rx.borrow(),
            Some(CancelReason::Skipped),
            "skip must inject Skipped, not Aborted"
        );

        // The requested park kind is recorded for the executor and consumed
        // exactly once.
        assert_eq!(
            take_requested_park_kind(),
            Some(crate::git::ParkStrategyKind::Commit),
            "request_skip_in_flight must stash the --changes choice"
        );
        assert_eq!(
            take_requested_park_kind(),
            None,
            "take must consume the value (no leak into a later skip)"
        );

        set_step_in_flight(false);
    }
}
