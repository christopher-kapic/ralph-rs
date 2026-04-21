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
    abort_tx: watch::Sender<bool>,
}

impl ShutdownHandle {
    /// Request graceful shutdown. Sets the abort flag so the current step
    /// finishes its lifecycle, then the runner exits. Idempotent.
    #[allow(dead_code)]
    pub fn shutdown(&self) {
        let _ = self.abort_tx.send(true);
    }

    /// Whether shutdown has already been requested (by signal or by a prior
    /// [`shutdown`](Self::shutdown) call).
    #[allow(dead_code)]
    pub fn is_shutdown_requested(&self) -> bool {
        *self.abort_tx.borrow()
    }
}

/// Manages the two-stage shutdown lifecycle.
///
/// Create one per run via [`ShutdownController::new`], then call
/// [`ShutdownController::spawn_signal_listener`] before entering the run loop.
/// Pass [`ShutdownController::abort_rx`] to the runner/executor.
#[allow(dead_code)]
pub struct ShutdownController {
    /// Sends `true` on first signal to request graceful abort.
    abort_tx: watch::Sender<bool>,
    /// Receivers cloned from here are handed to runner/executor.
    abort_rx: watch::Receiver<bool>,
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
        let (abort_tx, abort_rx) = watch::channel(false);
        Self {
            abort_tx,
            abort_rx,
            first_signal_received: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Obtain a cloneable receiver for the abort flag.
    ///
    /// Hand this to [`runner::run_plan`] / [`executor::execute_step`].
    pub fn abort_rx(&self) -> watch::Receiver<bool> {
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
    pub fn spawn_signal_listener(self) -> (ShutdownHandle, watch::Receiver<bool>) {
        let rx = self.abort_rx.clone();
        let handle = ShutdownHandle {
            abort_tx: self.abort_tx.clone(),
        };
        tokio::spawn(async move {
            Self::listen(self.abort_tx, self.first_signal_received).await;
        });
        (handle, rx)
    }

    /// Internal listener loop.
    async fn listen(abort_tx: watch::Sender<bool>, first_received: Arc<AtomicBool>) {
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
                // Tell the executor to abort after the current lifecycle phase.
                let _ = abort_tx.send(true);
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
        *self.abort_rx.borrow()
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
pub fn install() -> Result<(ShutdownController, watch::Receiver<bool>)> {
    let controller = ShutdownController::new();
    let rx = controller.abort_rx();
    Ok((controller, rx))
}

/// Install signal handlers and spawn the listener task.
///
/// Must be called from within an active tokio runtime.
pub fn install_and_spawn() -> watch::Receiver<bool> {
    let (_handle, rx) = install_and_spawn_with_handle();
    rx
}

/// Install signal handlers and spawn the listener task, returning both a
/// [`ShutdownHandle`] (for programmatic shutdown) and the abort receiver.
///
/// Must be called from within an active tokio runtime.
#[allow(dead_code)]
pub fn install_and_spawn_with_handle() -> (ShutdownHandle, watch::Receiver<bool>) {
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
        assert!(!*controller.abort_rx().borrow());
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
        assert!(!*rx.borrow());

        // Simulate first signal: send true.
        controller.abort_tx.send(true).unwrap();
        assert!(*rx.borrow());
        assert!(controller.is_shutdown_requested());
    }

    #[test]
    fn test_multiple_receivers() {
        let controller = ShutdownController::new();
        let rx1 = controller.abort_rx();
        let rx2 = controller.abort_rx();

        controller.abort_tx.send(true).unwrap();
        assert!(*rx1.borrow());
        assert!(*rx2.borrow());
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
        // Initially false.
        assert!(!*rx.borrow());
        assert!(!handle.is_shutdown_requested());
    }

    #[tokio::test]
    async fn test_shutdown_handle_triggers_abort() {
        // Regression for L36: application code can trigger graceful shutdown
        // via the handle returned from spawn_signal_listener, even though
        // spawn_signal_listener itself consumes the controller.
        let controller = ShutdownController::new();
        let (handle, mut rx) = controller.spawn_signal_listener();
        assert!(!*rx.borrow());

        handle.shutdown();

        // Wait for the value to propagate through the watch channel.
        rx.changed().await.unwrap();
        assert!(*rx.borrow());
        assert!(handle.is_shutdown_requested());
    }

    #[tokio::test]
    async fn test_install_and_spawn_with_handle() {
        let (handle, rx) = install_and_spawn_with_handle();
        assert!(!*rx.borrow());
        assert!(!handle.is_shutdown_requested());
    }

    #[tokio::test]
    async fn test_install_and_spawn() {
        let rx = install_and_spawn();
        assert!(!*rx.borrow());
    }

    #[test]
    fn test_install_returns_controller_and_rx() {
        let (controller, rx) = install().unwrap();
        assert!(!*rx.borrow());
        assert!(!controller.is_shutdown_requested());
    }

    #[test]
    fn test_exit_cleanup_runs_once_and_is_cleared() {
        let _guard = EXIT_CLEANUP_TEST_LOCK.lock().unwrap();
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
        let _guard = EXIT_CLEANUP_TEST_LOCK.lock().unwrap();
        let controller = ShutdownController::new();
        let (_handle, mut rx) = controller.spawn_signal_listener();
        assert!(!*rx.borrow());

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
        assert!(*rx.borrow(), "abort flag should be true after SIGTERM");
    }

    #[test]
    fn test_clear_exit_cleanup_prevents_run() {
        let _guard = EXIT_CLEANUP_TEST_LOCK.lock().unwrap();
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
}
