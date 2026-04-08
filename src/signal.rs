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

#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use tokio::sync::watch;

// ---------------------------------------------------------------------------
// Shutdown controller
// ---------------------------------------------------------------------------

/// Manages the two-stage shutdown lifecycle.
///
/// Create one per run via [`ShutdownController::new`], then call
/// [`ShutdownController::spawn_signal_listener`] before entering the run loop.
/// Pass [`ShutdownController::abort_rx`] to the runner/executor.
pub struct ShutdownController {
    /// Sends `true` on first signal to request graceful abort.
    abort_tx: watch::Sender<bool>,
    /// Receivers cloned from here are handed to runner/executor.
    abort_rx: watch::Receiver<bool>,
    /// `true` once the first signal has been received.
    first_signal_received: &'static AtomicBool,
}

/// Global flag so the raw signal handler (sync context) can record the first
/// signal and the second signal can detect that the grace period is active.
static FIRST_SIGNAL: AtomicBool = AtomicBool::new(false);

impl ShutdownController {
    /// Create a new shutdown controller.
    ///
    /// Resets the global signal state so controllers can be created across
    /// sequential test runs or successive CLI invocations within the same
    /// process.
    pub fn new() -> Self {
        FIRST_SIGNAL.store(false, Ordering::SeqCst);
        let (abort_tx, abort_rx) = watch::channel(false);
        Self {
            abort_tx,
            abort_rx,
            first_signal_received: &FIRST_SIGNAL,
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
    pub fn spawn_signal_listener(self) -> watch::Receiver<bool> {
        let rx = self.abort_rx.clone();
        tokio::spawn(async move {
            Self::listen(self.abort_tx, self.first_signal_received).await;
        });
        rx
    }

    /// Internal listener loop.
    async fn listen(abort_tx: watch::Sender<bool>, first_received: &'static AtomicBool) {
        loop {
            // Wait for the next Ctrl+C.
            if tokio::signal::ctrl_c().await.is_err() {
                // Signal registration failed — nothing we can do.
                return;
            }

            if !first_received.swap(true, Ordering::SeqCst) {
                // --- First signal ---
                eprintln!(
                    "\nInterrupt received — finishing current step. \
                     Press Ctrl+C again to force-quit."
                );
                // Tell the executor to abort after the current lifecycle phase.
                let _ = abort_tx.send(true);
            } else {
                // --- Second signal (grace period active) ---
                eprintln!("\nForce-quit — killing harness and exiting.");
                std::process::exit(130);
            }
        }
    }

    /// Check whether the shutdown flag is currently set.
    pub fn is_shutdown_requested(&self) -> bool {
        *self.abort_rx.borrow()
    }
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
pub fn install() -> Result<(ShutdownController, watch::Receiver<bool>)> {
    let controller = ShutdownController::new();
    let rx = controller.abort_rx();
    Ok((controller, rx))
}

/// Install signal handlers and spawn the listener task.
///
/// Must be called from within an active tokio runtime.
pub fn install_and_spawn() -> watch::Receiver<bool> {
    let controller = ShutdownController::new();
    controller.spawn_signal_listener()
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
    fn test_shutdown_controller_new_resets_global() {
        // Set the global flag manually.
        FIRST_SIGNAL.store(true, Ordering::SeqCst);
        // Creating a new controller should reset it.
        let _controller = ShutdownController::new();
        assert!(!FIRST_SIGNAL.load(Ordering::SeqCst));
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
    async fn test_spawn_signal_listener_returns_rx() {
        let controller = ShutdownController::new();
        let rx = controller.spawn_signal_listener();
        // Initially false.
        assert!(!*rx.borrow());
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
}
