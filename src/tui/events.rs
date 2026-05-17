// Live-run NDJSON event subscription for the TUI.
//
// When the TUI spawns its own runner subprocess (`R` keybinding or `/run`
// palette command per TUI-plan.md §13), this module forks `ralph run --json
// --non-interactive <slug>` with stdout piped, parses the line-buffered
// NDJSON stream into [`RunEvent`]s, and forwards them through a tokio mpsc
// channel into the surrounding sync TUI loop.
//
// The synchronous TUI loop drains pending events on every poll tick via
// [`RunSubscription::drain`] and dispatches them into the App via
// [`dispatch_event`]. Because the receiver is owned by the sync loop and
// the producer side runs inside an owned tokio runtime, the subscription's
// lifetime is bounded by the user's view: when the user pops back to the
// plan list, the subscription is dropped, the tokio runtime shuts down, the
// reader task is cancelled, and the runner subprocess is reaped via
// [`tokio::process::Child`]'s default kill_on_drop semantics (see
// [`spawn_streaming_runner`]).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;

use anyhow::{Context, Result};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, error::TryRecvError};
use tokio::sync::oneshot;

use crate::output::RunEvent;
use crate::tui::views::plan_detail::PlanDetailApp;

// ---------------------------------------------------------------------------
// Tail buffer sizing
// ---------------------------------------------------------------------------

/// Total tail capacity we keep in memory per stream. Larger than the
/// nominally-displayed window so the user can scroll back via `J`/`K`
/// without losing context.
pub const TAIL_BUFFER_LINES: usize = 256;

/// Default visible-window size for the right-pane tail rendering — the
/// "Last ~20 lines" the spec mentions in TUI-plan.md §13.
pub const TAIL_VISIBLE_LINES: usize = 20;

// ---------------------------------------------------------------------------
// RunSubscription — handle to a live event stream
// ---------------------------------------------------------------------------

/// Active subscription to a TUI-spawned runner's NDJSON stream.
///
/// Constructed via [`spawn_streaming_runner`]. The owned tokio runtime keeps
/// the reader task and child process alive for the subscription's lifetime;
/// dropping a `RunSubscription` shuts the runtime down, which cancels the
/// reader task and (via `kill_on_drop`) terminates the child.
pub struct RunSubscription {
    rx: UnboundedReceiver<RunEvent>,
    /// Single-use channel for the runner's stderr + non-zero exit status
    /// when it fails to start (preflight errors, missing harness, dirty
    /// tree, run-lock held by another process, etc.). The spawn task
    /// either sends a message exactly once (failure) or drops the sender
    /// without sending (clean exit). The TUI poll loop drains via
    /// [`Self::poll_failure_status`] when the subscription disconnects.
    failure: oneshot::Receiver<String>,
    /// Owned tokio runtime. Held only to keep the spawned task running —
    /// callers never reach into it. Wrapping in `Arc` lets the spawned
    /// task hold its own strong reference if it ever needs to (it doesn't
    /// today, but symmetric ownership keeps the API future-proof).
    _runtime: Arc<Runtime>,
}

/// A live [`RunSubscription`] tagged with the plan slug it belongs to.
///
/// Owned by the top-level TUI dispatcher (`run_plan_list_tui`) so it
/// survives navigation: pushing into plan-detail and popping back leaves
/// the subscription untouched, and the runner subprocess keeps streaming
/// the whole time. The slug lets a child dispatcher decide whether to
/// attach and dispatch events into its `App` or leave the subscription
/// alone (when the user is viewing a different plan than the one
/// running).
pub struct HostedSubscription {
    pub slug: String,
    pub sub: RunSubscription,
}

/// Current state of the runner-failure side channel.
pub enum FailureStatus {
    /// The runner task is still alive and hasn't produced a final outcome yet.
    Pending,
    /// The runner has settled cleanly and will never send a failure message.
    Clean,
    /// The runner failed and delivered a final one-shot message.
    Message(String),
}

impl RunSubscription {
    /// Drain every event currently sitting in the channel and return them
    /// in arrival order. Non-blocking: returns immediately when the channel
    /// is empty or the producer has hung up.
    pub fn drain(&mut self) -> Vec<RunEvent> {
        let mut out = Vec::new();
        while let Ok(evt) = self.rx.try_recv() {
            out.push(evt);
        }
        out
    }

    /// True once the producer side has hung up — i.e. the runner subprocess
    /// exited and the reader task drained its stdout. Callers use this to
    /// detect run completion and clean up the subscription.
    pub fn is_disconnected(&mut self) -> bool {
        matches!(self.rx.try_recv(), Err(TryRecvError::Disconnected))
    }

    /// Poll the runner-failure side channel without blocking. This keeps the
    /// TUI from dropping the subscription in the brief window after stdout
    /// closes but before the task has waited on the child and published the
    /// final success/failure outcome.
    ///
    /// Designed to be called by the TUI poll loop right after observing
    /// [`Self::is_disconnected`] returning true.
    pub fn poll_failure_status(&mut self) -> FailureStatus {
        match self.failure.try_recv() {
            Ok(msg) => FailureStatus::Message(msg),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty) => FailureStatus::Pending,
            Err(tokio::sync::oneshot::error::TryRecvError::Closed) => FailureStatus::Clean,
        }
    }
}

// ---------------------------------------------------------------------------
// Command construction
// ---------------------------------------------------------------------------

/// Whether the streaming subprocess should drive the plan via `ralph run`
/// or `ralph resume`. Threaded through so the TUI's auto-start path can
/// fork either subcommand without duplicating the rest of the streaming
/// scaffolding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamMode {
    /// `ralph run <slug>`, optionally with `--current-branch` and/or
    /// `--no-auto-stash`. Both flags are behavior knobs (not opt-outs from
    /// interactivity), so the TUI's auto-start path threads them through to
    /// the spawned subprocess rather than bypassing the TUI.
    Run {
        current_branch: bool,
        no_auto_stash: bool,
    },
    /// `ralph resume <slug>`. The resume code path always operates on the
    /// current branch internally, so no flag is needed.
    Resume,
}

/// Build the streaming runner [`Command`] without spawning. Mirrors the
/// non-streaming variant in `tui::run_dialog::build_run_command` but with
/// `--json` added and stdout piped so the TUI can parse NDJSON.
pub fn build_streaming_run_command(
    exe: &Path,
    project: &Path,
    slug: &str,
    mode: StreamMode,
) -> Command {
    let mut cmd = Command::new(exe);
    cmd.arg("-C")
        .arg(project)
        .arg("--non-interactive")
        .arg("--json");
    match mode {
        StreamMode::Run {
            current_branch,
            no_auto_stash,
        } => {
            cmd.arg("run");
            if current_branch {
                cmd.arg("--current-branch");
            }
            if no_auto_stash {
                cmd.arg("--no-auto-stash");
            }
        }
        StreamMode::Resume => {
            cmd.arg("resume");
        }
    }
    cmd.arg(slug);
    cmd.stdin(Stdio::null())
        .stdout(Stdio::piped())
        // Pipe stderr (was Null) so the spawn task can capture preflight
        // failures, harness-not-found, run-lock contention, etc., and
        // surface them to the TUI as a toast instead of silently dropping
        // them on the floor.
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    cmd
}

// ---------------------------------------------------------------------------
// Spawn entry point
// ---------------------------------------------------------------------------

/// Spawn the runner subprocess and return a [`RunSubscription`] that yields
/// the parsed NDJSON event stream. The owned tokio runtime stays alive for
/// the subscription; dropping the subscription tears the runtime down,
/// which kills the child and cancels the reader task.
pub fn spawn_streaming_runner(
    exe: PathBuf,
    project: PathBuf,
    slug: String,
    mode: StreamMode,
) -> Result<RunSubscription> {
    let runtime = Arc::new(
        tokio::runtime::Runtime::new().context("create tokio runtime for run subscription")?,
    );
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let (failure_tx, failure_rx) = oneshot::channel::<String>();
    // Held in an Option so the (at most one) send site can `.take()` it; on
    // clean exits the Option remains Some at task end, the Sender drops, and
    // the receiver observes a closed channel.
    let mut failure_tx = Some(failure_tx);

    runtime.spawn(async move {
        let mut cmd = build_streaming_run_command(&exe, &project, &slug, mode);
        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                if let Some(tx) = failure_tx.take() {
                    let _ = tx.send(format!("Failed to spawn ralph runner: {e}"));
                }
                return;
            }
        };
        let stdout = match child.stdout.take() {
            Some(s) => s,
            None => return,
        };
        // Concurrently drain stderr into a buffer so we have something to
        // show the user if the run aborts with a non-zero exit. Capped to
        // a few KB — preflight error messages are short, and we don't want
        // a runaway runner to balloon memory.
        let stderr_handle = child.stderr.take().map(|stderr| {
            tokio::spawn(async move {
                use tokio::io::AsyncReadExt;
                const STDERR_CAP: usize = 8192;
                let mut buf = Vec::with_capacity(1024);
                let mut reader = BufReader::new(stderr);
                let mut chunk = [0u8; 1024];
                while let Ok(n) = reader.read(&mut chunk).await {
                    if n == 0 {
                        break;
                    }
                    let remaining = STDERR_CAP.saturating_sub(buf.len());
                    if remaining == 0 {
                        // Already at cap — keep draining so the child's
                        // stderr pipe doesn't block, but discard the bytes.
                        continue;
                    }
                    buf.extend_from_slice(&chunk[..n.min(remaining)]);
                }
                String::from_utf8_lossy(&buf).into_owned()
            })
        });
        let reader = BufReader::new(stdout);
        consume_lines(reader, tx).await;
        // Reap the child and inspect its exit status. On non-zero, surface
        // captured stderr (or a generic message) into the failure slot so
        // the TUI poll loop can toast it.
        let exit_status = child.wait().await;
        let stderr_text = match stderr_handle {
            Some(h) => h.await.unwrap_or_default(),
            None => String::new(),
        };
        let nonzero = match exit_status {
            Ok(s) => !s.success(),
            Err(_) => true,
        };
        if nonzero {
            let trimmed = stderr_text.trim();
            let msg = if trimmed.is_empty() {
                "ralph runner exited with an error (no stderr output).".to_string()
            } else {
                // Take the last non-empty line — anyhow's error chain prints
                // the most-actionable message at the bottom, and toasts only
                // have room for a single line anyway.
                let last_line = trimmed
                    .lines()
                    .rev()
                    .find(|l| !l.trim().is_empty())
                    .unwrap_or(trimmed)
                    .trim()
                    .to_string();
                format!("Run failed: {last_line}")
            };
            if let Some(tx) = failure_tx.take() {
                let _ = tx.send(msg);
            }
        }
    });

    Ok(RunSubscription {
        rx,
        failure: failure_rx,
        _runtime: runtime,
    })
}

/// Read NDJSON lines from `reader`, parse each into a [`RunEvent`], and
/// forward through `tx`. Stops on EOF, reader error, or when the receiver
/// is dropped. Lines that fail to parse are silently skipped — the
/// runner's stdout may contain stray non-JSON lines (panics, crash backtraces),
/// and dropping a single line is preferable to tearing down the subscription.
pub async fn consume_lines<R>(reader: R, tx: UnboundedSender<RunEvent>)
where
    R: AsyncBufRead + Unpin,
{
    let mut lines = reader.lines();
    while let Ok(Some(line)) = lines.next_line().await {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<RunEvent>(trimmed) {
            Ok(evt) => {
                if tx.send(evt).is_err() {
                    break;
                }
            }
            Err(_) => continue,
        }
    }
}

// ---------------------------------------------------------------------------
// Dispatch — apply a single event to the App
// ---------------------------------------------------------------------------

/// Apply a single [`RunEvent`] to the plan-detail app, updating the
/// right-pane state (current phase, harness/test tail buffers, step
/// timer). Pure aside from the App mutation — no I/O.
///
/// Events that don't drive the right pane (e.g. `PromptPrepared`,
/// `StaleStepsSwept`) are observed but produce no state change.
pub fn dispatch_event(app: &mut PlanDetailApp, event: &RunEvent) {
    match event {
        RunEvent::RunStarted { started_at, .. } => {
            app.note_run_started(*started_at);
        }
        RunEvent::HarnessChunk { text, .. } => {
            app.push_harness_line(text.clone());
        }
        RunEvent::TestChunk { text, .. } => {
            app.push_test_line(text.clone());
        }
        RunEvent::PhaseChanged {
            phase,
            phase_started_at,
        } => {
            app.set_current_phase(*phase, *phase_started_at);
        }
        RunEvent::StepStarted { step_id, .. } => {
            app.note_step_started(step_id);
        }
        RunEvent::StepFinished { step_id, .. } => {
            app.note_step_finished(step_id);
        }
        RunEvent::PlanComplete { .. } | RunEvent::Summary { .. } => {
            app.note_run_finished();
        }
        RunEvent::PausedByUser { .. } => {
            app.note_run_finished();
            app.toasts.push(
                "Paused. Use `ralph resume` to continue.",
                crate::tui::toast::ToastKind::Success,
                std::time::Instant::now(),
            );
        }
        // Other events (PromptPrepared, StaleStepsSwept, PlanGrew) update
        // book-keeping handled by the DB-side sync; the right pane has no
        // dedicated rendering for them.
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::output::ChunkStream;
    use crate::plan::{Phase, Plan, PlanStatus, Step, StepStatus};
    use chrono::Utc;
    use tokio::io::AsyncWriteExt;

    fn make_plan() -> Plan {
        Plan {
            id: "p1".into(),
            slug: "live".into(),
            project: "/tmp".into(),
            branch_name: "live".into(),
            description: "live test plan".into(),
            status: PlanStatus::InProgress,
            harness: Some("claude".into()),
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
            review_enabled: None,
        }
    }

    fn make_step(id: &str) -> Step {
        Step {
            id: id.into(),
            short_id: String::new(),
            plan_id: "p1".into(),
            sort_key: "a0".into(),
            title: format!("step {id}"),
            description: String::new(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: StepStatus::Pending,
            attempts: 0,
            max_retries: Some(3),
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: crate::plan::ChangePolicy::Required,
            tags: vec![],
            retry_strategy: None,
            review_enabled: None,
            review_status: None,
            corrects_step_id: None,
        }
    }

    fn make_app() -> PlanDetailApp {
        PlanDetailApp::new(make_plan(), vec![make_step("s0")], &Config::default())
    }

    // -- dispatch_event --

    #[test]
    fn test_dispatch_harness_chunk_appends_to_tail() {
        let mut app = make_app();
        let evt = RunEvent::HarnessChunk {
            stream: ChunkStream::Stdout,
            text: "hello world".into(),
            seq: 0,
        };
        dispatch_event(&mut app, &evt);
        assert_eq!(app.harness_tail_lines(), &["hello world".to_string()]);
    }

    #[test]
    fn test_dispatch_test_chunk_appends_to_test_tail() {
        let mut app = make_app();
        let evt = RunEvent::TestChunk {
            test_index: 0,
            stream: ChunkStream::Stdout,
            text: "running tests...".into(),
            seq: 0,
        };
        dispatch_event(&mut app, &evt);
        assert_eq!(app.test_tail_lines(), &["running tests...".to_string()]);
    }

    #[test]
    fn test_dispatch_phase_changed_updates_phase() {
        let mut app = make_app();
        let evt = RunEvent::PhaseChanged {
            phase: Phase::Tests,
            phase_started_at: Utc::now(),
        };
        dispatch_event(&mut app, &evt);
        assert_eq!(app.current_phase(), Some(Phase::Tests));
    }

    #[test]
    fn test_dispatch_run_started_anchors_elapsed_timer() {
        let mut app = make_app();
        // Before `run_started`, the elapsed timer is zero and the run is
        // not considered live.
        assert!(!app.is_run_live());
        assert_eq!(app.elapsed_secs(), 0.0);

        // 5 seconds in the past so the timer reads ~5.
        let started_at = Utc::now() - chrono::Duration::seconds(5);
        let evt = RunEvent::RunStarted {
            plan_slug: "live".into(),
            started_at,
        };
        dispatch_event(&mut app, &evt);

        assert!(app.is_run_live());
        let elapsed = app.elapsed_secs();
        assert!(
            (4.0..=6.0).contains(&elapsed),
            "elapsed should reflect time since run_started, got {elapsed}"
        );
    }

    #[test]
    fn test_dispatch_phase_changed_overrides_run_started_for_timer() {
        let mut app = make_app();
        // `run_started` 60s ago, then `phase_changed` 2s ago. The timer
        // should reflect the phase boundary (per-step granularity), not the
        // whole-run elapsed.
        let run_started = Utc::now() - chrono::Duration::seconds(60);
        dispatch_event(
            &mut app,
            &RunEvent::RunStarted {
                plan_slug: "live".into(),
                started_at: run_started,
            },
        );
        let phase_started = Utc::now() - chrono::Duration::seconds(2);
        dispatch_event(
            &mut app,
            &RunEvent::PhaseChanged {
                phase: Phase::Harness,
                phase_started_at: phase_started,
            },
        );

        let elapsed = app.elapsed_secs();
        assert!(
            (1.0..=3.0).contains(&elapsed),
            "phase-elapsed should win over run-elapsed, got {elapsed}"
        );
    }

    #[test]
    fn test_dispatch_step_started_marks_run_live() {
        let mut app = make_app();
        assert!(!app.is_run_live());
        let evt = RunEvent::StepStarted {
            step_id: "s0".into(),
            step_title: "step s0".into(),
            step_num: 1,
            step_total: 1,
        };
        dispatch_event(&mut app, &evt);
        assert!(app.is_run_live());
        assert_eq!(app.subscribed_step_num, Some(1));
    }

    #[test]
    fn test_dispatch_plan_complete_clears_subscription_state() {
        let mut app = make_app();
        dispatch_event(
            &mut app,
            &RunEvent::StepStarted {
                step_id: "s0".into(),
                step_title: "step s0".into(),
                step_num: 1,
                step_total: 1,
            },
        );
        assert!(app.is_run_live());
        dispatch_event(
            &mut app,
            &RunEvent::PlanComplete {
                plan_slug: "live".into(),
                final_status: PlanStatus::Complete,
                steps_executed: 1,
                steps_succeeded: 1,
                steps_failed: 0,
            },
        );
        assert!(!app.is_run_live());
        assert_eq!(app.subscribed_step_num, None);
    }

    // -- consume_lines integration with an in-memory pipe --

    /// Integration test (per TUI-plan.md §13): the fake runner is a
    /// `tokio::io::duplex` pipe playing the role of the runner's stdout.
    /// We write NDJSON lines into it; `consume_lines` parses them and
    /// pushes events through the channel; the TUI dispatcher applies
    /// them to the App. Verifies that the rendered tails reflect the
    /// streamed chunks in arrival order.
    #[tokio::test]
    async fn test_fake_runner_streams_into_app_via_dispatch() {
        let (mut writer, reader) = tokio::io::duplex(4096);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RunEvent>();
        let buf_reader = BufReader::new(reader);

        // Spawn the consumer in the background so we can write while it reads.
        let consumer = tokio::spawn(async move {
            consume_lines(buf_reader, tx).await;
        });

        // Emit a representative sequence: phase change → harness output →
        // phase change → test output → step finish.
        let lines = [
            r#"{"event":"phase_changed","phase":"harness","phase_started_at":"2026-04-22T18:00:00Z"}"#,
            r#"{"event":"harness_chunk","stream":"stdout","text":"compiling…","seq":0}"#,
            r#"{"event":"harness_chunk","stream":"stdout","text":"done.","seq":1}"#,
            r#"{"event":"phase_changed","phase":"tests","phase_started_at":"2026-04-22T18:00:05Z"}"#,
            r#"{"event":"test_chunk","test_index":0,"stream":"stdout","text":"PASS","seq":2}"#,
            r#"{"event":"step_finished","step_id":"s0","step_title":"step s0","step_num":1,"step_total":1,"outcome":"success","attempts":1,"duration_secs":1.5}"#,
        ];
        for line in lines {
            writer.write_all(line.as_bytes()).await.unwrap();
            writer.write_all(b"\n").await.unwrap();
        }
        // Drop the writer so the reader sees EOF.
        drop(writer);

        // Wait for the consumer to finish processing all lines.
        consumer.await.unwrap();

        // Drain into the App and verify the rendered state.
        let mut app = make_app();
        while let Ok(evt) = rx.try_recv() {
            dispatch_event(&mut app, &evt);
        }

        assert_eq!(app.current_phase(), Some(Phase::Tests));
        assert_eq!(
            app.harness_tail_lines(),
            &["compiling…".to_string(), "done.".to_string()]
        );
        assert_eq!(app.test_tail_lines(), &["PASS".to_string()]);
    }

    /// A line that fails to parse as a `RunEvent` is dropped without
    /// poisoning the stream. Ensures stray non-JSON output (panic
    /// backtraces, debug prints) doesn't kill the subscription.
    #[tokio::test]
    async fn test_consume_lines_skips_malformed_lines() {
        let (mut writer, reader) = tokio::io::duplex(4096);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<RunEvent>();
        let buf_reader = BufReader::new(reader);
        let consumer = tokio::spawn(async move {
            consume_lines(buf_reader, tx).await;
        });

        let lines = [
            "not json at all",
            r#"{"event":"phase_changed","phase":"harness","phase_started_at":"2026-04-22T18:00:00Z"}"#,
            r#"{"event":"unknown_kind"}"#, // serde rejects unknown variant
            r#"{"event":"harness_chunk","stream":"stdout","text":"ok","seq":0}"#,
        ];
        for line in lines {
            writer.write_all(line.as_bytes()).await.unwrap();
            writer.write_all(b"\n").await.unwrap();
        }
        drop(writer);
        consumer.await.unwrap();

        let mut count = 0usize;
        while rx.try_recv().is_ok() {
            count += 1;
        }
        assert_eq!(count, 2, "exactly 2 well-formed events should pass through");
    }

    /// A delivered failure message is surfaced exactly once.
    #[test]
    fn test_poll_failure_status_message_then_clean() {
        let runtime = Arc::new(Runtime::new().unwrap());
        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel::<RunEvent>();
        let (failure_tx, failure_rx) = oneshot::channel::<String>();
        failure_tx.send("preflight failed".to_string()).unwrap();
        let mut sub = RunSubscription {
            rx,
            failure: failure_rx,
            _runtime: runtime,
        };

        match sub.poll_failure_status() {
            FailureStatus::Message(msg) => assert_eq!(msg, "preflight failed"),
            FailureStatus::Pending => panic!("message should have been ready"),
            FailureStatus::Clean => panic!("failure channel should not look clean"),
        }
        assert!(
            matches!(sub.poll_failure_status(), FailureStatus::Clean),
            "once consumed, the oneshot receiver should settle to Clean"
        );
    }

    /// A live sender with no value yet is distinct from a clean exit.
    #[test]
    fn test_poll_failure_status_pending() {
        let runtime = Arc::new(Runtime::new().unwrap());
        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel::<RunEvent>();
        let (failure_tx, failure_rx) = oneshot::channel::<String>();
        let _keep_sender_alive = failure_tx;
        let mut sub = RunSubscription {
            rx,
            failure: failure_rx,
            _runtime: runtime,
        };
        assert!(matches!(sub.poll_failure_status(), FailureStatus::Pending));
    }

    /// Dropping the sender without sending simulates a clean exit.
    #[test]
    fn test_poll_failure_status_clean_run() {
        let runtime = Arc::new(Runtime::new().unwrap());
        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel::<RunEvent>();
        let (failure_tx, failure_rx) = oneshot::channel::<String>();
        drop(failure_tx);
        let mut sub = RunSubscription {
            rx,
            failure: failure_rx,
            _runtime: runtime,
        };
        assert!(matches!(sub.poll_failure_status(), FailureStatus::Clean));
    }

    /// `RunSubscription::drain` returns an empty Vec when the producer
    /// has hung up and there are no pending events. `is_disconnected`
    /// flips to true once the channel is empty AND closed.
    #[test]
    fn test_run_subscription_disconnect_after_drain() {
        let runtime = Arc::new(Runtime::new().unwrap());
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let (_failure_tx, failure_rx) = oneshot::channel::<String>();
        let mut sub = RunSubscription {
            rx,
            failure: failure_rx,
            _runtime: runtime,
        };

        tx.send(RunEvent::PhaseChanged {
            phase: Phase::Harness,
            phase_started_at: Utc::now(),
        })
        .unwrap();
        // Producer hangs up.
        drop(tx);

        let drained = sub.drain();
        assert_eq!(drained.len(), 1);
        // Channel is now drained AND closed.
        assert!(sub.is_disconnected());
    }

    // -- build_streaming_run_command --

    #[test]
    fn test_build_streaming_run_command_args_layout() {
        let cmd = build_streaming_run_command(
            Path::new("/usr/bin/ralph"),
            Path::new("/proj"),
            "my-plan",
            StreamMode::Run {
                current_branch: true,
                no_auto_stash: false,
            },
        );
        let std_cmd = cmd.as_std();
        let args: Vec<&std::ffi::OsStr> = std_cmd.get_args().collect();
        let args: Vec<String> = args
            .into_iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec![
                "-C".to_string(),
                "/proj".to_string(),
                "--non-interactive".to_string(),
                "--json".to_string(),
                "run".to_string(),
                "--current-branch".to_string(),
                "my-plan".to_string(),
            ]
        );
    }

    #[test]
    fn test_build_streaming_run_command_omits_current_branch() {
        let cmd = build_streaming_run_command(
            Path::new("/usr/bin/ralph"),
            Path::new("/proj"),
            "my-plan",
            StreamMode::Run {
                current_branch: false,
                no_auto_stash: false,
            },
        );
        let std_cmd = cmd.as_std();
        let args: Vec<String> = std_cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(!args.contains(&"--current-branch".to_string()));
        assert!(!args.contains(&"--no-auto-stash".to_string()));
        // slug is still the final arg
        assert_eq!(args.last().map(String::as_str), Some("my-plan"));
    }

    /// `--no-auto-stash` is a behavior knob threaded through to the spawned
    /// runner — when set, the streaming command emits the flag after
    /// `--current-branch` (if present) and before the slug.
    #[test]
    fn test_build_streaming_run_command_with_no_auto_stash() {
        let cmd = build_streaming_run_command(
            Path::new("/usr/bin/ralph"),
            Path::new("/proj"),
            "my-plan",
            StreamMode::Run {
                current_branch: false,
                no_auto_stash: true,
            },
        );
        let std_cmd = cmd.as_std();
        let args: Vec<String> = std_cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec![
                "-C".to_string(),
                "/proj".to_string(),
                "--non-interactive".to_string(),
                "--json".to_string(),
                "run".to_string(),
                "--no-auto-stash".to_string(),
                "my-plan".to_string(),
            ]
        );
    }

    /// Both behavior knobs together: `--current-branch` precedes
    /// `--no-auto-stash`, both precede the slug.
    #[test]
    fn test_build_streaming_run_command_with_both_flags() {
        let cmd = build_streaming_run_command(
            Path::new("/usr/bin/ralph"),
            Path::new("/proj"),
            "my-plan",
            StreamMode::Run {
                current_branch: true,
                no_auto_stash: true,
            },
        );
        let std_cmd = cmd.as_std();
        let args: Vec<String> = std_cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec![
                "-C".to_string(),
                "/proj".to_string(),
                "--non-interactive".to_string(),
                "--json".to_string(),
                "run".to_string(),
                "--current-branch".to_string(),
                "--no-auto-stash".to_string(),
                "my-plan".to_string(),
            ]
        );
    }

    /// Resume streaming spawns `ralph resume <slug>` with `--non-interactive
    /// --json`. The auto-start path on `ralph resume` (TUI-plan.md §2)
    /// reuses the same NDJSON wiring as `run`, so the only on-the-wire
    /// difference is the subcommand itself.
    #[test]
    fn test_build_streaming_run_command_resume_subcommand() {
        let cmd = build_streaming_run_command(
            Path::new("/usr/bin/ralph"),
            Path::new("/proj"),
            "my-plan",
            StreamMode::Resume,
        );
        let std_cmd = cmd.as_std();
        let args: Vec<String> = std_cmd
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec![
                "-C".to_string(),
                "/proj".to_string(),
                "--non-interactive".to_string(),
                "--json".to_string(),
                "resume".to_string(),
                "my-plan".to_string(),
            ]
        );
        // `--current-branch` is implicit in resume — must NOT appear as a
        // CLI flag (resume::resume_plan sets it on RunOptions itself).
        assert!(!args.contains(&"--current-branch".to_string()));
    }
}
