// Read-only attach + edit lockdown (TUI-plan.md §13.2).
//
// When the TUI launches and finds a `run_locks` row owned by some other
// process — i.e. a `ralph run` started outside this TUI session — every
// edit keybinding is suppressed until that process releases the lock. A
// persistent banner replaces the per-view hint line so the user always
// sees who holds the lock and which keys still work; a toast announces
// the re-enable transition.
//
// This module owns the pure state machine and the DB-side detection
// helper. The dispatcher in `commands/run.rs` polls
// [`ReadOnlyTracker::should_poll`] each tick and feeds a fresh
// [`detect`] result into [`ReadOnlyTracker::observe`]; views consult
// [`ReadOnly`] from their App struct to decide whether to handle an
// edit key.

use std::time::{Duration, Instant};

use anyhow::Result;
use rusqlite::{Connection, OptionalExtension, params};

/// Cadence at which the dispatcher re-queries `run_locks` to look for
/// state changes. TUI-plan.md §13.2 specifies "poll every 500ms".
pub const POLL_INTERVAL: Duration = Duration::from_millis(500);

/// Toast pushed when an external lock holder releases the lock and edit
/// keys come back online.
pub const RELEASED_TOAST: &str = "Run finished — edits enabled.";

/// Whether the TUI is currently in read-only mode because an externally
/// spawned ralph runner holds this project's run lock.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadOnly {
    /// No external lock holder observed; edit keys are enabled.
    #[default]
    Editable,
    /// External lock holder observed; edit keys are suppressed and the
    /// banner displays the holder's PID.
    Locked { pid: i64 },
}

impl ReadOnly {
    /// True when edit keys should be ignored.
    pub fn is_locked(self) -> bool {
        matches!(self, ReadOnly::Locked { .. })
    }

    /// PID of the lock holder, when locked.
    pub fn pid(self) -> Option<i64> {
        match self {
            ReadOnly::Locked { pid } => Some(pid),
            ReadOnly::Editable => None,
        }
    }
}

/// Result of feeding a fresh observation into [`ReadOnlyTracker::observe`].
/// The dispatcher uses this to decide whether to push the
/// [`RELEASED_TOAST`] when edits come back online.
#[derive(Debug, PartialEq, Eq)]
pub enum Transition {
    /// State unchanged this tick.
    Unchanged,
    /// Lockdown engaged this tick (Editable → Locked).
    Engaged,
    /// Lockdown released this tick (Locked → Editable). The dispatcher
    /// pushes the "edits enabled" toast and re-enables edit keys.
    Released,
}

/// State-machine driver for the read-only banner.
///
/// Owns the cadence at which `run_locks` is polled and the toast emitted
/// on release. Constructed once per dispatcher and given an authoritative
/// `Instant::now()` on every tick so tests can drive a synthetic clock.
#[derive(Debug)]
pub struct ReadOnlyTracker {
    state: ReadOnly,
    last_poll: Option<Instant>,
}

impl ReadOnlyTracker {
    /// Construct a tracker pre-seeded with an initial observation. Callers
    /// typically pass [`ReadOnly::Editable`] and let the first poll discover
    /// any externally-held lock.
    pub fn new(initial: ReadOnly) -> Self {
        Self {
            state: initial,
            last_poll: None,
        }
    }

    /// Current state. Driven by [`Self::observe`].
    pub fn state(&self) -> ReadOnly {
        self.state
    }

    /// True when the dispatcher should run a fresh `run_locks` query: no
    /// poll has happened yet, or [`POLL_INTERVAL`] has elapsed since the
    /// most recent observation. The dispatcher reads this each tick and
    /// only calls [`detect`] when it returns `true`.
    pub fn should_poll(&self, now: Instant) -> bool {
        match self.last_poll {
            None => true,
            Some(t) => now.saturating_duration_since(t) >= POLL_INTERVAL,
        }
    }

    /// Apply a fresh observation and return the resulting transition. Updates
    /// the internal `last_poll` stamp so [`Self::should_poll`] gates the next
    /// query by the [`POLL_INTERVAL`] cadence.
    pub fn observe(&mut self, observed: ReadOnly, now: Instant) -> Transition {
        self.last_poll = Some(now);
        match (self.state, observed) {
            (ReadOnly::Editable, ReadOnly::Locked { pid }) => {
                self.state = ReadOnly::Locked { pid };
                Transition::Engaged
            }
            (ReadOnly::Locked { .. }, ReadOnly::Editable) => {
                self.state = ReadOnly::Editable;
                Transition::Released
            }
            (ReadOnly::Locked { pid: prev }, ReadOnly::Locked { pid: next }) if prev != next => {
                // External holder identity changed (rare — e.g. one runner
                // released and another grabbed the lock between polls). Stay
                // locked; dispatcher does not toast for this.
                self.state = ReadOnly::Locked { pid: next };
                Transition::Unchanged
            }
            _ => Transition::Unchanged,
        }
    }
}

/// Banner text shown across all views while in read-only mode. Returns
/// `None` when the TUI is not locked. The text matches the §13.2
/// specification verbatim: `🔒 Read-only — run in progress (PID <n>).
/// [S] cancel  [q] quit`.
pub fn banner(state: ReadOnly) -> Option<String> {
    state
        .pid()
        .map(|pid| format!("🔒 Read-only — run in progress (PID {pid}). [S] cancel  [q] quit"))
}

/// Detect read-only state by querying `run_locks` for the project. Returns
/// [`ReadOnly::Locked`] when a row exists owned by neither the TUI process
/// (`my_pid`) nor any TUI-spawned runner subprocess (`spawned_child_pid`).
///
/// The TUI's own pid is excluded so unit tests that share a process with a
/// real runner do not falsely trigger lockdown; the spawned child pid
/// covers the §13 streaming-runner case where the TUI itself launched
/// `ralph run --json` and is consuming its NDJSON.
pub fn detect(
    conn: &Connection,
    project: &str,
    my_pid: i64,
    spawned_child_pid: Option<i64>,
) -> Result<ReadOnly> {
    let row: Option<i64> = conn
        .query_row(
            "SELECT pid FROM run_locks WHERE project = ?1",
            params![project],
            |r| r.get(0),
        )
        .optional()?;
    Ok(match row {
        Some(pid) if pid != my_pid && Some(pid) != spawned_child_pid => {
            ReadOnly::Locked { pid }
        }
        _ => ReadOnly::Editable,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn t0() -> Instant {
        Instant::now()
    }

    // -- ReadOnly ---------------------------------------------------------

    #[test]
    fn editable_reports_not_locked_and_no_pid() {
        let s = ReadOnly::Editable;
        assert!(!s.is_locked());
        assert_eq!(s.pid(), None);
    }

    #[test]
    fn locked_reports_locked_and_pid() {
        let s = ReadOnly::Locked { pid: 42 };
        assert!(s.is_locked());
        assert_eq!(s.pid(), Some(42));
    }

    #[test]
    fn default_is_editable() {
        assert_eq!(ReadOnly::default(), ReadOnly::Editable);
    }

    // -- ReadOnlyTracker --------------------------------------------------

    #[test]
    fn new_tracker_should_poll_immediately() {
        let tracker = ReadOnlyTracker::new(ReadOnly::Editable);
        assert!(tracker.should_poll(t0()));
        assert_eq!(tracker.state(), ReadOnly::Editable);
    }

    #[test]
    fn observe_updates_last_poll_so_should_poll_returns_false_until_interval() {
        let mut tracker = ReadOnlyTracker::new(ReadOnly::Editable);
        let now = t0();
        tracker.observe(ReadOnly::Editable, now);
        // Just after observe — should not re-poll.
        assert!(!tracker.should_poll(now));
        assert!(!tracker.should_poll(now + Duration::from_millis(499)));
        // At/after the interval — should re-poll.
        assert!(tracker.should_poll(now + POLL_INTERVAL));
        assert!(tracker.should_poll(now + Duration::from_millis(750)));
    }

    #[test]
    fn observe_engaged_when_editable_to_locked() {
        let mut tracker = ReadOnlyTracker::new(ReadOnly::Editable);
        let t = tracker.observe(ReadOnly::Locked { pid: 7 }, t0());
        assert_eq!(t, Transition::Engaged);
        assert_eq!(tracker.state(), ReadOnly::Locked { pid: 7 });
    }

    #[test]
    fn observe_released_when_locked_to_editable() {
        let mut tracker = ReadOnlyTracker::new(ReadOnly::Locked { pid: 7 });
        let t = tracker.observe(ReadOnly::Editable, t0());
        assert_eq!(t, Transition::Released);
        assert_eq!(tracker.state(), ReadOnly::Editable);
    }

    #[test]
    fn observe_unchanged_when_state_steady() {
        let mut a = ReadOnlyTracker::new(ReadOnly::Editable);
        assert_eq!(a.observe(ReadOnly::Editable, t0()), Transition::Unchanged);

        let mut b = ReadOnlyTracker::new(ReadOnly::Locked { pid: 1 });
        assert_eq!(
            b.observe(ReadOnly::Locked { pid: 1 }, t0()),
            Transition::Unchanged
        );
    }

    #[test]
    fn observe_pid_change_is_unchanged_but_updates_pid() {
        // Rare but possible: one runner releases and another grabs the lock
        // between polls. We stay locked (no "edits enabled" toast) but the
        // banner pid updates.
        let mut tracker = ReadOnlyTracker::new(ReadOnly::Locked { pid: 1 });
        let t = tracker.observe(ReadOnly::Locked { pid: 2 }, t0());
        assert_eq!(t, Transition::Unchanged);
        assert_eq!(tracker.state(), ReadOnly::Locked { pid: 2 });
    }

    #[test]
    fn engage_then_release_drives_full_cycle() {
        let mut tracker = ReadOnlyTracker::new(ReadOnly::Editable);
        let now = t0();

        let t1 = tracker.observe(ReadOnly::Locked { pid: 99 }, now);
        assert_eq!(t1, Transition::Engaged);
        assert_eq!(tracker.state(), ReadOnly::Locked { pid: 99 });

        // Hold steady for one tick.
        let t2 = tracker.observe(ReadOnly::Locked { pid: 99 }, now + POLL_INTERVAL);
        assert_eq!(t2, Transition::Unchanged);
        assert_eq!(tracker.state(), ReadOnly::Locked { pid: 99 });

        // External runner exits.
        let t3 = tracker.observe(ReadOnly::Editable, now + 2 * POLL_INTERVAL);
        assert_eq!(t3, Transition::Released);
        assert_eq!(tracker.state(), ReadOnly::Editable);
    }

    // -- banner -----------------------------------------------------------

    #[test]
    fn banner_none_when_editable() {
        assert!(banner(ReadOnly::Editable).is_none());
    }

    #[test]
    fn banner_includes_pid_and_keybinding_hints() {
        let text = banner(ReadOnly::Locked { pid: 4242 }).expect("banner");
        assert!(text.contains("PID 4242"), "missing PID: {text}");
        assert!(text.contains("[S] cancel"), "missing [S]: {text}");
        assert!(text.contains("[q] quit"), "missing [q]: {text}");
        assert!(text.contains("Read-only"), "missing label: {text}");
    }

    // -- detect -----------------------------------------------------------

    fn mem_db() -> Connection {
        db::open_memory().expect("open_memory")
    }

    #[test]
    fn detect_returns_editable_when_no_row() {
        let conn = mem_db();
        let observed = detect(&conn, "/proj-empty", 1234, None).unwrap();
        assert_eq!(observed, ReadOnly::Editable);
    }

    #[test]
    fn detect_returns_editable_when_lock_belongs_to_us() {
        let conn = mem_db();
        let my_pid: i64 = 5555;
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params!["/proj-self", my_pid, "p1", "feat"],
        )
        .unwrap();
        let observed = detect(&conn, "/proj-self", my_pid, None).unwrap();
        assert_eq!(observed, ReadOnly::Editable);
    }

    #[test]
    fn detect_returns_editable_when_lock_belongs_to_our_spawned_child() {
        // §13: TUI launched the runner; the lock is held by the child pid
        // and we should NOT lock down our own UI.
        let conn = mem_db();
        let my_pid: i64 = 1;
        let child: i64 = 9999;
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params!["/proj-child", child, "p1", "feat"],
        )
        .unwrap();
        let observed = detect(&conn, "/proj-child", my_pid, Some(child)).unwrap();
        assert_eq!(observed, ReadOnly::Editable);
    }

    #[test]
    fn detect_returns_locked_for_external_pid() {
        let conn = mem_db();
        let external: i64 = 77;
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params!["/proj-ext", external, "p1", "feat"],
        )
        .unwrap();
        let observed = detect(&conn, "/proj-ext", 1, None).unwrap();
        assert_eq!(observed, ReadOnly::Locked { pid: external });
    }

    #[test]
    fn detect_locks_when_external_holds_despite_known_child() {
        // We track child pid X, but the lock is held by some unrelated Y.
        let conn = mem_db();
        let y: i64 = 88;
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params!["/proj-mix", y, "p1", "feat"],
        )
        .unwrap();
        let observed = detect(&conn, "/proj-mix", 1, Some(99)).unwrap();
        assert_eq!(observed, ReadOnly::Locked { pid: y });
    }

    #[test]
    fn detect_only_inspects_requested_project() {
        let conn = mem_db();
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params!["/proj-other", 77i64, "p1", "feat"],
        )
        .unwrap();
        // Querying a different project's row should not engage lockdown.
        let observed = detect(&conn, "/proj-mine", 1, None).unwrap();
        assert_eq!(observed, ReadOnly::Editable);
    }
}
