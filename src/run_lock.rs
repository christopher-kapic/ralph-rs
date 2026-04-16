// Per-project run lock.
//
// Prevents two concurrent `ralph run` invocations from executing against the
// same project directory. Uses a SQLite row keyed on absolute project path
// plus a PID liveness check to recover from crashed runs.

use std::process::Command;

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, params};

use crate::db;

type ReleaseFn = Box<dyn FnOnce(&str) -> Result<()> + Send>;

/// RAII guard that releases a run-lock row when dropped. The release strategy
/// is injected at construction time so tests can swap in a no-op closure that
/// doesn't touch the on-disk database.
pub struct RunLock {
    project: String,
    release: Option<ReleaseFn>,
}

impl std::fmt::Debug for RunLock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunLock")
            .field("project", &self.project)
            .field("released", &self.release.is_none())
            .finish()
    }
}

impl RunLock {
    /// Explicitly release the lock (same effect as dropping the guard). Useful
    /// when you want to observe any error that occurs during release — Drop
    /// swallows errors.
    #[allow(dead_code)]
    pub fn release(mut self) -> Result<()> {
        if let Some(release) = self.release.take() {
            release(&self.project)
        } else {
            Ok(())
        }
    }
}

impl Drop for RunLock {
    fn drop(&mut self) {
        if let Some(release) = self.release.take() {
            if let Err(e) = release(&self.project) {
                eprintln!(
                    "warning: failed to release run lock for {}: {}",
                    self.project, e
                );
            }
        }
    }
}

/// Attempt to acquire the run lock for `project`. Returns an error if another
/// live ralph run already holds the lock; reclaims the row if the recorded
/// pid is no longer alive.
pub fn acquire(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    plan_id: Option<&str>,
    force: bool,
) -> Result<RunLock> {
    // Production release: reopen a fresh connection (the caller's `conn` is a
    // borrow, not an owned value we could stash in the guard) and pid-scope
    // the DELETE so a racing reclaim of a stale row can't be clobbered.
    let release: ReleaseFn = Box::new(|project: &str| {
        let conn = Connection::open(db::db_path()?)
            .with_context(|| "reopening database to release run lock")?;
        conn.execute("PRAGMA foreign_keys = ON;", [])?;
        let my_pid = std::process::id() as i64;
        conn.execute(
            "DELETE FROM run_locks WHERE project = ?1 AND pid = ?2",
            params![project, my_pid],
        )
        .context("deleting run_locks row")?;
        Ok(())
    });
    acquire_inner(conn, project, plan_slug, plan_id, force, release)
}

/// Core acquire logic parameterized by the release closure. Called directly by
/// tests so they can inject a no-op release and avoid touching the real DB.
fn acquire_inner(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    plan_id: Option<&str>,
    force: bool,
    release: ReleaseFn,
) -> Result<RunLock> {
    let my_pid = std::process::id() as i64;

    if force {
        conn.execute("DELETE FROM run_locks WHERE project = ?1", params![project])
            .context("clearing run_locks row for --force")?;
    } else {
        let existing: Option<(i64, Option<String>, String)> = conn
            .query_row(
                "SELECT pid, plan_slug, started_at FROM run_locks WHERE project = ?1",
                params![project],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .optional()
            .context("querying run_locks")?;

        if let Some((pid, existing_slug, started_at)) = existing {
            if pid_is_alive(pid) {
                let plan_label = existing_slug.as_deref().unwrap_or("<all plans>");
                let db_path = db::db_path()?;
                let db_path_display = db_path.display();
                bail!(
                    "Another ralph run is already active in this project (pid {pid}, plan {plan_label}, started {started_at}).\n\
                     If the previous run crashed, re-run with --force to reclaim the lock, or\n\
                     manually remove the row: sqlite3 {db_path_display} \"DELETE FROM run_locks WHERE project = '{project}';\""
                );
            }
            conn.execute("DELETE FROM run_locks WHERE project = ?1", params![project])
                .context("clearing stale run_locks row")?;
        }
    }

    conn.execute(
        "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
        params![project, my_pid, plan_id, plan_slug],
    )
    .context("inserting run_locks row")?;

    Ok(RunLock {
        project: project.to_string(),
        release: Some(release),
    })
}

/// Returns true if a process with `pid` is still running. Uses `kill -0`,
/// which works on every Unix without pulling in libc as a direct dependency.
/// On non-Unix platforms this conservatively returns true (better to block a
/// duplicate run than risk trampling a live one).
fn pid_is_alive(pid: i64) -> bool {
    if pid <= 0 {
        return false;
    }
    #[cfg(unix)]
    {
        Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false)
    }
    #[cfg(not(unix))]
    {
        let _ = Command::new("true").status();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    fn mem_db() -> Connection {
        db::open_memory().expect("open_memory")
    }

    /// No-op release closure for tests: avoids reopening the on-disk database
    /// when the guard drops.
    fn noop_release() -> ReleaseFn {
        Box::new(|_| Ok(()))
    }

    #[test]
    fn fresh_acquire_succeeds() {
        let conn = mem_db();
        let _lock = acquire_inner(
            &conn,
            "/tmp/proj-a",
            Some("feat-x"),
            Some("p1"),
            false,
            noop_release(),
        )
        .expect("acquire");
        // Row should exist.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run_locks WHERE project = ?1",
                params!["/tmp/proj-a"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn second_acquire_with_live_pid_fails() {
        let conn = mem_db();
        let _lock = acquire_inner(
            &conn,
            "/tmp/proj-b",
            Some("a"),
            Some("p1"),
            false,
            noop_release(),
        )
        .expect("first");
        // Second acquire while the first row still records the current pid.
        let err = acquire_inner(
            &conn,
            "/tmp/proj-b",
            Some("a"),
            Some("p1"),
            false,
            noop_release(),
        )
        .unwrap_err();
        let msg = format!("{err}");
        assert!(
            msg.contains("Another ralph run is already active"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn stale_lock_is_reclaimed() {
        let conn = mem_db();
        // Pre-seed a row with a pid that (almost) certainly isn't live.
        // 0x7FFFFFFE is within i32 range and outside any real pid space on
        // Linux (default pid_max is 32768–4194304).
        let stale_pid: i64 = 0x7FFF_FFFE;
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params!["/tmp/proj-c", stale_pid, "p-stale", "stale-plan"],
        )
        .unwrap();

        let _lock = acquire_inner(
            &conn,
            "/tmp/proj-c",
            Some("new"),
            Some("p-new"),
            false,
            noop_release(),
        )
        .expect("reclaim");
        let (pid, slug): (i64, String) = conn
            .query_row(
                "SELECT pid, plan_slug FROM run_locks WHERE project = ?1",
                params!["/tmp/proj-c"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(pid, std::process::id() as i64);
        assert_eq!(slug, "new");
    }

    #[test]
    fn force_reclaims_live_lock() {
        let conn = mem_db();
        let live_pid = std::process::id() as i64;
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params!["/tmp/proj-d", live_pid, "p-old", "old-plan"],
        )
        .unwrap();

        let _lock = acquire_inner(
            &conn,
            "/tmp/proj-d",
            Some("new"),
            Some("p-new"),
            true,
            noop_release(),
        )
        .expect("force acquire should bypass liveness check");
        let (pid, slug): (i64, String) = conn
            .query_row(
                "SELECT pid, plan_slug FROM run_locks WHERE project = ?1",
                params!["/tmp/proj-d"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(pid, std::process::id() as i64);
        assert_eq!(slug, "new");
    }

    #[test]
    fn drop_calls_release_closure() {
        let conn = mem_db();
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = Arc::clone(&flag);
        let release: ReleaseFn = Box::new(move |_project| {
            flag_clone.store(true, Ordering::SeqCst);
            Ok(())
        });
        {
            let _lock = acquire_inner(
                &conn,
                "/tmp/proj-e",
                Some("feat"),
                Some("p1"),
                false,
                release,
            )
            .expect("acquire");
            assert!(!flag.load(Ordering::SeqCst));
        }
        assert!(
            flag.load(Ordering::SeqCst),
            "drop should have invoked the release closure"
        );
    }

    #[test]
    fn run_blocks_concurrent_skip_on_same_project() {
        // Simulates `ralph run` holding the per-project lock while `ralph skip`
        // tries to mutate step status. In-process both calls see the same pid,
        // so the liveness check on the second acquire bails with the shared
        // "Another ralph run is already active" error.
        let conn = mem_db();

        let _run_guard = acquire_inner(
            &conn,
            "/tmp/proj-contend",
            Some("feat"),
            Some("p1"),
            false,
            noop_release(),
        )
        .expect("run acquires first");

        let err = acquire_inner(
            &conn,
            "/tmp/proj-contend",
            Some("feat"),
            Some("p1"),
            false,
            noop_release(),
        )
        .expect_err("skip must not be able to acquire while run holds the lock");
        let msg = format!("{err}");
        assert!(
            msg.contains("Another ralph run is already active"),
            "unexpected error: {msg}"
        );

        // Only one row exists — the winner's — so the loser didn't stomp state.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run_locks WHERE project = ?1",
                params!["/tmp/proj-contend"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn pid_is_alive_self() {
        assert!(pid_is_alive(std::process::id() as i64));
    }

    #[test]
    fn pid_is_alive_zero_or_negative() {
        assert!(!pid_is_alive(0));
        assert!(!pid_is_alive(-1));
    }
}
