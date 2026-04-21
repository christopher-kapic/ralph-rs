// Per-project run lock.
//
// Prevents two concurrent `ralph run` invocations from executing against the
// same project directory. Uses a SQLite row keyed on absolute project path
// plus a PID liveness check to recover from crashed runs.

use std::process::Command;
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, OptionalExtension, Row, params};

use crate::db;
use crate::plan::Phase;

/// Canonical `SELECT` list for hydrating a [`LiveRun`] from the `run_locks`
/// table. Defines the authoritative column order so [`LiveRun::from_row`]'s
/// positional `row.get(N)` calls line up. Mirrors the `PLAN_COLUMNS` pattern
/// in `plan.rs`.
///
/// Read by `ralph cancel` and `ralph status` live view.
#[allow(dead_code)]
pub const LIVE_RUN_COLUMNS: &str = "project, pid, pid_start_token, plan_id, plan_slug, started_at, \
     step_id, step_num, attempt, max_attempts, phase, phase_started_at, current_command, \
     execution_log_id, child_pid, child_start_token, updated_at";

/// Snapshot of the currently-held run lock for a project, including every
/// observability column added in migration V11. Timestamps are kept as raw
/// strings so this struct is a thin mirror of the on-disk row; callers that
/// render times (status, cancel, the TUI) can parse them as they see fit.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct LiveRun {
    pub project: String,
    pub pid: i64,
    pub pid_start_token: Option<String>,
    pub plan_id: Option<String>,
    pub plan_slug: Option<String>,
    pub started_at: String,
    pub step_id: Option<String>,
    pub step_num: Option<i32>,
    pub attempt: Option<i32>,
    pub max_attempts: Option<i32>,
    pub phase: Option<Phase>,
    pub phase_started_at: Option<String>,
    pub current_command: Option<String>,
    pub execution_log_id: Option<i64>,
    pub child_pid: Option<i64>,
    pub child_start_token: Option<String>,
    pub updated_at: Option<String>,
}

impl LiveRun {
    /// Read a `LiveRun` from a SQLite row.
    ///
    /// Expected column order matches [`LIVE_RUN_COLUMNS`]. Phase parsing
    /// converts `FromStr` failures to
    /// [`rusqlite::Error::FromSqlConversionFailure`] so a malformed `phase`
    /// value surfaces as a type-conversion error rather than a silent
    /// default — this mirrors how `ExecutionLog::from_row` validates its
    /// `termination_reason` and `test_status` columns.
    #[allow(dead_code)]
    pub fn from_row(row: &Row<'_>) -> rusqlite::Result<Self> {
        let phase_str: Option<String> = row.get(10)?;
        let phase = match phase_str {
            Some(s) => Some(s.parse::<Phase>().map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    10,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?),
            None => None,
        };

        Ok(LiveRun {
            project: row.get(0)?,
            pid: row.get(1)?,
            pid_start_token: row.get(2)?,
            plan_id: row.get(3)?,
            plan_slug: row.get(4)?,
            started_at: row.get(5)?,
            step_id: row.get(6)?,
            step_num: row.get(7)?,
            attempt: row.get(8)?,
            max_attempts: row.get(9)?,
            phase,
            phase_started_at: row.get(11)?,
            current_command: row.get(12)?,
            execution_log_id: row.get(13)?,
            child_pid: row.get(14)?,
            child_start_token: row.get(15)?,
            updated_at: row.get(16)?,
        })
    }
}

type ReleaseFn = Box<dyn FnOnce(&str) -> Result<()> + Send>;

/// RAII guard that releases a run-lock row when dropped. The release strategy
/// is injected at construction time so tests can swap in a no-op closure that
/// doesn't touch the on-disk database.
pub struct RunLock {
    project: String,
    release: Option<ReleaseFn>,
    /// Whether normal Drop should also clear the forced-exit cleanup slot in
    /// `signal`. Only the production [`acquire`] registers that cleanup, so
    /// tests using [`acquire_inner`] leave it false to avoid disturbing any
    /// cleanup another parallel test has registered.
    clears_exit_cleanup: bool,
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
        if self.clears_exit_cleanup {
            crate::signal::clear_exit_cleanup();
        }
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
            if self.clears_exit_cleanup {
                crate::signal::clear_exit_cleanup();
            }
        }
    }
}

/// Escape a SQLite single-quoted string literal by doubling embedded quotes.
/// Used only to build a copy-pasteable recovery command in the error message;
/// all real queries use bound parameters.
fn sql_escape_single_quotes(s: &str) -> String {
    s.replace('\'', "''")
}

/// Delete this process's run_locks row for `project` using an already-open
/// connection. Shared by the normal Drop-path release closure and the
/// forced-exit cleanup so both behave identically and neither reopens the DB.
fn release_row_with_conn(conn: &Connection, project: &str) -> Result<()> {
    let my_pid = std::process::id() as i64;
    conn.execute(
        "DELETE FROM run_locks WHERE project = ?1 AND pid = ?2",
        params![project, my_pid],
    )
    .context("deleting run_locks row")?;
    Ok(())
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
    // Open one dedicated connection at acquire time and share it between the
    // Drop-path release closure and the forced-exit cleanup. Keeping the
    // connection alive for the guard's lifetime means neither release path
    // has to reopen the database (which could fail if the file is briefly
    // unavailable). The caller's `conn` is a borrow we can't stash, so we
    // open our own here.
    let release_conn = Connection::open(db::db_path()?)
        .with_context(|| "opening database for run lock release")?;
    release_conn.execute("PRAGMA foreign_keys = ON;", [])?;
    let release_conn = Arc::new(StdMutex::new(release_conn));

    let release_conn_for_drop = Arc::clone(&release_conn);
    let release: ReleaseFn = Box::new(move |project| {
        let conn = release_conn_for_drop
            .lock()
            .map_err(|e| anyhow::anyhow!("run lock release connection poisoned: {e}"))?;
        release_row_with_conn(&conn, project)
    });
    let mut lock = acquire_inner(conn, project, plan_slug, plan_id, force, release)?;

    // Register a forced-exit cleanup so a double Ctrl+C (which calls
    // std::process::exit(130) and skips Drop) still releases the lock.
    let project_owned = project.to_string();
    let release_conn_for_signal = Arc::clone(&release_conn);
    crate::signal::set_exit_cleanup(Box::new(move || {
        let conn = match release_conn_for_signal.lock() {
            Ok(c) => c,
            Err(e) => {
                eprintln!("warning: run lock release connection poisoned during forced exit: {e}");
                return;
            }
        };
        if let Err(e) = release_row_with_conn(&conn, &project_owned) {
            eprintln!(
                "warning: failed to release run lock during forced exit for {}: {}",
                project_owned, e
            );
        }
    }));
    lock.clears_exit_cleanup = true;
    Ok(lock)
}

/// Core acquire logic parameterized by the release closure. Called directly by
/// tests so they can inject a no-op release and avoid touching the real DB.
///
/// The entire query → liveness-check → delete → insert sequence runs inside a
/// `BEGIN IMMEDIATE` transaction so two concurrent acquirers cannot both pass
/// the liveness check and both insert a row.
fn acquire_inner(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    plan_id: Option<&str>,
    force: bool,
    release: ReleaseFn,
) -> Result<RunLock> {
    // Make concurrent acquirers (separate processes / connections) wait for a
    // held transaction instead of immediately erroring with SQLITE_BUSY.
    let _ = conn.busy_timeout(std::time::Duration::from_secs(5));

    conn.execute_batch("BEGIN IMMEDIATE;")
        .context("beginning run-lock acquisition transaction")?;

    let result = acquire_txn(conn, project, plan_slug, plan_id, force);

    match &result {
        Ok(()) => {
            conn.execute_batch("COMMIT;")
                .context("committing run-lock acquisition transaction")?;
        }
        Err(_) => {
            let _ = conn.execute_batch("ROLLBACK;");
        }
    }

    result?;

    Ok(RunLock {
        project: project.to_string(),
        release: Some(release),
        clears_exit_cleanup: false,
    })
}

/// Body of the acquire transaction. Separated so the outer wrapper can funnel
/// every exit path through COMMIT/ROLLBACK.
fn acquire_txn(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    plan_id: Option<&str>,
    force: bool,
) -> Result<()> {
    let my_pid = std::process::id() as i64;
    let my_start_token = process_start_token(my_pid);

    if force {
        conn.execute("DELETE FROM run_locks WHERE project = ?1", params![project])
            .context("clearing run_locks row for --force")?;
    } else {
        let existing: Option<(i64, Option<String>, Option<String>, String)> = conn
            .query_row(
                "SELECT pid, pid_start_token, plan_slug, started_at FROM run_locks WHERE project = ?1",
                params![project],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
            )
            .optional()
            .context("querying run_locks")?;

        if let Some((pid, stored_start_token, existing_slug, started_at)) = existing {
            if is_same_live_process(pid, stored_start_token.as_deref()) {
                let plan_label = existing_slug.as_deref().unwrap_or("<all plans>");
                let db_path = db::db_path()?;
                let db_path_display = db_path.display();
                let project_sql = sql_escape_single_quotes(project);
                bail!(
                    "Another ralph run is already active in this project (pid {pid}, plan {plan_label}, started {started_at}).\n\
                     If the previous run crashed, re-run with --force to reclaim the lock, or\n\
                     manually remove the row: sqlite3 {db_path_display} \"DELETE FROM run_locks WHERE project = '{project_sql}';\""
                );
            }
            conn.execute("DELETE FROM run_locks WHERE project = ?1", params![project])
                .context("clearing stale run_locks row")?;
        }
    }

    conn.execute(
        "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4, ?5)",
        params![project, my_pid, my_start_token, plan_id, plan_slug],
    )
    .context("inserting run_locks row")?;

    Ok(())
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

/// Returns true only if `pid` is alive AND the current process at that pid is
/// the same one that wrote the lock row. Without the second check, a recycled
/// pid — i.e. an unrelated live process that inherited the dead runner's pid —
/// would make `pid_is_alive` falsely report the lock as active and block the
/// new run forever. When `stored_start_token` is `None` (pre-v9 row or
/// unknown-platform writer) we fall back to liveness alone; when we can't
/// read the live process's current token we conservatively assume same
/// process, because blocking a duplicate run is strictly safer than
/// clobbering a live one.
fn is_same_live_process(pid: i64, stored_start_token: Option<&str>) -> bool {
    if !pid_is_alive(pid) {
        return false;
    }
    let Some(expected) = stored_start_token else {
        return true;
    };
    match process_start_token(pid) {
        Some(current) => current == expected,
        None => true,
    }
}

/// Returns a stable identifier for the process `pid`'s lifetime — distinct
/// across every `pid` reuse. On Linux reads field 22 (starttime in clock
/// ticks since boot) from `/proc/<pid>/stat`. On other Unix falls back to
/// `ps -o lstart=`. On non-Unix platforms returns `None`. Callers that get
/// `None` should fall back to liveness-only checking (and, for child
/// processes, record the token as `NULL` in the run_locks row).
///
/// Exposed to `executor.rs` so it can compute the token for a freshly-spawned
/// harness child. The format is platform-specific and opaque — compare tokens
/// for equality, don't parse them.
pub(crate) fn process_start_token(pid: i64) -> Option<String> {
    if pid <= 0 {
        return None;
    }
    #[cfg(target_os = "linux")]
    {
        let contents = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
        // `comm` (field 2) is wrapped in parens and may itself contain
        // whitespace or parens, so split on the LAST ')' to skip over it
        // safely. After that, fields[0] is `state` (field 3), fields[1] is
        // `ppid` (field 4), …, fields[19] is `starttime` (field 22).
        let after_comm = contents.rfind(')')?;
        let rest = contents[after_comm + 1..].trim_start();
        rest.split_whitespace().nth(19).map(|s| s.to_string())
    }
    #[cfg(all(unix, not(target_os = "linux")))]
    {
        let output = Command::new("ps")
            .args(["-o", "lstart=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        if !output.status.success() {
            return None;
        }
        let s = String::from_utf8(output.stdout).ok()?;
        let s = s.trim();
        if s.is_empty() {
            None
        } else {
            Some(s.to_string())
        }
    }
    #[cfg(not(unix))]
    {
        None
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
    fn double_signal_cleanup_clears_run_lock_row() {
        use std::sync::Mutex as StdMutex;

        let _guard = crate::signal::EXIT_CLEANUP_TEST_LOCK.lock().unwrap();
        crate::signal::clear_exit_cleanup();

        let conn = mem_db();
        let project = "/tmp/proj-m10";
        let my_pid = std::process::id() as i64;
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params![project, my_pid, "p-m10", "slug-m10"],
        )
        .unwrap();

        let count_before: i64 = conn
            .query_row("SELECT COUNT(*) FROM run_locks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count_before, 1);

        // Share the in-memory conn with the cleanup closure. We can't use the
        // production path (it reopens db::db_path()), so we inject an
        // equivalent DELETE targeting the same row.
        let shared = Arc::new(StdMutex::new(conn));
        let shared_for_cleanup = Arc::clone(&shared);
        let project_owned = project.to_string();
        crate::signal::set_exit_cleanup(Box::new(move || {
            let conn = shared_for_cleanup.lock().unwrap();
            conn.execute(
                "DELETE FROM run_locks WHERE project = ?1",
                params![project_owned],
            )
            .unwrap();
        }));

        // Simulate the second-Ctrl+C path: the signal listener runs the
        // registered cleanup right before std::process::exit(130).
        crate::signal::run_exit_cleanup();

        let count_after: i64 = shared
            .lock()
            .unwrap()
            .query_row("SELECT COUNT(*) FROM run_locks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count_after, 0,
            "run lock row should have been released by the forced-exit cleanup"
        );
    }

    #[test]
    fn error_message_escapes_apostrophe_in_project_path() {
        // Pre-seed a row under our own pid so the second acquire takes the
        // "live pid" branch that formats the recovery SQL.
        let conn = mem_db();
        let project = "/tmp/bob's proj";
        let my_pid = std::process::id() as i64;
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params![project, my_pid, "p1", "feat"],
        )
        .unwrap();

        let err = acquire_inner(
            &conn,
            project,
            Some("feat"),
            Some("p1"),
            false,
            noop_release(),
        )
        .expect_err("should bail because pid is live");
        let msg = format!("{err}");

        // The SQL literal in the suggestion must double-quote the embedded
        // apostrophe so a copy-paste doesn't break or become destructive.
        assert!(
            msg.contains("project = '/tmp/bob''s proj';"),
            "expected escaped project literal in suggestion, got: {msg}"
        );
    }

    #[test]
    fn sql_escape_single_quotes_doubles_apostrophes() {
        assert_eq!(sql_escape_single_quotes("no-quotes"), "no-quotes");
        assert_eq!(sql_escape_single_quotes("bob's"), "bob''s");
        assert_eq!(sql_escape_single_quotes("''"), "''''");
        assert_eq!(sql_escape_single_quotes(""), "");
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

    #[test]
    fn is_same_live_process_missing_stored_token_falls_back_to_liveness() {
        // Pre-v9 rows lack a token — we must not treat that as a mismatch, or
        // every upgrade would spuriously reclaim a live lock.
        assert!(is_same_live_process(std::process::id() as i64, None));
        assert!(!is_same_live_process(0x7FFF_FFFE, None));
    }

    /// Linux-gated because reliable PID→start-token resolution happens via
    /// `/proc/<pid>/stat`. On other platforms we fall back to `ps -o lstart=`
    /// (or `None` on Windows) and the conservative-same-process branch hides
    /// the mismatch we want to exercise.
    #[cfg(target_os = "linux")]
    #[test]
    fn pid_reuse_with_mismatched_start_token_reclaims_lock() {
        // Simulate the PID-reuse scenario: an earlier ralph process had the
        // same pid we now have but a different start token. Liveness alone
        // (`kill -0 <my_pid>`) would report the lock as active; the start
        // token comparison must catch the reuse and let us reclaim.
        let conn = mem_db();
        let my_pid = std::process::id() as i64;
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![
                "/tmp/proj-pidreuse",
                my_pid,
                "fabricated-stale-token",
                "p-old",
                "old",
            ],
        )
        .unwrap();

        let _lock = acquire_inner(
            &conn,
            "/tmp/proj-pidreuse",
            Some("new"),
            Some("p-new"),
            false,
            noop_release(),
        )
        .expect(
            "acquire should reclaim: pid matches but the recorded start token \
             differs from the live process's current start token",
        );

        // The row should now reflect our identity, with a real token (Some).
        let (pid, token, slug): (i64, Option<String>, String) = conn
            .query_row(
                "SELECT pid, pid_start_token, plan_slug FROM run_locks WHERE project = ?1",
                params!["/tmp/proj-pidreuse"],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(pid, my_pid);
        assert_eq!(slug, "new");
        assert!(
            token.is_some() && token.as_deref() != Some("fabricated-stale-token"),
            "stored token should be the live process's real token, got {token:?}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn process_start_token_self_is_stable_and_nonempty() {
        let me = std::process::id() as i64;
        let a = process_start_token(me).expect("linux /proc should yield a token");
        let b = process_start_token(me).expect("linux /proc should yield a token");
        assert!(!a.is_empty());
        assert_eq!(
            a, b,
            "start token for the same process should be stable across reads"
        );
        // Dead/invalid pids must not surface a token.
        assert_eq!(process_start_token(0), None);
        assert_eq!(process_start_token(-1), None);
        assert_eq!(process_start_token(0x7FFF_FFFE), None);
    }

    /// Opens a file-backed connection with just the `run_locks` table so the
    /// concurrent-acquire test can drive multiple connections against the same
    /// underlying database. Mirrors migrations v4 + v9.
    fn open_file_db(path: &std::path::Path) -> Connection {
        let conn = Connection::open(path).expect("open file db");
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(
            "
            CREATE TABLE IF NOT EXISTS run_locks (
                project TEXT PRIMARY KEY,
                pid INTEGER NOT NULL,
                plan_id TEXT,
                plan_slug TEXT,
                started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
                pid_start_token TEXT
            );
            ",
        )
        .unwrap();
        conn
    }

    #[test]
    fn release_closure_reuses_shared_connection() {
        // Mirrors the production pattern: open a dedicated connection at
        // acquire time, wrap it in Arc<Mutex<_>>, and hand clones into both
        // the Drop-path release closure and (conceptually) the forced-exit
        // cleanup. Verifying this here ensures the release path deletes the
        // row via the shared conn — without opening a new one.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("shared.db");

        // Main working connection (what acquire_inner drives).
        let conn = open_file_db(&db_path);

        // Dedicated release connection to the same DB file, shared via Arc.
        let release_conn = Arc::new(StdMutex::new(open_file_db(&db_path)));
        let release_conn_for_drop = Arc::clone(&release_conn);
        let release: ReleaseFn = Box::new(move |project| {
            let c = release_conn_for_drop
                .lock()
                .map_err(|e| anyhow::anyhow!("poisoned: {e}"))?;
            release_row_with_conn(&c, project)
        });

        {
            let _lock = acquire_inner(
                &conn,
                "/tmp/proj-share",
                Some("x"),
                Some("p"),
                false,
                release,
            )
            .expect("acquire");
        }

        // Row deleted by the release closure using the shared conn.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run_locks WHERE project = ?1",
                params!["/tmp/proj-share"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 0,
            "release closure should have deleted the row via the shared connection"
        );

        // The Arc still holds the release conn (only one ref now, since the
        // release closure was consumed by Drop). Closing it explicitly is
        // optional — it drops with the test scope.
        drop(release_conn);
    }

    #[test]
    fn release_row_with_conn_is_pid_scoped() {
        // A stray row owned by another pid must survive our release call so
        // concurrent processes don't clobber each other's locks.
        let conn = mem_db();
        let other_pid: i64 = 0x7FFF_FFFD;
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params!["/tmp/proj-other", other_pid, "p-other", "slug-other"],
        )
        .unwrap();

        release_row_with_conn(&conn, "/tmp/proj-other").expect("release");

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run_locks WHERE project = ?1",
                params!["/tmp/proj-other"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "release must only delete rows for our own pid");
    }

    #[test]
    fn concurrent_acquirers_leave_exactly_one_lock_row() {
        // N threads each open their own connection to the same file DB and
        // race to acquire the same project lock. Because all threads share
        // this test process's pid, every loser hits the live-pid branch and
        // bails with the "Another ralph run is already active" error. The
        // BEGIN IMMEDIATE wrapping ensures only one winner inserts a row.
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("stress.db");

        // Initialize schema up front so threads race only the acquire path.
        drop(open_file_db(&db_path));

        const THREADS: usize = 16;
        let project = "/tmp/proj-stress";
        let barrier = Arc::new(std::sync::Barrier::new(THREADS));

        let mut handles = Vec::with_capacity(THREADS);
        for _ in 0..THREADS {
            let barrier = Arc::clone(&barrier);
            let path = db_path.clone();
            let project = project.to_string();
            handles.push(std::thread::spawn(move || -> Result<()> {
                let conn = open_file_db(&path);
                barrier.wait();
                let _lock = acquire_inner(
                    &conn,
                    &project,
                    Some("feat"),
                    Some("p1"),
                    false,
                    noop_release(),
                )?;
                // Hold the guard briefly so other threads observe the row.
                std::thread::sleep(std::time::Duration::from_millis(20));
                Ok(())
            }));
        }

        let mut successes = 0;
        let mut failures = 0;
        for h in handles {
            match h.join().expect("thread join") {
                Ok(()) => successes += 1,
                Err(e) => {
                    let msg = format!("{e}");
                    assert!(
                        msg.contains("Another ralph run is already active"),
                        "unexpected failure: {msg}"
                    );
                    failures += 1;
                }
            }
        }
        assert_eq!(successes, 1, "exactly one acquirer should have won");
        assert_eq!(failures, THREADS - 1, "every other acquirer should bail");

        // The winning guard was released when its thread exited (noop_release
        // leaves the row in place), so exactly one row remains.
        let conn = open_file_db(&db_path);
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run_locks WHERE project = ?1",
                params![project],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "race left behind a duplicate lock row");
    }

    #[test]
    fn live_run_from_row_hydrates_every_column() {
        let conn = mem_db();
        // Insert a row with every V11 column populated so from_row exercises
        // each positional getter.
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug,
                                    started_at, step_id, step_num, attempt, max_attempts,
                                    phase, phase_started_at, current_command, execution_log_id,
                                    child_pid, child_start_token, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10,
                     ?11, ?12, ?13, ?14, ?15, ?16, ?17)",
            params![
                "/proj-roundtrip",
                4242i64,
                "parent-token",
                "plan-uuid",
                "my-slug",
                "2026-04-21T00:00:00.000Z",
                "step-uuid",
                5i32,
                2i32,
                3i32,
                "commit",
                "2026-04-21T00:01:00.000Z",
                "git commit -m ...",
                17i64,
                99_999i64,
                "child-token",
                "2026-04-21T00:02:00.000Z",
            ],
        )
        .unwrap();

        let query = format!("SELECT {LIVE_RUN_COLUMNS} FROM run_locks WHERE project = ?1");
        let live = conn
            .query_row(&query, params!["/proj-roundtrip"], LiveRun::from_row)
            .expect("from_row");

        assert_eq!(live.project, "/proj-roundtrip");
        assert_eq!(live.pid, 4242);
        assert_eq!(live.pid_start_token.as_deref(), Some("parent-token"));
        assert_eq!(live.plan_id.as_deref(), Some("plan-uuid"));
        assert_eq!(live.plan_slug.as_deref(), Some("my-slug"));
        assert_eq!(live.started_at, "2026-04-21T00:00:00.000Z");
        assert_eq!(live.step_id.as_deref(), Some("step-uuid"));
        assert_eq!(live.step_num, Some(5));
        assert_eq!(live.attempt, Some(2));
        assert_eq!(live.max_attempts, Some(3));
        assert_eq!(live.phase, Some(Phase::Commit));
        assert_eq!(
            live.phase_started_at.as_deref(),
            Some("2026-04-21T00:01:00.000Z")
        );
        assert_eq!(live.current_command.as_deref(), Some("git commit -m ..."));
        assert_eq!(live.execution_log_id, Some(17));
        assert_eq!(live.child_pid, Some(99_999));
        assert_eq!(live.child_start_token.as_deref(), Some("child-token"));
        assert_eq!(
            live.updated_at.as_deref(),
            Some("2026-04-21T00:02:00.000Z")
        );
    }

    #[test]
    fn live_run_from_row_rejects_malformed_phase() {
        let conn = mem_db();
        conn.execute(
            "INSERT INTO run_locks (project, pid, phase) VALUES (?1, ?2, ?3)",
            params!["/proj-bad-phase", 1i64, "not-a-phase"],
        )
        .unwrap();

        let query = format!("SELECT {LIVE_RUN_COLUMNS} FROM run_locks WHERE project = ?1");
        let err = conn
            .query_row(&query, params!["/proj-bad-phase"], LiveRun::from_row)
            .expect_err("malformed phase should surface as a type-conversion error");
        assert!(
            matches!(err, rusqlite::Error::FromSqlConversionFailure(_, _, _)),
            "expected FromSqlConversionFailure, got {err:?}"
        );
    }
}
