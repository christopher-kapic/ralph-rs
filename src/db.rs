// SQLite database layer

use anyhow::{Context, Result};
use rusqlite::Connection;
use std::fs;
use std::path::PathBuf;

use crate::config;

/// Each migration is a function that receives a connection (already inside a transaction).
/// Migrations are 1-indexed: MIGRATIONS[0] migrates from version 0 → 1.
const MIGRATIONS: &[fn(&Connection) -> Result<()>] = &[
    migrate_v1,
    migrate_v2,
    migrate_v3,
    migrate_v4,
    migrate_v5,
    migrate_v6,
    migrate_v7,
    migrate_v8,
    migrate_v9,
    migrate_v10,
    migrate_v11,
    migrate_v12,
    migrate_v13,
    migrate_v14,
    migrate_v15,
    migrate_v16,
    migrate_v17,
    migrate_v18,
    migrate_v19,
    migrate_v20,
    migrate_v21,
];

/// Current schema version — derived from the length of `MIGRATIONS` so that
/// adding a migration automatically bumps the version.
#[allow(dead_code)]
const CURRENT_VERSION: u32 = MIGRATIONS.len() as u32;

/// Returns the path to the SQLite database file.
pub fn db_path() -> Result<PathBuf> {
    let dir = config::data_dir()?;
    Ok(dir.join("ralph.db"))
}

/// Opens (or creates) the database and runs any pending migrations.
pub fn open() -> Result<Connection> {
    let path = db_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create data directory {}", parent.display()))?;
    }
    open_at(path)
}

/// Opens a database at the given path and runs migrations.
/// Useful for testing with a custom path.
fn open_at<P: AsRef<std::path::Path>>(path: P) -> Result<Connection> {
    let path = path.as_ref();
    let conn = Connection::open(path)
        .with_context(|| format!("Failed to open database at {}", path.display()))?;

    // Restrict to owner-only on Unix — the DB holds session ids, harness
    // output, diffs, and cost data that shouldn't be world-readable. Windows
    // relies on the user-profile directory ACL (per `dirs` crate guidance).
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))
            .with_context(|| format!("Failed to chmod database at {}", path.display()))?;
    }

    // Enable foreign keys — must happen outside any transaction and on every connection.
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;

    run_migrations(&conn)?;
    Ok(conn)
}

/// Opens an in-memory database with migrations applied. Used for tests.
#[allow(dead_code)]
pub fn open_memory() -> Result<Connection> {
    let conn = Connection::open_in_memory().context("Failed to open in-memory database")?;
    conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    run_migrations(&conn)?;
    Ok(conn)
}

/// Run all pending migrations in order, each inside its own transaction.
fn run_migrations(conn: &Connection) -> Result<()> {
    let current: u32 = conn.pragma_query_value(None, "user_version", |row| row.get(0))?;

    for (i, migration) in MIGRATIONS.iter().enumerate() {
        let version = (i as u32) + 1;
        if version <= current {
            continue;
        }
        conn.execute_batch("BEGIN;")?;
        match migration(conn) {
            Ok(()) => {
                conn.pragma_update(None, "user_version", version)?;
                conn.execute_batch("COMMIT;")?;
            }
            Err(e) => {
                let _ = conn.execute_batch("ROLLBACK;");
                return Err(e).with_context(|| format!("Migration to version {version} failed"));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V1: initial schema
// ---------------------------------------------------------------------------

fn migrate_v1(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        -- Plans
        CREATE TABLE plans (
            id TEXT PRIMARY KEY,
            slug TEXT NOT NULL,
            project TEXT NOT NULL,
            branch_name TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'planning',
            harness TEXT,
            agent TEXT,
            deterministic_tests TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            UNIQUE(slug, project)
        );

        CREATE INDEX idx_plans_project ON plans(project);
        CREATE INDEX idx_plans_project_status ON plans(project, status);

        -- Steps
        CREATE TABLE steps (
            id TEXT PRIMARY KEY,
            plan_id TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
            sort_key TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            agent TEXT,
            harness TEXT,
            acceptance_criteria TEXT NOT NULL DEFAULT '[]',
            status TEXT NOT NULL DEFAULT 'pending',
            attempts INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            UNIQUE(plan_id, sort_key)
        );

        CREATE INDEX idx_steps_plan_id ON steps(plan_id);
        CREATE INDEX idx_steps_plan_sort ON steps(plan_id, sort_key);

        -- Execution logs (one row per attempt)
        CREATE TABLE execution_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            step_id TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
            attempt INTEGER NOT NULL,
            started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            duration_secs REAL,
            prompt_text TEXT,
            diff TEXT,
            test_results TEXT NOT NULL DEFAULT '[]',
            rolled_back INTEGER NOT NULL DEFAULT 0,
            committed INTEGER NOT NULL DEFAULT 0,
            commit_hash TEXT,
            harness_stdout TEXT,
            harness_stderr TEXT,
            cost_usd REAL,
            input_tokens INTEGER,
            output_tokens INTEGER,
            session_id TEXT,
            UNIQUE(step_id, attempt)
        );

        CREATE INDEX idx_logs_step_id ON execution_logs(step_id);
        CREATE INDEX idx_logs_step_attempt ON execution_logs(step_id, attempt);
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V2: plan-level dependencies
// ---------------------------------------------------------------------------

fn migrate_v2(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        CREATE TABLE plan_dependencies (
            plan_id TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
            depends_on_plan_id TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            PRIMARY KEY (plan_id, depends_on_plan_id),
            CHECK (plan_id != depends_on_plan_id)
        );

        CREATE INDEX idx_plan_deps_plan ON plan_dependencies(plan_id);
        CREATE INDEX idx_plan_deps_dep  ON plan_dependencies(depends_on_plan_id);
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V3: hook associations (plan-wide + per-step)
// ---------------------------------------------------------------------------

fn migrate_v3(conn: &Connection) -> Result<()> {
    // `step_hooks` records which library-defined hook names apply at each
    // lifecycle event. `step_id NULL` means plan-wide (applies to every
    // step in the plan). The actual hook command/scope lives in the user's
    // hook library on disk, looked up by name at execution time.
    conn.execute_batch(
        "
        CREATE TABLE step_hooks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            plan_id TEXT NOT NULL REFERENCES plans(id) ON DELETE CASCADE,
            step_id TEXT REFERENCES steps(id) ON DELETE CASCADE,
            lifecycle TEXT NOT NULL,
            hook_name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        );

        CREATE INDEX idx_step_hooks_plan ON step_hooks(plan_id);
        CREATE INDEX idx_step_hooks_step ON step_hooks(step_id);
        CREATE INDEX idx_step_hooks_plan_lifecycle
            ON step_hooks(plan_id, lifecycle);
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V4: per-project run lock
// ---------------------------------------------------------------------------

fn migrate_v4(conn: &Connection) -> Result<()> {
    // `run_locks` prevents two `ralph run` invocations from executing
    // concurrently against the same project. Keyed on absolute project path;
    // `pid` is the OS process id of the active runner and is checked for
    // liveness when a new run tries to acquire the lock.
    conn.execute_batch(
        "
        CREATE TABLE run_locks (
            project TEXT PRIMARY KEY,
            pid INTEGER NOT NULL,
            plan_id TEXT,
            plan_slug TEXT,
            started_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        );
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V5: plan_harness column on plans
// ---------------------------------------------------------------------------

fn migrate_v5(conn: &Connection) -> Result<()> {
    conn.execute_batch(
        "
        ALTER TABLE plans ADD COLUMN plan_harness TEXT;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V6: per-step model override
// ---------------------------------------------------------------------------

fn migrate_v6(conn: &Connection) -> Result<()> {
    // Nullable: `NULL` means "no override — fall back to the harness's
    // default_model from config, or omit the model flag entirely".
    conn.execute_batch(
        "
        ALTER TABLE steps ADD COLUMN model TEXT;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V7: dedupe step_hooks and enforce uniqueness
// ---------------------------------------------------------------------------

fn migrate_v7(conn: &Connection) -> Result<()> {
    // SQLite treats NULLs as distinct in UNIQUE indexes, but plan-wide hooks
    // use step_id IS NULL and must also be unique per (plan_id, lifecycle,
    // hook_name). COALESCE(step_id, '') folds NULL into a sentinel for the
    // index so both per-step and plan-wide rows share a single rule.
    conn.execute_batch(
        "
        DELETE FROM step_hooks
        WHERE id NOT IN (
            SELECT MIN(id)
            FROM step_hooks
            GROUP BY plan_id, COALESCE(step_id, ''), lifecycle, hook_name
        );

        CREATE UNIQUE INDEX idx_step_hooks_unique
            ON step_hooks(plan_id, COALESCE(step_id, ''), lifecycle, hook_name);
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V8: skipped_reason column on steps
// ---------------------------------------------------------------------------

fn migrate_v8(conn: &Connection) -> Result<()> {
    // Nullable: only populated when `ralph skip --reason <r>` records why a
    // step was intentionally bypassed. Surfaced in `ralph status -v` and
    // `ralph log` so the operator's rationale isn't lost.
    conn.execute_batch(
        "
        ALTER TABLE steps ADD COLUMN skipped_reason TEXT;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V9: pid_start_token on run_locks (PID-reuse mitigation)
// ---------------------------------------------------------------------------

fn migrate_v9(conn: &Connection) -> Result<()> {
    // `pid` alone isn't enough to prove the recorded process is still the one
    // that wrote the lock: the kernel recycles PIDs, so an unrelated live
    // process can inherit a dead ralph's PID and make `kill -0` falsely report
    // the lock as still active. Store a per-process start token (Linux:
    // /proc/<pid>/stat starttime; other Unix: ps -o lstart) so acquire can
    // also compare the token against the live process's current token and
    // detect PID reuse. Nullable for rows written by pre-v9 binaries — those
    // fall back to liveness-only checking.
    conn.execute_batch(
        "
        ALTER TABLE run_locks ADD COLUMN pid_start_token TEXT;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V10: prompt prefix/suffix at project and plan scope
// ---------------------------------------------------------------------------

fn migrate_v10(conn: &Connection) -> Result<()> {
    // `project_settings` holds one row per project path with optional prompt
    // prefix/suffix. Layered outside the plan-scope wrap at execution time.
    //
    // `plans` gains matching columns so plan-scope wraps sit alongside the rest
    // of the plan's configuration rather than in a sibling table.
    conn.execute_batch(
        "
        CREATE TABLE project_settings (
            project TEXT PRIMARY KEY,
            prompt_prefix TEXT,
            prompt_suffix TEXT,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        );

        ALTER TABLE plans ADD COLUMN prompt_prefix TEXT;
        ALTER TABLE plans ADD COLUMN prompt_suffix TEXT;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V11: observability/control columns on run_locks + execution_logs
// ---------------------------------------------------------------------------

fn migrate_v11(conn: &Connection) -> Result<()> {
    // `run_locks` gains a set of nullable observability columns so the
    // executor can record which phase of a step is currently running (and,
    // if it's a hook or test, which subprocess owns that phase). Every
    // column is nullable because SQLite's `ADD COLUMN` can't introduce a
    // NOT NULL column without a default, and more importantly because the
    // lock row is created before any of this state is known.
    //
    // `execution_logs` gains `termination_reason` and `test_status` so
    // terminal outcome is explicit rather than inferred from the
    // committed/rolled_back/test_results tuple. Existing rows are backfilled:
    // `termination_reason` becomes `'unknown'` (we genuinely can't tell), and
    // `test_status` is inferred from the existing shape where possible.
    conn.execute_batch(
        "
        ALTER TABLE run_locks ADD COLUMN step_id TEXT;
        ALTER TABLE run_locks ADD COLUMN step_num INTEGER;
        ALTER TABLE run_locks ADD COLUMN attempt INTEGER;
        ALTER TABLE run_locks ADD COLUMN max_attempts INTEGER;
        ALTER TABLE run_locks ADD COLUMN phase TEXT;
        ALTER TABLE run_locks ADD COLUMN phase_started_at TEXT;
        ALTER TABLE run_locks ADD COLUMN current_command TEXT;
        ALTER TABLE run_locks ADD COLUMN execution_log_id INTEGER;
        ALTER TABLE run_locks ADD COLUMN child_pid INTEGER;
        ALTER TABLE run_locks ADD COLUMN child_start_token TEXT;
        ALTER TABLE run_locks ADD COLUMN updated_at TEXT;

        ALTER TABLE execution_logs ADD COLUMN termination_reason TEXT;
        ALTER TABLE execution_logs ADD COLUMN test_status TEXT;

        -- Backfill termination_reason: we can't tell after the fact, so mark
        -- every existing row 'unknown'. Fresh rows will get populated properly.
        UPDATE execution_logs SET termination_reason = 'unknown';

        -- Backfill test_status from the (committed, rolled_back, test_results)
        -- tuple. The four cases below are all we can infer; anything else
        -- (e.g. rows stuck mid-run) stays NULL.
        UPDATE execution_logs
           SET test_status = 'passed'
         WHERE committed = 1 AND test_results != '[]';
        UPDATE execution_logs
           SET test_status = 'not_configured'
         WHERE committed = 1 AND test_results = '[]';
        UPDATE execution_logs
           SET test_status = 'failed'
         WHERE rolled_back = 1 AND test_results != '[]';
        UPDATE execution_logs
           SET test_status = 'not_run'
         WHERE rolled_back = 1 AND test_results = '[]';
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V12: per-step change_policy column
// ---------------------------------------------------------------------------

fn migrate_v12(conn: &Connection) -> Result<()> {
    // `change_policy` governs whether a step must produce file changes to
    // succeed. `'required'` (default) preserves existing behavior — a harness
    // that exits cleanly with no diff is treated as a failure. `'optional'`
    // lets review/audit steps succeed on a clean no-diff harness exit.
    //
    // SQLite permits NOT NULL DEFAULT on `ALTER TABLE ADD COLUMN`, so every
    // pre-V12 row is backfilled to `'required'` and the invariant is
    // preserved going forward.
    conn.execute_batch(
        "
        ALTER TABLE steps ADD COLUMN change_policy TEXT NOT NULL DEFAULT 'required';
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V13: per-step tags column
// ---------------------------------------------------------------------------

fn migrate_v13(conn: &Connection) -> Result<()> {
    // `tags` stores a JSON array of user-supplied string tags attached to a
    // step. Storage + CRUD only — no execution-model semantics today. Every
    // pre-V13 row is backfilled to `'[]'` (empty array) via the NOT NULL
    // DEFAULT.
    conn.execute_batch(
        "
        ALTER TABLE steps ADD COLUMN tags TEXT NOT NULL DEFAULT '[]';
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V14: per-plan context_prepend override
// ---------------------------------------------------------------------------

fn migrate_v14(conn: &Connection) -> Result<()> {
    // `context_prepend` is the per-plan override for the default
    // "how to introspect this plan" text injected at the top of every step's
    // prompt. Nullable: `NULL` means "use the system default text baked into
    // the binary" (see `prompt::DEFAULT_CONTEXT_PREPEND`). An empty string
    // is an explicit "no prepend at all" escape hatch. Non-empty replaces
    // the default verbatim — not concatenated with it.
    conn.execute_batch(
        "
        ALTER TABLE plans ADD COLUMN context_prepend TEXT DEFAULT NULL;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V15: auto-stash bookkeeping on run_locks
// ---------------------------------------------------------------------------

fn migrate_v15(conn: &Connection) -> Result<()> {
    // `source_branch` is the branch the user was on when `ralph run`
    // started; we switch back to it during teardown before popping the
    // stash. `stash_sha` is the commit SHA of the ralph-owned stash
    // (NULL when the tree was clean at run start). Both nullable because
    // the run_locks row is created before we've checked for dirty state,
    // and because many runs won't have a stash at all.
    conn.execute_batch(
        "
        ALTER TABLE run_locks ADD COLUMN source_branch TEXT;
        ALTER TABLE run_locks ADD COLUMN stash_sha TEXT;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V16: per-step questions (opt-in pause-for-clarification feature)
// ---------------------------------------------------------------------------

fn migrate_v16(conn: &Connection) -> Result<()> {
    // `plans.questions_enabled` is the per-plan opt-in toggle for the
    // pause-for-question feature. Stored as INTEGER (0/1) because SQLite
    // has no native bool. NOT NULL with DEFAULT 0 keeps every pre-V16 row
    // explicitly opted-out.
    //
    // `step_questions` records each `ralph question ask` call made by a
    // harness mid-run. `attempt` matches `execution_logs.attempt` so the
    // runner can pull "questions asked during the current attempt" without
    // joining through execution_logs. `answer` stays NULL until the user
    // answers via `ralph question answer` or the TUI; the partial index on
    // `answer WHERE answer IS NULL` keeps the "is this plan paused?" lookup
    // O(rows-with-unanswered-questions) instead of O(all-rows).
    //
    // ON DELETE CASCADE on step_id mirrors the rest of the schema: drop a
    // plan/step and its questions go with it.
    conn.execute_batch(
        "
        ALTER TABLE plans ADD COLUMN questions_enabled INTEGER NOT NULL DEFAULT 0;

        CREATE TABLE step_questions (
            id TEXT PRIMARY KEY,
            step_id TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
            attempt INTEGER NOT NULL,
            question TEXT NOT NULL,
            suggestions TEXT NOT NULL DEFAULT '[]',
            answer TEXT,
            asked_at TEXT NOT NULL,
            answered_at TEXT
        );

        CREATE INDEX idx_step_questions_step ON step_questions(step_id);
        CREATE INDEX idx_step_questions_unanswered
            ON step_questions(answer) WHERE answer IS NULL;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V17: parent_tui_pid on run_locks (read-only detection ownership)
// ---------------------------------------------------------------------------

fn migrate_v17(conn: &Connection) -> Result<()> {
    // `parent_tui_pid` records the pid of the TUI process that spawned this
    // runner subprocess. When the same TUI later re-enters the plan-detail
    // view, `read_only::detect` treats `parent_tui_pid == my_pid` as
    // "lock owned by self" so the user is not falsely forced into read-only
    // mode in the same shell session that started the run.
    //
    // NULL for pre-V17 rows and for runs where the parent pid is unknown
    // (e.g. non-Unix platforms where we cannot resolve `getppid`). Rows
    // predating this column degrade to today's read-only behavior, which
    // is acceptable for in-flight runs at upgrade time.
    conn.execute_batch(
        "
        ALTER TABLE run_locks ADD COLUMN parent_tui_pid INTEGER;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V19: last_run_branch column on plans (branch-based resume resolver)
// ---------------------------------------------------------------------------

fn migrate_v19(conn: &Connection) -> Result<()> {
    // `plans.last_run_branch` records the git branch that was checked out
    // when the plan most recently started a run. The runner sets it on every
    // run start (both default and `--current-branch` modes), giving
    // `ralph resume` (no slug) a way to infer the active plan from the
    // current branch — essential when multiple plans share a branch
    // (e.g. several plans run on `master` with `--current-branch`) and the
    // user later checks out a feature branch that happens to share its name
    // with one of those plans' slugs.
    //
    // Nullable with no backfill: pre-V19 plans report NULL until their next
    // run, and the resolver falls back to `branch_name` only when
    // `last_run_branch IS NULL` (covers never-run plans).
    conn.execute_batch(
        "
        ALTER TABLE plans ADD COLUMN last_run_branch TEXT;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V18: pause_requested column on plans (graceful between-step pause)
// ---------------------------------------------------------------------------

fn migrate_v18(conn: &Connection) -> Result<()> {
    // `plans.pause_requested` lets the user (via the TUI `P` keybinding or
    // `ralph pause` CLI) ask the runner to stop after the currently-executing
    // step finishes — distinct from the immediate SIGTERM-on-`S` path. The
    // runner inspects it between steps and exits with
    // `TerminationReason::PausedByUser`, clearing the flag in the same
    // transaction so a subsequent `ralph resume` doesn't immediately re-pause.
    //
    // Stored as INTEGER (0/1) because SQLite has no native bool. NOT NULL
    // with DEFAULT 0 keeps every pre-V18 row explicitly opted-out.
    conn.execute_batch(
        "
        ALTER TABLE plans ADD COLUMN pause_requested INTEGER NOT NULL DEFAULT 0;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V20: last_run_started_at column on plans (resume-ordering anchor)
// ---------------------------------------------------------------------------

fn migrate_v20(conn: &Connection) -> Result<()> {
    // `plans.last_run_started_at` records the wall-clock time at which the
    // plan most recently *started* a run (written by the runner alongside
    // `last_run_branch`). It exists so resume-resolver ordering can use a
    // stable "last actually ran" timestamp instead of the easily-bumped
    // `updated_at` (which is also touched by unrelated edits like toggling
    // `questions_enabled` or `pause_requested`).
    //
    // Nullable with no backfill: pre-V20 plans report NULL until their next
    // run. The resume resolver's `ORDER BY` lists this column first with
    // `NULLS LAST` so never-run plans tiebreak via `updated_at`/`created_at`.
    conn.execute_batch(
        "
        ALTER TABLE plans ADD COLUMN last_run_started_at TEXT;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V21: drop the legacy per-plan prompt-wrap columns
// ---------------------------------------------------------------------------

fn migrate_v21(conn: &Connection) -> Result<()> {
    // The prompt model is collapsing to a strict four-layer
    // (Global/Project/Plan/Step) shape. Per-plan `prompt_prefix` /
    // `prompt_suffix` (added V10) and `context_prepend` (added V14) no longer
    // exist in that model — the plan layer is sourced from the plan itself,
    // not a separate wrap, and the introspection block now lives in the
    // global prompt seed. Drop all three columns outright; the data they held
    // is intentionally not migrated anywhere.
    //
    // The bundled SQLite (libsqlite3-sys 0.37 → SQLite 3.50.x) is well past
    // 3.35.0, where `ALTER TABLE ... DROP COLUMN` was introduced, so no
    // table-recreate dance is needed. None of these columns participate in an
    // index or constraint, so DROP COLUMN succeeds directly. Dropping
    // preserves the physical order of the remaining columns, so
    // `plan::PLAN_COLUMNS` / `Plan::from_row` only need their indices shifted.
    conn.execute_batch(
        "
        ALTER TABLE plans DROP COLUMN prompt_prefix;
        ALTER TABLE plans DROP COLUMN prompt_suffix;
        ALTER TABLE plans DROP COLUMN context_prepend;
        ",
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_open_memory_creates_schema() {
        let conn = open_memory().expect("open_memory");

        // Verify user_version is current
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("user_version");
        assert_eq!(version, CURRENT_VERSION);

        // Verify foreign keys are enabled
        let fk: i32 = conn
            .pragma_query_value(None, "foreign_keys", |row| row.get(0))
            .expect("foreign_keys");
        assert_eq!(fk, 1);
    }

    #[test]
    fn test_tables_exist() {
        let conn = open_memory().expect("open_memory");

        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            .expect("prepare")
            .query_map([], |row| row.get(0))
            .expect("query")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("collect");

        assert!(tables.contains(&"plans".to_string()));
        assert!(tables.contains(&"steps".to_string()));
        assert!(tables.contains(&"execution_logs".to_string()));
        assert!(tables.contains(&"run_locks".to_string()));
    }

    #[test]
    fn test_indexes_exist() {
        let conn = open_memory().expect("open_memory");

        let indexes: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%' ORDER BY name")
            .expect("prepare")
            .query_map([], |row| row.get(0))
            .expect("query")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("collect");

        let expected = [
            // V1
            "idx_logs_step_attempt",
            "idx_logs_step_id",
            "idx_plans_project",
            "idx_plans_project_status",
            "idx_steps_plan_id",
            "idx_steps_plan_sort",
            // V2
            "idx_plan_deps_dep",
            "idx_plan_deps_plan",
            // V3
            "idx_step_hooks_plan",
            "idx_step_hooks_plan_lifecycle",
            "idx_step_hooks_step",
            // V7
            "idx_step_hooks_unique",
        ];
        for idx in &expected {
            assert!(indexes.contains(&idx.to_string()), "Missing index: {idx}");
        }
    }

    #[test]
    fn test_insert_plan_and_step() {
        let conn = open_memory().expect("open_memory");

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "my-plan", "/tmp/proj", "feat/branch", "A test plan"],
        )
        .expect("insert plan");

        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step 1", "First step"],
        )
        .expect("insert step");

        let title: String = conn
            .query_row("SELECT title FROM steps WHERE id = ?1", ["s1"], |row| {
                row.get(0)
            })
            .expect("query step");
        assert_eq!(title, "Step 1");
    }

    #[test]
    fn test_cascade_delete() {
        let conn = open_memory().expect("open_memory");

        // Insert plan → step → execution_log
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "branch", "desc"],
        )
        .expect("insert plan");

        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "desc"],
        )
        .expect("insert step");

        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt) VALUES (?1, ?2)",
            rusqlite::params!["s1", 1],
        )
        .expect("insert log");

        // Delete plan — should cascade to steps and logs
        conn.execute("DELETE FROM plans WHERE id = ?1", ["p1"])
            .expect("delete plan");

        let step_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM steps", [], |row| row.get(0))
            .expect("count steps");
        assert_eq!(step_count, 0);

        let log_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM execution_logs", [], |row| row.get(0))
            .expect("count logs");
        assert_eq!(log_count, 0);
    }

    #[test]
    fn test_unique_constraints() {
        let conn = open_memory().expect("open_memory");

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "branch", "desc"],
        )
        .expect("insert plan");

        // Duplicate (slug, project) should fail
        let result = conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p2", "slug", "/proj", "branch2", "desc2"],
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_migration_idempotent() {
        let conn = open_memory().expect("first open");
        // Running migrations again on same connection should be a no-op
        run_migrations(&conn).expect("re-run migrations");

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .expect("user_version");
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_db_path_under_data_dir() {
        let path = db_path().expect("db_path");
        assert!(path.ends_with("ralph.db"));
        let parent = path.parent().unwrap();
        assert!(parent.ends_with("ralph-rs"));
    }

    #[test]
    fn test_plan_dependencies_table_and_check_constraint() {
        let conn = open_memory().expect("open_memory");

        // Table should exist.
        let tables: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='plan_dependencies'",
            )
            .expect("prepare")
            .query_map([], |row| row.get(0))
            .expect("query")
            .collect::<std::result::Result<Vec<_>, _>>()
            .expect("collect");
        assert_eq!(tables, vec!["plan_dependencies".to_string()]);

        // Insert two plans so the FK is satisfied.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug1", "/proj", "b1", "d1"],
        )
        .expect("insert plan 1");
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p2", "slug2", "/proj", "b2", "d2"],
        )
        .expect("insert plan 2");

        // Happy path insert.
        conn.execute(
            "INSERT INTO plan_dependencies (plan_id, depends_on_plan_id) VALUES (?1, ?2)",
            rusqlite::params!["p1", "p2"],
        )
        .expect("insert dep");

        // CHECK constraint: self-reference must fail.
        let result = conn.execute(
            "INSERT INTO plan_dependencies (plan_id, depends_on_plan_id) VALUES (?1, ?2)",
            rusqlite::params!["p1", "p1"],
        );
        assert!(result.is_err(), "self-reference should be rejected");
    }

    #[test]
    fn test_plan_dependencies_cascade_delete() {
        let conn = open_memory().expect("open_memory");

        // Three plans: p1 depends on p2, p3 depends on p1.
        for (id, slug) in &[("p1", "s1"), ("p2", "s2"), ("p3", "s3")] {
            conn.execute(
                "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![id, slug, "/proj", "b", "d"],
            )
            .expect("insert plan");
        }

        conn.execute(
            "INSERT INTO plan_dependencies (plan_id, depends_on_plan_id) VALUES (?1, ?2)",
            rusqlite::params!["p1", "p2"],
        )
        .expect("insert p1 -> p2");
        conn.execute(
            "INSERT INTO plan_dependencies (plan_id, depends_on_plan_id) VALUES (?1, ?2)",
            rusqlite::params!["p3", "p1"],
        )
        .expect("insert p3 -> p1");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM plan_dependencies", [], |row| {
                row.get(0)
            })
            .expect("count");
        assert_eq!(count, 2);

        // Deleting p1 should cascade in both directions (p1 -> p2 and p3 -> p1).
        conn.execute("DELETE FROM plans WHERE id = ?1", ["p1"])
            .expect("delete p1");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM plan_dependencies", [], |row| {
                row.get(0)
            })
            .expect("count after delete");
        assert_eq!(
            count, 0,
            "cascade delete should remove both the outgoing and incoming edges"
        );
    }

    #[test]
    fn test_file_based_db() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("test.db");

        {
            let conn = open_at(&path).expect("open_at");
            conn.execute(
                "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params!["p1", "slug", "/proj", "branch", "desc"],
            )
            .expect("insert");
        }

        // Re-open and verify data persisted and migrations don't re-run destructively
        {
            let conn = open_at(&path).expect("re-open");
            let slug: String = conn
                .query_row("SELECT slug FROM plans WHERE id = ?1", ["p1"], |row| {
                    row.get(0)
                })
                .expect("query");
            assert_eq!(slug, "slug");
        }
    }

    #[test]
    fn test_step_hooks_unique_index_enforced() {
        let conn = open_memory().expect("open_memory");

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "b", "d"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "d"],
        )
        .unwrap();

        conn.execute(
            "INSERT INTO step_hooks (plan_id, step_id, lifecycle, hook_name) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["p1", "s1", "pre-step", "h"],
        )
        .unwrap();

        // Duplicate per-step attachment is rejected at the DB level.
        let dup_step = conn.execute(
            "INSERT INTO step_hooks (plan_id, step_id, lifecycle, hook_name) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["p1", "s1", "pre-step", "h"],
        );
        assert!(dup_step.is_err());

        // Plan-wide (step_id NULL) must also be unique per (plan, lifecycle, name).
        conn.execute(
            "INSERT INTO step_hooks (plan_id, step_id, lifecycle, hook_name) VALUES (?1, NULL, ?2, ?3)",
            rusqlite::params!["p1", "post-step", "h"],
        )
        .unwrap();
        let dup_plan = conn.execute(
            "INSERT INTO step_hooks (plan_id, step_id, lifecycle, hook_name) VALUES (?1, NULL, ?2, ?3)",
            rusqlite::params!["p1", "post-step", "h"],
        );
        assert!(dup_plan.is_err());
    }

    #[test]
    fn test_migrate_v7_dedupes_existing_rows() {
        // Simulate an old database at version 6 with duplicate step_hooks rows,
        // then finish the migration to v7 and confirm dedup + uniqueness.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..v6 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(6) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "b", "d"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "d"],
        )
        .unwrap();

        // Three duplicate per-step rows + two duplicate plan-wide rows.
        for _ in 0..3 {
            conn.execute(
                "INSERT INTO step_hooks (plan_id, step_id, lifecycle, hook_name) VALUES (?1, ?2, ?3, ?4)",
                rusqlite::params!["p1", "s1", "pre-step", "h"],
            )
            .unwrap();
        }
        for _ in 0..2 {
            conn.execute(
                "INSERT INTO step_hooks (plan_id, step_id, lifecycle, hook_name) VALUES (?1, NULL, ?2, ?3)",
                rusqlite::params!["p1", "post-step", "h"],
            )
            .unwrap();
        }

        let before: i64 = conn
            .query_row("SELECT COUNT(*) FROM step_hooks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(before, 5);

        drop(conn);

        // Re-open — v7 now applies and should dedupe before creating the index.
        let conn = open_at(&path).unwrap();
        let after: i64 = conn
            .query_row("SELECT COUNT(*) FROM step_hooks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(after, 2, "duplicates should have been collapsed");

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        // Re-inserting a duplicate is now rejected.
        let err = conn.execute(
            "INSERT INTO step_hooks (plan_id, step_id, lifecycle, hook_name) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["p1", "s1", "pre-step", "h"],
        );
        assert!(err.is_err());
    }

    #[cfg(unix)]
    #[test]
    fn test_db_file_is_mode_0600() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempfile::tempdir().expect("tempdir");
        let path = tmp.path().join("perms.db");
        {
            let _conn = open_at(&path).expect("open_at");
        }

        let mode = fs::metadata(&path).expect("metadata").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "DB should be 0600, got {mode:o}");

        // Re-opening an existing DB must keep (or re-apply) the restrictive mode.
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644)).expect("chmod 0644");
        {
            let _conn = open_at(&path).expect("re-open_at");
        }
        let mode = fs::metadata(&path).expect("metadata").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "re-open should restore 0600, got {mode:o}");
    }

    #[test]
    fn test_migrate_v11_backfills_execution_logs() {
        // Seed a pre-V11 database, populate rows covering each backfill case,
        // then let V11 apply and verify termination_reason + test_status.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v10.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v10 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(10) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "b", "d"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "d"],
        )
        .unwrap();

        // Case A: committed + non-empty test_results -> test_status = 'passed'
        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, test_results, rolled_back, committed)
             VALUES (?1, ?2, ?3, 0, 1)",
            rusqlite::params!["s1", 1, r#"["cargo test: pass"]"#],
        )
        .unwrap();

        // Case B: committed + empty test_results -> 'not_configured'
        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, test_results, rolled_back, committed)
             VALUES (?1, ?2, '[]', 0, 1)",
            rusqlite::params!["s1", 2],
        )
        .unwrap();

        // Case C: rolled_back + non-empty test_results -> 'failed'
        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, test_results, rolled_back, committed)
             VALUES (?1, ?2, ?3, 1, 0)",
            rusqlite::params!["s1", 3, r#"["cargo test: fail"]"#],
        )
        .unwrap();

        // Case D: rolled_back + empty test_results -> 'not_run'
        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, test_results, rolled_back, committed)
             VALUES (?1, ?2, '[]', 1, 0)",
            rusqlite::params!["s1", 4],
        )
        .unwrap();

        // Case E: neither committed nor rolled_back (e.g. interrupted mid-run)
        // -> test_status stays NULL.
        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, test_results, rolled_back, committed)
             VALUES (?1, ?2, '[]', 0, 0)",
            rusqlite::params!["s1", 5],
        )
        .unwrap();

        drop(conn);

        // Re-open — V11 applies and backfills.
        let conn = open_at(&path).unwrap();

        // Every row should have termination_reason = 'unknown'.
        let unknown_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM execution_logs WHERE termination_reason = 'unknown'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(unknown_count, 5);

        let total: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM execution_logs WHERE termination_reason IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(total, 0, "no row should have NULL termination_reason");

        // Inspect test_status per case.
        let ts = |attempt: i32| -> Option<String> {
            conn.query_row(
                "SELECT test_status FROM execution_logs WHERE attempt = ?1",
                rusqlite::params![attempt],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(ts(1).as_deref(), Some("passed"));
        assert_eq!(ts(2).as_deref(), Some("not_configured"));
        assert_eq!(ts(3).as_deref(), Some("failed"));
        assert_eq!(ts(4).as_deref(), Some("not_run"));
        assert_eq!(ts(5), None, "unresolved rows should keep NULL test_status");

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migrate_v11_idempotent() {
        // Opening the DB twice should not reapply V11 (which would fail on
        // the duplicate ALTER TABLE ADD COLUMN).
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("idem_v11.db");
        {
            let _conn = open_at(&path).expect("first open runs all migrations");
        }
        // A second open is the actual idempotence check — if V11 re-ran it
        // would fail on "duplicate column name".
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migrate_v11_adds_run_lock_columns() {
        // Every new run_locks column should be queryable and accept NULL.
        let conn = open_memory().expect("open_memory");

        conn.execute(
            "INSERT INTO run_locks (project, pid, step_id, step_num, attempt, max_attempts,
                                    phase, phase_started_at, current_command, execution_log_id,
                                    child_pid, child_start_token, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            rusqlite::params![
                "/proj",
                1234,
                "s1",
                3,
                1,
                5,
                "harness",
                "2026-04-21T00:00:00.000Z",
                "cargo test",
                42,
                99999,
                "token",
                "2026-04-21T00:00:01.000Z",
            ],
        )
        .expect("insert with all v11 columns");

        // All columns readable.
        let (phase, cmd, updated): (Option<String>, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT phase, current_command, updated_at FROM run_locks WHERE project = ?1",
                ["/proj"],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .expect("query");
        assert_eq!(phase.as_deref(), Some("harness"));
        assert_eq!(cmd.as_deref(), Some("cargo test"));
        assert_eq!(updated.as_deref(), Some("2026-04-21T00:00:01.000Z"));

        // Null values also permitted (all columns nullable).
        conn.execute(
            "INSERT INTO run_locks (project, pid) VALUES (?1, ?2)",
            rusqlite::params!["/proj2", 1],
        )
        .expect("insert with only required columns");
    }

    #[test]
    fn test_migrate_v12_backfills_change_policy() {
        // Seed a pre-V12 database, populate some steps, then let V12 apply
        // and verify every existing row got change_policy = 'required'.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v11.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v11 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(11) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "b", "d"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step A", "d"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s2", "p1", "a1", "Step B", "d"],
        )
        .unwrap();

        drop(conn);

        // Re-open — V12 applies and backfills every row to 'required'.
        let conn = open_at(&path).unwrap();

        let required_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM steps WHERE change_policy = 'required'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(required_count, 2, "both pre-V12 rows should be 'required'");

        let null_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM steps WHERE change_policy IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(null_count, 0, "NOT NULL DEFAULT must leave no NULLs");

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migrate_v12_idempotent() {
        // Opening the DB twice must not re-apply V12 (which would fail with
        // "duplicate column name" on the second ALTER TABLE ADD COLUMN).
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("idem_v12.db");
        {
            let _conn = open_at(&path).expect("first open runs all migrations");
        }
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v13_adds_tags_column() {
        // Seed a pre-V13 database, populate a step, then let V13 apply and
        // verify the new `tags` column exists with the '[]' default.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v12.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v12 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(12) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "b", "d"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Legacy step", "d"],
        )
        .unwrap();

        drop(conn);

        // Re-open — V13 applies and backfills every row to '[]'.
        let conn = open_at(&path).unwrap();

        let tags: String = conn
            .query_row("SELECT tags FROM steps WHERE id = ?1", ["s1"], |r| r.get(0))
            .unwrap();
        assert_eq!(
            tags, "[]",
            "pre-V13 rows should backfill to empty JSON array"
        );

        let null_count: i64 = conn
            .query_row("SELECT COUNT(*) FROM steps WHERE tags IS NULL", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(null_count, 0, "NOT NULL DEFAULT must leave no NULLs");

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        // Second open must not reapply (ALTER TABLE would fail on duplicate column).
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v14_adds_context_prepend_column() {
        // V14 added `context_prepend`; V21 later drops it again. This test
        // pins V14's behavior in isolation — it applies migrations only
        // through v14 (NOT via `open_at`, which would run the full chain and
        // drop the column at v21) and verifies the column exists, defaults to
        // NULL, and round-trips an explicit value at that schema version.
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v14 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(14) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        // A plan inserted without the column defaults to NULL.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "b", "d"],
        )
        .unwrap();
        let prepend: Option<String> = conn
            .query_row(
                "SELECT context_prepend FROM plans WHERE id = ?1",
                ["p1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            prepend, None,
            "rows inserted at v14 should default to NULL context_prepend"
        );

        // New inserts with an explicit value are preserved.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, context_prepend)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["p2", "slug2", "/proj", "b", "d", "custom prepend"],
        )
        .unwrap();
        let p2: Option<String> = conn
            .query_row(
                "SELECT context_prepend FROM plans WHERE id = ?1",
                ["p2"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(p2.as_deref(), Some("custom prepend"));
    }

    #[test]
    fn test_migration_v21_drops_plan_prompt_wrap_columns() {
        // Seed a pre-V21 DB, populate a plan with values in all three
        // soon-to-be-dropped columns, then run V21 and verify the columns are
        // gone while the rest of the row (and the preserved columns) survive.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v20.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v20 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(20) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        // The three columns still exist at v20 — populate them so we can
        // prove the row's other data survives the drop.
        conn.execute(
            "INSERT INTO plans
                (id, slug, project, branch_name, description,
                 prompt_prefix, prompt_suffix, context_prepend, last_run_branch)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                "p1", "old", "/proj", "feat/x", "desc", "PRE", "SUF", "CTX", "feat/x"
            ],
        )
        .unwrap();

        drop(conn);

        // Re-open — V21 applies and drops the three columns.
        let conn = open_at(&path).unwrap();

        let cols: Vec<String> = conn
            .prepare("SELECT * FROM plans LIMIT 0")
            .unwrap()
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        for dropped in ["prompt_prefix", "prompt_suffix", "context_prepend"] {
            assert!(
                !cols.iter().any(|c| c == dropped),
                "column {dropped} should have been dropped by V21 (cols: {cols:?})"
            );
        }

        // Querying a dropped column must now error.
        assert!(
            conn.query_row("SELECT prompt_prefix FROM plans WHERE id = ?1", ["p1"], |r| r
                .get::<_, Option<String>>(0))
                .is_err(),
            "selecting a dropped column should fail"
        );

        // The rest of the row survived the table rewrite.
        let (slug, desc, lrb): (String, String, Option<String>) = conn
            .query_row(
                "SELECT slug, description, last_run_branch FROM plans WHERE id = ?1",
                ["p1"],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(slug, "old");
        assert_eq!(desc, "desc");
        assert_eq!(lrb.as_deref(), Some("feat/x"));

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        // Re-open is a no-op (DROP COLUMN must not run twice).
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v15_adds_source_branch_and_stash_sha_columns() {
        // Both columns are nullable and default to NULL; existing rows must
        // survive the upgrade untouched, and inserts without the new columns
        // must still work.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v14.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v14 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(14) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        // Seed a pre-V15 run_locks row.
        conn.execute(
            "INSERT INTO run_locks (project, pid) VALUES (?1, ?2)",
            rusqlite::params!["/proj-v15", 1i64],
        )
        .unwrap();

        drop(conn);

        // Re-open — V15 applies. Pre-V15 row must have NULL in both new
        // columns.
        let conn = open_at(&path).unwrap();
        let (src, sha): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT source_branch, stash_sha FROM run_locks WHERE project = ?1",
                ["/proj-v15"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(src, None);
        assert_eq!(sha, None);

        // New inserts with explicit values round-trip.
        conn.execute(
            "INSERT INTO run_locks (project, pid, source_branch, stash_sha)
             VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["/proj-v15b", 2i64, "master", "deadbeef"],
        )
        .unwrap();
        let (src2, sha2): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT source_branch, stash_sha FROM run_locks WHERE project = ?1",
                ["/proj-v15b"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(src2.as_deref(), Some("master"));
        assert_eq!(sha2.as_deref(), Some("deadbeef"));

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        // Second open is a no-op.
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v16_adds_questions_enabled_and_step_questions() {
        // Seed a pre-V16 database, populate a plan + step, then let V16 apply
        // and verify the new column defaults, table, and indexes.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v15.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v15 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(15) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "b", "d"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "d"],
        )
        .unwrap();

        drop(conn);

        // Re-open — V16 applies. Pre-V16 rows must default to 0 (disabled).
        let conn = open_at(&path).unwrap();

        let qe: i64 = conn
            .query_row(
                "SELECT questions_enabled FROM plans WHERE id = ?1",
                ["p1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(qe, 0, "pre-V16 rows should default questions_enabled to 0");

        let null_count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM plans WHERE questions_enabled IS NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(null_count, 0, "NOT NULL DEFAULT must leave no NULLs");

        // step_questions table exists and accepts a happy-path insert.
        conn.execute(
            "INSERT INTO step_questions (id, step_id, attempt, question, suggestions, asked_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                "q1",
                "s1",
                1,
                "Should I do A or B?",
                r#"["A","B"]"#,
                "2026-05-04T00:00:00.000Z",
            ],
        )
        .unwrap();

        let answer: Option<String> = conn
            .query_row(
                "SELECT answer FROM step_questions WHERE id = ?1",
                ["q1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(answer, None, "fresh question rows have NULL answer");

        // Cascade delete: dropping the step removes its questions.
        conn.execute("DELETE FROM steps WHERE id = ?1", ["s1"])
            .unwrap();
        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM step_questions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 0, "step deletion must cascade to step_questions");

        // Both indexes exist.
        let indexes: Vec<String> = conn
            .prepare(
                "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='step_questions' ORDER BY name",
            )
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(indexes.contains(&"idx_step_questions_step".to_string()));
        assert!(indexes.contains(&"idx_step_questions_unanswered".to_string()));

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        // Second open is a no-op (re-running V16 would fail on duplicate
        // column / duplicate table).
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v17_adds_parent_tui_pid_to_run_locks() {
        // Seed a pre-V17 DB with a run_locks row and verify that V17 leaves
        // it intact with NULL parent_tui_pid, while new inserts can populate
        // the column.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v16.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v16 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(16) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        // Seed a pre-V17 run_locks row.
        conn.execute(
            "INSERT INTO run_locks (project, pid) VALUES (?1, ?2)",
            rusqlite::params!["/proj-v17", 1i64],
        )
        .unwrap();

        drop(conn);

        // Re-open — V17 applies. Pre-V17 row must have NULL parent_tui_pid.
        let conn = open_at(&path).unwrap();
        let parent: Option<i64> = conn
            .query_row(
                "SELECT parent_tui_pid FROM run_locks WHERE project = ?1",
                ["/proj-v17"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(parent, None);

        // New inserts with explicit values round-trip.
        conn.execute(
            "INSERT INTO run_locks (project, pid, parent_tui_pid)
             VALUES (?1, ?2, ?3)",
            rusqlite::params!["/proj-v17b", 2i64, 4242i64],
        )
        .unwrap();
        let parent2: Option<i64> = conn
            .query_row(
                "SELECT parent_tui_pid FROM run_locks WHERE project = ?1",
                ["/proj-v17b"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(parent2, Some(4242));

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        // Second open is a no-op.
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v18_adds_pause_requested_to_plans() {
        // Seed a pre-V18 DB with a plans row, run V18, and verify that the
        // existing row defaults to pause_requested = 0 (preserves prior
        // behavior on upgrade).
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v17.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v17 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(17) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "old", "/proj", "b", "d"],
        )
        .unwrap();

        drop(conn);

        // Re-open — V18 applies. Pre-V18 row must default to 0.
        let conn = open_at(&path).unwrap();
        let pr: i64 = conn
            .query_row(
                "SELECT pause_requested FROM plans WHERE id = ?1",
                ["p1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(pr, 0, "pre-V18 plans must default pause_requested to 0");

        // Fresh inserts also default to 0; explicit 1 round-trips.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, pause_requested)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["p2", "new", "/proj", "b", "d", 1i64],
        )
        .unwrap();
        let pr2: i64 = conn
            .query_row(
                "SELECT pause_requested FROM plans WHERE id = ?1",
                ["p2"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(pr2, 1);

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        // Re-open is a no-op.
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v19_adds_last_run_branch_to_plans() {
        // Seed a pre-V19 DB with a plans row, run V19, and verify that the
        // existing row defaults to last_run_branch = NULL (no backfill —
        // the resolver explicitly relies on this to scope the
        // `branch_name` fallback to never-run plans).
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v18.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v18 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(18) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "old", "/proj", "b", "d"],
        )
        .unwrap();

        drop(conn);

        // Re-open — V19 applies. Pre-V19 row must remain NULL (no backfill).
        let conn = open_at(&path).unwrap();
        let lrb: Option<String> = conn
            .query_row(
                "SELECT last_run_branch FROM plans WHERE id = ?1",
                ["p1"],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            lrb.is_none(),
            "pre-V19 plans must have NULL last_run_branch (got {lrb:?})"
        );

        // Fresh inserts also default to NULL; explicit values round-trip.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, last_run_branch)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["p2", "new", "/proj", "b", "d", "master"],
        )
        .unwrap();
        let lrb2: Option<String> = conn
            .query_row(
                "SELECT last_run_branch FROM plans WHERE id = ?1",
                ["p2"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(lrb2.as_deref(), Some("master"));

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        // Re-open is a no-op.
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v20_adds_last_run_started_at_to_plans() {
        // Seed a pre-V20 DB with a plans row, run V20, and verify that the
        // existing row defaults to last_run_started_at = NULL (no backfill —
        // the resolver's ORDER BY explicitly puts NULLs last so never-run
        // plans tiebreak via updated_at/created_at).
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v19.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v19 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(19) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "old", "/proj", "b", "d"],
        )
        .unwrap();

        drop(conn);

        // Re-open — V20 applies. Pre-V20 row must remain NULL (no backfill).
        let conn = open_at(&path).unwrap();
        let lrs: Option<String> = conn
            .query_row(
                "SELECT last_run_started_at FROM plans WHERE id = ?1",
                ["p1"],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            lrs.is_none(),
            "pre-V20 plans must have NULL last_run_started_at (got {lrs:?})"
        );

        // Fresh inserts also default to NULL; explicit values round-trip.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, last_run_started_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["p2", "new", "/proj", "b", "d", "2026-05-05T00:00:00.000Z"],
        )
        .unwrap();
        let lrs2: Option<String> = conn
            .query_row(
                "SELECT last_run_started_at FROM plans WHERE id = ?1",
                ["p2"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(lrs2.as_deref(), Some("2026-05-05T00:00:00.000Z"));

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        // Re-open is a no-op.
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v16_runs_clean_on_fresh_db() {
        // A fresh in-memory DB applies every migration including V16
        // without requiring the staged-from-V15 path above.
        let conn = open_memory().expect("open_memory");

        // questions_enabled defaults to 0 on fresh inserts.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "b", "d"],
        )
        .unwrap();
        let qe: i64 = conn
            .query_row(
                "SELECT questions_enabled FROM plans WHERE id = ?1",
                ["p1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(qe, 0);

        // Explicit 1 round-trips.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, questions_enabled)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["p2", "slug2", "/proj", "b", "d", 1i64],
        )
        .unwrap();
        let qe: i64 = conn
            .query_row(
                "SELECT questions_enabled FROM plans WHERE id = ?1",
                ["p2"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(qe, 1);
    }

    #[test]
    fn test_plan_harness_column_exists() {
        let conn = open_memory().expect("open_memory");

        // The plan_harness column should exist after migration V5.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, plan_harness)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["p1", "slug", "/proj", "branch", "desc", "goose"],
        )
        .expect("insert plan with plan_harness");

        let ph: Option<String> = conn
            .query_row(
                "SELECT plan_harness FROM plans WHERE id = ?1",
                ["p1"],
                |row| row.get(0),
            )
            .expect("query plan_harness");
        assert_eq!(ph.as_deref(), Some("goose"));

        // NULL plan_harness should also work
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p2", "slug2", "/proj", "branch", "desc"],
        )
        .expect("insert plan without plan_harness");

        let ph2: Option<String> = conn
            .query_row(
                "SELECT plan_harness FROM plans WHERE id = ?1",
                ["p2"],
                |row| row.get(0),
            )
            .expect("query plan_harness null");
        assert_eq!(ph2, None);
    }
}
