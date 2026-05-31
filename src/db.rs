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
    migrate_v22,
    migrate_v23,
    migrate_v24,
    migrate_v25,
    migrate_v26,
    migrate_v27,
    migrate_v28,
    migrate_v29,
    migrate_v30,
    migrate_v31,
    migrate_v32,
    migrate_v33,
    migrate_v34,
    migrate_v35,
    migrate_v36,
    migrate_v37,
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

    // Wait (rather than fail immediately with SQLITE_BUSY) when another
    // process holds a write lock. The cross-process skip bridge has the
    // `ralph skip`/TUI process and the runner process contending on this
    // same file during a live run; without this, a momentary collision
    // would fail `ralph skip` outright or disable the runner's skip poll.
    // 5s mirrors the run-lock connection (`run_lock.rs`).
    conn.busy_timeout(std::time::Duration::from_secs(5))
        .with_context(|| format!("Failed to set busy_timeout on {}", path.display()))?;

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

/// Run `f` inside a single DEFERRED transaction over a shared `&Connection`.
///
/// Commits when `f` returns `Ok`; on `Err` (or a panic / early `?`) the
/// `Transaction` is dropped without `commit`, and rusqlite issues the
/// `ROLLBACK` via its `Drop` impl — so a rollback path can never be
/// missed or forgotten the way the hand-rolled `BEGIN;`/`COMMIT;`/
/// `ROLLBACK;` `execute_batch` triples could. `unchecked_transaction`
/// (not `transaction`) is used deliberately: the storage / command /
/// review layers thread a shared `&Connection`, so requiring `&mut` only
/// to open a transaction would force churn through every caller.
///
/// Two sites intentionally keep explicit blocks rather than this helper:
/// the migration runner (single-threaded startup that must also bump
/// `user_version` in the same transaction) and the run-lock
/// (`BEGIN IMMEDIATE` — it must grab the write lock up front for the lock
/// protocol; this helper is DEFERRED).
pub fn with_tx<T>(conn: &Connection, f: impl FnOnce(&Connection) -> Result<T>) -> Result<T> {
    let tx = conn
        .unchecked_transaction()
        .context("Failed to begin transaction")?;
    let out = f(&tx)?;
    tx.commit().context("Failed to commit transaction")?;
    Ok(out)
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
            session_id TEXT
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
    // answers via the TUI (or `interruption resolve`) ; the partial index on
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
    // `pause_requested`).
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

// ---------------------------------------------------------------------------
// Migration V22: collapse project_settings prompt prefix/suffix into `prompt`
// ---------------------------------------------------------------------------

fn migrate_v22(conn: &Connection) -> Result<()> {
    // The prompt model collapsed to a strict four-layer
    // (Global/Project/Plan/Step) shape with ONE content blob per layer. The
    // project layer kept a `prompt_prefix` / `prompt_suffix` pair (added
    // V10); fold them into a single `prompt` column.
    //
    // Backfill concatenates the two with a blank line between them, skipping
    // whichever side is NULL, so no user-set project prompt is lost. SQLite
    // 3.50.x (bundled) supports `ALTER TABLE ... DROP COLUMN`, and neither
    // column participates in an index or constraint (the table's only key is
    // `project`), so the drops succeed directly.
    conn.execute_batch(
        "
        ALTER TABLE project_settings ADD COLUMN prompt TEXT;

        UPDATE project_settings
        SET prompt = CASE
            WHEN prompt_prefix IS NOT NULL AND prompt_suffix IS NOT NULL
                THEN prompt_prefix || char(10) || char(10) || prompt_suffix
            WHEN prompt_prefix IS NOT NULL THEN prompt_prefix
            WHEN prompt_suffix IS NOT NULL THEN prompt_suffix
            ELSE NULL
        END;

        ALTER TABLE project_settings DROP COLUMN prompt_prefix;
        ALTER TABLE project_settings DROP COLUMN prompt_suffix;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V23: cross-process skip bridge columns on plans
// ---------------------------------------------------------------------------

fn migrate_v23(conn: &Connection) -> Result<()> {
    // `ralph skip` (and the TUI skip dialog) run in a *different process*
    // from the runner that actually owns the in-flight harness child. The
    // process-global cancel registry in `signal.rs` only works same-process,
    // so a cross-process skip needs a durable hand-off the runner can poll —
    // exactly like `plans.pause_requested` (added V18), but pause is a
    // boolean and a skip must identify *which* step it targets so a stale
    // request left behind by a race can't skip the wrong (next) step.
    //
    // `skip_requested_step_id` holds the step UUID the skip targets (NULL ==
    // no pending skip). `skip_changes` holds the serialized
    // `ParkStrategyKind` (`stash` | `commit` | `discard` | `cancel`) the
    // operator chose via `--changes` / the TUI dialog. The runner reads +
    // clears both atomically mid-attempt; on a match it funnels into the
    // same executor skip path the same-process registry uses.
    //
    // Both nullable with no backfill: every pre-V23 plan reports NULL (no
    // pending skip), which is the correct default.
    conn.execute_batch(
        "
        ALTER TABLE plans ADD COLUMN skip_requested_step_id TEXT;
        ALTER TABLE plans ADD COLUMN skip_changes TEXT;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V24: retry_strategy columns on plans and steps
// ---------------------------------------------------------------------------

fn migrate_v24(conn: &Connection) -> Result<()> {
    // Per-plan / per-step opt-in for how a step's working tree is treated
    // between failed attempts (`keep` vs `rollback`). Resolution is
    // step > plan > built-in default (`keep`), implemented by
    // `plan::Step::effective_retry_strategy`.
    //
    // Both columns are nullable TEXT with NO non-null default: NULL means
    // "inherit from the parent / use the built-in default" rather than
    // pinning every pre-V24 row to an explicit value. A non-null value is a
    // serialized `plan::RetryStrategy` (`keep` | `rollback`).
    conn.execute_batch(
        "
        ALTER TABLE plans ADD COLUMN retry_strategy TEXT;
        ALTER TABLE steps ADD COLUMN retry_strategy TEXT;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V25: short ids + step-level dependency DAG
// ---------------------------------------------------------------------------

fn migrate_v25(conn: &Connection) -> Result<()> {
    // The DAG redesign (docs/dag-redesign.md §3, §6) makes a plan a
    // dependency DAG of steps instead of a linear list. Two structural
    // additions:
    //
    // 1. `steps.short_id` — a stable, plan-unique 8-char handle that
    //    replaces the positional step number as the user-facing selector
    //    (a DAG has no stable linear ordinal). The internal UUID
    //    (`steps.id`) is unchanged. The unique index is on
    //    `(plan_id, short_id)` so ids are only plan-scoped; SQLite treats
    //    the pre-backfill NULLs as distinct, so creating the index before
    //    the backfill is safe.
    //
    // 2. `step_dependencies` — a direct structural clone of
    //    `plan_dependencies` (V2): same `ON DELETE CASCADE`, same
    //    self-edge `CHECK`, same two directional indexes. Cycle detection
    //    reuses the V2 `would_create_cycle` pattern via a later
    //    `would_create_step_cycle`.
    //
    // Backfill makes every existing (linear) plan a degenerate chain DAG
    // that executes identically: for each plan, walking its steps in
    // `sort_key` order (the authored order), each step gets a random
    // 8-char `short_id` collision-checked within the plan, and step *k*
    // gets a synthesized `depends_on` edge to step *k−1*. This is the
    // exact backfill `src/import.rs` mirrors for legacy bundles.
    conn.execute_batch(
        "
        ALTER TABLE steps ADD COLUMN short_id TEXT;
        CREATE UNIQUE INDEX idx_steps_short_id ON steps(plan_id, short_id);

        CREATE TABLE step_dependencies (
            step_id            TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
            depends_on_step_id TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
            created_at         TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
            PRIMARY KEY (step_id, depends_on_step_id),
            CHECK (step_id != depends_on_step_id)
        );
        CREATE INDEX idx_step_deps_step ON step_dependencies(step_id);
        CREATE INDEX idx_step_deps_dep  ON step_dependencies(depends_on_step_id);
        ",
    )?;

    let plan_ids: Vec<String> = {
        let mut stmt = conn.prepare("SELECT id FROM plans")?;
        let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()?
    };

    for plan_id in plan_ids {
        let step_ids: Vec<String> = {
            let mut stmt =
                conn.prepare("SELECT id FROM steps WHERE plan_id = ?1 ORDER BY sort_key ASC")?;
            let rows = stmt.query_map([&plan_id], |r| r.get::<_, String>(0))?;
            rows.collect::<rusqlite::Result<Vec<_>>>()?
        };

        let mut prev_step_id: Option<&str> = None;
        for step_id in &step_ids {
            // Mint via the one shared helper so migration-backfill and
            // runtime step creation are byte-for-byte the same logic
            // (docs/dag-redesign.md §13.3 requires import-backfill and
            // migration-backfill to produce the same DAG). The helper's
            // collision check reads prior same-loop UPDATEs on this
            // connection (SQLite read-your-own-writes), so ids stay
            // plan-unique without a local "already assigned" set.
            let short_id = crate::storage::mint_short_id(conn, &plan_id)?;
            conn.execute(
                "UPDATE steps SET short_id = ?1 WHERE id = ?2",
                rusqlite::params![short_id, step_id],
            )?;

            if let Some(prev) = prev_step_id {
                conn.execute(
                    "INSERT INTO step_dependencies (step_id, depends_on_step_id) \
                     VALUES (?1, ?2)",
                    rusqlite::params![step_id, prev],
                )?;
            }
            prev_step_id = Some(step_id.as_str());
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V26: unified interruptions (supersedes step_questions)
// ---------------------------------------------------------------------------

fn migrate_v26(conn: &Connection) -> Result<()> {
    // The DAG redesign (docs/dag-redesign.md §3.4, §6 `### V26`) collapses
    // the question system into one branch-pausing entity: an
    // `interruptions` row is either a `question` (carries ranked `options`,
    // priority 1 = the agent's best guess) or a `blocker` (no options, the
    // agent explains what it cannot do). One entity, one state machine:
    // `open` while the branch is `Blocked` and the scheduler works
    // elsewhere; `resolved` once a human records a `resolution`/`comment`.
    // This is the same "derived, never stored" plan-status mechanism as
    // today — `PlanStatus::Question` widens to a derived `Interrupted`.
    //
    // Schema is verbatim §6: `options` is a JSON array of `{text,priority}`
    // (DEFAULT '[]'), `state` DEFAULT 'open', and the open-lookup index is
    // *partial* (`WHERE state = 'open'`) so "is any branch blocked?" stays
    // O(open-rows) rather than O(all-rows) — the same trick the dropped
    // `idx_step_questions_unanswered` used.
    //
    // Data cutover: every `step_questions` row becomes a resolved/open
    // `question` interruption. `id`/`step_id`/`attempt` carry over verbatim
    // (preserving identity); `question` → `body`; the legacy
    // `suggestions` string array is synthesized into `options` with
    // ascending integer priorities (1,2,3,… in stored order — the order the
    // agent listed them, so the first stays the best guess); `answer` →
    // `resolution`; `state` is `resolved` iff `answer IS NOT NULL`;
    // `asked_at` carries over and `answered_at` → `resolved_at`.
    // `step_questions` had no comment, so `comment` stays NULL. Then the
    // legacy table is dropped, exactly as §6 mandates.
    //
    // This migration is append-only schema + a faithful one-way data copy
    // (legacy `step_questions` rows → `interruptions`) followed by
    // `DROP TABLE step_questions`. Every storage / CLI / executor /
    // scheduler consumer is interruption-native (Phase 2 is complete), so
    // there is no back-compat view/trigger shim — the legacy table is gone
    // for good once V26 runs.
    conn.execute_batch(
        "
        CREATE TABLE interruptions (
            id          TEXT PRIMARY KEY,
            step_id     TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
            attempt     INTEGER NOT NULL,
            kind        TEXT NOT NULL,                 -- 'question' | 'blocker'
            body        TEXT NOT NULL,
            options     TEXT NOT NULL DEFAULT '[]',    -- JSON [{text,priority}]
            resolution  TEXT,
            comment     TEXT,
            state       TEXT NOT NULL DEFAULT 'open',  -- 'open' | 'resolved'
            asked_at    TEXT NOT NULL,
            resolved_at TEXT
        );
        CREATE INDEX idx_interruptions_step ON interruptions(step_id);
        CREATE INDEX idx_interruptions_open ON interruptions(state) WHERE state = 'open';
        ",
    )?;

    // One legacy `step_questions` row, owned, for the cutover.
    struct LegacyQuestion {
        id: String,
        step_id: String,
        attempt: i64,
        question: String,
        suggestions: String, // JSON string array
        answer: Option<String>,
        asked_at: String,
        answered_at: Option<String>,
    }

    // Read every legacy question row up front (mirrors the V25 backfill
    // pattern: collect owned rows, then write) so the read statement is
    // dropped before the cutover inserts run.
    let rows: Vec<LegacyQuestion> = {
        let mut stmt = conn.prepare(
            "SELECT id, step_id, attempt, question, suggestions, answer, asked_at, answered_at \
             FROM step_questions",
        )?;
        let mapped = stmt.query_map([], |r| {
            Ok(LegacyQuestion {
                id: r.get(0)?,
                step_id: r.get(1)?,
                attempt: r.get(2)?,
                question: r.get(3)?,
                suggestions: r.get(4)?,
                answer: r.get(5)?,
                asked_at: r.get(6)?,
                answered_at: r.get(7)?,
            })
        })?;
        mapped.collect::<rusqlite::Result<Vec<_>>>()?
    };

    for LegacyQuestion {
        id,
        step_id,
        attempt,
        question,
        suggestions,
        answer,
        asked_at,
        answered_at,
    } in rows
    {
        // `suggestions` is always a JSON string array (`commands::question`
        // only ever writes `serde_json::to_string(&Vec<String>)`, column is
        // NOT NULL DEFAULT '[]'). Synthesize `[{text,priority}]` with
        // ascending 1-based priorities so the agent's stored order is
        // preserved (index 0 = priority 1 = the agent's best guess).
        // Defensive: a single legacy row with non-array JSON must not abort
        // the whole one-way V26 cutover (which would roll back to V25 and
        // re-fail on every subsequent invocation — unrecoverable without
        // manual DB surgery). The column is `NOT NULL DEFAULT '[]'` and the
        // only writer serializes `Vec<String>`, so this is not reachable
        // through normal operation; treat any unparseable value as "no
        // suggestions" with a warning rather than a hard failure.
        let texts: Vec<String> = serde_json::from_str(&suggestions).unwrap_or_else(|e| {
            eprintln!(
                "warning: step_questions.suggestions for row {id} is not a JSON \
                 string array ({e}); migrating it with no proposed answers"
            );
            Vec::new()
        });
        let options: Vec<serde_json::Value> = texts
            .iter()
            .enumerate()
            .map(|(i, text)| serde_json::json!({ "text": text, "priority": (i as i64) + 1 }))
            .collect();
        let options_json = serde_json::to_string(&options)
            .context("serializing synthesized interruption options during V26 cutover")?;

        let state = if answer.is_some() { "resolved" } else { "open" };

        conn.execute(
            "INSERT INTO interruptions \
                 (id, step_id, attempt, kind, body, options, resolution, comment, state, asked_at, resolved_at) \
             VALUES (?1, ?2, ?3, 'question', ?4, ?5, ?6, NULL, ?7, ?8, ?9)",
            rusqlite::params![
                id,
                step_id,
                attempt,
                question,
                options_json,
                answer,
                state,
                asked_at,
                answered_at,
            ],
        )?;
    }

    conn.execute_batch("DROP TABLE step_questions;")?;

    // No back-compat shim. §6 mandates `DROP TABLE step_questions` and the
    // canonical store is now `interruptions`. Every storage / CLI / executor
    // / scheduler consumer was cut over to the native `interruptions` table
    // in the Phase 2 steps (native CRUD, the `interruption` CLI + thin
    // `question` aliases, the cross-process bridge, scheduler integration),
    // so the transient `step_questions` *view* + INSTEAD-OF triggers that
    // previously kept not-yet-migrated consumers green have been removed.
    // V26 is now exactly: create `interruptions` + faithful data cutover +
    // `DROP TABLE step_questions`.

    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V27: review configuration columns on plans and steps
// ---------------------------------------------------------------------------

fn migrate_v27(conn: &Connection) -> Result<()> {
    // The DAG redesign (docs/dag-redesign.md §3.3, §6 `### V27`) makes
    // nondeterministic review a first-class per-step pipeline stage with an
    // off-switch at three scopes. These additive columns carry that state;
    // they are *wired but not yet consumed* by this batch (no behavior
    // change) — the pipeline that reads them lands in later Phase 3 steps.
    //
    // - `plans.review_enabled` / `steps.review_enabled`: nullable INTEGER
    //   tri-state booleans. NULL means "inherit from the parent scope"
    //   exactly like the V24 `retry_strategy` columns: effective review =
    //   step.review_enabled ?? plan.review_enabled ?? config.review.enabled
    //   ?? false (step > plan > global, mirroring `RetryStrategy`
    //   precedence). NO non-null default so pre-V27 rows inherit rather
    //   than being pinned.
    // - `steps.review_status`: nullable TEXT holding a serialized
    //   `plan::ReviewStatus` (`pending` | `in_flight` | `passed` |
    //   `failed` | `skipped` | `disabled`). NULL = pending (not yet
    //   reviewed) — analogous to how V24's `retry_strategy` NULL means
    //   "use the default".
    // - `steps.corrects_step_id`: nullable TEXT set on a reviewer-inserted
    //   corrective step, pointing at the `steps.id` it corrects (§10).
    //   NULL = an ordinary, non-corrective step.
    //
    // All four are additive nullable `ALTER`s with NO default, so old DBs
    // migrate forward untouched and old export JSON keeps round-tripping
    // via `#[serde(default)]` — same shape as V24.
    conn.execute_batch(
        "
        ALTER TABLE plans ADD COLUMN review_enabled INTEGER;
        ALTER TABLE steps ADD COLUMN review_enabled INTEGER;
        ALTER TABLE steps ADD COLUMN review_status TEXT;
        ALTER TABLE steps ADD COLUMN corrects_step_id TEXT;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V28: per-plan --squash-on-complete
// ---------------------------------------------------------------------------

fn migrate_v28(conn: &Connection) -> Result<()> {
    // docs/dag-redesign.md §14.1 (DECIDED): every per-iteration step commit
    // is KEPT by default (full audit trail). An *optional* per-plan
    // `squash_on_complete` collapses a step's iteration commits into one
    // commit when the step reaches `Complete`.
    //
    // Nullable INTEGER with NO non-null default, exactly like the V24/V27
    // tri-state columns: NULL means "not set" → the executor treats it as
    // `false` (default OFF = identical to step 32/33 behavior). A non-null
    // value is a 0/1 boolean (SQLite has no native bool). Additive `ALTER`,
    // so old DBs migrate forward untouched and old export JSON keeps
    // round-tripping via `#[serde(default)]`.
    conn.execute_batch("ALTER TABLE plans ADD COLUMN squash_on_complete INTEGER;")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V29: corrective-step request bridge (single DAG writer — §9-inv-3)
// ---------------------------------------------------------------------------

fn migrate_v29(conn: &Connection) -> Result<()> {
    // docs/dag-redesign.md §9 invariant 3 ("Single DAG writer"): only the
    // orchestrator mutates the DAG. A reviewer subprocess that finds a defect
    // does NOT write step rows/edges itself — it *requests* a corrective
    // step through a structured channel. The channel has two faces, both
    // landing here / in `output.rs`:
    //
    //   - an NDJSON `RunEvent::CorrectiveStepRequested` (live, for the TUI);
    //   - a durable DB bridge row, so a reviewer running in a *different*
    //     process than the orchestrator (or a request that outlives the
    //     emitting process) is still consumed at the next scheduler tick.
    //
    // This is a direct structural sibling of the V23 skip-bridge
    // (`plans.skip_requested_step_id` / `skip_changes`, polled + cleared by
    // the runner) and the V26 interruption bridge (an open row IS the
    // bridge). A dedicated table — not a `plans` column — because, unlike a
    // skip (one pending request per plan), multiple steps' reviews can fail
    // and queue corrective requests concurrently; a table lets each request
    // carry its own `reviewed_step_id` + `reviewed_iteration` + verdict
    // body and be consumed independently. The orchestrator drains rows
    // (oldest first) at a scheduler tick, performs the §10 insert+re-parent
    // as the SOLE writer, then deletes the row. `state` is kept for forward
    // flexibility but the consume path is a hard delete (the durable audit
    // trail is the corrective step + its `corrects_step_id` pointer, exactly
    // as the skip-bridge's audit trail is the `[ralph wip]` commit).
    //
    // All references are `ON DELETE CASCADE` on the owning step, mirroring
    // `step_dependencies` / `interruptions`, so deleting a step never leaves
    // a dangling request.
    conn.execute_batch(
        "
        CREATE TABLE corrective_step_requests (
            id                 TEXT PRIMARY KEY,
            reviewed_step_id   TEXT NOT NULL REFERENCES steps(id) ON DELETE CASCADE,
            reviewed_iteration INTEGER NOT NULL,
            commit_sha         TEXT NOT NULL,
            issues             INTEGER NOT NULL DEFAULT 1,
            verdict_body       TEXT,
            state              TEXT NOT NULL DEFAULT 'open',  -- 'open' | 'consumed'
            requested_at       TEXT NOT NULL
        );
        CREATE INDEX idx_corrective_requests_step
            ON corrective_step_requests(reviewed_step_id);
        CREATE INDEX idx_corrective_requests_open
            ON corrective_step_requests(state) WHERE state = 'open';
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V30: per-plan max_review_corrections (recursion cap — §10/§14.5)
// ---------------------------------------------------------------------------

fn migrate_v30(conn: &Connection) -> Result<()> {
    // docs/dag-redesign.md §10 item 4 / §14.5: a reviewer-inserted
    // corrective step A′ is ITSELF reviewed (recursion). Bound the
    // review→correction→review chain with a per-plan
    // `max_review_corrections` depth — a sibling concept to `max_retries`
    // (which is global config `max_retries_per_step` with per-step
    // overrides; the correction cap is per-plan because a deep
    // self-correcting chain is a property of the plan's review posture, set
    // alongside the plan's review toggle). Exceeding it raises a
    // `kind=blocker` interruption ("review loop — needs human") instead of
    // spawning indefinitely.
    //
    // Nullable INTEGER with NO non-null default, exactly like the V24/V27/
    // V28 tri-state/optional columns: NULL means "not set" → the runner uses
    // the built-in default (`DEFAULT_MAX_REVIEW_CORRECTIONS` in
    // `crate::review`). Additive `ALTER`, so old DBs migrate forward
    // untouched and old export JSON keeps round-tripping via
    // `#[serde(default)]`.
    conn.execute_batch("ALTER TABLE plans ADD COLUMN max_review_corrections INTEGER;")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V31: enforce plan-local step dependency edges
// ---------------------------------------------------------------------------

fn migrate_v31(conn: &Connection) -> Result<()> {
    // A step dependency is only meaningful inside one plan: the scheduler,
    // import/export, outline, and corrective-step re-parenting all operate on
    // a single plan's step set. V25's two independent foreign keys prevented
    // dangling step IDs but did not prevent `step_id` from one plan depending
    // on `depends_on_step_id` from another. Enforce the invariant at the DB
    // boundary with triggers so every writer (storage API, tests, ad-hoc SQL,
    // future tooling) gets the same guarantee.
    //
    // Existing cross-plan rows are invalid under the DAG model and were never
    // schedulable/exportable correctly, so the migration drops them before
    // installing the triggers.
    //
    // `IS NOT` is the null-safe distinct operator, deliberately so. A
    // `BEFORE INSERT/UPDATE` trigger's `WHEN` clause is evaluated *before*
    // SQLite's (enabled) foreign-key check, so a missing `step_id` makes the
    // left subquery NULL: `NULL IS NOT 'p'` is true and we abort here with the
    // cross-plan message rather than the more precise FK/not-found error. That
    // message imprecision only reaches raw-SQL callers — `add_step_dependency`
    // does its own `Step not found` vs. cross-plan classification first — and
    // it never lets a bad row through (both-missing → `NULL IS NOT NULL` is
    // false → the FK check still rejects it). Keeping `IS NOT` (vs. `=`/`!=`)
    // is what makes the both-missing case fall through to the FK instead of
    // silently passing the trigger.
    //
    // This same-plan invariant is encoded twice: here (the DB-boundary
    // triggers) and in `storage::add_step_dependency` (the in-process
    // defense-in-depth check). If the invariant ever changes, update both.
    conn.execute_batch(
        "
        DELETE FROM step_dependencies
        WHERE EXISTS (
            SELECT 1
            FROM steps child
            JOIN steps dep ON dep.id = step_dependencies.depends_on_step_id
            WHERE child.id = step_dependencies.step_id
              AND child.plan_id != dep.plan_id
        );

        CREATE TRIGGER step_dependencies_same_plan_insert
        BEFORE INSERT ON step_dependencies
        FOR EACH ROW
        WHEN (
            SELECT plan_id FROM steps WHERE id = NEW.step_id
        ) IS NOT (
            SELECT plan_id FROM steps WHERE id = NEW.depends_on_step_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'step dependencies must stay within one plan');
        END;

        CREATE TRIGGER step_dependencies_same_plan_update
        BEFORE UPDATE OF step_id, depends_on_step_id ON step_dependencies
        FOR EACH ROW
        WHEN (
            SELECT plan_id FROM steps WHERE id = NEW.step_id
        ) IS NOT (
            SELECT plan_id FROM steps WHERE id = NEW.depends_on_step_id
        )
        BEGIN
            SELECT RAISE(ABORT, 'step dependencies must stay within one plan');
        END;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V32: rebuild execution_logs (structural no-op, retained for
// version continuity)
// ---------------------------------------------------------------------------

fn migrate_v32(conn: &Connection) -> Result<()> {
    // Retry-with-parked-changes on the retry-exhausted blocker resets
    // `steps.attempts` to 0 but keeps the historical `execution_logs` rows
    // as the per-step audit trail — so duplicate logical `(step_id, attempt)`
    // values (e.g. a second attempt=1 after a from-scratch retry) must be
    // allowed. This rebuild was intended to drop a `UNIQUE(step_id, attempt)`
    // constraint, but — see the migration history (V1 `CREATE TABLE
    // execution_logs` and every later Vxx) — that constraint was never
    // actually created: `(step_id, attempt)` only ever had the *non-unique*
    // `CREATE INDEX idx_logs_step_attempt`. So duplicates were already
    // permitted and this rebuild is a structural no-op. It is kept (not
    // removed/renumbered) for upgrade-ordering continuity: renumbering a
    // shipped migration would corrupt the version sequence for anyone already
    // past V32. The rebuild faithfully copies every column and row (ordered
    // by id) into an identically-shaped table, so it is harmless.
    conn.execute_batch(
        "
        CREATE TABLE execution_logs_v32 (
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
            termination_reason TEXT,
            test_status TEXT
        );

        INSERT INTO execution_logs_v32 (
            id, step_id, attempt, started_at, duration_secs, prompt_text, diff,
            test_results, rolled_back, committed, commit_hash, harness_stdout,
            harness_stderr, cost_usd, input_tokens, output_tokens, session_id,
            termination_reason, test_status
        )
        SELECT
            id, step_id, attempt, started_at, duration_secs, prompt_text, diff,
            test_results, rolled_back, committed, commit_hash, harness_stdout,
            harness_stderr, cost_usd, input_tokens, output_tokens, session_id,
            termination_reason, test_status
        FROM execution_logs
        ORDER BY id;

        DROP TABLE execution_logs;
        ALTER TABLE execution_logs_v32 RENAME TO execution_logs;
        CREATE INDEX idx_logs_step_id ON execution_logs(step_id);
        CREATE INDEX idx_logs_step_attempt ON execution_logs(step_id, attempt);
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V33: per-step cycle_index for grouping retry-from-scratch cycles
// ---------------------------------------------------------------------------

fn migrate_v33(conn: &Connection) -> Result<()> {
    // The auto-blocker "Retry step with parked changes" resolver zeroes
    // `steps.attempts` while keeping the prior cycle's `execution_logs` rows
    // (V32 removed the UNIQUE constraint blocking that). After a reset the
    // next attempt is a *new* cycle — same logical attempt numbers (1, 2, …)
    // running over again. `current_cycle_index` is the step's "current cycle
    // pointer" (bumped every time `set_step_attempts(0)` follows a non-zero
    // value) and `execution_logs.cycle_index` is the value the log was
    // created at, so per-cycle grouping in `ralph log` and the
    // rendered-prompt picker is a simple GROUP BY without any new joins.
    //
    // Both columns default to 0 so backfill is automatic: every existing
    // execution_log was part of cycle 0, every existing step is currently
    // at cycle 0.
    conn.execute_batch(
        "
        ALTER TABLE steps ADD COLUMN current_cycle_index INTEGER NOT NULL DEFAULT 0;
        ALTER TABLE execution_logs ADD COLUMN cycle_index INTEGER NOT NULL DEFAULT 0;
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V34: parked step worktrees for interruption/resume
// ---------------------------------------------------------------------------

fn migrate_v34(conn: &Connection) -> Result<()> {
    // When a step pauses for a human-side interruption (question/blocker or
    // retry-exhaustion blocker), we now park its in-progress working tree in a
    // git stash so the scheduler can move on cleanly (including across plan
    // branch switches) without losing the agent's partial work. This table is
    // the durable pointer from `steps.id` -> stash commit SHA plus the list of
    // files that were staged at stash time, so the runner can re-apply the
    // stash as unstaged WIP just before that step is re-run.
    //
    // Separate table rather than extra `steps` columns so the canonical step
    // row shape stays stable (`STEP_COLUMNS` / `Step::from_row`), and because
    // the parked-worktree state is sparse/ephemeral: most steps never use it.
    conn.execute_batch(
        "
        CREATE TABLE step_parked_worktrees (
            step_id TEXT PRIMARY KEY REFERENCES steps(id) ON DELETE CASCADE,
            stash_sha TEXT NOT NULL,
            staged_files TEXT NOT NULL DEFAULT '[]',
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
        );
        ",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V35: human-approved corrective requests (review-loop escalation)
// ---------------------------------------------------------------------------

fn migrate_v35(conn: &Connection) -> Result<()> {
    // docs/dag-redesign.md §10 item 4 / §14.5: when a corrective chain
    // exceeds `max_review_corrections`, `consume_corrective_request` raises a
    // "review loop — needs human" blocker and leaves the step non-terminal.
    // Resolving that blocker now grants exactly ONE more review→correction
    // cycle: the resolver inserts a corrective request flagged
    // `human_approved = 1`, which `consume_corrective_request` honors by
    // bypassing the recursion-cap check for that single hop. If the resulting
    // corrective step also fails review, `finalize_review` inserts a NORMAL
    // (human_approved = 0) request → the cap check fires again → re-escalates,
    // so the human stays the loop gate.
    //
    // Constant `DEFAULT 0` keeps this a valid SQLite `ADD COLUMN` (additive,
    // old DBs migrate forward untouched; every pre-existing request is a
    // not-human-approved reviewer request).
    conn.execute_batch(
        "ALTER TABLE corrective_step_requests ADD COLUMN human_approved INTEGER NOT NULL DEFAULT 0;",
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V36: drop the per-plan questions_enabled toggle (always-on)
// ---------------------------------------------------------------------------

fn migrate_v36(conn: &Connection) -> Result<()> {
    // Questions/blockers are now ALWAYS enabled — a harness's `ralph question
    // ask` / `ralph block` always raises an interruption and the
    // question-ask instruction is always present in the step prompt. The
    // per-plan opt-out (`plans.questions_enabled`, added in V16) is gone.
    //
    // V16 added it as a plain INTEGER column with no index/trigger/view/
    // generated-column dependency (the V16 indexes are on `step_questions`,
    // a separate table dropped in V26), so a direct `DROP COLUMN` is safe on
    // the bundled modern SQLite and avoids a full table rebuild.
    //
    // `ALTER TABLE ... DROP COLUMN` requires SQLite >= 3.35.0. We rely on the
    // `rusqlite` `bundled` feature in Cargo.toml (currently rusqlite 0.39,
    // which bundles SQLite 3.50.x) to guarantee that floor at compile time, so
    // there is no runtime version guard here. Unlike V32's portable
    // table-rebuild recipe this takes the direct path *because* the bundled
    // floor is assured; if `bundled` is ever dropped, this migration (and the
    // direct DROP COLUMNs in V21/V22) would need a version guard or rebuild.
    conn.execute_batch("ALTER TABLE plans DROP COLUMN questions_enabled;")?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Migration V37: drop the vestigial retry_strategy + squash_on_complete columns
// ---------------------------------------------------------------------------

fn migrate_v37(conn: &Connection) -> Result<()> {
    // Post-DAG-redesign, `RetryStrategy {Keep, Rollback}` and the
    // `--squash-on-complete` toggle are dead surface. There is at most one
    // commit per step (commit-on-test-pass) and failed attempts preserve the
    // dirty tree, so there is nothing to keep/rollback across attempts and
    // nothing for squash to collapse. These columns are never read or written
    // by any live code path, so dropping them is a no-op for behavior.
    //
    // - `plans.retry_strategy` (TEXT, V24)
    // - `steps.retry_strategy` (TEXT, V24)
    // - `plans.squash_on_complete` (INTEGER, V28)
    //
    // All three were added as plain columns with no index/trigger/view/
    // generated-column dependency, so a direct `DROP COLUMN` is safe on the
    // bundled modern SQLite and avoids a full table rebuild.
    //
    // `ALTER TABLE ... DROP COLUMN` requires SQLite >= 3.35.0, guaranteed by
    // the `rusqlite` `bundled` feature (same assurance V36 relies on).
    conn.execute_batch(
        "ALTER TABLE plans DROP COLUMN retry_strategy;
         ALTER TABLE steps DROP COLUMN retry_strategy;
         ALTER TABLE plans DROP COLUMN squash_on_complete;",
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
        assert!(tables.contains(&"step_parked_worktrees".to_string()));
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
            conn.query_row(
                "SELECT prompt_prefix FROM plans WHERE id = ?1",
                ["p1"],
                |r| r.get::<_, Option<String>>(0)
            )
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
    fn test_migration_v22_collapses_project_settings_prompt_columns() {
        // Seed a pre-V22 DB, populate project_settings rows exercising every
        // prefix/suffix NULL combination, then run V22 and verify the
        // concatenation backfill and the dropped columns.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v21.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v21 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(21) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        // Four rows: both, prefix-only, suffix-only, neither.
        conn.execute_batch(
            "
            INSERT INTO project_settings (project, prompt_prefix, prompt_suffix)
                VALUES ('/both', 'PRE', 'SUF');
            INSERT INTO project_settings (project, prompt_prefix, prompt_suffix)
                VALUES ('/preonly', 'ONLY-PRE', NULL);
            INSERT INTO project_settings (project, prompt_prefix, prompt_suffix)
                VALUES ('/sufonly', NULL, 'ONLY-SUF');
            INSERT INTO project_settings (project, prompt_prefix, prompt_suffix)
                VALUES ('/neither', NULL, NULL);
            ",
        )
        .unwrap();

        drop(conn);

        // Re-open — V22 applies, backfills `prompt`, drops the old columns.
        let conn = open_at(&path).unwrap();

        let cols: Vec<String> = conn
            .prepare("SELECT * FROM project_settings LIMIT 0")
            .unwrap()
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        for dropped in ["prompt_prefix", "prompt_suffix"] {
            assert!(
                !cols.iter().any(|c| c == dropped),
                "column {dropped} should have been dropped by V22 (cols: {cols:?})"
            );
        }
        assert!(cols.iter().any(|c| c == "prompt"));

        let prompt_for = |project: &str| -> Option<String> {
            conn.query_row(
                "SELECT prompt FROM project_settings WHERE project = ?1",
                [project],
                |r| r.get(0),
            )
            .unwrap()
        };
        assert_eq!(prompt_for("/both").as_deref(), Some("PRE\n\nSUF"));
        assert_eq!(prompt_for("/preonly").as_deref(), Some("ONLY-PRE"));
        assert_eq!(prompt_for("/sufonly").as_deref(), Some("ONLY-SUF"));
        assert_eq!(prompt_for("/neither"), None);

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

        // Apply ONLY migration V16 (the upgrade under test) — NOT the full
        // chain via open_at(). A later migration (V26) drops step_questions,
        // so this test must verify V16's table/indexes at V16, not at HEAD.
        // V16 is MIGRATIONS[15] (1-indexed → index 15).
        conn.execute_batch("BEGIN;").unwrap();
        MIGRATIONS[15](&conn).unwrap();
        conn.pragma_update(None, "user_version", 16u32).unwrap();
        conn.execute_batch("COMMIT;").unwrap();

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
        assert_eq!(version, 16, "only V16 was applied in isolation here");

        // Applying the rest of the chain on top of a real V16-seeded DB
        // succeeds and lands at CURRENT_VERSION — this exercises V26's
        // step_questions → interruptions cutover running over an upgraded DB.
        drop(conn);
        let conn = open_at(&path).expect("full chain must apply on top of V16");
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
    fn test_migration_v23_adds_skip_bridge_columns_to_plans() {
        // Seed a pre-V23 DB with a plans row, run V23, and verify the
        // existing row defaults both skip-bridge columns to NULL (no pending
        // skip — the correct default), and that fresh values round-trip.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v22.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v22 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(22) {
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

        // Re-open — V23 applies. Pre-V23 row must default both columns NULL.
        let conn = open_at(&path).unwrap();
        let (sid, sch): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT skip_requested_step_id, skip_changes FROM plans WHERE id = ?1",
                ["p1"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert!(
            sid.is_none() && sch.is_none(),
            "pre-V23 plans must default skip-bridge columns to NULL (got {sid:?}, {sch:?})"
        );

        // Fresh inserts can carry explicit values; they round-trip.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, \
             skip_requested_step_id, skip_changes) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params!["p2", "new", "/proj", "b", "d", "step-uuid", "discard"],
        )
        .unwrap();
        let (sid2, sch2): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT skip_requested_step_id, skip_changes FROM plans WHERE id = ?1",
                ["p2"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(sid2.as_deref(), Some("step-uuid"));
        assert_eq!(sch2.as_deref(), Some("discard"));

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

    // (test_migration_v24_adds_retry_strategy_to_plans_and_steps removed: the
    // retry_strategy columns it exercised are dropped at HEAD by V37, so the
    // post-re-open `SELECT retry_strategy` assertions no longer apply.)

    #[test]
    fn test_migration_v25_adds_short_id_and_step_dependencies() {
        // Seed a pre-V25 DB with a plan + 3 steps in sort_key order, run
        // V25, and verify the backfill: every step gets a non-null
        // unique-per-plan short_id, and a synthesized linear chain
        // (step k depends_on step k-1) is written to step_dependencies so
        // the migrated linear plan is a degenerate DAG that executes
        // identically.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v24.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v24 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(24) {
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
        for (sid, sk, title) in [
            ("s1", "a0", "Step 1"),
            ("s2", "a1", "Step 2"),
            ("s3", "a2", "Step 3"),
        ] {
            conn.execute(
                "INSERT INTO steps (id, plan_id, sort_key, title, description) \
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![sid, "p1", sk, title, "d"],
            )
            .unwrap();
        }

        drop(conn);

        // Re-open — V25 applies (backfill short_ids + linear chain edges).
        let conn = open_at(&path).unwrap();

        // (a) Every step has a non-null, unique-per-plan, 8-char short_id.
        let pairs: Vec<(String, Option<String>)> = conn
            .prepare("SELECT id, short_id FROM steps WHERE plan_id = ?1")
            .unwrap()
            .query_map(["p1"], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(pairs.len(), 3);
        let mut short_ids: Vec<String> = Vec::new();
        for (id, sid) in &pairs {
            let sid = sid
                .clone()
                .unwrap_or_else(|| panic!("step {id} must have a non-null short_id post-V25"));
            assert_eq!(
                sid.chars().count(),
                8,
                "short_id must be 8 chars (got {sid:?})"
            );
            short_ids.push(sid);
        }
        short_ids.sort();
        let n = short_ids.len();
        short_ids.dedup();
        assert_eq!(
            n,
            short_ids.len(),
            "short_ids must be unique within the plan"
        );

        // (b) The synthesized linear chain edges exist: s2->s1, s3->s2.
        let edges: Vec<(String, String)> = conn
            .prepare("SELECT step_id, depends_on_step_id FROM step_dependencies ORDER BY step_id")
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        assert_eq!(
            edges,
            vec![
                ("s2".to_string(), "s1".to_string()),
                ("s3".to_string(), "s2".to_string()),
            ],
            "V25 must synthesize a linear chain (step k depends_on step k-1)"
        );

        // (c) user_version is current.
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        // (d) Re-open is a no-op (no re-backfill, version unchanged).
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v26_cuts_step_questions_over_to_interruptions() {
        // Seed a pre-V26 DB with a plan + step + three step_questions rows
        // (answered/no-suggestions, unanswered, answered-with-suggestions),
        // run V26, and verify the cutover is faithful: every row lands in
        // `interruptions` as a `question` with the right body, synthesized
        // ascending-priority options, resolution, and open/resolved state;
        // the legacy `step_questions` table is gone; the version is current;
        // and a re-open is a no-op.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v25.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v25 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(25) {
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
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "d"],
        )
        .unwrap();

        // q1: answered, no suggestions.
        conn.execute(
            "INSERT INTO step_questions (id, step_id, attempt, question, suggestions, answer, asked_at, answered_at) \
             VALUES ('q1', 's1', 1, 'Q1?', '[]', 'A1.', '2026-05-01T10:00:00.000Z', '2026-05-01T11:00:00.000Z')",
            [],
        )
        .unwrap();
        // q2: unanswered, no suggestions (stays open; resolved_at NULL).
        conn.execute(
            "INSERT INTO step_questions (id, step_id, attempt, question, suggestions, asked_at) \
             VALUES ('q2', 's1', 1, 'Q2-pending?', '[]', '2026-05-01T10:30:00.000Z')",
            [],
        )
        .unwrap();
        // q3: answered, with suggestions (priorities must be 1,2,3 in order).
        conn.execute(
            "INSERT INTO step_questions (id, step_id, attempt, question, suggestions, answer, asked_at, answered_at) \
             VALUES ('q3', 's1', 2, 'Q3?', '[\"alpha\",\"beta\",\"gamma\"]', 'beta', '2026-05-01T12:00:00.000Z', '2026-05-01T13:00:00.000Z')",
            [],
        )
        .unwrap();

        drop(conn);

        // Re-open — V26 applies (create interruptions + data cutover + drop).
        let conn = open_at(&path).unwrap();

        // (a) All three rows are faithfully present in `interruptions`.
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM interruptions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 3, "every step_questions row must cut over");

        // q1: resolved question, empty options, answer -> resolution,
        //     answered_at -> resolved_at, no comment, kind 'question'.
        struct Row {
            step_id: String,
            attempt: i64,
            kind: String,
            body: String,
            options: String,
            resolution: Option<String>,
            comment: Option<String>,
            state: String,
            asked_at: String,
            resolved_at: Option<String>,
        }
        let q1 = conn
            .query_row(
                "SELECT step_id, attempt, kind, body, options, resolution, comment, state, asked_at, resolved_at \
                 FROM interruptions WHERE id = 'q1'",
                [],
                |r| {
                    Ok(Row {
                        step_id: r.get(0)?,
                        attempt: r.get(1)?,
                        kind: r.get(2)?,
                        body: r.get(3)?,
                        options: r.get(4)?,
                        resolution: r.get(5)?,
                        comment: r.get(6)?,
                        state: r.get(7)?,
                        asked_at: r.get(8)?,
                        resolved_at: r.get(9)?,
                    })
                },
            )
            .unwrap();
        assert_eq!(q1.step_id, "s1");
        assert_eq!(q1.attempt, 1);
        assert_eq!(q1.kind, "question");
        assert_eq!(q1.body, "Q1?");
        assert_eq!(q1.options, "[]");
        assert_eq!(q1.resolution.as_deref(), Some("A1."));
        assert_eq!(q1.comment, None, "step_questions had no comment column");
        assert_eq!(q1.state, "resolved", "an answered question is resolved");
        assert_eq!(q1.asked_at, "2026-05-01T10:00:00.000Z");
        assert_eq!(
            q1.resolved_at.as_deref(),
            Some("2026-05-01T11:00:00.000Z"),
            "answered_at must carry over to resolved_at"
        );

        // q2: unanswered -> open, NULL resolution + resolved_at.
        let (state2, resolution2, resolved_at2): (String, Option<String>, Option<String>) = conn
            .query_row(
                "SELECT state, resolution, resolved_at FROM interruptions WHERE id = 'q2'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(state2, "open", "an unanswered question stays open");
        assert_eq!(resolution2, None);
        assert_eq!(resolved_at2, None);

        // q3: suggestions synthesized into ascending-priority options.
        let (body3, options3, resolution3, state3): (String, String, Option<String>, String) = conn
            .query_row(
                "SELECT body, options, resolution, state FROM interruptions WHERE id = 'q3'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(body3, "Q3?");
        assert_eq!(resolution3.as_deref(), Some("beta"));
        assert_eq!(state3, "resolved");
        let parsed: serde_json::Value = serde_json::from_str(&options3).unwrap();
        assert_eq!(
            parsed,
            serde_json::json!([
                {"text": "alpha", "priority": 1},
                {"text": "beta",  "priority": 2},
                {"text": "gamma", "priority": 3},
            ]),
            "suggestions must synthesize into [{{text,priority}}] with ascending priorities in stored order"
        );

        // (b) The legacy `step_questions` table no longer exists.
        let sq_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='step_questions'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(sq_exists, 0, "V26 must DROP TABLE step_questions");

        // The new indexes exist, and the open-lookup one is *partial*.
        let idx_sql: Vec<(String, Option<String>)> = conn
            .prepare(
                "SELECT name, sql FROM sqlite_master \
                 WHERE type='index' AND tbl_name='interruptions' ORDER BY name",
            )
            .unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        let names: Vec<&str> = idx_sql.iter().map(|(n, _)| n.as_str()).collect();
        assert!(names.contains(&"idx_interruptions_step"));
        assert!(names.contains(&"idx_interruptions_open"));
        let open_sql = idx_sql
            .iter()
            .find(|(n, _)| n == "idx_interruptions_open")
            .and_then(|(_, s)| s.clone())
            .unwrap();
        assert!(
            open_sql.contains("WHERE state = 'open'"),
            "idx_interruptions_open must be the §6 partial index (got {open_sql:?})"
        );

        // (c) MIGRATIONS length / user_version are current. V26 is the 26th
        // migration; assert it is registered (>= 26 so appending later
        // migrations like V27 doesn't re-break this V26-specific test).
        assert!(
            MIGRATIONS.len() >= 26,
            "V26 must be registered (MIGRATIONS.len() = {})",
            MIGRATIONS.len()
        );
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        // (d) Re-open is a no-op (no re-cutover against the dropped table).
        let conn = open_at(&path).expect("re-open must not reapply migrations");
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
        let count_after: i64 = conn
            .query_row("SELECT COUNT(*) FROM interruptions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count_after, 3, "re-open must not duplicate cutover rows");
    }

    #[test]
    fn test_migration_v26_runs_clean_on_fresh_db() {
        // A fresh in-memory DB applies every migration including V26 with no
        // legacy rows to cut over: `interruptions` exists, accepts a
        // happy-path insert with the schema defaults, and `step_questions`
        // is absent.
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
            "INSERT INTO interruptions (id, step_id, attempt, kind, body, asked_at) \
             VALUES ('i1', 's1', 1, 'blocker', 'cannot proceed', '2026-05-01T10:00:00.000Z')",
            [],
        )
        .unwrap();

        let (options, state): (String, String) = conn
            .query_row(
                "SELECT options, state FROM interruptions WHERE id = 'i1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(options, "[]", "options DEFAULT '[]'");
        assert_eq!(state, "open", "state DEFAULT 'open'");

        let sq_exists: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='step_questions'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(sq_exists, 0, "fresh DB must not carry the legacy table");

        // FK cascade: deleting the step removes its interruptions.
        conn.execute("DELETE FROM steps WHERE id = 's1'", [])
            .unwrap();
        let remaining: i64 = conn
            .query_row("SELECT COUNT(*) FROM interruptions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 0, "step deletion must cascade to interruptions");
    }

    #[test]
    fn test_migration_v27_adds_review_columns_to_plans_and_steps() {
        // Seed a pre-V27 DB with a plans row + a steps row, run V27, and
        // verify the existing rows default all four review columns to NULL
        // (inherit / pending — the correct behavior), and that fresh values
        // round-trip on both tables. Mirrors `test_migration_v24`.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v26.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v26 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(26) {
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
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "d"],
        )
        .unwrap();

        drop(conn);

        // Re-open — V27 applies. Pre-V27 rows must default every new column
        // NULL on both tables (NULL = inherit / pending).
        let conn = open_at(&path).unwrap();
        let plan_re: Option<i64> = conn
            .query_row(
                "SELECT review_enabled FROM plans WHERE id = ?1",
                ["p1"],
                |r| r.get(0),
            )
            .unwrap();
        let (step_re, step_rstat, step_corrects): (Option<i64>, Option<String>, Option<String>) =
            conn.query_row(
                "SELECT review_enabled, review_status, corrects_step_id FROM steps WHERE id = ?1",
                ["s1"],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert!(
            plan_re.is_none()
                && step_re.is_none()
                && step_rstat.is_none()
                && step_corrects.is_none(),
            "pre-V27 rows must default the review columns to NULL \
             (got plan.review_enabled={plan_re:?}, step.review_enabled={step_re:?}, \
             step.review_status={step_rstat:?}, step.corrects_step_id={step_corrects:?})"
        );

        // Confirm the schema actually carries the columns on both tables.
        let plan_cols: Vec<String> = conn
            .prepare("SELECT * FROM plans LIMIT 0")
            .unwrap()
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        assert!(
            plan_cols.iter().any(|c| c == "review_enabled"),
            "plans must have a review_enabled column post-V27 (cols: {plan_cols:?})"
        );
        let step_cols: Vec<String> = conn
            .prepare("SELECT * FROM steps LIMIT 0")
            .unwrap()
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        for col in ["review_enabled", "review_status", "corrects_step_id"] {
            assert!(
                step_cols.iter().any(|c| c == col),
                "steps must have a {col} column post-V27 (cols: {step_cols:?})"
            );
        }

        // Fresh inserts can carry explicit values; they round-trip.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, review_enabled) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["p2", "new", "/proj", "b", "d", 1_i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps \
                 (id, plan_id, sort_key, title, description, review_enabled, review_status, corrects_step_id) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params!["s2", "p2", "a0", "Step", "d", 0_i64, "passed", "s1"],
        )
        .unwrap();
        let plan_re2: Option<i64> = conn
            .query_row(
                "SELECT review_enabled FROM plans WHERE id = ?1",
                ["p2"],
                |r| r.get(0),
            )
            .unwrap();
        let (step_re2, step_rstat2, step_corrects2): (Option<i64>, Option<String>, Option<String>) =
            conn.query_row(
                "SELECT review_enabled, review_status, corrects_step_id FROM steps WHERE id = ?1",
                ["s2"],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(plan_re2, Some(1));
        assert_eq!(step_re2, Some(0));
        assert_eq!(step_rstat2.as_deref(), Some("passed"));
        assert_eq!(step_corrects2.as_deref(), Some("s1"));

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
    fn test_migration_v27_runs_clean_on_fresh_db() {
        // A fresh in-memory DB applies every migration including V27: the
        // review columns exist on both tables and accept explicit values.
        let conn = open_memory().expect("open_memory");

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, review_enabled) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["p1", "slug", "/proj", "b", "d", 1_i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps \
                 (id, plan_id, sort_key, title, description, review_enabled, review_status) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params!["s1", "p1", "a0", "Step", "d", 0_i64, "in_flight"],
        )
        .unwrap();

        let plan_re: Option<i64> = conn
            .query_row(
                "SELECT review_enabled FROM plans WHERE id = 'p1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let (step_re, step_rstat): (Option<i64>, Option<String>) = conn
            .query_row(
                "SELECT review_enabled, review_status FROM steps WHERE id = 's1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(plan_re, Some(1));
        assert_eq!(step_re, Some(0));
        assert_eq!(step_rstat.as_deref(), Some("in_flight"));

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    // (test_migration_v28_adds_squash_on_complete_to_plans removed: the
    // squash_on_complete column it exercised is dropped at HEAD by V37, so the
    // post-re-open `SELECT squash_on_complete` assertions no longer apply.)

    #[test]
    fn test_migration_v29_adds_corrective_step_request_bridge() {
        // Mirror of `test_migration_v24`: seed a pre-V29 DB (a plan + a
        // step), run V29, verify the bridge table exists, accepts a request
        // row keyed to the step, cascades on step delete, user_version lands
        // at CURRENT_VERSION, and re-open is a no-op.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v28.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v28 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(28) {
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
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "d"],
        )
        .unwrap();

        drop(conn);

        // Re-open — V29 (and V30) apply.
        let conn = open_at(&path).unwrap();

        // The bridge table exists.
        let tables: Vec<String> = conn
            .prepare("SELECT name FROM sqlite_master WHERE type='table'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            tables.iter().any(|t| t == "corrective_step_requests"),
            "V29 must create corrective_step_requests (tables: {tables:?})"
        );

        // A request row keyed to the step inserts and round-trips.
        conn.execute(
            "INSERT INTO corrective_step_requests \
                (id, reviewed_step_id, reviewed_iteration, commit_sha, issues, verdict_body, requested_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            rusqlite::params!["r1", "s1", 2_i64, "deadbeef", 3_i64, "missing edge case"],
        )
        .unwrap();
        let (sid, iter, st): (String, i64, String) = conn
            .query_row(
                "SELECT reviewed_step_id, reviewed_iteration, state \
                 FROM corrective_step_requests WHERE id = ?1",
                ["r1"],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(sid, "s1");
        assert_eq!(iter, 2);
        assert_eq!(st, "open", "state must default to 'open'");

        // ON DELETE CASCADE: deleting the step removes its request rows.
        conn.execute("DELETE FROM steps WHERE id = ?1", ["s1"])
            .unwrap();
        let remaining: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM corrective_step_requests WHERE reviewed_step_id = ?1",
                ["s1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            remaining, 0,
            "deleting the reviewed step must cascade-delete its requests"
        );

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
    fn test_migration_v30_adds_max_review_corrections_to_plans() {
        // Mirror of `test_migration_v24`/`v28`: seed a pre-V30 DB, run V30,
        // verify the existing row defaults `max_review_corrections` to NULL
        // (→ built-in default), a fresh explicit value round-trips, the
        // schema carries the column, user_version lands at CURRENT_VERSION,
        // and re-open is a no-op.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v29.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v29 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(29) {
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

        // Re-open — V30 applies. The pre-V30 row must default NULL.
        let conn = open_at(&path).unwrap();
        let plan_mrc: Option<i64> = conn
            .query_row(
                "SELECT max_review_corrections FROM plans WHERE id = ?1",
                ["p1"],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            plan_mrc.is_none(),
            "pre-V30 rows must default max_review_corrections to NULL (got {plan_mrc:?})"
        );

        // The schema actually carries the column.
        let cols: Vec<String> = conn
            .prepare("SELECT * FROM plans LIMIT 0")
            .unwrap()
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        assert!(
            cols.iter().any(|c| c == "max_review_corrections"),
            "plans must have a max_review_corrections column post-V30 (cols: {cols:?})"
        );

        // A fresh insert with an explicit value round-trips, and
        // `Plan::from_row` coerces it to the typed `Option<i64>`.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, max_review_corrections) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params!["p2", "new", "/proj", "b", "d", 4_i64],
        )
        .unwrap();
        let plan_mrc2: Option<i64> = conn
            .query_row(
                "SELECT max_review_corrections FROM plans WHERE id = ?1",
                ["p2"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(plan_mrc2, Some(4));
        let p1 = crate::storage::get_plan_by_id(&conn, "p1").unwrap();
        let p2 = crate::storage::get_plan_by_id(&conn, "p2").unwrap();
        assert_eq!(p1.max_review_corrections, None, "NULL stays None");
        assert_eq!(p2.max_review_corrections, Some(4));

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
    fn test_migration_v35_adds_human_approved_to_corrective_requests() {
        // Mirror of `test_migration_v29`/`v30`: seed a pre-V35 DB (a plan + a
        // step + an open corrective request), run V35, verify the existing
        // request row defaults `human_approved` to 0, a fresh explicit value
        // round-trips, the schema carries the column, user_version lands at
        // CURRENT_VERSION, and re-open is a no-op.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v34.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v34 only.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(34) {
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
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "d"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO corrective_step_requests \
                (id, reviewed_step_id, reviewed_iteration, commit_sha, issues, verdict_body, requested_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            rusqlite::params!["r1", "s1", 1_i64, "deadbeef", 1_i64, "defect"],
        )
        .unwrap();

        drop(conn);

        // Re-open — V35 applies. The pre-V35 row must default human_approved to 0.
        let conn = open_at(&path).unwrap();
        let approved: i64 = conn
            .query_row(
                "SELECT human_approved FROM corrective_step_requests WHERE id = ?1",
                ["r1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            approved, 0,
            "pre-V35 corrective requests must default human_approved to 0"
        );

        // The schema actually carries the column.
        let cols: Vec<String> = conn
            .prepare("SELECT * FROM corrective_step_requests LIMIT 0")
            .unwrap()
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        assert!(
            cols.iter().any(|c| c == "human_approved"),
            "corrective_step_requests must have a human_approved column post-V35 (cols: {cols:?})"
        );

        // A fresh insert with an explicit value round-trips.
        conn.execute(
            "INSERT INTO corrective_step_requests \
                (id, reviewed_step_id, reviewed_iteration, commit_sha, issues, verdict_body, human_approved, requested_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
            rusqlite::params!["r2", "s1", 2_i64, "cafe", 1_i64, "human", 1_i64],
        )
        .unwrap();
        let approved2: i64 = conn
            .query_row(
                "SELECT human_approved FROM corrective_step_requests WHERE id = ?1",
                ["r2"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(approved2, 1);

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
    fn test_migration_v36_drops_questions_enabled_column() {
        // Mirror of `test_migration_v35`: seed a pre-V36 DB (a plan still
        // carrying the questions_enabled column), run V36, verify the column
        // is gone, the plan row and its other columns survive, user_version
        // lands at CURRENT_VERSION, and re-open is a no-op.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v35.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v35 only — the schema still has the column.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(35) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        // Seed a plan with an explicit (now-doomed) questions_enabled value
        // plus other columns whose data must survive the drop.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, questions_enabled, max_review_corrections)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            rusqlite::params!["p1", "old", "/proj", "b", "desc", 1_i64, 7_i64],
        )
        .unwrap();

        drop(conn);

        // Re-open — V36 applies and drops the column.
        let conn = open_at(&path).unwrap();
        let cols: Vec<String> = conn
            .prepare("SELECT * FROM plans LIMIT 0")
            .unwrap()
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        assert!(
            !cols.iter().any(|c| c == "questions_enabled"),
            "plans must NOT have a questions_enabled column post-V36 (cols: {cols:?})"
        );

        // The row and its surviving columns are intact.
        let (slug, desc, mrc): (String, String, i64) = conn
            .query_row(
                "SELECT slug, description, max_review_corrections FROM plans WHERE id = ?1",
                ["p1"],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(slug, "old");
        assert_eq!(desc, "desc");
        assert_eq!(mrc, 7, "other plan columns/data must survive the drop");

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
    fn test_migration_v37_drops_retry_strategy_and_squash_columns() {
        // Mirror of `test_migration_v36`: seed a pre-V37 DB (a plan + a step
        // still carrying the vestigial retry_strategy / squash_on_complete
        // columns), run V37, verify those columns are gone, the rows and their
        // other columns survive, user_version lands at CURRENT_VERSION, and
        // re-open is a no-op.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v36.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v36 only — the schema still has the columns.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(36) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        // Seed a plan + step with explicit (now-doomed) retry_strategy /
        // squash_on_complete values plus other columns whose data must survive.
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description, retry_strategy, squash_on_complete, max_review_corrections)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params!["p1", "old", "/proj", "b", "desc", "rollback", 1_i64, 7_i64],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description, status, attempts, retry_strategy, short_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params!["s1", "p1", "a0", "title", "sdesc", "pending", 0_i64, "keep", "abcd1234"],
        )
        .unwrap();

        drop(conn);

        // Re-open — V37 applies and drops the columns.
        let conn = open_at(&path).unwrap();

        let plan_cols: Vec<String> = conn
            .prepare("SELECT * FROM plans LIMIT 0")
            .unwrap()
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        assert!(
            !plan_cols.iter().any(|c| c == "retry_strategy"),
            "plans must NOT have a retry_strategy column post-V37 (cols: {plan_cols:?})"
        );
        assert!(
            !plan_cols.iter().any(|c| c == "squash_on_complete"),
            "plans must NOT have a squash_on_complete column post-V37 (cols: {plan_cols:?})"
        );

        let step_cols: Vec<String> = conn
            .prepare("SELECT * FROM steps LIMIT 0")
            .unwrap()
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        assert!(
            !step_cols.iter().any(|c| c == "retry_strategy"),
            "steps must NOT have a retry_strategy column post-V37 (cols: {step_cols:?})"
        );

        // The rows and their surviving columns are intact.
        let (slug, desc, mrc): (String, String, i64) = conn
            .query_row(
                "SELECT slug, description, max_review_corrections FROM plans WHERE id = ?1",
                ["p1"],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(slug, "old");
        assert_eq!(desc, "desc");
        assert_eq!(mrc, 7, "other plan columns/data must survive the drop");

        let (title, short_id): (String, String) = conn
            .query_row(
                "SELECT title, short_id FROM steps WHERE id = ?1",
                ["s1"],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(title, "title");
        assert_eq!(short_id, "abcd1234", "step data must survive the drop");

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
    fn test_migration_v31_enforces_plan_local_step_dependencies() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v30.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v30 only, then seed one valid intra-plan edge
        // and one invalid cross-plan edge that V31 must clean up.
        for (i, migration) in MIGRATIONS.iter().enumerate().take(30) {
            let version = (i as u32) + 1;
            conn.execute_batch("BEGIN;").unwrap();
            migration(&conn).unwrap();
            conn.pragma_update(None, "user_version", version).unwrap();
            conn.execute_batch("COMMIT;").unwrap();
        }

        conn.execute_batch(
            "
            INSERT INTO plans (id, slug, project, branch_name, description)
            VALUES ('p1', 'p1', '/proj', 'b', 'd'),
                   ('p2', 'p2', '/proj', 'b', 'd');
            INSERT INTO steps (id, plan_id, sort_key, title, description, acceptance_criteria, short_id)
            VALUES ('s1', 'p1', 'a', 's1', 'd', '[]', 'aaaaaaaa'),
                   ('s2', 'p1', 'b', 's2', 'd', '[]', 'bbbbbbbb'),
                   ('s3', 'p2', 'a', 's3', 'd', '[]', 'cccccccc');
            INSERT INTO step_dependencies (step_id, depends_on_step_id)
            VALUES ('s2', 's1'),
                   ('s1', 's3');
            ",
        )
        .unwrap();
        drop(conn);

        let conn = open_at(&path).unwrap();
        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);

        let valid_edges: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM step_dependencies \
                 WHERE step_id = 's2' AND depends_on_step_id = 's1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(valid_edges, 1, "V31 must preserve same-plan edges");

        let cross_edges: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM step_dependencies \
                 WHERE step_id = 's1' AND depends_on_step_id = 's3'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(cross_edges, 0, "V31 must drop invalid cross-plan edges");

        let err = conn
            .execute(
                "INSERT INTO step_dependencies (step_id, depends_on_step_id) VALUES (?1, ?2)",
                rusqlite::params!["s1", "s3"],
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("step dependencies must stay within one plan"),
            "unexpected trigger error: {err}"
        );

        conn.execute(
            "INSERT INTO step_dependencies (step_id, depends_on_step_id) VALUES (?1, ?2)",
            rusqlite::params!["s1", "s2"],
        )
        .unwrap();
        let err = conn
            .execute(
                "UPDATE step_dependencies SET depends_on_step_id = ?1 \
                 WHERE step_id = ?2 AND depends_on_step_id = ?3",
                rusqlite::params!["s3", "s1", "s2"],
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("step dependencies must stay within one plan"),
            "unexpected update-trigger error: {err}"
        );
    }

    #[test]
    fn test_migration_v32_preserves_rows_and_keeps_duplicate_attempts_allowed() {
        // V32 is a structural no-op: it rebuilds execution_logs but the
        // `UNIQUE(step_id, attempt)` it was meant to drop never existed
        // (`(step_id, attempt)` only ever had the non-unique
        // `idx_logs_step_attempt`). This test asserts what is actually true:
        // duplicate logical `(step_id, attempt)` rows are accepted BOTH
        // before V32 (proving no UNIQUE existed) AND after it, and the
        // rebuild preserves every row in id order.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v31.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        // Apply migrations v1..=v31 only, so the pre-V32 execution_logs schema
        // is present (the one V32 rebuilds).
        for (i, migration) in MIGRATIONS.iter().enumerate().take(31) {
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
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "d"],
        )
        .unwrap();
        // PROOF no UNIQUE(step_id, attempt) existed pre-V32: two rows with the
        // SAME (step_id, attempt) both insert successfully on the v31 schema.
        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, prompt_text) VALUES (?1, ?2, ?3)",
            rusqlite::params!["s1", 1_i64, "first cycle"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, prompt_text) VALUES (?1, ?2, ?3)",
            rusqlite::params!["s1", 1_i64, "second cycle"],
        )
        .expect("pre-V32 schema already allows duplicate (step_id, attempt) rows");
        drop(conn);

        // Run V32 (the rebuild). It must preserve both existing rows...
        let conn = open_at(&path).unwrap();
        // ...and continue to accept further duplicates afterwards.
        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, prompt_text) VALUES (?1, ?2, ?3)",
            rusqlite::params!["s1", 1_i64, "third cycle"],
        )
        .unwrap();

        let prompts: Vec<String> = conn
            .prepare("SELECT prompt_text FROM execution_logs WHERE step_id = ?1 ORDER BY id ASC")
            .unwrap()
            .query_map(["s1"], |r| r.get(0))
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            prompts,
            vec![
                "first cycle".to_string(),
                "second cycle".to_string(),
                "third cycle".to_string()
            ],
            "the rebuild must preserve all pre-V32 rows in id order"
        );

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v32_runs_clean_on_fresh_db() {
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
            "INSERT INTO execution_logs (step_id, attempt, prompt_text) VALUES (?1, ?2, ?3)",
            rusqlite::params!["s1", 1_i64, "first cycle"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, prompt_text) VALUES (?1, ?2, ?3)",
            rusqlite::params!["s1", 1_i64, "second cycle"],
        )
        .unwrap();

        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM execution_logs WHERE step_id = ?1 AND attempt = ?2",
                rusqlite::params!["s1", 1_i64],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            count, 2,
            "fresh DB must allow duplicate logical attempts per step"
        );
    }

    #[test]
    fn test_migration_v33_adds_cycle_index_column() {
        // Stage to V32, seed legacy rows, then apply V33 and assert the
        // new columns exist with the expected defaults.
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("old_v32.db");
        let conn = Connection::open(&path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();

        for (i, migration) in MIGRATIONS.iter().enumerate().take(32) {
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
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["s1", "p1", "a0", "Step", "d"],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, prompt_text) VALUES (?1, ?2, ?3)",
            rusqlite::params!["s1", 1_i64, "pre-v33"],
        )
        .unwrap();
        drop(conn);

        // Open via the migration runner — V33 runs and backfills.
        let conn = open_at(&path).unwrap();

        let step_cycle: i64 = conn
            .query_row(
                "SELECT current_cycle_index FROM steps WHERE id = ?1",
                ["s1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            step_cycle, 0,
            "pre-V33 step rows must backfill current_cycle_index = 0"
        );

        let log_cycle: i64 = conn
            .query_row(
                "SELECT cycle_index FROM execution_logs WHERE step_id = ?1",
                ["s1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            log_cycle, 0,
            "pre-V33 log rows must backfill cycle_index = 0"
        );

        let version: u32 = conn
            .pragma_query_value(None, "user_version", |row| row.get(0))
            .unwrap();
        assert_eq!(version, CURRENT_VERSION);
    }

    #[test]
    fn test_migration_v33_runs_clean_on_fresh_db() {
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

        // Both columns must exist with NOT NULL DEFAULT 0 — these inserts
        // omit them and still succeed.
        conn.execute(
            "INSERT INTO execution_logs (step_id, attempt, prompt_text) VALUES (?1, ?2, ?3)",
            rusqlite::params!["s1", 1_i64, "x"],
        )
        .unwrap();

        let step_cycle: i64 = conn
            .query_row(
                "SELECT current_cycle_index FROM steps WHERE id = ?1",
                ["s1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(step_cycle, 0);

        let log_cycle: i64 = conn
            .query_row(
                "SELECT cycle_index FROM execution_logs WHERE step_id = ?1",
                ["s1"],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(log_cycle, 0);
    }

    #[test]
    fn test_migration_v16_runs_clean_on_fresh_db() {
        // A fresh in-memory DB applies every migration, including V16's
        // questions_enabled add and V36's subsequent drop. A basic plan
        // insert must succeed and the questions_enabled column must be gone
        // at HEAD (V36).
        let conn = open_memory().expect("open_memory");

        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params!["p1", "slug", "/proj", "b", "d"],
        )
        .unwrap();

        let cols: Vec<String> = conn
            .prepare("SELECT * FROM plans LIMIT 0")
            .unwrap()
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        assert!(
            !cols.iter().any(|c| c == "questions_enabled"),
            "questions_enabled must be dropped by V36 (cols: {cols:?})"
        );
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
