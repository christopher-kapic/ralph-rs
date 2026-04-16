// SQLite database layer

use anyhow::{Context, Result};
use rusqlite::Connection;
use std::fs;
use std::path::PathBuf;

use crate::config;

/// Each migration is a function that receives a connection (already inside a transaction).
/// Migrations are 1-indexed: MIGRATIONS[0] migrates from version 0 → 1.
const MIGRATIONS: &[fn(&Connection) -> Result<()>] = &[
    migrate_v1, migrate_v2, migrate_v3, migrate_v4, migrate_v5, migrate_v6, migrate_v7, migrate_v8,
    migrate_v9,
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
