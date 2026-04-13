// SQLite database layer

use anyhow::{Context, Result};
use rusqlite::Connection;
use std::fs;
use std::path::PathBuf;

use crate::config;

/// Current schema version. Bump this and add a new migration function
/// to `MIGRATIONS` whenever the schema changes.
#[allow(dead_code)]
const CURRENT_VERSION: u32 = 3;

/// Each migration is a function that receives a connection (already inside a transaction).
/// Migrations are 1-indexed: MIGRATIONS[0] migrates from version 0 → 1.
const MIGRATIONS: &[fn(&Connection) -> Result<()>] = &[migrate_v1, migrate_v2, migrate_v3];

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
    let conn = Connection::open(path.as_ref())
        .with_context(|| format!("Failed to open database at {}", path.as_ref().display()))?;

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
            "idx_logs_step_attempt",
            "idx_logs_step_id",
            "idx_plans_project",
            "idx_plans_project_status",
            "idx_steps_plan_id",
            "idx_steps_plan_sort",
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
}
