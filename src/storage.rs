// Storage abstraction: high-level CRUD operations wrapping db.rs
#![allow(dead_code)]

use anyhow::{Context, Result};
use rusqlite::{params, Connection};
use uuid::Uuid;

use crate::frac_index;
use crate::plan::{ExecutionLog, Plan, PlanStatus, Step, StepStatus};

// ---------------------------------------------------------------------------
// Plan operations
// ---------------------------------------------------------------------------

/// Insert a new plan and return it.
#[allow(clippy::too_many_arguments)]
pub fn create_plan(
    conn: &Connection,
    slug: &str,
    project: &str,
    branch_name: &str,
    description: &str,
    harness: Option<&str>,
    agent: Option<&str>,
    deterministic_tests: &[String],
) -> Result<Plan> {
    let id = Uuid::new_v4().to_string();
    let tests_json = serde_json::to_string(deterministic_tests)?;

    conn.execute(
        "INSERT INTO plans (id, slug, project, branch_name, description, harness, agent, deterministic_tests)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        params![id, slug, project, branch_name, description, harness, agent, tests_json],
    )
    .with_context(|| format!("Failed to insert plan '{slug}' for project '{project}'"))?;

    get_plan_by_id(conn, &id)
}

/// Find a plan by its (slug, project) combination.
pub fn get_plan_by_slug(conn: &Connection, slug: &str, project: &str) -> Result<Option<Plan>> {
    let mut stmt = conn.prepare(
        "SELECT id, slug, project, branch_name, description, status, harness, agent, deterministic_tests, created_at, updated_at
         FROM plans WHERE slug = ?1 AND project = ?2",
    )?;

    let mut rows = stmt.query_map(params![slug, project], Plan::from_row)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Fetch a plan by its primary key.
fn get_plan_by_id(conn: &Connection, id: &str) -> Result<Plan> {
    conn.query_row(
        "SELECT id, slug, project, branch_name, description, status, harness, agent, deterministic_tests, created_at, updated_at
         FROM plans WHERE id = ?1",
        params![id],
        Plan::from_row,
    )
    .with_context(|| format!("Plan not found: {id}"))
}

/// List plans. If `all` is false, only return plans for `project`.
pub fn list_plans(conn: &Connection, project: &str, all: bool) -> Result<Vec<Plan>> {
    let mut plans = Vec::new();

    if all {
        let mut stmt = conn.prepare(
            "SELECT id, slug, project, branch_name, description, status, harness, agent, deterministic_tests, created_at, updated_at
             FROM plans ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map([], Plan::from_row)?;
        for row in rows {
            plans.push(row?);
        }
    } else {
        let mut stmt = conn.prepare(
            "SELECT id, slug, project, branch_name, description, status, harness, agent, deterministic_tests, created_at, updated_at
             FROM plans WHERE project = ?1 ORDER BY created_at DESC",
        )?;
        let rows = stmt.query_map(params![project], Plan::from_row)?;
        for row in rows {
            plans.push(row?);
        }
    }

    Ok(plans)
}

/// Update a plan's status and set updated_at to now.
pub fn update_plan_status(conn: &Connection, plan_id: &str, status: PlanStatus) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET status = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![status.as_str(), plan_id],
    )?;

    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Delete a plan (cascades to steps and execution_logs via FK).
pub fn delete_plan(conn: &Connection, plan_id: &str) -> Result<()> {
    let affected = conn.execute("DELETE FROM plans WHERE id = ?1", params![plan_id])?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Step operations
// ---------------------------------------------------------------------------

/// Create a new step appended at the end of the plan's step list.
///
/// Automatically generates a sort_key after the last existing step.
#[allow(clippy::too_many_arguments)]
pub fn create_step(
    conn: &Connection,
    plan_id: &str,
    title: &str,
    description: &str,
    agent: Option<&str>,
    harness: Option<&str>,
    acceptance_criteria: &[String],
    max_retries: Option<i32>,
) -> Result<Step> {
    let id = Uuid::new_v4().to_string();
    let criteria_json = serde_json::to_string(acceptance_criteria)?;

    // Determine sort_key: after the last existing step, or initial_key if none.
    let last_key: Option<String> = conn
        .query_row(
            "SELECT sort_key FROM steps WHERE plan_id = ?1 ORDER BY sort_key DESC LIMIT 1",
            params![plan_id],
            |row| row.get(0),
        )
        .ok();

    let sort_key = match last_key {
        Some(ref k) => frac_index::key_after(k),
        None => frac_index::initial_key(),
    };

    conn.execute(
        "INSERT INTO steps (id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, max_retries)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![id, plan_id, sort_key, title, description, agent, harness, criteria_json, max_retries],
    )
    .with_context(|| format!("Failed to insert step '{title}' for plan '{plan_id}'"))?;

    get_step(conn, &id)
}

/// List steps for a plan, ordered by sort_key.
pub fn list_steps(conn: &Connection, plan_id: &str) -> Result<Vec<Step>> {
    let mut stmt = conn.prepare(
        "SELECT id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, status, attempts, max_retries, created_at, updated_at
         FROM steps WHERE plan_id = ?1 ORDER BY sort_key ASC",
    )?;

    let rows = stmt.query_map(params![plan_id], Step::from_row)?;
    let mut steps = Vec::new();
    for row in rows {
        steps.push(row?);
    }
    Ok(steps)
}

/// Fetch a single step by ID.
pub fn get_step(conn: &Connection, step_id: &str) -> Result<Step> {
    conn.query_row(
        "SELECT id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, status, attempts, max_retries, created_at, updated_at
         FROM steps WHERE id = ?1",
        params![step_id],
        Step::from_row,
    )
    .with_context(|| format!("Step not found: {step_id}"))
}

/// Update a step's status (and bump attempts if transitioning to in_progress).
pub fn update_step_status(conn: &Connection, step_id: &str, status: StepStatus) -> Result<()> {
    let affected = conn.execute(
        "UPDATE steps SET status = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![status.as_str(), step_id],
    )?;

    if affected == 0 {
        anyhow::bail!("Step not found: {step_id}");
    }
    Ok(())
}

/// Delete a step (cascades to execution_logs via FK).
pub fn delete_step(conn: &Connection, step_id: &str) -> Result<()> {
    let affected = conn.execute("DELETE FROM steps WHERE id = ?1", params![step_id])?;
    if affected == 0 {
        anyhow::bail!("Step not found: {step_id}");
    }
    Ok(())
}

/// Create a new step inserted at a specific sort_key position.
#[allow(clippy::too_many_arguments)]
pub fn create_step_at(
    conn: &Connection,
    plan_id: &str,
    sort_key: &str,
    title: &str,
    description: &str,
    agent: Option<&str>,
    harness: Option<&str>,
    acceptance_criteria: &[String],
    max_retries: Option<i32>,
) -> Result<Step> {
    let id = Uuid::new_v4().to_string();
    let criteria_json = serde_json::to_string(acceptance_criteria)?;

    conn.execute(
        "INSERT INTO steps (id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, max_retries)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        params![id, plan_id, sort_key, title, description, agent, harness, criteria_json, max_retries],
    )
    .with_context(|| format!("Failed to insert step '{title}' for plan '{plan_id}'"))?;

    get_step(conn, &id)
}

/// Update a step's title and/or description.
pub fn update_step_fields(
    conn: &Connection,
    step_id: &str,
    title: Option<&str>,
    description: Option<&str>,
) -> Result<()> {
    if let Some(t) = title {
        conn.execute(
            "UPDATE steps SET title = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
            params![t, step_id],
        )?;
    }
    if let Some(d) = description {
        conn.execute(
            "UPDATE steps SET description = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
            params![d, step_id],
        )?;
    }
    Ok(())
}

/// Reset a step's status to pending and zero out attempts.
pub fn reset_step(conn: &Connection, step_id: &str) -> Result<()> {
    let affected = conn.execute(
        "UPDATE steps SET status = 'pending', attempts = 0, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?1",
        params![step_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Step not found: {step_id}");
    }
    Ok(())
}

/// Update a step's sort_key (used for reordering).
pub fn update_step_sort_key(conn: &Connection, step_id: &str, sort_key: &str) -> Result<()> {
    let affected = conn.execute(
        "UPDATE steps SET sort_key = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![sort_key, step_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Step not found: {step_id}");
    }
    Ok(())
}

/// Get the next pending step for a plan (first by sort_key order).
pub fn get_next_pending_step(conn: &Connection, plan_id: &str) -> Result<Option<Step>> {
    let mut stmt = conn.prepare(
        "SELECT id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, status, attempts, max_retries, created_at, updated_at
         FROM steps WHERE plan_id = ?1 AND status = 'pending' ORDER BY sort_key ASC LIMIT 1",
    )?;

    let mut rows = stmt.query_map(params![plan_id], Step::from_row)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

// ---------------------------------------------------------------------------
// Execution log operations
// ---------------------------------------------------------------------------

/// Create a new execution log entry for a step attempt.
pub fn create_execution_log(
    conn: &Connection,
    step_id: &str,
    attempt: i32,
    prompt_text: Option<&str>,
    session_id: Option<&str>,
) -> Result<ExecutionLog> {
    conn.execute(
        "INSERT INTO execution_logs (step_id, attempt, prompt_text, session_id)
         VALUES (?1, ?2, ?3, ?4)",
        params![step_id, attempt, prompt_text, session_id],
    )
    .with_context(|| format!("Failed to create execution log for step '{step_id}' attempt {attempt}"))?;

    let id = conn.last_insert_rowid();
    get_execution_log_by_id(conn, id)
}

/// Get the latest (highest attempt) execution log for a step.
pub fn get_latest_log_for_step(conn: &Connection, step_id: &str) -> Result<Option<ExecutionLog>> {
    let mut stmt = conn.prepare(
        "SELECT id, step_id, attempt, started_at, duration_secs, prompt_text, diff, test_results, rolled_back, committed, commit_hash, harness_stdout, harness_stderr, cost_usd, input_tokens, output_tokens, session_id
         FROM execution_logs WHERE step_id = ?1 ORDER BY attempt DESC LIMIT 1",
    )?;

    let mut rows = stmt.query_map(params![step_id], ExecutionLog::from_row)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Update fields on an execution log (typically after the attempt completes).
#[allow(clippy::too_many_arguments)]
pub fn update_execution_log(
    conn: &Connection,
    log_id: i64,
    duration_secs: Option<f64>,
    diff: Option<&str>,
    test_results: &[String],
    rolled_back: bool,
    committed: bool,
    commit_hash: Option<&str>,
    harness_stdout: Option<&str>,
    harness_stderr: Option<&str>,
    cost_usd: Option<f64>,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
) -> Result<()> {
    let test_results_json = serde_json::to_string(test_results)?;

    let affected = conn.execute(
        "UPDATE execution_logs SET
            duration_secs = ?1,
            diff = ?2,
            test_results = ?3,
            rolled_back = ?4,
            committed = ?5,
            commit_hash = ?6,
            harness_stdout = ?7,
            harness_stderr = ?8,
            cost_usd = ?9,
            input_tokens = ?10,
            output_tokens = ?11
         WHERE id = ?12",
        params![
            duration_secs,
            diff,
            test_results_json,
            rolled_back as i32,
            committed as i32,
            commit_hash,
            harness_stdout,
            harness_stderr,
            cost_usd,
            input_tokens,
            output_tokens,
            log_id,
        ],
    )?;

    if affected == 0 {
        anyhow::bail!("Execution log not found: {log_id}");
    }
    Ok(())
}

/// List execution logs for a step, ordered by attempt.
pub fn list_execution_logs_for_step(
    conn: &Connection,
    step_id: &str,
) -> Result<Vec<ExecutionLog>> {
    let mut stmt = conn.prepare(
        "SELECT id, step_id, attempt, started_at, duration_secs, prompt_text, diff, test_results, rolled_back, committed, commit_hash, harness_stdout, harness_stderr, cost_usd, input_tokens, output_tokens, session_id
         FROM execution_logs WHERE step_id = ?1 ORDER BY attempt ASC",
    )?;

    let rows = stmt.query_map(params![step_id], ExecutionLog::from_row)?;
    let mut logs = Vec::new();
    for row in rows {
        logs.push(row?);
    }
    Ok(logs)
}

/// List all execution logs for a plan (across all steps), ordered by started_at.
pub fn list_execution_logs_for_plan(
    conn: &Connection,
    plan_id: &str,
    limit: Option<usize>,
) -> Result<Vec<(String, ExecutionLog)>> {
    let limit_val = limit.unwrap_or(100) as i64;
    let mut stmt = conn.prepare(
        "SELECT s.title, el.id, el.step_id, el.attempt, el.started_at, el.duration_secs,
                el.prompt_text, el.diff, el.test_results, el.rolled_back, el.committed,
                el.commit_hash, el.harness_stdout, el.harness_stderr, el.cost_usd,
                el.input_tokens, el.output_tokens, el.session_id
         FROM execution_logs el
         JOIN steps s ON s.id = el.step_id
         WHERE s.plan_id = ?1
         ORDER BY el.started_at DESC
         LIMIT ?2",
    )?;

    let rows = stmt.query_map(params![plan_id, limit_val], |row| {
        let step_title: String = row.get(0)?;
        // Shift columns by 1 for the ExecutionLog fields.
        let log = ExecutionLog {
            id: row.get(1)?,
            step_id: row.get(2)?,
            attempt: row.get(3)?,
            started_at: {
                let s: String = row.get(4)?;
                s.parse::<chrono::DateTime<chrono::Utc>>().map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        4,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?
            },
            duration_secs: row.get(5)?,
            prompt_text: row.get(6)?,
            diff: row.get(7)?,
            test_results: {
                let s: String = row.get(8)?;
                serde_json::from_str(&s).map_err(|e| {
                    rusqlite::Error::FromSqlConversionFailure(
                        8,
                        rusqlite::types::Type::Text,
                        Box::new(e),
                    )
                })?
            },
            rolled_back: {
                let v: i32 = row.get(9)?;
                v != 0
            },
            committed: {
                let v: i32 = row.get(10)?;
                v != 0
            },
            commit_hash: row.get(11)?,
            harness_stdout: row.get(12)?,
            harness_stderr: row.get(13)?,
            cost_usd: row.get(14)?,
            input_tokens: row.get(15)?,
            output_tokens: row.get(16)?,
            session_id: row.get(17)?,
        };
        Ok((step_title, log))
    })?;

    let mut result = Vec::new();
    for row in rows {
        result.push(row?);
    }
    Ok(result)
}

/// Fetch an execution log by its primary key.
fn get_execution_log_by_id(conn: &Connection, id: i64) -> Result<ExecutionLog> {
    conn.query_row(
        "SELECT id, step_id, attempt, started_at, duration_secs, prompt_text, diff, test_results, rolled_back, committed, commit_hash, harness_stdout, harness_stderr, cost_usd, input_tokens, output_tokens, session_id
         FROM execution_logs WHERE id = ?1",
        params![id],
        ExecutionLog::from_row,
    )
    .with_context(|| format!("Execution log not found: {id}"))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;

    fn setup() -> Connection {
        db::open_memory().expect("open_memory")
    }

    // -- Plan tests --

    #[test]
    fn test_create_plan_and_get_by_slug() {
        let conn = setup();
        let tests = vec!["cargo test".to_string(), "cargo clippy".to_string()];

        let plan = create_plan(
            &conn,
            "my-plan",
            "/tmp/proj",
            "feat/branch",
            "A test plan",
            Some("claude"),
            Some("opus"),
            &tests,
        )
        .expect("create_plan");

        assert_eq!(plan.slug, "my-plan");
        assert_eq!(plan.project, "/tmp/proj");
        assert_eq!(plan.branch_name, "feat/branch");
        assert_eq!(plan.description, "A test plan");
        assert_eq!(plan.status, PlanStatus::Planning);
        assert_eq!(plan.harness.as_deref(), Some("claude"));
        assert_eq!(plan.agent.as_deref(), Some("opus"));
        assert_eq!(plan.deterministic_tests, tests);

        // Retrieve by slug
        let found = get_plan_by_slug(&conn, "my-plan", "/tmp/proj")
            .expect("get_plan_by_slug")
            .expect("plan should exist");
        assert_eq!(found.id, plan.id);
    }

    #[test]
    fn test_get_plan_by_slug_not_found() {
        let conn = setup();
        let found = get_plan_by_slug(&conn, "nope", "/tmp/proj").expect("get_plan_by_slug");
        assert!(found.is_none());
    }

    #[test]
    fn test_list_plans_filters_by_project() {
        let conn = setup();

        create_plan(&conn, "p1", "/proj-a", "b1", "desc", None, None, &[]).unwrap();
        create_plan(&conn, "p2", "/proj-b", "b2", "desc", None, None, &[]).unwrap();
        create_plan(&conn, "p3", "/proj-a", "b3", "desc", None, None, &[]).unwrap();

        let proj_a = list_plans(&conn, "/proj-a", false).unwrap();
        assert_eq!(proj_a.len(), 2);
        for p in &proj_a {
            assert_eq!(p.project, "/proj-a");
        }

        let all = list_plans(&conn, "/proj-a", true).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_update_plan_status() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        assert_eq!(plan.status, PlanStatus::Planning);

        update_plan_status(&conn, &plan.id, PlanStatus::InProgress).unwrap();

        let found = get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert_eq!(found.status, PlanStatus::InProgress);
        // updated_at should have changed
        assert!(found.updated_at >= plan.updated_at);
    }

    #[test]
    fn test_delete_plan_cascades() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let step = create_step(&conn, &plan.id, "step", "desc", None, None, &[], None).unwrap();
        create_execution_log(&conn, &step.id, 1, None, None).unwrap();

        delete_plan(&conn, &plan.id).unwrap();

        // Plan gone
        assert!(get_plan_by_slug(&conn, "s", "/p").unwrap().is_none());
        // Steps gone
        assert!(list_steps(&conn, &plan.id).unwrap().is_empty());
        // Logs gone
        assert!(get_latest_log_for_step(&conn, &step.id).unwrap().is_none());
    }

    #[test]
    fn test_delete_plan_not_found() {
        let conn = setup();
        let result = delete_plan(&conn, "nonexistent");
        assert!(result.is_err());
    }

    // -- Step tests --

    #[test]
    fn test_create_step_generates_sort_keys() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        let s1 = create_step(&conn, &plan.id, "First", "d1", None, None, &[], None).unwrap();
        let s2 = create_step(&conn, &plan.id, "Second", "d2", None, None, &[], None).unwrap();
        let s3 = create_step(&conn, &plan.id, "Third", "d3", None, None, &[], None).unwrap();

        // Sort keys should be monotonically increasing
        assert!(s1.sort_key < s2.sort_key, "{} < {}", s1.sort_key, s2.sort_key);
        assert!(s2.sort_key < s3.sort_key, "{} < {}", s2.sort_key, s3.sort_key);

        // First key should be initial_key
        assert_eq!(s1.sort_key, frac_index::initial_key());
    }

    #[test]
    fn test_list_steps_ordered_by_sort_key() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        create_step(&conn, &plan.id, "First", "d", None, None, &[], None).unwrap();
        create_step(&conn, &plan.id, "Second", "d", None, None, &[], None).unwrap();
        create_step(&conn, &plan.id, "Third", "d", None, None, &[], None).unwrap();

        let steps = list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].title, "First");
        assert_eq!(steps[1].title, "Second");
        assert_eq!(steps[2].title, "Third");

        // Verify sort_key ordering
        for i in 0..steps.len() - 1 {
            assert!(steps[i].sort_key < steps[i + 1].sort_key);
        }
    }

    #[test]
    fn test_step_acceptance_criteria_roundtrip() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        let criteria = vec!["tests pass".to_string(), "lint clean".to_string()];
        let step = create_step(&conn, &plan.id, "Step", "desc", None, None, &criteria, Some(3)).unwrap();

        assert_eq!(step.acceptance_criteria, criteria);
        assert_eq!(step.max_retries, Some(3));
        assert_eq!(step.status, StepStatus::Pending);
        assert_eq!(step.attempts, 0);
    }

    #[test]
    fn test_update_step_status() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let step = create_step(&conn, &plan.id, "Step", "desc", None, None, &[], None).unwrap();

        update_step_status(&conn, &step.id, StepStatus::Complete).unwrap();

        let updated = get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.status, StepStatus::Complete);
    }

    #[test]
    fn test_delete_step() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let step = create_step(&conn, &plan.id, "Step", "desc", None, None, &[], None).unwrap();
        create_execution_log(&conn, &step.id, 1, None, None).unwrap();

        delete_step(&conn, &step.id).unwrap();

        assert!(list_steps(&conn, &plan.id).unwrap().is_empty());
        // Logs should cascade delete
        assert!(get_latest_log_for_step(&conn, &step.id).unwrap().is_none());
    }

    #[test]
    fn test_get_next_pending_step() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        let s1 = create_step(&conn, &plan.id, "First", "d", None, None, &[], None).unwrap();
        let s2 = create_step(&conn, &plan.id, "Second", "d", None, None, &[], None).unwrap();

        // Both pending — should return first by sort_key
        let next = get_next_pending_step(&conn, &plan.id).unwrap().unwrap();
        assert_eq!(next.id, s1.id);

        // Mark first as complete
        update_step_status(&conn, &s1.id, StepStatus::Complete).unwrap();

        let next = get_next_pending_step(&conn, &plan.id).unwrap().unwrap();
        assert_eq!(next.id, s2.id);

        // Mark second as complete
        update_step_status(&conn, &s2.id, StepStatus::Complete).unwrap();

        let next = get_next_pending_step(&conn, &plan.id).unwrap();
        assert!(next.is_none());
    }

    // -- Execution log tests --

    #[test]
    fn test_create_and_get_execution_log() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let step = create_step(&conn, &plan.id, "Step", "desc", None, None, &[], None).unwrap();

        let log = create_execution_log(&conn, &step.id, 1, Some("do the thing"), Some("sess-1")).unwrap();

        assert_eq!(log.step_id, step.id);
        assert_eq!(log.attempt, 1);
        assert_eq!(log.prompt_text.as_deref(), Some("do the thing"));
        assert_eq!(log.session_id.as_deref(), Some("sess-1"));
        assert!(!log.committed);
        assert!(!log.rolled_back);
        assert!(log.test_results.is_empty());
    }

    #[test]
    fn test_get_latest_log_for_step() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let step = create_step(&conn, &plan.id, "Step", "desc", None, None, &[], None).unwrap();

        create_execution_log(&conn, &step.id, 1, Some("first"), None).unwrap();
        create_execution_log(&conn, &step.id, 2, Some("second"), None).unwrap();

        let latest = get_latest_log_for_step(&conn, &step.id).unwrap().unwrap();
        assert_eq!(latest.attempt, 2);
        assert_eq!(latest.prompt_text.as_deref(), Some("second"));
    }

    #[test]
    fn test_update_execution_log() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let step = create_step(&conn, &plan.id, "Step", "desc", None, None, &[], None).unwrap();
        let log = create_execution_log(&conn, &step.id, 1, None, None).unwrap();

        let test_results = vec!["test1: pass".to_string(), "test2: fail".to_string()];
        update_execution_log(
            &conn,
            log.id,
            Some(45.5),
            Some("+added line"),
            &test_results,
            false,
            true,
            Some("abc123"),
            Some("stdout"),
            Some("stderr"),
            Some(0.05),
            Some(1000),
            Some(500),
        )
        .unwrap();

        let updated = get_latest_log_for_step(&conn, &step.id).unwrap().unwrap();
        assert_eq!(updated.duration_secs, Some(45.5));
        assert_eq!(updated.diff.as_deref(), Some("+added line"));
        assert_eq!(updated.test_results, test_results);
        assert!(!updated.rolled_back);
        assert!(updated.committed);
        assert_eq!(updated.commit_hash.as_deref(), Some("abc123"));
        assert_eq!(updated.harness_stdout.as_deref(), Some("stdout"));
        assert_eq!(updated.harness_stderr.as_deref(), Some("stderr"));
        assert_eq!(updated.cost_usd, Some(0.05));
        assert_eq!(updated.input_tokens, Some(1000));
        assert_eq!(updated.output_tokens, Some(500));
    }

    #[test]
    fn test_json_roundtrip_deterministic_tests() {
        let conn = setup();
        let tests = vec![
            "cargo build".to_string(),
            "cargo test".to_string(),
            "cargo clippy -- -D warnings".to_string(),
        ];

        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &tests).unwrap();
        let found = get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert_eq!(found.deterministic_tests, tests);
        assert_eq!(found.id, plan.id);
    }

    #[test]
    fn test_json_roundtrip_acceptance_criteria() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        let criteria = vec![
            "All tests pass".to_string(),
            "No clippy warnings".to_string(),
            "Code coverage > 80%".to_string(),
        ];
        let step = create_step(&conn, &plan.id, "Step", "d", None, None, &criteria, None).unwrap();

        let fetched = get_step(&conn, &step.id).unwrap();
        assert_eq!(fetched.acceptance_criteria, criteria);
    }

    #[test]
    fn test_json_roundtrip_empty_arrays() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        assert!(plan.deterministic_tests.is_empty());

        let step = create_step(&conn, &plan.id, "Step", "d", None, None, &[], None).unwrap();
        assert!(step.acceptance_criteria.is_empty());
    }
}
