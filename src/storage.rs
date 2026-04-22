// Storage abstraction: high-level CRUD operations wrapping db.rs

use anyhow::{Context, Result};
use rusqlite::types::Value;
use rusqlite::{Connection, params, params_from_iter};
use uuid::Uuid;

use crate::frac_index;
use crate::plan::{
    ChangePolicy, ExecutionLog, PLAN_COLUMNS, Phase, Plan, PlanStatus, Step, StepStatus,
};
use crate::run_lock::{LIVE_RUN_COLUMNS, LiveRun};

/// Canonical column list for `SELECT` queries against the `steps` table.
///
/// Matches the physical table layout after all migrations so [`Step::from_row`]
/// can index by column position. Kept as a single shared constant so adding a
/// new column (V13+ tags etc.) only requires editing one place instead of the
/// dozen scattered SELECTs.
const STEP_COLUMNS: &str = "id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, status, attempts, max_retries, created_at, updated_at, model, skipped_reason, change_policy, tags";

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
    let query = format!("SELECT {PLAN_COLUMNS} FROM plans WHERE slug = ?1 AND project = ?2");
    let mut stmt = conn.prepare(&query)?;

    let mut rows = stmt.query_map(params![slug, project], Plan::from_row)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Fetch a plan by its primary key.
fn get_plan_by_id(conn: &Connection, id: &str) -> Result<Plan> {
    let query = format!("SELECT {PLAN_COLUMNS} FROM plans WHERE id = ?1");
    conn.query_row(&query, params![id], Plan::from_row)
        .with_context(|| format!("Plan not found: {id}"))
}

/// Fetch just the slug for a plan by its primary key.
pub fn get_plan_slug_by_id(conn: &Connection, id: &str) -> Result<Option<String>> {
    let mut stmt = conn.prepare("SELECT slug FROM plans WHERE id = ?1")?;
    let mut rows = stmt.query_map(params![id], |row| row.get::<_, String>(0))?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Find the most recent active plan for a project. Active means in_progress,
/// ready, or failed. When `include_complete` is true, completed plans are also
/// considered (useful for `status` after a plan finishes).
pub fn find_active_plan(
    conn: &Connection,
    project: &str,
    include_complete: bool,
) -> Result<Option<Plan>> {
    let mut statuses: Vec<&'static str> = vec![
        PlanStatus::InProgress.as_str(),
        PlanStatus::Ready.as_str(),
        PlanStatus::Failed.as_str(),
    ];
    if include_complete {
        statuses.push(PlanStatus::Complete.as_str());
    }

    let placeholders = (0..statuses.len())
        .map(|i| format!("?{}", i + 2))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT {PLAN_COLUMNS} FROM plans \
         WHERE project = ?1 AND status IN ({placeholders}) \
         ORDER BY created_at DESC LIMIT 1"
    );

    let mut params: Vec<Value> = Vec::with_capacity(statuses.len() + 1);
    params.push(Value::Text(project.to_string()));
    for s in &statuses {
        params.push(Value::Text((*s).to_string()));
    }

    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query_map(params_from_iter(params.iter()), Plan::from_row)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// List plans. If `all` is false, only return plans for `project`.
pub fn list_plans(conn: &Connection, project: &str, all: bool) -> Result<Vec<Plan>> {
    let mut plans = Vec::new();

    if all {
        let query = format!("SELECT {PLAN_COLUMNS} FROM plans ORDER BY created_at DESC");
        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map([], Plan::from_row)?;
        for row in rows {
            plans.push(row?);
        }
    } else {
        let query =
            format!("SELECT {PLAN_COLUMNS} FROM plans WHERE project = ?1 ORDER BY created_at DESC");
        let mut stmt = conn.prepare(&query)?;
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

/// Set the plan-generation harness for a plan.
pub fn set_plan_harness_gen(conn: &Connection, plan_id: &str, harness: Option<&str>) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET plan_harness = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![harness, plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Set the plan-scope prompt prefix. Pass `None` to clear.
pub fn set_plan_prompt_prefix(
    conn: &Connection,
    plan_id: &str,
    prefix: Option<&str>,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET prompt_prefix = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![prefix, plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Set the plan-scope prompt suffix. Pass `None` to clear.
pub fn set_plan_prompt_suffix(
    conn: &Connection,
    plan_id: &str,
    suffix: Option<&str>,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET prompt_suffix = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![suffix, plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Set the plan-scope context prepend override. Pass `None` to fall back to
/// [`crate::prompt::DEFAULT_CONTEXT_PREPEND`]. An empty-string argument is
/// stored verbatim and means "no prepend at all" for this plan — see
/// [`crate::plan::Plan::context_prepend`] for the precedence rules.
pub fn set_plan_context_prepend(
    conn: &Connection,
    plan_id: &str,
    prepend: Option<&str>,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET context_prepend = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![prepend, plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Project settings (prompt prefix/suffix at project scope)
// ---------------------------------------------------------------------------

/// Prefix and suffix pair loaded from the `project_settings` table. Both
/// fields `None` represents "no project-scope wrap configured".
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectSettings {
    pub prompt_prefix: Option<String>,
    pub prompt_suffix: Option<String>,
}

/// Read project-scope settings for `project`. Returns a zero-value struct
/// when no row exists — callers treat missing rows identically to NULLs.
pub fn get_project_settings(conn: &Connection, project: &str) -> Result<ProjectSettings> {
    let mut stmt = conn
        .prepare("SELECT prompt_prefix, prompt_suffix FROM project_settings WHERE project = ?1")?;
    let mut rows = stmt.query_map(params![project], |row| {
        Ok(ProjectSettings {
            prompt_prefix: row.get(0)?,
            prompt_suffix: row.get(1)?,
        })
    })?;
    match rows.next() {
        Some(row) => Ok(row?),
        None => Ok(ProjectSettings::default()),
    }
}

/// Upsert the project-scope prompt prefix. Pass `None` to clear the column.
pub fn set_project_prompt_prefix(
    conn: &Connection,
    project: &str,
    prefix: Option<&str>,
) -> Result<()> {
    // ON CONFLICT … DO UPDATE keeps the sibling suffix untouched so a
    // standalone "set prefix" call never silently wipes a previously-set
    // suffix on the same project row.
    conn.execute(
        "INSERT INTO project_settings (project, prompt_prefix)
         VALUES (?1, ?2)
         ON CONFLICT(project) DO UPDATE SET
             prompt_prefix = excluded.prompt_prefix,
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
        params![project, prefix],
    )?;
    Ok(())
}

/// Upsert the project-scope prompt suffix. Pass `None` to clear the column.
pub fn set_project_prompt_suffix(
    conn: &Connection,
    project: &str,
    suffix: Option<&str>,
) -> Result<()> {
    conn.execute(
        "INSERT INTO project_settings (project, prompt_suffix)
         VALUES (?1, ?2)
         ON CONFLICT(project) DO UPDATE SET
             prompt_suffix = excluded.prompt_suffix,
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
        params![project, suffix],
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Step operations
// ---------------------------------------------------------------------------

/// Create a new step appended at the end of the plan's step list.
///
/// Automatically generates a sort_key after the last existing step.
/// Returns the new step and its 1-based position in the plan.
///
/// `change_policy`: pass `None` to default to [`ChangePolicy::Required`]
/// (the pre-V12 behavior). `Some(policy)` records the caller's explicit
/// choice. Kept as an Option to avoid churning every existing callsite — the
/// default behavior is what most callers want.
///
/// `tags`: optional per-step free-form string tags. Pass `None` to default
/// to an empty list (the pre-V13 behavior). Callers that already care about
/// tags can pass `Some(&tags)` to seed them at creation time.
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
    model: Option<&str>,
    change_policy: Option<ChangePolicy>,
    tags: Option<&[String]>,
) -> Result<(Step, usize)> {
    let id = Uuid::new_v4().to_string();
    let criteria_json = serde_json::to_string(acceptance_criteria)?;
    let change_policy = change_policy.unwrap_or_default();
    let tags_json = serde_json::to_string(tags.unwrap_or(&[]))?;

    // Determine sort_key: after the last existing step, or initial_key if none.
    let last_key: Option<String> = conn
        .query_row(
            "SELECT sort_key FROM steps WHERE plan_id = ?1 ORDER BY sort_key DESC LIMIT 1",
            params![plan_id],
            |row| row.get(0),
        )
        .ok();

    let sort_key = match last_key {
        Some(ref k) => frac_index::key_after(k)?,
        None => frac_index::initial_key(),
    };

    conn.execute(
        "INSERT INTO steps (id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, max_retries, model, change_policy, tags)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![id, plan_id, sort_key, title, description, agent, harness, criteria_json, max_retries, model, change_policy.as_str(), tags_json],
    )
    .with_context(|| format!("Failed to insert step '{title}' for plan '{plan_id}'"))?;

    // The new step is always appended, so its position is the total step count.
    let position: usize = conn.query_row(
        "SELECT COUNT(*) FROM steps WHERE plan_id = ?1",
        params![plan_id],
        |row| row.get(0),
    )?;

    Ok((get_step(conn, &id)?, position))
}

/// List steps for a plan, ordered by sort_key.
pub fn list_steps(conn: &Connection, plan_id: &str) -> Result<Vec<Step>> {
    let sql = format!("SELECT {STEP_COLUMNS} FROM steps WHERE plan_id = ?1 ORDER BY sort_key ASC",);
    let mut stmt = conn.prepare(&sql)?;

    let rows = stmt.query_map(params![plan_id], Step::from_row)?;
    let mut steps = Vec::new();
    for row in rows {
        steps.push(row?);
    }
    Ok(steps)
}

/// Fetch a single step by ID.
pub fn get_step(conn: &Connection, step_id: &str) -> Result<Step> {
    let sql = format!("SELECT {STEP_COLUMNS} FROM steps WHERE id = ?1");
    conn.query_row(&sql, params![step_id], Step::from_row)
        .with_context(|| format!("Step not found: {step_id}"))
}

/// Fetch a single step by ID, returning `None` if no row matches.
///
/// Unlike [`get_step`] (which errors on missing), this variant is useful when
/// the caller wants to handle the "not found" case explicitly (e.g. validating
/// a user-supplied `--step-id` flag).
pub fn get_step_by_id(conn: &Connection, step_id: &str) -> Result<Option<Step>> {
    let sql = format!("SELECT {STEP_COLUMNS} FROM steps WHERE id = ?1");
    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query_map(params![step_id], Step::from_row)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Update a step's status. Does not modify `attempts`; use [`set_step_attempts`] for that.
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

/// Atomically transition a step's status from `expected` to `new_status`.
///
/// Unlike [`update_step_status`], this variant is a no-op (returns `Ok(false)`)
/// when the row is missing or its current status doesn't equal `expected`.
/// The read+check+write is collapsed into a single `UPDATE ... WHERE status = ?`
/// so there's no TOCTOU gap between observing the current status and writing
/// the new one.
///
/// Returns `Ok(true)` if a row was updated, `Ok(false)` if none matched.
/// Used by `ralph cancel`'s stale-run finalization to flip `InProgress`
/// to `Aborted` only when the runner hasn't already moved it to a terminal
/// status during its own cleanup.
pub fn update_step_status_if(
    conn: &Connection,
    step_id: &str,
    expected: StepStatus,
    new_status: StepStatus,
) -> Result<bool> {
    let affected = conn.execute(
        "UPDATE steps SET status = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE id = ?2 AND status = ?3",
        params![new_status.as_str(), step_id, expected.as_str()],
    )?;
    Ok(affected > 0)
}

/// Mark a step as skipped and record the operator-supplied reason (if any).
///
/// Writes `status` and `skipped_reason` in a single UPDATE so a concurrent
/// reader can't observe the skipped status without its reason.
pub fn mark_step_skipped(conn: &Connection, step_id: &str, reason: Option<&str>) -> Result<()> {
    let affected = conn.execute(
        "UPDATE steps SET status = ?1, skipped_reason = ?2, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?3",
        params![StepStatus::Skipped.as_str(), reason, step_id],
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
/// Returns the new step and its 1-based position in the plan.
///
/// `change_policy`: see [`create_step`] — `None` defaults to
/// [`ChangePolicy::Required`].
/// `tags`: see [`create_step`] — `None` defaults to an empty list.
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
    model: Option<&str>,
    change_policy: Option<ChangePolicy>,
    tags: Option<&[String]>,
) -> Result<(Step, usize)> {
    let id = Uuid::new_v4().to_string();
    let criteria_json = serde_json::to_string(acceptance_criteria)?;
    let change_policy = change_policy.unwrap_or_default();
    let tags_json = serde_json::to_string(tags.unwrap_or(&[]))?;

    conn.execute(
        "INSERT INTO steps (id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, max_retries, model, change_policy, tags)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![id, plan_id, sort_key, title, description, agent, harness, criteria_json, max_retries, model, change_policy.as_str(), tags_json],
    )
    .with_context(|| format!("Failed to insert step '{title}' for plan '{plan_id}'"))?;

    // Count steps with sort_key <= the new one to get the 1-based position.
    let position: usize = conn.query_row(
        "SELECT COUNT(*) FROM steps WHERE plan_id = ?1 AND sort_key <= ?2",
        params![plan_id, sort_key],
        |row| row.get(0),
    )?;

    Ok((get_step(conn, &id)?, position))
}

/// Extended step update: title, description, agent, harness, criteria, max_retries, model, change_policy, tags.
///
/// - `agent_update`: `Some(Some("name"))` sets the agent, `Some(None)` clears it
///   (sets to NULL), `None` means don't change.
/// - `harness_update`: same pattern as agent.
/// - `criteria_update`: `Some(slice)` replaces the entire criteria list,
///   `None` means don't change.
/// - `retries_update`: `Some(Some(N))` sets max_retries to N,
///   `Some(None)` clears it (sets to NULL / plan default),
///   `None` means don't change.
/// - `model_update`: same pattern as agent — `Some(Some("name"))` sets the
///   per-step model override, `Some(None)` clears it, `None` means don't change.
/// - `change_policy_update`: `Some(policy)` replaces the stored policy,
///   `None` means don't change. `change_policy` is NOT NULL at the DB level
///   so there's no "clear" form — you always substitute one valid policy
///   for another.
/// - `tags_update`: `Some(slice)` replaces the entire tag list (pass an
///   empty slice to clear all tags), `None` means don't change.
#[allow(clippy::too_many_arguments)]
pub fn update_step_fields_ext(
    conn: &Connection,
    step_id: &str,
    title: Option<&str>,
    description: Option<&str>,
    agent_update: Option<Option<&str>>,
    harness_update: Option<Option<&str>>,
    criteria_update: Option<&[String]>,
    retries_update: Option<Option<i32>>,
    model_update: Option<Option<&str>>,
    change_policy_update: Option<ChangePolicy>,
    tags_update: Option<&[String]>,
) -> Result<()> {
    // Build a single UPDATE with dynamic SET clauses so all changed fields
    // share one `updated_at` and a partial failure can't leave the row half
    // updated.
    let mut clauses: Vec<&str> = Vec::new();
    let mut values: Vec<Value> = Vec::new();

    let text_or_null = |v: Option<&str>| match v {
        Some(s) => Value::Text(s.to_string()),
        None => Value::Null,
    };

    if let Some(t) = title {
        clauses.push("title = ?");
        values.push(Value::Text(t.to_string()));
    }
    if let Some(d) = description {
        clauses.push("description = ?");
        values.push(Value::Text(d.to_string()));
    }
    if let Some(agent) = agent_update {
        clauses.push("agent = ?");
        values.push(text_or_null(agent));
    }
    if let Some(harness) = harness_update {
        clauses.push("harness = ?");
        values.push(text_or_null(harness));
    }
    if let Some(criteria) = criteria_update {
        let criteria_json = serde_json::to_string(criteria)?;
        clauses.push("acceptance_criteria = ?");
        values.push(Value::Text(criteria_json));
    }
    if let Some(retries) = retries_update {
        clauses.push("max_retries = ?");
        values.push(match retries {
            Some(n) => Value::Integer(n as i64),
            None => Value::Null,
        });
    }
    if let Some(model) = model_update {
        clauses.push("model = ?");
        values.push(text_or_null(model));
    }
    if let Some(policy) = change_policy_update {
        clauses.push("change_policy = ?");
        values.push(Value::Text(policy.as_str().to_string()));
    }
    if let Some(tags) = tags_update {
        let tags_json = serde_json::to_string(tags)?;
        clauses.push("tags = ?");
        values.push(Value::Text(tags_json));
    }

    if clauses.is_empty() {
        return Ok(());
    }

    clauses.push("updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')");
    let sql = format!("UPDATE steps SET {} WHERE id = ?", clauses.join(", "));
    values.push(Value::Text(step_id.to_string()));

    let tx = conn
        .unchecked_transaction()
        .context("beginning step update transaction")?;
    let affected = tx.execute(&sql, params_from_iter(values.iter()))?;
    if affected == 0 {
        anyhow::bail!("Step not found: {step_id}");
    }
    tx.commit().context("committing step update transaction")?;
    Ok(())
}

/// Reset a step's status to pending and zero out attempts.
///
/// Also deletes the step's `execution_logs` rows — otherwise the zeroed
/// attempt counter collides with the `UNIQUE(step_id, attempt)` constraint
/// when the executor tries to create a fresh attempt=1 log on the next run
/// (e.g. via `ralph resume` on an in-progress step).
pub fn reset_step(conn: &Connection, step_id: &str) -> Result<()> {
    conn.execute(
        "DELETE FROM execution_logs WHERE step_id = ?1",
        params![step_id],
    )?;
    let affected = conn.execute(
        "UPDATE steps SET status = ?1, attempts = 0, skipped_reason = NULL, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![StepStatus::Pending.as_str(), step_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Step not found: {step_id}");
    }
    Ok(())
}

/// Flip every InProgress step for a plan to Aborted and return the affected rows.
///
/// Called at the start of `run_plan` / `resume_plan` to clean up orphaned
/// InProgress rows left behind by a crashed runner (OOM, disk full, hard kill).
/// The run lock is held by the caller, so any InProgress row observed here
/// cannot belong to a live run — it is definitively stale.
///
/// Uses `RETURNING` (bundled rusqlite supports it) so the pre-update row
/// snapshot is atomic with the flip: no TOCTOU window where a concurrent reader
/// sees the Aborted row but the caller's return slice reflects the pre-update
/// state.
pub fn sweep_stale_in_progress(conn: &Connection, plan_id: &str) -> Result<Vec<Step>> {
    let sql = format!(
        "UPDATE steps SET status = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE plan_id = ?2 AND status = ?3
         RETURNING {STEP_COLUMNS}",
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        params![
            StepStatus::Aborted.as_str(),
            plan_id,
            StepStatus::InProgress.as_str(),
        ],
        Step::from_row,
    )?;
    let mut swept = Vec::new();
    for row in rows {
        swept.push(row?);
    }
    // Sort by sort_key so callers can report them in plan order.
    swept.sort_by(|a, b| a.sort_key.cmp(&b.sort_key));
    Ok(swept)
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
#[allow(dead_code)]
pub fn get_next_pending_step(conn: &Connection, plan_id: &str) -> Result<Option<Step>> {
    let sql = format!(
        "SELECT {STEP_COLUMNS} FROM steps WHERE plan_id = ?1 AND status = ?2 ORDER BY sort_key ASC LIMIT 1",
    );
    let mut stmt = conn.prepare(&sql)?;

    let mut rows = stmt.query_map(
        params![plan_id, StepStatus::Pending.as_str()],
        Step::from_row,
    )?;
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
    .with_context(|| {
        format!("Failed to create execution log for step '{step_id}' attempt {attempt}")
    })?;

    let id = conn.last_insert_rowid();
    get_execution_log_by_id(conn, id)
}

/// Get the latest (highest attempt) execution log for a step.
///
/// Currently only referenced from tests — kept in the public API because it
/// was previously used by the prior-step-summaries builder and is still a
/// natural helper for anyone adding log-replay or post-mortem features.
#[allow(dead_code)]
pub fn get_latest_log_for_step(conn: &Connection, step_id: &str) -> Result<Option<ExecutionLog>> {
    let mut stmt = conn.prepare(
        "SELECT id, step_id, attempt, started_at, duration_secs, prompt_text, diff, test_results, rolled_back, committed, commit_hash, harness_stdout, harness_stderr, cost_usd, input_tokens, output_tokens, session_id, termination_reason, test_status
         FROM execution_logs WHERE step_id = ?1 ORDER BY attempt DESC LIMIT 1",
    )?;

    let mut rows = stmt.query_map(params![step_id], ExecutionLog::from_row)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Update fields on an execution log (typically after the attempt completes).
///
/// `termination_reason` records *why* this attempt stopped (success, timeout,
/// test failure, hook failure, user interrupt, etc.). `test_status` records
/// the outcome of the test phase specifically — separate because tests can be
/// `NotConfigured` or `NotRun` without the attempt itself failing.
///
/// ## COALESCE semantics
///
/// `session_id`, `termination_reason`, and `test_status` are all written with
/// `COALESCE(?n, <column>)`: passing `None` preserves whatever is already in
/// the row, passing `Some(...)` overwrites. This lets non-terminal writers
/// (intermediate progress updates within a retry loop) leave those columns
/// alone while terminal writers stomp them with the authoritative final
/// values. At every *terminal* callsite in the executor, callers MUST pass
/// `Some(...)` for `termination_reason`; `test_status` should be
/// `Some(TestStatus::NotRun)` for rows that never reached the test phase.
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
    session_id: Option<&str>,
    termination_reason: Option<crate::plan::TerminationReason>,
    test_status: Option<crate::plan::TestStatus>,
) -> Result<()> {
    debug_assert!(
        !(rolled_back && committed),
        "execution log cannot be both rolled_back and committed",
    );

    let test_results_json = serde_json::to_string(test_results)?;
    let termination_reason_str: Option<&str> = termination_reason.as_ref().map(|r| r.as_str());
    let test_status_str: Option<&str> = test_status.as_ref().map(|s| s.as_str());

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
            output_tokens = ?11,
            session_id = COALESCE(?12, session_id),
            termination_reason = COALESCE(?13, termination_reason),
            test_status = COALESCE(?14, test_status)
         WHERE id = ?15",
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
            session_id,
            termination_reason_str,
            test_status_str,
            log_id,
        ],
    )?;

    if affected == 0 {
        anyhow::bail!("Execution log not found: {log_id}");
    }
    Ok(())
}

/// List execution logs for a step, ordered by attempt.
pub fn list_execution_logs_for_step(conn: &Connection, step_id: &str) -> Result<Vec<ExecutionLog>> {
    let mut stmt = conn.prepare(
        "SELECT id, step_id, attempt, started_at, duration_secs, prompt_text, diff, test_results, rolled_back, committed, commit_hash, harness_stdout, harness_stderr, cost_usd, input_tokens, output_tokens, session_id, termination_reason, test_status
         FROM execution_logs WHERE step_id = ?1 ORDER BY attempt ASC",
    )?;

    let rows = stmt.query_map(params![step_id], ExecutionLog::from_row)?;
    let mut logs = Vec::new();
    for row in rows {
        logs.push(row?);
    }
    Ok(logs)
}

/// List all execution logs for a plan (across all steps), ordered by
/// started_at descending (most recent first).
///
/// When `limit` is `Some(n)`, returns at most `n` rows. When `limit` is
/// `None`, returns every matching row with no cap.
pub fn list_execution_logs_for_plan(
    conn: &Connection,
    plan_id: &str,
    limit: Option<usize>,
) -> Result<Vec<(String, ExecutionLog)>> {
    // SQLite treats a negative LIMIT as "no upper bound", which is how we
    // implement the unlimited case when the caller passes None.
    let limit_val: i64 = match limit {
        Some(n) => n as i64,
        None => -1,
    };
    let mut stmt = conn.prepare(
        "SELECT s.title, el.id, el.step_id, el.attempt, el.started_at, el.duration_secs,
                el.prompt_text, el.diff, el.test_results, el.rolled_back, el.committed,
                el.commit_hash, el.harness_stdout, el.harness_stderr, el.cost_usd,
                el.input_tokens, el.output_tokens, el.session_id,
                el.termination_reason, el.test_status
         FROM execution_logs el
         JOIN steps s ON s.id = el.step_id
         WHERE s.plan_id = ?1
         ORDER BY el.started_at DESC
         LIMIT ?2",
    )?;

    let rows = stmt.query_map(params![plan_id, limit_val], |row| {
        let step_title: String = row.get(0)?;
        // Shift columns by 1 for the ExecutionLog fields.
        let termination_reason_str: Option<String> = row.get(18)?;
        let termination_reason = match termination_reason_str {
            Some(s) => Some(s.parse::<crate::plan::TerminationReason>().map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    18,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?),
            None => None,
        };
        let test_status_str: Option<String> = row.get(19)?;
        let test_status = match test_status_str {
            Some(s) => Some(s.parse::<crate::plan::TestStatus>().map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    19,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?),
            None => None,
        };
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
            termination_reason,
            test_status,
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
pub(crate) fn get_execution_log_by_id(conn: &Connection, id: i64) -> Result<ExecutionLog> {
    conn.query_row(
        "SELECT id, step_id, attempt, started_at, duration_secs, prompt_text, diff, test_results, rolled_back, committed, commit_hash, harness_stdout, harness_stderr, cost_usd, input_tokens, output_tokens, session_id, termination_reason, test_status
         FROM execution_logs WHERE id = ?1",
        params![id],
        ExecutionLog::from_row,
    )
    .with_context(|| format!("Execution log not found: {id}"))
}

/// Mark the live execution log row as interrupted by the user without
/// clobbering any observability fields the target runner may have already
/// written (diff, stdout/stderr, commit_hash, etc.).
///
/// Used by `ralph cancel`: the regular `update_execution_log` path takes every
/// observability column as an `Option` but unconditionally overwrites most of
/// them, so calling it with `None`s would wipe whatever the runner persisted
/// before it died. This narrow helper only touches `termination_reason` and
/// `test_status`, both via COALESCE semantics — if the runner already recorded
/// a terminal reason (e.g. the attempt succeeded before cancel raced it), we
/// leave it alone.
///
/// Returns `Ok(true)` if a matching row was updated (the COALESCE means the
/// update may have been a no-op at the column level if the runner already
/// recorded a terminal reason, but the row still matched). Returns `Ok(false)`
/// if no such row exists — for `ralph cancel`'s stale-run path that's benign
/// (the runner may have deleted its own execution_log during cleanup).
pub fn finalize_execution_log_as_interrupted_if_exists(
    conn: &Connection,
    log_id: i64,
) -> Result<bool> {
    let affected = conn.execute(
        "UPDATE execution_logs SET
            termination_reason = COALESCE(termination_reason, ?1),
            test_status = COALESCE(test_status, ?2)
         WHERE id = ?3",
        params![
            crate::plan::TerminationReason::UserInterrupted.as_str(),
            crate::plan::TestStatus::NotRun.as_str(),
            log_id,
        ],
    )?;
    Ok(affected > 0)
}

/// Delete a run_locks row without requiring that the calling process owns it.
///
/// Unlike [`crate::run_lock::acquire`]'s normal Drop-path release, this is used
/// by `ralph cancel` from a *sibling* process that never held the lock. The
/// query is scoped by `(project, pid, pid_start_token)` so a racing new
/// `ralph run` that already inserted its own row (different pid, or same pid
/// with a different start token) is untouched.
pub fn delete_run_lock_row_unscoped(
    conn: &Connection,
    project: &str,
    pid: i64,
    pid_start_token: Option<&str>,
) -> Result<usize> {
    let affected = conn.execute(
        "DELETE FROM run_locks
         WHERE project = ?1
           AND pid = ?2
           AND COALESCE(pid_start_token, '') = COALESCE(?3, '')",
        params![project, pid, pid_start_token],
    )?;
    Ok(affected)
}

// ---------------------------------------------------------------------------
// Plan dependency operations
// ---------------------------------------------------------------------------

/// Record that `plan_id` depends on `depends_on_plan_id`.
///
/// Bails with a user-friendly error if the two IDs are the same, or if adding
/// the edge would create a cycle in the dependency graph. Cycle detection runs
/// before the insert via [`would_create_cycle`], so callers never need to
/// invoke it themselves.
pub fn add_plan_dependency(
    conn: &Connection,
    plan_id: &str,
    depends_on_plan_id: &str,
) -> Result<()> {
    if plan_id == depends_on_plan_id {
        anyhow::bail!("A plan cannot depend on itself");
    }

    if would_create_cycle(conn, plan_id, depends_on_plan_id)? {
        anyhow::bail!("Adding dependency {plan_id} -> {depends_on_plan_id} would create a cycle");
    }

    conn.execute(
        "INSERT INTO plan_dependencies (plan_id, depends_on_plan_id) VALUES (?1, ?2)",
        params![plan_id, depends_on_plan_id],
    )
    .with_context(|| format!("Failed to add dependency {plan_id} -> {depends_on_plan_id}"))?;

    Ok(())
}

/// Remove a specific dependency edge. No-op if the row does not exist.
pub fn remove_plan_dependency(
    conn: &Connection,
    plan_id: &str,
    depends_on_plan_id: &str,
) -> Result<()> {
    conn.execute(
        "DELETE FROM plan_dependencies WHERE plan_id = ?1 AND depends_on_plan_id = ?2",
        params![plan_id, depends_on_plan_id],
    )
    .with_context(|| format!("Failed to remove dependency {plan_id} -> {depends_on_plan_id}"))?;
    Ok(())
}

/// List the plan IDs that `plan_id` directly depends on.
pub fn list_plan_dependencies(conn: &Connection, plan_id: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT depends_on_plan_id FROM plan_dependencies WHERE plan_id = ?1 ORDER BY depends_on_plan_id ASC",
    )?;
    let rows = stmt.query_map(params![plan_id], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// List the plan IDs that directly depend on `plan_id` (reverse edges).
pub fn list_dependent_plans(conn: &Connection, plan_id: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT plan_id FROM plan_dependencies WHERE depends_on_plan_id = ?1 ORDER BY plan_id ASC",
    )?;
    let rows = stmt.query_map(params![plan_id], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// Check whether adding `plan_id -> new_dep_id` would create a cycle.
///
/// Walks the transitive dependencies of `new_dep_id`; if `plan_id` appears in
/// that set, the edge would close a cycle. A self-edge (`plan_id == new_dep_id`)
/// is also reported as a cycle.
pub fn would_create_cycle(conn: &Connection, plan_id: &str, new_dep_id: &str) -> Result<bool> {
    if plan_id == new_dep_id {
        return Ok(true);
    }

    let mut stack: Vec<String> = vec![new_dep_id.to_string()];
    let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();

    while let Some(current) = stack.pop() {
        if !visited.insert(current.clone()) {
            continue;
        }
        if current == plan_id {
            return Ok(true);
        }
        let deps = list_plan_dependencies(conn, &current)?;
        for d in deps {
            if !visited.contains(&d) {
                stack.push(d);
            }
        }
    }

    Ok(false)
}

/// Topologically sort the given plan IDs so that dependencies come before
/// their dependents.
///
/// Only edges where *both* endpoints appear in `plan_ids` are considered;
/// dependencies on plans outside the input slice are treated as already
/// satisfied. Uses Kahn's algorithm. If a cycle is detected the function
/// returns an error listing the plan IDs that could not be ordered.
pub fn topo_sort_plans(conn: &Connection, plan_ids: &[String]) -> Result<Vec<String>> {
    use std::collections::{HashMap, HashSet, VecDeque};

    let id_set: HashSet<&str> = plan_ids.iter().map(|s| s.as_str()).collect();

    // Build adjacency: for each plan, which plans within the input set does it depend on?
    // edges_in_degree[p] = number of dependencies of p that are in the input set.
    // reverse[dep] = list of plans that depend on dep (both within the input set).
    let mut in_degree: HashMap<String, usize> = HashMap::new();
    let mut reverse: HashMap<String, Vec<String>> = HashMap::new();

    for p in plan_ids {
        in_degree.insert(p.clone(), 0);
        reverse.entry(p.clone()).or_default();
    }

    for p in plan_ids {
        let deps = list_plan_dependencies(conn, p)?;
        for d in deps {
            if id_set.contains(d.as_str()) {
                *in_degree.entry(p.clone()).or_insert(0) += 1;
                reverse.entry(d).or_default().push(p.clone());
            }
        }
    }

    // Kahn's algorithm: seed queue with zero-in-degree nodes, preserving input
    // order for a stable result.
    let mut queue: VecDeque<String> = VecDeque::new();
    for p in plan_ids {
        if in_degree.get(p).copied().unwrap_or(0) == 0 {
            queue.push_back(p.clone());
        }
    }

    let mut sorted: Vec<String> = Vec::with_capacity(plan_ids.len());
    while let Some(node) = queue.pop_front() {
        sorted.push(node.clone());
        if let Some(dependents) = reverse.get(&node).cloned() {
            for dep in dependents {
                if let Some(deg) = in_degree.get_mut(&dep) {
                    *deg -= 1;
                    if *deg == 0 {
                        queue.push_back(dep);
                    }
                }
            }
        }
    }

    if sorted.len() != plan_ids.len() {
        let remaining: Vec<String> = plan_ids
            .iter()
            .filter(|p| !sorted.contains(p))
            .cloned()
            .collect();
        anyhow::bail!(
            "dependency cycle detected involving plans: {}",
            remaining.join(", ")
        );
    }

    Ok(sorted)
}

// ---------------------------------------------------------------------------
// Step hook operations
// ---------------------------------------------------------------------------

/// A hook association read from the db. `step_id == None` means plan-wide.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepHookRow {
    pub id: i64,
    pub plan_id: String,
    pub step_id: Option<String>,
    pub lifecycle: String,
    pub hook_name: String,
}

/// Returns true if `err` is a SQLite UNIQUE constraint violation.
fn is_unique_violation(err: &rusqlite::Error) -> bool {
    matches!(
        err,
        rusqlite::Error::SqliteFailure(e, _)
            if e.code == rusqlite::ErrorCode::ConstraintViolation
                && e.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE
    )
}

/// Attach a hook to a specific step at a lifecycle event.
pub fn attach_hook_to_step(
    conn: &Connection,
    plan_id: &str,
    step_id: &str,
    lifecycle: &str,
    hook_name: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO step_hooks (plan_id, step_id, lifecycle, hook_name) VALUES (?1, ?2, ?3, ?4)",
        params![plan_id, step_id, lifecycle, hook_name],
    )
    .map_err(|e| {
        if is_unique_violation(&e) {
            anyhow::anyhow!(
                "hook '{hook_name}' is already attached to step {step_id} at {lifecycle}"
            )
        } else {
            anyhow::Error::new(e).context(format!(
                "Failed to attach hook '{hook_name}' to step {step_id} at {lifecycle}"
            ))
        }
    })?;
    Ok(())
}

/// Attach a plan-wide hook (applies to every step in the plan).
pub fn attach_hook_to_plan(
    conn: &Connection,
    plan_id: &str,
    lifecycle: &str,
    hook_name: &str,
) -> Result<()> {
    conn.execute(
        "INSERT INTO step_hooks (plan_id, step_id, lifecycle, hook_name) VALUES (?1, NULL, ?2, ?3)",
        params![plan_id, lifecycle, hook_name],
    )
    .map_err(|e| {
        if is_unique_violation(&e) {
            anyhow::anyhow!(
                "hook '{hook_name}' is already attached to plan {plan_id} at {lifecycle}"
            )
        } else {
            anyhow::Error::new(e).context(format!(
                "Failed to attach plan-wide hook '{hook_name}' to plan {plan_id} at {lifecycle}"
            ))
        }
    })?;
    Ok(())
}

/// Remove a specific (plan, step, lifecycle, hook_name) row. If `step_id` is
/// `None`, removes the plan-wide association.
pub fn detach_hook(
    conn: &Connection,
    plan_id: &str,
    step_id: Option<&str>,
    lifecycle: &str,
    hook_name: &str,
) -> Result<usize> {
    let affected = match step_id {
        Some(sid) => conn.execute(
            "DELETE FROM step_hooks WHERE plan_id = ?1 AND step_id = ?2 AND lifecycle = ?3 AND hook_name = ?4",
            params![plan_id, sid, lifecycle, hook_name],
        )?,
        None => conn.execute(
            "DELETE FROM step_hooks WHERE plan_id = ?1 AND step_id IS NULL AND lifecycle = ?2 AND hook_name = ?3",
            params![plan_id, lifecycle, hook_name],
        )?,
    };
    Ok(affected)
}

/// List every hook applicable to a step at a given lifecycle: plan-wide hooks
/// first, then per-step hooks. Ordered by id so insertion order is preserved.
pub fn list_hooks_for_step(
    conn: &Connection,
    plan_id: &str,
    step_id: &str,
    lifecycle: &str,
) -> Result<Vec<StepHookRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, plan_id, step_id, lifecycle, hook_name
         FROM step_hooks
         WHERE plan_id = ?1 AND lifecycle = ?2 AND (step_id IS NULL OR step_id = ?3)
         ORDER BY (step_id IS NOT NULL), id",
    )?;
    let rows = stmt.query_map(params![plan_id, lifecycle, step_id], |row| {
        Ok(StepHookRow {
            id: row.get(0)?,
            plan_id: row.get(1)?,
            step_id: row.get(2)?,
            lifecycle: row.get(3)?,
            hook_name: row.get(4)?,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

/// List every hook attached to a plan (either plan-wide or to any of its steps).
pub fn list_all_hooks_for_plan(conn: &Connection, plan_id: &str) -> Result<Vec<StepHookRow>> {
    let mut stmt = conn.prepare(
        "SELECT id, plan_id, step_id, lifecycle, hook_name
         FROM step_hooks
         WHERE plan_id = ?1
         ORDER BY (step_id IS NOT NULL), id",
    )?;
    let rows = stmt.query_map(params![plan_id], |row| {
        Ok(StepHookRow {
            id: row.get(0)?,
            plan_id: row.get(1)?,
            step_id: row.get(2)?,
            lifecycle: row.get(3)?,
            hook_name: row.get(4)?,
        })
    })?;
    let mut out = Vec::new();
    for r in rows {
        out.push(r?);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Live-run observability (run_locks V11 columns)
// ---------------------------------------------------------------------------

/// Read the live-run snapshot for `project`, including every observability
/// column added in V11. Returns `Ok(None)` when no lock row exists.
///
/// Production callers are `ralph cancel` and `ralph status`. Tests exercise it
/// to verify phase writes, so the `#[allow(dead_code)]` marks the binary
/// surface area, not the function itself.
#[allow(dead_code)]
pub fn get_live_run(conn: &Connection, project: &str) -> Result<Option<LiveRun>> {
    let query = format!("SELECT {LIVE_RUN_COLUMNS} FROM run_locks WHERE project = ?1");
    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query_map(params![project], LiveRun::from_row)?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Bind the live run-lock row to the plan currently being executed.
///
/// `ralph run --all` acquires a project-wide lock before it knows which plan is
/// active. As the orchestrator advances from one plan to the next, bind the row
/// to that plan and clear step/phase fields from the previous plan. The next
/// executor phase write will populate the concrete step and attempt.
pub fn bind_live_run_to_plan(
    conn: &Connection,
    project: &str,
    plan_id: &str,
    plan_slug: &str,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE run_locks SET
            plan_id = ?1,
            plan_slug = ?2,
            step_id = NULL,
            step_num = NULL,
            attempt = NULL,
            max_attempts = NULL,
            phase = ?3,
            phase_started_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
            current_command = NULL,
            execution_log_id = NULL,
            child_pid = NULL,
            child_start_token = NULL,
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE project = ?4",
        params![plan_id, plan_slug, Phase::Idle.as_str(), project],
    )?;

    if affected == 0 {
        anyhow::bail!("No run_locks row for project: {project}");
    }
    Ok(())
}

/// How a phase write should treat the `child_pid` / `child_start_token`
/// columns on the `run_locks` row.
///
/// These two columns move together — we only ever write a matching (pid,
/// token) pair or clear both — so they share a single enum rather than two
/// independent args. Post-harness phases must explicitly `Clear` instead of
/// using COALESCE, otherwise a dead harness pid lingers on the row through
/// the Tests / PostTestHook / Commit / Rollback / PostStepHook phases,
/// confusing observers that read `ralph status` mid-cleanup.
#[derive(Debug, Clone)]
pub enum ChildUpdate<'a> {
    /// Preserve whatever (pid, token) is already on the row (COALESCE).
    Keep,
    /// Overwrite with these concrete values. `start_token` may be `None` on
    /// platforms that can't derive one (still writes NULL to that column).
    Set {
        pid: i64,
        start_token: Option<&'a str>,
    },
    /// Overwrite both columns with NULL. Use after the harness phase ends
    /// (Tests onward) so the row no longer advertises a dead pid.
    Clear,
}

/// Record a phase transition onto the `run_locks` row for `project`.
///
/// Semantics:
///
/// - `phase`, `phase_started_at`, and `updated_at` are **always** written.
///   `phase_started_at` and `updated_at` are set to `strftime('now')`.
/// - `step_id`, `step_num`, `attempt`, `max_attempts`, `execution_log_id`:
///   **COALESCE** semantics. Passing `None` leaves the existing column value
///   untouched. This lets callers set these fields once (e.g. at the start
///   of a step) without having to re-pass them on every phase write inside
///   the same step.
/// - `current_command`: **always overwrites**. Phases that don't have a
///   current command (e.g. `PostTestHook`) should pass `None` and the column
///   will be cleared back to NULL. Using COALESCE here would leave a stale
///   command (like `"cargo test"`) sitting on a phase that isn't running any
///   command.
/// - `child`: explicit [`ChildUpdate`] to disambiguate "preserve", "set",
///   and "clear" for `child_pid` / `child_start_token`. COALESCE here would
///   leave a dead harness pid visible through post-harness phases.
///
/// Errors when no row exists for `project` — the run_locks row is created by
/// [`crate::run_lock::acquire`] before the executor starts, so a missing row
/// indicates a programming error (likely a test forgot to seed the row).
#[allow(clippy::too_many_arguments)]
pub fn update_live_phase(
    conn: &Connection,
    project: &str,
    phase: Phase,
    step_id: Option<&str>,
    step_num: Option<i32>,
    attempt: Option<i32>,
    max_attempts: Option<i32>,
    execution_log_id: Option<i64>,
    current_command: Option<&str>,
    child: ChildUpdate<'_>,
) -> Result<()> {
    // Build the child-column fragment + bound params depending on the mode.
    // Keep uses COALESCE so Nones don't clobber; Set writes the values
    // directly; Clear overwrites both to NULL.
    let (child_sql, child_pid_param, child_token_param): (&str, Option<i64>, Option<&str>) =
        match child {
            ChildUpdate::Keep => (
                "child_pid = COALESCE(?8, child_pid),
                 child_start_token = COALESCE(?9, child_start_token),",
                None,
                None,
            ),
            ChildUpdate::Set { pid, start_token } => (
                "child_pid = ?8,
                 child_start_token = ?9,",
                Some(pid),
                start_token,
            ),
            ChildUpdate::Clear => (
                "child_pid = NULL,
                 child_start_token = NULL,",
                None,
                None,
            ),
        };

    let sql = format!(
        "UPDATE run_locks SET
            phase = ?1,
            phase_started_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'),
            step_id = COALESCE(?2, step_id),
            step_num = COALESCE(?3, step_num),
            attempt = COALESCE(?4, attempt),
            max_attempts = COALESCE(?5, max_attempts),
            execution_log_id = COALESCE(?6, execution_log_id),
            current_command = ?7,
            {child_sql}
            updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE project = ?10",
    );
    let affected = conn.execute(
        &sql,
        params![
            phase.as_str(),
            step_id,
            step_num,
            attempt,
            max_attempts,
            execution_log_id,
            current_command,
            child_pid_param,
            child_token_param,
            project,
        ],
    )?;

    if affected == 0 {
        anyhow::bail!("No run_locks row for project: {project}");
    }
    Ok(())
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
    fn test_find_active_plan_filters_by_status() {
        let conn = setup();

        // Seed one plan per status, plus a same-status plan in another project.
        let planning = create_plan(&conn, "p1", "/proj", "b1", "d", None, None, &[]).unwrap();
        let ready = create_plan(&conn, "p2", "/proj", "b2", "d", None, None, &[]).unwrap();
        let in_progress = create_plan(&conn, "p3", "/proj", "b3", "d", None, None, &[]).unwrap();
        let failed = create_plan(&conn, "p4", "/proj", "b4", "d", None, None, &[]).unwrap();
        let complete = create_plan(&conn, "p5", "/proj", "b5", "d", None, None, &[]).unwrap();
        let archived = create_plan(&conn, "p6", "/proj", "b6", "d", None, None, &[]).unwrap();
        let aborted = create_plan(&conn, "p7", "/proj", "b7", "d", None, None, &[]).unwrap();
        let other = create_plan(&conn, "p8", "/other", "b8", "d", None, None, &[]).unwrap();

        update_plan_status(&conn, &ready.id, PlanStatus::Ready).unwrap();
        update_plan_status(&conn, &in_progress.id, PlanStatus::InProgress).unwrap();
        update_plan_status(&conn, &failed.id, PlanStatus::Failed).unwrap();
        update_plan_status(&conn, &complete.id, PlanStatus::Complete).unwrap();
        update_plan_status(&conn, &archived.id, PlanStatus::Archived).unwrap();
        update_plan_status(&conn, &aborted.id, PlanStatus::Aborted).unwrap();
        update_plan_status(&conn, &other.id, PlanStatus::InProgress).unwrap();

        // Only in_progress / ready / failed in "/proj" count as active.
        let active_ids: std::collections::HashSet<String> =
            [ready.id.clone(), in_progress.id.clone(), failed.id.clone()]
                .into_iter()
                .collect();
        let found = find_active_plan(&conn, "/proj", false).unwrap().unwrap();
        assert!(active_ids.contains(&found.id));
        assert_eq!(found.project, "/proj");

        // With include_complete, the complete plan becomes eligible too.
        let active_with_complete: std::collections::HashSet<String> = [
            ready.id.clone(),
            in_progress.id.clone(),
            failed.id.clone(),
            complete.id.clone(),
        ]
        .into_iter()
        .collect();
        let found_inc = find_active_plan(&conn, "/proj", true).unwrap().unwrap();
        assert!(active_with_complete.contains(&found_inc.id));

        // Archive every active row; nothing should match without include_complete.
        update_plan_status(&conn, &ready.id, PlanStatus::Archived).unwrap();
        update_plan_status(&conn, &in_progress.id, PlanStatus::Archived).unwrap();
        update_plan_status(&conn, &failed.id, PlanStatus::Archived).unwrap();
        assert!(find_active_plan(&conn, "/proj", false).unwrap().is_none());
        // include_complete still resolves to the lone complete plan.
        let found_complete = find_active_plan(&conn, "/proj", true).unwrap().unwrap();
        assert_eq!(found_complete.id, complete.id);

        // Planning / aborted / archived are never treated as active.
        let _ = (planning, aborted);
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
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
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

        let (s1, _) = create_step(
            &conn,
            &plan.id,
            "First",
            "d1",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let (s2, _) = create_step(
            &conn,
            &plan.id,
            "Second",
            "d2",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let (s3, _) = create_step(
            &conn,
            &plan.id,
            "Third",
            "d3",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Sort keys should be monotonically increasing
        assert!(
            s1.sort_key < s2.sort_key,
            "{} < {}",
            s1.sort_key,
            s2.sort_key
        );
        assert!(
            s2.sort_key < s3.sort_key,
            "{} < {}",
            s2.sort_key,
            s3.sort_key
        );

        // First key should be initial_key
        assert_eq!(s1.sort_key, frac_index::initial_key());
    }

    #[test]
    fn test_list_steps_ordered_by_sort_key() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        create_step(
            &conn,
            &plan.id,
            "First",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        create_step(
            &conn,
            &plan.id,
            "Second",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        create_step(
            &conn,
            &plan.id,
            "Third",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

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
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &criteria,
            Some(3),
            None,
            None,
            None,
        )
        .unwrap();

        assert_eq!(step.acceptance_criteria, criteria);
        assert_eq!(step.max_retries, Some(3));
        assert_eq!(step.status, StepStatus::Pending);
        assert_eq!(step.attempts, 0);
    }

    #[test]
    fn test_create_step_stores_tags() {
        let conn = setup();
        let plan = create_plan(&conn, "tagged", "/p", "b", "d", None, None, &[]).unwrap();

        let tags = vec!["FIX".to_string(), "REGRESSION".to_string()];
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Fix bug",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            Some(&tags),
        )
        .unwrap();

        // Round-trip: the step returned from create_step should carry the tags
        // and a fresh SELECT should return the same list in the same order.
        assert_eq!(step.tags, tags);

        let fetched = get_step(&conn, &step.id).unwrap();
        assert_eq!(fetched.tags, tags);
    }

    #[test]
    fn test_tags_default_to_empty_for_legacy_rows() {
        // Insert a step via raw SQL so the JSON `tags` column picks up its
        // NOT NULL DEFAULT '[]' (mirrors the state of pre-V13 rows that V13
        // backfilled). Reading through Step::from_row must yield an empty
        // Vec without panicking.
        let conn = setup();
        let plan = create_plan(&conn, "legacy", "/p", "b", "d", None, None, &[]).unwrap();

        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["legacy-s1", &plan.id, "a0", "Legacy step", "desc"],
        )
        .unwrap();

        let step = get_step(&conn, "legacy-s1").unwrap();
        assert!(step.tags.is_empty());

        // An explicit empty string in the tags column (shouldn't happen in
        // practice because DEFAULT is '[]', but defensively handled in
        // Step::from_row) also deserializes as an empty vec.
        conn.execute(
            "UPDATE steps SET tags = '' WHERE id = ?1",
            params!["legacy-s1"],
        )
        .unwrap();
        let step_empty = get_step(&conn, "legacy-s1").unwrap();
        assert!(step_empty.tags.is_empty());
    }

    #[test]
    fn test_update_step_fields_ext_replaces_tags() {
        let conn = setup();
        let plan = create_plan(&conn, "p", "/p", "b", "d", None, None, &[]).unwrap();

        let initial = vec!["FIX".to_string()];
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "T",
            "",
            None,
            None,
            &[],
            None,
            None,
            None,
            Some(&initial),
        )
        .unwrap();

        // Replace with a fresh set.
        let replacement = vec!["REVIEW".to_string(), "DOCS".to_string()];
        update_step_fields_ext(
            &conn,
            &step.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&replacement),
        )
        .unwrap();
        let updated = get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.tags, replacement);

        // An empty slice clears the list.
        update_step_fields_ext(
            &conn,
            &step.id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&[]),
        )
        .unwrap();
        let cleared = get_step(&conn, &step.id).unwrap();
        assert!(cleared.tags.is_empty());
    }

    #[test]
    fn test_update_step_status() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        update_step_status(&conn, &step.id, StepStatus::Complete).unwrap();

        let updated = get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.status, StepStatus::Complete);
    }

    #[test]
    fn test_update_step_fields_ext_atomic_single_update() {
        // A single UPDATE carries one `updated_at` for every changed column,
        // so setting multiple fields in one call leaves no window for a
        // partial write with inconsistent timestamps.
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let baseline = get_step(&conn, &step.id).unwrap();
        // Sleep long enough that strftime('now') advances past the baseline.
        std::thread::sleep(std::time::Duration::from_millis(2));

        update_step_fields_ext(
            &conn,
            &step.id,
            Some("New Title"),
            Some("New Desc"),
            Some(Some("new-agent")),
            Some(Some("new-harness")),
            Some(&["criterion".to_string()]),
            Some(Some(5)),
            Some(Some("new-model")),
            Some(ChangePolicy::Optional),
            None,
        )
        .unwrap();

        let updated = get_step(&conn, &step.id).unwrap();
        assert_eq!(updated.title, "New Title");
        assert_eq!(updated.description, "New Desc");
        assert_eq!(updated.agent.as_deref(), Some("new-agent"));
        assert_eq!(updated.harness.as_deref(), Some("new-harness"));
        assert_eq!(updated.acceptance_criteria, vec!["criterion".to_string()]);
        assert_eq!(updated.max_retries, Some(5));
        assert_eq!(updated.model.as_deref(), Some("new-model"));
        assert_eq!(updated.change_policy, ChangePolicy::Optional);
        assert!(updated.updated_at > baseline.updated_at);
    }

    #[test]
    fn test_update_step_fields_ext_missing_step_rolls_back() {
        // When the step doesn't exist the transaction rolls back, leaving
        // other rows untouched.
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (other, _) = create_step(
            &conn,
            &plan.id,
            "Other",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let other_before = get_step(&conn, &other.id).unwrap();

        let err = update_step_fields_ext(
            &conn,
            "nonexistent-id",
            Some("New Title"),
            Some("New Desc"),
            Some(Some("agent")),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("Step not found"));

        let other_after = get_step(&conn, &other.id).unwrap();
        assert_eq!(other_before.title, other_after.title);
        assert_eq!(other_before.updated_at, other_after.updated_at);
    }

    #[test]
    fn test_update_step_fields_ext_clears_nullable_fields() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            Some("agent"),
            Some("harness"),
            &[],
            Some(3),
            Some("model"),
            None,
            None,
        )
        .unwrap();

        update_step_fields_ext(
            &conn,
            &step.id,
            None,
            None,
            Some(None),
            Some(None),
            None,
            Some(None),
            Some(None),
            None,
            None,
        )
        .unwrap();

        let updated = get_step(&conn, &step.id).unwrap();
        assert!(updated.agent.is_none());
        assert!(updated.harness.is_none());
        assert!(updated.max_retries.is_none());
        assert!(updated.model.is_none());
    }

    #[test]
    fn test_update_step_fields_ext_noop_when_all_none() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let before = get_step(&conn, &step.id).unwrap();

        update_step_fields_ext(
            &conn, &step.id, None, None, None, None, None, None, None, None, None,
        )
        .unwrap();

        let after = get_step(&conn, &step.id).unwrap();
        assert_eq!(before.updated_at, after.updated_at);
    }

    #[test]
    fn test_delete_step() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        create_execution_log(&conn, &step.id, 1, None, None).unwrap();

        delete_step(&conn, &step.id).unwrap();

        assert!(list_steps(&conn, &plan.id).unwrap().is_empty());
        // Logs should cascade delete
        assert!(get_latest_log_for_step(&conn, &step.id).unwrap().is_none());
    }

    #[test]
    fn test_reset_step_clears_execution_logs() {
        // Regression: `ralph resume` on an in-progress step called reset_step,
        // which zeroed `attempts` but left old execution_logs in place. The
        // next run then tried to create attempt=1 again and tripped the
        // UNIQUE(step_id, attempt) constraint.
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        update_step_status(&conn, &step.id, StepStatus::InProgress).unwrap();
        create_execution_log(&conn, &step.id, 1, Some("first try"), None).unwrap();

        reset_step(&conn, &step.id).unwrap();

        let reset = get_step(&conn, &step.id).unwrap();
        assert_eq!(reset.status, StepStatus::Pending);
        assert_eq!(reset.attempts, 0);
        assert!(get_latest_log_for_step(&conn, &step.id).unwrap().is_none());

        // And we can now create a fresh attempt=1 log without colliding.
        create_execution_log(&conn, &step.id, 1, Some("retry"), None).unwrap();
    }

    #[test]
    fn test_get_next_pending_step() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        let (s1, _) = create_step(
            &conn,
            &plan.id,
            "First",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let (s2, _) = create_step(
            &conn,
            &plan.id,
            "Second",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

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
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let log =
            create_execution_log(&conn, &step.id, 1, Some("do the thing"), Some("sess-1")).unwrap();

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
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

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
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
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
            Some("session-abc"),
            None,
            None,
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
        assert_eq!(updated.session_id.as_deref(), Some("session-abc"));
        assert!(updated.termination_reason.is_none());
        assert!(updated.test_status.is_none());
    }

    #[test]
    fn test_update_execution_log_persists_termination_and_test_status() {
        use crate::plan::{TerminationReason, TestStatus};
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let log = create_execution_log(&conn, &step.id, 1, None, None).unwrap();

        update_execution_log(
            &conn,
            log.id,
            Some(1.0),
            None,
            &[],
            false,
            true,
            Some("abc"),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(TerminationReason::Success),
            Some(TestStatus::Passed),
        )
        .unwrap();

        let updated = get_latest_log_for_step(&conn, &step.id).unwrap().unwrap();
        assert_eq!(updated.termination_reason, Some(TerminationReason::Success));
        assert_eq!(updated.test_status, Some(TestStatus::Passed));

        // Round-trip via list_execution_logs_for_step too.
        let logs = list_execution_logs_for_step(&conn, &step.id).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].termination_reason, Some(TerminationReason::Success));
        assert_eq!(logs[0].test_status, Some(TestStatus::Passed));
    }

    #[test]
    fn test_update_execution_log_coalesces_termination_and_test_status() {
        use crate::plan::{TerminationReason, TestStatus};
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let log = create_execution_log(&conn, &step.id, 1, None, None).unwrap();

        // First write: set both fields.
        update_execution_log(
            &conn,
            log.id,
            Some(1.0),
            None,
            &[],
            true,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(TerminationReason::TestFailed),
            Some(TestStatus::Failed),
        )
        .unwrap();

        // Second write: pass None for both — should preserve the first values.
        update_execution_log(
            &conn,
            log.id,
            Some(2.0),
            None,
            &[],
            true,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let updated = get_latest_log_for_step(&conn, &step.id).unwrap().unwrap();
        assert_eq!(
            updated.termination_reason,
            Some(TerminationReason::TestFailed),
            "None should preserve existing termination_reason via COALESCE"
        );
        assert_eq!(
            updated.test_status,
            Some(TestStatus::Failed),
            "None should preserve existing test_status via COALESCE"
        );
    }

    #[test]
    #[cfg(debug_assertions)]
    #[should_panic(expected = "execution log cannot be both rolled_back and committed")]
    fn test_update_execution_log_rolled_back_and_committed_panics() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let log = create_execution_log(&conn, &step.id, 1, None, None).unwrap();

        let _ = update_execution_log(
            &conn,
            log.id,
            None,
            None,
            &[],
            true,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        );
    }

    #[test]
    fn test_update_execution_log_preserves_session_id_when_none() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let log = create_execution_log(&conn, &step.id, 1, None, Some("initial-session")).unwrap();

        update_execution_log(
            &conn,
            log.id,
            Some(10.0),
            None,
            &[],
            false,
            true,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let updated = get_latest_log_for_step(&conn, &step.id).unwrap().unwrap();
        assert_eq!(
            updated.session_id.as_deref(),
            Some("initial-session"),
            "session_id set at creation should be preserved when update passes None"
        );
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
        let (step, _) = create_step(
            &conn, &plan.id, "Step", "d", None, None, &criteria, None, None, None, None,
        )
        .unwrap();

        let fetched = get_step(&conn, &step.id).unwrap();
        assert_eq!(fetched.acceptance_criteria, criteria);
    }

    #[test]
    fn test_json_roundtrip_empty_arrays() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        assert!(plan.deterministic_tests.is_empty());

        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert!(step.acceptance_criteria.is_empty());
    }

    // -- Plan dependency tests --

    /// Create `n` plans named p1..pn in the same project and return their IDs.
    fn make_plans(conn: &Connection, n: usize) -> Vec<String> {
        (1..=n)
            .map(|i| {
                let slug = format!("p{i}");
                create_plan(conn, &slug, "/proj", "branch", "desc", None, None, &[])
                    .expect("create_plan")
                    .id
            })
            .collect()
    }

    #[test]
    fn test_add_plan_dependency_happy_path() {
        let conn = setup();
        let ids = make_plans(&conn, 2);

        add_plan_dependency(&conn, &ids[0], &ids[1]).expect("add dep");

        let deps = list_plan_dependencies(&conn, &ids[0]).unwrap();
        assert_eq!(deps, vec![ids[1].clone()]);
    }

    #[test]
    fn test_add_plan_dependency_rejects_self_reference() {
        let conn = setup();
        let ids = make_plans(&conn, 1);

        let err = add_plan_dependency(&conn, &ids[0], &ids[0]).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("cannot depend on itself"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_add_plan_dependency_rejects_cycle() {
        let conn = setup();
        let ids = make_plans(&conn, 2);

        // A -> B
        add_plan_dependency(&conn, &ids[0], &ids[1]).expect("add A->B");

        // B -> A would create a 2-node cycle
        let err = add_plan_dependency(&conn, &ids[1], &ids[0]).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("cycle"), "unexpected error: {msg}");
    }

    #[test]
    fn test_remove_plan_dependency() {
        let conn = setup();
        let ids = make_plans(&conn, 2);

        add_plan_dependency(&conn, &ids[0], &ids[1]).unwrap();
        assert_eq!(list_plan_dependencies(&conn, &ids[0]).unwrap().len(), 1);

        remove_plan_dependency(&conn, &ids[0], &ids[1]).unwrap();
        assert!(list_plan_dependencies(&conn, &ids[0]).unwrap().is_empty());

        // Removing a non-existent edge is a no-op.
        remove_plan_dependency(&conn, &ids[0], &ids[1]).unwrap();
    }

    #[test]
    fn test_list_plan_dependencies_and_dependents() {
        let conn = setup();
        let ids = make_plans(&conn, 3);

        // p1 depends on p2 and p3.
        add_plan_dependency(&conn, &ids[0], &ids[1]).unwrap();
        add_plan_dependency(&conn, &ids[0], &ids[2]).unwrap();

        let mut deps = list_plan_dependencies(&conn, &ids[0]).unwrap();
        deps.sort();
        let mut expected = vec![ids[1].clone(), ids[2].clone()];
        expected.sort();
        assert_eq!(deps, expected);

        // p2 and p3 should both see p1 as a dependent.
        let dependents_p2 = list_dependent_plans(&conn, &ids[1]).unwrap();
        assert_eq!(dependents_p2, vec![ids[0].clone()]);

        let dependents_p3 = list_dependent_plans(&conn, &ids[2]).unwrap();
        assert_eq!(dependents_p3, vec![ids[0].clone()]);

        // p1 has no dependents.
        assert!(list_dependent_plans(&conn, &ids[0]).unwrap().is_empty());
    }

    #[test]
    fn test_would_create_cycle_direct() {
        let conn = setup();
        let ids = make_plans(&conn, 2);

        // Self-edge is always a cycle.
        assert!(would_create_cycle(&conn, &ids[0], &ids[0]).unwrap());

        // A -> B. Adding B -> A closes a direct cycle.
        add_plan_dependency(&conn, &ids[0], &ids[1]).unwrap();
        assert!(would_create_cycle(&conn, &ids[1], &ids[0]).unwrap());
    }

    #[test]
    fn test_would_create_cycle_transitive() {
        let conn = setup();
        let ids = make_plans(&conn, 3);

        // A -> B -> C. Adding C -> A would create a 3-node cycle.
        add_plan_dependency(&conn, &ids[0], &ids[1]).unwrap();
        add_plan_dependency(&conn, &ids[1], &ids[2]).unwrap();

        assert!(would_create_cycle(&conn, &ids[2], &ids[0]).unwrap());
    }

    #[test]
    fn test_would_create_cycle_no_cycle() {
        let conn = setup();
        let ids = make_plans(&conn, 3);

        // A -> B. Adding A -> C does not create a cycle.
        add_plan_dependency(&conn, &ids[0], &ids[1]).unwrap();

        assert!(!would_create_cycle(&conn, &ids[0], &ids[2]).unwrap());
    }

    #[test]
    fn test_topo_sort_linear_chain() {
        let conn = setup();
        let ids = make_plans(&conn, 3);

        // p1 -> p2 -> p3 (p1 depends on p2, p2 depends on p3)
        // Expected order: p3, p2, p1.
        add_plan_dependency(&conn, &ids[0], &ids[1]).unwrap();
        add_plan_dependency(&conn, &ids[1], &ids[2]).unwrap();

        let sorted = topo_sort_plans(&conn, &ids).unwrap();
        assert_eq!(sorted, vec![ids[2].clone(), ids[1].clone(), ids[0].clone()]);
    }

    #[test]
    fn test_topo_sort_diamond() {
        let conn = setup();
        let ids = make_plans(&conn, 4);
        // p1=A, p2=B, p3=C, p4=D
        // A -> B, A -> C, B -> D, C -> D
        // (A depends on B and C; B and C both depend on D.)
        // Expected order has D before B and C, and B and C before A.
        add_plan_dependency(&conn, &ids[0], &ids[1]).unwrap();
        add_plan_dependency(&conn, &ids[0], &ids[2]).unwrap();
        add_plan_dependency(&conn, &ids[1], &ids[3]).unwrap();
        add_plan_dependency(&conn, &ids[2], &ids[3]).unwrap();

        let sorted = topo_sort_plans(&conn, &ids).unwrap();
        assert_eq!(sorted.len(), 4);

        let pos = |id: &str| sorted.iter().position(|p| p == id).unwrap();
        assert!(pos(&ids[3]) < pos(&ids[1]));
        assert!(pos(&ids[3]) < pos(&ids[2]));
        assert!(pos(&ids[1]) < pos(&ids[0]));
        assert!(pos(&ids[2]) < pos(&ids[0]));
    }

    #[test]
    fn test_topo_sort_independent_plans() {
        let conn = setup();
        let ids = make_plans(&conn, 3);

        // No dependencies — topo sort should preserve input order.
        let sorted = topo_sort_plans(&conn, &ids).unwrap();
        assert_eq!(sorted, ids);
    }

    #[test]
    fn test_topo_sort_cycle_detection_error() {
        let conn = setup();
        let ids = make_plans(&conn, 3);

        // Build A -> B -> C via add_plan_dependency (which rejects cycles),
        // then bypass the cycle check and insert C -> A directly so we can
        // test topo_sort's own detection.
        add_plan_dependency(&conn, &ids[0], &ids[1]).unwrap();
        add_plan_dependency(&conn, &ids[1], &ids[2]).unwrap();
        conn.execute(
            "INSERT INTO plan_dependencies (plan_id, depends_on_plan_id) VALUES (?1, ?2)",
            params![&ids[2], &ids[0]],
        )
        .unwrap();

        let err = topo_sort_plans(&conn, &ids).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("cycle"), "unexpected error: {msg}");
        // All three plans should be named in the remaining set.
        for id in &ids {
            assert!(msg.contains(id), "missing plan id in error: {msg}");
        }
    }

    // -- step_hooks uniqueness tests --

    #[test]
    fn test_attach_hook_to_step_rejects_duplicate() {
        let conn = setup();
        let plan = create_plan(&conn, "p", "/proj", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "t",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        attach_hook_to_step(&conn, &plan.id, &step.id, "pre-step", "h1").unwrap();

        let err = attach_hook_to_step(&conn, &plan.id, &step.id, "pre-step", "h1").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("already attached"), "unexpected error: {msg}");

        // Only one row exists.
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM step_hooks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_attach_hook_to_plan_rejects_duplicate() {
        let conn = setup();
        let plan = create_plan(&conn, "p", "/proj", "b", "d", None, None, &[]).unwrap();

        attach_hook_to_plan(&conn, &plan.id, "post-step", "h1").unwrap();

        let err = attach_hook_to_plan(&conn, &plan.id, "post-step", "h1").unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("already attached"), "unexpected error: {msg}");

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM step_hooks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn test_attach_hook_allows_distinct_combinations() {
        let conn = setup();
        let plan = create_plan(&conn, "p", "/proj", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = create_step(
            &conn,
            &plan.id,
            "t1",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let (s2, _) = create_step(
            &conn,
            &plan.id,
            "t2",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();

        // Same hook on different steps: OK.
        attach_hook_to_step(&conn, &plan.id, &s1.id, "pre-step", "h1").unwrap();
        attach_hook_to_step(&conn, &plan.id, &s2.id, "pre-step", "h1").unwrap();
        // Same hook on same step but different lifecycle: OK.
        attach_hook_to_step(&conn, &plan.id, &s1.id, "post-step", "h1").unwrap();
        // Different hook name on same step/lifecycle: OK.
        attach_hook_to_step(&conn, &plan.id, &s1.id, "pre-step", "h2").unwrap();
        // Plan-wide alongside per-step with the same lifecycle/name: OK.
        attach_hook_to_plan(&conn, &plan.id, "pre-step", "h1").unwrap();

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM step_hooks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 5);
    }

    #[test]
    fn test_create_step_persists_change_policy_required_by_default() {
        let conn = setup();
        let plan = create_plan(&conn, "p", "/proj", "b", "d", None, None, &[]).unwrap();

        // None argument → Required default.
        let (s_default, _) = create_step(
            &conn,
            &plan.id,
            "def",
            "d",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        assert_eq!(s_default.change_policy, ChangePolicy::Required);

        let read = get_step(&conn, &s_default.id).unwrap();
        assert_eq!(read.change_policy, ChangePolicy::Required);
    }

    #[test]
    fn test_create_step_persists_change_policy_optional() {
        let conn = setup();
        let plan = create_plan(&conn, "p", "/proj", "b", "d", None, None, &[]).unwrap();

        let (s_opt, _) = create_step(
            &conn,
            &plan.id,
            "review",
            "d",
            None,
            None,
            &[],
            None,
            None,
            Some(ChangePolicy::Optional),
            None,
        )
        .unwrap();
        assert_eq!(s_opt.change_policy, ChangePolicy::Optional);

        let read = get_step(&conn, &s_opt.id).unwrap();
        assert_eq!(read.change_policy, ChangePolicy::Optional);

        // list_steps must also carry the new column through.
        let listed = list_steps(&conn, &plan.id).unwrap();
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].change_policy, ChangePolicy::Optional);
    }

    #[test]
    fn test_create_step_at_persists_change_policy() {
        let conn = setup();
        let plan = create_plan(&conn, "p", "/proj", "b", "d", None, None, &[]).unwrap();

        let (s, _) = create_step_at(
            &conn,
            &plan.id,
            "m5",
            "mid",
            "d",
            None,
            None,
            &[],
            None,
            None,
            Some(ChangePolicy::Optional),
            None,
        )
        .unwrap();
        assert_eq!(s.change_policy, ChangePolicy::Optional);
    }

    #[test]
    fn test_topo_sort_ignores_edges_outside_input() {
        let conn = setup();
        let ids = make_plans(&conn, 3);

        // p1 depends on p2 (in input) and p3 (NOT in input).
        add_plan_dependency(&conn, &ids[0], &ids[1]).unwrap();
        add_plan_dependency(&conn, &ids[0], &ids[2]).unwrap();

        // Sort only {p1, p2}. The p1 -> p3 edge should be ignored as
        // already-satisfied, so p2 must come before p1.
        let input = vec![ids[0].clone(), ids[1].clone()];
        let sorted = topo_sort_plans(&conn, &input).unwrap();
        assert_eq!(sorted, vec![ids[1].clone(), ids[0].clone()]);
    }

    // -- Live-run (run_locks V11) tests --

    /// Seed a minimal run_locks row for `project` so `update_live_phase`
    /// has something to update. Mirrors what `run_lock::acquire` does at the
    /// start of a real run.
    fn seed_run_lock(conn: &Connection, project: &str) {
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params![project, 1i64, "p1", "slug"],
        )
        .unwrap();
    }

    #[test]
    fn test_update_live_phase_sets_row() {
        let conn = setup();
        seed_run_lock(&conn, "/proj-lp1");

        update_live_phase(
            &conn,
            "/proj-lp1",
            Phase::Harness,
            Some("step-uuid"),
            Some(2),
            Some(1),
            Some(3),
            Some(42),
            Some("claude-code"),
            ChildUpdate::Set {
                pid: 99_999,
                start_token: Some("token-abc"),
            },
        )
        .unwrap();

        let live = get_live_run(&conn, "/proj-lp1").unwrap().unwrap();
        assert_eq!(live.project, "/proj-lp1");
        assert_eq!(live.pid, 1);
        assert_eq!(live.phase, Some(Phase::Harness));
        assert_eq!(live.step_id.as_deref(), Some("step-uuid"));
        assert_eq!(live.step_num, Some(2));
        assert_eq!(live.attempt, Some(1));
        assert_eq!(live.max_attempts, Some(3));
        assert_eq!(live.execution_log_id, Some(42));
        assert_eq!(live.current_command.as_deref(), Some("claude-code"));
        assert_eq!(live.child_pid, Some(99_999));
        assert_eq!(live.child_start_token.as_deref(), Some("token-abc"));
        assert!(live.phase_started_at.is_some());
        assert!(live.updated_at.is_some());
    }

    #[test]
    fn test_update_live_phase_coalesces_optional_fields() {
        let conn = setup();
        seed_run_lock(&conn, "/proj-lp2");

        // First write: populate step_id, max_attempts, child_pid.
        update_live_phase(
            &conn,
            "/proj-lp2",
            Phase::Harness,
            Some("step-1"),
            Some(1),
            Some(1),
            Some(3),
            Some(7),
            None,
            ChildUpdate::Set {
                pid: 12345,
                start_token: Some("tok-initial"),
            },
        )
        .unwrap();

        // Second write: pass None for everything except phase, and Keep for
        // the child. COALESCE should preserve the earlier values.
        update_live_phase(
            &conn,
            "/proj-lp2",
            Phase::Tests,
            None,
            None,
            None,
            None,
            None,
            None,
            ChildUpdate::Keep,
        )
        .unwrap();

        let live = get_live_run(&conn, "/proj-lp2").unwrap().unwrap();
        assert_eq!(live.phase, Some(Phase::Tests));
        assert_eq!(live.step_id.as_deref(), Some("step-1"));
        assert_eq!(live.step_num, Some(1));
        assert_eq!(live.attempt, Some(1));
        assert_eq!(live.max_attempts, Some(3));
        assert_eq!(live.execution_log_id, Some(7));
        assert_eq!(live.child_pid, Some(12345));
        assert_eq!(
            live.child_start_token.as_deref(),
            Some("tok-initial"),
            "child_start_token must be preserved when Keep is passed"
        );
    }

    #[test]
    fn test_update_live_phase_keep_preserves_child() {
        // Sanity check on the Keep variant: after a Set, a Keep must not
        // disturb either child column.
        let conn = setup();
        seed_run_lock(&conn, "/proj-keep");

        update_live_phase(
            &conn,
            "/proj-keep",
            Phase::Harness,
            None,
            None,
            None,
            None,
            None,
            None,
            ChildUpdate::Set {
                pid: 42,
                start_token: Some("tok"),
            },
        )
        .unwrap();

        update_live_phase(
            &conn,
            "/proj-keep",
            Phase::PreTestHook,
            None,
            None,
            None,
            None,
            None,
            None,
            ChildUpdate::Keep,
        )
        .unwrap();

        let live = get_live_run(&conn, "/proj-keep").unwrap().unwrap();
        assert_eq!(live.child_pid, Some(42));
        assert_eq!(live.child_start_token.as_deref(), Some("tok"));
    }

    #[test]
    fn test_update_live_phase_clear_child_sets_columns_null() {
        // After the harness phase ends, subsequent writes pass Clear so the
        // row stops advertising a dead pid.
        let conn = setup();
        seed_run_lock(&conn, "/proj-clear");

        // Set child fields.
        update_live_phase(
            &conn,
            "/proj-clear",
            Phase::Harness,
            None,
            None,
            None,
            None,
            None,
            None,
            ChildUpdate::Set {
                pid: 7777,
                start_token: Some("tok-set"),
            },
        )
        .unwrap();
        let before = get_live_run(&conn, "/proj-clear").unwrap().unwrap();
        assert_eq!(before.child_pid, Some(7777));
        assert_eq!(before.child_start_token.as_deref(), Some("tok-set"));

        // Clear them on the next phase write.
        update_live_phase(
            &conn,
            "/proj-clear",
            Phase::Tests,
            None,
            None,
            None,
            None,
            None,
            None,
            ChildUpdate::Clear,
        )
        .unwrap();

        let after = get_live_run(&conn, "/proj-clear").unwrap().unwrap();
        assert_eq!(after.child_pid, None, "Clear must null out child_pid");
        assert_eq!(
            after.child_start_token, None,
            "Clear must null out child_start_token"
        );
    }

    #[test]
    fn test_update_live_phase_overwrites_current_command() {
        let conn = setup();
        seed_run_lock(&conn, "/proj-lp3");

        // Set current_command = "cargo test".
        update_live_phase(
            &conn,
            "/proj-lp3",
            Phase::Tests,
            None,
            None,
            None,
            None,
            None,
            Some("cargo test"),
            ChildUpdate::Keep,
        )
        .unwrap();
        let before = get_live_run(&conn, "/proj-lp3").unwrap().unwrap();
        assert_eq!(before.current_command.as_deref(), Some("cargo test"));

        // Now move to PostTestHook with current_command = None; the column
        // should be cleared, NOT preserved.
        update_live_phase(
            &conn,
            "/proj-lp3",
            Phase::PostTestHook,
            None,
            None,
            None,
            None,
            None,
            None,
            ChildUpdate::Keep,
        )
        .unwrap();
        let after = get_live_run(&conn, "/proj-lp3").unwrap().unwrap();
        assert_eq!(
            after.current_command, None,
            "current_command must overwrite to NULL when None is passed"
        );
    }

    #[test]
    fn test_update_live_phase_errors_when_no_row() {
        let conn = setup();
        // Deliberately don't seed a row.
        let err = update_live_phase(
            &conn,
            "/proj-missing",
            Phase::Harness,
            None,
            None,
            None,
            None,
            None,
            None,
            ChildUpdate::Keep,
        )
        .unwrap_err();
        assert!(
            err.to_string().contains("No run_locks row for project"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_get_live_run_missing_returns_none() {
        let conn = setup();
        let result = get_live_run(&conn, "/nope").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_bind_live_run_to_plan_sets_plan_and_clears_stale_step_state() {
        let conn = setup();
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug, step_id, step_num,
                                    attempt, max_attempts, phase, current_command,
                                    execution_log_id, child_pid, child_start_token)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                "/proj-bind",
                1i64,
                "old-plan",
                "old-slug",
                "old-step",
                7i32,
                2i32,
                4i32,
                Phase::Harness.as_str(),
                "claude-code",
                99i64,
                12345i64,
                "child-token",
            ],
        )
        .unwrap();

        bind_live_run_to_plan(&conn, "/proj-bind", "new-plan", "new-slug").unwrap();

        let live = get_live_run(&conn, "/proj-bind").unwrap().unwrap();
        assert_eq!(live.plan_id.as_deref(), Some("new-plan"));
        assert_eq!(live.plan_slug.as_deref(), Some("new-slug"));
        assert_eq!(live.phase, Some(Phase::Idle));
        assert_eq!(live.step_id, None);
        assert_eq!(live.step_num, None);
        assert_eq!(live.attempt, None);
        assert_eq!(live.max_attempts, None);
        assert_eq!(live.current_command, None);
        assert_eq!(live.execution_log_id, None);
        assert_eq!(live.child_pid, None);
        assert_eq!(live.child_start_token, None);
        assert!(live.phase_started_at.is_some());
        assert!(live.updated_at.is_some());
    }

    // -- finalize_execution_log_as_interrupted_if_exists tests --

    #[test]
    fn test_finalize_execution_log_as_interrupted_sets_fields() {
        use crate::plan::{TerminationReason, TestStatus};
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let log = create_execution_log(&conn, &step.id, 1, None, None).unwrap();

        // Simulate the runner having written diff/stdout before dying.
        update_execution_log(
            &conn,
            log.id,
            Some(3.0),
            Some("+some diff"),
            &["unit: pass".to_string()],
            false,
            false,
            None,
            Some("hello stdout"),
            Some("warn stderr"),
            None,
            None,
            None,
            None,
            None,
            None,
        )
        .unwrap();

        let updated_row = finalize_execution_log_as_interrupted_if_exists(&conn, log.id).unwrap();
        assert!(updated_row, "expected row to match");

        let updated = get_execution_log_by_id(&conn, log.id).unwrap();
        assert_eq!(
            updated.termination_reason,
            Some(TerminationReason::UserInterrupted)
        );
        assert_eq!(updated.test_status, Some(TestStatus::NotRun));
        // Observability fields the runner wrote must survive.
        assert_eq!(updated.diff.as_deref(), Some("+some diff"));
        assert_eq!(updated.harness_stdout.as_deref(), Some("hello stdout"));
        assert_eq!(updated.harness_stderr.as_deref(), Some("warn stderr"));
        assert_eq!(updated.test_results, vec!["unit: pass".to_string()]);
    }

    #[test]
    fn test_finalize_execution_log_as_interrupted_preserves_existing_terminal() {
        use crate::plan::{TerminationReason, TestStatus};
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            &conn,
            &plan.id,
            "Step",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        let log = create_execution_log(&conn, &step.id, 1, None, None).unwrap();

        // Runner already finalized as Success before cancel raced in.
        update_execution_log(
            &conn,
            log.id,
            Some(1.0),
            None,
            &[],
            false,
            true,
            Some("abc"),
            None,
            None,
            None,
            None,
            None,
            None,
            Some(TerminationReason::Success),
            Some(TestStatus::Passed),
        )
        .unwrap();

        let updated_row = finalize_execution_log_as_interrupted_if_exists(&conn, log.id).unwrap();
        assert!(updated_row, "expected row to match");

        let updated = get_execution_log_by_id(&conn, log.id).unwrap();
        assert_eq!(updated.termination_reason, Some(TerminationReason::Success));
        assert_eq!(updated.test_status, Some(TestStatus::Passed));
    }

    #[test]
    fn test_finalize_execution_log_as_interrupted_missing_row_is_benign() {
        let conn = setup();
        let updated_row = finalize_execution_log_as_interrupted_if_exists(&conn, 99_999).unwrap();
        assert!(
            !updated_row,
            "expected no row to match for nonexistent log id"
        );
    }

    // -- delete_run_lock_row_unscoped tests --

    #[test]
    fn test_delete_run_lock_row_unscoped_matches_pid_and_token() {
        let conn = setup();
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["/proj-del", 4242i64, "tok-A", "pid1", "slug"],
        )
        .unwrap();

        let affected =
            delete_run_lock_row_unscoped(&conn, "/proj-del", 4242, Some("tok-A")).unwrap();
        assert_eq!(affected, 1);

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM run_locks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn test_delete_run_lock_row_unscoped_mismatched_token_leaves_row() {
        let conn = setup();
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["/proj-del2", 4242i64, "tok-A", "pid1", "slug"],
        )
        .unwrap();

        let affected =
            delete_run_lock_row_unscoped(&conn, "/proj-del2", 4242, Some("tok-OTHER")).unwrap();
        assert_eq!(affected, 0);

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM run_locks", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 1,
            "row owned by a different start token must survive"
        );
    }

    #[test]
    fn test_delete_run_lock_row_unscoped_null_token_both_sides() {
        let conn = setup();
        // A pre-v9 row without a start token.
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            params!["/proj-del3", 4242i64, "pid1", "slug"],
        )
        .unwrap();

        let affected = delete_run_lock_row_unscoped(&conn, "/proj-del3", 4242, None).unwrap();
        assert_eq!(affected, 1);
    }

    #[test]
    fn test_plan_context_prepend_round_trip() {
        let conn = setup();
        let plan = create_plan(&conn, "ctx", "/proj", "b", "d", None, None, &[]).unwrap();

        // Newly-created plans have `None` context_prepend — callers fall back
        // to the system default via `prompt::effective_context_prepend`.
        assert_eq!(plan.context_prepend, None);

        // Set to Some("custom"), read back.
        set_plan_context_prepend(&conn, &plan.id, Some("custom")).unwrap();
        let reloaded = get_plan_by_slug(&conn, "ctx", "/proj").unwrap().unwrap();
        assert_eq!(reloaded.context_prepend.as_deref(), Some("custom"));

        // Empty string is a real value — power-user escape hatch for "no
        // prepend at all". Must survive the round trip distinct from None.
        set_plan_context_prepend(&conn, &plan.id, Some("")).unwrap();
        let reloaded = get_plan_by_slug(&conn, "ctx", "/proj").unwrap().unwrap();
        assert_eq!(
            reloaded.context_prepend.as_deref(),
            Some(""),
            "empty string override must round-trip as Some(\"\"), not None"
        );

        // Clear back to None.
        set_plan_context_prepend(&conn, &plan.id, None).unwrap();
        let reloaded = get_plan_by_slug(&conn, "ctx", "/proj").unwrap().unwrap();
        assert_eq!(reloaded.context_prepend, None);
    }
}
