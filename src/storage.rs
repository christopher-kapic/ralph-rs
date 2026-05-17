// Storage abstraction: high-level CRUD operations wrapping db.rs

use anyhow::{Context, Result};
use rusqlite::types::Value;
use rusqlite::{Connection, params, params_from_iter};
use uuid::Uuid;

use crate::frac_index;
#[cfg(test)]
use crate::plan::InterruptionState;
use crate::plan::{
    ChangePolicy, ExecutionLog, Interruption, InterruptionKind, InterruptionOption, PLAN_COLUMNS,
    Phase, Plan, PlanStatus, RetryStrategy, Step, StepStatus,
};
use crate::run_lock::{LIVE_RUN_COLUMNS, LiveRun};

/// Canonical column list for `SELECT` queries against the `steps` table.
///
/// Matches the physical table layout after all migrations so [`Step::from_row`]
/// can index by column position. Kept as a single shared constant so adding a
/// new column (V13+ tags etc.) only requires editing one place instead of the
/// dozen scattered SELECTs.
const STEP_COLUMNS: &str = "id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, status, attempts, max_retries, created_at, updated_at, model, skipped_reason, change_policy, tags, retry_strategy, short_id, review_enabled, review_status, corrects_step_id";

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

    // `questions_enabled` is set explicitly to 1 here rather than relying on
    // the V16 column `DEFAULT 0`. New plans opt INTO the pause-for-question
    // feature by default; existing rows are untouched (no migration), so only
    // plans created via this path get the new default. The SQL column default
    // stays 0 so a bare INSERT (e.g. an import path that omits the column)
    // still behaves as before.
    conn.execute(
        "INSERT INTO plans (id, slug, project, branch_name, description, harness, agent, deterministic_tests, questions_enabled)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, 1)",
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
pub fn get_plan_by_id(conn: &Connection, id: &str) -> Result<Plan> {
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

/// List non-archived plans for a project, sorted by recency.
///
/// "Recency" is `MAX(execution_logs.started_at)` joined through
/// `steps.plan_id`, falling back to `plans.created_at` when the plan has no
/// execution logs yet. Most recent first. Archived plans are excluded.
///
/// Drives the TUI plan-list view (TUI-plan.md §5).
pub fn list_plans_sorted_by_recency(conn: &Connection, project: &str) -> Result<Vec<Plan>> {
    // Project the plan columns through the LEFT-JOIN with an alias so the
    // index positions seen by `Plan::from_row` line up with PLAN_COLUMNS.
    let plan_cols = PLAN_COLUMNS
        .split(", ")
        .map(|c| format!("p.{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT {plan_cols} \
         FROM plans p \
         LEFT JOIN ( \
             SELECT s.plan_id AS plan_id, MAX(l.started_at) AS last_run \
             FROM steps s JOIN execution_logs l ON l.step_id = s.id \
             GROUP BY s.plan_id \
         ) lr ON lr.plan_id = p.id \
         WHERE p.project = ?1 AND p.status != ?2 \
         ORDER BY COALESCE(lr.last_run, p.created_at) DESC, p.created_at DESC"
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map(
        params![project, PlanStatus::Archived.as_str()],
        Plan::from_row,
    )?;
    let mut plans = Vec::new();
    for row in rows {
        plans.push(row?);
    }
    Ok(plans)
}

/// List archived plans for a project, sorted by recency.
///
/// Mirror of [`list_plans_sorted_by_recency`] for the archived plan list view
/// (TUI-plan.md §6): same recency ordering, but the `WHERE` clause keeps only
/// plans whose `status = 'archived'`.
pub fn list_archived_plans_sorted_by_recency(
    conn: &Connection,
    project: &str,
) -> Result<Vec<Plan>> {
    let plan_cols = PLAN_COLUMNS
        .split(", ")
        .map(|c| format!("p.{c}"))
        .collect::<Vec<_>>()
        .join(", ");
    let query = format!(
        "SELECT {plan_cols} \
         FROM plans p \
         LEFT JOIN ( \
             SELECT s.plan_id AS plan_id, MAX(l.started_at) AS last_run \
             FROM steps s JOIN execution_logs l ON l.step_id = s.id \
             GROUP BY s.plan_id \
         ) lr ON lr.plan_id = p.id \
         WHERE p.project = ?1 AND p.status = ?2 \
         ORDER BY COALESCE(lr.last_run, p.created_at) DESC, p.created_at DESC"
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map(
        params![project, PlanStatus::Archived.as_str()],
        Plan::from_row,
    )?;
    let mut plans = Vec::new();
    for row in rows {
        plans.push(row?);
    }
    Ok(plans)
}

/// Number of archived plans for a project. Drives the conditional "Archived
/// (N)" tile rendered at the bottom of the plan-list view (TUI-plan.md §5).
pub fn count_archived_plans(conn: &Connection, project: &str) -> Result<u32> {
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM plans WHERE project = ?1 AND status = ?2")?;
    let n: i64 = stmt.query_row(params![project, PlanStatus::Archived.as_str()], |r| {
        r.get(0)
    })?;
    Ok(n as u32)
}

/// Most recent `execution_logs.started_at` across every step of `plan_id`,
/// or `None` when the plan has no logged attempts. Used to drive the
/// "Ran <date>" / "Created <date>" prefix on plan-list tiles.
pub fn last_log_started_at_for_plan(
    conn: &Connection,
    plan_id: &str,
) -> Result<Option<chrono::DateTime<chrono::Utc>>> {
    let mut stmt = conn.prepare(
        "SELECT MAX(el.started_at) FROM execution_logs el \
         JOIN steps s ON s.id = el.step_id \
         WHERE s.plan_id = ?1",
    )?;
    let row: Option<String> = stmt.query_row(params![plan_id], |r| r.get(0))?;
    match row {
        Some(s) => {
            let parsed = s
                .parse::<chrono::DateTime<chrono::Utc>>()
                .with_context(|| format!("parse execution_logs.started_at: {s}"))?;
            Ok(Some(parsed))
        }
        None => Ok(None),
    }
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

/// Set the `plans.questions_enabled` flag and bump `updated_at`.
///
/// Drives the `Q` keybinding in the TUI plan list (TUI-plan.md §17) and the
/// `ralph plan questions on|off` CLI commands. SQLite has no native bool, so
/// the value is stored as INTEGER 0/1.
pub fn set_plan_questions_enabled(conn: &Connection, plan_id: &str, enabled: bool) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET questions_enabled = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![enabled as i64, plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Record the git branch the plan most recently started a run on AND the
/// wall-clock timestamp at which that run started.
///
/// Written by the runner at run-start (both default and `--current-branch`
/// modes) so [`find_resumable_plans_for_branch`] can resolve `ralph resume`
/// (no slug) by current git branch without false-matching against a plan
/// whose `branch_name` happens to equal that branch but whose actual last
/// run executed elsewhere (e.g. a `--current-branch` run on `master`).
///
/// The same UPDATE also stamps `last_run_started_at` so the resume
/// resolver's `ORDER BY` can sort by "when did this plan last actually run"
/// rather than `updated_at` (which is bumped by unrelated edits like
/// toggling `questions_enabled` or `pause_requested`).
pub fn set_plan_last_run_branch(conn: &Connection, plan_id: &str, branch: &str) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET last_run_branch = ?1, \
                          last_run_started_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), \
                          updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ?2",
        params![branch, plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Find plans in this project that are resumable on `branch`.
///
/// "Resumable" = status in {in_progress, failed, aborted, ready}. A plan
/// matches when its `last_run_branch == branch` (its most recent run
/// physically executed on this branch) OR — only if `last_run_branch IS
/// NULL` — its `branch_name == branch` (covers plans created but never
/// run yet). The NULL fallback explicitly does NOT apply when
/// `last_run_branch` is set, which is what defends against false-matching
/// a paused plan whose slug a user later reused as a feature-branch name.
///
/// Ordered by `last_run_started_at DESC` (NULLS LAST) — the runner's
/// authoritative "when did this plan last actually start" stamp — falling
/// back to `updated_at DESC` for plans that have never run, then
/// `created_at DESC` defensively. Sorting on `last_run_started_at` rather
/// than `updated_at` defends against unrelated edits (e.g. `Q`/`P` flag
/// toggles, hook attachments) bumping `updated_at` and reordering recent
/// resumable plans.
pub fn find_resumable_plans_for_branch(
    conn: &Connection,
    project: &str,
    branch: &str,
) -> Result<Vec<Plan>> {
    let query = format!(
        "SELECT {PLAN_COLUMNS} FROM plans \
         WHERE project = ?1 \
           AND status IN (?2, ?3, ?4, ?5) \
           AND (last_run_branch = ?6 \
                OR (last_run_branch IS NULL AND branch_name = ?6)) \
         ORDER BY (last_run_started_at IS NULL), last_run_started_at DESC, \
                  updated_at DESC, created_at DESC"
    );
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map(
        params![
            project,
            PlanStatus::InProgress.as_str(),
            PlanStatus::Failed.as_str(),
            PlanStatus::Aborted.as_str(),
            PlanStatus::Ready.as_str(),
            branch,
        ],
        Plan::from_row,
    )?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// Find the most recent resumable plan in this project, ignoring branch.
///
/// Resumable status set matches [`find_resumable_plans_for_branch`]
/// ({in_progress, failed, aborted, ready}). Used by `ralph resume` (no
/// slug) as the fallback when the branch-based resolver finds no
/// candidates — e.g. running outside a git workdir, on a detached HEAD,
/// or on a branch that no plan has ever executed on. Ordered the same way
/// as [`find_resumable_plans_for_branch`] so the two resolvers agree on
/// "most recent" semantics.
///
/// Distinct from [`find_active_plan`] specifically because that helper
/// excludes `Aborted` — its callers (status / hint surfaces) treat
/// "active" more strictly than "resumable".
pub fn find_resumable_plan(conn: &Connection, project: &str) -> Result<Option<Plan>> {
    let query = format!(
        "SELECT {PLAN_COLUMNS} FROM plans \
         WHERE project = ?1 \
           AND status IN (?2, ?3, ?4, ?5) \
         ORDER BY (last_run_started_at IS NULL), last_run_started_at DESC, \
                  updated_at DESC, created_at DESC \
         LIMIT 1"
    );
    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query_map(
        params![
            project,
            PlanStatus::InProgress.as_str(),
            PlanStatus::Failed.as_str(),
            PlanStatus::Aborted.as_str(),
            PlanStatus::Ready.as_str(),
        ],
        Plan::from_row,
    )?;
    match rows.next() {
        Some(row) => Ok(Some(row?)),
        None => Ok(None),
    }
}

/// Set the `plans.pause_requested` flag and bump `updated_at`.
///
/// Drives the `P` keybinding in the TUI plan-detail view and the
/// `ralph pause` CLI. The runner reads this between step boundaries (see
/// [`get_plan_pause_requested`]) and exits with
/// `TerminationReason::PausedByUser` when set, clearing the flag in the
/// same transaction. SQLite has no native bool, so the value is stored as
/// INTEGER 0/1.
pub fn set_plan_pause_requested(conn: &Connection, plan_id: &str, requested: bool) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET pause_requested = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![requested as i64, plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Read the `plans.pause_requested` flag for a plan.
pub fn get_plan_pause_requested(conn: &Connection, plan_id: &str) -> Result<bool> {
    let value: i64 = match conn.query_row(
        "SELECT pause_requested FROM plans WHERE id = ?1",
        params![plan_id],
        |row| row.get(0),
    ) {
        Ok(v) => v,
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            anyhow::bail!("Plan not found: {plan_id}");
        }
        Err(e) => return Err(e.into()),
    };
    Ok(value != 0)
}

/// Atomically read `plans.pause_requested` and, if set, clear it in the
/// same transaction. Returns `true` when the flag was set on entry (and
/// has now been cleared), `false` otherwise. Used by the runner at step
/// boundaries so a subsequent `ralph resume` doesn't immediately re-pause.
pub fn take_plan_pause_requested(conn: &Connection, plan_id: &str) -> Result<bool> {
    let tx = conn.unchecked_transaction()?;
    let value: i64 = match tx.query_row(
        "SELECT pause_requested FROM plans WHERE id = ?1",
        params![plan_id],
        |row| row.get(0),
    ) {
        Ok(v) => v,
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            anyhow::bail!("Plan not found: {plan_id}");
        }
        Err(e) => return Err(e.into()),
    };
    let was_set = value != 0;
    if was_set {
        tx.execute(
            "UPDATE plans SET pause_requested = 0, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?1",
            params![plan_id],
        )?;
    }
    tx.commit()?;
    Ok(was_set)
}

// ---------------------------------------------------------------------------
// Cross-process skip bridge (V23)
// ---------------------------------------------------------------------------
//
// `ralph skip` and the TUI skip dialog run in a *different process* from the
// runner that owns the in-flight harness child. The process-global cancel
// registry in `signal.rs` only works when the skip and the runner share a
// process (e.g. unit tests). For production — where the runner is always a
// separate subprocess from both the TUI and `ralph skip` — the skip is
// handed off through `plans.skip_requested_step_id` / `plans.skip_changes`,
// modeled directly on the `plans.pause_requested` precedent above. The
// runner polls `take_skip_request` mid-attempt and, when the cleared
// request's step id matches the in-flight step, funnels into the *same*
// executor skip path the same-process registry uses.

/// Record a pending skip for `step_id` in `plan_id` with the operator's
/// chosen change-handling `kind`. Overwrites any prior pending request for
/// the plan (a fresh skip supersedes a stale one). Bumps `updated_at` like
/// the pause helper.
///
/// Deliberately *not* gated behind the per-project run lock: a run holds
/// that lock for its entire duration, so requiring it here would make
/// `ralph skip` impossible to issue against a live run — the exact case the
/// bridge exists for. This mirrors `set_plan_pause_requested`, which is also
/// lock-free.
pub fn request_skip(
    conn: &Connection,
    plan_id: &str,
    step_id: &str,
    kind: crate::git::ParkStrategyKind,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET skip_requested_step_id = ?1, skip_changes = ?2, \
         updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?3",
        params![step_id, kind.as_token(), plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Atomically read the pending skip request for `plan_id` and, if present,
/// clear it in the same transaction (read-and-clear, like
/// [`take_plan_pause_requested`]). Returns `Some((step_id, kind))` when a
/// request was pending on entry, `None` otherwise.
///
/// An unrecognized `skip_changes` token resolves to
/// [`crate::git::ParkStrategyKind::Stash`] via
/// [`crate::git::ParkStrategyKind::from_token`] so a corrupt value can never
/// make a skip silently destroy work.
///
/// Prefer [`take_skip_request_for_step`] from the runner poll loop: this
/// unconditional take, if paired with a separate `peek`, has a TOCTOU window
/// where a second `ralph skip` targeting a *different* step that lands
/// between the peek and the take is consumed and silently discarded.
#[allow(dead_code)]
pub fn take_skip_request(
    conn: &Connection,
    plan_id: &str,
) -> Result<Option<(String, crate::git::ParkStrategyKind)>> {
    let tx = conn.unchecked_transaction()?;
    let row: Option<(Option<String>, Option<String>)> = match tx.query_row(
        "SELECT skip_requested_step_id, skip_changes FROM plans WHERE id = ?1",
        params![plan_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    ) {
        Ok(v) => Some(v),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            anyhow::bail!("Plan not found: {plan_id}");
        }
        Err(e) => return Err(e.into()),
    };

    let result = match row {
        Some((Some(step_id), changes)) => {
            let kind = changes
                .as_deref()
                .map(crate::git::ParkStrategyKind::from_token)
                .unwrap_or(crate::git::ParkStrategyKind::Stash);
            tx.execute(
                "UPDATE plans SET skip_requested_step_id = NULL, skip_changes = NULL, \
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?1",
                params![plan_id],
            )?;
            Some((step_id, kind))
        }
        _ => None,
    };
    tx.commit()?;
    Ok(result)
}

/// Atomically consume the pending skip request for `plan_id` **only when it
/// targets `step_id`**, in a single predicate-guarded transaction. Returns
/// `Some(kind)` when a request for exactly this step was pending (and is now
/// cleared); `None` when nothing was pending or it targeted a *different*
/// step — in which case that request is left untouched so it is honored when
/// its own step runs.
///
/// This is the runner-poll-safe replacement for a separate
/// [`peek_skip_request`] + [`take_skip_request`]: the read and the clear
/// share the same `skip_requested_step_id = ?step_id` predicate inside one
/// transaction, so a concurrent `ralph skip` re-targeting a different step
/// can no longer slip in between and have its request swallowed against the
/// in-flight one.
///
/// An unrecognized `skip_changes` token resolves to
/// [`crate::git::ParkStrategyKind::Stash`] (non-destructive default), same as
/// [`take_skip_request`].
pub fn take_skip_request_for_step(
    conn: &Connection,
    plan_id: &str,
    step_id: &str,
) -> Result<Option<crate::git::ParkStrategyKind>> {
    let tx = conn.unchecked_transaction()?;
    // Read the change token for *this* step's pending request. The
    // `skip_requested_step_id = ?2` predicate means a row only comes back
    // when the pending request is for exactly the in-flight step.
    let changes: Option<Option<String>> = match tx.query_row(
        "SELECT skip_changes FROM plans WHERE id = ?1 AND skip_requested_step_id = ?2",
        params![plan_id, step_id],
        |r| r.get(0),
    ) {
        Ok(v) => Some(v),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(e) => return Err(e.into()),
    };
    let result = match changes {
        Some(changes) => {
            let kind = changes
                .as_deref()
                .map(crate::git::ParkStrategyKind::from_token)
                .unwrap_or(crate::git::ParkStrategyKind::Stash);
            // Clear under the same predicate so we never null out a request
            // that a concurrent writer just re-pointed at another step.
            tx.execute(
                "UPDATE plans SET skip_requested_step_id = NULL, skip_changes = NULL, \
                 updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                 WHERE id = ?1 AND skip_requested_step_id = ?2",
                params![plan_id, step_id],
            )?;
            Some(kind)
        }
        None => None,
    };
    tx.commit()?;
    Ok(result)
}

/// Non-clearing read of the pending skip request for `plan_id`. Returns
/// `Some((step_id, kind))` when one is pending, `None` otherwise.
///
/// The runner poll loop no longer peeks-then-takes (that had a TOCTOU
/// window); it uses the atomic predicate-guarded
/// [`take_skip_request_for_step`] instead. This read-only accessor is
/// retained for tests and external state inspection.
#[allow(dead_code)]
pub fn peek_skip_request(
    conn: &Connection,
    plan_id: &str,
) -> Result<Option<(String, crate::git::ParkStrategyKind)>> {
    let row: Option<(Option<String>, Option<String>)> = match conn.query_row(
        "SELECT skip_requested_step_id, skip_changes FROM plans WHERE id = ?1",
        params![plan_id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    ) {
        Ok(v) => Some(v),
        Err(rusqlite::Error::QueryReturnedNoRows) => {
            anyhow::bail!("Plan not found: {plan_id}");
        }
        Err(e) => return Err(e.into()),
    };
    Ok(match row {
        Some((Some(step_id), changes)) => {
            let kind = changes
                .as_deref()
                .map(crate::git::ParkStrategyKind::from_token)
                .unwrap_or(crate::git::ParkStrategyKind::Stash);
            Some((step_id, kind))
        }
        _ => None,
    })
}

/// Clear any pending skip request for `plan_id` without consuming it.
/// Idempotent — a no-op when nothing is pending. Used to tidy a stale
/// request the runner can no longer act on (e.g. the targeted step is no
/// longer the in-flight one) and, at run start, to drop a request a prior
/// run left behind so it can't spuriously skip the same step on this run.
pub fn clear_skip_request(conn: &Connection, plan_id: &str) -> Result<()> {
    conn.execute(
        "UPDATE plans SET skip_requested_step_id = NULL, skip_changes = NULL \
         WHERE id = ?1",
        params![plan_id],
    )?;
    Ok(())
}

/// One open interruption enriched with the plan + step context the CLI
/// list/show commands and the TUI inbox need to render. Driven by
/// [`list_open_questions`] (questions only) and
/// [`list_open_interruptions_enriched`] (questions *and* blockers).
///
/// Native: this is a projection of the `interruptions` table (state='open'),
/// **not** the dropped `step_questions` view. The struct name / `question`
/// field name are kept so existing TUI consumers (`plan_detail`, `run.rs`)
/// compile unchanged through the cutover; `kind` is added so a caller can
/// tell a blocker from a question.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct OpenQuestion {
    pub id: String,
    pub step_id: String,
    pub plan_id: String,
    pub plan_slug: String,
    /// 1-based position of the step within its plan (matches the numbering
    /// shown by `ralph step list`).
    pub step_num: usize,
    pub step_title: String,
    pub attempt: i32,
    /// The interruption body (the question text, or the blocker
    /// explanation). Named `question` for TUI source-compat.
    pub question: String,
    /// Proposed-answer texts in priority order (empty for blockers and
    /// freeform-only questions).
    pub suggestions: Vec<String>,
    pub kind: InterruptionKind,
    pub asked_at: String,
}

/// Shared native query behind [`list_open_questions`] /
/// [`list_open_interruptions_enriched`]: every *open* interruption for
/// `project` (optionally one plan slug), ordered `asked_at` ASC then `id`
/// ASC so an index is stable as new ones arrive. `kind_filter` of
/// `Some(InterruptionKind::Question)` restricts to questions (the legacy
/// `question list` surface); `None` returns questions *and* blockers (the
/// `interruption list` surface).
fn list_open_interruptions_enriched_impl(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    kind_filter: Option<InterruptionKind>,
) -> Result<Vec<OpenQuestion>> {
    use std::str::FromStr;

    // Compute each step's 1-based position via a window function so the
    // result matches the numbering users see in `ralph step list`.
    let mut base = String::from(
        "WITH step_pos AS (
            SELECT id, plan_id,
                   ROW_NUMBER() OVER (PARTITION BY plan_id ORDER BY sort_key) AS step_num
            FROM steps
        )
        SELECT i.id, i.step_id, s.plan_id, p.slug, sp.step_num,
               s.title, i.attempt, i.body, i.options, i.kind, i.asked_at
        FROM interruptions i
        JOIN steps s ON s.id = i.step_id
        JOIN plans p ON p.id = s.plan_id
        JOIN step_pos sp ON sp.id = i.step_id
        WHERE i.state = 'open' AND p.project = ?1",
    );
    if let Some(k) = kind_filter {
        base.push_str(&format!(" AND i.kind = '{}'", k.as_str()));
    }

    let map_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<OpenQuestion> {
        let options_json: String = row.get(8)?;
        let options: Vec<InterruptionOption> =
            serde_json::from_str(&options_json).map_err(|e| {
                rusqlite::Error::FromSqlConversionFailure(
                    8,
                    rusqlite::types::Type::Text,
                    Box::new(e),
                )
            })?;
        // Project options → priority-ordered texts so the legacy
        // `suggestions` shape is preserved for existing consumers.
        let mut ordered = options;
        ordered.sort_by_key(|o| o.priority);
        let suggestions: Vec<String> = ordered.into_iter().map(|o| o.text).collect();
        let kind_str: String = row.get(9)?;
        let kind = InterruptionKind::from_str(&kind_str).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(9, rusqlite::types::Type::Text, Box::new(e))
        })?;
        let step_num: i64 = row.get(4)?;
        Ok(OpenQuestion {
            id: row.get(0)?,
            step_id: row.get(1)?,
            plan_id: row.get(2)?,
            plan_slug: row.get(3)?,
            step_num: step_num as usize,
            step_title: row.get(5)?,
            attempt: row.get(6)?,
            question: row.get(7)?,
            suggestions,
            kind,
            asked_at: row.get(10)?,
        })
    };

    let mut out = Vec::new();
    if let Some(slug) = plan_slug {
        let sql = format!("{base} AND p.slug = ?2 ORDER BY i.asked_at ASC, i.id ASC");
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![project, slug], map_row)?;
        for row in rows {
            out.push(row?);
        }
    } else {
        let sql = format!("{base} ORDER BY i.asked_at ASC, i.id ASC");
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![project], map_row)?;
        for row in rows {
            out.push(row?);
        }
    }
    Ok(out)
}

/// List open *question* interruptions for plans in `project`, optionally
/// filtered to one plan slug. Native (`interruptions` table, `kind=question`,
/// `state=open`). Ordered `asked_at` ASC then `id` ASC so an index is stable
/// as new questions arrive. Drives the deprecated `ralph question list`
/// alias and the TUI's per-plan open-question surface.
pub fn list_open_questions(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
) -> Result<Vec<OpenQuestion>> {
    list_open_interruptions_enriched_impl(
        conn,
        project,
        plan_slug,
        Some(InterruptionKind::Question),
    )
}

/// List *every* open interruption (questions **and** blockers) for plans in
/// `project`, optionally filtered to one plan slug. Drives `ralph
/// interruption list` (docs/dag-redesign.md §7). Same ordering / stability
/// guarantee as [`list_open_questions`].
pub fn list_open_interruptions_enriched(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
) -> Result<Vec<OpenQuestion>> {
    list_open_interruptions_enriched_impl(conn, project, plan_slug, None)
}

// `list_answered_questions_for_step` (the unbounded "Previously answered
// questions" prompt feed) was removed in the §8/§4 cutover. Prompt assembly
// now uses the **bounded** `list_resolved_interruptions_for_step` above,
// which `LIMIT`s to the most-recent N resolved interruptions and is
// interruption-native (questions *and* blockers).

/// Resolve a *question* interruption with a freeform `answer` (no comment).
///
/// Thin native wrapper over [`resolve_interruption`] kept so the deprecated
/// `ralph question answer` alias and the TUI answer modal have a
/// question-shaped entry point. "Question not found" is preserved as the
/// not-found message (the alias only ever targets questions) while
/// [`resolve_interruption`] itself reports "Interruption …".
pub fn set_question_answer(conn: &Connection, question_id: &str, answer: &str) -> Result<()> {
    let exists: i64 = conn.query_row(
        "SELECT COUNT(*) FROM interruptions WHERE id = ?1",
        params![question_id],
        |r| r.get(0),
    )?;
    if exists == 0 {
        anyhow::bail!("Question not found: {question_id}");
    }
    resolve_interruption(conn, question_id, answer, None)
}

/// Count *open* interruptions (questions **or** blockers) for a specific
/// (step, attempt) pair.
///
/// Driven by [`crate::executor::execute_step`] after the harness exits to
/// detect whether the harness called `ralph question ask` / `ralph block`
/// during this attempt (docs/dag-redesign.md §7 "harness protocol"). A
/// non-zero count means the orchestrator skips tests + commit, rolls back
/// any diff, marks the branch `Blocked`, and — per §3.4/§9 invariant 4 —
/// consumes **no** retry budget.
pub fn count_unanswered_questions_for_attempt(
    conn: &Connection,
    step_id: &str,
    attempt: i32,
) -> Result<i64> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM interruptions
         WHERE step_id = ?1 AND attempt = ?2 AND state = 'open'",
        params![step_id, attempt],
        |row| row.get(0),
    )?;
    Ok(count)
}

/// Compute the *effective* status of a plan.
///
/// Per docs/dag-redesign.md §3.4/§6, [`PlanStatus::Interrupted`] is a derived
/// state — never written to the `plans.status` column. A plan reports
/// `Interrupted` whenever any **open interruption** (a question *or* a
/// blocker) exists for one of its steps; the underlying lifecycle column
/// un-shadows automatically once the human resolves the last open
/// interruption.
///
/// This helper is the single source of truth for that derivation: read the
/// stored status, then upgrade to `Interrupted` if any open interruption
/// exists. Reads the native `interruptions` table directly, so a blocker
/// (a question *or* a blocker) interrupts the plan.
#[allow(dead_code)] // TUI plan-status derivation lands in Phase 4.
pub fn plan_effective_status(conn: &Connection, plan_id: &str) -> Result<PlanStatus> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM interruptions i
         JOIN steps s ON s.id = i.step_id
         WHERE s.plan_id = ?1 AND i.state = 'open'",
        params![plan_id],
        |row| row.get(0),
    )?;
    if count > 0 {
        return Ok(PlanStatus::Interrupted);
    }
    let status_str: String = conn.query_row(
        "SELECT status FROM plans WHERE id = ?1",
        params![plan_id],
        |row| row.get(0),
    )?;
    use std::str::FromStr;
    PlanStatus::from_str(&status_str)
        .map_err(|e| anyhow::anyhow!("Invalid plan status '{status_str}' for plan {plan_id}: {e}"))
}

// ---------------------------------------------------------------------------
// Interruptions (native `interruptions` table, V26)
// ---------------------------------------------------------------------------

/// Canonical column list for `SELECT` queries against the `interruptions`
/// table, in the physical order [`Interruption::from_row`] expects. Every
/// `Interruption`-returning query must use this list so the positional
/// indices line up.
const INTERRUPTION_COLUMNS: &str =
    "id, step_id, attempt, kind, body, options, resolution, comment, state, asked_at, resolved_at";

/// Default cap for [`list_resolved_interruptions_for_step`]. Bounding the
/// resolved-interruption injection to the most-recent N entries closes the
/// §4 unbounded-context leak the old "Previously answered questions" section
/// had (no `LIMIT`, no per-entry truncation).
pub const DEFAULT_RESOLVED_INTERRUPTION_LIMIT: usize = 5;

/// Insert a fresh (open) interruption for a step+attempt.
///
/// The agent calls this (via `ralph question ask` / `ralph block`) from
/// inside a running step. The row starts `Open` with no resolution/comment;
/// the orchestrator observes the open row after the harness returns and marks
/// the branch `Blocked` (no retry budget consumed — docs/dag-redesign.md
/// §3.4). Returns the generated interruption id.
pub fn insert_interruption(
    conn: &Connection,
    step_id: &str,
    attempt: i32,
    kind: InterruptionKind,
    body: &str,
    options: &[InterruptionOption],
) -> Result<String> {
    let id = Uuid::new_v4().to_string();
    let options_json =
        serde_json::to_string(options).context("serializing interruption options for insert")?;
    conn.execute(
        "INSERT INTO interruptions \
            (id, step_id, attempt, kind, body, options, state, asked_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'open', \
                 strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        params![id, step_id, attempt, kind.as_str(), body, options_json],
    )?;
    Ok(id)
}

/// List every *open* interruption across `project`, optionally narrowed to a
/// single plan slug. Ordered `asked_at ASC, id ASC` so an interruption's
/// index is stable as new ones arrive (mirrors [`list_open_questions`]).
#[allow(dead_code)] // `interruption list` CLI + TUI inbox land in later steps.
pub fn list_open_interruptions(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
) -> Result<Vec<Interruption>> {
    let base = format!(
        "SELECT {cols} FROM interruptions i \
         JOIN steps s ON s.id = i.step_id \
         JOIN plans p ON p.id = s.plan_id \
         WHERE i.state = 'open' AND p.project = ?1",
        cols = INTERRUPTION_COLUMNS
            .split(", ")
            .map(|c| format!("i.{c}"))
            .collect::<Vec<_>>()
            .join(", "),
    );
    let mut out = Vec::new();
    if let Some(slug) = plan_slug {
        let sql = format!("{base} AND p.slug = ?2 ORDER BY i.asked_at ASC, i.id ASC");
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![project, slug], Interruption::from_row)?;
        for row in rows {
            out.push(row?);
        }
    } else {
        let sql = format!("{base} ORDER BY i.asked_at ASC, i.id ASC");
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params![project], Interruption::from_row)?;
        for row in rows {
            out.push(row?);
        }
    }
    Ok(out)
}

/// List every *open* interruption whose step belongs to `plan_id`. Ordered
/// `asked_at ASC, id ASC`. Drives the per-plan derived `Interrupted` status
/// and the plan-scoped inbox.
pub fn list_open_interruptions_for_plan(
    conn: &Connection,
    plan_id: &str,
) -> Result<Vec<Interruption>> {
    let sql = format!(
        "SELECT {cols} FROM interruptions i \
         JOIN steps s ON s.id = i.step_id \
         WHERE i.state = 'open' AND s.plan_id = ?1 \
         ORDER BY i.asked_at ASC, i.id ASC",
        cols = INTERRUPTION_COLUMNS
            .split(", ")
            .map(|c| format!("i.{c}"))
            .collect::<Vec<_>>()
            .join(", "),
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![plan_id], Interruption::from_row)?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// List *every* interruption (open and resolved) for a single step, oldest
/// first. Used by the step-detail TUI surface. NOT used for prompt assembly —
/// the prompt path uses the bounded [`list_resolved_interruptions_for_step`].
#[allow(dead_code)] // step-detail TUI surface lands in a later step.
pub fn list_interruptions_for_step(conn: &Connection, step_id: &str) -> Result<Vec<Interruption>> {
    let sql = format!(
        "SELECT {INTERRUPTION_COLUMNS} FROM interruptions \
         WHERE step_id = ?1 \
         ORDER BY asked_at ASC, id ASC"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![step_id], Interruption::from_row)?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// The **bounded** resolved-interruption query — the centerpiece of the §4
/// fix. Returns at most `limit` (the most-recent) *resolved* interruptions
/// for `step_id`, **newest first**. The prompt builder feeds these into the
/// bounded "Resolved interruptions" section so a step that has been
/// blocked/answered many times does not accumulate unbounded prompt context
/// (the pre-existing leak documented in docs/dag-redesign.md §4).
///
/// `limit` of 0 returns nothing. Ordering is `resolved_at DESC` (then
/// `asked_at DESC`, then `id DESC` as a stable tie-break) so the freshest
/// clarifications win the budget; callers that want chronological order can
/// reverse the result.
pub fn list_resolved_interruptions_for_step(
    conn: &Connection,
    step_id: &str,
    limit: usize,
) -> Result<Vec<Interruption>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let sql = format!(
        "SELECT {INTERRUPTION_COLUMNS} FROM interruptions \
         WHERE step_id = ?1 AND state = 'resolved' \
         ORDER BY resolved_at DESC, asked_at DESC, id DESC \
         LIMIT ?2"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(params![step_id, limit as i64], Interruption::from_row)?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// Resolve an open interruption: record the chosen `resolution` (an option
/// text or a freeform answer) and an optional `comment`, flip `state` to
/// `resolved`, and stamp `resolved_at`. Errors if no row matches `id`
/// ("Interruption not found") or it is already resolved.
///
/// Targets the native `interruptions` table directly, so `execute`'s
/// changed-row count is accurate; the post-resolve open count dropping to
/// zero for the step is what un-shadows its `Blocked` overlay and lets the
/// scheduler re-queue it (docs/dag-redesign.md §3.4/§3.5).
pub fn resolve_interruption(
    conn: &Connection,
    id: &str,
    resolution: &str,
    comment: Option<&str>,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE interruptions \
         SET resolution = ?1, comment = ?2, state = 'resolved', \
             resolved_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         WHERE id = ?3 AND state = 'open'",
        params![resolution, comment, id],
    )?;
    if affected == 0 {
        // Distinguish "no such id" from "already resolved" for a precise
        // error (the row exists but isn't open).
        let exists: i64 = conn.query_row(
            "SELECT COUNT(*) FROM interruptions WHERE id = ?1",
            params![id],
            |r| r.get(0),
        )?;
        if exists == 0 {
            anyhow::bail!("Interruption not found: {id}");
        }
        anyhow::bail!("Interruption already resolved: {id}");
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

/// Set (or clear) the plan-level retry-strategy override and bump
/// `updated_at`.
///
/// `Some(strategy)` records a plan-wide default; `None` writes SQL NULL,
/// meaning "no plan-level override" — resolution then falls through to the
/// global default ([`RetryStrategy::Keep`]) unless a step overrides it.
/// Kept as a dedicated setter (rather than threaded through `create_plan`)
/// to mirror [`set_plan_harness_gen`] and avoid churning every existing
/// `create_plan` callsite.
pub fn set_plan_retry_strategy(
    conn: &Connection,
    plan_id: &str,
    strategy: Option<RetryStrategy>,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET retry_strategy = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![strategy.map(|s| s.as_str()), plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Set (or clear) a plan's `review_enabled` override (V27,
/// docs/dag-redesign.md §6/§7) and bump `updated_at`. Stored as a nullable
/// INTEGER: `Some(true)`/`Some(false)` write 1/0 (an explicit per-plan
/// on/off that wins over the global `config.review.enabled`), `None` writes
/// NULL so the plan inherits the global default. `Plan::from_row` coerces
/// the column back to `Option<bool>`. Sibling setter to
/// [`set_plan_retry_strategy`] — the per-plan way to scope review on/off,
/// resolved by [`crate::config::effective_review_enabled`].
pub fn set_plan_review_enabled(
    conn: &Connection,
    plan_id: &str,
    enabled: Option<bool>,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET review_enabled = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![enabled.map(|b| if b { 1 } else { 0 }), plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// True when ANY plan **or** step anywhere in the DB has its
/// `review_enabled` override set truthy (`= 1`). Drives the `ralph doctor`
/// non-fatal review-harness warning (STEP 44, docs/dag-redesign.md §13.3):
/// if review is turned on somewhere but no review harness is configured (or
/// the configured one is off PATH), doctor surfaces it without failing.
/// Project-independent because the review harness is *global* config — a
/// review-enabled plan in any project means a missing review harness is
/// worth flagging. Cheap: two `EXISTS` probes, no row materialization.
pub fn any_review_enabled(conn: &Connection) -> Result<bool> {
    let found: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM plans WHERE review_enabled = 1) \
         OR EXISTS(SELECT 1 FROM steps WHERE review_enabled = 1)",
        [],
        |row| row.get(0),
    )?;
    Ok(found)
}

/// Set (or clear) a plan's `--squash-on-complete` toggle and bump
/// `updated_at`. Stored as a nullable INTEGER (V28): `false` writes 0
/// rather than NULL so the value round-trips explicitly; `Plan::from_row`
/// coerces both NULL and 0 to `false`.
pub fn set_plan_squash_on_complete(conn: &Connection, plan_id: &str, squash: bool) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET squash_on_complete = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![if squash { 1 } else { 0 }, plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Set (or clear) a plan's `max_review_corrections` cap (V30,
/// docs/dag-redesign.md §10 item 4 / §14.5) and bump `updated_at`. `None`
/// writes NULL → the runner uses the built-in default
/// ([`crate::review::DEFAULT_MAX_REVIEW_CORRECTIONS`]); `Some(n)` pins the
/// per-plan cap. Sibling setter to [`set_plan_squash_on_complete`] /
/// [`set_plan_retry_strategy`] — the per-plan way to configure the review
/// recursion bound, consistent with how `retry_strategy` is plan-configured.
pub fn set_plan_max_review_corrections(
    conn: &Connection,
    plan_id: &str,
    cap: Option<i32>,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET max_review_corrections = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![cap, plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Update a plan's description and bump `updated_at`. The plan description
/// IS the Plan layer of the four-layer prompt model, so this is the write
/// path behind the step-detail "Plan prompt" pane editor.
pub fn update_plan_description(conn: &Connection, plan_id: &str, description: &str) -> Result<()> {
    let affected = conn.execute(
        "UPDATE plans SET description = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![description, plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

/// Replace the plan's deterministic test commands. The slice is JSON-encoded
/// into the `deterministic_tests` column verbatim — empty slice clears the
/// list (one row of `[]`).
pub fn set_plan_deterministic_tests(
    conn: &Connection,
    plan_id: &str,
    tests: &[String],
) -> Result<()> {
    let tests_json = serde_json::to_string(tests)?;
    let affected = conn.execute(
        "UPDATE plans SET deterministic_tests = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![tests_json, plan_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Plan not found: {plan_id}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Project settings (the Project layer of the four-layer prompt model)
// ---------------------------------------------------------------------------

/// The project-scope prompt — one content blob, the Project layer of the
/// four-layer prompt model. `None` represents "no project-scope prompt
/// configured".
///
/// The value can be sourced from a checked-in file at
/// `<project>/.ralph/prompt.md` (so teams can share it via version control)
/// or from the `project_settings.prompt` DB column (the solo-user default).
/// **The file wins on read**; see [`resolve_project_prompt`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectSettings {
    pub prompt: Option<String>,
}

/// Where a resolved project-scope prompt came from. `prompt set`/`clear`
/// route their writes by inspecting this so the file, when present, stays
/// the source of truth.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectPromptSource {
    /// Sourced from `<project>/.ralph/prompt.md` (path carried for messaging).
    File(std::path::PathBuf),
    /// Sourced from (or destined for) the `project_settings.prompt` column.
    Db,
}

/// Path to the optional checked-in project-prompt file for `project`.
/// `project` is the project workdir (an absolute path string, as produced
/// by [`crate::commands::resolve_project`]).
pub fn project_prompt_file_path(project: &str) -> std::path::PathBuf {
    std::path::Path::new(project)
        .join(".ralph")
        .join("prompt.md")
}

/// Read `<project>/.ralph/prompt.md` if it exists and has non-whitespace
/// content. An empty / whitespace-only file is treated as "not present" so
/// an accidentally-blank file can't shadow a valid DB value.
///
/// Anything that isn't a usable regular file — missing, a *directory* at
/// that path, or any other read error (permissions, IsADirectory, etc.) —
/// is treated the same as absent (`Ok(None)`) so the DB fallback applies.
/// Otherwise a `.ralph/prompt.md` directory (or an unreadable file) would
/// make *every* `ralph run` hard-fail instead of degrading gracefully. A
/// genuinely present, readable, non-empty file still wins as before.
pub fn read_project_prompt_file(project: &str) -> Result<Option<String>> {
    let path = project_prompt_file_path(project);
    match std::fs::read_to_string(&path) {
        Ok(s) if s.trim().is_empty() => Ok(None),
        Ok(s) => Ok(Some(s)),
        // Missing, a directory, or otherwise not readable as a file: fall
        // back to the DB rather than aborting the run. (Reading a directory
        // surfaces as `IsADirectory` on Linux and `PermissionDenied` /
        // other kinds elsewhere — none of them mean "use this as a prompt".)
        Err(_) => Ok(None),
    }
}

/// Read the DB-only project-scope prompt for `project` (the raw
/// `project_settings.prompt` column), ignoring any checked-in file. Returns
/// a zero-value struct when no row exists.
pub fn get_project_settings_db(conn: &Connection, project: &str) -> Result<ProjectSettings> {
    let mut stmt = conn.prepare("SELECT prompt FROM project_settings WHERE project = ?1")?;
    let mut rows = stmt.query_map(params![project], |row| {
        Ok(ProjectSettings {
            prompt: row.get(0)?,
        })
    })?;
    match rows.next() {
        Some(row) => Ok(row?),
        None => Ok(ProjectSettings::default()),
    }
}

/// Resolve the effective project-scope prompt for `project`, file-first.
///
/// Returns the resolved content (if any) plus which source supplied it.
/// When the checked-in file has usable content the file wins; otherwise we
/// fall back to the DB column. The reported source reflects which path is
/// *active* for writes: if the file exists with content it's [`File`], else
/// [`Db`] even when both are empty.
///
/// [`File`]: ProjectPromptSource::File
/// [`Db`]: ProjectPromptSource::Db
pub fn resolve_project_prompt(
    conn: &Connection,
    project: &str,
) -> Result<(ProjectSettings, ProjectPromptSource)> {
    if let Some(content) = read_project_prompt_file(project)? {
        let path = project_prompt_file_path(project);
        return Ok((
            ProjectSettings {
                prompt: Some(content),
            },
            ProjectPromptSource::File(path),
        ));
    }
    let db = get_project_settings_db(conn, project)?;
    Ok((db, ProjectPromptSource::Db))
}

/// Read project-scope settings for `project`, file-first.
///
/// This is the central read used by the prompt-assembly path
/// ([`crate::prompt::build_step_prompt`] callers). It checks
/// `<project>/.ralph/prompt.md` before the DB column so a checked-in file
/// transparently overrides per-machine DB state.
pub fn get_project_settings(conn: &Connection, project: &str) -> Result<ProjectSettings> {
    Ok(resolve_project_prompt(conn, project)?.0)
}

/// Upsert the project-scope prompt into the DB column. Pass `None` to clear
/// the column. This writes the DB unconditionally — callers that want
/// file-aware routing should consult [`resolve_project_prompt`] first (see
/// `commands::prompt`).
pub fn set_project_prompt(conn: &Connection, project: &str, prompt: Option<&str>) -> Result<()> {
    conn.execute(
        "INSERT INTO project_settings (project, prompt)
         VALUES (?1, ?2)
         ON CONFLICT(project) DO UPDATE SET
             prompt = excluded.prompt,
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')",
        params![project, prompt],
    )?;
    Ok(())
}

/// Write the project-scope prompt to the checked-in file, creating the
/// `.ralph/` directory as needed. Used by `prompt set --scope project`
/// when the file is the active source.
pub fn write_project_prompt_file(project: &str, content: &str) -> Result<()> {
    let path = project_prompt_file_path(project);
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("Failed to create {}", parent.display()))?;
    }
    std::fs::write(&path, content)
        .with_context(|| format!("Failed to write {}", path.display()))?;
    Ok(())
}

/// Delete the checked-in project-prompt file. A missing file is benign
/// (the clear is idempotent).
pub fn delete_project_prompt_file(project: &str) -> Result<()> {
    let path = project_prompt_file_path(project);
    match std::fs::remove_file(&path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => {
            Err(anyhow::Error::new(e).context(format!("Failed to delete {}", path.display())))
        }
    }
}

// ---------------------------------------------------------------------------
// Step operations
// ---------------------------------------------------------------------------

/// Length of a step `short_id` (docs/dag-redesign.md §3): the stable,
/// plan-unique handle that replaces the positional step number as the
/// user-facing selector once a plan is a dependency DAG.
const SHORT_ID_LEN: usize = 8;

/// The base-62 alphabet (`0-9A-Za-z`) `short_id`s draw from — the same
/// alphabet `frac_index` uses for sort keys.
const SHORT_ID_ALPHABET: &[u8; 62] =
    b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

/// Generate one random 8-char base-62 candidate `short_id`.
///
/// UUID v4 supplies 122 bits of entropy; we consume it as a `u128` and peel
/// off [`SHORT_ID_LEN`] base-62 digits. 62^8 ≈ 2.18e14 keeps single-plan
/// collisions astronomically rare; [`mint_short_id`] still re-rolls on the
/// off chance, so the (negligible) modulo bias here is irrelevant.
fn random_short_id() -> String {
    let mut n = Uuid::new_v4().as_u128();
    let mut buf = [0u8; SHORT_ID_LEN];
    for slot in buf.iter_mut() {
        *slot = SHORT_ID_ALPHABET[(n % 62) as usize];
        n /= 62;
    }
    // Invariant: every byte came from SHORT_ID_ALPHABET (ASCII).
    String::from_utf8(buf.to_vec()).expect("base-62 alphabet is valid ASCII")
}

/// Mint a plan-unique 8-char base-62 `short_id`, re-rolling on collision
/// against existing `steps.short_id` rows for `plan_id`.
///
/// The V25 unique index `idx_steps_short_id` (`(plan_id, short_id)`)
/// enforces uniqueness at the DB layer; checking here avoids the
/// round-trip insert failure. This is the **single** source of minting
/// logic: the V25 migration backfill and runtime step creation both call
/// it, so migration-backfill and import-backfill produce the same DAG for
/// the same linear input (docs/dag-redesign.md §13.3). The collision check
/// observes prior same-transaction writes (SQLite read-your-own-writes), so
/// callers that mint-then-write in a loop on one connection stay unique
/// without a local "already assigned" set.
pub fn mint_short_id(conn: &Connection, plan_id: &str) -> Result<String> {
    loop {
        let candidate = random_short_id();
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM steps WHERE plan_id = ?1 AND short_id = ?2)",
            params![plan_id, candidate],
            |r| r.get(0),
        )?;
        if !exists {
            return Ok(candidate);
        }
    }
}

/// True when `s` has the exact *shape* of a step `short_id`: precisely
/// [`SHORT_ID_LEN`] characters, every one drawn from the base-62 alphabet
/// (`[0-9A-Za-z]`).
///
/// Shape only — this never touches the DB. The shared step-selector
/// resolver (`commands::resolve_step`) calls it to decide whether a
/// positional token *could* be a short id before checking for an actual
/// match, so the numeric and short-id selector forms can coexist under
/// one deterministic rule (docs/dag-redesign.md §7). Single-sources the
/// length/alphabet so the shape test can never drift from [`mint_short_id`].
///
/// Because every accepted byte is ASCII, `s.len() == SHORT_ID_LEN` here is
/// equivalent to "exactly 8 characters": any multibyte char would push the
/// byte length off 8 or fail the alphabet check.
pub fn is_short_id_shaped(s: &str) -> bool {
    s.len() == SHORT_ID_LEN && s.bytes().all(|b| SHORT_ID_ALPHABET.contains(&b))
}

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

    // Mint the plan-unique short_id via the one shared helper so runtime
    // step creation and the V25 migration/import backfill produce the same
    // handle for the same input (docs/dag-redesign.md §3.1, §13.3).
    let short_id = mint_short_id(conn, plan_id)?;

    conn.execute(
        "INSERT INTO steps (id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, max_retries, model, change_policy, tags, short_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![id, plan_id, sort_key, title, description, agent, harness, criteria_json, max_retries, model, change_policy.as_str(), tags_json, short_id],
    )
    .with_context(|| format!("Failed to insert step '{title}' for plan '{plan_id}'"))?;

    // The new step is always appended, so its position is the total step count.
    let position: i64 = conn.query_row(
        "SELECT COUNT(*) FROM steps WHERE plan_id = ?1",
        params![plan_id],
        |row| row.get(0),
    )?;

    Ok((get_step(conn, &id)?, position as usize))
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

    // See [`create_step`]: same single-source short_id minting helper so
    // every step-creation path is consistent (docs/dag-redesign.md §3.1).
    let short_id = mint_short_id(conn, plan_id)?;

    conn.execute(
        "INSERT INTO steps (id, plan_id, sort_key, title, description, agent, harness, acceptance_criteria, max_retries, model, change_policy, tags, short_id)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
        params![id, plan_id, sort_key, title, description, agent, harness, criteria_json, max_retries, model, change_policy.as_str(), tags_json, short_id],
    )
    .with_context(|| format!("Failed to insert step '{title}' for plan '{plan_id}'"))?;

    // Count steps with sort_key <= the new one to get the 1-based position.
    let position: i64 = conn.query_row(
        "SELECT COUNT(*) FROM steps WHERE plan_id = ?1 AND sort_key <= ?2",
        params![plan_id, sort_key],
        |row| row.get(0),
    )?;

    Ok((get_step(conn, &id)?, position as usize))
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

/// Set (or clear) the step-level retry-strategy override and bump
/// `updated_at`.
///
/// `Some(strategy)` records a per-step override; `None` writes SQL NULL,
/// meaning "no step-level override" — resolution falls through to the
/// plan's value and then the global default ([`RetryStrategy::Keep`]).
/// Kept as a dedicated setter (rather than a new field on
/// [`update_step_fields_ext`]) so the ~100 `create_step` callsites and the
/// existing `update_step_fields_ext` callers stay untouched, mirroring how
/// `plan_harness` is set via [`set_plan_harness_gen`] after `create_plan`.
pub fn set_step_retry_strategy(
    conn: &Connection,
    step_id: &str,
    strategy: Option<RetryStrategy>,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE steps SET retry_strategy = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![strategy.map(|s| s.as_str()), step_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Step not found: {step_id}");
    }
    Ok(())
}

/// Set (or clear) a step's `review_enabled` override (V27,
/// docs/dag-redesign.md §6/§7) and bump `updated_at`. Stored as a nullable
/// INTEGER: `Some(true)`/`Some(false)` write 1/0 (an explicit per-step
/// on/off that wins over the plan/global default), `None` writes NULL so
/// the step inherits the plan (then global) default. Sibling setter to
/// [`set_step_retry_strategy`] — the per-step way to scope review on/off,
/// resolved by [`crate::config::effective_review_enabled`] (step > plan >
/// config > false).
pub fn set_step_review_enabled(
    conn: &Connection,
    step_id: &str,
    enabled: Option<bool>,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE steps SET review_enabled = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![enabled.map(|b| if b { 1 } else { 0 }), step_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Step not found: {step_id}");
    }
    Ok(())
}

/// Overwrite a step's `short_id` with a caller-supplied value.
///
/// The V25 backfill and runtime `create_step` mint a fresh random
/// `short_id`; the DAG-aware import path instead **preserves** the
/// bundle's portable edge handles (docs/dag-redesign.md §13.3), so it
/// creates the step (which mints a throwaway id) and then calls this to
/// pin the bundle's `short_id`. Mirrors the V25 migration's raw
/// `UPDATE steps SET short_id` (no `updated_at` bump — `short_id` is an
/// identity handle, not a mutable user field). The
/// `idx_steps_short_id (plan_id, short_id)` unique index still enforces
/// plan-uniqueness; a violation surfaces as an `Err` here and (because
/// imports are transactional) rolls the whole import back, so no partial
/// plan is written.
pub fn set_step_short_id(conn: &Connection, step_id: &str, short_id: &str) -> Result<()> {
    let affected = conn
        .execute(
            "UPDATE steps SET short_id = ?1 WHERE id = ?2",
            params![short_id, step_id],
        )
        .with_context(|| format!("Failed to set short_id '{short_id}' for step {step_id}"))?;
    if affected == 0 {
        anyhow::bail!("Step not found: {step_id}");
    }
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
    // Also reset a stranded `review_status = in_flight` back to `pending`
    // in the SAME atomic UPDATE. A crash *during a concurrent review*
    // (docs/dag-redesign.md §3.5 item 3) leaves a step `InProgress` +
    // `review_status = InFlight`; sweeping only the step status would
    // produce the impossible `Aborted` + `InFlight` combination (a review
    // can never be in flight for an aborted step — its detached task died
    // with the crashed runner). Resetting it to `pending` makes a
    // subsequent re-run re-review the step from a clean state rather than
    // believing a phantom reviewer is still running. Other review_status
    // values (passed/failed/disabled/skipped) are durable verdicts and are
    // left untouched.
    let sql = format!(
        "UPDATE steps SET status = ?1,
             review_status = CASE WHEN review_status = ?4 THEN ?5 ELSE review_status END,
             updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE plan_id = ?2 AND status = ?3
         RETURNING {STEP_COLUMNS}",
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map(
        params![
            StepStatus::Aborted.as_str(),
            plan_id,
            StepStatus::InProgress.as_str(),
            crate::plan::ReviewStatus::InFlight.as_str(),
            crate::plan::ReviewStatus::Pending.as_str(),
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

/// Delete a single `execution_logs` row by id.
///
/// Used by the executor's TUI-skip *cancel* path (step 18): the retry loop
/// creates the `execution_logs` row (with the prompt) *before* spawning the
/// harness, so a cancelled attempt must delete that row to honor the
/// guarantee that a cancelled attempt leaves no `UNIQUE(step_id, attempt)`
/// row behind and consumes no retry budget. Idempotent — deleting a missing
/// id is a no-op.
pub fn delete_execution_log(conn: &Connection, log_id: i64) -> Result<()> {
    conn.execute("DELETE FROM execution_logs WHERE id = ?1", params![log_id])
        .with_context(|| format!("Failed to delete execution log {log_id}"))?;
    Ok(())
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
// Step dependency operations
// ---------------------------------------------------------------------------

/// Record that `step_id` depends on `depends_on_step_id`.
///
/// A direct structural clone of [`add_plan_dependency`] against the V25
/// `step_dependencies` table (docs/dag-redesign.md §3.1). Bails with a
/// user-friendly error if the two IDs are the same (mirroring the table's
/// self-edge `CHECK`), or if adding the edge would create a cycle in the step
/// dependency graph. Cycle detection runs before the insert via
/// [`would_create_step_cycle`], so callers never need to invoke it themselves
/// (docs/dag-redesign.md §6: DAG acyclicity validated on every edge mutation).
///
/// The CLI/scheduler callers land in later DAG-redesign steps (`ralph step
/// dependency`, `--depends-on`, the topological scheduler); until then tests
/// are the only consumers, so `#[allow(dead_code)]` marks the binary surface
/// area, not the function itself.
#[allow(dead_code)]
pub fn add_step_dependency(
    conn: &Connection,
    step_id: &str,
    depends_on_step_id: &str,
) -> Result<()> {
    if step_id == depends_on_step_id {
        anyhow::bail!("A step cannot depend on itself");
    }

    if would_create_step_cycle(conn, step_id, depends_on_step_id)? {
        anyhow::bail!("Adding dependency {step_id} -> {depends_on_step_id} would create a cycle");
    }

    conn.execute(
        "INSERT INTO step_dependencies (step_id, depends_on_step_id) VALUES (?1, ?2)",
        params![step_id, depends_on_step_id],
    )
    .with_context(|| format!("Failed to add dependency {step_id} -> {depends_on_step_id}"))?;

    Ok(())
}

/// Remove a specific step-dependency edge. No-op if the row does not exist.
#[allow(dead_code)] // CLI/scheduler callers land in later DAG-redesign steps.
pub fn remove_step_dependency(
    conn: &Connection,
    step_id: &str,
    depends_on_step_id: &str,
) -> Result<()> {
    conn.execute(
        "DELETE FROM step_dependencies WHERE step_id = ?1 AND depends_on_step_id = ?2",
        params![step_id, depends_on_step_id],
    )
    .with_context(|| format!("Failed to remove dependency {step_id} -> {depends_on_step_id}"))?;
    Ok(())
}

/// List the step IDs that `step_id` directly depends on.
#[allow(dead_code)] // CLI/scheduler callers land in later DAG-redesign steps.
pub fn list_step_dependencies(conn: &Connection, step_id: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT depends_on_step_id FROM step_dependencies WHERE step_id = ?1 ORDER BY depends_on_step_id ASC",
    )?;
    let rows = stmt.query_map(params![step_id], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// List the step IDs that directly depend on `step_id` (reverse edges).
#[allow(dead_code)] // CLI/scheduler callers land in later DAG-redesign steps.
pub fn list_step_dependents(conn: &Connection, step_id: &str) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT step_id FROM step_dependencies WHERE depends_on_step_id = ?1 ORDER BY step_id ASC",
    )?;
    let rows = stmt.query_map(params![step_id], |row| row.get::<_, String>(0))?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// Check whether adding `step_id -> new_dep_id` would create a cycle.
///
/// A direct structural clone of [`would_create_cycle`] against the V25
/// `step_dependencies` table (docs/dag-redesign.md §6): walks the transitive
/// dependencies of `new_dep_id`; if `step_id` appears in that set, the edge
/// would close a cycle. A self-edge (`step_id == new_dep_id`) is also reported
/// as a cycle. Reused by import validation (docs/dag-redesign.md §13.3).
#[allow(dead_code)] // CLI/scheduler callers land in later DAG-redesign steps.
pub fn would_create_step_cycle(conn: &Connection, step_id: &str, new_dep_id: &str) -> Result<bool> {
    if step_id == new_dep_id {
        return Ok(true);
    }

    let mut stack: Vec<String> = vec![new_dep_id.to_string()];
    let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();

    while let Some(current) = stack.pop() {
        if !visited.insert(current.clone()) {
            continue;
        }
        if current == step_id {
            return Ok(true);
        }
        let deps = list_step_dependencies(conn, &current)?;
        for d in deps {
            if !visited.contains(&d) {
                stack.push(d);
            }
        }
    }

    Ok(false)
}

/// Load every step-dependency edge for `plan_id` as an adjacency map
/// `step_id -> [depends_on_step_id, ...]`.
///
/// One query for the whole plan instead of N calls to
/// [`list_step_dependencies`]; the topological scheduler
/// (docs/dag-redesign.md §3.5) re-reads this every tick so a step added
/// mid-run with `--depends-on` is picked up. Steps with no outgoing edges
/// are simply absent from the map (callers treat a missing key as
/// "no dependencies"). Edges are scoped to the plan via a join on
/// `steps.plan_id`, so a returned `depends_on_step_id` always belongs to
/// the same plan.
pub fn list_step_dependency_edges(
    conn: &Connection,
    plan_id: &str,
) -> Result<std::collections::HashMap<String, Vec<String>>> {
    let mut stmt = conn.prepare(
        "SELECT sd.step_id, sd.depends_on_step_id \
         FROM step_dependencies sd \
         JOIN steps s ON s.id = sd.step_id \
         WHERE s.plan_id = ?1 \
         ORDER BY sd.step_id ASC, sd.depends_on_step_id ASC",
    )?;
    let rows = stmt.query_map(params![plan_id], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    let mut edges: std::collections::HashMap<String, Vec<String>> =
        std::collections::HashMap::new();
    for row in rows {
        let (step_id, dep_id) = row?;
        edges.entry(step_id).or_default().push(dep_id);
    }
    Ok(edges)
}

// ---------------------------------------------------------------------------
// Review pipeline: review_status, corrective steps, corrective-step request
// bridge (docs/dag-redesign.md §3.3, §9-inv-3, §10)
// ---------------------------------------------------------------------------

/// Set a step's `review_status` (V27 `steps.review_status` TEXT column) and
/// bump `updated_at`. NULL on disk means [`crate::plan::ReviewStatus::Pending`];
/// this writes the explicit serialized variant so the
/// Pending→InFlight→Passed/Failed transitions the review pipeline drives are
/// durable and observable cross-process (mirrors [`update_step_status`]).
pub fn update_step_review_status(
    conn: &Connection,
    step_id: &str,
    status: crate::plan::ReviewStatus,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE steps SET review_status = ?1, updated_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') WHERE id = ?2",
        params![status.as_str(), step_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Step not found: {step_id}");
    }
    Ok(())
}

/// Point a (reviewer-inserted) step at the step it corrects (§10): sets
/// `steps.corrects_step_id`. Only the orchestrator calls this, as the SOLE
/// DAG writer (§9-inv-3). `None` clears it (an ordinary, non-corrective
/// step). Does not bump `updated_at` — `corrects_step_id` is immutable
/// provenance set once at corrective-step creation, like `short_id`.
pub fn set_step_corrects_step_id(
    conn: &Connection,
    step_id: &str,
    corrects: Option<&str>,
) -> Result<()> {
    let affected = conn.execute(
        "UPDATE steps SET corrects_step_id = ?1 WHERE id = ?2",
        params![corrects, step_id],
    )?;
    if affected == 0 {
        anyhow::bail!("Step not found: {step_id}");
    }
    Ok(())
}

/// One open corrective-step request — the durable face of the §9-inv-3
/// "structured channel" by which a reviewer *requests* (never performs) a
/// DAG mutation. Rows live in `corrective_step_requests` (V29).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CorrectiveStepRequest {
    pub id: String,
    pub reviewed_step_id: String,
    pub reviewed_iteration: i32,
    pub commit_sha: String,
    pub issues: i32,
    pub verdict_body: Option<String>,
}

/// Insert an OPEN corrective-step request (V29 bridge — docs/dag-redesign.md
/// §9 invariant 3). This is the *only* DAG-related write a failed review
/// performs: it records a *request*, keyed to the reviewed step + iteration +
/// commit, that the orchestrator drains at a scheduler tick and acts on as
/// the sole writer. Structural sibling of [`request_skip`] / V23 skip-bridge
/// and [`insert_interruption`] / V26 interruption-bridge. Returns the
/// generated request id.
pub fn insert_corrective_step_request(
    conn: &Connection,
    reviewed_step_id: &str,
    reviewed_iteration: i32,
    commit_sha: &str,
    issues: i32,
    verdict_body: Option<&str>,
) -> Result<String> {
    let id = Uuid::new_v4().to_string();
    conn.execute(
        "INSERT INTO corrective_step_requests \
            (id, reviewed_step_id, reviewed_iteration, commit_sha, issues, verdict_body, state, requested_at) \
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'open', strftime('%Y-%m-%dT%H:%M:%fZ','now'))",
        params![
            id,
            reviewed_step_id,
            reviewed_iteration,
            commit_sha,
            issues,
            verdict_body
        ],
    )?;
    Ok(id)
}

/// List every OPEN corrective-step request whose reviewed step belongs to
/// `plan_id`, oldest-first (`requested_at ASC, id ASC` — stable). The
/// orchestrator drains these at a scheduler tick. Ordering is deterministic
/// so the scheduler tie-break stays reproducible (§3.5 item 4 / §11).
pub fn list_open_corrective_step_requests_for_plan(
    conn: &Connection,
    plan_id: &str,
) -> Result<Vec<CorrectiveStepRequest>> {
    let mut stmt = conn.prepare(
        "SELECT r.id, r.reviewed_step_id, r.reviewed_iteration, r.commit_sha, r.issues, r.verdict_body \
         FROM corrective_step_requests r \
         JOIN steps s ON s.id = r.reviewed_step_id \
         WHERE r.state = 'open' AND s.plan_id = ?1 \
         ORDER BY r.requested_at ASC, r.id ASC",
    )?;
    let rows = stmt.query_map(params![plan_id], |row| {
        Ok(CorrectiveStepRequest {
            id: row.get(0)?,
            reviewed_step_id: row.get(1)?,
            reviewed_iteration: row.get(2)?,
            commit_sha: row.get(3)?,
            issues: row.get(4)?,
            verdict_body: row.get(5)?,
        })
    })?;
    let mut out = Vec::new();
    for row in rows {
        out.push(row?);
    }
    Ok(out)
}

/// Atomically mark a corrective-step request `consumed` **only when it is
/// still `open`**, in one predicate-guarded transaction (the same
/// read-and-clear discipline as [`take_skip_request_for_step`]). Returns
/// `Ok(true)` when this call transitioned an open row (the caller owns the
/// consumption and must perform the §10 mutation), `Ok(false)` when it was
/// already consumed or gone — so the §9-inv-3 single-writer guarantee holds
/// even if the drain is ever entered twice.
pub fn consume_corrective_step_request(conn: &Connection, request_id: &str) -> Result<bool> {
    let affected = conn.execute(
        "UPDATE corrective_step_requests SET state = 'consumed' \
         WHERE id = ?1 AND state = 'open'",
        params![request_id],
    )?;
    Ok(affected > 0)
}

/// Length of the corrective chain ending at `step_id`, i.e. how many
/// `corrects_step_id` hops it takes to walk back to a non-corrective step.
///
/// An ordinary step returns 0. A first corrective step A′ (`corrects A`,
/// A ordinary) returns 1; A″ correcting A′ returns 2; and so on. The
/// recursion-cap check (§10 item 4 / §14.5) compares the chain length the
/// *next* correction would have against the per-plan
/// `max_review_corrections`. A `visited` guard bounds the walk even if a
/// `corrects_step_id` pointer is ever cyclic (it cannot be under normal
/// operation — corrective steps only ever point *backward* at an
/// already-existing step).
pub fn corrective_chain_len(conn: &Connection, step_id: &str) -> Result<usize> {
    let mut len = 0usize;
    let mut current = step_id.to_string();
    let mut visited: std::collections::HashSet<String> = std::collections::HashSet::new();
    loop {
        if !visited.insert(current.clone()) {
            break;
        }
        let corrects: Option<String> = conn
            .query_row(
                "SELECT corrects_step_id FROM steps WHERE id = ?1",
                params![current],
                |r| r.get(0),
            )
            .ok()
            .flatten();
        match corrects {
            Some(parent) => {
                len += 1;
                current = parent;
            }
            None => break,
        }
    }
    Ok(len)
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

    // -- short_id minting --

    #[test]
    fn test_mint_short_id_unique_length_charset() {
        let conn = setup();
        let plan = create_plan(&conn, "mint", "/proj", "b", "d", None, None, &[]).unwrap();

        // Mint a short_id per step, persisting each so the next mint's
        // collision check (against steps.short_id) actually observes prior
        // assignments — exactly the V25-migration usage pattern.
        let mut seen = std::collections::HashSet::new();
        for i in 0..256 {
            let (step, _) = create_step(
                &conn,
                &plan.id,
                &format!("Step {i}"),
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
            let sid = mint_short_id(&conn, &plan.id).expect("mint_short_id");
            assert_eq!(sid.chars().count(), 8, "short_id must be 8 chars: {sid:?}");
            assert!(
                sid.bytes().all(|b| SHORT_ID_ALPHABET.contains(&b)),
                "short_id must be base-62 ([0-9A-Za-z]): {sid:?}"
            );
            assert!(seen.insert(sid.clone()), "duplicate short_id minted: {sid}");
            conn.execute(
                "UPDATE steps SET short_id = ?1 WHERE id = ?2",
                params![sid, step.id],
            )
            .unwrap();
        }
        assert_eq!(seen.len(), 256, "every minted short_id must be unique");
    }

    #[test]
    fn test_create_step_assigns_unique_short_id() {
        let conn = setup();
        let plan = create_plan(&conn, "sid", "/proj", "b", "d", None, None, &[]).unwrap();

        let (s1, _) = create_step(
            &conn,
            &plan.id,
            "Step one",
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
            "Step two",
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

        assert!(
            !s1.short_id.is_empty(),
            "create_step must assign a non-empty short_id"
        );
        assert!(
            !s2.short_id.is_empty(),
            "create_step must assign a non-empty short_id"
        );
        assert_ne!(
            s1.short_id, s2.short_id,
            "two steps in one plan must get distinct short_ids"
        );

        // create_step_at goes through the same minting helper and must also
        // produce a plan-unique short_id distinct from the appended steps.
        let (s3, _) = create_step_at(
            &conn,
            &plan.id,
            "z",
            "Step three",
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
        assert!(!s3.short_id.is_empty());
        assert_ne!(s3.short_id, s1.short_id);
        assert_ne!(s3.short_id, s2.short_id);
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
    fn test_list_plans_sorted_by_recency_no_logs_uses_created_at() {
        // With no execution_logs rows, the order should be `created_at DESC`.
        let conn = setup();

        let p1 = create_plan(&conn, "p1", "/proj", "b1", "d", None, None, &[]).unwrap();
        // Force distinct created_at stamps so ordering is deterministic.
        conn.execute(
            "UPDATE plans SET created_at = ?1 WHERE id = ?2",
            params!["2026-01-01T00:00:00.000Z", p1.id],
        )
        .unwrap();
        let p2 = create_plan(&conn, "p2", "/proj", "b2", "d", None, None, &[]).unwrap();
        conn.execute(
            "UPDATE plans SET created_at = ?1 WHERE id = ?2",
            params!["2026-03-01T00:00:00.000Z", p2.id],
        )
        .unwrap();
        let p3 = create_plan(&conn, "p3", "/proj", "b3", "d", None, None, &[]).unwrap();
        conn.execute(
            "UPDATE plans SET created_at = ?1 WHERE id = ?2",
            params!["2026-02-01T00:00:00.000Z", p3.id],
        )
        .unwrap();

        let plans = list_plans_sorted_by_recency(&conn, "/proj").unwrap();
        let slugs: Vec<&str> = plans.iter().map(|p| p.slug.as_str()).collect();
        assert_eq!(slugs, vec!["p2", "p3", "p1"]);
    }

    #[test]
    fn test_list_plans_sorted_by_recency_logs_win_over_created_at() {
        // A plan with a recent execution log should sort above a plan that
        // was created more recently but has never been run.
        let conn = setup();

        // p1 created earliest, but will get a recent log.
        let p1 = create_plan(&conn, "p1", "/proj", "b1", "d", None, None, &[]).unwrap();
        conn.execute(
            "UPDATE plans SET created_at = ?1 WHERE id = ?2",
            params!["2026-01-01T00:00:00.000Z", p1.id],
        )
        .unwrap();
        // p2 created most recently, but never run.
        let p2 = create_plan(&conn, "p2", "/proj", "b2", "d", None, None, &[]).unwrap();
        conn.execute(
            "UPDATE plans SET created_at = ?1 WHERE id = ?2",
            params!["2026-04-01T00:00:00.000Z", p2.id],
        )
        .unwrap();

        // Add a step + execution log to p1, dated after p2's created_at.
        let (step, _) = create_step(
            &conn,
            &p1.id,
            "s",
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
        let log = create_execution_log(&conn, &step.id, 1, None, None).unwrap();
        conn.execute(
            "UPDATE execution_logs SET started_at = ?1 WHERE id = ?2",
            params!["2026-05-01T00:00:00.000Z", log.id],
        )
        .unwrap();

        let plans = list_plans_sorted_by_recency(&conn, "/proj").unwrap();
        let slugs: Vec<&str> = plans.iter().map(|p| p.slug.as_str()).collect();
        assert_eq!(slugs, vec!["p1", "p2"]);
    }

    #[test]
    fn test_list_plans_sorted_by_recency_uses_max_log() {
        // When a plan has multiple logs, the MAX(started_at) wins.
        let conn = setup();

        let p1 = create_plan(&conn, "p1", "/proj", "b1", "d", None, None, &[]).unwrap();
        let p2 = create_plan(&conn, "p2", "/proj", "b2", "d", None, None, &[]).unwrap();

        // p1 has an old log + a fresh log → MAX is fresh.
        let (s1, _) = create_step(
            &conn,
            &p1.id,
            "s1",
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
        let l_old = create_execution_log(&conn, &s1.id, 1, None, None).unwrap();
        conn.execute(
            "UPDATE execution_logs SET started_at = ?1 WHERE id = ?2",
            params!["2026-01-01T00:00:00.000Z", l_old.id],
        )
        .unwrap();
        let l_new = create_execution_log(&conn, &s1.id, 2, None, None).unwrap();
        conn.execute(
            "UPDATE execution_logs SET started_at = ?1 WHERE id = ?2",
            params!["2026-06-01T00:00:00.000Z", l_new.id],
        )
        .unwrap();

        // p2 has one log between p1's old and new.
        let (s2, _) = create_step(
            &conn,
            &p2.id,
            "s2",
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
        let l_p2 = create_execution_log(&conn, &s2.id, 1, None, None).unwrap();
        conn.execute(
            "UPDATE execution_logs SET started_at = ?1 WHERE id = ?2",
            params!["2026-03-01T00:00:00.000Z", l_p2.id],
        )
        .unwrap();

        let plans = list_plans_sorted_by_recency(&conn, "/proj").unwrap();
        let slugs: Vec<&str> = plans.iter().map(|p| p.slug.as_str()).collect();
        assert_eq!(slugs, vec!["p1", "p2"]);
    }

    #[test]
    fn test_list_plans_sorted_by_recency_excludes_archived_and_other_projects() {
        let conn = setup();

        let _own = create_plan(&conn, "own", "/proj", "b1", "d", None, None, &[]).unwrap();
        let archived = create_plan(&conn, "archived", "/proj", "b2", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &archived.id, PlanStatus::Archived).unwrap();
        let _other = create_plan(&conn, "other", "/elsewhere", "b3", "d", None, None, &[]).unwrap();

        let plans = list_plans_sorted_by_recency(&conn, "/proj").unwrap();
        let slugs: Vec<&str> = plans.iter().map(|p| p.slug.as_str()).collect();
        assert_eq!(slugs, vec!["own"]);
    }

    #[test]
    fn test_list_archived_plans_sorted_by_recency_only_returns_archived() {
        let conn = setup();

        let active = create_plan(&conn, "active", "/proj", "b1", "d", None, None, &[]).unwrap();
        let arch_a = create_plan(&conn, "arch-a", "/proj", "b2", "d", None, None, &[]).unwrap();
        let arch_b = create_plan(&conn, "arch-b", "/proj", "b3", "d", None, None, &[]).unwrap();
        let other = create_plan(&conn, "other", "/elsewhere", "b4", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &arch_a.id, PlanStatus::Archived).unwrap();
        update_plan_status(&conn, &arch_b.id, PlanStatus::Archived).unwrap();
        update_plan_status(&conn, &other.id, PlanStatus::Archived).unwrap();

        let plans = list_archived_plans_sorted_by_recency(&conn, "/proj").unwrap();
        let slugs: Vec<&str> = plans.iter().map(|p| p.slug.as_str()).collect();
        // Both /proj archived plans, but not the active one or the other-project one.
        assert!(slugs.contains(&"arch-a"));
        assert!(slugs.contains(&"arch-b"));
        assert!(!slugs.contains(&"active"));
        assert!(!slugs.contains(&"other"));
        assert_eq!(slugs.len(), 2);
        let _ = active;
    }

    #[test]
    fn test_count_archived_plans() {
        let conn = setup();

        assert_eq!(count_archived_plans(&conn, "/proj").unwrap(), 0);

        let p1 = create_plan(&conn, "p1", "/proj", "b1", "d", None, None, &[]).unwrap();
        let p2 = create_plan(&conn, "p2", "/proj", "b2", "d", None, None, &[]).unwrap();
        let p3 = create_plan(&conn, "p3", "/proj", "b3", "d", None, None, &[]).unwrap();
        // Different project — must not count.
        let other = create_plan(&conn, "other", "/elsewhere", "b4", "d", None, None, &[]).unwrap();

        // p1 still planning; only p2 and p3 archived.
        update_plan_status(&conn, &p2.id, PlanStatus::Archived).unwrap();
        update_plan_status(&conn, &p3.id, PlanStatus::Archived).unwrap();
        update_plan_status(&conn, &other.id, PlanStatus::Archived).unwrap();
        let _ = p1;

        assert_eq!(count_archived_plans(&conn, "/proj").unwrap(), 2);
        assert_eq!(count_archived_plans(&conn, "/elsewhere").unwrap(), 1);
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
    fn test_set_plan_questions_enabled_flips_column() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        assert!(plan.questions_enabled, "new plans default to on");

        set_plan_questions_enabled(&conn, &plan.id, false).unwrap();
        let off = get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert!(!off.questions_enabled);
        assert!(off.updated_at >= plan.updated_at);

        set_plan_questions_enabled(&conn, &plan.id, true).unwrap();
        let on = get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert!(on.questions_enabled);
    }

    /// STEP 44 / docs/dag-redesign.md §13.3: `any_review_enabled` drives
    /// the doctor review-harness warning. It is true iff some plan OR step
    /// has `review_enabled = 1`; an explicit `Some(false)` or NULL/inherit
    /// must NOT count (only a truthy override means review is actually on).
    #[test]
    fn test_any_review_enabled_detects_plan_or_step_truthy_only() {
        let conn = setup();
        // Fresh DB: nothing enables review.
        assert!(!any_review_enabled(&conn).unwrap());

        let plan = create_plan(&conn, "rv", "/p", "b", "d", None, None, &[]).unwrap();
        let (step, _) =
            create_step(&conn, &plan.id, "s", "d", None, None, &[], None, None, None, None)
                .unwrap();
        // Defaults (NULL/inherit) ⇒ still false.
        assert!(!any_review_enabled(&conn).unwrap());

        // Explicit OFF on both ⇒ still false (not a truthy override).
        set_plan_review_enabled(&conn, &plan.id, Some(false)).unwrap();
        set_step_review_enabled(&conn, &step.id, Some(false)).unwrap();
        assert!(!any_review_enabled(&conn).unwrap());

        // Step ON ⇒ true.
        set_step_review_enabled(&conn, &step.id, Some(true)).unwrap();
        assert!(any_review_enabled(&conn).unwrap());

        // Step back to inherit, plan ON ⇒ true (plan side of the OR).
        set_step_review_enabled(&conn, &step.id, None).unwrap();
        set_plan_review_enabled(&conn, &plan.id, Some(true)).unwrap();
        assert!(any_review_enabled(&conn).unwrap());

        // Everything back to inherit ⇒ false again.
        set_plan_review_enabled(&conn, &plan.id, None).unwrap();
        assert!(!any_review_enabled(&conn).unwrap());
    }

    #[test]
    fn test_set_plan_pause_requested_round_trips() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        assert!(!plan.pause_requested, "default should be false");
        assert!(!get_plan_pause_requested(&conn, &plan.id).unwrap());

        set_plan_pause_requested(&conn, &plan.id, true).unwrap();
        let on = get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert!(on.pause_requested);
        assert!(get_plan_pause_requested(&conn, &plan.id).unwrap());
        assert!(on.updated_at >= plan.updated_at);

        set_plan_pause_requested(&conn, &plan.id, false).unwrap();
        let off = get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert!(!off.pause_requested);
        assert!(!get_plan_pause_requested(&conn, &plan.id).unwrap());
    }

    #[test]
    fn test_take_plan_pause_requested_clears_flag_atomically() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        // Unset → take returns false, flag stays cleared.
        assert!(!take_plan_pause_requested(&conn, &plan.id).unwrap());
        assert!(!get_plan_pause_requested(&conn, &plan.id).unwrap());

        // Set, then take → returns true and clears the flag in one shot so
        // the runner's between-step check is one-shot per request.
        set_plan_pause_requested(&conn, &plan.id, true).unwrap();
        assert!(take_plan_pause_requested(&conn, &plan.id).unwrap());
        assert!(
            !get_plan_pause_requested(&conn, &plan.id).unwrap(),
            "take must clear the flag",
        );
        // Subsequent take returns false (idempotent on a cleared flag).
        assert!(!take_plan_pause_requested(&conn, &plan.id).unwrap());
    }

    #[test]
    fn test_set_plan_pause_requested_missing_plan_errs() {
        let conn = setup();
        let err = set_plan_pause_requested(&conn, "no-such-id", true).unwrap_err();
        assert!(err.to_string().contains("Plan not found"));
    }

    // -- cross-process skip bridge (V23) --

    #[test]
    fn test_request_skip_round_trips_and_take_clears_atomically() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        // No pending skip → take returns None, peek returns None.
        assert!(take_skip_request(&conn, &plan.id).unwrap().is_none());
        assert!(peek_skip_request(&conn, &plan.id).unwrap().is_none());

        // Request a skip; it must be visible on the plan row and via peek
        // WITHOUT being consumed.
        request_skip(
            &conn,
            &plan.id,
            "step-uuid-1",
            crate::git::ParkStrategyKind::Commit,
        )
        .unwrap();
        let on = get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert_eq!(on.skip_requested_step_id.as_deref(), Some("step-uuid-1"));
        assert_eq!(on.skip_changes.as_deref(), Some("commit"));
        assert!(on.updated_at >= plan.updated_at);

        let peeked = peek_skip_request(&conn, &plan.id).unwrap();
        assert_eq!(
            peeked,
            Some((
                "step-uuid-1".to_string(),
                crate::git::ParkStrategyKind::Commit
            ))
        );
        // Peek must NOT clear.
        assert!(peek_skip_request(&conn, &plan.id).unwrap().is_some());

        // take returns it and clears in one shot.
        let taken = take_skip_request(&conn, &plan.id).unwrap();
        assert_eq!(
            taken,
            Some((
                "step-uuid-1".to_string(),
                crate::git::ParkStrategyKind::Commit
            ))
        );
        assert!(
            take_skip_request(&conn, &plan.id).unwrap().is_none(),
            "take must read-and-clear so the runner consumes a request once"
        );
        assert!(peek_skip_request(&conn, &plan.id).unwrap().is_none());
    }

    #[test]
    fn test_request_skip_overwrites_prior_and_unknown_token_defaults_stash() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        request_skip(
            &conn,
            &plan.id,
            "step-A",
            crate::git::ParkStrategyKind::Discard,
        )
        .unwrap();
        // A fresh request supersedes the stale one (last-writer-wins).
        request_skip(
            &conn,
            &plan.id,
            "step-B",
            crate::git::ParkStrategyKind::Cancel,
        )
        .unwrap();
        let peeked = peek_skip_request(&conn, &plan.id).unwrap();
        assert_eq!(
            peeked,
            Some(("step-B".to_string(), crate::git::ParkStrategyKind::Cancel))
        );

        // A corrupt / forward-compat skip_changes token resolves to the
        // non-destructive Stash default so a skip never silently loses work.
        conn.execute(
            "UPDATE plans SET skip_changes = 'bogus' WHERE id = ?1",
            params![plan.id],
        )
        .unwrap();
        let (_sid, kind) = take_skip_request(&conn, &plan.id).unwrap().unwrap();
        assert_eq!(kind, crate::git::ParkStrategyKind::Stash);
    }

    #[test]
    fn test_clear_skip_request_is_idempotent_noop_when_empty() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        // No-op on an empty slot.
        clear_skip_request(&conn, &plan.id).unwrap();
        assert!(peek_skip_request(&conn, &plan.id).unwrap().is_none());

        request_skip(&conn, &plan.id, "x", crate::git::ParkStrategyKind::Stash).unwrap();
        clear_skip_request(&conn, &plan.id).unwrap();
        assert!(
            peek_skip_request(&conn, &plan.id).unwrap().is_none(),
            "clear must drop a pending request without consuming it via take"
        );
    }

    #[test]
    fn test_take_skip_request_for_step_is_targeted_and_atomic() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();

        // Nothing pending → None for any step.
        assert!(
            take_skip_request_for_step(&conn, &plan.id, "step-A")
                .unwrap()
                .is_none()
        );

        // A request targeting step-B must NOT be consumed when the in-flight
        // step is step-A (this is the TOCTOU the targeted take closes: the
        // old peek-then-take would have cleared and discarded it here).
        request_skip(
            &conn,
            &plan.id,
            "step-B",
            crate::git::ParkStrategyKind::Commit,
        )
        .unwrap();
        assert!(
            take_skip_request_for_step(&conn, &plan.id, "step-A")
                .unwrap()
                .is_none(),
            "a request for a different step must be left untouched"
        );
        // …and it's still pending for step-B to honor when it runs.
        assert_eq!(
            peek_skip_request(&conn, &plan.id).unwrap(),
            Some(("step-B".to_string(), crate::git::ParkStrategyKind::Commit))
        );

        // The matching step consumes-and-clears in one shot.
        assert_eq!(
            take_skip_request_for_step(&conn, &plan.id, "step-B").unwrap(),
            Some(crate::git::ParkStrategyKind::Commit)
        );
        assert!(
            take_skip_request_for_step(&conn, &plan.id, "step-B")
                .unwrap()
                .is_none(),
            "take must read-and-clear so a request is consumed exactly once"
        );
        assert!(peek_skip_request(&conn, &plan.id).unwrap().is_none());

        // Corrupt / forward-compat token resolves to the non-destructive
        // Stash default, same contract as take_skip_request.
        request_skip(
            &conn,
            &plan.id,
            "step-C",
            crate::git::ParkStrategyKind::Discard,
        )
        .unwrap();
        conn.execute(
            "UPDATE plans SET skip_changes = 'bogus' WHERE id = ?1",
            params![plan.id],
        )
        .unwrap();
        assert_eq!(
            take_skip_request_for_step(&conn, &plan.id, "step-C").unwrap(),
            Some(crate::git::ParkStrategyKind::Stash)
        );
    }

    #[test]
    fn test_request_skip_missing_plan_errs() {
        let conn = setup();
        let err = request_skip(
            &conn,
            "no-such-id",
            "step",
            crate::git::ParkStrategyKind::Stash,
        )
        .unwrap_err();
        assert!(err.to_string().contains("Plan not found"));
    }

    #[test]
    fn test_set_plan_last_run_branch_round_trips() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/p", "b", "d", None, None, &[]).unwrap();
        assert!(plan.last_run_branch.is_none());

        set_plan_last_run_branch(&conn, &plan.id, "feature/x").unwrap();
        let on = get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert_eq!(on.last_run_branch.as_deref(), Some("feature/x"));
        assert!(on.updated_at >= plan.updated_at);

        // Subsequent writes overwrite (records the most recent run's branch).
        set_plan_last_run_branch(&conn, &plan.id, "master").unwrap();
        let updated = get_plan_by_slug(&conn, "s", "/p").unwrap().unwrap();
        assert_eq!(updated.last_run_branch.as_deref(), Some("master"));
    }

    #[test]
    fn test_set_plan_last_run_branch_missing_plan_errs() {
        let conn = setup();
        let err = set_plan_last_run_branch(&conn, "no-such-id", "master").unwrap_err();
        assert!(err.to_string().contains("Plan not found"));
    }

    #[test]
    fn test_find_resumable_plans_for_branch_orders_by_last_run_started_at_desc() {
        let conn = setup();
        // Three resumable plans on master; stamp last_run_started_at via
        // set_plan_last_run_branch in p1 → p2 → p3 order so DESC reflects
        // insertion order.
        let p1 = create_plan(&conn, "p1", "/proj", "b1", "d", None, None, &[]).unwrap();
        let p2 = create_plan(&conn, "p2", "/proj", "b2", "d", None, None, &[]).unwrap();
        let p3 = create_plan(&conn, "p3", "/proj", "b3", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &p1.id, PlanStatus::Failed).unwrap();
        update_plan_status(&conn, &p2.id, PlanStatus::InProgress).unwrap();
        update_plan_status(&conn, &p3.id, PlanStatus::Aborted).unwrap();
        // Set last_run_branch in p1 → p2 → p3 order. SQLite's strftime
        // truncates to milliseconds, so add a small sleep between writes
        // to guarantee the timestamps differ.
        set_plan_last_run_branch(&conn, &p1.id, "master").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        set_plan_last_run_branch(&conn, &p2.id, "master").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        set_plan_last_run_branch(&conn, &p3.id, "master").unwrap();

        let candidates = find_resumable_plans_for_branch(&conn, "/proj", "master").unwrap();
        let slugs: Vec<&str> = candidates.iter().map(|p| p.slug.as_str()).collect();
        assert_eq!(slugs, vec!["p3", "p2", "p1"], "DESC by last_run_started_at");
    }

    /// Regression for finding 3: an unrelated edit that bumps
    /// `updated_at` (e.g. toggling a flag) on an older plan must NOT
    /// re-rank it above a more recently *run* plan. The order is
    /// anchored on `last_run_started_at`, which only the runner writes.
    #[test]
    fn test_find_resumable_plans_orders_by_run_time_not_updated_at() {
        let conn = setup();
        let stale = create_plan(&conn, "stale", "/proj", "b", "d", None, None, &[]).unwrap();
        let fresh = create_plan(&conn, "fresh", "/proj", "b", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &stale.id, PlanStatus::Failed).unwrap();
        update_plan_status(&conn, &fresh.id, PlanStatus::Failed).unwrap();

        // Real run order: stale ran first, fresh ran second.
        set_plan_last_run_branch(&conn, &stale.id, "master").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        set_plan_last_run_branch(&conn, &fresh.id, "master").unwrap();

        // Now bump `stale.updated_at` via an unrelated flag toggle. Under
        // the OLD `ORDER BY updated_at DESC`, this would put `stale`
        // first; under the new ordering anchored on
        // `last_run_started_at`, `fresh` still wins.
        std::thread::sleep(std::time::Duration::from_millis(5));
        set_plan_questions_enabled(&conn, &stale.id, true).unwrap();

        let candidates = find_resumable_plans_for_branch(&conn, "/proj", "master").unwrap();
        let slugs: Vec<&str> = candidates.iter().map(|p| p.slug.as_str()).collect();
        assert_eq!(
            slugs,
            vec!["fresh", "stale"],
            "ordering must follow last_run_started_at, not updated_at"
        );
    }

    #[test]
    fn test_set_plan_last_run_branch_stamps_last_run_started_at() {
        let conn = setup();
        let plan = create_plan(&conn, "s", "/proj", "b", "d", None, None, &[]).unwrap();
        assert!(plan.last_run_started_at.is_none());

        set_plan_last_run_branch(&conn, &plan.id, "master").unwrap();
        let after = get_plan_by_slug(&conn, "s", "/proj").unwrap().unwrap();
        assert!(
            after.last_run_started_at.is_some(),
            "set_plan_last_run_branch must also stamp last_run_started_at"
        );
    }

    #[test]
    fn test_find_resumable_plan_returns_aborted_in_any_branch_context() {
        let conn = setup();
        // No matching branch row, but Aborted plan must still come back.
        let plan = create_plan(&conn, "ab", "/proj", "any", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &plan.id, PlanStatus::Aborted).unwrap();

        let p = find_resumable_plan(&conn, "/proj").unwrap();
        assert!(p.is_some());
        assert_eq!(p.unwrap().slug, "ab");
    }

    #[test]
    fn test_find_resumable_plan_excludes_complete_and_planning() {
        let conn = setup();
        // Planning (default), complete, archived must not be returned.
        let _planning = create_plan(&conn, "pl", "/proj", "b", "d", None, None, &[]).unwrap();
        let cp = create_plan(&conn, "cp", "/proj", "b", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &cp.id, PlanStatus::Complete).unwrap();
        let ar = create_plan(&conn, "ar", "/proj", "b", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &ar.id, PlanStatus::Archived).unwrap();

        let p = find_resumable_plan(&conn, "/proj").unwrap();
        assert!(p.is_none());
    }

    #[test]
    fn test_find_resumable_plan_orders_by_run_time_not_updated_at() {
        let conn = setup();
        let stale = create_plan(&conn, "stale", "/proj", "b", "d", None, None, &[]).unwrap();
        let fresh = create_plan(&conn, "fresh", "/proj", "b", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &stale.id, PlanStatus::Failed).unwrap();
        update_plan_status(&conn, &fresh.id, PlanStatus::Aborted).unwrap();

        set_plan_last_run_branch(&conn, &stale.id, "main").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        set_plan_last_run_branch(&conn, &fresh.id, "main").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(5));
        // Bumping unrelated flag on the older plan must not change ranking.
        set_plan_questions_enabled(&conn, &stale.id, true).unwrap();

        let p = find_resumable_plan(&conn, "/proj").unwrap().unwrap();
        assert_eq!(p.slug, "fresh");
    }

    #[test]
    fn test_find_resumable_plans_for_branch_excludes_completed_and_planning() {
        let conn = setup();
        let resumable_statuses = [
            PlanStatus::InProgress,
            PlanStatus::Failed,
            PlanStatus::Aborted,
            PlanStatus::Ready,
        ];
        for (i, status) in resumable_statuses.iter().enumerate() {
            let slug = format!("rp{i}");
            let plan = create_plan(&conn, &slug, "/proj", "main", "d", None, None, &[]).unwrap();
            update_plan_status(&conn, &plan.id, *status).unwrap();
            set_plan_last_run_branch(&conn, &plan.id, "main").unwrap();
        }
        // Non-resumable: planning (default), complete, archived.
        let _planning = create_plan(&conn, "pl", "/proj", "main", "d", None, None, &[]).unwrap();
        let cp = create_plan(&conn, "cp", "/proj", "main", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &cp.id, PlanStatus::Complete).unwrap();
        set_plan_last_run_branch(&conn, &cp.id, "main").unwrap();
        let ar = create_plan(&conn, "ar", "/proj", "main", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &ar.id, PlanStatus::Archived).unwrap();
        set_plan_last_run_branch(&conn, &ar.id, "main").unwrap();

        let candidates = find_resumable_plans_for_branch(&conn, "/proj", "main").unwrap();
        let slugs: std::collections::HashSet<&str> =
            candidates.iter().map(|p| p.slug.as_str()).collect();
        assert_eq!(
            slugs,
            ["rp0", "rp1", "rp2", "rp3"].iter().copied().collect(),
            "only InProgress/Failed/Aborted/Ready should match"
        );
    }

    #[test]
    fn test_find_resumable_plans_for_branch_scopes_to_project() {
        let conn = setup();
        let here = create_plan(&conn, "here", "/proj", "main", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &here.id, PlanStatus::Failed).unwrap();
        set_plan_last_run_branch(&conn, &here.id, "main").unwrap();
        let elsewhere =
            create_plan(&conn, "elsewhere", "/other", "main", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &elsewhere.id, PlanStatus::Failed).unwrap();
        set_plan_last_run_branch(&conn, &elsewhere.id, "main").unwrap();

        let candidates = find_resumable_plans_for_branch(&conn, "/proj", "main").unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].slug, "here");
    }

    #[test]
    fn test_find_resumable_plans_never_run_uses_branch_name_fallback() {
        // A plan that has never run (last_run_branch IS NULL) should still
        // match when current_branch == branch_name.
        let conn = setup();
        let plan = create_plan(&conn, "fresh", "/proj", "feat-x", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &plan.id, PlanStatus::Ready).unwrap();
        assert!(plan.last_run_branch.is_none());

        let on_match = find_resumable_plans_for_branch(&conn, "/proj", "feat-x").unwrap();
        assert_eq!(on_match.len(), 1);
        assert_eq!(on_match[0].slug, "fresh");

        // ...but NOT when the current branch differs from branch_name.
        let on_other = find_resumable_plans_for_branch(&conn, "/proj", "feat-y").unwrap();
        assert!(on_other.is_empty());
    }

    #[test]
    fn test_find_resumable_plans_no_false_match_when_last_run_branch_set() {
        // Slug-collision hard test from the step description.
        // Plan A: slug='deploy', branch_name='deploy'. Last run was on
        // 'master' (--current-branch mode), captured in last_run_branch.
        // After the user creates a new branch 'deploy', `ralph resume` on
        // that branch must NOT match A — A's last_run_branch='master' is
        // set, so the NULL+branch_name fallback is skipped.
        let conn = setup();
        let a = create_plan(&conn, "deploy", "/proj", "deploy", "d", None, None, &[]).unwrap();
        update_plan_status(&conn, &a.id, PlanStatus::Failed).unwrap();
        set_plan_last_run_branch(&conn, &a.id, "master").unwrap();

        // Sanity: A.branch_name == 'deploy' but last_run_branch == 'master'.
        let reread = get_plan_by_slug(&conn, "deploy", "/proj").unwrap().unwrap();
        assert_eq!(reread.branch_name, "deploy");
        assert_eq!(reread.last_run_branch.as_deref(), Some("master"));

        // On 'deploy': must NOT match A (the false-match the V19 column
        // exists to prevent).
        let on_deploy = find_resumable_plans_for_branch(&conn, "/proj", "deploy").unwrap();
        assert!(
            on_deploy.is_empty(),
            "must NOT match plan whose last_run_branch differs from current branch even if branch_name collides"
        );

        // On 'master': matches A (the actual run home).
        let on_master = find_resumable_plans_for_branch(&conn, "/proj", "master").unwrap();
        let slugs: Vec<&str> = on_master.iter().map(|p| p.slug.as_str()).collect();
        assert_eq!(slugs, vec!["deploy"]);
    }

    // `test_list_answered_questions_for_step_*` were removed in the §8/§4
    // cutover (the unbounded prompt feed they covered is gone). The bounded
    // replacement (`list_resolved_interruptions_for_step`) is exercised by
    // `test_list_resolved_interruptions_for_step_is_bounded_and_newest_first`
    // and the round-trip tests above.

    #[test]
    fn test_count_unanswered_questions_for_attempt_scopes_by_step_and_attempt() {
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

        // Different combinations of (attempt, open/resolved state) so we can
        // verify the scoping. Only attempt=2 still-open should count. Native
        // `interruptions` rows — no `step_questions`.
        let q1 =
            insert_interruption(&conn, &step.id, 1, InterruptionKind::Question, "a1", &[]).unwrap();
        resolve_interruption(&conn, &q1, "done", None).unwrap();
        insert_interruption(
            &conn,
            &step.id,
            1,
            InterruptionKind::Question,
            "old open",
            &[],
        )
        .unwrap();
        insert_interruption(
            &conn,
            &step.id,
            2,
            InterruptionKind::Question,
            "current open A",
            &[],
        )
        .unwrap();
        // A blocker on attempt 2 must also count (the bridge pauses on
        // questions OR blockers — docs/dag-redesign.md §7).
        insert_interruption(
            &conn,
            &step.id,
            2,
            InterruptionKind::Blocker,
            "current open B",
            &[],
        )
        .unwrap();

        assert_eq!(
            count_unanswered_questions_for_attempt(&conn, &step.id, 2).unwrap(),
            2,
            "two open interruptions for attempt=2",
        );
        assert_eq!(
            count_unanswered_questions_for_attempt(&conn, &step.id, 1).unwrap(),
            1,
            "one open interruption for attempt=1 (the resolved one is excluded)",
        );
        assert_eq!(
            count_unanswered_questions_for_attempt(&conn, &step.id, 99).unwrap(),
            0,
            "no rows for an attempt that doesn't exist",
        );
    }

    #[test]
    fn test_plan_effective_status_returns_interrupted_when_open_interruption_exists() {
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
        // Set the underlying lifecycle to in_progress so we can verify the
        // derived status overrides it.
        update_plan_status(&conn, &plan.id, PlanStatus::InProgress).unwrap();

        // An open *question* (native `interruptions` row) derives
        // Interrupted.
        let q1 = insert_interruption(&conn, &step.id, 1, InterruptionKind::Question, "open?", &[])
            .unwrap();

        assert_eq!(
            plan_effective_status(&conn, &plan.id).unwrap(),
            PlanStatus::Interrupted,
            "an open interruption must shadow the underlying lifecycle"
        );

        // Resolve it, then prove a *blocker* also derives Interrupted — the
        // §3.4/§6 unification: question OR blocker interrupts the plan.
        resolve_interruption(&conn, &q1, "answered", None).unwrap();
        assert_eq!(
            plan_effective_status(&conn, &plan.id).unwrap(),
            PlanStatus::InProgress,
            "resolving the only open interruption un-shadows the lifecycle"
        );

        insert_interruption(
            &conn,
            &step.id,
            1,
            InterruptionKind::Blocker,
            "needs sudo",
            &[],
        )
        .unwrap();
        assert_eq!(
            plan_effective_status(&conn, &plan.id).unwrap(),
            PlanStatus::Interrupted,
            "an open blocker (not just a question) must derive Interrupted"
        );
    }

    #[test]
    fn test_plan_effective_status_returns_underlying_when_no_open_questions() {
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
        update_plan_status(&conn, &plan.id, PlanStatus::Complete).unwrap();

        // Resolved interruptions must not trigger the Interrupted shadow.
        let q1 = insert_interruption(&conn, &step.id, 1, InterruptionKind::Question, "old?", &[])
            .unwrap();
        resolve_interruption(&conn, &q1, "yes", None).unwrap();

        assert_eq!(
            plan_effective_status(&conn, &plan.id).unwrap(),
            PlanStatus::Complete,
        );
    }

    #[test]
    fn test_set_plan_questions_enabled_missing_plan_errs() {
        let conn = setup();
        let err = set_plan_questions_enabled(&conn, "no-such-id", true).unwrap_err();
        assert!(err.to_string().contains("Plan not found"));
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

    // -- Step dependency tests --

    /// Create one plan plus `n` steps named s1..sn in it; return the step IDs.
    fn make_steps(conn: &Connection, n: usize) -> Vec<String> {
        let plan = create_plan(conn, "sp", "/proj", "branch", "desc", None, None, &[])
            .expect("create_plan");
        (1..=n)
            .map(|i| {
                create_step(
                    conn,
                    &plan.id,
                    &format!("s{i}"),
                    "d",
                    None,
                    None,
                    &[],
                    None,
                    None,
                    None,
                    None,
                )
                .expect("create_step")
                .0
                .id
            })
            .collect()
    }

    #[test]
    fn test_add_step_dependency_happy_path() {
        let conn = setup();
        let ids = make_steps(&conn, 2);

        add_step_dependency(&conn, &ids[0], &ids[1]).expect("add dep");

        let deps = list_step_dependencies(&conn, &ids[0]).unwrap();
        assert_eq!(deps, vec![ids[1].clone()]);
    }

    #[test]
    fn test_add_step_dependency_rejects_self_reference() {
        let conn = setup();
        let ids = make_steps(&conn, 1);

        let err = add_step_dependency(&conn, &ids[0], &ids[0]).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("cannot depend on itself"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn test_remove_step_dependency() {
        let conn = setup();
        let ids = make_steps(&conn, 2);

        add_step_dependency(&conn, &ids[0], &ids[1]).unwrap();
        assert_eq!(list_step_dependencies(&conn, &ids[0]).unwrap().len(), 1);

        remove_step_dependency(&conn, &ids[0], &ids[1]).unwrap();
        assert!(list_step_dependencies(&conn, &ids[0]).unwrap().is_empty());

        // Removing a non-existent edge is a no-op.
        remove_step_dependency(&conn, &ids[0], &ids[1]).unwrap();
    }

    #[test]
    fn test_list_step_dependencies_and_dependents() {
        let conn = setup();
        let ids = make_steps(&conn, 3);

        // s1 depends on s2 and s3.
        add_step_dependency(&conn, &ids[0], &ids[1]).unwrap();
        add_step_dependency(&conn, &ids[0], &ids[2]).unwrap();

        let mut deps = list_step_dependencies(&conn, &ids[0]).unwrap();
        deps.sort();
        let mut expected = vec![ids[1].clone(), ids[2].clone()];
        expected.sort();
        assert_eq!(deps, expected);

        // s2 and s3 should both see s1 as a dependent.
        let dependents_s2 = list_step_dependents(&conn, &ids[1]).unwrap();
        assert_eq!(dependents_s2, vec![ids[0].clone()]);

        let dependents_s3 = list_step_dependents(&conn, &ids[2]).unwrap();
        assert_eq!(dependents_s3, vec![ids[0].clone()]);

        // s1 has no dependents.
        assert!(list_step_dependents(&conn, &ids[0]).unwrap().is_empty());
    }

    #[test]
    fn test_step_dependency_cascades_on_step_delete() {
        let conn = setup();
        let ids = make_steps(&conn, 2);

        add_step_dependency(&conn, &ids[0], &ids[1]).unwrap();
        assert_eq!(list_step_dependencies(&conn, &ids[0]).unwrap().len(), 1);

        // V25's ON DELETE CASCADE drops dependent edges with the step.
        delete_step(&conn, &ids[1]).unwrap();
        assert!(list_step_dependencies(&conn, &ids[0]).unwrap().is_empty());
    }

    /// Create a plan with `n` steps; return `(plan_id, step_ids)`.
    fn make_plan_with_steps(conn: &Connection, slug: &str, n: usize) -> (String, Vec<String>) {
        let plan = create_plan(conn, slug, "/proj", "branch", "desc", None, None, &[])
            .expect("create_plan");
        let ids = (1..=n)
            .map(|i| {
                create_step(
                    conn,
                    &plan.id,
                    &format!("s{i}"),
                    "d",
                    None,
                    None,
                    &[],
                    None,
                    None,
                    None,
                    None,
                )
                .expect("create_step")
                .0
                .id
            })
            .collect();
        (plan.id, ids)
    }

    #[test]
    fn test_list_step_dependency_edges() {
        let conn = setup();
        let (plan_id, ids) = make_plan_with_steps(&conn, "edges", 4);

        // Diamond: s1 -> s2, s1 -> s3, s2 -> s4, s3 -> s4.
        add_step_dependency(&conn, &ids[0], &ids[1]).unwrap();
        add_step_dependency(&conn, &ids[0], &ids[2]).unwrap();
        add_step_dependency(&conn, &ids[1], &ids[3]).unwrap();
        add_step_dependency(&conn, &ids[2], &ids[3]).unwrap();

        let edges = list_step_dependency_edges(&conn, &plan_id).unwrap();

        let mut s1 = edges.get(&ids[0]).cloned().unwrap_or_default();
        s1.sort();
        let mut expected_s1 = vec![ids[1].clone(), ids[2].clone()];
        expected_s1.sort();
        assert_eq!(s1, expected_s1);
        assert_eq!(edges.get(&ids[1]).cloned().unwrap(), vec![ids[3].clone()]);
        assert_eq!(edges.get(&ids[2]).cloned().unwrap(), vec![ids[3].clone()]);
        // The sink has no outgoing edges → absent from the map.
        assert!(!edges.contains_key(&ids[3]));

        // Edges are plan-scoped: a second plan's edges don't leak in.
        let (_other_plan, other_ids) = make_plan_with_steps(&conn, "other", 2);
        add_step_dependency(&conn, &other_ids[0], &other_ids[1]).unwrap();
        let edges = list_step_dependency_edges(&conn, &plan_id).unwrap();
        assert!(!edges.contains_key(&other_ids[0]));
    }

    #[test]
    fn test_would_create_step_cycle_direct() {
        let conn = setup();
        let ids = make_steps(&conn, 2);

        // Self-edge is always a cycle.
        assert!(would_create_step_cycle(&conn, &ids[0], &ids[0]).unwrap());

        // A -> B. Adding B -> A closes a direct cycle.
        add_step_dependency(&conn, &ids[0], &ids[1]).unwrap();
        assert!(would_create_step_cycle(&conn, &ids[1], &ids[0]).unwrap());
    }

    #[test]
    fn test_would_create_step_cycle_transitive() {
        let conn = setup();
        let ids = make_steps(&conn, 3);

        // A -> B -> C. Adding C -> A would create a 3-node cycle.
        add_step_dependency(&conn, &ids[0], &ids[1]).unwrap();
        add_step_dependency(&conn, &ids[1], &ids[2]).unwrap();

        assert!(would_create_step_cycle(&conn, &ids[2], &ids[0]).unwrap());
    }

    #[test]
    fn test_would_create_step_cycle_no_cycle() {
        let conn = setup();
        let ids = make_steps(&conn, 3);

        // A -> B. Adding A -> C does not create a cycle.
        add_step_dependency(&conn, &ids[0], &ids[1]).unwrap();

        assert!(!would_create_step_cycle(&conn, &ids[0], &ids[2]).unwrap());
    }

    #[test]
    fn test_add_step_dependency_rejects_cycle() {
        let conn = setup();
        let ids = make_steps(&conn, 3);

        // A -> B -> C, then attempt C -> A: rejected before the insert.
        add_step_dependency(&conn, &ids[0], &ids[1]).unwrap();
        add_step_dependency(&conn, &ids[1], &ids[2]).unwrap();

        let err = add_step_dependency(&conn, &ids[2], &ids[0]).unwrap_err();
        let msg = format!("{err:#}");
        assert!(msg.contains("cycle"), "unexpected error: {msg}");

        // The rejected edge was not persisted.
        assert!(list_step_dependencies(&conn, &ids[2]).unwrap().is_empty());
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

    /// A crash *during a concurrent review* (docs/dag-redesign.md §3.5
    /// item 3) leaves a step `InProgress` + `review_status = InFlight`. The
    /// stale-sweep must reset BOTH so the impossible `Aborted` + `InFlight`
    /// combination can never persist; other review verdicts are durable and
    /// must be left untouched.
    #[test]
    fn test_sweep_stale_in_progress_resets_in_flight_review_status() {
        let conn = setup();
        let plan = create_plan(&conn, "sweep", "/proj", "b", "d", None, None, &[]).unwrap();

        // Step A: crashed mid-review (InProgress + InFlight) — the bug case.
        let (a, _) = create_step(
            &conn, &plan.id, "A", "d", None, None, &[], None, None, None, None,
        )
        .unwrap();
        update_step_status(&conn, &a.id, StepStatus::InProgress).unwrap();
        update_step_review_status(&conn, &a.id, crate::plan::ReviewStatus::InFlight).unwrap();

        // Step B: crashed mid-implement, review never started (InProgress +
        // Pending). Sweep flips status; review_status stays Pending.
        let (b, _) = create_step(
            &conn, &plan.id, "B", "d", None, None, &[], None, None, None, None,
        )
        .unwrap();
        update_step_status(&conn, &b.id, StepStatus::InProgress).unwrap();

        // Step C: a *completed, durably-passed* review on a still-Complete
        // step — NOT swept (status != InProgress) and verdict untouched.
        let (c, _) = create_step(
            &conn, &plan.id, "C", "d", None, None, &[], None, None, None, None,
        )
        .unwrap();
        update_step_status(&conn, &c.id, StepStatus::Complete).unwrap();
        update_step_review_status(&conn, &c.id, crate::plan::ReviewStatus::Passed).unwrap();

        let swept = sweep_stale_in_progress(&conn, &plan.id).unwrap();
        assert_eq!(swept.len(), 2, "only A and B were InProgress");

        let a2 = get_step(&conn, &a.id).unwrap();
        assert_eq!(a2.status, StepStatus::Aborted);
        assert_eq!(
            a2.review_status,
            Some(crate::plan::ReviewStatus::Pending),
            "InFlight on a swept step MUST reset to Pending — no \
             Aborted+InFlight (the bug)"
        );

        let b2 = get_step(&conn, &b.id).unwrap();
        assert_eq!(b2.status, StepStatus::Aborted);
        assert_eq!(
            b2.review_status, None,
            "a never-set (on-disk NULL ⇒ semantically Pending) review_status \
             on a swept step is left as NULL — the CASE only rewrites the \
             literal 'in_flight'"
        );

        let c2 = get_step(&conn, &c.id).unwrap();
        assert_eq!(c2.status, StepStatus::Complete, "C was not InProgress");
        assert_eq!(
            c2.review_status,
            Some(crate::plan::ReviewStatus::Passed),
            "a durable Passed verdict must NOT be clobbered by the sweep"
        );

        // The RETURNING snapshot reflects the post-update review_status.
        let swept_a = swept.iter().find(|s| s.id == a.id).unwrap();
        assert_eq!(
            swept_a.review_status,
            Some(crate::plan::ReviewStatus::Pending),
            "the returned row snapshot must show the reset review_status"
        );
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
    fn test_set_plan_deterministic_tests_round_trip() {
        let conn = setup();
        let plan = create_plan(
            &conn,
            "tests-rt",
            "/proj",
            "b",
            "d",
            None,
            None,
            &["cargo build".to_string()],
        )
        .unwrap();

        // Sanity: row was created with the seeded list.
        assert_eq!(plan.deterministic_tests, vec!["cargo build".to_string()]);

        // Replace with a multi-test list.
        let new_tests = vec!["cargo test".to_string(), "cargo clippy".to_string()];
        set_plan_deterministic_tests(&conn, &plan.id, &new_tests).unwrap();
        let reloaded = get_plan_by_slug(&conn, "tests-rt", "/proj")
            .unwrap()
            .unwrap();
        assert_eq!(reloaded.deterministic_tests, new_tests);

        // Empty slice clears the list.
        set_plan_deterministic_tests(&conn, &plan.id, &[]).unwrap();
        let reloaded = get_plan_by_slug(&conn, "tests-rt", "/proj")
            .unwrap()
            .unwrap();
        assert!(reloaded.deterministic_tests.is_empty());
    }

    #[test]
    fn test_set_plan_deterministic_tests_unknown_plan_errors() {
        let conn = setup();
        let err = set_plan_deterministic_tests(&conn, "no-such-plan", &[]).unwrap_err();
        assert!(
            err.to_string().contains("Plan not found"),
            "unexpected error: {err}"
        );
    }

    // -- Project-scope prompt: file-vs-db precedence --

    /// `<project>/.ralph/prompt.md` content wins over the DB column on read.
    #[test]
    fn test_project_prompt_file_wins_over_db() {
        let conn = setup();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();

        set_project_prompt(&conn, &project, Some("from db")).unwrap();
        write_project_prompt_file(&project, "from file").unwrap();

        let (settings, source) = resolve_project_prompt(&conn, &project).unwrap();
        assert_eq!(settings.prompt.as_deref(), Some("from file"));
        assert!(matches!(source, ProjectPromptSource::File(_)));
        // The central assembly read also returns the file content.
        assert_eq!(
            get_project_settings(&conn, &project)
                .unwrap()
                .prompt
                .as_deref(),
            Some("from file")
        );
    }

    /// File absent → DB column is used and the source is `Db`.
    #[test]
    fn test_project_prompt_db_used_when_file_absent() {
        let conn = setup();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();

        set_project_prompt(&conn, &project, Some("only db")).unwrap();

        let (settings, source) = resolve_project_prompt(&conn, &project).unwrap();
        assert_eq!(settings.prompt.as_deref(), Some("only db"));
        assert_eq!(source, ProjectPromptSource::Db);
    }

    /// An empty / whitespace-only file must NOT shadow a valid DB value.
    #[test]
    fn test_project_prompt_blank_file_does_not_shadow_db() {
        let conn = setup();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();

        set_project_prompt(&conn, &project, Some("real db value")).unwrap();
        write_project_prompt_file(&project, "   \n\t  \n").unwrap();

        let (settings, source) = resolve_project_prompt(&conn, &project).unwrap();
        assert_eq!(settings.prompt.as_deref(), Some("real db value"));
        assert_eq!(source, ProjectPromptSource::Db);
        assert!(read_project_prompt_file(&project).unwrap().is_none());
    }

    /// Nothing configured anywhere → `None` / `Db`.
    #[test]
    fn test_project_prompt_none_when_unconfigured() {
        let conn = setup();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();

        let (settings, source) = resolve_project_prompt(&conn, &project).unwrap();
        assert_eq!(settings.prompt, None);
        assert_eq!(source, ProjectPromptSource::Db);
    }

    /// Write helper creates `.ralph/` and the file with exact content;
    /// delete helper removes it and is idempotent on a missing file.
    #[test]
    fn test_project_prompt_file_write_and_delete() {
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();

        write_project_prompt_file(&project, "hello").unwrap();
        let path = project_prompt_file_path(&project);
        assert!(path.exists());
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "hello");

        delete_project_prompt_file(&project).unwrap();
        assert!(!path.exists());
        // Idempotent: deleting a now-missing file is benign.
        delete_project_prompt_file(&project).unwrap();
    }

    /// A `.ralph/prompt.md` that is actually a *directory* must not abort
    /// the run: the read degrades to "absent" so the DB value is used.
    #[test]
    fn test_project_prompt_dir_at_path_falls_back_to_db() {
        let conn = setup();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();

        set_project_prompt(&conn, &project, Some("db survives")).unwrap();

        // Create `<project>/.ralph/prompt.md` as a directory.
        let path = project_prompt_file_path(&project);
        std::fs::create_dir_all(&path).unwrap();
        assert!(path.is_dir());

        // No error, treated as absent.
        assert!(read_project_prompt_file(&project).unwrap().is_none());

        let (settings, source) = resolve_project_prompt(&conn, &project).unwrap();
        assert_eq!(settings.prompt.as_deref(), Some("db survives"));
        assert_eq!(source, ProjectPromptSource::Db);
        // The central assembly read also degrades gracefully.
        assert_eq!(
            get_project_settings(&conn, &project)
                .unwrap()
                .prompt
                .as_deref(),
            Some("db survives")
        );
    }

    /// `get_project_settings_db` ignores the file even when it exists.
    #[test]
    fn test_get_project_settings_db_ignores_file() {
        let conn = setup();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();

        set_project_prompt(&conn, &project, Some("db only")).unwrap();
        write_project_prompt_file(&project, "file wins on resolve").unwrap();

        assert_eq!(
            get_project_settings_db(&conn, &project)
                .unwrap()
                .prompt
                .as_deref(),
            Some("db only")
        );
    }

    /// `STEP_COLUMNS` must enumerate columns in the order SQLite stores them
    /// so `Step::from_row` indices line up even under `SELECT *`. Mirrors
    /// `test_plan_columns_matches_physical_table_order` for the steps table —
    /// added when step 21 appended `retry_strategy` to `STEP_COLUMNS`.
    #[test]
    fn test_step_columns_matches_physical_table_order() {
        let conn = setup();
        let physical: Vec<String> = conn
            .prepare("SELECT * FROM steps LIMIT 0")
            .expect("prepare")
            .column_names()
            .into_iter()
            .map(String::from)
            .collect();
        let canonical: Vec<&str> = STEP_COLUMNS.split(", ").collect();
        assert_eq!(
            physical.iter().map(String::as_str).collect::<Vec<_>>(),
            canonical,
            "STEP_COLUMNS drifted from the physical steps table layout"
        );
    }

    // -- interruption CRUD (native `interruptions` table, V26) --

    /// Create a plan + one step and return the step id.
    fn step_for_interruptions(conn: &Connection) -> String {
        let plan = create_plan(conn, "intr", "/proj", "b", "d", None, None, &[]).unwrap();
        let (step, _) = create_step(
            conn,
            &plan.id,
            "Step A",
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
        step.id
    }

    /// Force a specific `resolved_at` (and `asked_at`) on a row so ordering
    /// assertions don't race the `strftime('now')` clock when many rows are
    /// resolved within the same millisecond.
    fn stamp_resolved_at(conn: &Connection, id: &str, ts: &str) {
        conn.execute(
            "UPDATE interruptions SET resolved_at = ?1, asked_at = ?1 WHERE id = ?2",
            params![ts, id],
        )
        .unwrap();
    }

    #[test]
    fn test_insert_interruption_round_trips_options_state() {
        let conn = setup();
        let step_id = step_for_interruptions(&conn);

        let opts = vec![
            InterruptionOption {
                text: "Use OAuth".to_string(),
                priority: 1,
            },
            InterruptionOption {
                text: "Use SAML".to_string(),
                priority: 2,
            },
        ];
        let id = insert_interruption(
            &conn,
            &step_id,
            3,
            InterruptionKind::Question,
            "Which auth?",
            &opts,
        )
        .unwrap();

        let all = list_interruptions_for_step(&conn, &step_id).unwrap();
        assert_eq!(all.len(), 1);
        let i = &all[0];
        assert_eq!(i.id, id);
        assert_eq!(i.step_id, step_id);
        assert_eq!(i.attempt, 3);
        assert_eq!(i.kind, InterruptionKind::Question);
        assert_eq!(i.body, "Which auth?");
        assert_eq!(i.options, opts, "options must round-trip verbatim");
        assert_eq!(i.state, InterruptionState::Open);
        assert_eq!(i.resolution, None);
        assert_eq!(i.comment, None);
        assert_eq!(i.resolved_at, None);

        // A fresh interruption is open ⇒ visible in the open lists, absent
        // from the bounded resolved list.
        assert_eq!(
            list_open_interruptions(&conn, "/proj", None).unwrap().len(),
            1
        );
        assert_eq!(
            list_open_interruptions_for_plan(
                &conn,
                &conn
                    .query_row(
                        "SELECT plan_id FROM steps WHERE id = ?1",
                        params![step_id],
                        |r| r.get::<_, String>(0)
                    )
                    .unwrap()
            )
            .unwrap()
            .len(),
            1
        );
        assert!(
            list_resolved_interruptions_for_step(&conn, &step_id, 5)
                .unwrap()
                .is_empty(),
            "open interruptions must not appear in the resolved list"
        );
    }

    #[test]
    fn test_resolve_interruption_round_trips_resolution_comment_state() {
        let conn = setup();
        let step_id = step_for_interruptions(&conn);

        // Blocker: no options.
        let id = insert_interruption(
            &conn,
            &step_id,
            1,
            InterruptionKind::Blocker,
            "Needs sudo to install deps",
            &[],
        )
        .unwrap();

        resolve_interruption(&conn, &id, "Installed by operator", Some("ran apt-get")).unwrap();

        let resolved = list_resolved_interruptions_for_step(&conn, &step_id, 5).unwrap();
        assert_eq!(resolved.len(), 1);
        let r = &resolved[0];
        assert_eq!(r.kind, InterruptionKind::Blocker);
        assert!(r.options.is_empty());
        assert_eq!(r.state, InterruptionState::Resolved);
        assert_eq!(r.resolution.as_deref(), Some("Installed by operator"));
        assert_eq!(r.comment.as_deref(), Some("ran apt-get"));
        assert!(r.resolved_at.is_some(), "resolved_at must be stamped");

        // A resolved interruption no longer counts as open.
        assert!(
            list_open_interruptions(&conn, "/proj", None)
                .unwrap()
                .is_empty()
        );

        // Double-resolve is a precise error (row exists but not open).
        let err = resolve_interruption(&conn, &id, "again", None).unwrap_err();
        assert!(err.to_string().contains("already resolved"), "got: {err}");

        // Unknown id is a distinct precise error.
        let err = resolve_interruption(&conn, "no-such-id", "x", None).unwrap_err();
        assert!(err.to_string().contains("not found"), "got: {err}");
    }

    #[test]
    fn test_list_resolved_interruptions_for_step_is_bounded_and_newest_first() {
        let conn = setup();
        let step_id = step_for_interruptions(&conn);

        const LIMIT: usize = DEFAULT_RESOLVED_INTERRUPTION_LIMIT; // 5
        let total = LIMIT + 3; // insert N+3, expect only N back

        // Insert + resolve N+3 interruptions with strictly increasing
        // `resolved_at` so "newest first" is deterministic regardless of how
        // fast the inserts run.
        let mut ids = Vec::new();
        for i in 0..total {
            let id = insert_interruption(
                &conn,
                &step_id,
                i as i32,
                InterruptionKind::Question,
                &format!("Q{i}"),
                &[],
            )
            .unwrap();
            resolve_interruption(&conn, &id, &format!("A{i}"), None).unwrap();
            // 2026-01-01T00:00:0{i}.000Z — monotonically increasing.
            stamp_resolved_at(&conn, &id, &format!("2026-01-01T00:00:{:02}.000Z", i));
            ids.push(id);
        }

        // Also insert one *open* interruption — it must never appear here.
        insert_interruption(
            &conn,
            &step_id,
            99,
            InterruptionKind::Blocker,
            "still open",
            &[],
        )
        .unwrap();

        let got = list_resolved_interruptions_for_step(&conn, &step_id, LIMIT).unwrap();

        // (1) Bounded to exactly LIMIT despite N+3 resolved rows present.
        assert_eq!(
            got.len(),
            LIMIT,
            "resolved-interruption query MUST LIMIT to {LIMIT} (had {total} resolved)"
        );

        // (2) Newest-first: the last `LIMIT` inserted ids, reversed.
        let expected_newest_first: Vec<&String> = ids.iter().rev().take(LIMIT).collect();
        let got_ids: Vec<&String> = got.iter().map(|i| &i.id).collect();
        assert_eq!(
            got_ids, expected_newest_first,
            "must return the most-recent {LIMIT}, newest first"
        );
        assert!(
            got.iter().all(|i| i.state == InterruptionState::Resolved),
            "open interruptions must be excluded"
        );

        // (3) limit == 0 returns nothing (cheap short-circuit, no query).
        assert!(
            list_resolved_interruptions_for_step(&conn, &step_id, 0)
                .unwrap()
                .is_empty()
        );

        // (4) A limit larger than the row count returns all resolved rows
        // (and still excludes the open one).
        let all_resolved =
            list_resolved_interruptions_for_step(&conn, &step_id, total + 100).unwrap();
        assert_eq!(all_resolved.len(), total);
    }

    #[test]
    fn test_list_interruptions_for_step_scopes_to_step() {
        let conn = setup();
        let plan = create_plan(&conn, "scope", "/proj", "b", "d", None, None, &[]).unwrap();
        let (s1, _) = create_step(
            &conn,
            &plan.id,
            "S1",
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
            "S2",
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

        insert_interruption(&conn, &s1.id, 1, InterruptionKind::Question, "q1", &[]).unwrap();
        insert_interruption(&conn, &s2.id, 1, InterruptionKind::Question, "q2", &[]).unwrap();

        let for_s1 = list_interruptions_for_step(&conn, &s1.id).unwrap();
        assert_eq!(for_s1.len(), 1);
        assert_eq!(for_s1[0].body, "q1");
    }
}
