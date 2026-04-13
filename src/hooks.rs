// Lifecycle hooks.
//
// Hooks are stored in two places:
//
//   1. The user's hook library (`~/.config/ralph-rs/hooks/*.md`) — the shared
//      catalog of reusable hook definitions with scope metadata.
//   2. The `step_hooks` table — per-plan / per-step associations by hook name.
//
// At each lifecycle event the runner looks up which hook names apply to the
// current step, resolves them against the library (filtered by project path
// scope), and runs the resulting shell commands. Unknown hook names produce
// a warning and are skipped so missing library entries don't block execution.

use std::path::Path;
use std::process::Command;

use anyhow::{Context, Result, bail};
use rusqlite::Connection;

use crate::hook_library::{self, Hook, Lifecycle};
use crate::plan::{Plan, Step};
use crate::storage;

/// A cache of the user's hook library + project path, populated once per
/// plan run. Passed through to each hook invocation so we don't re-read the
/// library for every step.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct HookContext {
    /// Hooks loaded from the library and already filtered by project path
    /// scope — so every hook here is guaranteed applicable to the current run.
    pub applicable: Vec<Hook>,
    /// Absolute path to the current project directory.
    pub project_dir: std::path::PathBuf,
}

impl HookContext {
    /// Load the library and filter by the given project directory. Safe to
    /// call even if the library is empty or missing.
    pub fn load(project_dir: &Path) -> Result<Self> {
        let all = hook_library::load_all().unwrap_or_default();
        let applicable = hook_library::filter_by_project(all, project_dir);
        Ok(Self {
            applicable,
            project_dir: project_dir.to_path_buf(),
        })
    }

    fn find(&self, name: &str) -> Option<&Hook> {
        self.applicable.iter().find(|h| h.name == name)
    }
}

/// Build the base environment passed to every hook invocation.
fn base_env(
    plan: &Plan,
    step: &Step,
    attempt: i32,
    workdir: &Path,
) -> Vec<(&'static str, String)> {
    vec![
        ("RALPH_PLAN_SLUG", plan.slug.clone()),
        ("RALPH_PLAN_ID", plan.id.clone()),
        ("RALPH_STEP_TITLE", step.title.clone()),
        ("RALPH_STEP_ID", step.id.clone()),
        ("RALPH_STEP_ATTEMPT", attempt.to_string()),
        ("RALPH_PROJECT_DIR", workdir.to_string_lossy().into_owned()),
    ]
}

/// Execute a single hook's shell command via `sh -c`. Exit code zero = success.
fn run_one_hook(
    hook: &Hook,
    workdir: &Path,
    env: &[(&'static str, String)],
    extra_env: &[(&'static str, String)],
) -> Result<()> {
    let mut cmd = Command::new("sh");
    cmd.arg("-c").arg(&hook.command).current_dir(workdir);

    for (k, v) in env.iter().chain(extra_env.iter()) {
        cmd.env(k, v);
    }
    cmd.env("RALPH_HOOK_NAME", &hook.name);

    let output = cmd
        .output()
        .with_context(|| format!("Failed to execute hook '{}'", hook.name))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Hook '{}' failed (exit {}): {}",
            hook.name,
            output.status,
            stderr.trim()
        );
    }

    Ok(())
}

/// Resolve every hook name attached to this plan+step+lifecycle from the db,
/// then run each one via the library. Unknown hook names log a warning and
/// are skipped (warn-and-skip policy).
#[allow(clippy::too_many_arguments)]
fn run_lifecycle(
    conn: &Connection,
    ctx: &HookContext,
    plan: &Plan,
    step: &Step,
    attempt: i32,
    lifecycle: Lifecycle,
    workdir: &Path,
    extra_env: &[(&'static str, String)],
) -> Result<()> {
    let rows = storage::list_hooks_for_step(conn, &plan.id, &step.id, lifecycle.as_str())?;
    if rows.is_empty() {
        return Ok(());
    }

    let env = base_env(plan, step, attempt, workdir);

    for row in rows {
        match ctx.find(&row.hook_name) {
            Some(hook) => run_one_hook(hook, workdir, &env, extra_env)?,
            None => {
                eprintln!(
                    "Warning: hook '{}' referenced by plan '{}' is not in the local library (skipped)",
                    row.hook_name, plan.slug
                );
            }
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Public lifecycle entry points
// ---------------------------------------------------------------------------

/// Run pre-step hooks. Returns `Err` if any hook exits non-zero (caller
/// should treat this as a failed attempt).
pub fn run_pre_step(
    conn: &Connection,
    ctx: &HookContext,
    plan: &Plan,
    step: &Step,
    attempt: i32,
    workdir: &Path,
) -> Result<()> {
    run_lifecycle(conn, ctx, plan, step, attempt, Lifecycle::PreStep, workdir, &[])
}

/// Run post-step hooks. Failures are logged as warnings but do not propagate.
pub fn run_post_step(
    conn: &Connection,
    ctx: &HookContext,
    plan: &Plan,
    step: &Step,
    attempt: i32,
    status: &str,
    workdir: &Path,
) {
    let extra = vec![("RALPH_STEP_STATUS", status.to_string())];
    if let Err(e) = run_lifecycle(
        conn,
        ctx,
        plan,
        step,
        attempt,
        Lifecycle::PostStep,
        workdir,
        &extra,
    ) {
        eprintln!("Warning: post-step hook failed: {e}");
    }
}

/// Run pre-test hooks. Returns `Err` if any hook exits non-zero.
pub fn run_pre_test(
    conn: &Connection,
    ctx: &HookContext,
    plan: &Plan,
    step: &Step,
    attempt: i32,
    workdir: &Path,
) -> Result<()> {
    run_lifecycle(conn, ctx, plan, step, attempt, Lifecycle::PreTest, workdir, &[])
}

/// Run post-test hooks. Failures are logged as warnings but do not propagate.
pub fn run_post_test(
    conn: &Connection,
    ctx: &HookContext,
    plan: &Plan,
    step: &Step,
    attempt: i32,
    test_passed: bool,
    workdir: &Path,
) {
    let extra = vec![(
        "RALPH_TEST_PASSED",
        if test_passed { "true" } else { "false" }.to_string(),
    )];
    if let Err(e) = run_lifecycle(
        conn,
        ctx,
        plan,
        step,
        attempt,
        Lifecycle::PostTest,
        workdir,
        &extra,
    ) {
        eprintln!("Warning: post-test hook failed: {e}");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::hook_library::Scope;
    use crate::plan::{PlanStatus, StepStatus};
    use chrono::Utc;
    use tempfile::TempDir;

    fn make_plan(id: &str, slug: &str) -> Plan {
        Plan {
            id: id.to_string(),
            slug: slug.to_string(),
            project: "/tmp/proj".to_string(),
            branch_name: "main".to_string(),
            description: "desc".to_string(),
            status: PlanStatus::InProgress,
            harness: None,
            agent: None,
            deterministic_tests: vec![],
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn make_step(id: &str, plan_id: &str, title: &str) -> Step {
        Step {
            id: id.to_string(),
            plan_id: plan_id.to_string(),
            sort_key: "a0".to_string(),
            title: title.to_string(),
            description: "".to_string(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: StepStatus::Pending,
            attempts: 0,
            max_retries: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    fn insert_plan_and_step(conn: &Connection, plan: &Plan, step: &Step) {
        conn.execute(
            "INSERT INTO plans (id, slug, project, branch_name, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![plan.id, plan.slug, plan.project, plan.branch_name, plan.description],
        ).unwrap();
        conn.execute(
            "INSERT INTO steps (id, plan_id, sort_key, title, description) VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![step.id, step.plan_id, step.sort_key, step.title, step.description],
        ).unwrap();
    }

    fn marker_hook(name: &str, lifecycle: Lifecycle, marker_path: &Path) -> Hook {
        Hook {
            name: name.to_string(),
            description: String::new(),
            lifecycle,
            scope: Scope::Global,
            command: format!(
                "echo \"$RALPH_PLAN_SLUG $RALPH_STEP_ID $RALPH_HOOK_NAME\" >> {}",
                marker_path.display()
            ),
        }
    }

    #[test]
    fn test_run_lifecycle_with_no_hooks() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let ctx = HookContext {
            applicable: vec![],
            project_dir: tmp.path().to_path_buf(),
        };

        // No hooks attached: nothing should happen, no error.
        run_pre_step(&conn, &ctx, &plan, &step, 1, tmp.path()).unwrap();
    }

    #[test]
    fn test_run_plan_wide_and_per_step_hooks() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let marker = tmp.path().join("marker.txt");

        let plan_hook = marker_hook("plan-wide", Lifecycle::PostStep, &marker);
        let step_hook = marker_hook("per-step", Lifecycle::PostStep, &marker);

        let ctx = HookContext {
            applicable: vec![plan_hook, step_hook],
            project_dir: tmp.path().to_path_buf(),
        };

        storage::attach_hook_to_plan(&conn, &plan.id, "post-step", "plan-wide").unwrap();
        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "post-step", "per-step").unwrap();

        run_post_step(&conn, &ctx, &plan, &step, 1, "complete", tmp.path());

        let contents = std::fs::read_to_string(&marker).unwrap();
        let lines: Vec<&str> = contents.lines().collect();
        assert_eq!(lines.len(), 2, "both hooks should have fired");
        // Plan-wide runs first (ORDER BY step_id IS NOT NULL).
        assert!(lines[0].contains("plan-wide"));
        assert!(lines[1].contains("per-step"));
        // Env vars are substituted.
        assert!(lines[0].contains("my-plan"));
        assert!(lines[0].contains("s1"));
    }

    #[test]
    fn test_warn_and_skip_missing_hook() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let ctx = HookContext {
            applicable: vec![],
            project_dir: tmp.path().to_path_buf(),
        };

        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "pre-step", "nonexistent")
            .unwrap();

        // Missing hook should NOT error — just warn and skip.
        run_pre_step(&conn, &ctx, &plan, &step, 1, tmp.path()).unwrap();
    }

    #[test]
    fn test_pre_step_hook_failure_propagates() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let fail = Hook {
            name: "fail".to_string(),
            description: String::new(),
            lifecycle: Lifecycle::PreStep,
            scope: Scope::Global,
            command: "exit 1".to_string(),
        };
        let ctx = HookContext {
            applicable: vec![fail],
            project_dir: tmp.path().to_path_buf(),
        };

        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "pre-step", "fail").unwrap();

        // pre-step failure returns Err.
        assert!(run_pre_step(&conn, &ctx, &plan, &step, 1, tmp.path()).is_err());
    }

    #[test]
    fn test_post_step_hook_failure_is_warning() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let fail = Hook {
            name: "fail".to_string(),
            description: String::new(),
            lifecycle: Lifecycle::PostStep,
            scope: Scope::Global,
            command: "exit 1".to_string(),
        };
        let ctx = HookContext {
            applicable: vec![fail],
            project_dir: tmp.path().to_path_buf(),
        };

        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "post-step", "fail").unwrap();

        // post-step failure is just a warning — no panic, no error return.
        run_post_step(&conn, &ctx, &plan, &step, 1, "complete", tmp.path());
    }

    #[test]
    fn test_status_env_var_passed_to_post_step() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let marker = tmp.path().join("status.txt");
        let capture = Hook {
            name: "capture".to_string(),
            description: String::new(),
            lifecycle: Lifecycle::PostStep,
            scope: Scope::Global,
            command: format!("echo $RALPH_STEP_STATUS > {}", marker.display()),
        };
        let ctx = HookContext {
            applicable: vec![capture],
            project_dir: tmp.path().to_path_buf(),
        };
        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "post-step", "capture").unwrap();

        run_post_step(&conn, &ctx, &plan, &step, 1, "timeout", tmp.path());

        let contents = std::fs::read_to_string(&marker).unwrap();
        assert_eq!(contents.trim(), "timeout");
    }

    #[test]
    fn test_test_passed_env_var() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let marker = tmp.path().join("tp.txt");
        let capture = Hook {
            name: "capture".to_string(),
            description: String::new(),
            lifecycle: Lifecycle::PostTest,
            scope: Scope::Global,
            command: format!("echo $RALPH_TEST_PASSED > {}", marker.display()),
        };
        let ctx = HookContext {
            applicable: vec![capture],
            project_dir: tmp.path().to_path_buf(),
        };
        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "post-test", "capture").unwrap();

        run_post_test(&conn, &ctx, &plan, &step, 1, false, tmp.path());

        let contents = std::fs::read_to_string(&marker).unwrap();
        assert_eq!(contents.trim(), "false");
    }
}
