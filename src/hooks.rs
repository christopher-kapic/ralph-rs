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
use std::time::Duration;

use anyhow::{Result, anyhow};
use rusqlite::Connection;
use tokio::process::Command;

use crate::hook_library::{self, Hook, Lifecycle};
use crate::plan::{Plan, Step};
use crate::storage;

/// Categorized hook failure. The post-step / post-test entry points use the
/// variant to choose between a soft warning and a hard failure: only `Db` is
/// fatal, since DB problems indicate the run can't reliably continue.
#[derive(Debug)]
enum HookFailure {
    /// Failed to query the `step_hooks` table for bindings on this lifecycle.
    /// Treated as fatal — the database is the source of truth for what runs.
    Db(anyhow::Error),
    /// Could not spawn or wait on the hook subprocess (typically a missing
    /// shell, EACCES on the workdir, or another `std::io::Error` from
    /// `tokio::process::Command`).
    Spawn {
        hook_name: String,
        source: std::io::Error,
    },
    /// Hook exceeded the configured `hook_timeout_secs` and was killed.
    Timeout { hook_name: String, secs: u64 },
    /// Hook ran to completion but exited with a non-zero status (or signal).
    Exit {
        hook_name: String,
        code: Option<i32>,
        stderr: String,
    },
}

impl HookFailure {
    /// True for failures the post-step / post-test entry points must escalate
    /// rather than swallow as a warning. Today only `Db` qualifies.
    fn is_fatal(&self) -> bool {
        matches!(self, HookFailure::Db(_))
    }
}

impl std::fmt::Display for HookFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HookFailure::Db(e) => {
                write!(f, "database error reading hook bindings: {e:#}")
            }
            HookFailure::Spawn { hook_name, source } => write!(
                f,
                "could not spawn hook '{hook_name}' ({:?}): {source}",
                source.kind()
            ),
            HookFailure::Timeout { hook_name, secs } => {
                write!(f, "hook '{hook_name}' timed out after {secs}s")
            }
            HookFailure::Exit {
                hook_name,
                code,
                stderr,
            } => {
                let code_s = code
                    .map(|c| c.to_string())
                    .unwrap_or_else(|| "<signal>".to_string());
                write!(
                    f,
                    "hook '{hook_name}' exited with status {code_s}: {}",
                    stderr.trim()
                )
            }
        }
    }
}

impl std::error::Error for HookFailure {}

/// Build the warning text emitted when a post-lifecycle hook fails non-fatally.
/// Kept separate from emission so it can be unit-tested without capturing
/// stderr. The `Db` variant is intentionally handled here too even though the
/// caller escalates it — keeping the formatter total makes it easier to reuse
/// if escalation policy changes later.
fn warning_message(which: Lifecycle, failure: &HookFailure) -> String {
    format!("Warning: {} {}", which.as_str(), failure)
}

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
    /// Per-hook execution timeout in seconds. `0` disables the timeout.
    pub hook_timeout_secs: u64,
}

impl HookContext {
    /// Load the library and filter by the given project directory. Safe to
    /// call even if the library is empty or missing.
    pub fn load(project_dir: &Path, hook_timeout_secs: u64) -> Result<Self> {
        let all = hook_library::load_all()?;
        let applicable = hook_library::filter_by_project(all, project_dir);
        Ok(Self {
            applicable,
            project_dir: project_dir.to_path_buf(),
            hook_timeout_secs,
        })
    }

    fn find(&self, name: &str) -> Option<&Hook> {
        self.applicable.iter().find(|h| h.name == name)
    }
}

/// Build the base environment passed to every hook invocation.
fn base_env(plan: &Plan, step: &Step, attempt: i32, workdir: &Path) -> Vec<(&'static str, String)> {
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
///
/// Ralph's env vars (e.g. `RALPH_PROJECT_DIR`) are passed through `Command::env`
/// rather than interpolated into the command string. Under POSIX shell rules,
/// parameter expansion is performed once and its result is not rescanned for
/// command substitution — so a project path containing `$(...)` is emitted
/// literally, never executed. See `test_env_var_values_not_reexpanded_by_shell`.
///
/// `timeout_secs` bounds wall-clock runtime — a hook still executing after
/// the deadline is killed (via `kill_on_drop`) and the call returns an error.
/// `0` disables the timeout entirely.
async fn run_one_hook(
    hook: &Hook,
    workdir: &Path,
    env: &[(&'static str, String)],
    extra_env: &[(&'static str, String)],
    timeout_secs: u64,
) -> Result<(), HookFailure> {
    let mut cmd = Command::new("sh");
    cmd.arg("-c")
        .arg(&hook.command)
        .current_dir(workdir)
        // Ensure a hook that blows through its timeout is reaped when the
        // enclosing future is dropped, rather than leaking as a zombie.
        .kill_on_drop(true);

    for (k, v) in env.iter().chain(extra_env.iter()) {
        cmd.env(k, v);
    }
    cmd.env("RALPH_HOOK_NAME", &hook.name);

    let run = cmd.output();
    let output = if timeout_secs == 0 {
        run.await.map_err(|source| HookFailure::Spawn {
            hook_name: hook.name.clone(),
            source,
        })?
    } else {
        match tokio::time::timeout(Duration::from_secs(timeout_secs), run).await {
            Ok(r) => r.map_err(|source| HookFailure::Spawn {
                hook_name: hook.name.clone(),
                source,
            })?,
            Err(_) => {
                return Err(HookFailure::Timeout {
                    hook_name: hook.name.clone(),
                    secs: timeout_secs,
                });
            }
        }
    };

    if !output.status.success() {
        return Err(HookFailure::Exit {
            hook_name: hook.name.clone(),
            code: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        });
    }

    Ok(())
}

/// Resolve every hook name attached to this plan+step+lifecycle from the db,
/// then run each one via the library. Unknown hook names log a warning and
/// are skipped (warn-and-skip policy).
#[allow(clippy::too_many_arguments)]
async fn run_lifecycle(
    conn: &Connection,
    ctx: &HookContext,
    plan: &Plan,
    step: &Step,
    attempt: i32,
    lifecycle: Lifecycle,
    workdir: &Path,
    extra_env: &[(&'static str, String)],
) -> Result<(), HookFailure> {
    let rows = storage::list_hooks_for_step(conn, &plan.id, &step.id, lifecycle.as_str())
        .map_err(HookFailure::Db)?;
    if rows.is_empty() {
        return Ok(());
    }

    let env = base_env(plan, step, attempt, workdir);

    for row in rows {
        match ctx.find(&row.hook_name) {
            Some(hook) => {
                run_one_hook(hook, workdir, &env, extra_env, ctx.hook_timeout_secs).await?
            }
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

/// Run pre-step hooks. Returns `Err` for any failure (DB lookup, spawn,
/// timeout, or non-zero exit) — callers treat this as a failed attempt.
pub async fn run_pre_step(
    conn: &Connection,
    ctx: &HookContext,
    plan: &Plan,
    step: &Step,
    attempt: i32,
    workdir: &Path,
) -> Result<()> {
    run_lifecycle(
        conn,
        ctx,
        plan,
        step,
        attempt,
        Lifecycle::PreStep,
        workdir,
        &[],
    )
    .await
    .map_err(|e| anyhow!(e))
}

/// Run post-step hooks. Hook-execution failures (spawn / timeout / non-zero
/// exit) are logged as kind-specific warnings; DB lookup failures escalate
/// to a hard error since the run can't reliably continue without trusting
/// the hook bindings table.
pub async fn run_post_step(
    conn: &Connection,
    ctx: &HookContext,
    plan: &Plan,
    step: &Step,
    attempt: i32,
    status: &str,
    workdir: &Path,
) -> Result<()> {
    let extra = vec![("RALPH_STEP_STATUS", status.to_string())];
    handle_post_lifecycle(
        Lifecycle::PostStep,
        run_lifecycle(
            conn,
            ctx,
            plan,
            step,
            attempt,
            Lifecycle::PostStep,
            workdir,
            &extra,
        )
        .await,
    )
}

/// Run pre-test hooks. Returns `Err` for any failure.
pub async fn run_pre_test(
    conn: &Connection,
    ctx: &HookContext,
    plan: &Plan,
    step: &Step,
    attempt: i32,
    workdir: &Path,
) -> Result<()> {
    run_lifecycle(
        conn,
        ctx,
        plan,
        step,
        attempt,
        Lifecycle::PreTest,
        workdir,
        &[],
    )
    .await
    .map_err(|e| anyhow!(e))
}

/// Run post-test hooks. Same error policy as `run_post_step`.
pub async fn run_post_test(
    conn: &Connection,
    ctx: &HookContext,
    plan: &Plan,
    step: &Step,
    attempt: i32,
    test_passed: bool,
    workdir: &Path,
) -> Result<()> {
    let extra = vec![(
        "RALPH_TEST_PASSED",
        if test_passed { "true" } else { "false" }.to_string(),
    )];
    handle_post_lifecycle(
        Lifecycle::PostTest,
        run_lifecycle(
            conn,
            ctx,
            plan,
            step,
            attempt,
            Lifecycle::PostTest,
            workdir,
            &extra,
        )
        .await,
    )
}

/// Apply the post-lifecycle policy: fatal failures propagate, the rest log
/// a kind-specific warning and return `Ok`.
fn handle_post_lifecycle(
    which: Lifecycle,
    result: Result<(), HookFailure>,
) -> Result<()> {
    match result {
        Ok(()) => Ok(()),
        Err(e) if e.is_fatal() => Err(anyhow!(e)),
        Err(e) => {
            eprintln!("{}", warning_message(which, &e));
            Ok(())
        }
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
            plan_harness: None,
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
            model: None,
            skipped_reason: None,
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

    fn ctx_for(applicable: Vec<Hook>, project_dir: std::path::PathBuf) -> HookContext {
        HookContext {
            applicable,
            project_dir,
            // Generous enough that the correctness tests never trip it, but
            // short enough that a bug causing infinite wait would still time
            // out within a test harness in a reasonable window.
            hook_timeout_secs: 30,
        }
    }

    #[tokio::test]
    async fn test_run_lifecycle_with_no_hooks() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let ctx = ctx_for(vec![], tmp.path().to_path_buf());

        // No hooks attached: nothing should happen, no error.
        run_pre_step(&conn, &ctx, &plan, &step, 1, tmp.path())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_run_plan_wide_and_per_step_hooks() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let marker = tmp.path().join("marker.txt");

        let plan_hook = marker_hook("plan-wide", Lifecycle::PostStep, &marker);
        let step_hook = marker_hook("per-step", Lifecycle::PostStep, &marker);

        let ctx = ctx_for(vec![plan_hook, step_hook], tmp.path().to_path_buf());

        storage::attach_hook_to_plan(&conn, &plan.id, "post-step", "plan-wide").unwrap();
        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "post-step", "per-step").unwrap();

        run_post_step(&conn, &ctx, &plan, &step, 1, "complete", tmp.path())
            .await
            .unwrap();

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

    #[tokio::test]
    async fn test_warn_and_skip_missing_hook() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let ctx = ctx_for(vec![], tmp.path().to_path_buf());

        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "pre-step", "nonexistent").unwrap();

        // Missing hook should NOT error — just warn and skip.
        run_pre_step(&conn, &ctx, &plan, &step, 1, tmp.path())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_pre_step_hook_failure_propagates() {
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
        let ctx = ctx_for(vec![fail], tmp.path().to_path_buf());

        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "pre-step", "fail").unwrap();

        // pre-step failure returns Err.
        assert!(
            run_pre_step(&conn, &ctx, &plan, &step, 1, tmp.path())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn test_post_step_hook_failure_is_warning() {
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
        let ctx = ctx_for(vec![fail], tmp.path().to_path_buf());

        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "post-step", "fail").unwrap();

        // post-step exit failure is a warning, not a hard error.
        run_post_step(&conn, &ctx, &plan, &step, 1, "complete", tmp.path())
            .await
            .expect("non-zero hook exit must surface as warning, not Err");
    }

    #[tokio::test]
    async fn test_status_env_var_passed_to_post_step() {
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
        let ctx = ctx_for(vec![capture], tmp.path().to_path_buf());
        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "post-step", "capture").unwrap();

        run_post_step(&conn, &ctx, &plan, &step, 1, "timeout", tmp.path())
            .await
            .unwrap();

        let contents = std::fs::read_to_string(&marker).unwrap();
        assert_eq!(contents.trim(), "timeout");
    }

    #[tokio::test]
    async fn test_env_var_values_not_reexpanded_by_shell() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let output_marker = tmp.path().join("output.txt");

        // Build a project directory whose path contains a literal $(...). If
        // the shell re-scanned RALPH_PROJECT_DIR after parameter expansion,
        // the command substitution would fire and create pwned.txt inside
        // this directory.
        let tricky_dir = tmp.path().join("proj$(touch pwned.txt)");
        std::fs::create_dir(&tricky_dir).unwrap();
        let malicious_marker = tricky_dir.join("pwned.txt");

        let hook = Hook {
            name: "probe".to_string(),
            description: String::new(),
            lifecycle: Lifecycle::PreStep,
            scope: Scope::Global,
            command: format!("echo \"$RALPH_PROJECT_DIR\" > {}", output_marker.display()),
        };
        let ctx = ctx_for(vec![hook], tricky_dir.clone());
        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "pre-step", "probe").unwrap();

        run_pre_step(&conn, &ctx, &plan, &step, 1, &tricky_dir)
            .await
            .unwrap();

        assert!(
            !malicious_marker.exists(),
            "shell must not execute $(...) substring embedded in RALPH_PROJECT_DIR"
        );
        let contents = std::fs::read_to_string(&output_marker).unwrap();
        assert!(
            contents.contains("$(touch"),
            "project dir should be preserved literally, got: {contents}"
        );
    }

    #[tokio::test]
    async fn test_test_passed_env_var() {
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
        let ctx = ctx_for(vec![capture], tmp.path().to_path_buf());
        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "post-test", "capture").unwrap();

        run_post_test(&conn, &ctx, &plan, &step, 1, false, tmp.path())
            .await
            .unwrap();

        let contents = std::fs::read_to_string(&marker).unwrap();
        assert_eq!(contents.trim(), "false");
    }

    /// A hook that runs longer than `hook_timeout_secs` is killed and the
    /// call returns an error naming the hook and the timeout duration.
    #[tokio::test]
    async fn test_hook_timeout_aborts_long_running_hook() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let slow = Hook {
            name: "slow".to_string(),
            description: String::new(),
            lifecycle: Lifecycle::PreStep,
            scope: Scope::Global,
            // sleep well past the configured timeout so we're certain the
            // timeout path fires, not the normal completion path.
            command: "sleep 60".to_string(),
        };
        let ctx = HookContext {
            applicable: vec![slow],
            project_dir: tmp.path().to_path_buf(),
            hook_timeout_secs: 1,
        };

        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "pre-step", "slow").unwrap();

        let start = std::time::Instant::now();
        let err = run_pre_step(&conn, &ctx, &plan, &step, 1, tmp.path())
            .await
            .expect_err("timed-out hook must surface as an error");
        let elapsed = start.elapsed();

        // We must abort well before the 60s sleep would have finished — the
        // 10s budget gives slow CI plenty of slack while still proving the
        // timeout kicked in.
        assert!(
            elapsed < Duration::from_secs(10),
            "timeout should fire quickly, elapsed = {elapsed:?}"
        );
        let msg = format!("{err}");
        assert!(
            msg.contains("slow") && msg.contains("timed out"),
            "error should name the hook and mention the timeout: {msg}"
        );
    }

    /// Each `HookFailure` variant must produce a distinct, kind-specific
    /// message — that is the whole point of the typed enum.
    #[test]
    fn test_warning_message_is_distinct_per_failure_kind() {
        let db = warning_message(
            Lifecycle::PostStep,
            &HookFailure::Db(anyhow!("connection closed")),
        );
        let spawn = warning_message(
            Lifecycle::PostStep,
            &HookFailure::Spawn {
                hook_name: "myhook".into(),
                source: std::io::Error::from(std::io::ErrorKind::PermissionDenied),
            },
        );
        let timeout = warning_message(
            Lifecycle::PostStep,
            &HookFailure::Timeout {
                hook_name: "myhook".into(),
                secs: 5,
            },
        );
        let exit = warning_message(
            Lifecycle::PostStep,
            &HookFailure::Exit {
                hook_name: "myhook".into(),
                code: Some(2),
                stderr: "boom".into(),
            },
        );

        let all = [&db, &spawn, &timeout, &exit];
        for (i, a) in all.iter().enumerate() {
            for (j, b) in all.iter().enumerate() {
                if i != j {
                    assert_ne!(a, b, "messages must differ between variants");
                }
            }
        }

        // Each message carries kind-specific text so a reader can tell
        // them apart at a glance.
        assert!(db.to_lowercase().contains("database"), "db: {db}");
        assert!(spawn.contains("spawn"), "spawn: {spawn}");
        assert!(spawn.contains("PermissionDenied"), "spawn kind: {spawn}");
        assert!(timeout.contains("timed out"), "timeout: {timeout}");
        assert!(exit.contains("exited"), "exit: {exit}");

        // Lifecycle name is included so post-step vs post-test failures
        // are distinguishable in logs.
        assert!(db.contains("post-step"));
        let pt = warning_message(
            Lifecycle::PostTest,
            &HookFailure::Exit {
                hook_name: "h".into(),
                code: Some(1),
                stderr: String::new(),
            },
        );
        assert!(pt.contains("post-test"));
    }

    /// A DB lookup failure during a post-step hook escalates to a hard error
    /// rather than being silently warned about. We provoke it by dropping
    /// the table that `list_hooks_for_step` queries.
    #[tokio::test]
    async fn test_post_step_db_error_escalates_to_hard_failure() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let ctx = ctx_for(vec![], tmp.path().to_path_buf());

        conn.execute("DROP TABLE step_hooks", []).unwrap();

        let err = run_post_step(&conn, &ctx, &plan, &step, 1, "complete", tmp.path())
            .await
            .expect_err("DB errors must escalate, not be warned about");
        let msg = format!("{err:#}");
        assert!(
            msg.to_lowercase().contains("database")
                || msg.to_lowercase().contains("hook bindings"),
            "error should name the DB problem: {msg}"
        );
    }

    /// Same policy applies to post-test hooks.
    #[tokio::test]
    async fn test_post_test_db_error_escalates_to_hard_failure() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let ctx = ctx_for(vec![], tmp.path().to_path_buf());

        conn.execute("DROP TABLE step_hooks", []).unwrap();

        let err = run_post_test(&conn, &ctx, &plan, &step, 1, true, tmp.path())
            .await
            .expect_err("DB errors must escalate, not be warned about");
        let msg = format!("{err:#}");
        assert!(
            msg.to_lowercase().contains("database")
                || msg.to_lowercase().contains("hook bindings"),
            "error should name the DB problem: {msg}"
        );
    }

    /// `hook_timeout_secs = 0` disables the timeout — a short-lived hook
    /// still completes normally.
    #[tokio::test]
    async fn test_hook_timeout_zero_disables_timeout() {
        let conn = db::open_memory().unwrap();
        let plan = make_plan("p1", "my-plan");
        let step = make_step("s1", "p1", "Step one");
        insert_plan_and_step(&conn, &plan, &step);

        let tmp = TempDir::new().unwrap();
        let ok = Hook {
            name: "ok".to_string(),
            description: String::new(),
            lifecycle: Lifecycle::PreStep,
            scope: Scope::Global,
            command: "true".to_string(),
        };
        let ctx = HookContext {
            applicable: vec![ok],
            project_dir: tmp.path().to_path_buf(),
            hook_timeout_secs: 0,
        };

        storage::attach_hook_to_step(&conn, &plan.id, &step.id, "pre-step", "ok").unwrap();

        run_pre_step(&conn, &ctx, &plan, &step, 1, tmp.path())
            .await
            .expect("hook should complete when timeout is disabled");
    }
}
