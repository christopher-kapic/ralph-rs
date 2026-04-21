// Run-related CLI command implementations (status, log, cancel)

use std::time::Duration;

use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::output::{self, OutputContext, OutputFormat};
use crate::plan::{ChangePolicy, ExecutionLog, StepStatus};
use crate::run_lock::{self, LiveRun};
use crate::storage;

// ---------------------------------------------------------------------------
// Status command
// ---------------------------------------------------------------------------

pub fn cmd_status(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    verbose: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = if let Some(slug) = plan_slug {
        storage::get_plan_by_slug(conn, slug, project)?
            .with_context(|| format!("Plan not found: {slug}"))?
    } else {
        // Find the most recent active plan, including completed plans so that
        // running `status` right after a plan finishes still shows it.
        match storage::find_active_plan(conn, project, true)? {
            Some(p) => p,
            None => {
                if out.format == OutputFormat::Json {
                    println!("null");
                } else {
                    eprintln!(
                        "No active plan found. Specify a plan slug as a positional argument."
                    );
                }
                return Ok(());
            }
        }
    };

    let (summary, steps) = build_status_summary(conn, project, &plan)?;

    if out.format == OutputFormat::Json {
        println!("{}", serde_json::to_string(&summary)?);
        return Ok(());
    }

    render_status_plain(&summary, &plan, &steps, verbose, out);
    Ok(())
}

/// Assemble a [`output::StatusSummary`] for `plan`, computing step counts and
/// attaching a live-run snapshot when one exists and is bound to this plan
/// (or unbound). Exposed to tests so the JSON contract can be exercised
/// without capturing stdout.
fn build_status_summary(
    conn: &Connection,
    project: &str,
    plan: &crate::plan::Plan,
) -> Result<(output::StatusSummary, Vec<crate::plan::Step>)> {
    let steps = storage::list_steps(conn, &plan.id)?;

    let total = steps.len();
    let complete = steps
        .iter()
        .filter(|s| s.status == StepStatus::Complete)
        .count();
    let failed = steps
        .iter()
        .filter(|s| s.status == StepStatus::Failed)
        .count();
    let skipped = steps
        .iter()
        .filter(|s| s.status == StepStatus::Skipped)
        .count();
    let pending = steps
        .iter()
        .filter(|s| s.status == StepStatus::Pending)
        .count();
    let in_progress = steps
        .iter()
        .filter(|s| s.status == StepStatus::InProgress)
        .count();

    // Load the live-run snapshot for this project and attach it iff its
    // recorded plan_id matches (or is unset — an unbound lock still covers
    // this project). If the live row records a different plan, it belongs to
    // someone else's run — omit it so the current plan's status doesn't
    // falsely show live.
    let live = storage::get_live_run(conn, project)?;
    let live_display: Option<output::LiveRunDisplay> =
        live.and_then(|lr| match lr.plan_id.as_deref() {
            Some(pid) if pid != plan.id => None,
            _ => Some(output::LiveRunDisplay::from_live_run(&lr)),
        });

    let summary = output::StatusSummary {
        slug: plan.slug.clone(),
        status: plan.status,
        branch_name: plan.branch_name.clone(),
        steps: output::StepCounts {
            total,
            complete,
            failed,
            skipped,
            pending,
            in_progress,
        },
        live: live_display,
    };
    Ok((summary, steps))
}

/// Render the plain-text status output for an assembled summary. Separated
/// from [`build_status_summary`] so the JSON contract can be tested without
/// capturing stdout.
fn render_status_plain(
    summary: &output::StatusSummary,
    plan: &crate::plan::Plan,
    steps: &[crate::plan::Step],
    verbose: bool,
    out: &OutputContext,
) {
    println!(
        "{}  {}",
        output::bold(&summary.slug, out.color),
        output::colored_plan_status(summary.status, out.color),
    );
    println!("  Branch: {}", summary.branch_name);

    if steps.is_empty() {
        println!("  No steps.");
        return;
    }

    let c = &summary.steps;
    println!(
        "  Progress: {}/{} complete, {} failed, {} skipped, {} pending, {} in-progress",
        c.complete, c.total, c.failed, c.skipped, c.pending, c.in_progress
    );

    if let Some(lv) = summary.live.as_ref() {
        print_live_block(lv, steps);
    }

    if verbose {
        println!();
        for (i, step) in steps.iter().enumerate() {
            let policy_tag = if step.change_policy == ChangePolicy::Optional {
                " [optional]"
            } else {
                ""
            };
            println!(
                "  {:>3}. {} {}{} [{}] (attempts: {})",
                i + 1,
                output::status_icon(step.status, out.color),
                step.title,
                policy_tag,
                output::colored_status(step.status, out.color),
                step.attempts,
            );
            if step.status == StepStatus::Skipped
                && let Some(reason) = step.skipped_reason.as_deref()
            {
                println!("       reason: {reason}");
            }
        }
    }
    let _ = plan; // quiet unused-param warning; kept for future plan-level fields.
}

/// Render the plain-text `Current:` block for the live-run snapshot. Lines
/// are written to `out` so the rendering is testable without capturing
/// stdout. Fields that aren't populated are skipped, so an unbound lock
/// (runner sitting between steps with no phase recorded yet) quietly emits
/// only what it has.
fn render_live_block(lv: &output::LiveRunDisplay, steps: &[crate::plan::Step]) -> String {
    use std::fmt::Write;
    let mut s = String::new();
    let _ = writeln!(s, "  Current:");

    // "step: N/M \"Title\"" — look up the title from the step list if the
    // live row's step_id resolves there.
    if let Some(num) = lv.step_num {
        let title = lv.step_id.as_deref().and_then(|id| {
            steps.iter().find(|st| st.id == id).map(|st| st.title.as_str())
        });
        match title {
            Some(t) => {
                let _ = writeln!(s, "    step: {}/{} \"{}\"", num, steps.len(), t);
            }
            None => {
                let _ = writeln!(s, "    step: {}/{}", num, steps.len());
            }
        }
    }

    if let Some(phase) = lv.phase {
        match lv.phase_elapsed_secs {
            Some(secs) => {
                let rounded = secs.round().max(0.0) as u64;
                let _ = writeln!(s, "    phase: {} ({}s)", phase.as_str(), rounded);
            }
            None => {
                let _ = writeln!(s, "    phase: {}", phase.as_str());
            }
        }
    }

    if let (Some(a), Some(m)) = (lv.attempt, lv.max_attempts) {
        let _ = writeln!(s, "    attempt: {a}/{m}");
    } else if let Some(a) = lv.attempt {
        let _ = writeln!(s, "    attempt: {a}");
    }

    if let Some(cmd) = lv.current_command.as_deref() {
        let _ = writeln!(s, "    command: {cmd}");
    }

    // runner pid + human-readable started_at. If the stored timestamp doesn't
    // parse as chrono we fall back to the raw string rather than dropping the
    // line — it's diagnostic information and something is strictly better
    // than nothing.
    let started_display = lv
        .started_at
        .parse::<chrono::DateTime<chrono::Utc>>()
        .ok()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S UTC").to_string())
        .unwrap_or_else(|| lv.started_at.clone());
    let _ = writeln!(s, "    runner: pid {}, started {}", lv.pid, started_display);

    s
}

/// Thin wrapper over `render_live_block` that prints to stdout.
fn print_live_block(lv: &output::LiveRunDisplay, steps: &[crate::plan::Step]) {
    print!("{}", render_live_block(lv, steps));
}

// ---------------------------------------------------------------------------
// Log command
// ---------------------------------------------------------------------------

/// Controls how harness stdout/stderr is displayed in log output.
///
/// - `Hidden` — don't show output (default when no flags given).
/// - `Truncated(n)` — show up to `n` lines per stream.
/// - `Full` — show everything, no truncation.
pub enum LogOutputMode {
    Hidden,
    Truncated(usize),
    Full,
}

pub fn cmd_log(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    step_num: Option<usize>,
    limit: Option<usize>,
    output_mode: &LogOutputMode,
    out: &OutputContext,
) -> Result<()> {
    // Resolve plan
    let plan = if let Some(slug) = plan_slug {
        storage::get_plan_by_slug(conn, slug, project)?
            .with_context(|| format!("Plan not found: {slug}"))?
    } else {
        match storage::find_active_plan(conn, project, true)? {
            Some(p) => p,
            None => {
                if out.format == OutputFormat::Json {
                    println!("null");
                } else {
                    eprintln!("No plan found. Specify a plan slug as a positional argument.");
                }
                return Ok(());
            }
        }
    };

    if let Some(step_idx) = step_num {
        // Show logs for a specific step
        let steps = storage::list_steps(conn, &plan.id)?;
        if step_idx == 0 || step_idx > steps.len() {
            anyhow::bail!(
                "Step {} is out of range (plan has {} steps)",
                step_idx,
                steps.len()
            );
        }
        let step = &steps[step_idx - 1];
        let logs = storage::list_execution_logs_for_step(conn, &step.id)?;

        if out.format == OutputFormat::Json {
            for log in &logs {
                output::emit_ndjson(&output::LogEntrySummary::new(log, output_mode))?;
            }
            return Ok(());
        }

        eprintln!(
            "Logs for step #{} '{}' ({} attempts):",
            step_idx,
            step.title,
            logs.len()
        );
        eprintln!();

        if step.status == StepStatus::Skipped {
            match step.skipped_reason.as_deref() {
                Some(reason) => println!("  (skipped: {reason})"),
                None => println!("  (skipped)"),
            }
            println!();
        }

        for log in &logs {
            print_log_entry(&step.title, log, output_mode, out.color);
        }
    } else {
        // Show all logs for the plan
        let entries = storage::list_execution_logs_for_plan(conn, &plan.id, limit)?;

        if out.format == OutputFormat::Json {
            for (_, log) in &entries {
                output::emit_ndjson(&output::LogEntrySummary::new(log, output_mode))?;
            }
            return Ok(());
        }

        // Surface skipped steps' reasons alongside execution logs — skips
        // don't produce an execution_log row, so they'd otherwise be invisible
        // in this view.
        let steps = storage::list_steps(conn, &plan.id)?;
        let skipped_with_reason: Vec<&crate::plan::Step> = steps
            .iter()
            .filter(|s| s.status == StepStatus::Skipped)
            .collect();

        if entries.is_empty() && skipped_with_reason.is_empty() {
            eprintln!("No execution logs for plan '{}'.", plan.slug);
            return Ok(());
        }

        if !skipped_with_reason.is_empty() {
            eprintln!("Skipped steps for plan '{}':", plan.slug);
            eprintln!();
            for step in &skipped_with_reason {
                let num = steps.iter().position(|s| s.id == step.id).unwrap_or(0) + 1;
                match step.skipped_reason.as_deref() {
                    Some(reason) => {
                        println!("  #{num} {} — skipped ({reason})", step.title);
                    }
                    None => {
                        println!("  #{num} {} — skipped", step.title);
                    }
                }
            }
            println!();
        }

        if entries.is_empty() {
            return Ok(());
        }

        eprintln!(
            "Execution logs for plan '{}' ({} entries):",
            plan.slug,
            entries.len()
        );
        eprintln!();

        for (step_title, log) in &entries {
            print_log_entry(step_title, log, output_mode, out.color);
        }
    }

    Ok(())
}

fn print_log_entry(step_title: &str, log: &ExecutionLog, output_mode: &LogOutputMode, color: bool) {
    let icon = output::log_status_icon(log.committed, log.rolled_back, color);

    let duration_str = log
        .duration_secs
        .map(|d| format!("{:.1}s", d))
        .unwrap_or_else(|| "-".to_string());

    println!(
        "  {} [attempt {}] {} ({}) {}",
        icon,
        log.attempt,
        step_title,
        duration_str,
        log.started_at.format("%Y-%m-%d %H:%M:%S UTC"),
    );

    if let Some(ref hash) = log.commit_hash {
        println!("    commit: {}", &hash[..hash.len().min(8)]);
    }

    // Surface the optional-policy no-op marker explicitly so a successful log
    // row with no commit doesn't look like a mysterious empty success. We key
    // off termination_reason = Success + absent commit_hash, then fall back to
    // the sentinel string embedded in test_results when observability data is
    // missing (older rows, or the deliberate `change_policy=optional` marker
    // written by the executor).
    let optional_no_change = log
        .test_results
        .iter()
        .any(|r| r.contains("change_policy=optional"))
        || (log.commit_hash.is_none()
            && log.termination_reason
                == Some(crate::plan::TerminationReason::Success));
    if optional_no_change {
        println!("    (no changes — change_policy=optional)");
    }

    if !log.test_results.is_empty() {
        println!("    tests: {}", log.test_results.join(", "));
    }

    // Always print termination_reason when Some so a stuck/interrupted row
    // that has no commit hash, no diff, and no test_results still surfaces
    // unambiguous diagnostic information. Rows that predate V11 (Option::None)
    // just omit the line.
    if let Some(reason) = log.termination_reason {
        println!(
            "    reason: {}",
            output::colored_termination_reason(reason, color)
        );
    }

    // Always print test_status when Some. Same rationale — this is cheap to
    // render and painful to miss when a row's test_results field is empty
    // but the phase actually ran.
    if let Some(status) = log.test_status {
        println!(
            "    test status: {}",
            output::colored_test_status(status, color)
        );
    }

    if let Some(cost) = log.cost_usd {
        let tokens = match (log.input_tokens, log.output_tokens) {
            (Some(i), Some(o)) => format!(" ({i} in / {o} out tokens)"),
            _ => String::new(),
        };
        println!("    cost: ${:.4}{}", cost, tokens);
    }

    if !matches!(output_mode, LogOutputMode::Hidden) {
        // --lines N is a *total* budget across both streams. Distribute it
        // proportionally so --lines 50 never prints more than 50 lines.
        let (stdout_cap, stderr_cap) = match output_mode {
            LogOutputMode::Truncated(n) => {
                let out_n = log
                    .harness_stdout
                    .as_deref()
                    .map(|s| s.lines().count())
                    .unwrap_or(0);
                let err_n = log
                    .harness_stderr
                    .as_deref()
                    .map(|s| s.lines().count())
                    .unwrap_or(0);
                let (a, b) = output::split_lines_budget(out_n, err_n, *n);
                (Some(a), Some(b))
            }
            _ => (None, None),
        };
        let print_stream = |label: &str, text: &Option<String>, cap: Option<usize>| {
            if let Some(s) = text.as_deref()
                && !s.is_empty()
                && cap != Some(0)
            {
                println!("    --- {label} ---");
                let lines_iter = s.lines();
                let lines: Box<dyn Iterator<Item = &str>> = match cap {
                    Some(n) => Box::new(lines_iter.take(n)),
                    None => Box::new(lines_iter),
                };
                for line in lines {
                    println!("    {line}");
                }
            }
        };
        print_stream("stdout", &log.harness_stdout, stdout_cap);
        print_stream("stderr", &log.harness_stderr, stderr_cap);
    }

    println!();
}

// ---------------------------------------------------------------------------
// Cancel command
// ---------------------------------------------------------------------------

/// Cancel the live `ralph run` for this project.
///
/// Finds the live run via the `run_locks` row, sends SIGTERM to the recorded
/// ralph pid (routed through the graceful-shutdown path), polls for the lock
/// to release, and falls back to SIGKILL if the runner doesn't release within
/// `timeout`. After the target is gone, stale execution-log and step-status
/// rows are reconciled so the history isn't left ambiguous.
///
/// Idempotent: a no-op when there is no active run.
///
/// Unix-only: the graceful shutdown relies on POSIX signals. On non-unix
/// platforms cancel returns an error rather than silently falling back to
/// SIGKILL-equivalents, since the partial semantics would be confusing.
pub fn cmd_cancel(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    force: bool,
    timeout: Duration,
    out: &OutputContext,
) -> Result<()> {
    #[cfg(not(unix))]
    {
        let _ = (conn, project, plan_slug, force, timeout, out);
        anyhow::bail!("ralph cancel is only supported on unix platforms");
    }

    #[cfg(unix)]
    {
        cmd_cancel_unix(conn, project, plan_slug, force, timeout, out)
    }
}

#[cfg(unix)]
fn cmd_cancel_unix(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    force: bool,
    timeout: Duration,
    out: &OutputContext,
) -> Result<()> {
    // 1. Load the live run.
    let live = match storage::get_live_run(conn, project)? {
        Some(l) => l,
        None => {
            emit_no_active(out)?;
            return Ok(());
        }
    };

    // 2. Validate plan mismatch.
    if let Some(requested) = plan_slug
        && live.plan_slug.as_deref() != Some(requested)
    {
        let live_label = live.plan_slug.as_deref().unwrap_or("<none>");
        anyhow::bail!(
            "Live run is for plan {live_label}, not {requested}. Refusing to cancel."
        );
    }

    // 3. Verify the pid is the same process we think it is. If the token
    //    mismatches, the pid was reused since the lock was taken, so the
    //    original ralph is already dead.
    let current_token = run_lock::process_start_token(live.pid);
    let target_dead = match (live.pid_start_token.as_deref(), current_token.as_deref()) {
        // We have a stored token: the target is alive iff the live token matches.
        (Some(stored), Some(current)) => stored != current,
        // Stored token but no live one → pid is gone.
        (Some(_), None) => true,
        // No stored token (pre-v9 row) → fall back to liveness probe.
        (None, _) => !pid_is_alive(live.pid),
    };

    if target_dead {
        return finalize_stale_run(conn, &live, /*forced=*/ false, out)
            .context("cleaning up after already-dead runner");
    }

    // 4. Graceful path or --force path.
    let forced = if force {
        kill_force(&live)?;
        true
    } else {
        // SIGTERM the runner, then wait for the lock to release. If it
        // doesn't, escalate to SIGKILL on the runner *and* the harness
        // process group.
        if !out.quiet && out.format == OutputFormat::Plain {
            eprintln!(
                "Waiting for runner (pid {}) to release lock (timeout {}s)...",
                live.pid,
                timeout.as_secs()
            );
        }
        send_signal(live.pid, libc::SIGTERM)
            .with_context(|| format!("sending SIGTERM to pid {}", live.pid))?;

        let released = wait_for_release(conn, project, timeout, out)?;
        if released {
            // Runner handled it gracefully. Its Drop-path release already
            // deleted the run_locks row; just emit summary.
            emit_summary(out, &live, /*forced=*/ false, /*already_dead=*/ false)?;
            return Ok(());
        }
        // Escalate.
        if !out.quiet && out.format == OutputFormat::Plain {
            eprintln!(
                "\nRunner did not release lock within {}s — escalating to SIGKILL.",
                timeout.as_secs()
            );
        }
        kill_force(&live)?;
        true
    };

    // 5/6/7/8: After the target is gone, reconcile bookkeeping the runner
    //     didn't get a chance to write.
    finalize_stale_run(conn, &live, forced, out)
}

/// Emit the "no active run" message for both plain and JSON formats.
#[cfg(unix)]
fn emit_no_active(out: &OutputContext) -> Result<()> {
    if out.format == OutputFormat::Json {
        let summary = output::CancelSummary {
            cancelled: false,
            forced: false,
            plan_slug: None,
            step_num: None,
            phase: None,
            attempt: None,
            max_attempts: None,
            pid: None,
            already_dead: false,
        };
        output::emit_ndjson(&summary)?;
    } else if !out.quiet {
        println!("No active run in this project.");
    }
    Ok(())
}

/// Poll the run_locks row every 200ms; returns `true` if it disappeared before
/// `timeout` elapsed, `false` otherwise. A progress dot is printed every ~2s
/// in plain/non-quiet mode so the user sees we're still waiting.
#[cfg(unix)]
fn wait_for_release(
    conn: &Connection,
    project: &str,
    timeout: Duration,
    out: &OutputContext,
) -> Result<bool> {
    const POLL: Duration = Duration::from_millis(200);
    let start = std::time::Instant::now();
    let mut ticks: u64 = 0;
    let show_progress = !out.quiet && out.format == OutputFormat::Plain;

    while start.elapsed() < timeout {
        if storage::get_live_run(conn, project)?.is_none() {
            if show_progress {
                eprintln!();
            }
            return Ok(true);
        }
        std::thread::sleep(POLL);
        ticks += 1;
        // 200ms * 10 = 2s.
        if show_progress && ticks % 10 == 0 {
            eprint!(".");
            use std::io::Write as _;
            let _ = std::io::stderr().flush();
        }
    }
    if show_progress {
        eprintln!();
    }
    Ok(false)
}

/// Handle the case where the target ralph process is already gone (either
/// pid-start-token mismatch from pid reuse, or `--force` after escalation).
/// Deletes the run_locks row (pid + start-token scoped), finalizes a stale
/// execution_log if one was recorded, and flips an InProgress step to Aborted.
#[cfg(unix)]
fn finalize_stale_run(
    conn: &Connection,
    live: &LiveRun,
    forced: bool,
    out: &OutputContext,
) -> Result<()> {
    // Stale execution log: COALESCE-based helper only fills in fields still
    // NULL, so it never clobbers diff/stdout/commit data the runner persisted
    // or a terminal reason the runner already recorded. A missing row is
    // benign — the runner may have deleted its own log during cleanup.
    if let Some(log_id) = live.execution_log_id {
        storage::finalize_execution_log_as_interrupted_if_exists(conn, log_id)?;
    }

    // Atomically flip InProgress → Aborted. A step that's already Complete /
    // Failed / etc. from the runner's own cleanup won't match the predicate
    // and is left alone. Errors from the UPDATE propagate so the operator
    // doesn't see a "cancelled successfully" summary after a DB failure.
    if let Some(step_id) = live.step_id.as_deref() {
        storage::update_step_status_if(
            conn,
            step_id,
            StepStatus::InProgress,
            StepStatus::Aborted,
        )?;
    }

    // Delete the run_locks row scoped by pid + start token so a new ralph run
    // that already inserted its row (different pid, or reused pid with a new
    // start token) is untouched.
    storage::delete_run_lock_row_unscoped(
        conn,
        &live.project,
        live.pid,
        live.pid_start_token.as_deref(),
    )?;

    emit_summary(out, live, forced, /*already_dead=*/ !forced)
}

/// Escalation / force path: SIGKILL the runner (if still alive) and the
/// harness process group (if the child_pid's start token still matches,
/// guarding against grandchild-pid reuse). Waits briefly for the runner pid
/// to actually disappear.
#[cfg(unix)]
fn kill_force(live: &LiveRun) -> Result<()> {
    // Only SIGKILL the runner if it's still the same live process.
    let runner_alive = match live.pid_start_token.as_deref() {
        Some(stored) => run_lock::process_start_token(live.pid).as_deref() == Some(stored),
        None => pid_is_alive(live.pid),
    };
    if runner_alive {
        // Best-effort: a race where the pid dies between the liveness check
        // and the kill surfaces as ESRCH which we happily ignore.
        let _ = send_signal(live.pid, libc::SIGKILL);
    }

    // SIGKILL the harness process group if we can positively identify it.
    if let (Some(child_pid), Some(stored_child_token)) =
        (live.child_pid, live.child_start_token.as_deref())
        && run_lock::process_start_token(child_pid).as_deref() == Some(stored_child_token)
    {
        // Negative pid targets the whole process group led by `child_pid`.
        let _ = send_signal(-child_pid, libc::SIGKILL);
    }

    // Brief wait (~2s) for the runner to actually die. `kill(pid, 0)` returns
    // -1 with ESRCH when the pid no longer exists.
    for _ in 0..40 {
        if !pid_is_alive(live.pid) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    // Don't fail hard — stale bookkeeping still needs to run — but warn.
    if !runner_alive {
        // Never was alive; nothing to report.
        return Ok(());
    }
    eprintln!(
        "warning: pid {} did not exit after SIGKILL; cleaning up anyway",
        live.pid
    );
    Ok(())
}

/// Send a signal; returns the errno on failure. SAFETY: `libc::kill` is a
/// plain syscall wrapper.
#[cfg(unix)]
fn send_signal(pid: i64, signo: i32) -> Result<()> {
    // Clamp pid into i32 since libc::kill takes `pid_t` which is i32 on Linux
    // and every other unix we care about.
    let pid_i32 = i32::try_from(pid)
        .with_context(|| format!("pid {pid} does not fit in i32"))?;
    let rc = unsafe { libc::kill(pid_i32, signo) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        // ESRCH (no such process) is OK — caller may have raced us.
        if err.raw_os_error() == Some(libc::ESRCH) {
            return Ok(());
        }
        return Err(err).with_context(|| format!("kill({pid_i32}, {signo})"));
    }
    Ok(())
}

/// Liveness probe without requiring a start token. Returns true only if the
/// pid is currently valid.
#[cfg(unix)]
fn pid_is_alive(pid: i64) -> bool {
    if pid <= 0 {
        return false;
    }
    let Ok(pid_i32) = i32::try_from(pid) else {
        return false;
    };
    // SAFETY: kill(pid, 0) is a pure liveness probe.
    let r = unsafe { libc::kill(pid_i32, 0) };
    r == 0
}

/// Final user-facing summary in both plain and JSON modes.
#[cfg(unix)]
fn emit_summary(
    out: &OutputContext,
    live: &LiveRun,
    forced: bool,
    already_dead: bool,
) -> Result<()> {
    if out.format == OutputFormat::Json {
        let summary = output::CancelSummary {
            cancelled: true,
            forced,
            plan_slug: live.plan_slug.clone(),
            step_num: live.step_num,
            phase: live.phase.map(|p| p.as_str().to_string()),
            attempt: live.attempt,
            max_attempts: live.max_attempts,
            pid: Some(live.pid),
            already_dead,
        };
        output::emit_ndjson(&summary)?;
        return Ok(());
    }
    if out.quiet {
        return Ok(());
    }

    let plan_label = live.plan_slug.as_deref().unwrap_or("<unknown>");
    let phase_label = live
        .phase
        .map(|p| p.as_str().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let step_label = match (live.step_num, live.max_attempts) {
        (Some(n), _) => format!("step {n}"),
        _ => "no active step".to_string(),
    };
    let attempts_label = match (live.attempt, live.max_attempts) {
        (Some(a), Some(m)) => format!("attempt {a}/{m}"),
        (Some(a), None) => format!("attempt {a}"),
        _ => "no attempt".to_string(),
    };
    let qualifier = if already_dead {
        " (runner was already dead)"
    } else if forced {
        " (forced)"
    } else {
        ""
    };
    println!(
        "Cancelled run for plan {plan_label}, {step_label} (phase {phase_label}, {attempts_label}){qualifier}.",
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------
//
// FOLLOWUP: end-to-end cancel test with a real `ralph run` subprocess. The
// current coverage exercises the stale-target bookkeeping paths (pid-start-
// token mismatch → cleanup) plus the plan-mismatch guard. Driving a live
// graceful SIGTERM handshake requires spawning the full binary and is best
// done as an integration test in a separate module.

#[cfg(all(test, unix))]
mod cancel_tests {
    use super::*;
    use crate::db;
    use crate::output::{OutputContext, OutputFormat};
    use crate::plan::{Phase, StepStatus, TerminationReason, TestStatus};
    use rusqlite::params;

    fn test_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    /// A deliberately-bogus pid outside any real pid space on Linux — not
    /// alive, and `/proc/<pid>/stat` returns nothing.
    const DEAD_PID: i64 = 0x7FFF_FFFE;

    fn seed_plan_and_step(
        conn: &Connection,
        slug: &str,
        project: &str,
    ) -> (String, String) {
        let plan =
            storage::create_plan(conn, slug, project, "br", "desc", None, None, &[]).unwrap();
        let (step, _) = storage::create_step(
            conn, &plan.id, "t", "d", None, None, &[], None, None, None,
        )
        .unwrap();
        (plan.id, step.id)
    }

    #[test]
    fn cancel_no_live_run_is_ok() {
        let conn = db::open_memory().unwrap();
        let result = cmd_cancel(
            &conn,
            "/tmp/proj-no-run",
            None,
            false,
            Duration::from_secs(1),
            &test_out(),
        );
        assert!(
            result.is_ok(),
            "cancel with no row should succeed: {result:?}"
        );
    }

    #[test]
    fn cancel_plan_mismatch_errors() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-mismatch";
        let (plan_id, _) = seed_plan_and_step(&conn, "plan-a", project);

        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![project, DEAD_PID, "fabricated-token", plan_id, "plan-a"],
        )
        .unwrap();

        let err = cmd_cancel(
            &conn,
            project,
            Some("plan-b"),
            false,
            Duration::from_secs(1),
            &test_out(),
        )
        .expect_err("plan mismatch should error");
        let msg = format!("{err}");
        assert!(
            msg.contains("Refusing to cancel"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn cancel_dead_target_cleans_up_row() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-dead";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "plan-dead", project);

        // Set the step InProgress so cancel can flip it to Aborted.
        storage::update_step_status(&conn, &step_id, StepStatus::InProgress).unwrap();

        // Seed a run_locks row with a fabricated start token against a pid
        // that's definitely dead (or at least mismatched).
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug, step_id, step_num, phase, attempt, max_attempts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                project,
                DEAD_PID,
                "fabricated-token",
                plan_id,
                "plan-dead",
                step_id,
                1i32,
                Phase::Harness.as_str(),
                1i32,
                3i32,
            ],
        )
        .unwrap();

        cmd_cancel(
            &conn,
            project,
            None,
            false,
            Duration::from_secs(1),
            &test_out(),
        )
        .expect("cancel");

        // Row is gone.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run_locks WHERE project = ?1",
                params![project],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0, "run_locks row should have been deleted");

        // Step flipped to Aborted.
        let step = storage::get_step_by_id(&conn, &step_id).unwrap().unwrap();
        assert_eq!(step.status, StepStatus::Aborted);
    }

    #[test]
    fn cancel_stale_log_cleanup_preserves_observability_fields() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-stalelog";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "plan-sl", project);

        // Simulate the runner having written diff + stdout before it died.
        let log = storage::create_execution_log(&conn, &step_id, 1, None, None).unwrap();
        storage::update_execution_log(
            &conn,
            log.id,
            Some(2.5),
            Some("+runner wrote this diff"),
            &[],
            false,
            false,
            None,
            Some("runner stdout"),
            Some("runner stderr"),
            None,
            None,
            None,
            None,
            None, // termination_reason still NULL
            None,
        )
        .unwrap();

        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug, step_id, execution_log_id, phase)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                project,
                DEAD_PID,
                "fabricated-token",
                plan_id,
                "plan-sl",
                step_id,
                log.id,
                Phase::Tests.as_str(),
            ],
        )
        .unwrap();

        cmd_cancel(
            &conn,
            project,
            None,
            false,
            Duration::from_secs(1),
            &test_out(),
        )
        .expect("cancel");

        let updated = storage::get_execution_log_by_id(&conn, log.id).unwrap();
        assert_eq!(
            updated.termination_reason,
            Some(TerminationReason::UserInterrupted)
        );
        assert_eq!(updated.test_status, Some(TestStatus::NotRun));
        // Fields the runner had persisted must survive.
        assert_eq!(updated.diff.as_deref(), Some("+runner wrote this diff"));
        assert_eq!(updated.harness_stdout.as_deref(), Some("runner stdout"));
        assert_eq!(updated.harness_stderr.as_deref(), Some("runner stderr"));
        assert_eq!(updated.duration_secs, Some(2.5));
    }

    #[test]
    fn cancel_stale_log_does_not_overwrite_existing_terminal_reason() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-stalelog-done";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "plan-sld", project);

        // Runner finished Success before cancel raced in.
        let log = storage::create_execution_log(&conn, &step_id, 1, None, None).unwrap();
        storage::update_execution_log(
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

        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug, step_id, execution_log_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                project,
                DEAD_PID,
                "fabricated-token",
                plan_id,
                "plan-sld",
                step_id,
                log.id,
            ],
        )
        .unwrap();

        cmd_cancel(
            &conn,
            project,
            None,
            false,
            Duration::from_secs(1),
            &test_out(),
        )
        .expect("cancel");

        let updated = storage::get_execution_log_by_id(&conn, log.id).unwrap();
        assert_eq!(updated.termination_reason, Some(TerminationReason::Success));
        assert_eq!(updated.test_status, Some(TestStatus::Passed));
    }

    #[test]
    fn cancel_summary_json_shape() {
        // Build a CancelSummary directly and round-trip through JSON to make
        // sure the field names/shape the CLI advertises are stable. This
        // avoids capturing stdout, which is awkward from a library test.
        let s = output::CancelSummary {
            cancelled: true,
            forced: false,
            plan_slug: Some("plan-xyz".to_string()),
            step_num: Some(4),
            phase: Some("harness".to_string()),
            attempt: Some(2),
            max_attempts: Some(3),
            pid: Some(4242),
            already_dead: false,
        };
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"cancelled\":true"));
        assert!(json.contains("\"forced\":false"));
        assert!(json.contains("\"plan_slug\":\"plan-xyz\""));
        assert!(json.contains("\"step_num\":4"));
        assert!(json.contains("\"phase\":\"harness\""));
        assert!(json.contains("\"attempt\":2"));
        assert!(json.contains("\"max_attempts\":3"));
        assert!(json.contains("\"pid\":4242"));
        assert!(json.contains("\"already_dead\":false"));
    }

    /// Smoke test for the SIGKILL mechanics. Spawns a tiny shell loop,
    /// registers it as the live runner with a matching start token, and
    /// calls `cmd_cancel` with `force=true`. The script has no SIGTERM
    /// handler, so this exercises the escalation-to-SIGKILL path rather
    /// than the full graceful handshake. The pid should be dead (ESRCH)
    /// after cancel returns.
    #[test]
    fn cancel_force_kills_live_script() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-force-kill";
        let (plan_id, _step_id) = seed_plan_and_step(&conn, "plan-fk", project);

        // Spawn `sh -c 'while true; do sleep 1; done'`. Use Rust's
        // std::process so we can read the pid; the child inherits a default
        // SIGTERM disposition (default action: terminate), but we're going
        // straight to SIGKILL via --force.
        let child = std::process::Command::new("sh")
            .arg("-c")
            .arg("while true; do sleep 1; done")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("spawn sh loop");

        let child_pid = child.id() as i64;
        // Give the shell a beat to start so /proc/<pid>/stat is populated.
        std::thread::sleep(Duration::from_millis(50));
        let token =
            run_lock::process_start_token(child_pid).expect("child start token");

        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![project, child_pid, token, plan_id, "plan-fk"],
        )
        .unwrap();

        // Reap the child asynchronously so `kill(pid, 0)` inside cmd_cancel
        // sees ESRCH instead of lingering on a zombie. In real ralph usage the
        // cancel process is a *sibling* of the runner (not its parent), so it
        // never observes zombification — this thread emulates that.
        let mut child_mut = child;
        let reaper = std::thread::spawn(move || {
            let _ = child_mut.wait();
        });

        // --force → SIGKILL-on-runner straight away, with no graceful wait.
        cmd_cancel(
            &conn,
            project,
            None,
            true,
            Duration::from_secs(2),
            &test_out(),
        )
        .expect("cancel --force");

        let _ = reaper.join();
        assert!(
            !pid_is_alive(child_pid),
            "script pid {child_pid} should be dead after cancel --force"
        );

        // Row cleaned up.
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM run_locks WHERE project = ?1",
                params![project],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 0);
    }
}

// ---------------------------------------------------------------------------
// Status live-view + log termination_reason/test_status tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod status_live_view_tests {
    use super::*;
    use crate::db;
    use crate::plan::{Phase, TerminationReason, TestStatus};
    use rusqlite::params;

    fn seed_plan_and_step(
        conn: &Connection,
        slug: &str,
        project: &str,
    ) -> (String, String) {
        let plan =
            storage::create_plan(conn, slug, project, "br", "desc", None, None, &[]).unwrap();
        let (step, _) = storage::create_step(
            conn, &plan.id, "t", "d", None, None, &[], None, None, None,
        )
        .unwrap();
        (plan.id, step.id)
    }

    #[test]
    fn test_status_with_live_run_populates_json_live_field() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-status-live";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "live-plan", project);

        // Seed a run_locks row with live observability data.
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug,
                                    step_id, step_num, attempt, max_attempts, phase,
                                    phase_started_at, current_command)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            params![
                project,
                12345i64,
                "tok",
                plan_id,
                "live-plan",
                step_id,
                1i32,
                2i32,
                4i32,
                Phase::Tests.as_str(),
                "2026-04-21T17:23:10.000Z",
                "cargo test",
            ],
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "live-plan", project)
            .unwrap()
            .unwrap();
        let (summary, _steps) = build_status_summary(&conn, project, &plan).unwrap();

        let live = summary.live.clone().expect("live field should be populated");
        assert_eq!(live.pid, 12345);
        assert_eq!(live.phase, Some(Phase::Tests));
        assert_eq!(live.attempt, Some(2));
        assert_eq!(live.max_attempts, Some(4));
        assert_eq!(live.current_command.as_deref(), Some("cargo test"));

        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"live\":{"));
        assert!(json.contains("\"pid\":12345"));
        assert!(json.contains("\"phase\":\"tests\""));
    }

    #[test]
    fn test_status_without_live_run_omits_live_field() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-status-nolive";
        let (_plan_id, _step_id) = seed_plan_and_step(&conn, "quiet-plan", project);

        let plan = storage::get_plan_by_slug(&conn, "quiet-plan", project)
            .unwrap()
            .unwrap();
        let (summary, _) = build_status_summary(&conn, project, &plan).unwrap();
        assert!(summary.live.is_none());

        let json = serde_json::to_string(&summary).unwrap();
        assert!(
            !json.contains("\"live\""),
            "expected live field to be omitted from JSON, got: {json}"
        );
    }

    #[test]
    fn test_status_plan_mismatch_omits_live_field() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-status-mismatch";
        let (plan_a_id, _step_a) = seed_plan_and_step(&conn, "plan-a", project);
        let (_plan_b_id, _step_b) = seed_plan_and_step(&conn, "plan-b", project);

        // The live run is for plan-a.
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug, phase)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                project,
                12345i64,
                "tok",
                plan_a_id,
                "plan-a",
                Phase::Harness.as_str(),
            ],
        )
        .unwrap();

        // But we query status for plan-b.
        let plan_b = storage::get_plan_by_slug(&conn, "plan-b", project)
            .unwrap()
            .unwrap();
        let (summary, _) = build_status_summary(&conn, project, &plan_b).unwrap();
        assert!(
            summary.live.is_none(),
            "live run is for a different plan; queried plan should not see it"
        );
    }

    #[test]
    fn test_status_live_with_unbound_plan_id_still_attaches() {
        // An unbound lock (plan_id NULL) covers the whole project; we should
        // attach it to whatever plan is queried rather than silently hiding
        // the live snapshot.
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-status-unbound";
        let (_plan_id, _step_id) = seed_plan_and_step(&conn, "any-plan", project);

        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, phase)
             VALUES (?1, ?2, ?3, ?4)",
            params![project, 12345i64, "tok", Phase::Idle.as_str()],
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "any-plan", project)
            .unwrap()
            .unwrap();
        let (summary, _) = build_status_summary(&conn, project, &plan).unwrap();
        assert!(
            summary.live.is_some(),
            "unbound live lock should attach to any plan queried"
        );
    }

    #[test]
    fn test_cmd_log_json_includes_termination_reason() {
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-log-reason";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "log-plan", project);

        // Seed an execution_log row with termination_reason + test_status set.
        let log = storage::create_execution_log(&conn, &step_id, 1, None, None).unwrap();
        storage::update_execution_log(
            &conn,
            log.id,
            Some(1.0),
            None,
            &[],
            false,
            false,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(TerminationReason::UserInterrupted),
            Some(TestStatus::Passed),
        )
        .unwrap();

        // Round-trip through the same projection cmd_log uses when emitting
        // NDJSON. Verifies the fields flow through LogEntrySummary::new into
        // the JSON payload.
        let logs = storage::list_execution_logs_for_step(&conn, &step_id).unwrap();
        assert_eq!(logs.len(), 1);
        let summary =
            output::LogEntrySummary::new(&logs[0], &LogOutputMode::Hidden);
        let json = serde_json::to_string(&summary).unwrap();
        assert!(json.contains("\"termination_reason\":\"user_interrupted\""));
        assert!(json.contains("\"test_status\":\"passed\""));
    }

    #[test]
    fn test_render_live_block_formats_current_section() {
        // Exercise the plain-text rendering path so the live block format
        // contract is guarded by a test. Uses a phase_started_at a few
        // seconds in the past so the `(Ns)` tag shows up.
        let started = chrono::Utc::now() - chrono::Duration::seconds(12);
        let live = output::LiveRunDisplay {
            pid: 12345,
            plan_slug: Some("plan".into()),
            started_at: "2026-04-21T17:23:10.000Z".into(),
            step_id: Some("step-uuid".into()),
            step_num: Some(3),
            attempt: Some(2),
            max_attempts: Some(4),
            phase: Some(Phase::Tests),
            phase_started_at: Some(
                started.to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
            ),
            phase_elapsed_secs: Some(12.0),
            current_command: Some("cargo test".into()),
            child_pid: Some(54321),
        };
        // Seed a minimal fake step list matching the live.step_id so the
        // title resolves.
        let fake_step = crate::plan::Step {
            id: "step-uuid".into(),
            plan_id: "p".into(),
            sort_key: "a0".into(),
            title: "Add repository types".into(),
            description: "".into(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: crate::plan::StepStatus::InProgress,
            attempts: 2,
            max_retries: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: crate::plan::ChangePolicy::Required,
        };
        let rendered = render_live_block(&live, std::slice::from_ref(&fake_step));
        assert!(rendered.contains("Current:"));
        assert!(rendered.contains("step: 3/1 \"Add repository types\""));
        assert!(rendered.contains("phase: tests"));
        assert!(rendered.contains("attempt: 2/4"));
        assert!(rendered.contains("command: cargo test"));
        assert!(rendered.contains("runner: pid 12345"));
    }

    /// Finding 3 regression: after the harness phase ends and `update_live_phase`
    /// is called with `ChildUpdate::Clear` (simulating the Tests phase), the
    /// emitted status JSON must not advertise the dead harness pid. With
    /// `skip_serializing_if = "Option::is_none"` the field is omitted entirely.
    #[test]
    fn test_status_live_child_pid_clears_after_harness_phase() {
        use crate::storage::ChildUpdate;
        let conn = db::open_memory().unwrap();
        let project = "/tmp/proj-status-clear-child";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "clear-plan", project);

        // Seed a run_locks row representing a live run currently in the
        // Harness phase, with child_pid set.
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug,
                                    step_id, step_num, attempt, max_attempts, phase,
                                    phase_started_at, child_pid, child_start_token)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)",
            params![
                project,
                12345i64,
                "tok",
                plan_id,
                "clear-plan",
                step_id,
                1i32,
                1i32,
                1i32,
                Phase::Harness.as_str(),
                "2026-04-21T17:23:10.000Z",
                98_765i64,
                "child-tok",
            ],
        )
        .unwrap();

        // Simulate the Tests phase write, which clears the child columns.
        storage::update_live_phase(
            &conn,
            project,
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

        let plan = storage::get_plan_by_slug(&conn, "clear-plan", project)
            .unwrap()
            .unwrap();
        let (summary, _steps) = build_status_summary(&conn, project, &plan).unwrap();

        let live = summary.live.clone().expect("live field should be populated");
        assert_eq!(
            live.child_pid, None,
            "child_pid must be cleared once the harness phase ends",
        );

        // With `skip_serializing_if = "Option::is_none"`, the field is
        // absent from the JSON payload entirely.
        let json = serde_json::to_string(&summary).unwrap();
        assert!(
            !json.contains("child_pid"),
            "cleared child_pid must be absent from status JSON: {json}"
        );
    }
}
