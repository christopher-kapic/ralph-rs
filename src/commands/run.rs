// Run-related CLI command implementations (status, log)

use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::output::{self, OutputContext, OutputFormat};
use crate::plan::{ExecutionLog, StepStatus};
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

    if out.format == OutputFormat::Json {
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
        };
        println!("{}", serde_json::to_string(&summary)?);
        return Ok(());
    }

    println!(
        "{}  {}",
        output::bold(&plan.slug, out.color),
        output::colored_plan_status(plan.status, out.color),
    );
    println!("  Branch: {}", plan.branch_name);

    if steps.is_empty() {
        println!("  No steps.");
        return Ok(());
    }

    println!(
        "  Progress: {}/{} complete, {} failed, {} skipped, {} pending, {} in-progress",
        complete, total, failed, skipped, pending, in_progress
    );

    if verbose {
        println!();
        for (i, step) in steps.iter().enumerate() {
            println!(
                "  {:>3}. {} {} [{}] (attempts: {})",
                i + 1,
                output::status_icon(step.status, out.color),
                step.title,
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

    Ok(())
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

    if !log.test_results.is_empty() {
        println!("    tests: {}", log.test_results.join(", "));
    }

    if let Some(cost) = log.cost_usd {
        let tokens = match (log.input_tokens, log.output_tokens) {
            (Some(i), Some(o)) => format!(" ({i} in / {o} out tokens)"),
            _ => String::new(),
        };
        println!("    cost: ${:.4}{}", cost, tokens);
    }

    if !matches!(output_mode, LogOutputMode::Hidden) {
        let take_n = match output_mode {
            LogOutputMode::Truncated(n) => Some(*n),
            _ => None,
        };
        if let Some(ref stdout) = log.harness_stdout
            && !stdout.is_empty()
        {
            println!("    --- stdout ---");
            let lines_iter = stdout.lines();
            let lines: Box<dyn Iterator<Item = &str>> = match take_n {
                Some(n) => Box::new(lines_iter.take(n)),
                None => Box::new(lines_iter),
            };
            for line in lines {
                println!("    {line}");
            }
        }
        if let Some(ref stderr) = log.harness_stderr
            && !stderr.is_empty()
        {
            println!("    --- stderr ---");
            let lines_iter = stderr.lines();
            let lines: Box<dyn Iterator<Item = &str>> = match take_n {
                Some(n) => Box::new(lines_iter.take(n)),
                None => Box::new(lines_iter),
            };
            for line in lines {
                println!("    {line}");
            }
        }
    }

    println!();
}
