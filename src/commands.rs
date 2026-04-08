// Plan and step CLI command implementations

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use std::io::{self, Write};
use std::path::Path;

use crate::config;
use crate::db;
use crate::frac_index;
use crate::plan::{self, ExecutionLog, PlanStatus};
use crate::preflight;
use crate::storage;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Resolve the project directory to a canonical absolute path string.
pub fn resolve_project(project: Option<&Path>) -> Result<String> {
    let dir = match project {
        Some(p) => p.to_path_buf(),
        None => std::env::current_dir().context("Failed to get current directory")?,
    };
    let canonical = dir
        .canonicalize()
        .with_context(|| format!("Cannot resolve project path: {}", dir.display()))?;
    Ok(canonical.to_string_lossy().into_owned())
}

/// Status indicator symbols with ANSI colors.
fn status_icon(status: &str) -> &'static str {
    match status {
        "planning" => "\x1b[33m◯\x1b[0m",    // yellow circle
        "ready" => "\x1b[36m◉\x1b[0m",       // cyan filled circle
        "in_progress" => "\x1b[34m▶\x1b[0m", // blue play
        "complete" => "\x1b[32m✔\x1b[0m",    // green check
        "failed" => "\x1b[31m✘\x1b[0m",      // red X
        "aborted" => "\x1b[31m⊘\x1b[0m",     // red circle-slash
        "pending" => "\x1b[90m○\x1b[0m",     // gray circle
        "skipped" => "\x1b[90m⊘\x1b[0m",     // gray circle-slash
        "archived" => "\x1b[90m▪\x1b[0m",    // gray square
        _ => "?",
    }
}

/// Colored status text.
fn colored_status(status: &str) -> String {
    let color = match status {
        "planning" => "\x1b[33m",    // yellow
        "ready" => "\x1b[36m",       // cyan
        "in_progress" => "\x1b[34m", // blue
        "complete" => "\x1b[32m",    // green
        "failed" => "\x1b[31m",      // red
        "aborted" => "\x1b[31m",     // red
        "pending" => "\x1b[90m",     // gray
        "skipped" => "\x1b[90m",     // gray
        "archived" => "\x1b[90m",    // gray
        _ => "\x1b[0m",
    };
    format!("{color}{status}\x1b[0m")
}

// ---------------------------------------------------------------------------
// Plan commands
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn plan_create(
    conn: &Connection,
    slug: &str,
    project: &str,
    description: Option<&str>,
    branch: Option<&str>,
    harness: Option<&str>,
    agent: Option<&str>,
    tests: &[String],
) -> Result<()> {
    let desc = description.unwrap_or(slug);
    let branch_name = branch.unwrap_or(slug);

    let plan = storage::create_plan(
        conn,
        slug,
        project,
        branch_name,
        desc,
        harness,
        agent,
        tests,
    )?;

    println!(
        "{} Created plan: \x1b[1m{}\x1b[0m",
        status_icon("complete"),
        plan.slug
    );
    if !tests.is_empty() {
        println!("  Tests: {}", tests.join(", "));
    }
    Ok(())
}

pub fn plan_list(
    conn: &Connection,
    project: &str,
    all: bool,
    status: Option<&str>,
    show_archived: bool,
) -> Result<()> {
    let plans = storage::list_plans(conn, project, all)?;

    if plans.is_empty() {
        println!("No plans found.");
        return Ok(());
    }

    // Filter by status if provided, otherwise hide archived unless --archived
    let plans: Vec<_> = if let Some(s) = status {
        let target: PlanStatus = s
            .parse()
            .map_err(|_| anyhow::anyhow!("Invalid status: {s}"))?;
        plans.into_iter().filter(|p| p.status == target).collect()
    } else if !show_archived {
        plans
            .into_iter()
            .filter(|p| p.status != PlanStatus::Archived)
            .collect()
    } else {
        plans
    };

    if plans.is_empty() {
        println!("No plans match the filter.");
        return Ok(());
    }

    for plan in &plans {
        let status_str = plan.status.as_str();
        println!(
            "  {} \x1b[1m{}\x1b[0m  {}  [{}]",
            status_icon(status_str),
            plan.slug,
            plan.description,
            colored_status(status_str),
        );
        if all {
            println!("    project: {}", plan.project);
        }
    }

    Ok(())
}

pub fn plan_show(conn: &Connection, slug: &str, project: &str) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    let status_str = plan.status.as_str();
    println!(
        "\x1b[1m{}\x1b[0m  {}",
        plan.slug,
        colored_status(status_str)
    );
    println!("  Description: {}", plan.description);
    println!("  Branch:      {}", plan.branch_name);
    println!("  Project:     {}", plan.project);
    if let Some(ref h) = plan.harness {
        println!("  Harness:     {h}");
    }
    if let Some(ref a) = plan.agent {
        println!("  Agent:       {a}");
    }
    if !plan.deterministic_tests.is_empty() {
        println!("  Tests:");
        for t in &plan.deterministic_tests {
            println!("    - {t}");
        }
    }
    println!(
        "  Created:     {}",
        plan.created_at.format("%Y-%m-%d %H:%M:%S UTC")
    );

    // Show steps
    let steps = storage::list_steps(conn, &plan.id)?;
    if !steps.is_empty() {
        println!();
        println!("  Steps:");
        for (i, step) in steps.iter().enumerate() {
            let ss = step.status.as_str();
            println!(
                "    {:>3}. {} {} [{}]",
                i + 1,
                status_icon(ss),
                step.title,
                colored_status(ss),
            );
        }
    }

    Ok(())
}

pub fn plan_approve(conn: &Connection, slug: &str, project: &str) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    if plan.status != PlanStatus::Planning {
        bail!(
            "Plan '{}' is in status '{}', can only approve plans in 'planning' status",
            slug,
            plan.status
        );
    }

    storage::update_plan_status(conn, &plan.id, PlanStatus::Ready)?;
    println!(
        "{} Plan '{}' approved and ready for execution",
        status_icon("complete"),
        slug
    );
    Ok(())
}

pub fn plan_archive(conn: &Connection, slug: &str, project: &str) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    match plan.status {
        PlanStatus::Complete | PlanStatus::Failed | PlanStatus::Aborted => {}
        _ => bail!(
            "Plan '{}' is in status '{}'; only complete, failed, or aborted plans can be archived",
            slug,
            plan.status
        ),
    }

    storage::update_plan_status(conn, &plan.id, PlanStatus::Archived)?;
    println!("{} Archived plan '{}'", status_icon("archived"), slug);
    Ok(())
}

pub fn plan_unarchive(conn: &Connection, slug: &str, project: &str) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    if plan.status != PlanStatus::Archived {
        bail!(
            "Plan '{}' is not archived (status: '{}')",
            slug,
            plan.status
        );
    }

    // Restore to complete — the most neutral terminal state.
    storage::update_plan_status(conn, &plan.id, PlanStatus::Complete)?;
    println!(
        "{} Unarchived plan '{}' (status: complete)",
        status_icon("complete"),
        slug
    );
    Ok(())
}

pub fn plan_delete(conn: &Connection, slug: &str, project: &str, force: bool) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, slug, project)?
        .with_context(|| format!("Plan not found: {slug}"))?;

    if !force {
        print!("Delete plan '{}' and all its steps/logs? [y/N] ", slug);
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    storage::delete_plan(conn, &plan.id)?;
    println!("{} Deleted plan '{}'", status_icon("complete"), slug);
    Ok(())
}

// ---------------------------------------------------------------------------
// Step commands
// ---------------------------------------------------------------------------

pub fn step_list(conn: &Connection, plan_slug: &str, project: &str) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let steps = storage::list_steps(conn, &plan.id)?;

    if steps.is_empty() {
        println!("No steps in plan '{}'.", plan_slug);
        return Ok(());
    }

    println!(
        "Steps for \x1b[1m{}\x1b[0m ({} total):",
        plan_slug,
        steps.len()
    );
    for (i, step) in steps.iter().enumerate() {
        let ss = step.status.as_str();
        println!(
            "  {:>3}. {} \x1b[1m{}\x1b[0m  [{}]",
            i + 1,
            status_icon(ss),
            step.title,
            colored_status(ss),
        );
        if !step.description.is_empty() {
            println!("       {}", step.description);
        }
        if step.attempts > 0 {
            println!("       attempts: {}", step.attempts);
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn step_add(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    title: &str,
    description: Option<&str>,
    after: Option<usize>,
    agent: Option<&str>,
    harness: Option<&str>,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let desc = description.unwrap_or("");

    let step = if let Some(after_pos) = after {
        // Insert after a specific position using fractional indexing
        let steps = storage::list_steps(conn, &plan.id)?;
        if after_pos > steps.len() {
            bail!(
                "Position {} is out of range (plan has {} steps)",
                after_pos,
                steps.len()
            );
        }

        let sort_key = if after_pos == 0 {
            // Insert before the first step
            if steps.is_empty() {
                frac_index::initial_key()
            } else {
                let first_key = &steps[0].sort_key;
                if first_key.as_str() > "0" {
                    frac_index::key_between("0", first_key)
                } else {
                    "00".to_string()
                }
            }
        } else if after_pos == steps.len() {
            // Append at end
            frac_index::key_after(&steps[steps.len() - 1].sort_key)
        } else {
            // Insert between after_pos-1 and after_pos
            let before = &steps[after_pos - 1].sort_key;
            let after_key = &steps[after_pos].sort_key;
            frac_index::key_between(before, after_key)
        };

        storage::create_step_at(
            conn,
            &plan.id,
            &sort_key,
            title,
            desc,
            agent,
            harness,
            &[],
            None,
        )?
    } else {
        // Append at the end (default)
        storage::create_step(conn, &plan.id, title, desc, agent, harness, &[], None)?
    };

    // Determine the position
    let steps = storage::list_steps(conn, &plan.id)?;
    let pos = steps
        .iter()
        .position(|s| s.id == step.id)
        .map(|i| i + 1)
        .unwrap_or(0);

    println!(
        "{} Added step #{}: \x1b[1m{}\x1b[0m",
        status_icon("complete"),
        pos,
        step.title
    );
    Ok(())
}

pub fn step_remove(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_num: usize,
    force: bool,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let steps = storage::list_steps(conn, &plan.id)?;
    if step_num == 0 || step_num > steps.len() {
        bail!(
            "Step {} is out of range (plan has {} steps)",
            step_num,
            steps.len()
        );
    }

    let step = &steps[step_num - 1];

    if !force {
        print!("Remove step #{} '{}'? [y/N] ", step_num, step.title);
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        if !input.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    storage::delete_step(conn, &step.id)?;
    println!(
        "{} Removed step #{}: {}",
        status_icon("complete"),
        step_num,
        step.title
    );
    Ok(())
}

pub fn step_edit(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_num: usize,
    title: Option<&str>,
    description: Option<&str>,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let steps = storage::list_steps(conn, &plan.id)?;
    if step_num == 0 || step_num > steps.len() {
        bail!(
            "Step {} is out of range (plan has {} steps)",
            step_num,
            steps.len()
        );
    }

    let step = &steps[step_num - 1];

    if title.is_none() && description.is_none() {
        bail!("Nothing to edit: provide --title and/or --description");
    }

    storage::update_step_fields(conn, &step.id, title, description)?;
    println!(
        "{} Updated step #{}: {}",
        status_icon("complete"),
        step_num,
        title.unwrap_or(&step.title)
    );
    Ok(())
}

pub fn step_reset(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_num: usize,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let steps = storage::list_steps(conn, &plan.id)?;
    if step_num == 0 || step_num > steps.len() {
        bail!(
            "Step {} is out of range (plan has {} steps)",
            step_num,
            steps.len()
        );
    }

    let step = &steps[step_num - 1];
    storage::reset_step(conn, &step.id)?;
    println!(
        "{} Reset step #{} '{}' to pending (0 attempts)",
        status_icon("complete"),
        step_num,
        step.title
    );
    Ok(())
}

pub fn step_move(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_num: usize,
    to: usize,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let steps = storage::list_steps(conn, &plan.id)?;
    if step_num == 0 || step_num > steps.len() {
        bail!(
            "Step {} is out of range (plan has {} steps)",
            step_num,
            steps.len()
        );
    }
    if to == 0 || to > steps.len() {
        bail!(
            "Target position {} is out of range (plan has {} steps)",
            to,
            steps.len()
        );
    }
    if step_num == to {
        println!("Step is already at position {}.", to);
        return Ok(());
    }

    let step = &steps[step_num - 1];

    // Calculate the new sort_key for the target position.
    // We need a key that places the step at position `to` (1-based)
    // after removing it from its current position.
    let target_idx = to - 1; // 0-based target index

    // Build a list of sort keys excluding the step being moved
    let other_keys: Vec<&str> = steps
        .iter()
        .filter(|s| s.id != step.id)
        .map(|s| s.sort_key.as_str())
        .collect();

    let new_sort_key = if target_idx == 0 {
        // Move to first position: need a key before the first remaining step
        if other_keys.is_empty() {
            frac_index::initial_key()
        } else {
            let first = other_keys[0];
            // Use "0" as a synthetic lower bound; it sorts before any key
            // starting with a digit > '0' or a letter.
            if first > "0" {
                frac_index::key_between("0", first)
            } else {
                // Extremely unlikely: first key is "0". Prepend with shorter key.
                "00".to_string()
            }
        }
    } else if target_idx >= other_keys.len() {
        // Move to last position
        frac_index::key_after(other_keys[other_keys.len() - 1])
    } else {
        // Move between two existing steps
        let before = other_keys[target_idx - 1];
        let after_key = other_keys[target_idx];
        frac_index::key_between(before, after_key)
    };

    storage::update_step_sort_key(conn, &step.id, &new_sort_key)?;
    println!(
        "{} Moved step '{}' to position {}",
        status_icon("complete"),
        step.title,
        to
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Init command
// ---------------------------------------------------------------------------

pub fn cmd_init() -> Result<()> {
    use std::fs;

    // Create config dir
    let config_dir = config::config_dir()?;
    fs::create_dir_all(&config_dir)
        .with_context(|| format!("Failed to create config directory {}", config_dir.display()))?;
    println!(
        "\x1b[32m\u{2714}\x1b[0m Config directory: {}",
        config_dir.display()
    );

    // Create agents dir
    let agents_dir = config::agents_dir()?;
    fs::create_dir_all(&agents_dir)
        .with_context(|| format!("Failed to create agents directory {}", agents_dir.display()))?;
    println!(
        "\x1b[32m\u{2714}\x1b[0m Agents directory: {}",
        agents_dir.display()
    );

    // Create default config file if it doesn't exist
    let config_path = config_dir.join("config.json");
    if !config_path.exists() {
        let default_config = config::Config::default();
        let json = serde_json::to_string_pretty(&default_config)?;
        fs::write(&config_path, &json)
            .with_context(|| format!("Failed to write config to {}", config_path.display()))?;
        println!(
            "\x1b[32m\u{2714}\x1b[0m Default config: {}",
            config_path.display()
        );
    } else {
        println!(
            "\x1b[32m\u{2714}\x1b[0m Config exists: {}",
            config_path.display()
        );
    }

    // Initialize database
    let _conn = db::open()?;
    let db_path = db::db_path()?;
    println!("\x1b[32m\u{2714}\x1b[0m Database: {}", db_path.display());

    println!();
    println!("ralph-rs initialized successfully.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Doctor command
// ---------------------------------------------------------------------------

pub fn cmd_doctor(config: &config::Config) -> Result<()> {
    println!("ralph-rs doctor");
    println!();

    let checks = preflight::run_doctor_checks(config);

    let mut has_errors = false;
    for check in &checks {
        let icon = match check.severity {
            preflight::CheckSeverity::Pass => "\x1b[32m\u{2714}\x1b[0m",
            preflight::CheckSeverity::Warning => "\x1b[33m\u{26a0}\x1b[0m",
            preflight::CheckSeverity::Error => {
                has_errors = true;
                "\x1b[31m\u{2718}\x1b[0m"
            }
        };
        println!("  {} {}: {}", icon, check.name, check.message);
    }

    println!();
    if has_errors {
        println!("Some checks failed. Please fix the issues above.");
    } else {
        println!("All checks passed.");
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Status command
// ---------------------------------------------------------------------------

pub fn cmd_status(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    verbose: bool,
) -> Result<()> {
    let plan = if let Some(slug) = plan_slug {
        storage::get_plan_by_slug(conn, slug, project)?
            .with_context(|| format!("Plan not found: {slug}"))?
    } else {
        // Find the most recent active plan (in_progress, ready, or failed).
        let plans = storage::list_plans(conn, project, false)?;
        let active = plans.into_iter().find(|p| {
            matches!(
                p.status,
                plan::PlanStatus::InProgress | plan::PlanStatus::Ready | plan::PlanStatus::Failed
            )
        });
        match active {
            Some(p) => p,
            None => {
                println!("No active plan found. Use --plan to specify a plan slug.");
                return Ok(());
            }
        }
    };

    let status_str = plan.status.as_str();
    let color = match plan.status {
        plan::PlanStatus::Planning => "\x1b[33m",
        plan::PlanStatus::Ready => "\x1b[36m",
        plan::PlanStatus::InProgress => "\x1b[34m",
        plan::PlanStatus::Complete => "\x1b[32m",
        plan::PlanStatus::Failed => "\x1b[31m",
        plan::PlanStatus::Aborted => "\x1b[31m",
        plan::PlanStatus::Archived => "\x1b[90m",
    };

    println!(
        "\x1b[1m{}\x1b[0m  {}{}\x1b[0m",
        plan.slug, color, status_str
    );
    println!("  Branch: {}", plan.branch_name);

    let steps = storage::list_steps(conn, &plan.id)?;
    if steps.is_empty() {
        println!("  No steps.");
        return Ok(());
    }

    let total = steps.len();
    let complete = steps
        .iter()
        .filter(|s| s.status == plan::StepStatus::Complete)
        .count();
    let failed = steps
        .iter()
        .filter(|s| s.status == plan::StepStatus::Failed)
        .count();
    let skipped = steps
        .iter()
        .filter(|s| s.status == plan::StepStatus::Skipped)
        .count();
    let pending = steps
        .iter()
        .filter(|s| s.status == plan::StepStatus::Pending)
        .count();
    let in_progress = steps
        .iter()
        .filter(|s| s.status == plan::StepStatus::InProgress)
        .count();

    println!(
        "  Progress: {}/{} complete, {} failed, {} skipped, {} pending, {} in-progress",
        complete, total, failed, skipped, pending, in_progress
    );

    if verbose {
        println!();
        for (i, step) in steps.iter().enumerate() {
            let ss = step.status.as_str();
            let icon = match step.status {
                plan::StepStatus::Pending => "\x1b[90m\u{25cb}\x1b[0m",
                plan::StepStatus::InProgress => "\x1b[34m\u{25b6}\x1b[0m",
                plan::StepStatus::Complete => "\x1b[32m\u{2714}\x1b[0m",
                plan::StepStatus::Failed => "\x1b[31m\u{2718}\x1b[0m",
                plan::StepStatus::Skipped => "\x1b[90m\u{2298}\x1b[0m",
                plan::StepStatus::Aborted => "\x1b[31m\u{2298}\x1b[0m",
            };
            println!(
                "  {:>3}. {} {} [{}] (attempts: {})",
                i + 1,
                icon,
                step.title,
                ss,
                step.attempts,
            );
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Log command
// ---------------------------------------------------------------------------

pub fn cmd_log(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    step_num: Option<usize>,
    limit: Option<usize>,
    full: bool,
) -> Result<()> {
    // Resolve plan
    let plan = if let Some(slug) = plan_slug {
        storage::get_plan_by_slug(conn, slug, project)?
            .with_context(|| format!("Plan not found: {slug}"))?
    } else {
        let plans = storage::list_plans(conn, project, false)?;
        let active = plans.into_iter().find(|p| {
            matches!(
                p.status,
                plan::PlanStatus::InProgress
                    | plan::PlanStatus::Ready
                    | plan::PlanStatus::Failed
                    | plan::PlanStatus::Complete
            )
        });
        match active {
            Some(p) => p,
            None => {
                println!("No plan found. Use --plan to specify a plan slug.");
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

        println!(
            "Logs for step #{} '{}' ({} attempts):",
            step_idx,
            step.title,
            logs.len()
        );
        println!();

        for log in &logs {
            print_log_entry(&step.title, log, full);
        }
    } else {
        // Show all logs for the plan
        let entries = storage::list_execution_logs_for_plan(conn, &plan.id, limit)?;

        if entries.is_empty() {
            println!("No execution logs for plan '{}'.", plan.slug);
            return Ok(());
        }

        println!(
            "Execution logs for plan '{}' ({} entries):",
            plan.slug,
            entries.len()
        );
        println!();

        for (step_title, log) in &entries {
            print_log_entry(step_title, log, full);
        }
    }

    Ok(())
}

fn print_log_entry(step_title: &str, log: &ExecutionLog, full: bool) {
    let status_icon = if log.committed {
        "\x1b[32m\u{2714}\x1b[0m"
    } else if log.rolled_back {
        "\x1b[31m\u{21ba}\x1b[0m"
    } else {
        "\x1b[90m\u{25cb}\x1b[0m"
    };

    let duration_str = log
        .duration_secs
        .map(|d| format!("{:.1}s", d))
        .unwrap_or_else(|| "-".to_string());

    println!(
        "  {} [attempt {}] {} ({}) {}",
        status_icon,
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

    if full {
        if let Some(ref stdout) = log.harness_stdout
            && !stdout.is_empty()
        {
            println!("    --- stdout ---");
            for line in stdout.lines().take(50) {
                println!("    {line}");
            }
        }
        if let Some(ref stderr) = log.harness_stderr
            && !stderr.is_empty()
        {
            println!("    --- stderr ---");
            for line in stderr.lines().take(50) {
                println!("    {line}");
            }
        }
    }

    println!();
}

// ---------------------------------------------------------------------------
// Agents commands
// ---------------------------------------------------------------------------

pub fn cmd_agents_list() -> Result<()> {
    let agents_dir = config::agents_dir()?;

    if !agents_dir.exists() {
        println!("Agents directory not found: {}", agents_dir.display());
        println!("Run `ralph-rs init` to create it.");
        return Ok(());
    }

    let mut found = false;
    let mut entries: Vec<_> = std::fs::read_dir(&agents_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "md"))
        .collect();

    entries.sort_by_key(|e| e.file_name());

    for entry in &entries {
        let name = entry
            .file_name()
            .to_string_lossy()
            .trim_end_matches(".md")
            .to_string();
        let metadata = entry.metadata().ok();
        let size = metadata.map(|m| m.len()).unwrap_or(0);
        println!("  {} ({} bytes)", name, size);
        found = true;
    }

    if !found {
        println!("No agent files found in {}", agents_dir.display());
    }

    Ok(())
}

pub fn cmd_agents_show(name: &str) -> Result<()> {
    let agents_dir = config::agents_dir()?;
    let path = agents_dir.join(format!("{name}.md"));

    if !path.exists() {
        anyhow::bail!("Agent file not found: {}", path.display());
    }

    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    println!("{contents}");
    Ok(())
}

pub fn cmd_agents_create(name: &str, file: Option<&std::path::Path>) -> Result<()> {
    let agents_dir = config::agents_dir()?;
    std::fs::create_dir_all(&agents_dir)?;
    let path = agents_dir.join(format!("{name}.md"));

    if path.exists() {
        anyhow::bail!("Agent file already exists: {}", path.display());
    }

    let contents = if let Some(src) = file {
        std::fs::read_to_string(src).with_context(|| format!("Failed to read {}", src.display()))?
    } else {
        format!("# {name}\n\nAgent instructions go here.\n")
    };

    std::fs::write(&path, &contents)
        .with_context(|| format!("Failed to write {}", path.display()))?;
    println!("Created agent file: {}", path.display());
    Ok(())
}

pub fn cmd_agents_delete(name: &str) -> Result<()> {
    let agents_dir = config::agents_dir()?;
    let path = agents_dir.join(format!("{name}.md"));

    if !path.exists() {
        anyhow::bail!("Agent file not found: {}", path.display());
    }

    std::fs::remove_file(&path).with_context(|| format!("Failed to delete {}", path.display()))?;
    println!("Deleted agent file: {name}");
    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::plan::StepStatus;

    fn setup() -> (Connection, String) {
        let conn = db::open_memory().expect("open_memory");
        let project = "/tmp/test-project".to_string();
        (conn, project)
    }

    #[test]
    fn test_plan_create_and_list() {
        let (conn, project) = setup();

        plan_create(
            &conn,
            "my-plan",
            &project,
            Some("A test plan"),
            Some("feat/test"),
            None,
            None,
            &["cargo build".to_string()],
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        assert_eq!(plan.slug, "my-plan");
        assert_eq!(plan.description, "A test plan");
        assert_eq!(plan.branch_name, "feat/test");
        assert_eq!(plan.deterministic_tests, vec!["cargo build"]);
    }

    #[test]
    fn test_plan_approve() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[]).unwrap();
        plan_approve(&conn, "my-plan", &project).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        assert_eq!(plan.status, PlanStatus::Ready);
    }

    #[test]
    fn test_plan_approve_rejects_non_planning() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[]).unwrap();
        plan_approve(&conn, "my-plan", &project).unwrap();

        // Second approve should fail - plan is now ready, not planning
        let result = plan_approve(&conn, "my-plan", &project);
        assert!(result.is_err());
    }

    #[test]
    fn test_plan_delete_forced() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[]).unwrap();
        plan_delete(&conn, "my-plan", &project, true).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project).unwrap();
        assert!(plan.is_none());
    }

    #[test]
    fn test_step_add_and_list() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[]).unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "First step",
            Some("Do something"),
            None,
            None,
            None,
        )
        .unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "Second step",
            Some("Do another thing"),
            None,
            None,
            None,
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 2);
        assert_eq!(steps[0].title, "First step");
        assert_eq!(steps[1].title, "Second step");
    }

    #[test]
    fn test_step_add_after() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[]).unwrap();
        step_add(&conn, "my-plan", &project, "First", None, None, None, None).unwrap();
        step_add(&conn, "my-plan", &project, "Third", None, None, None, None).unwrap();
        // Insert after position 1
        step_add(
            &conn,
            "my-plan",
            &project,
            "Second",
            None,
            Some(1),
            None,
            None,
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 3);
        assert_eq!(steps[0].title, "First");
        assert_eq!(steps[1].title, "Second");
        assert_eq!(steps[2].title, "Third");
    }

    #[test]
    fn test_step_remove_forced() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[]).unwrap();
        step_add(&conn, "my-plan", &project, "First", None, None, None, None).unwrap();
        step_add(&conn, "my-plan", &project, "Second", None, None, None, None).unwrap();

        step_remove(&conn, "my-plan", &project, 2, true).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].title, "First");
    }

    #[test]
    fn test_step_edit() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[]).unwrap();
        step_add(
            &conn,
            "my-plan",
            &project,
            "Old title",
            None,
            None,
            None,
            None,
        )
        .unwrap();

        step_edit(
            &conn,
            "my-plan",
            &project,
            1,
            Some("New title"),
            Some("New desc"),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].title, "New title");
        assert_eq!(steps[0].description, "New desc");
    }

    #[test]
    fn test_step_reset() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[]).unwrap();
        step_add(&conn, "my-plan", &project, "Step", None, None, None, None).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        storage::update_step_status(&conn, &steps[0].id, StepStatus::Failed).unwrap();

        step_reset(&conn, "my-plan", &project, 1).unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Pending);
        assert_eq!(steps[0].attempts, 0);
    }

    #[test]
    fn test_step_move() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[]).unwrap();
        step_add(&conn, "my-plan", &project, "A", None, None, None, None).unwrap();
        step_add(&conn, "my-plan", &project, "B", None, None, None, None).unwrap();
        step_add(&conn, "my-plan", &project, "C", None, None, None, None).unwrap();

        // Move step 3 (C) to position 1
        step_move(&conn, "my-plan", &project, 3, 1).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].title, "C");
        assert_eq!(steps[1].title, "A");
        assert_eq!(steps[2].title, "B");
    }

    #[test]
    fn test_step_move_to_end() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[]).unwrap();
        step_add(&conn, "my-plan", &project, "A", None, None, None, None).unwrap();
        step_add(&conn, "my-plan", &project, "B", None, None, None, None).unwrap();
        step_add(&conn, "my-plan", &project, "C", None, None, None, None).unwrap();

        // Move step 1 (A) to position 3
        step_move(&conn, "my-plan", &project, 1, 3).unwrap();

        let plan = storage::get_plan_by_slug(&conn, "my-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].title, "B");
        assert_eq!(steps[1].title, "C");
        assert_eq!(steps[2].title, "A");
    }

    #[test]
    fn test_step_out_of_range() {
        let (conn, project) = setup();

        plan_create(&conn, "my-plan", &project, None, None, None, None, &[]).unwrap();
        step_add(&conn, "my-plan", &project, "Step", None, None, None, None).unwrap();

        let result = step_remove(&conn, "my-plan", &project, 5, true);
        assert!(result.is_err());
    }
}
