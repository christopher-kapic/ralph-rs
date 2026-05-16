// Step CLI command implementations (CRUD, move, hooks)

use anyhow::{Context, Result, bail};
use rusqlite::Connection;
use std::io::Read;

use crate::config::Config;
use crate::frac_index;
use crate::hook_library::{self, Lifecycle};
use crate::import::ImportedStep;
use crate::output::{self, OutputContext, OutputFormat};
use crate::plan::{ChangePolicy, RetryStrategy, Step, StepStatus};
use crate::storage;

use super::resolve_step;

// ---------------------------------------------------------------------------
// Step commands
// ---------------------------------------------------------------------------

/// Normalize user-supplied tags from a single CLI invocation.
///
/// Trims whitespace from each value, rejects empty/whitespace-only entries,
/// and rejects exact duplicates within the same invocation. Case is preserved
/// as the user typed it. Returns the normalized list ready to store.
pub(crate) fn normalize_tag_inputs(raw: &[String]) -> Result<Vec<String>> {
    let mut out: Vec<String> = Vec::with_capacity(raw.len());
    for t in raw {
        let trimmed = t.trim();
        if trimmed.is_empty() {
            bail!("Tag values cannot be empty or whitespace-only");
        }
        if out.iter().any(|existing| existing == trimmed) {
            bail!("Duplicate tag '{trimmed}' in this invocation");
        }
        out.push(trimmed.to_string());
    }
    Ok(out)
}

/// Render a step's tags for plain-text output (e.g. `[FIX][REGRESSION]`).
///
/// Returns an empty string when the step has no tags so list rendering stays
/// unchanged for pre-V13 data and steps that never opted in.
pub(crate) fn render_tags_inline(step: &Step) -> String {
    if step.tags.is_empty() {
        return String::new();
    }
    let mut s = String::new();
    for t in &step.tags {
        s.push('[');
        s.push_str(t);
        s.push(']');
    }
    s
}

/// Render the effective retry strategy for a step *with provenance*, for
/// human-readable step detail output (`ralph status --verbose`).
///
/// Resolution mirrors [`crate::plan::Step::effective_retry_strategy`]
/// (step > plan > default `keep`) but the returned string also reports
/// *where* the effective value came from so the operator can tell an
/// inherited value apart from an explicit per-step one:
///
/// - step sets it          → `"<value> (step-level)"`
/// - only the plan sets it → `"<value> (inherited from plan)"`
/// - neither sets it       → `"<unset — default keep>"`
pub(crate) fn retry_strategy_provenance(step: &Step, plan: &crate::plan::Plan) -> String {
    if let Some(rs) = step.retry_strategy {
        format!("{rs} (step-level)")
    } else if let Some(rs) = plan.retry_strategy {
        format!("{rs} (inherited from plan)")
    } else {
        "<unset — default keep>".to_string()
    }
}

pub fn step_list(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    config: &Config,
    filter_tags: &[String],
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let all_steps = storage::list_steps(conn, &plan.id)?;

    // AND-filter: step must carry every requested tag (case-sensitive exact
    // match). No tags requested → no filtering, preserving the legacy shape.
    let steps: Vec<Step> = if filter_tags.is_empty() {
        all_steps
    } else {
        all_steps
            .into_iter()
            .filter(|s| filter_tags.iter().all(|t| s.tags.iter().any(|st| st == t)))
            .collect()
    };

    if out.format == OutputFormat::Json {
        let summaries: Vec<output::StepSummary> =
            steps.iter().map(output::StepSummary::from).collect();
        println!("{}", serde_json::to_string(&summaries)?);
        return Ok(());
    }

    if steps.is_empty() {
        if filter_tags.is_empty() {
            eprintln!("No steps in plan '{}'.", plan_slug);
        } else {
            eprintln!(
                "No steps in plan '{}' matching tags {:?}.",
                plan_slug, filter_tags
            );
        }
        return Ok(());
    }

    eprintln!(
        "Steps for {} ({} total):",
        output::bold(plan_slug, out.color),
        steps.len()
    );
    for (i, step) in steps.iter().enumerate() {
        let policy_tag = if step.change_policy == ChangePolicy::Optional {
            " [optional]"
        } else {
            ""
        };
        let tags_inline = render_tags_inline(step);
        let tags_prefix = if tags_inline.is_empty() {
            String::new()
        } else {
            format!("{tags_inline} ")
        };
        let budget_tag = render_budget_tag(step, config);
        println!(
            "  {:>3}. {} {}{}{}  [{}]{}",
            i + 1,
            output::status_icon(step.status, out.color),
            tags_prefix,
            output::bold(&step.title, out.color),
            policy_tag,
            output::colored_status(step.status, out.color),
            budget_tag,
        );
        if !step.description.is_empty() {
            println!("       {}", step.description);
        }
    }

    Ok(())
}

/// Render the `(attempts: N/M)` tag shown at end of a step-list line.
///
/// Returns an empty string for the "noisy for the common case" rule:
/// a step that is still Pending with zero attempts and no custom
/// `max_retries` doesn't need the budget cluttering every row. As soon as the
/// step has been attempted (or failed/aborted/etc.) or the user explicitly
/// bound `max_retries`, the tag renders.
pub(crate) fn render_budget_tag(step: &Step, config: &Config) -> String {
    let show =
        step.attempts > 0 || step.status != StepStatus::Pending || step.max_retries.is_some();
    if !show {
        return String::new();
    }
    // Match executor.rs: max_attempts = max_retries.unwrap_or(default) + 1.
    let max_retries = step
        .max_retries
        .unwrap_or(config.max_retries_per_step as i32);
    let max_attempts = max_retries + 1;
    format!(" (attempts: {}/{})", step.attempts, max_attempts)
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
    model: Option<&str>,
    criteria: &[String],
    max_retries: Option<i32>,
    change_policy: Option<ChangePolicy>,
    retry_strategy: Option<RetryStrategy>,
    tags: &[String],
    depends_on: &[String],
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    // Resolve each dependency selector to an existing step BEFORE creating
    // the new step so a bad selector fails fast without leaving a
    // half-created step behind — mirroring how `plan_create` resolves
    // `--depends-on` slugs into `resolved_deps` before `create_plan`. Deps
    // are plan-internal pre-existing steps, so resolving them ahead of
    // creation is correct (the new step has no number/short id yet anyway).
    let mut resolved_deps: Vec<(String, String)> = Vec::with_capacity(depends_on.len());
    for dep_sel in depends_on {
        let (dep, _) = resolve_step(conn, &plan.id, Some(dep_sel.as_str()), None)?;
        resolved_deps.push((dep_sel.clone(), dep.id));
    }

    let desc = description.unwrap_or("");
    let normalized_tags = normalize_tag_inputs(tags)?;
    let tags_arg: Option<&[String]> = if normalized_tags.is_empty() {
        None
    } else {
        Some(&normalized_tags)
    };

    let (step, pos) = if let Some(after_pos) = after {
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
                    frac_index::key_between("0", first_key)?
                } else {
                    "00".to_string()
                }
            }
        } else if after_pos == steps.len() {
            // Append at end
            frac_index::key_after(&steps[steps.len() - 1].sort_key)?
        } else {
            // Insert between after_pos-1 and after_pos
            let before = &steps[after_pos - 1].sort_key;
            let after_key = &steps[after_pos].sort_key;
            frac_index::key_between(before, after_key)?
        };

        storage::create_step_at(
            conn,
            &plan.id,
            &sort_key,
            title,
            desc,
            agent,
            harness,
            criteria,
            max_retries,
            model,
            change_policy,
            tags_arg,
        )?
    } else {
        // Append at the end (default)
        storage::create_step(
            conn,
            &plan.id,
            title,
            desc,
            agent,
            harness,
            criteria,
            max_retries,
            model,
            change_policy,
            tags_arg,
        )?
    };

    // Persist a step-level retry-strategy override when the user supplied
    // one. `None` is the column default (inherit plan/global) so we skip
    // the write entirely in that case — mirroring how the plan-level
    // override is set after `create_plan`.
    if let Some(rs) = retry_strategy {
        storage::set_step_retry_strategy(conn, &step.id, Some(rs))?;
    }

    // Attach each resolved dependency. The new step has no edges yet, so a
    // cycle is impossible here; `add_step_dependency` still guards
    // self-edges and cycles defensively (docs/dag-redesign.md §6/§7).
    for (dep_sel, dep_id) in &resolved_deps {
        storage::add_step_dependency(conn, &step.id, dep_id)
            .with_context(|| format!("Failed to add dependency on '{dep_sel}'"))?;
    }

    eprintln!(
        "{} Added step #{} [{}]: {}",
        output::check_icon(out.color),
        pos,
        step.short_id,
        output::bold(&step.title, out.color),
    );
    if !resolved_deps.is_empty() {
        let sels: Vec<&str> = resolved_deps.iter().map(|(s, _)| s.as_str()).collect();
        eprintln!("  Depends on: {}", sels.join(", "));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Bulk step add via JSON (stdin or file)
// ---------------------------------------------------------------------------

/// Parse the bulk-import payload. Accepts either a JSON array of step objects
/// or a single object; the latter is wrapped into a 1-element Vec. Each object
/// must at minimum provide `title`; all other fields default via serde.
///
/// Kept as a free function so the unit tests can exercise it without touching
/// stdin or the filesystem.
pub(crate) fn parse_bulk_steps(raw: &str) -> Result<Vec<ImportedStep>> {
    // Try array first, fall back to single object.
    if let Ok(arr) = serde_json::from_str::<Vec<ImportedStep>>(raw) {
        return Ok(arr);
    }
    let single: ImportedStep = serde_json::from_str(raw)
        .context("Invalid --import-json payload: expected a JSON array of step objects or a single step object (each must have a `title` field)")?;
    Ok(vec![single])
}

/// Bulk-add steps from a JSON source. `source` is either `-` (stdin) or a
/// filesystem path. All inserts happen inside a single DB transaction so
/// the batch is atomic: any failure rolls the whole batch back.
pub fn step_add_bulk(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    source: &str,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    // Read the JSON payload from stdin or a file.
    let raw = if source == "-" {
        let mut buf = String::new();
        std::io::stdin()
            .read_to_string(&mut buf)
            .context("Failed to read --import-json payload from stdin")?;
        buf
    } else {
        std::fs::read_to_string(source)
            .with_context(|| format!("Failed to read --import-json file: {source}"))?
    };

    let steps = parse_bulk_steps(&raw)?;

    if steps.is_empty() {
        bail!("--import-json payload contained no steps");
    }

    // Validate each step up front so we fail before touching the database.
    for (i, s) in steps.iter().enumerate() {
        if s.title.trim().is_empty() {
            bail!("Step #{} is missing a non-empty `title`", i + 1);
        }
    }

    // Insert atomically inside a transaction. On any error, roll back.
    conn.execute_batch("BEGIN;")
        .context("Failed to begin bulk-import transaction")?;

    let mut inserted: Vec<(crate::plan::Step, usize)> = Vec::with_capacity(steps.len());
    let insert_result: Result<()> = (|| {
        for s in &steps {
            let tags_arg: Option<&[String]> = if s.tags.is_empty() {
                None
            } else {
                Some(&s.tags)
            };
            let (step, pos) = storage::create_step(
                conn,
                &plan.id,
                &s.title,
                &s.description,
                s.agent.as_deref(),
                s.harness.as_deref(),
                &s.acceptance_criteria,
                s.max_retries,
                s.model.as_deref(),
                Some(s.change_policy),
                tags_arg,
            )?;
            inserted.push((step, pos));
        }
        Ok(())
    })();

    if let Err(e) = insert_result {
        let _ = conn.execute_batch("ROLLBACK;");
        return Err(e).context("Bulk step insert failed; rolled back (no steps added)");
    }

    conn.execute_batch("COMMIT;")
        .context("Failed to commit bulk-import transaction")?;

    // Emit results.
    if out.format == OutputFormat::Json {
        let summaries: Vec<output::StepSummary> = inserted
            .iter()
            .map(|(s, _)| output::StepSummary::from(s))
            .collect();
        println!("{}", serde_json::to_string(&summaries)?);
    } else {
        for (step, pos) in &inserted {
            eprintln!(
                "{} Added step #{} [{}]: {}",
                output::check_icon(out.color),
                pos,
                step.short_id,
                output::bold(&step.title, out.color),
            );
        }
    }

    Ok(())
}

pub fn step_remove(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_sel: Option<&str>,
    step_id: Option<&str>,
    force: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_sel, step_id)?;

    if !force {
        let prompt = format!("Remove step #{} '{}'?", display_num, step.title);
        if !output::confirm(&prompt)? {
            eprintln!("Aborted.");
            return Ok(());
        }
    }

    storage::delete_step(conn, &step.id)?;
    eprintln!(
        "{} Removed step #{}: {}",
        output::check_icon(out.color),
        display_num,
        step.title
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn step_edit(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_sel: Option<&str>,
    step_id: Option<&str>,
    title: Option<&str>,
    description: Option<&str>,
    agent: Option<&str>,
    harness: Option<&str>,
    model: Option<&str>,
    criteria: &[String],
    clear_criteria: bool,
    max_retries: Option<i32>,
    clear_max_retries: bool,
    change_policy: Option<ChangePolicy>,
    retry_strategy: Option<RetryStrategy>,
    clear_retry_strategy: bool,
    tags: &[String],
    clear_tags: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_sel, step_id)?;

    if title.is_none()
        && description.is_none()
        && agent.is_none()
        && harness.is_none()
        && model.is_none()
        && criteria.is_empty()
        && !clear_criteria
        && max_retries.is_none()
        && !clear_max_retries
        && change_policy.is_none()
        && retry_strategy.is_none()
        && !clear_retry_strategy
        && tags.is_empty()
        && !clear_tags
    {
        bail!(
            "Nothing to edit: provide at least one of --title, --description, --agent, --harness, --model, --criteria, --clear-criteria, --max-retries, --clear-max-retries, --change-policy, --retry-strategy, --clear-retry-strategy, --tag, or --clear-tags"
        );
    }

    // We only pass non-None fields to the update function for fields the
    // user explicitly changed. The "None means don't change" rule applies
    // for agent/harness/model when the user passed the flag; empty string means
    // "clear".
    let agent_update = agent.map(|a| if a.is_empty() { None } else { Some(a) });

    let harness_update = harness.map(|h| if h.is_empty() { None } else { Some(h) });

    let model_update = model.map(|m| if m.is_empty() { None } else { Some(m) });

    // For max_retries: Some(N) means set to N, clear_max_retries means
    // set to NULL (use plan default), None means don't change.
    let retries_update: Option<Option<i32>> = if clear_max_retries {
        Some(None) // Set to NULL
    } else {
        max_retries.map(Some) // Set to specific value
    };

    // Tags: `--clear-tags` substitutes an empty list, any `--tag` invocation
    // replaces the existing list wholesale after normalization, otherwise
    // don't change the stored tags.
    let normalized_tags = if tags.is_empty() {
        Vec::new()
    } else {
        normalize_tag_inputs(tags)?
    };
    let tags_update: Option<&[String]> = if clear_tags {
        Some(&[])
    } else if !tags.is_empty() {
        Some(&normalized_tags)
    } else {
        None
    };

    // Criteria: `--clear-criteria` substitutes an empty list, any `--criteria`
    // invocation replaces the existing list, otherwise leave criteria
    // untouched. Mirrors the tags handling above.
    let criteria_update: Option<&[String]> = if clear_criteria {
        Some(&[])
    } else if !criteria.is_empty() {
        Some(criteria)
    } else {
        None
    };

    storage::update_step_fields_ext(
        conn,
        &step.id,
        title,
        description,
        agent_update,
        harness_update,
        criteria_update,
        retries_update,
        model_update,
        change_policy,
        tags_update,
    )?;

    // Retry strategy lives on its own dedicated setter (kept off
    // `update_step_fields_ext` to avoid churning that call surface).
    // `--clear-retry-strategy` writes NULL (inherit plan/global);
    // `--retry-strategy V` writes V; absence of both leaves the stored
    // value untouched. clap already rejects passing both at once.
    if clear_retry_strategy {
        storage::set_step_retry_strategy(conn, &step.id, None)?;
    } else if let Some(rs) = retry_strategy {
        storage::set_step_retry_strategy(conn, &step.id, Some(rs))?;
    }

    eprintln!(
        "{} Updated step #{}: {}",
        output::check_icon(out.color),
        display_num,
        title.unwrap_or(&step.title)
    );
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn step_reset(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_sel: Option<&str>,
    step_id: Option<&str>,
    force: bool,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_sel, step_id)?;

    // Before flipping the step back to pending, undo any `[ralph wip]`
    // skip commit(s) we parked on the plan branch for this step. We revert
    // (never `reset --hard`) so branch history is preserved even when later
    // steps committed on top of the WIP.
    let workdir = std::path::Path::new(project);
    // Only scan when the plan branch actually exists in this repo. A clean
    // `Ok(false)` (the project dir isn't a git repo, or the branch was never
    // created) means there can't be a skip-WIP commit to revert — reset
    // proceeds as a plain status flip. An *error* is different: silently
    // treating it as "absent" would orphan any parked `[ralph wip]` commits
    // with no hint, so we warn before degrading to the plain flip.
    let branch_present = match crate::git::branch_exists(workdir, &plan.branch_name) {
        Ok(present) => present,
        Err(e) => {
            eprintln!(
                "{} could not check whether branch '{}' exists ({e}); skipping \
                 skip-WIP revert — any parked `[ralph wip]` commits for this \
                 step will remain on the branch",
                output::severity_icon("warning", out.color),
                plan.branch_name
            );
            false
        }
    };
    let wip_shas = if branch_present {
        crate::git::skip_wip_commits_for_step(workdir, &plan.branch_name, &step.id).with_context(
            || {
                format!(
                    "could not scan branch '{}' for skip-WIP commits",
                    plan.branch_name
                )
            },
        )?
    } else {
        Vec::new()
    };

    if !wip_shas.is_empty() {
        // `git revert` operates on the *currently checked-out* HEAD. Unlike
        // a run (which checks out `plan.branch_name`), `step reset` is a
        // standalone command with no branch guarantee — so if the user is on
        // a different branch the revert commits would land on the wrong
        // branch and the WIP SHAs may not even be in its history (confusing
        // conflict / misplaced revert). Refuse rather than misplace commits.
        let current = crate::git::get_current_branch(workdir).with_context(|| {
            format!(
                "could not determine the current branch before reverting \
                 skip-WIP commits for step #{display_num}"
            )
        })?;
        if current != plan.branch_name {
            bail!(
                "Step #{display_num} has {} parked skip-WIP commit(s) on \
                 branch '{}', but the working tree is on '{}'. Reverting here \
                 would misplace the revert commits. Check out '{}' first \
                 (e.g. `git checkout {}`), then re-run `ralph step reset`.",
                wip_shas.len(),
                plan.branch_name,
                current,
                plan.branch_name,
                plan.branch_name
            );
        }
        if !force {
            let plural = if wip_shas.len() == 1 { "" } else { "s" };
            let shorts: Vec<String> = wip_shas
                .iter()
                .map(|s| s[..s.len().min(8)].to_string())
                .collect();
            let prompt = format!(
                "Resetting step #{display_num} will revert {} skip-WIP commit{plural} ({}) on branch '{}'. This adds revert commit(s). Continue?",
                wip_shas.len(),
                shorts.join(", "),
                plan.branch_name
            );
            if !output::confirm(&prompt)? {
                eprintln!("Aborted; step not reset.");
                return Ok(());
            }
        }

        // `wip_shas` is newest-first; revert in that order so each revert
        // applies cleanly on top of the previous one. We attempt *every*
        // commit and collect failures rather than `?`-bailing on the first:
        // a `revert_commit` error leaves the tree clean (the in-progress
        // revert is aborted), so continuing is safe, and the user gets one
        // summary of exactly what was and wasn't reverted instead of a
        // silently half-applied operation.
        let mut failed: Vec<String> = Vec::new();
        for sha in &wip_shas {
            let short = &sha[..sha.len().min(8)];
            match crate::git::revert_commit(workdir, sha) {
                Ok(crate::git::RevertOutcome::Reverted { revert_sha }) => {
                    eprintln!(
                        "{} Reverted skip-WIP commit {short} (revert {})",
                        output::check_icon(out.color),
                        &revert_sha[..revert_sha.len().min(8)]
                    );
                }
                Ok(crate::git::RevertOutcome::AlreadyReverted) => {
                    eprintln!("  skip-WIP commit {short} was already reverted — skipping");
                }
                Err(e) => {
                    eprintln!(
                        "{} Could not revert skip-WIP commit {short}: {e}",
                        output::severity_icon("warning", out.color)
                    );
                    failed.push(short.to_string());
                }
            }
        }
        if !failed.is_empty() {
            // Don't flip the step to pending while WIP commits are still
            // live on the branch. The successful reverts above stay applied,
            // so a later `ralph step reset` retry skips them as
            // `AlreadyReverted` and only retries the stragglers.
            bail!(
                "Reverted what it could, but {} skip-WIP commit(s) ({}) could \
                 not be reverted (likely a genuine conflict with later work). \
                 Step #{display_num} was left unchanged — resolve the \
                 conflict(s) or revert those commits manually, then re-run \
                 `ralph step reset`.",
                failed.len(),
                failed.join(", ")
            );
        }
    }

    storage::reset_step(conn, &step.id)?;
    eprintln!(
        "{} Reset step #{} '{}' to pending (0 attempts)",
        output::check_icon(out.color),
        display_num,
        step.title
    );
    Ok(())
}

pub fn step_move(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_sel: Option<&str>,
    step_id: Option<&str>,
    to: usize,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let steps = storage::list_steps(conn, &plan.id)?;

    // Resolve the step and its current 1-based position via the shared
    // selector resolver so `<num>` and `<short_id>` disambiguate the same
    // way they do for every other step command.
    let (step, display_num) = resolve_step(conn, &plan.id, step_sel, step_id)?;

    if to == 0 || to > steps.len() {
        bail!(
            "Target position {} is out of range (plan has {} steps)",
            to,
            steps.len()
        );
    }
    if display_num == to {
        eprintln!("Step is already at position {}.", to);
        return Ok(());
    }

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
                frac_index::key_between("0", first)?
            } else {
                // Extremely unlikely: first key is "0". Prepend with shorter key.
                "00".to_string()
            }
        }
    } else if target_idx >= other_keys.len() {
        // Move to last position
        frac_index::key_after(other_keys[other_keys.len() - 1])?
    } else {
        // Move between two existing steps
        let before = other_keys[target_idx - 1];
        let after_key = other_keys[target_idx];
        frac_index::key_between(before, after_key)?
    };

    storage::update_step_sort_key(conn, &step.id, &new_sort_key)?;
    eprintln!(
        "{} Moved step '{}' to position {}",
        output::check_icon(out.color),
        step.title,
        to
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// Step hook attachment commands
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
pub fn cmd_step_set_hook(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_sel: Option<&str>,
    step_id: Option<&str>,
    lifecycle: Lifecycle,
    hook_name: &str,
    _out: &OutputContext,
) -> Result<()> {
    // Warn if the hook isn't in the library (user can still attach — it will
    // be warn-and-skipped at run time until they import it).
    if hook_library::try_load(hook_name)?.is_none() {
        eprintln!(
            "Warning: hook '{hook_name}' is not in the local library. It will be skipped at run time until imported."
        );
    }

    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_sel, step_id)?;

    storage::attach_hook_to_step(conn, &plan.id, &step.id, lifecycle.as_str(), hook_name)?;
    println!("Attached hook '{hook_name}' to step {display_num} of '{plan_slug}' at {lifecycle}");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub fn cmd_step_unset_hook(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_sel: Option<&str>,
    step_id: Option<&str>,
    lifecycle: Lifecycle,
    hook_name: &str,
    _out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, display_num) = resolve_step(conn, &plan.id, step_sel, step_id)?;

    let removed = storage::detach_hook(
        conn,
        &plan.id,
        Some(&step.id),
        lifecycle.as_str(),
        hook_name,
    )?;
    if removed == 0 {
        bail!("No hook '{hook_name}' attached to step {display_num} at {lifecycle}");
    }
    println!("Detached hook '{hook_name}' from step {display_num} of '{plan_slug}'");
    Ok(())
}

// ---------------------------------------------------------------------------
// Step dependency commands (docs/dag-redesign.md §7)
//
// Structural step-scoped clones of `plan_dependency_add/remove/list`. Every
// selector — the subject step and each `--depends-on` value — is resolved via
// the shared [`resolve_step`] disambiguator, so `<num>` and `<short_id>`
// behave exactly as they do for every other step command. Dependencies are
// plan-internal: both endpoints are resolved against the same plan.
// ---------------------------------------------------------------------------

/// Add one or more step-dependency edges to the step named by `step_sel`.
///
/// Cycle/self-edge rejection lives in [`storage::add_step_dependency`]
/// (docs/dag-redesign.md §6), so this layer only resolves selectors and
/// reports.
pub fn step_dependency_add(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_sel: &str,
    depends_on_sels: &[String],
    out: &OutputContext,
) -> Result<()> {
    if depends_on_sels.is_empty() {
        bail!("At least one --depends-on step is required");
    }

    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, _) = resolve_step(conn, &plan.id, Some(step_sel), None)?;

    for dep_sel in depends_on_sels {
        let (dep, _) = resolve_step(conn, &plan.id, Some(dep_sel.as_str()), None)?;
        storage::add_step_dependency(conn, &step.id, &dep.id)?;
        eprintln!(
            "{} Added dependency: {} -> {}",
            output::check_icon(out.color),
            step.short_id,
            dep.short_id
        );
    }

    Ok(())
}

/// Remove one or more step-dependency edges from the step named by `step_sel`.
pub fn step_dependency_remove(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_sel: &str,
    depends_on_sels: &[String],
    out: &OutputContext,
) -> Result<()> {
    if depends_on_sels.is_empty() {
        bail!("At least one --depends-on step is required");
    }

    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, _) = resolve_step(conn, &plan.id, Some(step_sel), None)?;

    for dep_sel in depends_on_sels {
        let (dep, _) = resolve_step(conn, &plan.id, Some(dep_sel.as_str()), None)?;
        storage::remove_step_dependency(conn, &step.id, &dep.id)?;
        eprintln!(
            "{} Removed dependency: {} -> {}",
            output::check_icon(out.color),
            step.short_id,
            dep.short_id
        );
    }

    Ok(())
}

/// Print the direct dependencies and dependents of the step named by
/// `step_sel`. Mirrors `plan_dependency_list`: a `StepDependencyListSummary`
/// under `--json`, an indented two-section listing otherwise. Each related
/// step is rendered `<short_id>  <title>`, ordered by short id.
pub fn step_dependency_list(
    conn: &Connection,
    plan_slug: &str,
    project: &str,
    step_sel: &str,
    out: &OutputContext,
) -> Result<()> {
    let plan = storage::get_plan_by_slug(conn, plan_slug, project)?
        .with_context(|| format!("Plan not found: {plan_slug}"))?;

    let (step, _) = resolve_step(conn, &plan.id, Some(step_sel), None)?;

    let dep_ids = storage::list_step_dependencies(conn, &step.id)?;
    let dependent_ids = storage::list_step_dependents(conn, &step.id)?;

    // Build display labels (`<short_id>  <title>`), sorted by short id so the
    // output is deterministic regardless of storage row order.
    let label = |id: &str| -> Result<Option<(String, String)>> {
        Ok(storage::get_step_by_id(conn, id)?
            .map(|s| (s.short_id.clone(), format!("{}  {}", s.short_id, s.title))))
    };
    let collect = |ids: &[String]| -> Result<Vec<(String, String)>> {
        let mut v: Vec<(String, String)> = Vec::with_capacity(ids.len());
        for id in ids {
            if let Some(pair) = label(id)? {
                v.push(pair);
            }
        }
        v.sort();
        Ok(v)
    };

    let deps = collect(&dep_ids)?;
    let dependents = collect(&dependent_ids)?;

    if out.format == OutputFormat::Json {
        let summary = output::StepDependencyListSummary {
            short_id: step.short_id.clone(),
            depends_on: deps.iter().map(|(sid, _)| sid.clone()).collect(),
            depended_on_by: dependents.iter().map(|(sid, _)| sid.clone()).collect(),
        };
        println!("{}", serde_json::to_string(&summary)?);
        return Ok(());
    }

    println!(
        "{}  {}",
        output::bold(&step.short_id, out.color),
        step.title
    );
    println!("  depends on:");
    if deps.is_empty() {
        println!("    (none)");
    } else {
        for (_, line) in &deps {
            println!("    - {line}");
        }
    }
    println!("  depended on by:");
    if dependents.is_empty() {
        println!("    (none)");
    } else {
        for (_, line) in &dependents {
            println!("    - {line}");
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commands::plan_create;
    use crate::db;
    use crate::output::OutputFormat;

    fn test_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    fn setup_with_plan() -> (Connection, String) {
        let conn = db::open_memory().expect("open_memory");
        let project = "/tmp/bulk-test".to_string();
        plan_create(
            &conn,
            "bulk-plan",
            &project,
            None,
            None,
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        (conn, project)
    }

    #[test]
    fn test_parse_bulk_steps_array() {
        let json = r#"[
            {"title": "a", "description": "first"},
            {"title": "b", "acceptance_criteria": ["passes"], "max_retries": 5}
        ]"#;
        let parsed = parse_bulk_steps(json).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].title, "a");
        assert_eq!(parsed[1].max_retries, Some(5));
    }

    #[test]
    fn test_parse_bulk_steps_single_object() {
        let json = r#"{"title": "lonely"}"#;
        let parsed = parse_bulk_steps(json).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].title, "lonely");
    }

    #[test]
    fn test_parse_bulk_steps_invalid_rejected() {
        // Missing `title`.
        let json = r#"[{"description": "no title"}]"#;
        assert!(parse_bulk_steps(json).is_err());
    }

    #[test]
    fn test_step_add_bulk_from_file_inserts_array() {
        let (conn, project) = setup_with_plan();

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("steps.json");
        let json = r#"[
            {
                "title": "Write tests",
                "description": "Cover the happy path",
                "acceptance_criteria": ["tests pass"],
                "max_retries": 2
            },
            {
                "title": "Implement feature",
                "agent": "claude-code",
                "harness": "claude-code"
            }
        ]"#;
        std::fs::write(&file, json).unwrap();

        step_add_bulk(
            &conn,
            "bulk-plan",
            &project,
            file.to_str().unwrap(),
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 2, "both steps should have been inserted");
        assert_eq!(steps[0].title, "Write tests");
        assert_eq!(steps[0].description, "Cover the happy path");
        assert_eq!(steps[0].acceptance_criteria, vec!["tests pass".to_string()]);
        assert_eq!(steps[0].max_retries, Some(2));
        assert_eq!(steps[1].title, "Implement feature");
        assert_eq!(steps[1].agent.as_deref(), Some("claude-code"));
        assert_eq!(steps[1].harness.as_deref(), Some("claude-code"));
    }

    #[test]
    fn test_step_add_bulk_from_file_single_object() {
        let (conn, project) = setup_with_plan();

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("one.json");
        std::fs::write(&file, r#"{"title": "just one"}"#).unwrap();

        step_add_bulk(
            &conn,
            "bulk-plan",
            &project,
            file.to_str().unwrap(),
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].title, "just one");
    }

    #[test]
    fn test_step_add_bulk_empty_title_fails_atomically() {
        let (conn, project) = setup_with_plan();

        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("bad.json");
        // Second entry has an empty title — the whole batch must fail and
        // leave no steps in the DB.
        let json = r#"[
            {"title": "ok"},
            {"title": ""}
        ]"#;
        std::fs::write(&file, json).unwrap();

        let result = step_add_bulk(
            &conn,
            "bulk-plan",
            &project,
            file.to_str().unwrap(),
            &test_out(),
        );
        assert!(result.is_err());

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert!(steps.is_empty(), "no steps should have been inserted");
    }

    // -- step-list attempt budget ------------------------------------------

    fn default_config() -> Config {
        Config {
            max_retries_per_step: 3, // explicit default budget for the test
            ..Config::default()
        }
    }

    #[test]
    fn test_step_list_shows_attempts_budget_when_relevant() {
        let (conn, project) = setup_with_plan();
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "With custom retries",
            None,
            None,
            None,
            None,
            None,
            &[],
            Some(3),
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        // Simulate attempts=2 on that step.
        conn.execute(
            "UPDATE steps SET attempts = 2 WHERE id = ?1",
            rusqlite::params![steps[0].id],
        )
        .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();

        // Inspect the budget tag directly — no need to capture stdout for a
        // format contract that's fully rendered by render_budget_tag.
        let tag = render_budget_tag(&steps[0], &default_config());
        assert_eq!(tag, " (attempts: 2/4)", "tag was: {tag:?}");
    }

    #[test]
    fn test_step_list_omits_budget_for_pending_default_steps() {
        let (conn, project) = setup_with_plan();
        // No max_retries override, no attempts yet, Pending.
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "Plain pending",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        let tag = render_budget_tag(&steps[0], &default_config());
        assert_eq!(
            tag, "",
            "pending default-retry step should not render the budget tag; got {tag:?}"
        );
    }

    #[test]
    fn test_step_list_shows_budget_after_attempts_even_without_override() {
        let (conn, project) = setup_with_plan();
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "No override",
            None,
            None,
            None,
            None,
            None,
            &[],
            None, // no max_retries override — falls back to config default.
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        conn.execute(
            "UPDATE steps SET attempts = 1 WHERE id = ?1",
            rusqlite::params![steps[0].id],
        )
        .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();

        let tag = render_budget_tag(&steps[0], &default_config());
        // Default config has max_retries_per_step=3 → max_attempts=4.
        assert_eq!(tag, " (attempts: 1/4)");
    }

    // -- Tag tests ---------------------------------------------------------

    /// Helper: invoke `step_add` with a minimum set of args plus user-provided tags.
    fn add_with_tags(conn: &Connection, project: &str, title: &str, tags: &[String]) {
        step_add(
            conn,
            "bulk-plan",
            project,
            title,
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            tags,
            &[],
            &test_out(),
        )
        .unwrap();
    }

    #[test]
    fn test_step_add_with_tags() {
        let (conn, project) = setup_with_plan();
        let tags = vec!["FIX".to_string(), "REGRESSION".to_string()];
        add_with_tags(&conn, &project, "tagged", &tags);

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps.len(), 1);
        assert_eq!(steps[0].tags, tags);
    }

    #[test]
    fn test_step_add_rejects_empty_tag() {
        let (conn, project) = setup_with_plan();
        let tags = vec!["FIX".to_string(), "  ".to_string()];
        let err = step_add(
            &conn,
            "bulk-plan",
            &project,
            "t",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &tags,
            &[],
            &test_out(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("empty"));
    }

    #[test]
    fn test_step_add_rejects_duplicate_tag_in_same_invocation() {
        let (conn, project) = setup_with_plan();
        let tags = vec!["FIX".to_string(), "FIX".to_string()];
        let err = step_add(
            &conn,
            "bulk-plan",
            &project,
            "t",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &tags,
            &[],
            &test_out(),
        )
        .unwrap_err();
        assert!(err.to_string().to_lowercase().contains("duplicate"));
    }

    #[test]
    fn test_step_edit_replaces_tags() {
        let (conn, project) = setup_with_plan();
        add_with_tags(
            &conn,
            &project,
            "t",
            &["INITIAL".to_string(), "OTHER".to_string()],
        );

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();

        // Replace with a brand-new set.
        let new_tags = vec!["REVIEW".to_string()];
        step_edit(
            &conn,
            "bulk-plan",
            &project,
            Some("1"),
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
            false,
            None,
            false,
            None,
            None,
            false,
            &new_tags,
            false,
            &test_out(),
        )
        .unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].tags, new_tags);
    }

    #[test]
    fn test_step_edit_clear_tags() {
        let (conn, project) = setup_with_plan();
        add_with_tags(&conn, &project, "t", &["FIX".to_string()]);

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();

        step_edit(
            &conn,
            "bulk-plan",
            &project,
            Some("1"),
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
            false,
            None,
            false,
            None,
            None,
            false,
            &[],
            true, // clear_tags
            &test_out(),
        )
        .unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert!(steps[0].tags.is_empty());
    }

    #[test]
    fn test_step_edit_clear_criteria() {
        let (conn, project) = setup_with_plan();
        let initial_criteria = vec!["tests pass".to_string(), "lint clean".to_string()];
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "with criteria",
            None,
            None,
            None,
            None,
            None,
            &initial_criteria,
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();

        // Sanity: the step really started with criteria.
        let before = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(before[0].acceptance_criteria, initial_criteria);

        step_edit(
            &conn,
            "bulk-plan",
            &project,
            Some("1"),
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
            true, // clear_criteria
            None,
            false,
            None,
            None,
            false,
            &[],
            false,
            &test_out(),
        )
        .unwrap();

        let after = storage::list_steps(&conn, &plan.id).unwrap();
        assert!(after[0].acceptance_criteria.is_empty());
    }

    #[test]
    fn test_step_edit_no_tag_flag_leaves_tags_unchanged() {
        let (conn, project) = setup_with_plan();
        let original = vec!["KEEP".to_string()];
        add_with_tags(&conn, &project, "t", &original);

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();

        // Edit just the title — tags should be unchanged.
        step_edit(
            &conn,
            "bulk-plan",
            &project,
            Some("1"),
            None,
            Some("new title"),
            None,
            None,
            None,
            None,
            &[],
            false,
            None,
            false,
            None,
            None,
            false,
            &[],
            false,
            &test_out(),
        )
        .unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].title, "new title");
        assert_eq!(steps[0].tags, original);
    }

    #[test]
    fn test_step_list_filter_by_tag() {
        let (conn, project) = setup_with_plan();
        add_with_tags(&conn, &project, "A", &["FIX".to_string()]);
        add_with_tags(&conn, &project, "B", &["REVIEW".to_string()]);
        add_with_tags(
            &conn,
            &project,
            "C",
            &["FIX".to_string(), "URGENT".to_string()],
        );

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();

        // No filter -> all three.
        let all = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(all.len(), 3);

        // Single-tag filter picks the two steps that carry FIX.
        let filter = ["FIX".to_string()];
        let filtered: Vec<&Step> = all
            .iter()
            .filter(|s| filter.iter().all(|t| s.tags.iter().any(|st| st == t)))
            .collect();
        assert_eq!(filtered.len(), 2);
        let titles: Vec<&str> = filtered.iter().map(|s| s.title.as_str()).collect();
        assert!(titles.contains(&"A"));
        assert!(titles.contains(&"C"));
    }

    #[test]
    fn test_step_list_filter_requires_all_tags() {
        let (conn, project) = setup_with_plan();
        add_with_tags(&conn, &project, "A", &["FIX".to_string()]);
        add_with_tags(
            &conn,
            &project,
            "B",
            &["FIX".to_string(), "URGENT".to_string()],
        );

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();

        // Demand BOTH `FIX` and `URGENT` -> only B matches.
        let filter = ["FIX".to_string(), "URGENT".to_string()];
        let all = storage::list_steps(&conn, &plan.id).unwrap();
        let filtered: Vec<&Step> = all
            .iter()
            .filter(|s| filter.iter().all(|t| s.tags.iter().any(|st| st == t)))
            .collect();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].title, "B");
    }

    #[test]
    fn test_render_tags_inline() {
        let (conn, project) = setup_with_plan();
        add_with_tags(
            &conn,
            &project,
            "tagged",
            &["FIX".to_string(), "REGRESSION".to_string()],
        );
        add_with_tags(&conn, &project, "untagged", &[]);

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();

        assert_eq!(render_tags_inline(&steps[0]), "[FIX][REGRESSION]");
        assert_eq!(render_tags_inline(&steps[1]), "");
    }

    // ----- step_reset reverts skip-WIP commits (STEP 19) -----

    /// A git repo + plan + one step, with the plan's branch_name pointing at
    /// the repo's actual branch. Returns (conn, project, dir, step_id, branch).
    fn reset_fixture() -> (
        Connection,
        String,
        std::path::PathBuf,
        String,
        String,
        tempfile::TempDir,
    ) {
        use std::fs;
        let tmp = tempfile::TempDir::new().unwrap();
        let dir = tmp.path().to_path_buf();
        for args in [
            vec!["init"],
            vec!["config", "user.email", "t@t.com"],
            vec!["config", "user.name", "t"],
        ] {
            std::process::Command::new("git")
                .args(&args)
                .current_dir(&dir)
                .output()
                .unwrap();
        }
        fs::write(dir.join("README.md"), "# hi").unwrap();
        crate::git::commit_changes(&dir, "init").unwrap();
        let branch = crate::git::get_current_branch(&dir).unwrap();
        let project = dir.canonicalize().unwrap().to_string_lossy().into_owned();

        let conn = db::open_memory().unwrap();
        let plan =
            storage::create_plan(&conn, "p", &project, &branch, "d", None, None, &[]).unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Wire it",
            "",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .unwrap();
        (conn, project, dir, step.id, branch, tmp)
    }

    /// Park a WIP commit the way `park_changes(Commit)` does.
    fn park_wip(dir: &std::path::Path, subject: &str, step_id: &str) -> String {
        let msg = format!("{subject}\n\nRalph-Skipped-Step: {step_id}\n");
        crate::git::commit_changes(dir, &msg).unwrap();
        crate::git::get_commit_hash(dir).unwrap()
    }

    #[test]
    fn test_step_reset_force_reverts_wip() {
        let (conn, project, dir, step_id, _branch, _tmp) = reset_fixture();
        std::fs::write(dir.join("wip.txt"), "wip").unwrap();
        park_wip(&dir, "[ralph wip] skipped step 1: Wire it", &step_id);
        assert!(dir.join("wip.txt").exists());

        step_reset(&conn, "p", &project, Some("1"), None, true, &test_out()).unwrap();

        // Revert happened (no prompt because force) and the step is pending.
        assert!(!dir.join("wip.txt").exists(), "WIP reverted");
        let plan = storage::get_plan_by_slug(&conn, "p", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Pending);
    }

    #[test]
    fn test_step_reset_without_force_prompt_declined_no_revert() {
        // Under `cargo test` stdin is at EOF, so `confirm` returns false:
        // exercises the prompt path. The WIP must be left intact and the
        // step NOT reset.
        let (conn, project, dir, step_id, _branch, _tmp) = reset_fixture();
        std::fs::write(dir.join("wip.txt"), "wip").unwrap();
        park_wip(&dir, "[ralph wip] skipped step 1: Wire it", &step_id);

        step_reset(&conn, "p", &project, Some("1"), None, false, &test_out()).unwrap();

        assert!(
            dir.join("wip.txt").exists(),
            "declined prompt must not revert"
        );
        let plan = storage::get_plan_by_slug(&conn, "p", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(
            steps[0].status,
            StepStatus::Pending,
            "step starts pending; reset aborted leaves it unchanged"
        );
        // Mark it failed then re-confirm the abort really skipped reset.
        storage::update_step_status(&conn, &steps[0].id, StepStatus::Failed).unwrap();
        step_reset(&conn, "p", &project, Some("1"), None, false, &test_out()).unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(
            steps[0].status,
            StepStatus::Failed,
            "aborted reset must not flip status"
        );
    }

    #[test]
    fn test_step_reset_no_wip_still_resets() {
        // No skip-WIP commit at all: reset is a plain status flip even
        // without --force (no prompt should appear).
        let (conn, project, _dir, _step_id, _branch, _tmp) = reset_fixture();
        let plan = storage::get_plan_by_slug(&conn, "p", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        storage::update_step_status(&conn, &steps[0].id, StepStatus::Failed).unwrap();

        step_reset(&conn, "p", &project, Some("1"), None, false, &test_out()).unwrap();

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Pending);
    }

    #[test]
    fn test_step_reset_wip_not_on_head() {
        let (conn, project, dir, step_id, _branch, _tmp) = reset_fixture();
        std::fs::write(dir.join("wip.txt"), "wip").unwrap();
        park_wip(&dir, "[ralph wip] skipped step 1: Wire it", &step_id);
        // A later step lands on top of the WIP.
        std::fs::write(dir.join("later.txt"), "later").unwrap();
        crate::git::commit_changes(&dir, "step 2 done").unwrap();

        step_reset(&conn, "p", &project, Some("1"), None, true, &test_out()).unwrap();

        assert!(!dir.join("wip.txt").exists(), "WIP reverted");
        assert!(dir.join("later.txt").exists(), "later work preserved");
    }

    #[test]
    fn test_step_reset_already_reverted_clean() {
        let (conn, project, dir, step_id, _branch, _tmp) = reset_fixture();
        std::fs::write(dir.join("wip.txt"), "wip").unwrap();
        let wip = park_wip(&dir, "[ralph wip] skipped step 1: Wire it", &step_id);
        // Manually revert it already.
        match crate::git::revert_commit(&dir, &wip).unwrap() {
            crate::git::RevertOutcome::Reverted { .. } => {}
            o => panic!("setup revert failed: {o:?}"),
        }

        // step_reset should detect the already-reverted state and not error.
        step_reset(&conn, "p", &project, Some("1"), None, true, &test_out()).unwrap();

        assert!(!crate::git::has_uncommitted_changes(&dir).unwrap());
        let plan = storage::get_plan_by_slug(&conn, "p", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(steps[0].status, StepStatus::Pending);
    }

    #[test]
    fn test_step_reset_multiple_wip_commits_reverted() {
        let (conn, project, dir, step_id, _branch, _tmp) = reset_fixture();
        std::fs::write(dir.join("f.txt"), "v1\n").unwrap();
        park_wip(&dir, "[ralph wip] skipped step 1: a", &step_id);
        std::fs::write(dir.join("f.txt"), "v1\nv2\n").unwrap();
        park_wip(&dir, "[ralph wip] skipped step 1: a again", &step_id);

        step_reset(&conn, "p", &project, Some("1"), None, true, &test_out()).unwrap();

        // Both WIP layers undone (newest-first reverts applied cleanly).
        assert!(!dir.join("f.txt").exists());
        assert!(!crate::git::has_uncommitted_changes(&dir).unwrap());
    }

    // -----------------------------------------------------------------
    // Retry strategy: CLI handler round-trip + provenance (Step 23)
    // -----------------------------------------------------------------

    /// Add a step via `step_add` with an explicit retry strategy and read
    /// it back, asserting the override was persisted at the step level.
    #[test]
    fn test_step_add_persists_retry_strategy() {
        let (conn, project) = setup_with_plan();
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "rollback step",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            Some(crate::plan::RetryStrategy::Rollback),
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(
            steps[0].retry_strategy,
            Some(crate::plan::RetryStrategy::Rollback)
        );
    }

    /// `step_add` without `--retry-strategy` leaves the column NULL so the
    /// step inherits the plan/global value.
    #[test]
    fn test_step_add_without_retry_strategy_is_none() {
        let (conn, project) = setup_with_plan();
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "plain",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert!(steps[0].retry_strategy.is_none());
    }

    // -----------------------------------------------------------------
    // `ralph step add --depends-on` (docs/dag-redesign.md §7): resolve
    // each selector to an existing step and attach the edge after create.
    // -----------------------------------------------------------------

    /// Add a step with a single `--depends-on <num>` selector — the new
    /// step gains exactly that dependency edge.
    fn add_plain(conn: &Connection, project: &str, title: &str, depends_on: &[String]) {
        step_add(
            conn,
            "bulk-plan",
            project,
            title,
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            depends_on,
            &test_out(),
        )
        .unwrap();
    }

    #[test]
    fn test_step_add_with_depends_on_attaches_edges() {
        let (conn, project) = setup_with_plan();
        add_plain(&conn, &project, "a", &[]);
        add_plain(&conn, &project, "b", &[]);

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        let (a, b) = (&steps[0], &steps[1]);

        // c depends on step #1 (by number) and step b (by short id).
        add_plain(
            &conn,
            &project,
            "c",
            &["1".to_string(), b.short_id.clone()],
        );

        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        let c = steps.iter().find(|s| s.title == "c").unwrap();
        let mut deps = storage::list_step_dependencies(&conn, &c.id).unwrap();
        deps.sort();
        let mut expected = vec![a.id.clone(), b.id.clone()];
        expected.sort();
        assert_eq!(deps, expected);
    }

    #[test]
    fn test_step_add_depends_on_bad_selector_fails_fast() {
        // An unresolvable selector aborts before the step is created — no
        // half-created step is left behind (mirrors plan_create's
        // resolve-before-create ordering).
        let (conn, project) = setup_with_plan();
        let err = step_add(
            &conn,
            "bulk-plan",
            &project,
            "orphan",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &["99".to_string()],
            &test_out(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("out of range"));

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        assert!(storage::list_steps(&conn, &plan.id).unwrap().is_empty());
    }

    /// Provenance: a step that sets its own strategy reports `(step-level)`;
    /// a step that inherits from the plan reports `(inherited from plan)`;
    /// a step where neither is set reports the default-keep marker.
    #[test]
    fn test_retry_strategy_provenance_all_three_states() {
        let (conn, project) = setup_with_plan();

        // Plan-level default = rollback so the inherited case is observable.
        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        storage::set_plan_retry_strategy(
            &conn,
            &plan.id,
            Some(crate::plan::RetryStrategy::Rollback),
        )
        .unwrap();

        // step 1: explicit step-level keep (wins over plan rollback).
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "explicit",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            Some(crate::plan::RetryStrategy::Keep),
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        // step 2: no step-level override -> inherits plan rollback.
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "inherits",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();

        assert_eq!(
            retry_strategy_provenance(&steps[0], &plan),
            "keep (step-level)"
        );
        assert_eq!(
            retry_strategy_provenance(&steps[1], &plan),
            "rollback (inherited from plan)"
        );

        // Now clear the plan default: step 2 falls through to the global
        // default-keep marker.
        storage::set_plan_retry_strategy(&conn, &plan.id, None).unwrap();
        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(
            retry_strategy_provenance(&steps[1], &plan),
            "<unset — default keep>"
        );
        // Effective resolution must agree with the displayed provenance.
        assert_eq!(
            steps[1].effective_retry_strategy(&plan),
            crate::plan::RetryStrategy::Keep
        );
    }

    /// `--clear-retry-strategy` on `step edit` reverts a step-level override
    /// back to NULL so the step re-inherits the plan/global value.
    #[test]
    fn test_step_edit_clear_retry_strategy_reverts_to_inheritance() {
        let (conn, project) = setup_with_plan();

        // Plan default = rollback.
        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        storage::set_plan_retry_strategy(
            &conn,
            &plan.id,
            Some(crate::plan::RetryStrategy::Rollback),
        )
        .unwrap();

        // Step starts with an explicit keep override.
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "s",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            Some(crate::plan::RetryStrategy::Keep),
            &[],
            &[],
            &test_out(),
        )
        .unwrap();

        // Clear it via step_edit.
        step_edit(
            &conn,
            "bulk-plan",
            &project,
            Some("1"),
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
            false,
            None,
            false,
            None,
            None, // retry_strategy
            true, // clear_retry_strategy
            &[],
            false,
            &test_out(),
        )
        .unwrap();

        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert!(
            steps[0].retry_strategy.is_none(),
            "clear must NULL out the step-level override"
        );
        // Now reports inherited-from-plan, and effective resolves to rollback.
        assert_eq!(
            retry_strategy_provenance(&steps[0], &plan),
            "rollback (inherited from plan)"
        );
        assert_eq!(
            steps[0].effective_retry_strategy(&plan),
            crate::plan::RetryStrategy::Rollback
        );
    }

    /// `step edit --retry-strategy V` sets a fresh step-level override on a
    /// step that previously had none.
    #[test]
    fn test_step_edit_sets_retry_strategy() {
        let (conn, project) = setup_with_plan();
        step_add(
            &conn,
            "bulk-plan",
            &project,
            "s",
            None,
            None,
            None,
            None,
            None,
            &[],
            None,
            None,
            None,
            &[],
            &[],
            &test_out(),
        )
        .unwrap();
        step_edit(
            &conn,
            "bulk-plan",
            &project,
            Some("1"),
            None,
            None,
            None,
            None,
            None,
            None,
            &[],
            false,
            None,
            false,
            None,
            Some(crate::plan::RetryStrategy::Rollback),
            false,
            &[],
            false,
            &test_out(),
        )
        .unwrap();
        let plan = storage::get_plan_by_slug(&conn, "bulk-plan", &project)
            .unwrap()
            .unwrap();
        let steps = storage::list_steps(&conn, &plan.id).unwrap();
        assert_eq!(
            steps[0].retry_strategy,
            Some(crate::plan::RetryStrategy::Rollback)
        );
    }
}
