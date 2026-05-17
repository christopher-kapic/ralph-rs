// Built-in nondeterministic review pipeline (docs/dag-redesign.md §3.2-§3.3,
// §8, §9, §10, §14.5).
//
// This module owns the *reviewer side* of the pipeline and the
// *orchestrator's* DAG mutation in response to a verdict. It deliberately
// holds NO ability to mutate the DAG from a reviewer subprocess: a reviewer
// only ever produces a verdict and (on FAIL) *requests* a corrective step
// through the structured channel (NDJSON event + V29 DB bridge row). The
// orchestrator — the single DAG writer (§9-inv-3) — drains the request at a
// scheduler tick and performs the §10 insert + re-parent.
//
// Hard invariants enforced here:
//  - O(1) reviewer prompt: a SINGLE `git show <sha>` diff (Decision 5).
//  - Reviews are strictly read-only w.r.t. the working tree: the reviewer is
//    run against a *fixed* committed SHA; `assert_tree_unchanged_by_review`
//    guards that the reviewer subprocess did not mutate the tree/HEAD.
//  - Single DAG writer: nothing in the reviewer path writes step rows/edges;
//    only `consume_corrective_request` (orchestrator) does.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::config::Config;
use crate::output::{OutputContext, OutputFormat, RunEvent};
use crate::plan::{InterruptionKind, Plan, ReviewStatus, Step};
use crate::{git, harness, output, prompt, storage};

/// Built-in default for the per-plan review→correction→review recursion cap
/// (docs/dag-redesign.md §10 item 4 / §14.5). Used when a plan's
/// `max_review_corrections` is `None` (unset). A small bound: review is a
/// safety net, not an iterative optimizer — if three successive corrections
/// of the same step still fail review, a human needs to look.
pub const DEFAULT_MAX_REVIEW_CORRECTIONS: usize = 3;

/// Parsed reviewer verdict (the structured contract documented in
/// [`prompt::REVIEW_VERDICT_CONTRACT`] and embedded verbatim in the reviewer
/// prompt). The parser keys off the leading `REVIEW PASS` / `REVIEW FAIL`
/// token so a hyphen/spacing wobble in the free-text tail cannot flip a FAIL
/// into a silently-ignored line.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewVerdict {
    Pass,
    /// Reviewer rejected; `issues` is the advisory defect count (≥1; defaults
    /// to 1 when the harness omitted or mangled the number).
    Fail {
        issues: i32,
    },
}

/// Parse the reviewer harness's stdout into a [`ReviewVerdict`].
///
/// Contract (must match [`prompt::REVIEW_VERDICT_CONTRACT`] exactly): the
/// verdict is the LAST line that starts with `REVIEW PASS` or `REVIEW FAIL`.
/// We scan bottom-up so trailing reasoning that merely *quotes* the contract
/// earlier in the transcript can't be mistaken for the verdict, and so the
/// harness's final word wins. A transcript with no verdict line at all is a
/// contract violation: we treat it as a FAIL with 1 issue (fail-safe — an
/// unparseable review must not silently pass un-reviewed work).
pub fn parse_review_verdict(stdout: &str) -> ReviewVerdict {
    for line in stdout.lines().rev() {
        let t = line.trim();
        let upper = t.to_ascii_uppercase();
        if upper.starts_with("REVIEW PASS") {
            return ReviewVerdict::Pass;
        }
        if upper.starts_with("REVIEW FAIL") {
            // Best-effort defect count from the first integer after the
            // token; advisory only — absence/garble ⇒ 1.
            let issues = t
                .split(|c: char| !c.is_ascii_digit())
                .filter(|s| !s.is_empty())
                .find_map(|s| s.parse::<i32>().ok())
                .filter(|n| *n >= 1)
                .unwrap_or(1);
            return ReviewVerdict::Fail { issues };
        }
    }
    // No verdict line: fail-safe. An unreadable review NEVER passes work.
    ReviewVerdict::Fail { issues: 1 }
}

/// Resolve the per-plan recursion cap (§10 item 4): the plan's
/// `max_review_corrections` if set, else [`DEFAULT_MAX_REVIEW_CORRECTIONS`].
/// A non-positive override is clamped to 0 (any failed review immediately
/// escalates to a blocker rather than spawning a correction).
pub fn effective_max_review_corrections(plan: &Plan) -> usize {
    plan.max_review_corrections
        .map(|n| n.max(0) as usize)
        .unwrap_or(DEFAULT_MAX_REVIEW_CORRECTIONS)
}

/// Snapshot of git state taken just before a reviewer subprocess runs, used
/// by [`assert_tree_unchanged_by_review`] to PROVE the review was read-only
/// w.r.t. the working tree / HEAD (the §9-inv-2 hard invariant). A review
/// runs against a fixed commit SHA and must never check out, edit, or
/// commit.
#[derive(Debug, Clone)]
pub struct ReviewTreeGuard {
    head: Option<String>,
    diff: String,
}

impl ReviewTreeGuard {
    /// Capture HEAD + the full working-tree diff before the reviewer runs.
    pub fn capture(workdir: &Path) -> Self {
        Self {
            head: git::get_commit_hash(workdir).ok(),
            diff: git::get_diff(workdir).unwrap_or_default(),
        }
    }
}

/// Hard assertion that the reviewer subprocess did NOT mutate the working
/// tree or move HEAD (docs/dag-redesign.md §9 invariant 2). Reviews are
/// "strictly read-only w.r.t. the working tree" — this is the guard that
/// makes that machine-checkable rather than aspirational. A violation is a
/// blocker (returns `Err`), never silently tolerated, because a review that
/// edited the tree would corrupt the concurrently-running implementation.
pub fn assert_tree_unchanged_by_review(workdir: &Path, before: &ReviewTreeGuard) -> Result<()> {
    let after_head = git::get_commit_hash(workdir).ok();
    if after_head != before.head {
        anyhow::bail!(
            "read-only review invariant violated: HEAD moved during review \
             ({:?} -> {:?}). A reviewer must never commit/checkout (§9-inv-2).",
            before.head,
            after_head
        );
    }
    let after_diff = git::get_diff(workdir).unwrap_or_default();
    if after_diff != before.diff {
        anyhow::bail!(
            "read-only review invariant violated: working tree changed during \
             review. A reviewer must never modify files (§9-inv-2)."
        );
    }
    Ok(())
}

/// Outcome of running one review (STEP 37). The orchestrator turns a `Fail`
/// into a corrective-step *request* (STEP 39) which it later consumes
/// (STEP 40).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReviewOutcome {
    /// Review passed (or the harness produced a PASS verdict). The step is
    /// `Complete` with `review_status = Passed`.
    Passed,
    /// Review failed; a corrective-step request row was written to the V29
    /// bridge and a `CorrectiveStepRequested` NDJSON event emitted. The
    /// orchestrator performs the actual DAG mutation later.
    Failed { request_id: String, issues: i32 },
}

/// Spawn the configured review harness/model against a **committed SHA** and
/// return the verdict (docs/dag-redesign.md §3.2-§3.3, §9-inv-2). STEP 37.
///
/// - Builds the dedicated read-only reviewer prompt
///   ([`prompt::build_review_prompt`]) from the *single* `git show <sha>`
///   diff (O(1) — Decision 5).
/// - Reuses `harness.rs` spawn machinery, but with the **review** harness
///   (`config.review.harness` / `.model`), not the implementation harness.
/// - Captures a [`ReviewTreeGuard`] before spawning and asserts the tree is
///   unchanged after — the read-only hard invariant.
/// - Transitions `review_status` Pending → InFlight → Passed/Failed and
///   annotates the commit's `Ralph-Review` trailer (`passed`/`failed`) via
///   the history-safe note path.
///
/// On `Fail`, this writes the V29 bridge row + emits the
/// `CorrectiveStepRequested` event (STEP 39) and returns
/// [`ReviewOutcome::Failed`]; it NEVER mutates step rows/edges itself.
#[allow(clippy::too_many_arguments)]
pub async fn run_review(
    conn: &Connection,
    plan: &Plan,
    step: &Step,
    config: &Config,
    workdir: &Path,
    commit_sha: &str,
    iteration: i32,
    step_num: usize,
    out: &OutputContext,
) -> Result<ReviewOutcome> {
    // Resolve the REVIEW harness (distinct from the implementation harness).
    let review_harness_name = config.review.harness.trim();
    if review_harness_name.is_empty() {
        anyhow::bail!(
            "review is enabled for step '{}' but no review harness is configured \
             (set `review.harness` in config.json — `ralph doctor` warns about this)",
            step.title
        );
    }
    let harness_config = config
        .harnesses
        .get(review_harness_name)
        .with_context(|| {
            format!(
                "review harness '{review_harness_name}' is not defined in config.json (harnesses: {:?})",
                config.harnesses.keys().collect::<Vec<_>>()
            )
        })?;
    let model_override = if config.review.model.trim().is_empty() {
        None
    } else {
        Some(config.review.model.trim())
    };

    storage::update_step_review_status(conn, &step.id, ReviewStatus::InFlight)?;

    let short = git::short_sha(workdir, commit_sha);
    if out.format == OutputFormat::Json {
        output::emit_ndjson(&RunEvent::ReviewStarted {
            step_id: step.id.clone(),
            step_num,
            commit_sha: commit_sha.to_string(),
            iteration,
        })?;
    }

    // O(1) reviewer diff: EXACTLY one commit's `git show` patch. This is the
    // single place the reviewer diff is produced — never a cumulative/range
    // or dependency diff (Decision 5 / §9 hard invariant).
    let commit_diff = git::show_commit_diff(workdir, commit_sha)?;
    let review_prompt = prompt::build_review_prompt(plan, step, &short, iteration, &commit_diff);

    let (args, delivery) = harness::prepare_harness_invocation(
        review_harness_name,
        harness_config,
        &review_prompt,
        None, // reviewer uses no agent file — the rubric IS the acceptance criteria
        model_override,
    )?;
    // The reviewer gets no agent-file env (None above), so this is empty in
    // practice; kept for parity with the implementation spawn path.
    let env_vars = harness::build_harness_env(harness_config, None);

    // PROVE read-only: snapshot tree+HEAD, run, assert unchanged.
    let guard = ReviewTreeGuard::capture(workdir);
    let (child, _tmp) =
        harness::spawn_harness_with_delivery(harness_config, &args, &env_vars, workdir, delivery)
            .await?;
    let output_captured = child
        .wait_with_output()
        .await
        .context("failed to wait for review harness")?;
    let stdout = String::from_utf8_lossy(&output_captured.stdout).to_string();

    // HARD INVARIANT: a review never touches the working tree / HEAD.
    assert_tree_unchanged_by_review(workdir, &guard)?;

    let verdict = parse_review_verdict(&stdout);

    match verdict {
        ReviewVerdict::Pass => {
            storage::update_step_review_status(conn, &step.id, ReviewStatus::Passed)?;
            // History-safe verdict annotation (note on a fixed SHA — never an
            // amend, see git::annotate_review_verdict).
            git::annotate_review_verdict(workdir, commit_sha, "passed")?;
            if out.format == OutputFormat::Json {
                output::emit_ndjson(&RunEvent::ReviewFinished {
                    step_id: step.id.clone(),
                    step_num,
                    commit_sha: commit_sha.to_string(),
                    iteration,
                    passed: true,
                })?;
            } else {
                eprintln!(
                    "  review of {short} ({}.{iteration}) ... PASS",
                    step.short_id
                );
            }
            Ok(ReviewOutcome::Passed)
        }
        ReviewVerdict::Fail { issues } => {
            storage::update_step_review_status(conn, &step.id, ReviewStatus::Failed)?;
            git::annotate_review_verdict(workdir, commit_sha, "failed")?;

            // STEP 39 — the reviewer side of the §9-inv-3 structured channel:
            // *request* a corrective step (DB bridge row + NDJSON event).
            // This is the ONLY DAG-adjacent write the review path performs,
            // and it writes a *request*, not the DAG. The orchestrator
            // consumes it later as the sole writer.
            let request_id = storage::insert_corrective_step_request(
                conn,
                &step.id,
                iteration,
                commit_sha,
                issues,
                last_nonempty_line(&stdout).as_deref(),
            )?;

            if out.format == OutputFormat::Json {
                output::emit_ndjson(&RunEvent::ReviewFinished {
                    step_id: step.id.clone(),
                    step_num,
                    commit_sha: commit_sha.to_string(),
                    iteration,
                    passed: false,
                })?;
                output::emit_ndjson(&RunEvent::CorrectiveStepRequested {
                    reviewed_step_id: step.id.clone(),
                    reviewed_step_num: step_num,
                    commit_sha: commit_sha.to_string(),
                    iteration,
                    issues,
                })?;
            } else {
                eprintln!(
                    "  review of {short} ({}.{iteration}) ... FAIL ({issues} issue(s)) — corrective step requested",
                    step.short_id
                );
            }
            Ok(ReviewOutcome::Failed { request_id, issues })
        }
    }
}

/// The reviewer's last non-empty stdout line, used as the corrective
/// request's `verdict_body` (a short human-readable note — bounded by taking
/// only the final line, never the whole transcript, so the bridge row stays
/// O(1) like the §4-bounded interruption fields).
fn last_nonempty_line(s: &str) -> Option<String> {
    s.lines()
        .rev()
        .map(str::trim)
        .find(|l| !l.is_empty())
        .map(str::to_string)
}

/// Outcome of the orchestrator draining one corrective-step request
/// (STEP 40 / STEP 41). Returned so the runner can log/emit appropriately.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CorrectiveConsumeOutcome {
    /// A corrective step `A′` was inserted and dependents re-parented.
    Inserted {
        corrective_step_id: String,
        corrective_short_id: String,
    },
    /// The recursion cap was hit; a `kind=blocker` interruption was raised
    /// instead of spawning another correction (§10 item 4 / §14.5).
    Escalated { chain_len: usize, cap: usize },
    /// The request was already consumed by a prior tick (no-op — the
    /// single-writer guard fired).
    AlreadyConsumed,
}

/// Drain a single corrective-step request as the SOLE DAG writer (STEP 40,
/// docs/dag-redesign.md §9-inv-3 / §10). The reviewer only ever *requested*
/// this; the mutation happens here, in the orchestrator, exactly once.
///
/// Performs, atomically from the scheduler's point of view:
///  1. Consume the bridge row (predicate-guarded — `Ok(false)` ⇒ a prior
///     tick already handled it; we return `AlreadyConsumed` and write
///     nothing — the §9-inv-3 single-writer guarantee even under a double
///     drain).
///  2. **Recursion-cap check (STEP 41 / §10 item 4 / §14.5):** if inserting
///     another correction would exceed the per-plan
///     `max_review_corrections`, raise ONE `kind=blocker` interruption
///     ("review loop — needs human") on the reviewed step and STOP — no new
///     step is spawned.
///  3. Otherwise insert corrective step `A′` (`corrects_step_id = A`,
///     `A′ depends_on A`) immediately after `A` in sort order, and
///     **re-parent**: every step that depended on `A` now ALSO depends on
///     `A′` (via the cycle-safe `add_step_dependency`).
///  4. Transition `A` to `Complete` with `review_status = Failed` (its
///     commit stays in history; the fix lives in `A′`). Dependents are gated
///     by the new structural edge to `A′`, not `A`'s status.
pub fn consume_corrective_request(
    conn: &Connection,
    plan: &Plan,
    request: &storage::CorrectiveStepRequest,
    out: &OutputContext,
) -> Result<CorrectiveConsumeOutcome> {
    // (1) Single-writer guard: only the tick that flips open→consumed acts.
    if !storage::consume_corrective_step_request(conn, &request.id)? {
        return Ok(CorrectiveConsumeOutcome::AlreadyConsumed);
    }

    let reviewed = storage::get_step(conn, &request.reviewed_step_id)?;
    let step_num = step_position(conn, plan, &reviewed.id)?;

    // (2) Recursion cap (STEP 41 / §10 item 4 / §14.5). The chain length the
    // *next* correction would have is `len(reviewed) + 1`: if `reviewed` is
    // itself a corrective step A′ (chain_len 1), inserting A″ would be
    // chain_len 2, etc. Escalate when that would exceed the cap.
    let cap = effective_max_review_corrections(plan);
    let next_chain_len = storage::corrective_chain_len(conn, &reviewed.id)? + 1;
    if next_chain_len > cap {
        // Raise exactly ONE blocker interruption and stop spawning.
        storage::insert_interruption(
            conn,
            &reviewed.id,
            request.reviewed_iteration,
            InterruptionKind::Blocker,
            &format!(
                "review loop — needs human: step '{}' has been corrected {} time(s) \
                 and still fails review (cap {}). A human must intervene; ralph will \
                 not spawn further corrective steps for this chain.",
                reviewed.title,
                next_chain_len - 1,
                cap
            ),
            &[],
        )?;
        // The reviewed step still becomes Complete/Failed (its commit is
        // history); the open blocker keeps its dependents gated until a
        // human resolves it, exactly like any other interruption.
        finalize_reviewed_step_failed(conn, &reviewed.id)?;
        if out.format == OutputFormat::Json {
            output::emit_ndjson(&RunEvent::ReviewLoopEscalated {
                step_id: reviewed.id.clone(),
                step_num,
                chain_len: next_chain_len - 1,
                cap,
            })?;
        } else {
            eprintln!(
                "  review loop on '{}' exceeded cap {cap} — raised a blocker (needs human)",
                reviewed.title
            );
        }
        return Ok(CorrectiveConsumeOutcome::Escalated {
            chain_len: next_chain_len - 1,
            cap,
        });
    }

    // (3) Insert corrective step A′ immediately after A (sort order), then
    // re-parent every former dependent of A onto A′.
    let (corrective, _pos) = insert_corrective_step(conn, plan, &reviewed)?;
    storage::add_step_dependency(conn, &corrective.id, &reviewed.id)?;
    storage::set_step_corrects_step_id(conn, &corrective.id, Some(&reviewed.id))?;

    // Re-parent: snapshot A's former direct dependents *before* adding A′'s
    // own edge (A′ depends_on A is already in place; we must not re-point A′
    // at itself). Every other former dependent of A now ALSO depends on A′.
    let former_dependents = storage::list_step_dependents(conn, &reviewed.id)?;
    for dep in former_dependents {
        if dep == corrective.id {
            continue; // the A′ -> A edge we just added
        }
        // Cycle-safe (would_create_step_cycle guards inside).
        storage::add_step_dependency(conn, &dep, &corrective.id)?;
    }

    // (4) A becomes Complete with review_status = Failed (its commit stays in
    // history; dependents are gated by the structural edge to A′, not A's
    // status).
    finalize_reviewed_step_failed(conn, &reviewed.id)?;

    if out.format == OutputFormat::Json {
        output::emit_ndjson(&RunEvent::CorrectiveStepInserted {
            corrective_step_id: corrective.id.clone(),
            corrective_short_id: corrective.short_id.clone(),
            corrects_step_id: reviewed.id.clone(),
        })?;
    } else {
        eprintln!(
            "  inserted corrective step {} (corrects '{}') + re-parented dependents",
            corrective.short_id, reviewed.title
        );
    }

    Ok(CorrectiveConsumeOutcome::Inserted {
        corrective_step_id: corrective.id,
        corrective_short_id: corrective.short_id,
    })
}

/// Transition the reviewed step to `Complete` with `review_status = Failed`
/// (§10 item 3). Its per-iteration commit stays in linear history; the fix
/// lives in the corrective step (or, on escalation, awaits a human). Done in
/// the orchestrator only.
fn finalize_reviewed_step_failed(conn: &Connection, step_id: &str) -> Result<()> {
    storage::update_step_review_status(conn, step_id, ReviewStatus::Failed)?;
    storage::update_step_status(conn, step_id, crate::plan::StepStatus::Complete)?;
    Ok(())
}

/// Insert corrective step `A′` immediately after `A` in sort order (§10).
/// The corrective step is `change_policy = Required` (it MUST change code —
/// §14.7) and inherits `A`'s harness/agent/model so the fix is implemented
/// the same way the original was. Its acceptance criteria are `A`'s criteria
/// plus the review's defect note, so the next implementation knows what to
/// fix and the *next* review has the same rubric.
fn insert_corrective_step(
    conn: &Connection,
    plan: &Plan,
    reviewed: &Step,
) -> Result<(Step, usize)> {
    let all = storage::list_steps(conn, &plan.id)?;
    let idx = all
        .iter()
        .position(|s| s.id == reviewed.id)
        .context("reviewed step vanished before corrective insert")?;
    // sort_key strictly between A and the next step (or after A if A is last)
    // so A′ is scheduled immediately after A. `create_step_at` takes an
    // explicit key; mirror `step add --after` keying.
    let sort_key = match all.get(idx + 1) {
        Some(next) => crate::frac_index::key_between(&reviewed.sort_key, &next.sort_key)
            .or_else(|_| crate::frac_index::key_after(&reviewed.sort_key))
            .context("could not allocate sort_key for corrective step")?,
        None => crate::frac_index::key_after(&reviewed.sort_key)
            .context("could not allocate sort_key for corrective step")?,
    };

    let title = format!("Fix review defects in: {}", reviewed.title);
    let description = format!(
        "A read-only review of `{}` rejected the implementation. Correct the \
         defect(s) so this step's acceptance criteria are genuinely met. This \
         is a corrective step inserted by ralph's review pipeline; everything \
         that depended on the original step now depends on THIS step.",
        reviewed.title
    );
    let mut criteria = reviewed.acceptance_criteria.clone();
    criteria.push(
        "The defects flagged by the prior review are fixed and the original \
         step's acceptance criteria genuinely hold."
            .to_string(),
    );

    storage::create_step_at(
        conn,
        &plan.id,
        &sort_key,
        &title,
        &description,
        reviewed.agent.as_deref(),
        reviewed.harness.as_deref(),
        &criteria,
        reviewed.max_retries,
        reviewed.model.as_deref(),
        Some(crate::plan::ChangePolicy::Required),
        None,
    )
}

/// 1-based position of `step_id` in `plan` (best-effort, for event/log
/// payloads only — never used for scheduling).
fn step_position(conn: &Connection, plan: &Plan, step_id: &str) -> Result<usize> {
    let all = storage::list_steps(conn, &plan.id)?;
    Ok(all
        .iter()
        .position(|s| s.id == step_id)
        .map(|i| i + 1)
        .unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_review_verdict_pass() {
        assert_eq!(
            parse_review_verdict("looks good\nREVIEW PASS"),
            ReviewVerdict::Pass
        );
        assert_eq!(
            parse_review_verdict("  review pass  "),
            ReviewVerdict::Pass,
            "case-insensitive + trimmed"
        );
    }

    #[test]
    fn test_parse_review_verdict_fail_with_count() {
        assert_eq!(
            parse_review_verdict("REVIEW FAIL — 3 issue(s)"),
            ReviewVerdict::Fail { issues: 3 }
        );
        assert_eq!(
            parse_review_verdict("REVIEW FAIL - one issue"),
            ReviewVerdict::Fail { issues: 1 },
            "no parseable integer ⇒ default 1"
        );
    }

    #[test]
    fn test_parse_review_verdict_last_line_wins() {
        // A transcript that QUOTES the contract earlier must not be mistaken
        // for the verdict; the final verdict line wins (scan bottom-up).
        let t = "I will emit REVIEW PASS or REVIEW FAIL — N issue(s).\n\
                 Analysis...\n\
                 REVIEW FAIL — 2 issue(s)";
        assert_eq!(parse_review_verdict(t), ReviewVerdict::Fail { issues: 2 });
    }

    #[test]
    fn test_parse_review_verdict_missing_is_fail_safe() {
        // No verdict line at all: an unreadable review must NEVER pass work.
        assert_eq!(
            parse_review_verdict("the harness rambled and never concluded"),
            ReviewVerdict::Fail { issues: 1 }
        );
        assert_eq!(parse_review_verdict(""), ReviewVerdict::Fail { issues: 1 });
    }

    /// Minimal in-memory plan for pure-function tests (no DB).
    fn bare_plan() -> Plan {
        Plan {
            id: "p1".to_string(),
            slug: "p".to_string(),
            project: "/tmp".to_string(),
            branch_name: "b".to_string(),
            description: String::new(),
            status: crate::plan::PlanStatus::InProgress,
            harness: None,
            agent: None,
            deterministic_tests: vec![],
            plan_harness: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            questions_enabled: false,
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            retry_strategy: None,
            review_enabled: None,
            squash_on_complete: false,
            max_review_corrections: None,
        }
    }

    #[test]
    fn test_effective_max_review_corrections() {
        let mut p = bare_plan();
        assert_eq!(
            effective_max_review_corrections(&p),
            DEFAULT_MAX_REVIEW_CORRECTIONS,
            "None ⇒ built-in default"
        );
        p.max_review_corrections = Some(5);
        assert_eq!(effective_max_review_corrections(&p), 5);
        p.max_review_corrections = Some(-1);
        assert_eq!(
            effective_max_review_corrections(&p),
            0,
            "non-positive clamps to 0 (immediate escalation)"
        );
    }

    #[test]
    fn test_last_nonempty_line() {
        assert_eq!(last_nonempty_line("a\nb\n\n  \n"), Some("b".to_string()));
        assert_eq!(last_nonempty_line(""), None);
    }

    // ---------------------------------------------------------------------
    // Integration tests (real git repo + in-memory DB + stub harness).
    // These prove the §9 hard invariants: read-only review (STEP 37),
    // single DAG writer / structured channel (STEP 39), corrective insert +
    // re-parent (STEP 40), recursion cap → blocker (STEP 41).
    // ---------------------------------------------------------------------

    use crate::config::{Config, HarnessConfig};
    use crate::plan::StepStatus;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    fn git_run(dir: &Path, args: &[&str]) {
        let out = std::process::Command::new("git")
            .args(args)
            .current_dir(dir)
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&out.stderr)
        );
    }

    fn init_repo(dir: &Path) {
        git_run(dir, &["init", "-q"]);
        git_run(dir, &["config", "user.email", "t@t.com"]);
        git_run(dir, &["config", "user.name", "t"]);
        fs::write(dir.join("README.md"), "init\n").unwrap();
        git_run(dir, &["add", "-A"]);
        git_run(dir, &["commit", "-q", "-m", "init"]);
    }

    /// Write a stub "review harness" shell script that prints a fixed
    /// verdict and (optionally) tries to mutate the tree, then make it
    /// executable. We invoke it via `/bin/sh <path>` (see CLAUDE.md ETXTBSY
    /// footgun) by configuring the harness `command` as `sh`.
    fn write_stub(dir: &Path, name: &str, body: &str) -> String {
        let p = dir.join(name);
        fs::write(&p, format!("#!/bin/sh\n{body}\n")).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut perms = fs::metadata(&p).unwrap().permissions();
            perms.set_mode(0o755);
            fs::set_permissions(&p, perms).unwrap();
        }
        p.to_string_lossy().into_owned()
    }

    fn config_with_review_harness(script_path: &str) -> Config {
        let mut config = Config::default();
        // `sh <script>` avoids the ETXTBSY exec footgun (CLAUDE.md).
        config.harnesses.insert(
            "reviewer".to_string(),
            HarnessConfig {
                command: "sh".to_string(),
                args: vec![script_path.to_string()],
                plan_args: vec![],
                supports_agent_file: false,
                supports_json_output: false,
                json_output_args: vec![],
                agent_file_env: None,
                agent_file_args: vec![],
                model_args: vec![],
                default_model: None,
                auth_env_vars: vec![],
                auth_probe_args: vec![],
                prompt_input: crate::config::PromptInputMode::Stdin,
                argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );
        config.review.enabled = Some(true);
        config.review.harness = "reviewer".to_string();
        config
    }

    /// Create a plan + a step, make a real per-iteration commit, return
    /// `(conn, plan, step, commit_sha)`.
    fn seed_committed_step(dir: &Path) -> (rusqlite::Connection, Plan, Step, String) {
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "rev-plan",
            &dir.to_string_lossy(),
            "branch",
            "desc",
            None,
            None,
            &[],
        )
        .unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            "Implement widget",
            "build the widget",
            None,
            None,
            &["The widget builds".to_string()],
            Some(0),
            None,
            None,
            None,
        )
        .unwrap();
        // A real committed iteration the reviewer runs `git show` against.
        fs::write(dir.join("widget.rs"), "fn widget() {}\n").unwrap();
        git_run(dir, &["add", "-A"]);
        let msg =
            crate::git::build_iteration_commit_message(&step.short_id, 1, &step.title, &plan.slug);
        git_run(dir, &["commit", "-q", "-m", &msg]);
        let sha = crate::git::get_commit_hash(dir).unwrap();
        (conn, plan, step, sha)
    }

    fn silent_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            color: false,
            quiet: true,
        }
    }

    #[tokio::test]
    async fn test_review_pass_transitions_status_and_annotates_trailer() {
        // STEP 37: a PASS verdict ⇒ review_status Passed + the commit's
        // Ralph-Review trailer annotated `passed` (history-safe note).
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        init_repo(dir);
        let script = write_stub(dir, "rev.sh", "echo 'looks correct'\necho 'REVIEW PASS'");
        let config = config_with_review_harness(&script);
        let (conn, plan, step, sha) = seed_committed_step(dir);

        let outcome = run_review(&conn, &plan, &step, &config, dir, &sha, 1, 1, &silent_out())
            .await
            .unwrap();

        assert_eq!(outcome, ReviewOutcome::Passed);
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(s.review_status, Some(ReviewStatus::Passed));
        assert_eq!(
            crate::git::read_review_verdict(dir, &sha)
                .unwrap()
                .as_deref(),
            Some("passed"),
            "Ralph-Review trailer must be annotated 'passed'"
        );
    }

    /// HARD-INVARIANT PROOF (§9-inv-2): a review is strictly read-only
    /// w.r.t. the working tree. A reviewer that tries to edit the tree /
    /// move HEAD is detected and the review errors — un-reviewed work is
    /// never silently passed.
    #[tokio::test]
    async fn test_review_is_read_only_wrt_working_tree() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        init_repo(dir);
        // Malicious reviewer: mutates a tracked file before "passing".
        let script = write_stub(
            dir,
            "evil.sh",
            "echo 'tampered' >> README.md\necho 'REVIEW PASS'",
        );
        let config = config_with_review_harness(&script);
        let (conn, plan, step, sha) = seed_committed_step(dir);

        let res = run_review(&conn, &plan, &step, &config, dir, &sha, 1, 1, &silent_out()).await;

        assert!(
            res.is_err(),
            "a reviewer that mutated the working tree MUST be rejected \
             (§9-inv-2 read-only review hard invariant)"
        );
        let msg = format!("{:#}", res.unwrap_err());
        assert!(
            msg.contains("read-only review invariant violated"),
            "error must name the violated invariant, got: {msg}"
        );
    }

    /// HARD-INVARIANT PROOF (§9-inv-3): a failed review does NOT mutate the
    /// DAG. It only writes a *request* (V29 bridge row); step rows/edges are
    /// untouched until the orchestrator consumes it.
    #[tokio::test]
    async fn test_failed_review_only_requests_never_mutates_dag() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        init_repo(dir);
        let script = write_stub(dir, "fail.sh", "echo 'REVIEW FAIL — 2 issue(s)'");
        let config = config_with_review_harness(&script);
        let (conn, plan, step, sha) = seed_committed_step(dir);

        let steps_before = storage::list_steps(&conn, &plan.id).unwrap().len();
        let edges_before = storage::list_step_dependency_edges(&conn, &plan.id).unwrap();

        let outcome = run_review(&conn, &plan, &step, &config, dir, &sha, 1, 1, &silent_out())
            .await
            .unwrap();

        // The reviewer requested — but did NOT perform — a correction.
        match outcome {
            ReviewOutcome::Failed { issues, .. } => assert_eq!(issues, 2),
            other => panic!("expected Failed, got {other:?}"),
        }
        // DAG is byte-for-byte unchanged: no new step, no new edge.
        assert_eq!(
            storage::list_steps(&conn, &plan.id).unwrap().len(),
            steps_before,
            "a reviewer must NEVER insert a step row (§9-inv-3)"
        );
        assert_eq!(
            storage::list_step_dependency_edges(&conn, &plan.id).unwrap(),
            edges_before,
            "a reviewer must NEVER write an edge (§9-inv-3)"
        );
        // The request IS delivered, but only through the channel.
        let reqs = storage::list_open_corrective_step_requests_for_plan(&conn, &plan.id).unwrap();
        assert_eq!(reqs.len(), 1, "the request is delivered via the V29 bridge");
        assert_eq!(reqs[0].reviewed_step_id, step.id);
        assert_eq!(reqs[0].issues, 2);
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(s.review_status, Some(ReviewStatus::Failed));
        assert_eq!(
            crate::git::read_review_verdict(dir, &sha)
                .unwrap()
                .as_deref(),
            Some("failed")
        );
    }

    /// HARD-INVARIANT PROOF (§10): the orchestrator (sole writer) consuming
    /// a corrective request inserts A′ (corrects_step_id + edge), RE-PARENTS
    /// every former dependent of A onto A′, and finalizes A
    /// Complete/review_status=Failed; a dependent cannot run until A′ is
    /// Complete.
    #[test]
    fn test_consume_corrective_request_inserts_and_reparents() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        init_repo(dir);
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "dag-plan",
            &dir.to_string_lossy(),
            "b",
            "d",
            None,
            None,
            &[],
        )
        .unwrap();
        // A -> B (B depends_on A), and a sibling C depends_on A too.
        let (a, _) = storage::create_step(
            &conn,
            &plan.id,
            "A",
            "d",
            None,
            None,
            &[],
            Some(0),
            None,
            None,
            None,
        )
        .unwrap();
        let (b, _) = storage::create_step(
            &conn,
            &plan.id,
            "B",
            "d",
            None,
            None,
            &[],
            Some(0),
            None,
            None,
            None,
        )
        .unwrap();
        let (c, _) = storage::create_step(
            &conn,
            &plan.id,
            "C",
            "d",
            None,
            None,
            &[],
            Some(0),
            None,
            None,
            None,
        )
        .unwrap();
        storage::add_step_dependency(&conn, &b.id, &a.id).unwrap();
        storage::add_step_dependency(&conn, &c.id, &a.id).unwrap();

        // A reviewer requested a correction for A.
        let req_id = storage::insert_corrective_step_request(
            &conn,
            &a.id,
            1,
            "deadbeef",
            1,
            Some("missing X"),
        )
        .unwrap();
        let req = storage::list_open_corrective_step_requests_for_plan(&conn, &plan.id).unwrap()[0]
            .clone();
        assert_eq!(req.id, req_id);

        let plan = storage::get_plan_by_id(&conn, &plan.id).unwrap();
        let res = consume_corrective_request(&conn, &plan, &req, &silent_out()).unwrap();
        let a_prime_id = match res {
            CorrectiveConsumeOutcome::Inserted {
                corrective_step_id, ..
            } => corrective_step_id,
            other => panic!("expected Inserted, got {other:?}"),
        };

        // A′ has corrects_step_id = A and an edge A′ depends_on A.
        let a_prime = storage::get_step(&conn, &a_prime_id).unwrap();
        assert_eq!(a_prime.corrects_step_id.as_deref(), Some(a.id.as_str()));
        assert!(
            storage::list_step_dependencies(&conn, &a_prime_id)
                .unwrap()
                .contains(&a.id)
        );

        // RE-PARENT: every former dependent of A (B and C) now ALSO depends
        // on A′.
        for dep in [&b.id, &c.id] {
            let deps = storage::list_step_dependencies(&conn, dep).unwrap();
            assert!(
                deps.contains(&a_prime_id),
                "former dependent {dep} must be re-pointed at A′ (§10)"
            );
            assert!(deps.contains(&a.id), "the original A edge is preserved");
        }

        // A is Complete with review_status = Failed (its commit stays in
        // history; the fix lives in A′).
        let a_after = storage::get_step(&conn, &a.id).unwrap();
        assert_eq!(a_after.status, StepStatus::Complete);
        assert_eq!(a_after.review_status, Some(ReviewStatus::Failed));

        // A dependent cannot run until A′ is Complete: B depends on A′, and
        // A′ is freshly Pending.
        assert_eq!(a_prime.status, StepStatus::Pending);

        // Single-writer guard: a second consume of the same (now consumed)
        // request is a no-op (no duplicate A″).
        let again = consume_corrective_request(&conn, &plan, &req, &silent_out()).unwrap();
        assert_eq!(again, CorrectiveConsumeOutcome::AlreadyConsumed);
        let n_corrective = storage::list_steps(&conn, &plan.id)
            .unwrap()
            .iter()
            .filter(|s| s.corrects_step_id.is_some())
            .count();
        assert_eq!(n_corrective, 1, "consume must be exactly-once (§9-inv-3)");
    }

    /// STEP 41 / §10 item 4 / §14.5: the review→correction→review chain is
    /// bounded by `max_review_corrections`; exceeding it raises EXACTLY ONE
    /// blocker interruption and stops spawning corrective steps.
    #[test]
    fn test_recursion_cap_escalates_to_single_blocker() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        init_repo(dir);
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            "cap-plan",
            &dir.to_string_lossy(),
            "b",
            "d",
            None,
            None,
            &[],
        )
        .unwrap();
        // Tight cap: at most 1 correction in the chain.
        storage::set_plan_max_review_corrections(&conn, &plan.id, Some(1)).unwrap();
        let plan = storage::get_plan_by_id(&conn, &plan.id).unwrap();

        let (a, _) = storage::create_step(
            &conn,
            &plan.id,
            "A",
            "d",
            None,
            None,
            &[],
            Some(0),
            None,
            None,
            None,
        )
        .unwrap();

        // 1st failed review of A ⇒ A′ inserted (chain_len 1 ≤ cap 1).
        let req = {
            storage::insert_corrective_step_request(&conn, &a.id, 1, "sha1", 1, None).unwrap();
            storage::list_open_corrective_step_requests_for_plan(&conn, &plan.id).unwrap()[0]
                .clone()
        };
        let a_prime_id =
            match consume_corrective_request(&conn, &plan, &req, &silent_out()).unwrap() {
                CorrectiveConsumeOutcome::Inserted {
                    corrective_step_id, ..
                } => corrective_step_id,
                other => panic!("first correction must insert, got {other:?}"),
            };

        // 2nd failed review — now of A′ — would be chain_len 2 > cap 1 ⇒
        // ESCALATE to a blocker, NO new step.
        let steps_before = storage::list_steps(&conn, &plan.id).unwrap().len();
        let req2 = {
            storage::insert_corrective_step_request(&conn, &a_prime_id, 1, "sha2", 1, None)
                .unwrap();
            storage::list_open_corrective_step_requests_for_plan(&conn, &plan.id).unwrap()[0]
                .clone()
        };
        let res2 = consume_corrective_request(&conn, &plan, &req2, &silent_out()).unwrap();
        match res2 {
            CorrectiveConsumeOutcome::Escalated { cap, .. } => assert_eq!(cap, 1),
            other => panic!("expected Escalated at the cap, got {other:?}"),
        }

        // No new corrective step was spawned.
        assert_eq!(
            storage::list_steps(&conn, &plan.id).unwrap().len(),
            steps_before,
            "exceeding the cap must NOT spawn another correction"
        );
        // EXACTLY ONE blocker interruption was raised, on A′.
        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        let blockers: Vec<_> = open
            .iter()
            .filter(|i| i.kind == crate::plan::InterruptionKind::Blocker)
            .collect();
        assert_eq!(blockers.len(), 1, "exactly one blocker must be raised");
        assert_eq!(blockers[0].step_id, a_prime_id);
        assert!(
            blockers[0].body.contains("review loop"),
            "blocker body must name the review loop"
        );
    }
}
