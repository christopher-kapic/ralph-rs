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
//  - Reviews are strictly read-only w.r.t. the working tree (§9-inv-2),
//    enforced *structurally*: the reviewer harness is spawned in a THROWAWAY
//    detached `git worktree` pinned at the reviewed SHA, NOT in the shared
//    implementation workdir. It is physically incapable of touching the live
//    tree the next implementation commits from. `assert_tree_unchanged_by_review`
//    is kept as a cheap defense-in-depth ancestry check on the main repo.
//  - Single DAG writer: nothing in the reviewer path writes step rows/edges;
//    only `consume_corrective_request` (orchestrator) does.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use rusqlite::Connection;

use crate::config::Config;
use crate::output::{OutputContext, OutputFormat, RunEvent};
use crate::plan::{InterruptionKind, Plan, ReviewStatus, Step, StepStatus};
use crate::{git, harness, io_util, output, prompt, storage};

/// Bounded-tail cap for the reviewer subprocess's stdout/stderr. Matches the
/// implementation harness's `HARNESS_OUTPUT_TAIL_BYTES` (executor.rs) — a
/// reviewer is a coding-agent harness producing comparable transcript volume,
/// so the same 4 MiB tail rationale applies (keep the verdict-bearing end of
/// the transcript without letting a runaway reviewer balloon memory).
const REVIEW_OUTPUT_TAIL_BYTES: usize = 4 * 1024 * 1024;

/// Built-in default for the per-plan review→correction→review recursion cap
/// (docs/dag-redesign.md §10 item 4 / §14.5). Used when a plan's
/// `max_review_corrections` is `None` (unset). A small bound: review is a
/// safety net, not an iterative optimizer — if three successive corrections
/// of the same step still fail review, a human needs to look.
pub const DEFAULT_MAX_REVIEW_CORRECTIONS: usize = 3;

/// Machine-recognizable body prefix on the "review loop — needs human"
/// escalation blocker raised by [`consume_corrective_request`] when a
/// corrective chain exceeds `max_review_corrections` (§10 item 4 / §14.5).
/// The interruption resolver (`commands::interruption`) dispatches on this
/// prefix to grant exactly ONE more review→correction cycle (inserting a
/// `human_approved = true` corrective request). Mirrors
/// [`crate::runner::PARKED_RESTORE_BLOCKER_MARKER`] — a body marker (not an
/// option-content check) because this blocker carries empty options.
pub const REVIEW_LOOP_ESCALATION_MARKER: &str = "[ralph:review-loop-escalation]";

/// True iff `body` is a "review loop — needs human" escalation blocker body
/// (detected by the [`REVIEW_LOOP_ESCALATION_MARKER`] prefix). Mirrors
/// `commands::interruption::is_parked_restore_blocker` but takes the body
/// string directly so callers that only have the body can use it too.
pub fn is_review_loop_escalation_blocker(body: &str) -> bool {
    body.starts_with(REVIEW_LOOP_ESCALATION_MARKER)
}

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
        if let Some(rest) = upper.strip_prefix("REVIEW PASS") {
            // The contract makes the PASS line *exactly* `REVIEW PASS` with
            // nothing after it (only FAIL carries free text). Accept trailing
            // punctuation/whitespace (`REVIEW PASS.`) but reject a word
            // continuation (`REVIEW PASSED WITH 3 CAVEATS`): an ambiguous
            // pass-with-caveats line must fall through to the fail-safe rather
            // than silently passing un-reviewed work. A non-matching line just
            // keeps the bottom-up scan going.
            if !rest
                .chars()
                .next()
                .is_some_and(|c| c.is_ascii_alphanumeric())
            {
                return ReviewVerdict::Pass;
            }
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

/// Snapshot taken just before a reviewer subprocess runs, used by
/// [`assert_tree_unchanged_by_review`] as **defense-in-depth** for the
/// history dimension of the §9-inv-2 hard invariant. The *primary* guarantee
/// is now structural: the reviewer runs in a throwaway detached worktree
/// (`git::ReviewWorktree`) and is physically unable to touch the main
/// workdir/HEAD. This ancestry snapshot remains as a cheap belt-and-suspenders
/// check that the reviewed commit is still reachable from the main repo's
/// HEAD afterwards (catching a hypothetical reviewer that rewrote the
/// reviewed line). A review runs against a *fixed commit SHA* and must never
/// check out, edit, amend, reset, or rebase the line of history it is
/// reviewing.
///
/// **Why reachability-from-HEAD, not a live HEAD/worktree snapshot:** under
/// the spec's concurrency model (§2 Decision 3 / §3.5 item 3) a review of
/// step A runs *concurrently with the next unrelated implementation*
/// (step B), which legitimately commits — advancing `HEAD` and transiently
/// dirtying the shared worktree (the explicitly-accepted §5 linear-history
/// entanglement the retry path's dirty-tree-preservation already tolerates). A
/// live-HEAD / live-worktree snapshot around the reviewer therefore *cannot*
/// distinguish "the reviewer mutated state" from "a concurrent sibling
/// implementation legitimately committed" — it false-positives on the
/// latter, defeating the entire concurrency payoff. (Pinning the reviewed
/// commit's own object id is also useless: git keeps an amended/orphaned
/// commit reachable *by its SHA* until GC.) The property that *is* sound
/// under concurrency: the reviewed commit stays an **ancestor of HEAD**. A
/// concurrent forward commit keeps it so; a reviewer that checks out /
/// resets / amends / rebases the reviewed line removes it from HEAD's
/// ancestry. See [`git::is_ancestor_of_head`].
#[derive(Debug, Clone)]
pub struct ReviewTreeGuard {
    /// Whether the reviewed commit was an ancestor of HEAD *before* the
    /// reviewer ran. Captured so a synthetic test SHA that was never an
    /// ancestor to begin with degrades the assertion to a no-op rather than
    /// a false failure (real runs always pass a real committed-on-branch
    /// SHA, so this is `true`).
    reviewed_reachable_before: bool,
}

impl ReviewTreeGuard {
    /// Snapshot reachability of the reviewed commit from HEAD before the
    /// reviewer runs. `commit_sha` is the *fixed* SHA the reviewer is told
    /// to `git show` (§8/§9-inv-2).
    pub fn capture(workdir: &Path, commit_sha: &str) -> Self {
        Self {
            reviewed_reachable_before: git::is_ancestor_of_head(workdir, commit_sha)
                .unwrap_or(false),
        }
    }
}

/// Hard assertion that the reviewer subprocess did NOT check out, edit,
/// amend, reset, or rebase the **history line under review**
/// (docs/dag-redesign.md §9 invariant 2). Reviews are "strictly read-only" —
/// this is the guard that makes that machine-checkable rather than
/// aspirational. A violation is a blocker (returns `Err`), never silently
/// tolerated, because a review that rewrote the reviewed history would
/// corrupt the linear history the concurrent implementation is building on.
///
/// This deliberately does **not** compare live `HEAD` / worktree: under
/// genuine concurrency (§2 Decision 3) the next unrelated implementation
/// commits while this review runs, which legitimately moves `HEAD` and
/// transiently dirties the shared tree (accepted §5 entanglement). The
/// sound, concurrency-safe invariant is that the *reviewed commit remains
/// an ancestor of HEAD* — a concurrent forward commit preserves that; only
/// a tampering reviewer (checkout/reset/amend/rebase of the reviewed line)
/// breaks it. See [`ReviewTreeGuard`].
pub fn assert_tree_unchanged_by_review(
    workdir: &Path,
    commit_sha: &str,
    before: &ReviewTreeGuard,
) -> Result<()> {
    if !before.reviewed_reachable_before {
        // The reviewed commit was not an ancestor of HEAD even before the
        // reviewer ran (synthetic test SHA / detached edge case): the guard
        // cannot make a sound claim, so it is a no-op rather than a false
        // positive. Real runs always review a commit on the plan branch.
        return Ok(());
    }
    if !git::is_ancestor_of_head(workdir, commit_sha)? {
        anyhow::bail!(
            "read-only review invariant violated: the reviewed commit {commit_sha} \
             is no longer an ancestor of HEAD after review. A reviewer must never \
             check out / reset / amend / rebase the history under review \
             (§9-inv-2)."
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
    /// The reviewed step no longer exists — it was removed (e.g. `ralph step
    /// remove`, which CASCADE-deletes the row) while its read-only review was
    /// in flight. The verdict is for a step that is gone, so it is discarded:
    /// no status/verdict write, no corrective request. Without this the
    /// `update_step_review_status`/`update_step_status` calls would
    /// `bail!("Step not found")` and that error would propagate out of the
    /// scheduler, failing the entire run over an orphaned verdict.
    Discarded,
}

/// The verdict-only result a *spawned* (detached, concurrent) review task
/// returns to the orchestrator (docs/dag-redesign.md §3.5 item 3 / §9).
///
/// This is the entire payload the read-only review subprocess produces. It
/// carries **no `Connection`** and performs **no DB mutation** — that is the
/// whole point: a review runs concurrently with the next unrelated
/// implementation precisely because the spawned task touches neither the DB
/// (the orchestrator is the SOLE DAG writer — §9-inv-3) nor the working tree
/// (it ran `git show <fixed sha>` only — §9-inv-2). The orchestrator drains
/// this at a scheduler tick via [`finalize_review`] and performs every DB /
/// git-note side effect there, serialized on the single scheduler loop.
#[derive(Debug, Clone)]
pub struct ReviewTaskResult {
    /// The reviewed step's id (so the orchestrator can re-key DB writes
    /// without holding a `&Step` across the await).
    pub step_id: String,
    /// 1-based step position, for event/log payloads only.
    pub step_num: usize,
    /// The FIXED committed SHA the reviewer ran `git show` against.
    pub commit_sha: String,
    /// The reviewed iteration number.
    pub iteration: i32,
    /// Short SHA (precomputed for human log lines).
    pub short_sha: String,
    /// The parsed verdict.
    pub verdict: ReviewVerdict,
    /// The reviewer's last non-empty stdout line — bounded provenance for the
    /// V29 bridge row's `verdict_body` (never the whole transcript).
    pub verdict_body: Option<String>,
}

/// What a *spawned* (detached) review task hands back to the orchestrator's
/// sole-writer drain. Unlike [`ReviewTaskResult`] (the success-only payload),
/// this wrapper carries the reviewed step's identity **unconditionally** —
/// even when `result` is `Err` (the review subprocess errored, or the
/// §9-inv-2 read-only invariant fired, before any verdict was produced).
///
/// Without this, a review *error* lost the step id, so the drain could only
/// blanket-mark the plan `Failed`: the implementation-complete step was left
/// `InProgress` + `review_status = InFlight` and the *next* run's
/// stale-`InProgress` sweep silently re-implemented it (discarding correct,
/// committed work). With the id in hand the drain instead resets
/// `review_status` and raises a targeted blocker so the failure surfaces
/// cleanly and dependents stay gated.
pub struct SpawnedReview {
    /// The reviewed step's id — present even on `Err`.
    pub step_id: String,
    /// The reviewed iteration number (for the blocker interruption's attempt).
    pub iteration: i32,
    /// The subprocess outcome: `Ok` ⇒ a verdict was produced; `Err` ⇒ the
    /// reviewer could not run / violated the read-only invariant.
    pub result: Result<ReviewTaskResult>,
}

/// Run a synchronous closure (one or more blocking `git()` subprocess calls)
/// without starving the tokio scheduler. `run_review_subprocess` is a DETACHED
/// `tokio::spawn`ed task running on a runtime worker thread; the inline git
/// calls it makes (`short_sha`, `show_commit_diff`, `ReviewWorktree::create`,
/// `assert_tree_unchanged_by_review`) can block (notably under a contended
/// `.git/index.lock`), which on a multi-thread runtime starves the worker. So
/// when we are on a multi-thread runtime, wrap the call in `block_in_place`,
/// which hands the worker's other tasks off to a sibling worker for the
/// duration. On a current-thread runtime `block_in_place` would panic (no
/// sibling to hand work to — e.g. a `#[tokio::test]`, which defaults to
/// current-thread), and with no runtime at all there is nothing to starve —
/// both fall through to a plain inline call. Mirrors the exact rationale of
/// `git::ReviewWorktree::Drop` (git.rs).
fn review_blocking_git<R>(f: impl FnOnce() -> R) -> R {
    let on_multi_thread_runtime = matches!(
        tokio::runtime::Handle::try_current().map(|h| h.runtime_flavor()),
        Ok(tokio::runtime::RuntimeFlavor::MultiThread)
    );
    if on_multi_thread_runtime {
        tokio::task::block_in_place(f)
    } else {
        f()
    }
}

/// Grouped inputs to [`run_review_subprocess`]: the plan/step under review,
/// the config (which selects the review harness), the working dir, and the
/// reviewed commit's SHA / iteration / 1-based step number.
pub struct ReviewSubprocessArgs<'a> {
    pub plan: &'a Plan,
    pub step: &'a Step,
    pub config: &'a Config,
    pub workdir: &'a Path,
    pub commit_sha: &'a str,
    pub iteration: i32,
    pub step_num: usize,
}

/// Run the configured review harness/model against a **committed SHA** and
/// return the parsed verdict — *without touching the DB* (STEP 37,
/// docs/dag-redesign.md §3.5 item 3 / §9-inv-2/3).
///
/// This is the **spawnable** half of the review pipeline: it is `Send` (no
/// `rusqlite::Connection`), so the runner can `tokio::spawn` it and let it
/// run concurrently with the next unrelated implementation while the
/// scheduler loop keeps advancing. It:
///
/// - Builds the dedicated read-only reviewer prompt
///   ([`prompt::build_review_prompt`]) from the *single* `git show <sha>`
///   diff (O(1) — Decision 5).
/// - Reuses `harness.rs` spawn machinery with the **review** harness
///   (`config.review.harness` / `.model`), not the implementation harness.
/// - Spawns the reviewer in a THROWAWAY detached `git worktree` pinned at
///   the reviewed SHA ([`git::ReviewWorktree`]) — *not* in the shared
///   implementation `workdir`. This makes the §9-inv-2 read-only invariant
///   *structural*: a reviewer that does `echo evil >> src/foo.rs` (no commit)
///   writes into the disposable tree, which is torn down on every exit path
///   (RAII `Drop`) and is never the directory the next implementation commits
///   from. The throwaway tree cannot interfere with a concurrent unrelated
///   implementation in the main workdir — which is the entire premise of
///   §3.5-item-3 concurrent review.
/// - Additionally captures a [`ReviewTreeGuard`] on the **main repo** before
///   spawning and asserts the reviewed commit is still an ancestor of HEAD
///   after — kept as cheap defense-in-depth for the history dimension (it
///   catches a reviewer that somehow rewrote the reviewed line). With the
///   reviewer isolated in its own worktree it cannot affect the main tree or
///   HEAD anyway; the assertion is a belt-and-suspenders invariant check.
///
/// It does NOT write `review_status`, does NOT annotate the git note, and
/// does NOT write the V29 bridge row — all of that is the orchestrator's
/// job in [`finalize_review`], serialized on the single scheduler loop so
/// the §9-inv-3 single-DAG-writer guarantee holds even with a review in
/// flight.
pub async fn run_review_subprocess(args: ReviewSubprocessArgs<'_>) -> Result<ReviewTaskResult> {
    let ReviewSubprocessArgs {
        plan,
        step,
        config,
        workdir,
        commit_sha,
        iteration,
        step_num,
    } = args;
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

    // O(1) reviewer diff: EXACTLY one commit's `git show` patch. This is the
    // single place the reviewer diff is produced — never a cumulative/range
    // or dependency diff (Decision 5 / §9 hard invariant). `short_sha` and
    // `show_commit_diff` are synchronous blocking git subprocess calls on this
    // runtime-worker task, so run them under `review_blocking_git`.
    let (short, commit_diff) = review_blocking_git(|| {
        let short = git::short_sha(workdir, commit_sha);
        let commit_diff = git::show_commit_diff(workdir, commit_sha)?;
        Ok::<_, anyhow::Error>((short, commit_diff))
    })?;
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

    // STRUCTURAL §9-inv-2 ENFORCEMENT: spawn the reviewer in a THROWAWAY
    // detached worktree pinned at the reviewed SHA — never the shared
    // implementation `workdir`. A reviewer that edits files (with or without
    // committing) can only touch this disposable tree; it physically cannot
    // reach the live workdir the next implementation commits from. The RAII
    // guard's `Drop` removes the worktree on EVERY exit path of this function
    // (normal return, the `?` below, a spawn/await error, a panic unwinding
    // through this spawned task, or the task being aborted) — there is no
    // path that creates a worktree and leaks it; `git worktree prune` runs
    // unconditionally so no orphan administrative entry survives either.
    // `ReviewWorktree::create` shells out to `git worktree add` (blocking) on
    // this runtime worker — wrap it like the other inline git calls.
    let review_wt = review_blocking_git(|| git::ReviewWorktree::create(workdir, commit_sha))
        .context("could not create isolated review worktree (§9-inv-2)")?;

    // Defense-in-depth (history dimension): snapshot that the reviewed commit
    // is an ancestor of HEAD on the MAIN repo, run, assert it still is. With
    // the reviewer isolated in its own worktree it cannot move the main
    // workdir's HEAD anyway, but this is a cheap invariant check that still
    // catches a reviewer that somehow rewrote the reviewed line. It is sound
    // under genuine concurrency — a concurrent unrelated implementation may
    // legitimately commit & move HEAD forward while this review runs (the
    // accepted §5 entanglement), which keeps the reviewed commit reachable.
    let guard = ReviewTreeGuard::capture(workdir, commit_sha);

    // The harness's cwd is the THROWAWAY worktree, NOT `workdir`. The child
    // is a process-group leader (spawn_harness_with_delivery sets
    // process_group(0) on unix), so a timeout can SIGKILL the whole group.
    let (child, _tmp) = harness::spawn_harness_with_delivery(
        harness_config,
        &args,
        &env_vars,
        review_wt.path(),
        delivery,
    )
    .await?;

    // Bounded concurrent drain + an ALWAYS-PRESENT timeout. Unlike the
    // implementation harness — which honors `config.timeout_secs` and may run
    // untimed when that is `None` — a review is never unbounded: the
    // orchestrator blocks on the in-flight-review `JoinSet` once the runnable
    // set empties, so a reviewer that hangs with no timer would deadlock the
    // whole scheduler (holding the run lock) until Ctrl+C. `effective_timeout_secs`
    // applies the user's explicit `review.timeout_secs` if set, else the
    // built-in default cap (`default_review_timeout_secs`). On expiry the
    // reviewer's process group is SIGKILL'd and the review fails — which the
    // orchestrator treats as a transient review failure (re-run the review,
    // keeping the already-committed work), NOT a step re-implementation.
    let review_timeout_secs = config.review.effective_timeout_secs(config.timeout_secs);
    let timeout = Some(Duration::from_secs(review_timeout_secs));
    let wait = io_util::wait_capped(child, timeout, REVIEW_OUTPUT_TAIL_BYTES).await;
    if wait.timed_out {
        // The process group was already SIGKILL'd and the child reaped
        // inside `wait_capped`. FINDING 3: even on the timeout leg, run the
        // history-rewrite guard before tearing down — a reviewer that escaped
        // its worktree to rewrite the reviewed line and THEN hung would
        // otherwise evade detection entirely. Best-effort: the timeout is the
        // authoritative outcome, so a guard failure here is logged (warning)
        // but must not mask the timeout error we return below.
        if let Err(e) =
            review_blocking_git(|| assert_tree_unchanged_by_review(workdir, commit_sha, &guard))
        {
            eprintln!(
                "warning: review history-rewrite guard failed on the timeout leg \
                 for step '{}': {e:#}",
                step.title
            );
        }
        // Tear the worktree down explicitly before erroring (Drop would also
        // do it on the `?`-return below).
        drop(review_wt);
        return Err(anyhow!(
            "review harness timed out after {review_timeout_secs}s for step '{}' and was \
             killed (process group SIGKILL'd); treating the review as failed",
            step.title
        ));
    }
    // Only stdout feeds `parse_review_verdict`; stderr is captured (bounded)
    // for diagnostics but intentionally unused here.
    let stdout = wait.stdout;

    // Defense-in-depth: the reviewed commit must remain reachable from the
    // main repo's HEAD (catches a hypothetical history rewrite of the
    // reviewed line; the worktree isolation already prevents worktree-only
    // tampering of the live implementation tree structurally).
    review_blocking_git(|| assert_tree_unchanged_by_review(workdir, commit_sha, &guard))?;
    // Explicit teardown on the success path (Drop would also do this on any
    // early return / panic above; calling it here makes the lifecycle
    // unambiguous and frees the disk before the verdict is parsed/returned).
    drop(review_wt);

    let verdict = parse_review_verdict(&stdout);
    Ok(ReviewTaskResult {
        step_id: step.id.clone(),
        step_num,
        commit_sha: commit_sha.to_string(),
        iteration,
        short_sha: short,
        verdict,
        verdict_body: last_nonempty_line(&stdout),
    })
}

/// Drain a finished review's verdict as the SOLE DB writer (STEP 37/39,
/// docs/dag-redesign.md §9-inv-3). Called by the orchestrator from the
/// scheduler loop — NEVER from a spawned task.
///
/// Given the [`ReviewTaskResult`] produced (possibly concurrently) by
/// [`run_review_subprocess`], this performs every DB / git-note side effect,
/// serialized on the single scheduler loop:
///
/// - Transitions `review_status` Passed/Failed.
/// - Annotates the commit's `Ralph-Review` trailer (`passed`/`failed`) via
///   the history-safe note path (note on a fixed SHA — never an amend; this
///   is also serialized here so two concurrent reviews never race on
///   `refs/notes`).
/// - On `Pass`, atomically promotes the reviewed step to `Complete`
///   alongside `review_status = Passed`, closing the crash window where a
///   restart sweep could previously strand `InProgress + Passed` and rerun
///   already-reviewed work.
/// - On `Fail`, writes the V29 bridge row (the §9-inv-3 structured channel)
///   and emits the `CorrectiveStepRequested` event (STEP 39). It NEVER
///   mutates step rows/edges itself — the orchestrator consumes the bridge
///   row separately via [`consume_corrective_request`].
pub fn finalize_review(
    conn: &Connection,
    workdir: &Path,
    result: &ReviewTaskResult,
    out: &OutputContext,
) -> Result<ReviewOutcome> {
    let ReviewTaskResult {
        step_id,
        step_num,
        commit_sha,
        iteration,
        short_sha,
        verdict,
        verdict_body,
    } = result;
    let step_num = *step_num;
    let iteration = *iteration;

    // The review ran detached and concurrently; the step may have been
    // removed (`ralph step remove` → CASCADE) while it was in flight. A
    // verdict for a step that no longer exists is discarded — finalizing it
    // would `bail!("Step not found")` and fail the whole run over an
    // orphaned verdict.
    if storage::get_step_by_id(conn, step_id)?.is_none() {
        if out.format != OutputFormat::Json {
            eprintln!(
                "  review of {short_sha} (.{iteration}) ... step removed mid-review — verdict discarded"
            );
        }
        return Ok(ReviewOutcome::Discarded);
    }

    match verdict {
        ReviewVerdict::Pass => {
            crate::db::with_tx(conn, |conn| {
                storage::update_step_review_status(conn, step_id, ReviewStatus::Passed)?;
                storage::update_step_status(conn, step_id, StepStatus::Complete)?;
                Ok(())
            })?;
            // History-safe verdict annotation (note on a fixed SHA — never an
            // amend, see git::annotate_review_verdict). Best-effort: the
            // verdict is already DURABLY committed above (DB status is the
            // source of truth), and the note is advisory audit only. A failed
            // note write (ref-lock contention on `refs/notes`, a transient git
            // error) must NOT abort the run over a passing review — warn and
            // continue so an operator can notice the note is missing.
            if let Err(e) = git::annotate_review_verdict(workdir, commit_sha, "passed")
                && out.format != OutputFormat::Json
            {
                eprintln!(
                    "  warning: review of {short_sha} (.{iteration}) passed but the \
                     git note could not be written: {e:#} (advisory audit only; the \
                     DB review_status is the source of truth)"
                );
            }
            if out.format == OutputFormat::Json {
                output::emit_ndjson(&RunEvent::ReviewFinished {
                    step_id: step_id.clone(),
                    step_num,
                    commit_sha: commit_sha.clone(),
                    iteration,
                    passed: true,
                })?;
            } else {
                eprintln!("  review of {short_sha} (.{iteration}) ... PASS");
            }
            Ok(ReviewOutcome::Passed)
        }
        ReviewVerdict::Fail { issues } => {
            let issues = *issues;

            // M1 fix: the `Failed` verdict and its V29 corrective-request
            // bridge row must land atomically. Previously these were two
            // separate statements: a crash between them left the step
            // `review_status = Failed` with NO bridge row, and
            // `sweep_stale_in_progress` only rescues `InFlight` (never
            // `Failed`) — so the step ended terminal `Aborted` with no
            // corrective request and its dependents gated forever, with no
            // recovery short of a manual `ralph step reset`. Wrapping both
            // writes in one transaction guarantees the durable corrective
            // request always exists whenever `review_status = Failed` was
            // persisted (the §9-inv-3 promise that the bridge survives a
            // crash between request and drain).
            //
            // STEP 39 — the reviewer side of the §9-inv-3 structured channel:
            // *request* a corrective step (DB bridge row + NDJSON event).
            // This is the ONLY DAG-adjacent write the review path performs,
            // and it writes a *request*, not the DAG. The orchestrator
            // consumes it later as the sole writer.
            let request_id = crate::db::with_tx(conn, |conn| {
                storage::update_step_review_status(conn, step_id, ReviewStatus::Failed)?;
                let request_id = storage::insert_corrective_step_request(
                    conn,
                    step_id,
                    iteration,
                    commit_sha,
                    issues,
                    verdict_body.as_deref(),
                    // A reviewer-originated request is never human-approved:
                    // it must still be capped. Only the escalation-blocker
                    // resolver inserts a `human_approved = true` request.
                    false,
                )?;
                Ok(request_id)
            })?;
            // The git note is a non-DB audit annotation; do it only after
            // the verdict is durably committed so a crashed/rolled-back
            // verdict leaves no contradictory `failed` note either.
            // Best-effort: the `Failed` verdict + corrective-request bridge
            // row are already DURABLY committed above (the source of truth);
            // a failed note write must NOT abort the run — warn and continue.
            if let Err(e) = git::annotate_review_verdict(workdir, commit_sha, "failed")
                && out.format != OutputFormat::Json
            {
                eprintln!(
                    "  warning: review of {short_sha} (.{iteration}) failed and the \
                     corrective step was requested, but the git note could not be \
                     written: {e:#} (advisory audit only; the DB review_status + \
                     corrective request are the source of truth)"
                );
            }

            if out.format == OutputFormat::Json {
                output::emit_ndjson(&RunEvent::ReviewFinished {
                    step_id: step_id.clone(),
                    step_num,
                    commit_sha: commit_sha.clone(),
                    iteration,
                    passed: false,
                })?;
                output::emit_ndjson(&RunEvent::CorrectiveStepRequested {
                    reviewed_step_id: step_id.clone(),
                    reviewed_step_num: step_num,
                    commit_sha: commit_sha.clone(),
                    iteration,
                    issues,
                })?;
            } else {
                eprintln!(
                    "  review of {short_sha} (.{iteration}) ... FAIL ({issues} issue(s)) — corrective step requested"
                );
            }
            Ok(ReviewOutcome::Failed { request_id, issues })
        }
    }
}

/// Synchronous-DB convenience wrapper (STEP 37): mark the step `InFlight`,
/// emit `ReviewStarted`, run the reviewer subprocess INLINE, then finalize.
///
/// This is the *sequential* entry point — it does not give concurrency
/// (it awaits the subprocess inline). The runner uses the spawnable
/// [`run_review_subprocess`] + [`finalize_review`] pair instead so a review
/// overlaps the next unrelated implementation (§3.5 item 3). This wrapper is
/// kept for the focused unit tests in this module (and any future
/// single-shot caller), composing the exact same steps as the concurrent
/// path so the two cannot drift.
// Used by this module's focused integration tests; the runner deliberately
// uses the spawnable `run_review_subprocess` + `finalize_review` pair to get
// concurrency, so the composed wrapper is dead in the non-test binary build.
#[cfg_attr(not(test), allow(dead_code))]
pub async fn run_review(
    conn: &Connection,
    args: ReviewSubprocessArgs<'_>,
    out: &OutputContext,
) -> Result<ReviewOutcome> {
    let ReviewSubprocessArgs {
        step,
        workdir,
        commit_sha,
        iteration,
        step_num,
        ..
    } = args;
    storage::update_step_review_status(conn, &step.id, ReviewStatus::InFlight)?;
    if out.format == OutputFormat::Json {
        output::emit_ndjson(&RunEvent::ReviewStarted {
            step_id: step.id.clone(),
            step_num,
            commit_sha: commit_sha.to_string(),
            iteration,
        })?;
    }
    let result = run_review_subprocess(args).await?;
    finalize_review(conn, workdir, &result, out)
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
    Escalated {
        chain_len: usize,
        cap: usize,
        interruption_id: String,
    },
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
    // The guarded bridge-row delete AND every follow-up DAG mutation (the §10
    // insert + re-parent + status writes, OR the cap-escalation blocker) run
    // inside ONE transaction. Before this, the delete committed immediately
    // and any later failure — a cycle-guard error mid re-parent, an I/O
    // error, an `insert_interruption` failure — lost the durable request with
    // NO corrective step / blocker ever created: the failed review then
    // silently vanished and its dependents were gated forever with no
    // recovery. With the transaction, such a failure rolls back the delete
    // too (rusqlite ROLLBACK-on-drop), so the still-`open` row survives and
    // the next drain tick retries it. NDJSON / stderr side effects are
    // deferred until AFTER the commit so a rolled-back mutation is never
    // announced.
    let outcome = crate::db::with_tx(conn, |conn| {
        // (1) Single-writer guard: only the tick that flips open→consumed acts.
        if !storage::consume_corrective_step_request(conn, &request.id)? {
            return Ok(CorrectiveConsumeOutcome::AlreadyConsumed);
        }

        let reviewed = storage::get_step(conn, &request.reviewed_step_id)?;

        // (2) Recursion cap (STEP 41 / §10 item 4 / §14.5). The chain length
        // the *next* correction would have is `len(reviewed) + 1`: if
        // `reviewed` is itself a corrective step A′ (chain_len 1), inserting
        // A″ would be chain_len 2, etc. Escalate when that would exceed the
        // cap.
        //
        // WHY the bypass: a human-approved request is the resolution of a
        // prior escalation blocker — the human explicitly granted ONE more
        // review→correction cycle, so we skip the cap entirely for this hop.
        // The resulting corrective step is itself reviewed; if THAT review
        // fails, `finalize_review` inserts a NORMAL (human_approved=false)
        // request, the cap check below fires again, and we re-escalate — so
        // the human stays the loop gate (no unbounded spawning).
        let cap = effective_max_review_corrections(plan);
        let next_chain_len = storage::corrective_chain_len(conn, &reviewed.id)? + 1;
        if !request.human_approved && next_chain_len > cap {
            // Raise exactly ONE blocker interruption and stop spawning.
            let interruption_id = storage::insert_interruption(
                conn,
                &reviewed.id,
                request.reviewed_iteration,
                InterruptionKind::Blocker,
                &format!(
                    "{REVIEW_LOOP_ESCALATION_MARKER}\n\
                     review loop — needs human: step '{}' has been corrected {} time(s) \
                     and still fails review (cap {}). A human must intervene. Resolving \
                     this blocker grants exactly ONE more review→correction cycle; if \
                     that also fails review and re-exceeds the cap, ralph will escalate \
                     again.",
                    reviewed.title,
                    next_chain_len - 1,
                    cap
                ),
                &[],
            )?;
            // C1 fix (docs/dag-redesign.md §10 item 4): on escalation the
            // reviewed step must NOT become `Complete`. A step-level blocker
            // only shadows the step it is keyed to — never its dependents — so
            // if the step went `Complete` here, `deps_satisfied` would
            // immediately unblock every dependent (they'd run on the
            // known-defective output) and `all_done` would finalize the whole
            // plan as `Complete` with the "needs human" blocker silently
            // ignored. Gating an escalated loop is therefore *structural*:
            // leave the step non-terminal. With a non-terminal status + an
            // open blocker it renders derived-`Blocked`, `deps_satisfied`
            // keeps every dependent gated, and the plan reports derived
            // `Interrupted` until a human resolves the loop. Its
            // `review_status` is already `Failed` (set by `finalize_review`);
            // set it again defensively in case a resume sweep cleared it.
            storage::update_step_review_status(conn, &reviewed.id, ReviewStatus::Failed)?;
            return Ok(CorrectiveConsumeOutcome::Escalated {
                chain_len: next_chain_len - 1,
                cap,
                interruption_id,
            });
        }

        // (3) Insert corrective step A′ immediately after A (sort order), then
        // re-parent every former dependent of A onto A′.
        let (corrective, _pos) = insert_corrective_step(conn, plan, &reviewed)?;
        storage::add_step_dependency(conn, &corrective.id, &reviewed.id)?;
        storage::set_step_corrects_step_id(conn, &corrective.id, Some(&reviewed.id))?;

        // Re-parent: snapshot A's former direct dependents *before* adding A′'s
        // own edge (A′ depends_on A is already in place; we must not re-point
        // A′ at itself). Every other former dependent of A now ALSO depends on
        // A′.
        let former_dependents = storage::list_step_dependents(conn, &reviewed.id)?;
        for dep in former_dependents {
            if dep == corrective.id {
                continue; // the A′ -> A edge we just added
            }
            // Defensive parity with `delete_step`'s re-parent loop: never
            // close a cycle to re-point a dependent. A′ is brand-new and
            // depends only on A, and `dep` is a former *dependent* of A, so
            // A′ can never be an ancestor of `dep` — this guard cannot fire
            // on an acyclic DAG. But `add_step_dependency` *bails* on a
            // cycle, which would roll back the whole drain transaction and
            // leave the (still-open) corrective request to retry-loop every
            // tick; skip-rather-than-abort matches `delete_step` and removes
            // that failure mode entirely.
            if storage::would_create_step_cycle(conn, &dep, &corrective.id)? {
                continue;
            }
            storage::add_step_dependency(conn, &dep, &corrective.id)?;
        }

        // (4) A becomes Complete with review_status = Failed (its commit stays
        // in history; dependents are gated by the structural edge to A′, not
        // A's status).
        finalize_reviewed_step_failed(conn, &reviewed.id)?;

        Ok(CorrectiveConsumeOutcome::Inserted {
            corrective_step_id: corrective.id,
            corrective_short_id: corrective.short_id,
        })
    })?;

    // Post-commit side effects (NDJSON event / human log line), emitted only
    // after the transaction durably committed so a rolled-back corrective
    // step / escalation is never announced. These are pure reads + emits and
    // are safe outside the transaction.
    match &outcome {
        CorrectiveConsumeOutcome::AlreadyConsumed => {}
        CorrectiveConsumeOutcome::Escalated {
            chain_len,
            cap,
            interruption_id,
        } => {
            output::emit_interruption_raised(
                conn,
                out.format == OutputFormat::Json,
                interruption_id,
                &request.reviewed_step_id,
                InterruptionKind::Blocker.as_str(),
                false,
                request.reviewed_iteration,
            );
            let reviewed = storage::get_step(conn, &request.reviewed_step_id)?;
            let step_num = step_position(conn, plan, &reviewed.id)?;
            if out.format == OutputFormat::Json {
                output::emit_ndjson(&RunEvent::ReviewLoopEscalated {
                    step_id: reviewed.id.clone(),
                    step_num,
                    chain_len: *chain_len,
                    cap: *cap,
                })?;
            } else {
                eprintln!(
                    "  review loop on '{}' exceeded cap {cap} — raised a blocker (needs human)",
                    reviewed.title
                );
            }
        }
        CorrectiveConsumeOutcome::Inserted {
            corrective_step_id,
            corrective_short_id,
        } => {
            if out.format == OutputFormat::Json {
                output::emit_ndjson(&RunEvent::CorrectiveStepInserted {
                    corrective_step_id: corrective_step_id.clone(),
                    corrective_short_id: corrective_short_id.clone(),
                    corrects_step_id: request.reviewed_step_id.clone(),
                })?;
            } else {
                let reviewed = storage::get_step(conn, &request.reviewed_step_id)?;
                eprintln!(
                    "  inserted corrective step {} (corrects '{}') + re-parented dependents",
                    corrective_short_id, reviewed.title
                );
            }
        }
    }

    Ok(outcome)
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
        crate::storage::NewStep {
            title: &title,
            description: &description,
            agent: reviewed.agent.as_deref(),
            harness: reviewed.harness.as_deref(),
            acceptance_criteria: &criteria,
            max_retries: reviewed.max_retries,
            model: reviewed.model.as_deref(),
            change_policy: Some(crate::plan::ChangePolicy::Required),
            tags: None,
        },
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
    fn test_parse_review_verdict_pass_with_trailing_punctuation() {
        // The PASS line is exactly `REVIEW PASS` per the contract; trailing
        // punctuation/whitespace is tolerated.
        assert_eq!(parse_review_verdict("REVIEW PASS."), ReviewVerdict::Pass);
        assert_eq!(parse_review_verdict("  REVIEW PASS  "), ReviewVerdict::Pass);
    }

    #[test]
    fn test_parse_review_verdict_pass_word_continuation_is_fail_safe() {
        // A word continuation (`REVIEW PASSED WITH 3 CAVEATS`) is NOT a clean
        // PASS verdict — it must fall through to the fail-safe rather than
        // silently passing un-reviewed work.
        assert_eq!(
            parse_review_verdict("REVIEW PASSED WITH 3 CAVEATS"),
            ReviewVerdict::Fail { issues: 1 }
        );
        // ...but a real PASS line below such prose still wins (bottom-up scan).
        let t = "REVIEW PASSED WITH CAVEATS (this is prose)\nREVIEW PASS";
        assert_eq!(parse_review_verdict(t), ReviewVerdict::Pass);
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
            pause_requested: false,
            last_run_branch: None,
            last_run_started_at: None,
            skip_requested_step_id: None,
            skip_changes: None,
            review_enabled: None,
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
            crate::storage::NewPlan {
                slug: "rev-plan",
                project: &dir.to_string_lossy(),
                branch_name: "branch",
                description: "desc",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "Implement widget",
                description: "build the widget",
                agent: None,
                harness: None,
                acceptance_criteria: &["The widget builds".to_string()],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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

    /// Poll `list_worktree_paths(dir).len()` until it converges to
    /// `expected` (or a 5s deadline fires). `ReviewWorktree::Drop` now
    /// cleans up SYNCHRONOUSLY (the worktree is gone before `Drop`
    /// returns), so this normally converges on the first poll. The bounded
    /// poll is retained only as a cheap hedge against filesystem-visibility
    /// lag on slow CI runners; it is no longer load-bearing for
    /// correctness.
    async fn await_worktree_count(dir: &Path, expected: usize) -> Vec<String> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let wts = crate::git::list_worktree_paths(dir).unwrap();
            if wts.len() == expected {
                return wts;
            }
            if std::time::Instant::now() >= deadline {
                return wts;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }

    fn json_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Json,
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

        let outcome = run_review(
            &conn,
            ReviewSubprocessArgs {
                plan: &plan,
                step: &step,
                config: &config,
                workdir: dir,
                commit_sha: &sha,
                iteration: 1,
                step_num: 1,
            },
            &silent_out(),
        )
        .await
        .unwrap();

        assert_eq!(outcome, ReviewOutcome::Passed);
        let s = storage::get_step(&conn, &step.id).unwrap();
        assert_eq!(s.status, StepStatus::Complete);
        assert_eq!(s.review_status, Some(ReviewStatus::Passed));
        assert_eq!(
            crate::git::read_review_verdict(dir, &sha)
                .unwrap()
                .as_deref(),
            Some("passed"),
            "Ralph-Review trailer must be annotated 'passed'"
        );
    }

    /// HARD-INVARIANT PROOF (§9-inv-2) — the **worktree-only tamper class**
    /// (the regression this fix closes). A malicious reviewer that ONLY edits
    /// the working tree (writes/overwrites files, NEVER commits) must not be
    /// able to corrupt the implementation's live workdir. Before the fix the
    /// ancestry guard returned `Ok` for this case (the reviewed commit stayed
    /// an ancestor of HEAD) and the injected junk was swept into the next
    /// per-iteration commit. Now the reviewer is spawned in a THROWAWAY
    /// detached worktree, so its writes land there and CANNOT appear in the
    /// shared `workdir`. We prove the live workdir is byte-for-byte untouched.
    #[tokio::test]
    async fn test_review_worktree_only_tamper_cannot_touch_live_workdir() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        init_repo(dir);
        // Malicious reviewer: edits a tracked file AND drops a brand-new
        // untracked file — but NEVER commits (the exact class the ancestry
        // guard alone could not catch). It runs in its own cwd (the throwaway
        // worktree); these writes must never reach `dir`.
        let script = write_stub(
            dir,
            "evil.sh",
            "echo 'tampered' >> README.md\n\
             echo 'evil' > injected_by_reviewer.txt\n\
             echo 'REVIEW PASS'",
        );
        let config = config_with_review_harness(&script);
        let (conn, plan, step, sha) = seed_committed_step(dir);

        // The review itself "passes" (the harness printed PASS) — isolation,
        // not detection, is what protects the tree here.
        let outcome = run_review(
            &conn,
            ReviewSubprocessArgs {
                plan: &plan,
                step: &step,
                config: &config,
                workdir: dir,
                commit_sha: &sha,
                iteration: 1,
                step_num: 1,
            },
            &silent_out(),
        )
        .await
        .unwrap();
        assert_eq!(outcome, ReviewOutcome::Passed);

        // The injected untracked file NEVER appears in the live workdir.
        assert!(
            !dir.join("injected_by_reviewer.txt").exists(),
            "reviewer-written file leaked into the live implementation workdir \
             (§9-inv-2 worktree isolation violated)"
        );
        // The tracked file the reviewer appended to is unchanged in the
        // live workdir (seed_committed_step never wrote to README.md after
        // init, so it still holds exactly the init content).
        assert_eq!(
            fs::read_to_string(dir.join("README.md")).unwrap(),
            "init\n",
            "reviewer edit to a tracked file leaked into the live workdir"
        );
        // And the live workdir has NO uncommitted changes at all — so the
        // next per-iteration commit (which stages tracked+untracked) cannot
        // sweep in any reviewer-introduced content.
        assert!(
            !crate::git::has_uncommitted_changes(dir).unwrap(),
            "reviewer tampering dirtied the live workdir; the next step's \
             commit would sweep it in (the corruption §9-inv-2 prevents)"
        );
        // No orphan review worktree left behind on a passing review.
        // `Drop` cleans up synchronously; `await_worktree_count` converges
        // immediately (the poll is a CI-lag hedge, not load-bearing).
        let wts = await_worktree_count(dir, 1).await;
        assert_eq!(
            wts.len(),
            1,
            "exactly the main worktree must remain (no orphan review tree): {wts:?}"
        );
    }

    /// HARD-INVARIANT PROOF (§9-inv-2) — the **history-rewrite dimension**
    /// (kept defense-in-depth coverage). A reviewer that reaches back into
    /// the *main* repo and rewrites the reviewed line (`git -C <workdir>
    /// commit --amend`) removes the reviewed commit from HEAD's ancestry; the
    /// `is_ancestor_of_head` guard on the main repo still catches that and
    /// the review errors — un-reviewed/rewritten work is never silently
    /// passed. A *concurrent* unrelated implementation committing on top is
    /// NOT a violation (proven separately by the runner's
    /// `test_run_plan_overlaps_unrelated_impl_with_in_flight_review`).
    #[tokio::test]
    async fn test_review_is_read_only_wrt_reviewed_commit() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        init_repo(dir);
        let (conn, plan, step, sha) = seed_committed_step(dir);
        // Malicious reviewer that explicitly escapes its sandbox and rewrites
        // history in the MAIN repo (`git -C <dir>`), the worst-case "reviewer
        // must never rewrite the reviewed line" violation. The defense-in-
        // depth ancestry guard must reject it.
        let script = write_stub(
            dir,
            "evil.sh",
            &format!(
                "git -C '{d}' commit -q --amend --no-edit --allow-empty -m rewritten\n\
                 echo 'REVIEW PASS'",
                d = dir.display()
            ),
        );
        let config = config_with_review_harness(&script);

        let res = run_review(
            &conn,
            ReviewSubprocessArgs {
                plan: &plan,
                step: &step,
                config: &config,
                workdir: dir,
                commit_sha: &sha,
                iteration: 1,
                step_num: 1,
            },
            &silent_out(),
        )
        .await;

        assert!(
            res.is_err(),
            "a reviewer that rewrote the reviewed commit MUST be rejected \
             (§9-inv-2 read-only review hard invariant)"
        );
        let msg = format!("{:#}", res.unwrap_err());
        assert!(
            msg.contains("read-only review invariant violated"),
            "error must name the violated invariant, got: {msg}"
        );
    }

    /// LIFECYCLE PROOF: the throwaway review worktree is removed (no orphan
    /// `git worktree list` entry, no leftover dir) after BOTH a passing
    /// review AND a failing/erroring one — RAII `Drop` + unconditional prune
    /// on every exit path. `Drop` cleans up synchronously (`block_in_place`
    /// on a multi-thread runtime so it doesn't starve the scheduler), so the
    /// worktree is gone before the guard drop returns — see
    /// `await_worktree_count`.
    #[tokio::test]
    async fn test_review_worktree_cleaned_up_on_pass_and_failure() {
        // (a) passing review.
        {
            let tmp = TempDir::new().unwrap();
            let dir = tmp.path();
            init_repo(dir);
            let script = write_stub(dir, "rev.sh", "echo 'REVIEW PASS'");
            let config = config_with_review_harness(&script);
            let (conn, plan, step, sha) = seed_committed_step(dir);
            run_review(
                &conn,
                ReviewSubprocessArgs {
                    plan: &plan,
                    step: &step,
                    config: &config,
                    workdir: dir,
                    commit_sha: &sha,
                    iteration: 1,
                    step_num: 1,
                },
                &silent_out(),
            )
            .await
            .unwrap();
            let wts = await_worktree_count(dir, 1).await;
            assert_eq!(
                wts.len(),
                1,
                "passing review left an orphan worktree: {wts:?}"
            );
        }
        // (b) failing review (FAIL verdict — still a clean teardown).
        {
            let tmp = TempDir::new().unwrap();
            let dir = tmp.path();
            init_repo(dir);
            let script = write_stub(dir, "fail.sh", "echo 'REVIEW FAIL — 1 issue(s)'");
            let config = config_with_review_harness(&script);
            let (conn, plan, step, sha) = seed_committed_step(dir);
            run_review(
                &conn,
                ReviewSubprocessArgs {
                    plan: &plan,
                    step: &step,
                    config: &config,
                    workdir: dir,
                    commit_sha: &sha,
                    iteration: 1,
                    step_num: 1,
                },
                &silent_out(),
            )
            .await
            .unwrap();
            let wts = await_worktree_count(dir, 1).await;
            assert_eq!(
                wts.len(),
                1,
                "failing review left an orphan worktree: {wts:?}"
            );
        }
        // (c) erroring review (the harness binary cannot be spawned — the
        // worktree is created, then the spawn `?`-errors; Drop must still
        // remove it).
        {
            let tmp = TempDir::new().unwrap();
            let dir = tmp.path();
            init_repo(dir);
            let (conn, plan, step, sha) = seed_committed_step(dir);
            let mut config = config_with_review_harness("/nonexistent/script.sh");
            // Force a hard spawn failure: command that does not exist.
            config.harnesses.get_mut("reviewer").unwrap().command =
                "/nonexistent/ralph-no-such-binary".to_string();
            let res = run_review(
                &conn,
                ReviewSubprocessArgs {
                    plan: &plan,
                    step: &step,
                    config: &config,
                    workdir: dir,
                    commit_sha: &sha,
                    iteration: 1,
                    step_num: 1,
                },
                &silent_out(),
            )
            .await;
            assert!(res.is_err(), "spawn of a missing harness must error");
            let wts = await_worktree_count(dir, 1).await;
            assert_eq!(
                wts.len(),
                1,
                "erroring review left an orphan worktree (Drop did not run): {wts:?}"
            );
        }
    }

    /// FINDING 1: a review harness that hangs past `config.timeout_secs` has
    /// its process group SIGKILL'd and `run_review_subprocess` returns an
    /// `Err` clearly mentioning the timeout — promptly, well under the
    /// stub's sleep (proving the timer fired rather than us waiting it out).
    #[tokio::test]
    async fn test_review_subprocess_times_out_and_errors() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        init_repo(dir);
        // Reviewer sleeps far past the 1s timeout and would print PASS — the
        // timeout must pre-empt it so this never becomes a silent pass.
        let script = write_stub(dir, "slow_rev.sh", "sleep 60\necho 'REVIEW PASS'");
        let mut config = config_with_review_harness(&script);
        config.timeout_secs = Some(1);
        // `run_review_subprocess` takes no `Connection` (it is the spawnable,
        // DB-free half) so the seeded conn is intentionally unused here.
        let (_conn, plan, step, sha) = seed_committed_step(dir);

        let start = std::time::Instant::now();
        let res = run_review_subprocess(ReviewSubprocessArgs {
            plan: &plan,
            step: &step,
            config: &config,
            workdir: dir,
            commit_sha: &sha,
            iteration: 1,
            step_num: 1,
        })
        .await;
        let elapsed = start.elapsed();

        assert!(
            res.is_err(),
            "a review harness that exceeds config.timeout_secs must error"
        );
        let msg = format!("{:#}", res.unwrap_err());
        assert!(
            msg.contains("timed out"),
            "error must mention the timeout, got: {msg}"
        );
        assert!(
            elapsed < std::time::Duration::from_secs(15),
            "timeout should fire promptly (elapsed {elapsed:?}), not wait out \
             the 60s reviewer sleep"
        );
        // No orphan review worktree left behind on the timeout path.
        // `Drop` now detaches cleanup; poll briefly.
        let wts = await_worktree_count(dir, 1).await;
        assert_eq!(
            wts.len(),
            1,
            "timed-out review left an orphan worktree: {wts:?}"
        );
    }

    /// FINDING 3 (best-effort history guard on the TIMEOUT leg): a reviewer
    /// that ESCAPES its worktree to rewrite the reviewed line in the main repo
    /// AND THEN hangs past the timeout must still fail with the *timeout*
    /// error — the history-rewrite guard now runs best-effort on the timeout
    /// leg, but a guard failure there must NOT mask the authoritative timeout
    /// outcome (it is only logged as a warning).
    ///
    /// This deterministically forces the guard to FAIL on the timeout path:
    /// the reviewer amends HEAD in the main repo (orphaning the reviewed
    /// commit so it is no longer an ancestor of HEAD — exactly what
    /// `assert_tree_unchanged_by_review` rejects), and the amend happens
    /// BEFORE the long sleep, so the 1s timer fires while the reviewer is
    /// still hung. We assert the returned error names the timeout, NOT the
    /// read-only invariant — proving the guard call was made (the warning
    /// path) yet did not override the timeout Err.
    #[tokio::test]
    async fn test_review_timeout_guard_failure_does_not_mask_timeout() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        init_repo(dir);
        let (_conn, plan, step, sha) = seed_committed_step(dir);
        // Sanity: the reviewed commit IS an ancestor of HEAD before review,
        // so `ReviewTreeGuard::capture` records `reviewed_reachable_before =
        // true` and the timeout-leg guard is armed (not a no-op).
        assert!(
            crate::git::is_ancestor_of_head(dir, &sha).unwrap(),
            "reviewed commit must be an ancestor of HEAD before the reviewer runs"
        );

        // Malicious reviewer: rewrite the reviewed line in the MAIN repo
        // (orphaning `sha` from HEAD's ancestry), THEN hang past the timeout.
        // The amend precedes the sleep so the guard is guaranteed to see the
        // tampered state when the 1s timer fires.
        let script = write_stub(
            dir,
            "evil_hang.sh",
            &format!(
                "git -C '{d}' commit -q --amend --no-edit --allow-empty -m rewritten\n\
                 sleep 60\n\
                 echo 'REVIEW PASS'",
                d = dir.display()
            ),
        );
        let mut config = config_with_review_harness(&script);
        config.timeout_secs = Some(1);

        let start = std::time::Instant::now();
        let res = run_review_subprocess(ReviewSubprocessArgs {
            plan: &plan,
            step: &step,
            config: &config,
            workdir: dir,
            commit_sha: &sha,
            iteration: 1,
            step_num: 1,
        })
        .await;
        let elapsed = start.elapsed();

        assert!(res.is_err(), "a hung reviewer must still error");
        let msg = format!("{:#}", res.unwrap_err());
        // The authoritative error is the TIMEOUT, not the guard violation.
        assert!(
            msg.contains("timed out"),
            "timeout must be the authoritative error even when the history \
             guard also failed, got: {msg}"
        );
        assert!(
            !msg.contains("read-only review invariant violated"),
            "the best-effort guard failure must NOT mask/replace the timeout \
             error (it is only logged as a warning), got: {msg}"
        );
        // Confirm the guard would in fact have FAILED here (the amend really
        // did orphan the reviewed commit), so this test exercises the
        // guard-FAILS-on-timeout path rather than a vacuous guard-passes one.
        assert!(
            !crate::git::is_ancestor_of_head(dir, &sha).unwrap(),
            "the reviewer's amend must have orphaned the reviewed commit so the \
             timeout-leg guard genuinely fails (otherwise this test wouldn't \
             exercise the no-mask path)"
        );
        // Timer fired promptly rather than waiting out the 60s sleep.
        assert!(
            elapsed < std::time::Duration::from_secs(15),
            "timeout should fire promptly (elapsed {elapsed:?})"
        );
        // No orphan review worktree left behind on the timeout+guard-fail path.
        let wts = await_worktree_count(dir, 1).await;
        assert_eq!(
            wts.len(),
            1,
            "timed-out review (with a failed history guard) left an orphan \
             worktree: {wts:?}"
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

        let outcome = run_review(
            &conn,
            ReviewSubprocessArgs {
                plan: &plan,
                step: &step,
                config: &config,
                workdir: dir,
                commit_sha: &sha,
                iteration: 1,
                step_num: 1,
            },
            &silent_out(),
        )
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
            crate::storage::NewPlan {
                slug: "dag-plan",
                project: &dir.to_string_lossy(),
                branch_name: "b",
                description: "d",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        // A -> B (B depends_on A), and a sibling C depends_on A too.
        let (a, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "A",
                description: "d",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();
        let (b, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "B",
                description: "d",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();
        let (c, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "C",
                description: "d",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
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
            false,
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
            crate::storage::NewPlan {
                slug: "cap-plan",
                project: &dir.to_string_lossy(),
                branch_name: "b",
                description: "d",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        // Tight cap: at most 1 correction in the chain.
        storage::set_plan_max_review_corrections(&conn, &plan.id, Some(1)).unwrap();
        let plan = storage::get_plan_by_id(&conn, &plan.id).unwrap();

        let (a, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "A",
                description: "d",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        // 1st failed review of A ⇒ A′ inserted (chain_len 1 ≤ cap 1).
        let req = {
            storage::insert_corrective_step_request(&conn, &a.id, 1, "sha1", 1, None, false)
                .unwrap();
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
            storage::insert_corrective_step_request(&conn, &a_prime_id, 1, "sha2", 1, None, false)
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

        // C1 regression guard: an escalated step must NOT be `Complete`.
        // Marking it Complete would let `deps_satisfied` unblock its
        // dependents onto the known-defective output and let the plan
        // finalize Complete with the blocker ignored. It stays non-terminal
        // (gating is structural) with `review_status = Failed`.
        let a_prime = storage::get_step(&conn, &a_prime_id).unwrap();
        assert_ne!(
            a_prime.status,
            StepStatus::Complete,
            "escalated step must stay non-terminal so dependents stay gated"
        );
        assert_eq!(
            a_prime.review_status,
            Some(ReviewStatus::Failed),
            "escalated step keeps the Failed review verdict"
        );
    }

    /// §10 item 4 / §14.5: a `human_approved = true` request (the resolution
    /// of a review-loop escalation blocker) BYPASSES the recursion cap for one
    /// hop — it inserts the corrective step + re-parents + finalizes the
    /// reviewed step Complete EVEN when `corrective_chain_len + 1 > cap`.
    #[test]
    fn test_human_approved_request_bypasses_recursion_cap() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        init_repo(dir);
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "cap-bypass",
                project: &dir.to_string_lossy(),
                branch_name: "b",
                description: "d",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        // Cap 0: a normal request would escalate immediately.
        storage::set_plan_max_review_corrections(&conn, &plan.id, Some(0)).unwrap();
        let plan = storage::get_plan_by_id(&conn, &plan.id).unwrap();

        let (a, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "A",
                description: "d",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();
        let (b, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "B",
                description: "d",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(1),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();
        storage::add_step_dependency(&conn, &b.id, &a.id).unwrap();

        // Sanity: a NORMAL request at cap 0 escalates (no bypass).
        storage::insert_corrective_step_request(&conn, &a.id, 1, "sha1", 1, None, false).unwrap();
        let normal = storage::list_open_corrective_step_requests_for_plan(&conn, &plan.id).unwrap()
            [0]
        .clone();
        assert!(!normal.human_approved);
        assert!(matches!(
            consume_corrective_request(&conn, &plan, &normal, &silent_out()).unwrap(),
            CorrectiveConsumeOutcome::Escalated { cap: 0, .. }
        ));

        // A human-approved request for the SAME step (still chain_len+1 > cap)
        // must instead insert + re-parent + finalize-Complete.
        storage::insert_corrective_step_request(&conn, &a.id, 1, "", 0, None, true).unwrap();
        let approved = storage::list_open_corrective_step_requests_for_plan(&conn, &plan.id)
            .unwrap()
            .into_iter()
            .find(|r| r.human_approved)
            .expect("human-approved request present");
        let a_prime_id = match consume_corrective_request(&conn, &plan, &approved, &silent_out())
            .unwrap()
        {
            CorrectiveConsumeOutcome::Inserted {
                corrective_step_id, ..
            } => corrective_step_id,
            other => panic!("human-approved request must bypass the cap and Insert, got {other:?}"),
        };

        // A becomes Complete (review_status Failed); A′ exists and corrects A.
        let a_after = storage::get_step(&conn, &a.id).unwrap();
        assert_eq!(a_after.status, StepStatus::Complete);
        assert_eq!(a_after.review_status, Some(ReviewStatus::Failed));
        let a_prime = storage::get_step(&conn, &a_prime_id).unwrap();
        assert_eq!(a_prime.corrects_step_id.as_deref(), Some(a.id.as_str()));

        // Re-parent: B now depends on A′ as well.
        let b_deps = storage::list_step_dependencies(&conn, &b.id).unwrap();
        assert!(
            b_deps.contains(&a_prime_id),
            "B must be re-parented onto A′ after a human-approved correction"
        );

        // Chaining: if A′ then fails review, a NORMAL request re-escalates
        // (cap fires again — the human stays the loop gate).
        storage::insert_corrective_step_request(&conn, &a_prime_id, 1, "sha2", 1, None, false)
            .unwrap();
        let again = storage::list_open_corrective_step_requests_for_plan(&conn, &plan.id)
            .unwrap()
            .into_iter()
            .find(|r| r.reviewed_step_id == a_prime_id)
            .expect("normal request on A′ present");
        assert!(matches!(
            consume_corrective_request(&conn, &plan, &again, &silent_out()).unwrap(),
            CorrectiveConsumeOutcome::Escalated { .. }
        ));
    }

    #[test]
    fn test_recursion_cap_escalation_json_mode_still_commits_blocker() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path();
        init_repo(dir);
        let conn = crate::db::open_memory().unwrap();
        let plan = storage::create_plan(
            &conn,
            crate::storage::NewPlan {
                slug: "cap-json",
                project: &dir.to_string_lossy(),
                branch_name: "b",
                description: "d",
                harness: None,
                agent: None,
                deterministic_tests: &[],
            },
        )
        .unwrap();
        storage::set_plan_max_review_corrections(&conn, &plan.id, Some(0)).unwrap();
        let plan = storage::get_plan_by_id(&conn, &plan.id).unwrap();
        let (step, _) = storage::create_step(
            &conn,
            &plan.id,
            crate::storage::NewStep {
                title: "A",
                description: "d",
                agent: None,
                harness: None,
                acceptance_criteria: &[],
                max_retries: Some(0),
                model: None,
                change_policy: None,
                tags: None,
            },
        )
        .unwrap();

        storage::insert_corrective_step_request(&conn, &step.id, 1, "sha1", 1, None, false)
            .unwrap();
        let req = storage::list_open_corrective_step_requests_for_plan(&conn, &plan.id).unwrap()[0]
            .clone();
        let res = consume_corrective_request(&conn, &plan, &req, &json_out()).unwrap();
        assert!(matches!(
            res,
            CorrectiveConsumeOutcome::Escalated { cap: 0, .. }
        ));

        let open = storage::list_open_interruptions_for_plan(&conn, &plan.id).unwrap();
        let blockers: Vec<_> = open
            .iter()
            .filter(|i| i.kind == crate::plan::InterruptionKind::Blocker)
            .collect();
        assert_eq!(
            blockers.len(),
            1,
            "JSON-mode emission must not drop blocker writes"
        );
        assert_eq!(blockers[0].step_id, step.id);
    }
}
