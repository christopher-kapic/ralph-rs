// `ralph interruption list/show/resolve` — the human-side CLI for the
// unified interruption model (docs/dag-redesign.md §7). The TUI inbox is the
// primary path; this is the scriptable equivalent. All I/O is native against
// the `interruptions` table (no `step_questions`).

use std::path::Path;

use anyhow::{Result, bail};
use rusqlite::Connection;

use crate::db;
use crate::executor::{RETRY_EXHAUSTED_OPTION_FAIL, RETRY_EXHAUSTED_OPTION_RETRY};
use crate::output::{OutputContext, OutputFormat};
use crate::plan::{InterruptionKind, StepStatus};
#[cfg(test)]
use crate::runner::PARKED_RESTORE_OPTION_MARK_PENDING;
use crate::runner::{PARKED_RESTORE_BLOCKER_MARKER, PARKED_RESTORE_OPTION_MARK_FAILED};
use crate::storage::{self, OpenQuestion};

/// Phase C: react to the human-side resolution of the **Phase B auto-raised
/// retry-exhausted blocker** (the only interruption that carries the two
/// ranked options [`RETRY_EXHAUSTED_OPTION_RETRY`] /
/// [`RETRY_EXHAUSTED_OPTION_FAIL`]; harness-raised blockers have empty
/// options and never match).
///
/// **Phase E — atomic with the resolve write.** This is now a thin wrapper
/// around [`resolve_interruption_with_retry_handling`] that performs both
/// the `interruptions.state = 'resolved'` flip and the retry-exhausted
/// side-effect inside a single `unchecked_transaction`. The old two-write
/// shape (resolve then apply) let a concurrent `ralph run` process observe
/// the half-state `(interruption resolved + status Pending + attempts == max)`
/// between the writes — `pick_next_step` would return the step,
/// `executor::execute_step`'s budget guard would bail with "already used all
/// N attempts", and the run would terminate. This entry point is preserved
/// for the test suite (which exercises the retry-exhausted-only side effect
/// against a pre-resolved row); production callers must use
/// [`resolve_interruption_with_retry_handling`] directly.
///
/// `Ok(true)` — the interruption was the auto-blocker and the side-effect
/// has been applied (`Retry` → `attempts = 0` + status `Pending` so the
/// scheduler re-queues; `Fail` → status `Failed`, terminal). `Ok(false)` —
/// the interruption was a normal question or harness-raised blocker; no
/// step-level side effect is required.
///
/// Resolution-text matching uses the executor's `pub const` strings (so
/// drift between writer and reader trips `cargo test --lib`, not production).
/// A freeform resolution that matches neither option is treated as
/// **Retry-with-parked-changes + hint**: the freeform string (and any
/// comment) flow into the next step prompt via the bounded
/// `list_resolved_interruptions_for_step` injection (§8). This preserves the
/// "Enter on the default-selected option" UX while honoring the spec
/// guidance that the safest interpretation of an ambiguous freeform answer
/// to a retry-exhausted blocker is "try again with this as a hint."
#[cfg_attr(not(test), allow(dead_code))]
pub fn apply_retry_exhausted_resolution(
    conn: &Connection,
    project: &str,
    interruption_id: &str,
    resolution_text: &str,
) -> Result<bool> {
    let interruption = storage::get_interruption(conn, interruption_id)?;
    if !is_retry_exhausted_auto_blocker(&interruption) {
        return Ok(false);
    }

    let step_id = interruption.step_id.clone();
    let want_fail = resolution_matches_option(resolution_text, RETRY_EXHAUSTED_OPTION_FAIL);

    let parked_to_discard = db::with_tx(conn, |tx| {
        apply_retry_exhausted_side_effect_in_tx(tx, &step_id, want_fail)
    })?;

    crate::runner::discard_parked_worktree_state(Path::new(project), parked_to_discard)?;
    Ok(true)
}

/// True iff `interruption` is the Phase B executor-raised
/// retry-exhausted auto-blocker (the only blocker that carries the two
/// ranked options [`RETRY_EXHAUSTED_OPTION_RETRY`] /
/// [`RETRY_EXHAUSTED_OPTION_FAIL`]).
///
/// Harness-raised blockers have empty `options`; a hypothetical future
/// blocker with a different option set fails the membership checks. The
/// kind discriminator is the first gate (a Question with these option texts
/// must still be a Question).
fn is_retry_exhausted_auto_blocker(interruption: &crate::plan::Interruption) -> bool {
    interruption.kind == InterruptionKind::Blocker
        && interruption.options.len() == 2
        && interruption
            .options
            .iter()
            .any(|o| o.text == RETRY_EXHAUSTED_OPTION_RETRY)
        && interruption
            .options
            .iter()
            .any(|o| o.text == RETRY_EXHAUSTED_OPTION_FAIL)
}

/// Match a resolution against a special option's exact stored text.
///
/// `--option K` selection maps to the option's exact `.text` (see
/// `cmd_interruption_resolve`), so an exact (case-sensitive) compare still
/// recognizes a deliberate option pick. The match is intentionally
/// case-SENSITIVE: a freeform `--answer "mark step failed"` must NOT be
/// coerced into the terminal option branch — a freeform answer that matches
/// neither option text is the documented retry-with-hint path (it flows into
/// the next prompt via the bounded "Resolved interruptions" section). `trim()`
/// is kept so leading/trailing whitespace on an exact pick is forgiven.
fn resolution_matches_option(resolution_text: &str, option_text: &str) -> bool {
    resolution_text.trim() == option_text
}

/// The retry-exhausted side-effect, in-transaction. Returns the parked
/// worktree state the caller must discard *after commit* (on the Fail arm
/// only) — file-system mutations can't safely be rolled back if the
/// transaction aborts, so the caller is responsible for the post-commit
/// `discard_parked_worktree_state` invocation.
fn apply_retry_exhausted_side_effect_in_tx(
    tx: &Connection,
    step_id: &str,
    want_fail: bool,
) -> Result<Option<crate::storage::ParkedWorktreeState>> {
    if want_fail {
        // Explicit give-up: terminal Failed. The interruption row is
        // resolved by the same transaction; the freeform comment (if any)
        // is on the resolved row but never feeds another prompt — the step
        // is done. Drop the automatic re-apply pointer and discard the
        // underlying stash so a later unrelated run cannot resurrect this WIP.
        storage::update_step_status(tx, step_id, StepStatus::Failed)?;
        let parked = storage::get_step_parked_worktree(tx, step_id)?;
        if parked.is_some() {
            storage::clear_step_parked_worktree(tx, step_id)?;
        }
        Ok(parked)
    } else {
        // "Retry step with parked changes" — the explicit option text —
        // and the freeform-doesn't-match fallthrough both land here. The
        // freeform text (and optional comment) are stamped on the resolved
        // interruption by the same transaction; the bounded
        // resolved-interruptions section of the next prompt will pick it
        // up automatically. The next run restores the parked stash as
        // unstaged changes so the step can continue from its prior WIP.
        storage::set_step_attempts(tx, step_id, 0)?;
        storage::update_step_status(tx, step_id, StepStatus::Pending)?;
        Ok(None)
    }
}

/// True iff `interruption` is the parked-worktree-restore blocker
/// (`runner::raise_parked_restore_blocker`). Detected by the
/// [`PARKED_RESTORE_BLOCKER_MARKER`] prefix on the body — chosen over an
/// option-content check because the parked-restore option set is not
/// `pub const` in the same defended way the retry-exhausted set is, and a
/// body marker is the minimum-invasive shape matching the existing pattern.
///
/// The kind discriminator is the first gate (a Question that happens to
/// carry the marker text by accident is still a Question — the marker is
/// only meaningful on a Blocker).
fn is_parked_restore_blocker(interruption: &crate::plan::Interruption) -> bool {
    interruption.kind == InterruptionKind::Blocker
        && interruption.body.starts_with(PARKED_RESTORE_BLOCKER_MARKER)
}

/// True iff `interruption` is the review-loop escalation blocker
/// (`review::consume_corrective_request`'s cap-exceeded path). Detected by the
/// [`crate::review::REVIEW_LOOP_ESCALATION_MARKER`] body prefix, mirroring
/// [`is_parked_restore_blocker`] (the blocker carries empty options, so a
/// body marker — not an option-content check — is the recognition contract).
fn is_review_loop_escalation_blocker(interruption: &crate::plan::Interruption) -> bool {
    interruption.kind == InterruptionKind::Blocker
        && crate::review::is_review_loop_escalation_blocker(&interruption.body)
}

/// The parked-restore side-effect, in-transaction. Mirrors
/// [`apply_retry_exhausted_side_effect_in_tx`]: reads any surviving parked
/// bridge row, clears it IN the transaction, and **returns the
/// `ParkedWorktreeState`** so the caller drops the underlying stash
/// post-commit via [`crate::runner::discard_parked_worktree_state`]
/// (file-system mutations can't be rolled back if the tx aborts, so they
/// must happen on the success leg only).
///
/// Why this clears the row + drops the stash on BOTH arms (Fix #1(b)): the
/// runner's Conflicted branch keeps the bridge row + stash and resets the
/// working tree clean at raise time. If resolution left that row behind,
/// the next scheduler tick would re-enter `restore_parked_step_worktree`,
/// re-attempt the (still-conflicting) pop, and re-raise the blocker —
/// the loop bug. Clearing the row here breaks the loop; dropping the stash
/// keeps the stack tidy. On the NotFound path the bridge row was already
/// dropped at raise time, so `get_step_parked_worktree` returns `None`,
/// `clear` is skipped, and `discard_parked_worktree_state(None)` is a
/// harmless no-op (the stash was already gone).
///
/// Dispatch rules:
/// - [`PARKED_RESTORE_OPTION_MARK_FAILED`] (or a freeform answer matching
///   that exact text): flip the step to terminal `Failed`.
/// - [`PARKED_RESTORE_OPTION_MARK_PENDING`]: leave the step `Pending`
///   with attempts unchanged so the scheduler re-picks it fresh on the
///   next tick (the tree was already reset clean at raise time).
/// - Any other freeform answer: treat as "Mark Pending" (start fresh, with
///   the freeform string flowing into the next prompt via the bounded
///   "Resolved interruptions" section — same convention as the
///   retry-exhausted blocker's freeform fallthrough).
fn apply_parked_restore_side_effect_in_tx(
    tx: &Connection,
    step_id: &str,
    resolution_text: &str,
) -> Result<Option<crate::storage::ParkedWorktreeState>> {
    // Clear any surviving bridge row (Conflicted) so the scheduler can't
    // re-enter the restore path and re-raise the blocker, and capture the
    // parked state so the caller drops the stash after commit.
    let parked = storage::get_step_parked_worktree(tx, step_id)?;
    if parked.is_some() {
        storage::clear_step_parked_worktree(tx, step_id)?;
    }

    if resolution_matches_option(resolution_text, PARKED_RESTORE_OPTION_MARK_FAILED) {
        // Explicit give-up: terminal Failed. The step's `attempts` value
        // is left alone — the row records whatever attempt count the
        // executor had reached before parking.
        storage::update_step_status(tx, step_id, StepStatus::Failed)?;
    }
    // MARK_PENDING and any freeform fallthrough: step stays Pending, the
    // scheduler re-picks on the next tick on a clean tree. The interruption
    // row's `resolution` (the freeform text) plus `comment` get injected
    // into the next prompt by `list_resolved_interruptions_for_step`.
    Ok(parked)
}

/// The review-loop-escalation side-effect, in-transaction (§10 item 4 /
/// §14.5). Resolving the "review loop — needs human" blocker grants exactly
/// ONE more review→correction cycle: we insert a `human_approved = true`
/// corrective request for the escalated step so the orchestrator's drain
/// (`review::consume_corrective_request`) inserts the corrective step +
/// re-parents + finalizes the escalated step `Complete`, bypassing the
/// recursion cap for this single hop. If that corrective step also fails
/// review, `finalize_review` inserts a NORMAL (human_approved = false)
/// request → the cap fires again → re-escalates, so the human stays the gate.
///
/// `commit_sha` / `issues` / `verdict_body` are audit-only (never read by
/// `insert_corrective_step`, which builds the corrective step purely from the
/// reviewed step's criteria), so a best-effort placeholder suffices — we do
/// NOT add git plumbing just to fill an unused audit field.
fn apply_review_loop_escalation_side_effect_in_tx(
    tx: &Connection,
    step_id: &str,
    iteration: i32,
) -> Result<()> {
    storage::insert_corrective_step_request(
        tx,
        step_id,
        iteration,
        "", // commit_sha: audit-only, unused on insert
        0,  // issues: audit-only
        Some("human approved one more correction cycle after review-loop escalation"),
        true, // human_approved → consume_corrective_request bypasses the cap for this hop
    )?;
    Ok(())
}

/// Atomic resolve: flip `interruptions.state='resolved'` AND, when the row
/// is the Phase B retry-exhausted auto-blocker, apply the Retry/Fail
/// side-effect — **all inside a single `unchecked_transaction`**.
///
/// The pre-Phase-E shape called `storage::resolve_interruption` and then
/// `apply_retry_exhausted_resolution` in two separate transactions. A peer
/// `ralph run` process polling `pick_next_step` between the writes could
/// observe the half-state `(interruption resolved, step Pending,
/// attempts == max)`, return the step, and bail in
/// `executor::execute_step`'s budget guard with "already used all N
/// attempts" — terminating the whole run. Collapsing both writes into one
/// transaction makes the half-state unobservable to any other reader.
///
/// `resolution_text` is the chosen option text (matched against the
/// executor's `pub const` strings) or a freeform answer; `comment` is the
/// optional human note. Both the file-system discard (Fail arm) and the
/// NDJSON `InterruptionResolved` emission happen *after* commit, mirroring
/// the executor's `raise_retry_exhausted_blocker` ordering — the durable
/// state is committed first, the advisory side-effects fire only on the
/// success leg.
///
/// Returns `Ok(true)` when the row was the retry-exhausted auto-blocker
/// (the Retry/Fail side-effect was applied), `Ok(false)` otherwise.
pub fn resolve_interruption_with_retry_handling(
    conn: &Connection,
    project: &str,
    interruption_id: &str,
    resolution_text: &str,
    comment: Option<&str>,
) -> Result<bool> {
    // Read the interruption shape up front (outside the tx is fine — the
    // kind/options are immutable for a given id; we only need the read to
    // decide which side-effect branch to take inside the tx).
    let interruption = storage::get_interruption(conn, interruption_id)?;
    let is_auto = is_retry_exhausted_auto_blocker(&interruption);
    // Bug #2 fix: dispatch parked-restore blockers (body-prefix-marked) to
    // the matching side-effect. Pre-fix these were inserted with empty
    // options and the resolver had no branch — resolution just closed the
    // row, the step stayed Pending with the same Conflicted/NotFound
    // condition, and the scheduler re-fired the same restore error on the
    // next tick. The two dispatch branches are mutually exclusive (a
    // single interruption is either retry-exhausted OR parked-restore, never
    // both — they're raised by different code paths with different option
    // sets / body markers).
    let is_parked_restore = !is_auto && is_parked_restore_blocker(&interruption);
    // Review-loop escalation blocker (§10 item 4 / §14.5): resolving it grants
    // exactly ONE more review→correction cycle. Mutually exclusive with the
    // other two (raised by a different path with a distinct body marker).
    let is_review_loop_escalation =
        !is_auto && !is_parked_restore && is_review_loop_escalation_blocker(&interruption);
    let step_id = interruption.step_id.clone();
    let escalation_iteration = interruption.attempt;
    let want_fail =
        is_auto && resolution_matches_option(resolution_text, RETRY_EXHAUSTED_OPTION_FAIL);

    let parked_to_discard = db::with_tx(conn, |tx| {
        storage::resolve_interruption(tx, interruption_id, resolution_text, comment)?;
        if is_auto {
            apply_retry_exhausted_side_effect_in_tx(tx, &step_id, want_fail)
        } else if is_parked_restore {
            // Fix #1(b): the parked-restore side-effect now clears the
            // surviving bridge row (breaking the Conflicted re-raise loop)
            // and returns the parked state so the stash is dropped
            // post-commit, mirroring the retry-exhausted Fail arm.
            apply_parked_restore_side_effect_in_tx(tx, &step_id, resolution_text)
        } else if is_review_loop_escalation {
            // Insert a `human_approved = true` corrective request in the SAME
            // transaction as the resolve. This is a *request* write (not a DAG
            // write), so the CLI/TUI process may do it; the orchestrator
            // drains it as the sole writer on the next scheduler tick (same
            // run) or on `ralph resume`, bypassing the cap for this one hop.
            // commit_sha/issues/verdict_body are audit-only (unused by
            // `insert_corrective_step`), so a best-effort placeholder is fine.
            apply_review_loop_escalation_side_effect_in_tx(tx, &step_id, escalation_iteration)?;
            Ok(None)
        } else {
            Ok(None)
        }
    })?;

    if parked_to_discard.is_some() {
        crate::runner::discard_parked_worktree_state(Path::new(project), parked_to_discard)?;
    }
    // The return-bool's pre-Phase-E meaning was "was this the retry-exhausted
    // auto-blocker?" — only one caller (a Phase C compat wrapper) inspects
    // it. We keep that contract here; parked-restore returns `false`
    // (callers that care about parked-restore detection should call
    // `is_parked_restore_blocker` directly).
    Ok(is_auto)
}

/// Resolve an interruption *selector* (a uuid OR a 1-based index in `ralph
/// interruption list`) against the project's open-interruption list.
///
/// Index resolution recomputes the same stable-ordered list `interruption
/// list` prints, so a number the user just saw maps to the right row. A
/// value that parses as a `usize` is treated as an index **first** (matching
/// the numbered-list UX); anything else is treated as an id. An id that does
/// not match any *open* interruption falls through to the caller's
/// resolve/show, which produces a precise native error.
/// A short, human-readable preview of an interruption body for the resolve
/// echo: strip any internal body marker
/// ([`PARKED_RESTORE_BLOCKER_MARKER`] /
/// [`crate::review::REVIEW_LOOP_ESCALATION_MARKER`], each a prefix on its own
/// line) so the operator sees the readable text rather than the machine
/// marker, take the first non-empty line, then char-truncate.
fn body_preview(body: &str) -> String {
    let mut text = body.trim_start();
    for marker in [
        PARKED_RESTORE_BLOCKER_MARKER,
        crate::review::REVIEW_LOOP_ESCALATION_MARKER,
    ] {
        if let Some(rest) = text.strip_prefix(marker) {
            text = rest.trim_start();
            break;
        }
    }
    let first = text.lines().find(|l| !l.trim().is_empty()).unwrap_or("");
    let first = first.trim();
    const MAX: usize = 80;
    if first.chars().count() > MAX {
        let truncated: String = first.chars().take(MAX).collect();
        format!("{truncated}…")
    } else {
        first.to_string()
    }
}

fn resolve_selector<'a>(opens: &'a [OpenQuestion], selector: &str) -> Option<&'a OpenQuestion> {
    if let Ok(idx) = selector.parse::<usize>() {
        if idx >= 1 && idx <= opens.len() {
            return Some(&opens[idx - 1]);
        }
        return None;
    }
    opens.iter().find(|o| o.id == selector)
}

/// `ralph interruption list [<plan>]` — every open interruption (questions
/// and blockers) for the project, numbered 1..N.
pub fn cmd_interruption_list(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    out: &OutputContext,
) -> Result<()> {
    let items = storage::list_open_interruptions_enriched(conn, project, plan_slug)?;

    if out.format == OutputFormat::Json {
        let arr: Vec<serde_json::Value> = items
            .iter()
            .enumerate()
            .map(|(i, q)| {
                serde_json::json!({
                    "index": i + 1,
                    "id": q.id,
                    "kind": q.kind.as_str(),
                    "plan_slug": q.plan_slug,
                    "step_num": q.step_num,
                    "step_title": q.step_title,
                    "attempt": q.attempt,
                    "body": q.question,
                    "options": q.suggestions,
                    "asked_at": q.asked_at,
                })
            })
            .collect();
        println!("{}", serde_json::to_string(&arr)?);
        return Ok(());
    }

    if items.is_empty() {
        if let Some(slug) = plan_slug {
            println!("No open interruptions on plan '{slug}'.");
        } else {
            println!("No open interruptions.");
        }
        return Ok(());
    }

    for (i, q) in items.iter().enumerate() {
        let kind = q.kind.as_str();
        let context = format!(
            "{} step {} ({})",
            q.plan_slug,
            q.step_num,
            q.step_title.trim()
        );
        let single_line = q.question.trim().lines().next().unwrap_or("").to_string();
        println!("[{}] {kind}  {context}", i + 1);
        println!("    {single_line}");
    }
    Ok(())
}

/// `ralph interruption show [PLAN] <id|index>` — one interruption's full body,
/// kind, proposed options, and state.
///
/// When PLAN is supplied, the selector is resolved (and any index is
/// interpreted) against only the open interruptions for that plan.
pub fn cmd_interruption_show(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    selector: &str,
    out: &OutputContext,
) -> Result<()> {
    let opens = storage::list_open_interruptions_enriched(conn, project, plan_slug)?;
    let q = match resolve_selector(&opens, selector) {
        Some(q) => q,
        None => bail!(
            "No open interruption matched '{selector}' (use an id or a 1-based \
             index from `ralph interruption list`)."
        ),
    };

    if out.format == OutputFormat::Json {
        let json = serde_json::json!({
            "id": q.id,
            "kind": q.kind.as_str(),
            "plan_slug": q.plan_slug,
            "step_num": q.step_num,
            "step_title": q.step_title,
            "attempt": q.attempt,
            "body": q.question,
            "options": q.suggestions,
            "asked_at": q.asked_at,
        });
        println!("{}", serde_json::to_string(&json)?);
        return Ok(());
    }

    let kind = q.kind.as_str();
    println!(
        "{kind}  {} step {} ({})",
        q.plan_slug,
        q.step_num,
        q.step_title.trim()
    );
    println!("Id: {}", q.id);
    println!("Asked: {} (attempt {})", q.asked_at, q.attempt);
    println!();
    println!("{}", q.question);
    if !q.suggestions.is_empty() {
        println!();
        println!("Proposed options (priority order, 1 = agent's best):");
        for (i, s) in q.suggestions.iter().enumerate() {
            println!("  {}. {s}", i + 1);
        }
    }
    Ok(())
}

/// `ralph interruption resolve [PLAN] <id|index> [--option K] [--answer T]
/// [--comment T]`.
///
/// `--option K` picks the K-th proposed option (1-based, priority order, as
/// `interruption show` prints). `--answer` is a freeform resolution. Exactly
/// one of the two is required for a question; a blocker (no options) needs
/// `--answer` or resolves with an empty resolution + comment. `--comment` is
/// an always-injectable note. Resolving flips state→resolved and un-shadows
/// the step (its `Blocked` overlay clears — docs/dag-redesign.md §3.4).
///
/// When PLAN is supplied, the selector is resolved (and any index is
/// interpreted) against only the open interruptions for that plan.
#[allow(clippy::too_many_arguments)]
pub fn cmd_interruption_resolve(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    selector: &str,
    option: Option<usize>,
    answer: Option<&str>,
    comment: Option<&str>,
    out: &OutputContext,
) -> Result<()> {
    let opens = storage::list_open_interruptions_enriched(conn, project, plan_slug)?;
    let q = match resolve_selector(&opens, selector) {
        Some(q) => q.clone(),
        None => bail!(
            "No open interruption matched '{selector}' (use an id or a 1-based \
             index from `ralph interruption list`)."
        ),
    };

    // Determine the resolution text. `--option K` selects the K-th proposed
    // option by priority order; `--answer` is freeform. clap already makes
    // them mutually exclusive (`conflicts_with`).
    let resolution: String = match (option, answer) {
        (Some(k), _) => {
            if k == 0 || k > q.suggestions.len() {
                bail!(
                    "--option {k} is out of range (interruption has {} proposed \
                     option{}).",
                    q.suggestions.len(),
                    if q.suggestions.len() == 1 { "" } else { "s" }
                );
            }
            q.suggestions[k - 1].clone()
        }
        (None, Some(a)) => a.to_string(),
        (None, None) => {
            // No explicit resolution.
            //
            // **Blocker:** a blocker carries a binding semantic — at minimum
            // "did the operator pick Retry or Mark Failed?" for the Phase B
            // auto-blocker. The pre-Phase-E behavior silently fell through to
            // an empty resolution which routed through the Retry path (the
            // Retry arm is the default when `resolution_text` is neither
            // option text), so a `--comment "thoughts"`-only resolve on a
            // retry-exhausted blocker would burn another retry budget
            // without surfacing the choice. Reject explicitly and name the
            // options so the operator can re-issue the right command.
            if q.kind == InterruptionKind::Blocker {
                if q.suggestions.is_empty() {
                    bail!(
                        "Resolving a blocker requires --option <k> or --answer <text>. \
                         This blocker has no proposed options; use --answer <text>. \
                         (Just adding --comment leaves the blocker ambiguous.)"
                    );
                }
                let options = q
                    .suggestions
                    .iter()
                    .enumerate()
                    .map(|(i, s)| format!("{}) {s}", i + 1))
                    .collect::<Vec<_>>()
                    .join(", ");
                bail!(
                    "Resolving a blocker requires --option <k> or --answer <text>. \
                     Available options for this blocker: {options}. \
                     (Just adding --comment leaves the blocker ambiguous.)"
                );
            }
            // Question: a comment-only resolution is valid — the comment IS
            // the answer, and it flows into the next prompt via the bounded
            // resolved-interruptions injection.
            if comment.is_none() {
                bail!(
                    "Provide a resolution: --option <k>, --answer <text>, or \
                     --comment <text>."
                );
            }
            String::new()
        }
    };

    // Capture the step_id BEFORE resolve so we can emit the NDJSON event
    // even after the row transitions to resolved.
    let step_id_for_event = q.step_id.clone();
    // Phase E Fix 1: resolve + retry-exhausted side-effect run in a SINGLE
    // transaction. The pre-Phase-E shape ran them in two separate writes,
    // letting a peer `ralph run` observe (resolved interruption + Pending
    // status + attempts==max) between the writes and bail in the executor's
    // budget guard.
    resolve_interruption_with_retry_handling(conn, project, &q.id, &resolution, comment)?;
    // Phase E Fix 4: emit `InterruptionResolved` for the CLI resolve path.
    // Best-effort + JSON-mode-gated, matching the raised-event helper.
    crate::output::emit_interruption_resolved(
        conn,
        out.format == OutputFormat::Json,
        &q.id,
        &step_id_for_event,
        &resolution,
        comment,
    );

    // Echo which interruption was actually resolved. The selector may be a
    // 1-based index, and indices shift if a concurrent `ralph run` raises a
    // new interruption between the user's `list` and `resolve` — surfacing the
    // resolved target's id/plan/step/kind/body makes a mismatch obvious.
    let preview = body_preview(&q.question);
    if out.format == OutputFormat::Json {
        let json = serde_json::json!({
            "resolved": true,
            "id": q.id,
            "kind": q.kind.as_str(),
            "plan_slug": q.plan_slug,
            "step_num": q.step_num,
            "step_title": q.step_title,
            "body": preview,
            "resolution": resolution,
            "comment": comment,
        });
        println!("{}", serde_json::to_string(&json)?);
    } else {
        let id_prefix: String = q.id.chars().take(8).collect();
        println!(
            "Resolved {} interruption {} on plan '{}' step {} ({}).",
            q.kind.as_str(),
            id_prefix,
            q.plan_slug,
            q.step_num,
            q.step_title.trim()
        );
        if !preview.is_empty() {
            println!("    {preview}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::plan::{InterruptionKind, InterruptionOption};

    fn quiet_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    fn seed_plan_and_step(conn: &Connection, slug: &str, project: &str) -> (String, String) {
        let plan = storage::create_plan(conn, slug, project, "br", "desc", None, None, &[])
            .expect("create_plan");
        let (step, _) = storage::create_step(
            conn,
            &plan.id,
            "title",
            "desc",
            None,
            None,
            &[],
            None,
            None,
            None,
            None,
        )
        .expect("create_step");
        (plan.id, step.id)
    }

    #[test]
    fn list_show_resolve_round_trip_native() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-irt";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);

        let qid = storage::insert_interruption(
            &conn,
            &step_id,
            1,
            InterruptionKind::Question,
            "DB engine?",
            &[
                InterruptionOption {
                    text: "Postgres".into(),
                    priority: 1,
                },
                InterruptionOption {
                    text: "SQLite".into(),
                    priority: 2,
                },
            ],
        )
        .unwrap();
        let bid = storage::insert_interruption(
            &conn,
            &step_id,
            1,
            InterruptionKind::Blocker,
            "needs sudo",
            &[],
        )
        .unwrap();

        // list surfaces both kinds.
        let items = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert_eq!(items.len(), 2);

        cmd_interruption_list(&conn, project, None, &quiet_out()).unwrap();
        cmd_interruption_show(&conn, project, None, &qid, &quiet_out()).unwrap();
        // Index selector resolves too.
        cmd_interruption_show(&conn, project, None, "1", &quiet_out()).unwrap();

        // Resolve the question via --option 2 (SQLite by priority).
        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &qid,
            Some(2),
            None,
            Some("go with file db"),
            &quiet_out(),
        )
        .unwrap();
        let resolved = storage::list_interruptions_for_step(&conn, &step_id).unwrap();
        let q = resolved.iter().find(|i| i.id == qid).unwrap();
        assert_eq!(q.state, crate::plan::InterruptionState::Resolved);
        assert_eq!(q.resolution.as_deref(), Some("SQLite"));
        assert_eq!(q.comment.as_deref(), Some("go with file db"));

        // Resolve the blocker via --answer.
        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &bid,
            None,
            Some("granted"),
            None,
            &quiet_out(),
        )
        .unwrap();
        let after = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert!(after.is_empty(), "both interruptions resolved");
    }

    #[test]
    fn resolve_option_out_of_range_errors_without_writing() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-oor";
        let (_p, step_id) = seed_plan_and_step(&conn, "p", project);
        let qid = storage::insert_interruption(
            &conn,
            &step_id,
            1,
            InterruptionKind::Question,
            "?",
            &[InterruptionOption {
                text: "only".into(),
                priority: 1,
            }],
        )
        .unwrap();

        let err = cmd_interruption_resolve(
            &conn,
            project,
            None,
            &qid,
            Some(5),
            None,
            None,
            &quiet_out(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("out of range"));

        // Still open.
        let still = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert_eq!(still.len(), 1);
    }

    #[test]
    fn resolve_unknown_selector_errors() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-unk";
        seed_plan_and_step(&conn, "p", project);
        let err = cmd_interruption_resolve(
            &conn,
            project,
            None,
            "nope",
            None,
            Some("x"),
            None,
            &quiet_out(),
        )
        .unwrap_err();
        assert!(err.to_string().contains("No open interruption matched"));
    }

    // -- Phase C: apply_retry_exhausted_resolution -------------------------

    /// Helper: insert the Phase B auto-raised retry-exhausted blocker
    /// (two ranked options, [`RETRY_EXHAUSTED_OPTION_RETRY`] /
    /// [`RETRY_EXHAUSTED_OPTION_FAIL`]). Mirrors the shape produced by
    /// `executor::raise_retry_exhausted_blocker`.
    fn seed_auto_blocker(conn: &Connection, step_id: &str) -> String {
        storage::insert_interruption(
            conn,
            step_id,
            3,
            InterruptionKind::Blocker,
            "Step failed after 3 attempts.",
            &[
                InterruptionOption {
                    text: RETRY_EXHAUSTED_OPTION_RETRY.into(),
                    priority: 1,
                },
                InterruptionOption {
                    text: RETRY_EXHAUSTED_OPTION_FAIL.into(),
                    priority: 2,
                },
            ],
        )
        .unwrap()
    }

    fn step_attempts(conn: &Connection, step_id: &str) -> i32 {
        conn.query_row(
            "SELECT attempts FROM steps WHERE id = ?1",
            rusqlite::params![step_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn step_status(conn: &Connection, step_id: &str) -> StepStatus {
        let s: String = conn
            .query_row(
                "SELECT status FROM steps WHERE id = ?1",
                rusqlite::params![step_id],
                |r| r.get(0),
            )
            .unwrap();
        use std::str::FromStr;
        StepStatus::from_str(&s).unwrap()
    }

    #[test]
    fn test_apply_retry_exhausted_resolution_retry_resets_attempts() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-rer-retry";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        // Simulate the executor's pre-resolve state: attempts at max,
        // status Pending (executor parks the step at Pending per the
        // Phase B contract).
        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_auto_blocker(&conn, &step_id);

        let acted =
            apply_retry_exhausted_resolution(&conn, project, &id, RETRY_EXHAUSTED_OPTION_RETRY)
                .unwrap();
        assert!(acted, "auto-blocker recognized");
        assert_eq!(step_attempts(&conn, &step_id), 0, "attempts reset to 0");
        assert_eq!(
            step_status(&conn, &step_id),
            StepStatus::Pending,
            "status stays Pending so the scheduler re-queues"
        );
    }

    /// Fix 1: the Fail option match is EXACT (case-sensitive) but trims
    /// surrounding whitespace. A whitespace-padded EXACT pick still flips to
    /// Failed; a case-mismatched freeform string is NOT coerced into Fail.
    #[test]
    fn test_apply_retry_exhausted_resolution_fail_match_is_trimmed_exact() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-rer-fail-normalized";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_auto_blocker(&conn, &step_id);

        // Whitespace-padded but otherwise EXACT option text => Fail.
        let padded = format!("  {RETRY_EXHAUSTED_OPTION_FAIL}  ");
        let acted = apply_retry_exhausted_resolution(&conn, project, &id, &padded).unwrap();
        assert!(acted);
        assert_eq!(step_status(&conn, &step_id), StepStatus::Failed);
    }

    /// Fix 1: a case-mismatched freeform answer (`"mark step failed"` lower)
    /// must NOT terminally fail the step — it falls through to
    /// retry-with-hint (status stays Pending, attempts reset). Pre-fix the
    /// match was `eq_ignore_ascii_case`, which mis-classified this as Fail.
    #[test]
    fn test_apply_retry_exhausted_resolution_case_mismatch_freeform_is_retry() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-rer-case-mismatch";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_auto_blocker(&conn, &step_id);

        let acted =
            apply_retry_exhausted_resolution(&conn, project, &id, "  mark step failed  ").unwrap();
        assert!(acted, "auto-blocker still recognized regardless of text");
        assert_eq!(
            step_status(&conn, &step_id),
            StepStatus::Pending,
            "case-mismatched freeform is retry-with-hint, not Fail",
        );
        assert_eq!(step_attempts(&conn, &step_id), 0);
    }

    #[test]
    fn test_apply_retry_exhausted_resolution_fail_marks_failed() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-rer-fail";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        let id = seed_auto_blocker(&conn, &step_id);

        let acted =
            apply_retry_exhausted_resolution(&conn, project, &id, RETRY_EXHAUSTED_OPTION_FAIL)
                .unwrap();
        assert!(acted);
        assert_eq!(
            step_status(&conn, &step_id),
            StepStatus::Failed,
            "terminal Failed"
        );
        // attempts is NOT reset — the row records the exhausted budget.
        assert_eq!(step_attempts(&conn, &step_id), 3);
    }

    #[test]
    fn test_apply_retry_exhausted_resolution_returns_false_for_normal_question() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-rer-q";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 2).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::InProgress).unwrap();

        // Normal harness-raised question with two options that LOOK
        // like the auto-blocker texts: still Question kind → must not
        // match. (Defensive: the kind discriminator is the first gate.)
        let qid = storage::insert_interruption(
            &conn,
            &step_id,
            1,
            InterruptionKind::Question,
            "?",
            &[
                InterruptionOption {
                    text: RETRY_EXHAUSTED_OPTION_RETRY.into(),
                    priority: 1,
                },
                InterruptionOption {
                    text: RETRY_EXHAUSTED_OPTION_FAIL.into(),
                    priority: 2,
                },
            ],
        )
        .unwrap();

        let acted =
            apply_retry_exhausted_resolution(&conn, project, &qid, RETRY_EXHAUSTED_OPTION_RETRY)
                .unwrap();
        assert!(!acted, "Question kind must never match auto-blocker");
        // No state was touched.
        assert_eq!(step_attempts(&conn, &step_id), 2);
        assert_eq!(step_status(&conn, &step_id), StepStatus::InProgress);
    }

    #[test]
    fn test_apply_retry_exhausted_resolution_returns_false_for_harness_raised_blocker() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-rer-hb";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 1).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::InProgress).unwrap();

        // Harness-raised blocker (empty options) — the most common
        // false-positive guard: empty options must not match.
        let bid = storage::insert_interruption(
            &conn,
            &step_id,
            1,
            InterruptionKind::Blocker,
            "needs sudo",
            &[],
        )
        .unwrap();

        let acted = apply_retry_exhausted_resolution(&conn, project, &bid, "granted").unwrap();
        assert!(!acted, "empty-options blocker must not match");
        assert_eq!(step_attempts(&conn, &step_id), 1);
        assert_eq!(step_status(&conn, &step_id), StepStatus::InProgress);
    }

    #[test]
    fn test_apply_retry_exhausted_resolution_freeform_treated_as_retry() {
        // Spec: a freeform resolution that matches neither option text
        // is the "Enter on default-selected option + hint" UX — treat as
        // Retry. The hint flows into the next prompt via the bounded
        // resolved-interruptions injection (already persisted by the
        // caller's `resolve_interruption`).
        let conn = db::open_memory().unwrap();
        let project = "/proj-rer-ff";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_auto_blocker(&conn, &step_id);

        let acted = apply_retry_exhausted_resolution(
            &conn,
            project,
            &id,
            "try with --foo bar to skip the broken path",
        )
        .unwrap();
        assert!(
            acted,
            "auto-blocker recognized regardless of resolution text"
        );
        assert_eq!(step_attempts(&conn, &step_id), 0);
        assert_eq!(step_status(&conn, &step_id), StepStatus::Pending);
    }

    #[test]
    fn test_apply_retry_exhausted_resolution_missing_id_errors() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-rer-missing";
        let err = apply_retry_exhausted_resolution(&conn, project, "no-such-id", "x").unwrap_err();
        assert!(
            err.to_string().contains("interruption not found"),
            "got: {err}"
        );
    }

    #[test]
    fn test_cli_resolve_with_auto_blocker_retry_resets_step() {
        // End-to-end via the CLI handler: a retry-exhausted auto-blocker
        // resolved with `--option 1` (priority 1 = Retry the step from
        // scratch) must (a) resolve the interruption and (b) reset
        // attempts/status. This is the contract for `ralph interruption
        // resolve <id> --option 1` and `ralph interruption resolve <id>
        // --answer "Retry step with parked changes"`.
        let conn = db::open_memory().unwrap();
        let project = "/proj-cli-retry";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_auto_blocker(&conn, &step_id);

        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &id,
            Some(1), // priority 1 = Retry
            None,
            None,
            &quiet_out(),
        )
        .unwrap();

        // Interruption resolved.
        let after = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert!(after.is_empty(), "interruption resolved");
        // Side-effect applied.
        assert_eq!(step_attempts(&conn, &step_id), 0);
        assert_eq!(step_status(&conn, &step_id), StepStatus::Pending);
    }

    #[test]
    fn test_cli_resolve_with_auto_blocker_retry_keeps_audit_trail_and_allows_new_attempt_one_log() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-cli-retry-logs";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);

        // Simulate the exhausted cycle's persisted audit rows. The reset path
        // must preserve them while still allowing a fresh logical attempt=1.
        storage::create_execution_log(&conn, &step_id, 1, Some("attempt 1"), None).unwrap();
        storage::create_execution_log(&conn, &step_id, 2, Some("attempt 2"), None).unwrap();
        storage::create_execution_log(&conn, &step_id, 3, Some("attempt 3"), None).unwrap();

        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_auto_blocker(&conn, &step_id);

        cmd_interruption_resolve(&conn, project, None, &id, Some(1), None, None, &quiet_out())
            .unwrap();

        // Retry-with-parked-changes preserved the old audit rows. A fresh logical
        // attempt=1 log must now insert successfully for the new cycle.
        storage::create_execution_log(&conn, &step_id, 1, Some("retry cycle attempt 1"), None)
            .unwrap();

        let logs = storage::list_execution_logs_for_step(&conn, &step_id).unwrap();
        assert_eq!(logs.len(), 4);
        assert_eq!(
            logs.iter().map(|l| l.attempt).collect::<Vec<_>>(),
            vec![1, 2, 3, 1],
            "logical attempt numbers may repeat across retry-from-scratch cycles"
        );
        assert_eq!(logs[0].prompt_text.as_deref(), Some("attempt 1"));
        assert_eq!(
            logs[3].prompt_text.as_deref(),
            Some("retry cycle attempt 1")
        );
        // Phase E Fix 2: the old audit rows stay at cycle 0; the post-reset
        // log lands in cycle 1 (V33 cycle bump on set_step_attempts(0)).
        assert_eq!(
            logs.iter().map(|l| l.cycle_index).collect::<Vec<_>>(),
            vec![0, 0, 0, 1],
            "cycle pointer bumps from 0 to 1 after the auto-blocker reset",
        );
        assert_eq!(step_attempts(&conn, &step_id), 0);
        assert_eq!(step_status(&conn, &step_id), StepStatus::Pending);
    }

    #[test]
    fn test_cli_resolve_emits_no_panic_in_json_mode() {
        // Phase E Fix 4: the JSON-gated `InterruptionResolved` emission path
        // must not break the resolve happy path. We can't observe the NDJSON
        // line from a unit test, but we can drive the JSON-mode handler end
        // to end and assert the underlying state transitioned correctly.
        let conn = db::open_memory().unwrap();
        let project = "/proj-json-resolve";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        let qid = storage::insert_interruption(
            &conn,
            &step_id,
            1,
            InterruptionKind::Question,
            "Pick one?",
            &[
                InterruptionOption {
                    text: "A".into(),
                    priority: 1,
                },
                InterruptionOption {
                    text: "B".into(),
                    priority: 2,
                },
            ],
        )
        .unwrap();

        let json_out = OutputContext {
            format: OutputFormat::Json,
            quiet: true,
            color: false,
        };
        cmd_interruption_resolve(&conn, project, None, &qid, Some(2), None, None, &json_out)
            .unwrap();

        let after = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert!(after.is_empty(), "resolved row drops out of the open set");
    }

    #[test]
    fn test_cli_resolve_with_auto_blocker_fail_marks_step_failed() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-cli-fail";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_auto_blocker(&conn, &step_id);

        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &id,
            None,
            Some(RETRY_EXHAUSTED_OPTION_FAIL), // freeform that matches the Fail option text
            None,
            &quiet_out(),
        )
        .unwrap();

        assert_eq!(step_status(&conn, &step_id), StepStatus::Failed);
    }

    // -- Phase E Fix 1: atomic resolve + retry-exhausted side-effect --------

    /// `resolve_interruption_with_retry_handling` must commit (a) the
    /// interruptions.state flip AND (b) the step-side reset/Fail in a single
    /// transaction. If a `BEGIN` is in flight on a *separate* connection,
    /// SQLite's default `journal_mode=delete` plus a deferred write lock
    /// means the second connection cannot read the half-written state — the
    /// transaction is atomic at the SQL level. We exercise this by holding a
    /// concurrent read transaction on a second connection and asserting it
    /// either sees the pre-resolve state or the fully-post-resolve state,
    /// never the in-between.
    #[test]
    fn test_resolve_with_retry_handling_atomic_resolve_and_reset() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-atomic-retry";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_auto_blocker(&conn, &step_id);

        // Pre-state: open interruption, status Pending, attempts 3.
        let opens_pre = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert_eq!(opens_pre.len(), 1);
        assert_eq!(step_attempts(&conn, &step_id), 3);

        let acted = resolve_interruption_with_retry_handling(
            &conn,
            project,
            &id,
            RETRY_EXHAUSTED_OPTION_RETRY,
            None,
        )
        .unwrap();
        assert!(
            acted,
            "the auto-blocker was recognized and side-effect applied"
        );

        // Post-state: zero open interruptions AND attempts reset AND still
        // Pending. The bug was that pre-Phase-E left a window where the
        // interruption was resolved but attempts was still 3 — the scheduler
        // would re-pick the step and bail on the budget guard.
        let opens_post = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert!(opens_post.is_empty(), "interruption resolved");
        assert_eq!(
            step_attempts(&conn, &step_id),
            0,
            "attempts reset atomically with resolve",
        );
        assert_eq!(step_status(&conn, &step_id), StepStatus::Pending);
    }

    /// `resolve_interruption_with_retry_handling` rolls back the resolve
    /// when the side-effect write fails. We can't easily inject a failure on
    /// the storage helpers, but we *can* prove the transaction shape by
    /// asserting that hitting a missing-id error from `resolve_interruption`
    /// leaves the DB untouched.
    #[test]
    fn test_resolve_with_retry_handling_missing_id_leaves_state_clean() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-atomic-missing";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();

        let err = resolve_interruption_with_retry_handling(
            &conn,
            project,
            "no-such-id",
            RETRY_EXHAUSTED_OPTION_RETRY,
            None,
        )
        .unwrap_err();
        assert!(err.to_string().contains("not found"), "got: {err}");

        // Step state untouched.
        assert_eq!(step_attempts(&conn, &step_id), 3);
        assert_eq!(step_status(&conn, &step_id), StepStatus::Pending);
    }

    /// A normal question routed through the atomic helper still resolves and
    /// returns `false` (no retry-exhausted side-effect).
    #[test]
    fn test_resolve_with_retry_handling_normal_question_only_resolves() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-atomic-q";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        let qid = storage::insert_interruption(
            &conn,
            &step_id,
            1,
            InterruptionKind::Question,
            "?",
            &[InterruptionOption {
                text: "A".into(),
                priority: 1,
            }],
        )
        .unwrap();

        let acted =
            resolve_interruption_with_retry_handling(&conn, project, &qid, "A", Some("note"))
                .unwrap();
        assert!(!acted, "normal question: no retry-exhausted side-effect");

        let resolved = storage::list_interruptions_for_step(&conn, &step_id).unwrap();
        let q = resolved.iter().find(|i| i.id == qid).unwrap();
        assert_eq!(q.state, crate::plan::InterruptionState::Resolved);
        assert_eq!(q.resolution.as_deref(), Some("A"));
        assert_eq!(q.comment.as_deref(), Some("note"));
    }

    // -- Phase E Fix 3: reject --comment-only resolve on retry-exhausted blockers --

    /// `cmd_interruption_resolve` must reject a `--comment` only resolution
    /// of a Phase B retry-exhausted auto-blocker. Pre-Phase-E this silently
    /// routed through Retry (empty resolution_text != RETRY_EXHAUSTED_OPTION_FAIL),
    /// burning a retry budget without surfacing the operator's choice.
    #[test]
    fn test_cli_resolve_blocker_with_comment_only_is_rejected() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-comment-only-blocker";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_auto_blocker(&conn, &step_id);

        let err = cmd_interruption_resolve(
            &conn,
            project,
            None,
            &id,
            None,
            None,
            Some("just a note, not a decision"),
            &quiet_out(),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("Resolving a blocker requires --option <k> or --answer <text>"),
            "error message must explain the requirement: {msg}",
        );
        // Surface the available options so the operator can re-issue the
        // right command without inspecting the row.
        assert!(
            msg.contains(RETRY_EXHAUSTED_OPTION_RETRY),
            "error must surface the Retry option text: {msg}",
        );
        assert!(
            msg.contains(RETRY_EXHAUSTED_OPTION_FAIL),
            "error must surface the Mark Failed option text: {msg}",
        );
        // Nothing should have been written.
        let still_open = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert_eq!(
            still_open.len(),
            1,
            "interruption still open after rejection"
        );
        assert_eq!(
            step_attempts(&conn, &step_id),
            3,
            "no retry budget consumed",
        );
        assert_eq!(step_status(&conn, &step_id), StepStatus::Pending);
    }

    /// Harness-raised blockers (no options) also reject `--comment` only —
    /// without an `--answer` the operator has not actually committed to a
    /// resolution, just left a note.
    #[test]
    fn test_cli_resolve_harness_blocker_with_comment_only_is_rejected() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-harness-blocker-comment-only";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        let bid = storage::insert_interruption(
            &conn,
            &step_id,
            1,
            InterruptionKind::Blocker,
            "needs sudo",
            &[], // no options — harness-raised
        )
        .unwrap();

        let err = cmd_interruption_resolve(
            &conn,
            project,
            None,
            &bid,
            None,
            None,
            Some("looked into it"),
            &quiet_out(),
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("--option <k> or --answer <text>"),
            "got: {msg}"
        );
        assert!(
            msg.contains("no proposed options"),
            "error should mention that this blocker has no options: {msg}",
        );
    }

    /// Questions remain `--comment` only friendly — the comment IS the
    /// answer there. (Regression guard: don't tighten the question path
    /// while tightening the blocker path.)
    #[test]
    fn test_cli_resolve_question_with_comment_only_still_works() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-q-comment-only";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        let qid = storage::insert_interruption(
            &conn,
            &step_id,
            1,
            InterruptionKind::Question,
            "Which DB?",
            &[InterruptionOption {
                text: "SQLite".into(),
                priority: 1,
            }],
        )
        .unwrap();

        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &qid,
            None,
            None,
            Some("Postgres please, found prior art"),
            &quiet_out(),
        )
        .unwrap();

        let after = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert!(after.is_empty(), "question resolved by comment-only");
    }

    // -- Bug #2: parked-restore blocker resolution side-effects ----------

    /// Helper: insert a parked-restore blocker the way
    /// `runner::raise_parked_restore_blocker` would (the marker-prefixed
    /// body + the two ranked options). Mirrors the executor's
    /// `seed_auto_blocker` pattern.
    fn seed_parked_restore_blocker(conn: &Connection, step_id: &str) -> String {
        let body = format!(
            "{PARKED_RESTORE_BLOCKER_MARKER}\nParked stash gone; mark Failed or mark Pending."
        );
        storage::insert_interruption(
            conn,
            step_id,
            1,
            InterruptionKind::Blocker,
            &body,
            &[
                InterruptionOption {
                    text: PARKED_RESTORE_OPTION_MARK_FAILED.into(),
                    priority: 1,
                },
                InterruptionOption {
                    text: PARKED_RESTORE_OPTION_MARK_PENDING.into(),
                    priority: 2,
                },
            ],
        )
        .unwrap()
    }

    fn step_status_q(conn: &Connection, step_id: &str) -> StepStatus {
        let s: String = conn
            .query_row(
                "SELECT status FROM steps WHERE id = ?1",
                rusqlite::params![step_id],
                |r| r.get(0),
            )
            .unwrap();
        use std::str::FromStr;
        StepStatus::from_str(&s).unwrap()
    }

    fn step_attempts_q(conn: &Connection, step_id: &str) -> i32 {
        conn.query_row(
            "SELECT attempts FROM steps WHERE id = ?1",
            rusqlite::params![step_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// Marker detection: only a Blocker that starts with the marker matches.
    #[test]
    fn test_is_parked_restore_blocker_marker_detection() {
        use crate::plan::{Interruption, InterruptionState};
        use chrono::Utc;

        let mut base = Interruption {
            id: "x".into(),
            step_id: "s".into(),
            attempt: 1,
            kind: InterruptionKind::Blocker,
            body: format!("{PARKED_RESTORE_BLOCKER_MARKER}\nrest of body"),
            options: vec![],
            resolution: None,
            comment: None,
            state: InterruptionState::Open,
            asked_at: Utc::now(),
            resolved_at: None,
        };
        assert!(
            is_parked_restore_blocker(&base),
            "marker prefix + Blocker => true"
        );

        // Wrong kind: a Question carrying the marker text is still a Question.
        base.kind = InterruptionKind::Question;
        assert!(
            !is_parked_restore_blocker(&base),
            "Question kind never matches"
        );
        base.kind = InterruptionKind::Blocker;

        // No marker: a harness-raised blocker without the prefix does not match.
        base.body = "needs sudo".into();
        assert!(
            !is_parked_restore_blocker(&base),
            "no marker prefix => false"
        );
    }

    #[test]
    fn test_is_parked_restore_blocker_requires_marker_prefix() {
        use crate::plan::{Interruption, InterruptionState};
        use chrono::Utc;

        let mut base = Interruption {
            id: "x".into(),
            step_id: "s".into(),
            attempt: 1,
            kind: InterruptionKind::Blocker,
            body: "Applying the parked interruption stash for step 'Foo' conflicted.".into(),
            options: vec![],
            resolution: None,
            comment: None,
            state: InterruptionState::Open,
            asked_at: Utc::now(),
            resolved_at: None,
        };
        assert!(
            !is_parked_restore_blocker(&base),
            "markerless blocker bodies must not be auto-routed"
        );

        base.body = format!("{PARKED_RESTORE_BLOCKER_MARKER}\nconflicted");
        assert!(is_parked_restore_blocker(&base));
    }

    /// `--option 1` (MARK_FAILED) flips the step to terminal Failed and
    /// resolves the interruption. Pre-fix this was a no-op (options were
    /// empty, no side effect wired) and the step stayed Pending; the
    /// scheduler then re-fired the same restore error on the next tick.
    /// Fix 1: the MARK_FAILED match is EXACT (case-sensitive) but trims
    /// surrounding whitespace. A whitespace-padded EXACT freeform answer
    /// still flips the step to Failed.
    #[test]
    fn test_parked_restore_blocker_mark_failed_match_is_trimmed_exact() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-pr-fail-normalized";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 2).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_parked_restore_blocker(&conn, &step_id);

        let padded = format!("  {PARKED_RESTORE_OPTION_MARK_FAILED}  ");
        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &id,
            None,
            Some(&padded),
            None,
            &quiet_out(),
        )
        .unwrap();

        assert_eq!(step_status_q(&conn, &step_id), StepStatus::Failed);
    }

    /// Fix 1: a case-mismatched freeform answer (`"skip and mark failed"`
    /// lower) must NOT terminally fail the step — it falls through to the
    /// MARK_PENDING (start-fresh-with-hint) path. Pre-fix the match was
    /// `eq_ignore_ascii_case`, which mis-classified this as Fail.
    #[test]
    fn test_parked_restore_blocker_case_mismatch_freeform_is_pending() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-pr-case-mismatch";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 2).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_parked_restore_blocker(&conn, &step_id);

        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &id,
            None,
            Some("  skip and mark failed  "),
            None,
            &quiet_out(),
        )
        .unwrap();

        assert_eq!(
            step_status_q(&conn, &step_id),
            StepStatus::Pending,
            "case-mismatched freeform falls through to MARK_PENDING, not Fail",
        );
    }

    #[test]
    fn test_parked_restore_blocker_mark_failed_resolution() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-pr-fail";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        // Simulate the executor's pre-park state.
        storage::set_step_attempts(&conn, &step_id, 2).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_parked_restore_blocker(&conn, &step_id);

        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &id,
            Some(1), // priority 1 = MARK_FAILED
            None,
            None,
            &quiet_out(),
        )
        .unwrap();

        // Interruption resolved.
        let after = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert!(after.is_empty(), "interruption resolved");
        // Side-effect applied.
        assert_eq!(
            step_status_q(&conn, &step_id),
            StepStatus::Failed,
            "MARK_FAILED flips status to terminal Failed",
        );
        // attempts is untouched — the row records whatever counter the
        // executor reached before parking.
        assert_eq!(step_attempts_q(&conn, &step_id), 2);
    }

    /// `--option 2` (MARK_PENDING) leaves the step Pending (attempts
    /// unchanged) and resolves the interruption. The scheduler can then
    /// re-pick the step on the next tick with a clean slate.
    #[test]
    fn test_parked_restore_blocker_mark_pending_resolution() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-pr-pending";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 2).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_parked_restore_blocker(&conn, &step_id);

        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &id,
            Some(2), // priority 2 = MARK_PENDING
            None,
            None,
            &quiet_out(),
        )
        .unwrap();

        let after = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert!(after.is_empty(), "interruption resolved");
        assert_eq!(
            step_status_q(&conn, &step_id),
            StepStatus::Pending,
            "MARK_PENDING leaves status Pending so the scheduler re-picks",
        );
        assert_eq!(step_attempts_q(&conn, &step_id), 2, "attempts unchanged");
    }

    /// `--answer "<freeform>"` falls through to MARK_PENDING semantics
    /// (start fresh with the hint), matching the retry-exhausted
    /// freeform-with-hint convention. The freeform string is persisted on
    /// the resolved interruption row so the bounded "Resolved
    /// interruptions" prompt section picks it up on the next attempt.
    #[test]
    fn test_parked_restore_blocker_freeform_resolution() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-pr-freeform";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 1).unwrap();
        storage::update_step_status(&conn, &step_id, StepStatus::Pending).unwrap();
        let id = seed_parked_restore_blocker(&conn, &step_id);

        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &id,
            None,
            Some("the disk was full"),
            None,
            &quiet_out(),
        )
        .unwrap();

        let after = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert!(
            after.is_empty(),
            "freeform resolution still resolves the row"
        );
        assert_eq!(
            step_status_q(&conn, &step_id),
            StepStatus::Pending,
            "freeform falls through to MARK_PENDING (start fresh)",
        );
        assert_eq!(step_attempts_q(&conn, &step_id), 1);

        // The freeform text is persisted as the resolution, so the bounded
        // "Resolved interruptions" prompt section sees it on the next run.
        let rows = storage::list_interruptions_for_step(&conn, &step_id).unwrap();
        let row = rows.iter().find(|i| i.id == id).unwrap();
        assert_eq!(row.resolution.as_deref(), Some("the disk was full"));
    }

    /// Atomic-resolver direct entry point: same dispatch as the CLI
    /// handler, exercised here without the clap layer. Asserts that the
    /// auto-blocker return-bool stays `false` for a parked-restore blocker
    /// (the bool only flags the retry-exhausted shape — the Phase C
    /// `apply_retry_exhausted_resolution` wrapper inspects it; parked-restore
    /// callers should call `is_parked_restore_blocker` directly).
    #[test]
    fn test_resolve_with_retry_handling_parked_restore_returns_false() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-pr-helper";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        let id = seed_parked_restore_blocker(&conn, &step_id);

        let acted = resolve_interruption_with_retry_handling(
            &conn,
            project,
            &id,
            PARKED_RESTORE_OPTION_MARK_FAILED,
            None,
        )
        .unwrap();
        assert!(
            !acted,
            "return-bool is the retry-exhausted flag only; parked-restore returns false"
        );
        // But the side-effect still happened.
        assert_eq!(step_status_q(&conn, &step_id), StepStatus::Failed);
    }

    // -- §10 item 4: review-loop escalation blocker resolution ----------

    /// Helper: insert a review-loop escalation blocker the way
    /// `review::consume_corrective_request` would — the marker-prefixed body
    /// + empty options (the human resolves it with `--answer`).
    fn seed_review_loop_escalation_blocker(
        conn: &Connection,
        step_id: &str,
        attempt: i32,
    ) -> String {
        let body = format!(
            "{}\nreview loop — needs human: step has been corrected 1 time(s) and still fails.",
            crate::review::REVIEW_LOOP_ESCALATION_MARKER
        );
        storage::insert_interruption(
            conn,
            step_id,
            attempt,
            InterruptionKind::Blocker,
            &body,
            &[],
        )
        .unwrap()
    }

    /// Resolving the escalation blocker inserts exactly ONE
    /// `human_approved = true` open corrective request for the escalated step
    /// and marks the interruption resolved.
    #[test]
    fn test_resolve_escalation_blocker_inserts_one_human_approved_request() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-escalation";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        let id = seed_review_loop_escalation_blocker(&conn, &step_id, 3);

        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &id,
            None,
            Some("approved, take one more pass"),
            None,
            &quiet_out(),
        )
        .unwrap();

        // The blocker is resolved.
        let after = storage::list_open_interruptions_enriched(&conn, project, None).unwrap();
        assert!(after.is_empty(), "escalation blocker resolved");

        // Exactly one open corrective request, human-approved, for this step.
        let reqs = storage::list_open_corrective_step_requests_for_plan(&conn, &plan_id).unwrap();
        assert_eq!(reqs.len(), 1, "exactly one corrective request inserted");
        assert!(reqs[0].human_approved, "request must be human-approved");
        assert_eq!(reqs[0].reviewed_step_id, step_id);
        assert_eq!(
            reqs[0].reviewed_iteration, 3,
            "iteration carried from the blocker's attempt"
        );
    }

    /// A non-escalation blocker resolution must NOT insert a corrective
    /// request (the marker gate is exact).
    #[test]
    fn test_resolve_plain_blocker_inserts_no_corrective_request() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-plain-blocker";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        let id = storage::insert_interruption(
            &conn,
            &step_id,
            1,
            InterruptionKind::Blocker,
            "needs sudo",
            &[],
        )
        .unwrap();

        cmd_interruption_resolve(
            &conn,
            project,
            None,
            &id,
            None,
            Some("granted"),
            None,
            &quiet_out(),
        )
        .unwrap();

        let reqs = storage::list_open_corrective_step_requests_for_plan(&conn, &plan_id).unwrap();
        assert!(
            reqs.is_empty(),
            "a plain blocker must not insert a corrective request"
        );
    }
}
