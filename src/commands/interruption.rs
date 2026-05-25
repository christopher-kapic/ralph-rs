// `ralph interruption list/show/resolve` — the human-side CLI for the
// unified interruption model (docs/dag-redesign.md §7). The TUI inbox is the
// primary path; this is the scriptable equivalent. All I/O is native against
// the `interruptions` table (no `step_questions`).

use anyhow::{Result, bail};
use rusqlite::Connection;

use crate::db;
use crate::executor::{RETRY_EXHAUSTED_OPTION_FAIL, RETRY_EXHAUSTED_OPTION_RETRY};
use crate::output::{OutputContext, OutputFormat};
use crate::plan::{InterruptionKind, StepStatus};
use crate::storage::{self, OpenQuestion};

/// Phase C: react to the human-side resolution of the **Phase B auto-raised
/// retry-exhausted blocker** (the only interruption that carries the two
/// ranked options [`RETRY_EXHAUSTED_OPTION_RETRY`] /
/// [`RETRY_EXHAUSTED_OPTION_FAIL`]; harness-raised blockers have empty
/// options and never match).
///
/// `Ok(true)` — the interruption was the auto-blocker and the side-effect
/// has been applied (`Retry` → `attempts = 0` + status `Pending` so the
/// scheduler re-queues; `Fail` → status `Failed`, terminal). `Ok(false)` —
/// the interruption was a normal question or harness-raised blocker; the
/// caller's existing `resolve_interruption` flow is the whole story.
///
/// Resolution-text matching uses the executor's `pub const` strings (so
/// drift between writer and reader trips `cargo test --lib`, not production).
/// A freeform resolution that matches neither option is treated as
/// **Retry-from-scratch + hint**: `resolve_interruption` already persisted
/// the freeform string as the resolution and the comment, both of which
/// flow into the next step prompt via the bounded
/// `list_resolved_interruptions_for_step` injection (§8). This preserves the
/// "Enter on the default-selected option" UX while honoring the spec
/// guidance that the safest interpretation of an ambiguous freeform answer
/// to a retry-exhausted blocker is "try again with this as a hint."
///
/// The reset writes are wrapped in [`db::with_tx`] so a concurrent scheduler
/// poll cannot observe `attempts = 0` with the step still `InProgress` (or
/// vice versa) — the half-state would let the scheduler skip re-queueing
/// the step.
pub fn apply_retry_exhausted_resolution(
    conn: &Connection,
    interruption_id: &str,
    resolution_text: &str,
) -> Result<bool> {
    let interruption = storage::get_interruption(conn, interruption_id)?;
    if interruption.kind != InterruptionKind::Blocker {
        return Ok(false);
    }
    // Auto-blocker recognition: exactly two options whose texts match the
    // Phase B constants. A harness-raised blocker has empty options; a
    // hypothetical future blocker with a different option set won't match
    // either — both correctly fall through to `Ok(false)`.
    let is_auto = interruption.options.len() == 2
        && interruption
            .options
            .iter()
            .any(|o| o.text == RETRY_EXHAUSTED_OPTION_RETRY)
        && interruption
            .options
            .iter()
            .any(|o| o.text == RETRY_EXHAUSTED_OPTION_FAIL);
    if !is_auto {
        return Ok(false);
    }

    let step_id = interruption.step_id.clone();
    let want_fail = resolution_text == RETRY_EXHAUSTED_OPTION_FAIL;

    db::with_tx(conn, |tx| {
        if want_fail {
            // Explicit give-up: terminal Failed. The interruption is already
            // resolved by the caller; the freeform comment (if any) is on
            // the resolved row but never feeds another prompt — the step is
            // done.
            storage::update_step_status(tx, &step_id, StepStatus::Failed)?;
        } else {
            // "Retry the step from scratch" — the explicit option text —
            // and the freeform-doesn't-match fallthrough both land here.
            // The freeform text (and optional comment) were already stamped
            // on the resolved interruption by the caller; the bounded
            // resolved-interruptions section of the next prompt will pick
            // it up automatically.
            storage::set_step_attempts(tx, &step_id, 0)?;
            storage::update_step_status(tx, &step_id, StepStatus::Pending)?;
        }
        Ok(())
    })?;

    Ok(true)
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
        let kind = match q.kind {
            InterruptionKind::Question => "question",
            InterruptionKind::Blocker => "blocker",
        };
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

    let kind = match q.kind {
        InterruptionKind::Question => "question",
        InterruptionKind::Blocker => "blocker",
    };
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
            // No explicit resolution: a comment-only resolution is valid
            // (covers a blocker the human just clears with a note, and a
            // question resolved purely by comment).
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
    storage::resolve_interruption(conn, &q.id, &resolution, comment)?;
    // Phase C: if this was the Phase B auto-raised retry-exhausted blocker,
    // reset attempts / mark Failed per the chosen option. No-op for normal
    // interruptions (returns Ok(false) without writing).
    apply_retry_exhausted_resolution(conn, &q.id, &resolution)?;
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

    if out.format == OutputFormat::Json {
        let json = serde_json::json!({
            "resolved": true,
            "id": q.id,
            "kind": q.kind.as_str(),
            "plan_slug": q.plan_slug,
            "step_num": q.step_num,
            "resolution": resolution,
            "comment": comment,
        });
        println!("{}", serde_json::to_string(&json)?);
    } else {
        println!(
            "Resolved interruption on plan '{}' step {}.",
            q.plan_slug, q.step_num
        );
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
            apply_retry_exhausted_resolution(&conn, &id, RETRY_EXHAUSTED_OPTION_RETRY).unwrap();
        assert!(acted, "auto-blocker recognized");
        assert_eq!(step_attempts(&conn, &step_id), 0, "attempts reset to 0");
        assert_eq!(
            step_status(&conn, &step_id),
            StepStatus::Pending,
            "status stays Pending so the scheduler re-queues"
        );
    }

    #[test]
    fn test_apply_retry_exhausted_resolution_fail_marks_failed() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-rer-fail";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p", project);
        storage::set_step_attempts(&conn, &step_id, 3).unwrap();
        let id = seed_auto_blocker(&conn, &step_id);

        let acted =
            apply_retry_exhausted_resolution(&conn, &id, RETRY_EXHAUSTED_OPTION_FAIL).unwrap();
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
            apply_retry_exhausted_resolution(&conn, &qid, RETRY_EXHAUSTED_OPTION_RETRY).unwrap();
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

        let acted = apply_retry_exhausted_resolution(&conn, &bid, "granted").unwrap();
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
        let err = apply_retry_exhausted_resolution(&conn, "no-such-id", "x").unwrap_err();
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
        // --answer "Retry the step from scratch"`.
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

        // Retry-from-scratch preserved the old audit rows. A fresh logical
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
}
