// `ralph question ask` CLI implementation.
//
// Designed for the harness to call mid-step. Binds the question to the
// currently-executing step via the project's run lock — no env vars or
// harness-side context plumbing required. See TUI-plan.md §17 for the full
// design.

use anyhow::{Context, Result};
use rusqlite::{Connection, params};

use crate::output::{self, OutputContext, OutputFormat};
use crate::plan::{InterruptionKind, InterruptionOption};
use crate::storage;

/// Outcome of [`record_question_ask`]. The dispatcher in main.rs maps each
/// variant to the appropriate stderr message + exit code; the recorder itself
/// stays free of process-exit and printing concerns so it's straightforward to
/// unit-test.
#[derive(Debug)]
pub enum QuestionAskOutcome {
    /// Successfully wrote a native (open) `interruptions` row. Fields are
    /// returned for tests and the cross-process bridge (the orchestrator
    /// observes the open row after the harness returns) — main.rs's
    /// dispatcher destructures with `..` because the CLI prints nothing on
    /// success.
    #[allow(dead_code)]
    Recorded {
        question_id: String,
        step_id: String,
        attempt: i32,
    },
    /// No active `ralph run` for this project. Caller should print the
    /// "no active run" message and exit non-zero. Per TUI-plan.md §17 we
    /// treat a lock row with NULL `step_id` the same way: there's no
    /// step to bind a question to.
    NoActiveRun,
    /// The plan has `questions_enabled = false`. Caller should print the
    /// encouraging "do your best, document assumptions" message and exit
    /// non-zero. **No DB write is performed.**
    Disabled,
}

/// Stderr message for [`QuestionAskOutcome::NoActiveRun`].
pub const NO_ACTIVE_RUN_MESSAGE: &str = "ralph question ask: no active ralph run found for this project. This command only works while a step is being executed by `ralph run`.";

/// Stderr message for [`QuestionAskOutcome::NoActiveRun`] on `ralph block`.
pub const BLOCK_NO_ACTIVE_RUN_MESSAGE: &str = "ralph block: no active ralph run found for this project. This command only works while a step is being executed by `ralph run`.";

/// Stderr message for [`QuestionAskOutcome::Disabled`]. Tone is encouraging
/// rather than adversarial — the harness will see this in stderr and we want
/// it to fall back to "make a reasonable guess and flag it" rather than retry
/// in a loop. Verbatim from TUI-plan.md §17 "Behavior when questions are
/// disabled".
pub const DISABLED_MESSAGE: &str = "ralph question ask: questions are not enabled for this plan.

Continue with the work as best you can given the information you have.
Document any assumption you make in a comment near the relevant code so
the user can review and adjust. A reasonable guess that's clearly
flagged is preferable to halting; do not retry this command.

(If the user wants to enable questions, they can press `Q` on this plan
in the ralph TUI, or run `ralph plan questions on <slug>`.)";

/// Stderr message for [`QuestionAskOutcome::Disabled`] on `ralph block`.
pub const BLOCK_DISABLED_MESSAGE: &str =
    "ralph block: question/blocker interruptions are not enabled for this plan.

Do not continue past this blocker silently. Stop, surface the missing
prerequisite to the user, and explain that ralph's interruption feature
is disabled for this plan. Do not retry this command.

(If the user wants to enable interruptions, they can press `Q` on this
plan in the ralph TUI, or run `ralph plan questions on <slug>`.)";

/// The (step_id, attempt) an interruption-raising CLI call binds to, after
/// the run-lock + `questions_enabled` gate has passed.
struct BoundStep {
    step_id: String,
    attempt: i32,
}

/// Shared gate for `ralph question ask` / `ralph block` (docs/dag-redesign.md
/// §7 "harness protocol", preserving the pre-DAG `question ask` binding
/// model verbatim):
///
/// 1. Look up the run lock for `project`. Missing lock or NULL step_id ⇒
///    [`QuestionAskOutcome::NoActiveRun`].
/// 2. Read `step_id` and the current attempt from the lock row. The lock
///    column mirrors the in-flight `execution_logs.attempt`; a missing
///    attempt falls back to 1 (the runner's first-attempt behavior).
/// 3. Read `plans.questions_enabled` for that step's plan. If false, return
///    [`QuestionAskOutcome::Disabled`] **without touching the DB** — the
///    same guard, byte-identical, that the pre-DAG `question ask` enforced
///    for *both* questions and blockers.
///
/// `Ok(Ok(BoundStep))` means the gate passed; `Ok(Err(outcome))` is a
/// short-circuit the caller must return as-is.
fn resolve_bound_step(
    conn: &Connection,
    project: &str,
) -> Result<std::result::Result<BoundStep, QuestionAskOutcome>> {
    let live = match storage::get_live_run(conn, project)? {
        Some(l) => l,
        None => return Ok(Err(QuestionAskOutcome::NoActiveRun)),
    };

    let step_id = match live.step_id.as_deref() {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return Ok(Err(QuestionAskOutcome::NoActiveRun)),
    };

    let attempt = live.attempt.unwrap_or(1);

    // Look up the plan via the step (rather than `live.plan_id`) so a
    // bound-but-unsynced lock row in `--all` mode still works: the step row
    // is the source of truth for plan membership.
    let step = storage::get_step_by_id(conn, &step_id)?
        .with_context(|| format!("Step {step_id} from run lock not found"))?;

    let questions_enabled: bool = conn
        .query_row(
            "SELECT questions_enabled FROM plans WHERE id = ?1",
            params![&step.plan_id],
            |row| row.get::<_, i64>(0).map(|v| v != 0),
        )
        .with_context(|| format!("Plan {} not found for step {}", step.plan_id, step_id))?;

    if !questions_enabled {
        return Ok(Err(QuestionAskOutcome::Disabled));
    }

    Ok(Ok(BoundStep { step_id, attempt }))
}

/// Pair each suggestion with a priority (docs/dag-redesign.md §7): the k-th
/// `--priority` binds to the k-th suggestion by position; any suggestion
/// past the last supplied priority defaults to its 1-based append order
/// (so no `--priority` at all yields 1,2,3,… in listed order — the same
/// rule the V26 cutover used).
fn build_options(suggestions: &[String], priorities: &[i32]) -> Vec<InterruptionOption> {
    suggestions
        .iter()
        .enumerate()
        .map(|(i, text)| InterruptionOption {
            text: text.clone(),
            priority: priorities.get(i).copied().unwrap_or((i as i32) + 1),
        })
        .collect()
}

/// Shared body of `record_question_ask` / `record_block` — gates via
/// [`resolve_bound_step`], writes the open native `interruptions` row, and
/// emits the Phase E `InterruptionRaised` NDJSON event. Kept private so
/// the two public wrappers fix the `kind`/`options` shape (and keep
/// stable, harness-facing names + per-kind doc comments).
///
/// The two public wrappers used to be 95% identical clones; collapsing
/// them here keeps the gate + insert + NDJSON ordering in one place. The
/// `auto_raised: false` argument is fixed for both — the executor's
/// retry-exhausted auto-blocker (the only `true` writer) goes through
/// `executor::raise_retry_exhausted_blocker`, not this path.
fn record_interruption(
    conn: &Connection,
    project: &str,
    kind: InterruptionKind,
    body: &str,
    options: &[InterruptionOption],
    out: &OutputContext,
) -> Result<QuestionAskOutcome> {
    let bound = match resolve_bound_step(conn, project)? {
        Ok(b) => b,
        Err(outcome) => return Ok(outcome),
    };

    let id = storage::insert_interruption(
        conn,
        &bound.step_id,
        bound.attempt,
        kind,
        body,
        options,
    )
    .with_context(|| format!("inserting {} interruption", kind.as_str()))?;

    // Phase E Fix 4: harness-raised interruptions emit `InterruptionRaised`
    // (auto_raised=false) for observers wiring `--json` consumers. No-op
    // outside JSON mode and best-effort on slug-lookup failures.
    output::emit_interruption_raised(
        conn,
        out.format == OutputFormat::Json,
        &id,
        &bound.step_id,
        kind.as_str(),
        false,
        bound.attempt,
    );

    Ok(QuestionAskOutcome::Recorded {
        question_id: id,
        step_id: bound.step_id,
        attempt: bound.attempt,
    })
}

/// Implement `ralph question ask` against an open DB connection.
///
/// Gates via [`resolve_bound_step`] (the pre-DAG binding model + the
/// `questions_enabled` guard, unchanged), then inserts a fresh **open
/// native `interruptions` row** (`kind=question`, options synthesized from
/// `suggestions`/`priorities`). The orchestrator's cross-process bridge
/// observes the open row after the harness returns.
pub fn record_question_ask(
    conn: &Connection,
    project: &str,
    question: &str,
    suggestions: &[String],
    priorities: &[i32],
    out: &OutputContext,
) -> Result<QuestionAskOutcome> {
    let options = build_options(suggestions, priorities);
    record_interruption(conn, project, InterruptionKind::Question, question, &options, out)
}

/// Implement `ralph block` against an open DB connection.
///
/// Same gate as [`record_question_ask`] (a blocker is rejected outside a
/// run / when the feature is off, exactly like a question — preserving the
/// pre-DAG guard), then inserts a fresh **open native `interruptions` row**
/// (`kind=blocker`, no options). docs/dag-redesign.md §3.4/§7.
pub fn record_block(
    conn: &Connection,
    project: &str,
    body: &str,
    out: &OutputContext,
) -> Result<QuestionAskOutcome> {
    record_interruption(conn, project, InterruptionKind::Blocker, body, &[], out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::plan::{InterruptionKind, InterruptionState};
    use crate::storage;
    use rusqlite::params;

    /// Set up an in-memory DB with a plan + step and return their ids.
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

    /// Insert a run_locks row with the provided step_id + attempt. Bypasses
    /// `run_lock::acquire` because the test pid is the live one and acquire's
    /// liveness check would refuse to overwrite.
    fn seed_run_lock(
        conn: &Connection,
        project: &str,
        plan_id: &str,
        plan_slug: &str,
        step_id: &str,
        attempt: i32,
    ) {
        let pid = std::process::id() as i64;
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug, step_id, step_num, phase, attempt, max_attempts)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                project, pid, "test-token", plan_id, plan_slug, step_id, 1i32, "harness",
                attempt, 3i32,
            ],
        )
        .expect("seed run_locks");
    }

    fn quiet_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    fn json_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Json,
            quiet: true,
            color: false,
        }
    }

    fn open_count(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT COUNT(*) FROM interruptions WHERE state = 'open'",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    // -----------------------------------------------------------------
    // `ralph question ask` — native interruptions write + the preserved
    // run-lock / questions_enabled guard (STEP 21).
    // -----------------------------------------------------------------

    #[test]
    fn no_active_run_returns_no_active_run() {
        let conn = db::open_memory().unwrap();
        let _ = seed_plan_and_step(&conn, "p", "/proj-no-lock");

        let outcome = record_question_ask(
            &conn,
            "/proj-no-lock",
            "Q?",
            &["A".into()],
            &[],
            &quiet_out(),
        )
        .expect("ok");
        assert!(matches!(outcome, QuestionAskOutcome::NoActiveRun));
        assert_eq!(
            open_count(&conn),
            0,
            "must not write a row when there's no active run"
        );
    }

    #[test]
    fn questions_disabled_returns_disabled_with_no_db_write() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-disabled";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-disabled", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, false).unwrap();
        seed_run_lock(&conn, project, &plan_id, "p-disabled", &step_id, 2);

        let outcome = record_question_ask(
            &conn,
            project,
            "Should I do A or B?",
            &["A".into(), "B".into()],
            &[],
            &quiet_out(),
        )
        .expect("ok");
        assert!(matches!(outcome, QuestionAskOutcome::Disabled));
        assert_eq!(
            open_count(&conn),
            0,
            "questions disabled must not insert an interruption"
        );
    }

    #[test]
    fn block_is_rejected_outside_a_run_exactly_like_question() {
        // STEP 21: `ralph block` must hit the SAME guard as `question ask`.
        let conn = db::open_memory().unwrap();
        let _ = seed_plan_and_step(&conn, "p", "/proj-blk-no-lock");
        let outcome =
            record_block(&conn, "/proj-blk-no-lock", "needs sudo", &quiet_out()).expect("ok");
        assert!(matches!(outcome, QuestionAskOutcome::NoActiveRun));
        assert_eq!(open_count(&conn), 0);
    }

    #[test]
    fn block_is_rejected_when_questions_disabled() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-blk-disabled";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-blk", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, false).unwrap();
        seed_run_lock(&conn, project, &plan_id, "p-blk", &step_id, 1);
        let outcome = record_block(&conn, project, "needs access", &quiet_out()).expect("ok");
        assert!(matches!(outcome, QuestionAskOutcome::Disabled));
        assert_eq!(open_count(&conn), 0);
    }

    #[test]
    fn questions_enabled_inserts_native_question_interruption() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-enabled";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-enabled", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, true).unwrap();
        seed_run_lock(&conn, project, &plan_id, "p-enabled", &step_id, 2);

        let suggestions = vec!["Option A".to_string(), "Option B".to_string()];
        let outcome = record_question_ask(
            &conn,
            project,
            "What should I do?",
            &suggestions,
            &[],
            &quiet_out(),
        )
        .expect("ok");
        let (qid, recorded_step_id, attempt) = match outcome {
            QuestionAskOutcome::Recorded {
                question_id,
                step_id,
                attempt,
            } => (question_id, step_id, attempt),
            other => panic!("expected Recorded, got {other:?}"),
        };
        assert_eq!(recorded_step_id, step_id);
        assert_eq!(attempt, 2, "attempt must come from the run lock row");

        let rows = storage::list_interruptions_for_step(&conn, &step_id).unwrap();
        let q = rows.iter().find(|i| i.id == qid).unwrap();
        assert_eq!(q.kind, InterruptionKind::Question);
        assert_eq!(q.attempt, 2);
        assert_eq!(q.body, "What should I do?");
        assert_eq!(q.state, InterruptionState::Open);
        assert!(q.resolution.is_none(), "resolution must start None");
        assert!(q.resolved_at.is_none(), "resolved_at must start None");
        // No explicit priorities ⇒ ascending append order 1,2.
        assert_eq!(q.options.len(), 2);
        assert_eq!(q.options[0].text, "Option A");
        assert_eq!(q.options[0].priority, 1);
        assert_eq!(q.options[1].text, "Option B");
        assert_eq!(q.options[1].priority, 2);
    }

    #[test]
    fn ask_priorities_pair_by_position_then_default_append() {
        // STEP 21: k-th --priority binds to k-th -s; the rest default to
        // 1-based append order.
        let conn = db::open_memory().unwrap();
        let project = "/proj-prio";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-prio", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, true).unwrap();
        seed_run_lock(&conn, project, &plan_id, "p-prio", &step_id, 1);

        let suggestions = vec!["best".into(), "second".into(), "third".into()];
        // Only two priorities supplied for three suggestions.
        let outcome = record_question_ask(
            &conn,
            project,
            "Which?",
            &suggestions,
            &[10, 20],
            &quiet_out(),
        )
        .expect("ok");
        let qid = match outcome {
            QuestionAskOutcome::Recorded { question_id, .. } => question_id,
            other => panic!("expected Recorded, got {other:?}"),
        };
        let rows = storage::list_interruptions_for_step(&conn, &step_id).unwrap();
        let q = rows.iter().find(|i| i.id == qid).unwrap();
        assert_eq!(q.options[0].priority, 10);
        assert_eq!(q.options[1].priority, 20);
        assert_eq!(
            q.options[2].priority, 3,
            "suggestion past the last --priority defaults to append order (index+1)"
        );
    }

    #[test]
    fn block_inserts_native_blocker_with_no_options() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-block";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-block", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, true).unwrap();
        seed_run_lock(&conn, project, &plan_id, "p-block", &step_id, 3);

        let outcome =
            record_block(&conn, project, "Cannot apt-get without sudo", &quiet_out()).expect("ok");
        let qid = match outcome {
            QuestionAskOutcome::Recorded { question_id, .. } => question_id,
            other => panic!("expected Recorded, got {other:?}"),
        };
        let rows = storage::list_interruptions_for_step(&conn, &step_id).unwrap();
        let b = rows.iter().find(|i| i.id == qid).unwrap();
        assert_eq!(b.kind, InterruptionKind::Blocker);
        assert_eq!(b.attempt, 3);
        assert_eq!(b.body, "Cannot apt-get without sudo");
        assert!(b.options.is_empty(), "blockers carry no options");
        assert_eq!(b.state, InterruptionState::Open);
    }

    #[test]
    fn lock_row_with_null_step_id_returns_no_active_run() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-unbound";
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token) VALUES (?1, ?2, ?3)",
            params![project, std::process::id() as i64, "test-token"],
        )
        .unwrap();

        let outcome =
            record_question_ask(&conn, project, "Q?", &[], &[], &quiet_out()).expect("ok");
        assert!(matches!(outcome, QuestionAskOutcome::NoActiveRun));
        // The block verb shares the gate.
        let outcome = record_block(&conn, project, "blk", &quiet_out()).expect("ok");
        assert!(matches!(outcome, QuestionAskOutcome::NoActiveRun));
    }

    #[test]
    fn empty_suggestions_round_trip_as_empty_options() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-no-suggestions";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-empty", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, true).unwrap();
        seed_run_lock(&conn, project, &plan_id, "p-empty", &step_id, 1);

        let outcome = record_question_ask(
            &conn,
            project,
            "What should we name X?",
            &[],
            &[],
            &quiet_out(),
        )
        .expect("ok");
        let qid = match outcome {
            QuestionAskOutcome::Recorded { question_id, .. } => question_id,
            other => panic!("expected Recorded, got {other:?}"),
        };
        let rows = storage::list_interruptions_for_step(&conn, &step_id).unwrap();
        let q = rows.iter().find(|i| i.id == qid).unwrap();
        assert!(q.options.is_empty());
    }

    #[test]
    fn missing_attempt_on_lock_row_defaults_to_one() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-no-attempt";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-na", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, true).unwrap();
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token, plan_id, plan_slug, step_id)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                project,
                std::process::id() as i64,
                "tok",
                plan_id,
                "p-na",
                step_id,
            ],
        )
        .unwrap();

        let outcome =
            record_question_ask(&conn, project, "Q?", &[], &[], &quiet_out()).expect("ok");
        let attempt = match outcome {
            QuestionAskOutcome::Recorded { attempt, .. } => attempt,
            other => panic!("expected Recorded, got {other:?}"),
        };
        assert_eq!(attempt, 1);
    }

    // -----------------------------------------------------------------
    // Phase E Fix 4: NDJSON emission paths must not panic / leak DB state
    // -----------------------------------------------------------------
    //
    // The NDJSON `InterruptionRaised` event is best-effort and writes to
    // process stdout, so we can't observe it from a unit test directly.
    // We instead pin three contract properties:
    //   1. JSON-mode `record_question_ask` still records the row (the
    //      event emission is purely additive — no inserts dropped).
    //   2. JSON-mode `record_block` still records the row (same).
    //   3. Both paths set `kind` correctly inside the DB so the event we
    //      would emit reflects the actual state — the `kind` argument
    //      threading is what would silently break.

    #[test]
    fn record_question_ask_in_json_mode_still_inserts_question_row() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-json-q";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-json-q", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, true).unwrap();
        seed_run_lock(&conn, project, &plan_id, "p-json-q", &step_id, 1);

        let outcome =
            record_question_ask(&conn, project, "json-q?", &[], &[], &json_out()).expect("ok");
        let qid = match outcome {
            QuestionAskOutcome::Recorded { question_id, .. } => question_id,
            other => panic!("expected Recorded, got {other:?}"),
        };
        let rows = storage::list_interruptions_for_step(&conn, &step_id).unwrap();
        let q = rows.iter().find(|i| i.id == qid).unwrap();
        assert_eq!(q.kind, InterruptionKind::Question);
    }

    #[test]
    fn record_block_in_json_mode_still_inserts_blocker_row() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-json-b";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-json-b", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, true).unwrap();
        seed_run_lock(&conn, project, &plan_id, "p-json-b", &step_id, 2);

        let outcome = record_block(&conn, project, "json-blk", &json_out()).expect("ok");
        let bid = match outcome {
            QuestionAskOutcome::Recorded { question_id, .. } => question_id,
            other => panic!("expected Recorded, got {other:?}"),
        };
        let rows = storage::list_interruptions_for_step(&conn, &step_id).unwrap();
        let b = rows.iter().find(|i| i.id == bid).unwrap();
        assert_eq!(b.kind, InterruptionKind::Blocker);
    }
}
