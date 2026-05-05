// `ralph question ask` CLI implementation.
//
// Designed for the harness to call mid-step. Binds the question to the
// currently-executing step via the project's run lock — no env vars or
// harness-side context plumbing required. See TUI-plan.md §17 for the full
// design.

use anyhow::{Context, Result};
use rusqlite::{Connection, params};
use uuid::Uuid;

use crate::storage;

/// Outcome of [`record_question_ask`]. The dispatcher in main.rs maps each
/// variant to the appropriate stderr message + exit code; the recorder itself
/// stays free of process-exit and printing concerns so it's straightforward to
/// unit-test.
#[derive(Debug)]
pub enum QuestionAskOutcome {
    /// Successfully wrote a `step_questions` row. Fields are returned for
    /// tests and forthcoming runner integration (TUI-plan.md §17 step 42)
    /// — main.rs's dispatcher currently destructures with `..` because the
    /// CLI itself prints nothing on success.
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

/// Implement `ralph question ask` against an open DB connection.
///
/// Resolution order matches TUI-plan.md §17 "Binding model":
/// 1. Look up the run lock for `project`. Missing lock or NULL step_id ⇒
///    [`QuestionAskOutcome::NoActiveRun`].
/// 2. Read `step_id` and the current attempt from the lock row. The lock
///    column already mirrors the in-flight `execution_logs.attempt`, so no
///    extra query is needed; a missing attempt falls back to 1 (matches the
///    runner's first-attempt behavior).
/// 3. Read `plans.questions_enabled` for that step's plan. If false, return
///    [`QuestionAskOutcome::Disabled`] without touching the DB.
/// 4. Otherwise insert a fresh `step_questions` row (uuid v4 id, suggestions
///    serialized as a JSON string array, `asked_at` = SQLite `now()`).
pub fn record_question_ask(
    conn: &Connection,
    project: &str,
    question: &str,
    suggestions: &[String],
) -> Result<QuestionAskOutcome> {
    let live = storage::get_live_run(conn, project)?;
    let live = match live {
        Some(l) => l,
        None => return Ok(QuestionAskOutcome::NoActiveRun),
    };

    let step_id = match live.step_id.as_deref() {
        Some(s) if !s.is_empty() => s.to_string(),
        _ => return Ok(QuestionAskOutcome::NoActiveRun),
    };

    // run_locks.attempt mirrors the in-flight execution_logs.attempt and is
    // written by every executor phase transition. If the lock predates that
    // bookkeeping (or a hook ran before the executor's first phase write),
    // attempt 1 is the correct default — the harness is on its first attempt
    // when there's no recorded attempt yet.
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
        return Ok(QuestionAskOutcome::Disabled);
    }

    let id = Uuid::new_v4().to_string();
    let suggestions_json = serde_json::to_string(suggestions)?;
    conn.execute(
        "INSERT INTO step_questions (id, step_id, attempt, question, suggestions, asked_at)
         VALUES (?1, ?2, ?3, ?4, ?5, strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))",
        params![id, step_id, attempt, question, suggestions_json],
    )
    .context("inserting step_questions row")?;

    Ok(QuestionAskOutcome::Recorded {
        question_id: id,
        step_id,
        attempt,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::storage;
    use rusqlite::params;

    /// Set up an in-memory DB with a plan + step and return their ids.
    /// Mirrors the seed pattern used by `cancel_tests` in commands/run.rs.
    fn seed_plan_and_step(conn: &Connection, slug: &str, project: &str) -> (String, String) {
        let plan = storage::create_plan(conn, slug, project, "br", "desc", None, None, &[])
            .expect("create_plan");
        let (step, _) = storage::create_step(
            conn, &plan.id, "title", "desc", None, None, &[], None, None, None, None,
        )
        .expect("create_step");
        (plan.id, step.id)
    }

    /// Insert a run_locks row with the provided step_id + attempt. Bypasses
    /// `run_lock::acquire` because the test pid is the live one and acquire's
    /// liveness check would refuse to overwrite. Mirrors the seed pattern in
    /// `cancel_tests`.
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
                project,
                pid,
                "test-token",
                plan_id,
                plan_slug,
                step_id,
                1i32,
                "harness",
                attempt,
                3i32,
            ],
        )
        .expect("seed run_locks");
    }

    #[test]
    fn no_active_run_returns_no_active_run() {
        let conn = db::open_memory().unwrap();
        // Plan/step exist, but no run_locks row → "no active run".
        let _ = seed_plan_and_step(&conn, "p", "/proj-no-lock");

        let outcome =
            record_question_ask(&conn, "/proj-no-lock", "Q?", &["A".into()]).expect("ok");
        assert!(matches!(outcome, QuestionAskOutcome::NoActiveRun));

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM step_questions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "must not write a row when there's no active run");
    }

    #[test]
    fn questions_disabled_returns_disabled_with_no_db_write() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-disabled";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-disabled", project);
        // Default `questions_enabled = false` (V16 default).
        seed_run_lock(&conn, project, &plan_id, "p-disabled", &step_id, 2);

        let outcome = record_question_ask(
            &conn,
            project,
            "Should I do A or B?",
            &["A".into(), "B".into()],
        )
        .expect("ok");
        assert!(matches!(outcome, QuestionAskOutcome::Disabled));

        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM step_questions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            count, 0,
            "questions disabled must not insert a step_questions row"
        );
    }

    #[test]
    fn questions_enabled_inserts_step_questions_row() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-enabled";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-enabled", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, true).unwrap();
        seed_run_lock(&conn, project, &plan_id, "p-enabled", &step_id, 2);

        let suggestions = vec!["Option A".to_string(), "Option B".to_string()];
        let outcome =
            record_question_ask(&conn, project, "What should I do?", &suggestions).expect("ok");
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

        // Row exists with the expected fields, including NULL answer/answered_at.
        let (
            row_step_id,
            row_attempt,
            row_question,
            row_suggestions_json,
            row_answer,
            row_answered_at,
        ): (
            String,
            i32,
            String,
            String,
            Option<String>,
            Option<String>,
        ) = conn
            .query_row(
                "SELECT step_id, attempt, question, suggestions, answer, answered_at
                 FROM step_questions WHERE id = ?1",
                params![&qid],
                |r| {
                    Ok((
                        r.get(0)?,
                        r.get(1)?,
                        r.get(2)?,
                        r.get(3)?,
                        r.get(4)?,
                        r.get(5)?,
                    ))
                },
            )
            .unwrap();

        assert_eq!(row_step_id, step_id);
        assert_eq!(row_attempt, 2);
        assert_eq!(row_question, "What should I do?");
        let parsed_suggestions: Vec<String> = serde_json::from_str(&row_suggestions_json).unwrap();
        assert_eq!(parsed_suggestions, suggestions);
        assert!(row_answer.is_none(), "answer must start NULL");
        assert!(row_answered_at.is_none(), "answered_at must start NULL");
    }

    #[test]
    fn lock_row_with_null_step_id_returns_no_active_run() {
        // `--all` mode acquires the lock before binding to a plan/step. A
        // run lock with step_id = NULL means no step is currently executing,
        // so there's nothing to bind a question to.
        let conn = db::open_memory().unwrap();
        let project = "/proj-unbound";
        conn.execute(
            "INSERT INTO run_locks (project, pid, pid_start_token) VALUES (?1, ?2, ?3)",
            params![project, std::process::id() as i64, "test-token"],
        )
        .unwrap();

        let outcome = record_question_ask(&conn, project, "Q?", &[]).expect("ok");
        assert!(matches!(outcome, QuestionAskOutcome::NoActiveRun));
    }

    #[test]
    fn empty_suggestions_round_trip_as_empty_json_array() {
        // `-s` is repeatable but optional — the open-ended question case must
        // store an empty JSON array, matching the column's DEFAULT '[]'.
        let conn = db::open_memory().unwrap();
        let project = "/proj-no-suggestions";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-empty", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, true).unwrap();
        seed_run_lock(&conn, project, &plan_id, "p-empty", &step_id, 1);

        let outcome =
            record_question_ask(&conn, project, "What should we name X?", &[]).expect("ok");
        let qid = match outcome {
            QuestionAskOutcome::Recorded { question_id, .. } => question_id,
            other => panic!("expected Recorded, got {other:?}"),
        };

        let suggestions_json: String = conn
            .query_row(
                "SELECT suggestions FROM step_questions WHERE id = ?1",
                params![&qid],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(suggestions_json, "[]");
    }

    #[test]
    fn missing_attempt_on_lock_row_defaults_to_one() {
        // A lock row without an attempt column populated (e.g. taken between
        // acquire and the executor's first phase write) should still record a
        // question, defaulting attempt to 1 to match the runner's first-pass
        // behavior.
        let conn = db::open_memory().unwrap();
        let project = "/proj-no-attempt";
        let (plan_id, step_id) = seed_plan_and_step(&conn, "p-na", project);
        storage::set_plan_questions_enabled(&conn, &plan_id, true).unwrap();
        // Manually insert a lock row with NULL attempt.
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

        let outcome = record_question_ask(&conn, project, "Q?", &[]).expect("ok");
        let attempt = match outcome {
            QuestionAskOutcome::Recorded { attempt, .. } => attempt,
            other => panic!("expected Recorded, got {other:?}"),
        };
        assert_eq!(attempt, 1);
    }
}
