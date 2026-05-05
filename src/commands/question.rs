// `ralph question ask` CLI implementation.
//
// Designed for the harness to call mid-step. Binds the question to the
// currently-executing step via the project's run lock — no env vars or
// harness-side context plumbing required. See TUI-plan.md §17 for the full
// design.

use anyhow::{Context, Result, bail};
use rusqlite::{Connection, params};
use uuid::Uuid;

use crate::output::{OutputContext, OutputFormat};
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

// ---------------------------------------------------------------------------
// `ralph question list`
// ---------------------------------------------------------------------------

/// Implementation of `ralph question list [<plan-slug>]`.
///
/// Prints the project's open questions, numbered 1..N. Those numbers are the
/// indices accepted by [`cmd_question_answer`] and [`cmd_question_show`].
/// `--json` emits an array of objects so meta-harnesses can parse the list.
pub fn cmd_question_list(
    conn: &Connection,
    project: &str,
    plan_slug: Option<&str>,
    out: &OutputContext,
) -> Result<()> {
    let questions = storage::list_open_questions(conn, project, plan_slug)?;

    if out.format == OutputFormat::Json {
        let arr: Vec<serde_json::Value> = questions
            .iter()
            .enumerate()
            .map(|(i, q)| {
                serde_json::json!({
                    "index": i + 1,
                    "id": q.id,
                    "plan_slug": q.plan_slug,
                    "step_num": q.step_num,
                    "step_title": q.step_title,
                    "attempt": q.attempt,
                    "question": q.question,
                    "suggestions": q.suggestions,
                    "asked_at": q.asked_at,
                })
            })
            .collect();
        println!("{}", serde_json::to_string(&arr)?);
        return Ok(());
    }

    if questions.is_empty() {
        if let Some(slug) = plan_slug {
            println!("No open questions on plan '{slug}'.");
        } else {
            println!("No open questions.");
        }
        return Ok(());
    }

    for (i, q) in questions.iter().enumerate() {
        let context = format!(
            "{} step {} ({})",
            q.plan_slug,
            q.step_num,
            q.step_title.trim()
        );
        let single_line = q.question.trim().lines().next().unwrap_or("").to_string();
        println!("[{}] {context}", i + 1);
        println!("    {single_line}");
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `ralph question answer`
// ---------------------------------------------------------------------------

/// Implementation of `ralph question answer <num> <text>`.
///
/// `num` is the 1-based index from the most recent `ralph question list` for
/// the same project; this function recomputes that list to resolve the index
/// to a question id, then writes the answer.
pub fn cmd_question_answer(
    conn: &Connection,
    project: &str,
    num: usize,
    text: &str,
    out: &OutputContext,
) -> Result<()> {
    let questions = storage::list_open_questions(conn, project, None)?;
    if questions.is_empty() {
        bail!("No open questions to answer.");
    }
    if num == 0 || num > questions.len() {
        bail!(
            "Question index {num} out of range (project has {} open question{}).",
            questions.len(),
            if questions.len() == 1 { "" } else { "s" }
        );
    }
    let q = &questions[num - 1];
    storage::set_question_answer(conn, &q.id, text)?;

    if out.format == OutputFormat::Json {
        let json = serde_json::json!({
            "answered": true,
            "index": num,
            "id": q.id,
            "plan_slug": q.plan_slug,
            "step_num": q.step_num,
        });
        println!("{}", serde_json::to_string(&json)?);
    } else {
        println!(
            "Answered question [{num}] on plan '{}' step {}.",
            q.plan_slug, q.step_num
        );
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `ralph question show`
// ---------------------------------------------------------------------------

/// Implementation of `ralph question show <num>`.
///
/// Prints the full question text plus any harness-supplied suggestions.
pub fn cmd_question_show(
    conn: &Connection,
    project: &str,
    num: usize,
    out: &OutputContext,
) -> Result<()> {
    let questions = storage::list_open_questions(conn, project, None)?;
    if questions.is_empty() {
        bail!("No open questions.");
    }
    if num == 0 || num > questions.len() {
        bail!(
            "Question index {num} out of range (project has {} open question{}).",
            questions.len(),
            if questions.len() == 1 { "" } else { "s" }
        );
    }
    let q = &questions[num - 1];

    if out.format == OutputFormat::Json {
        let json = serde_json::json!({
            "index": num,
            "id": q.id,
            "plan_slug": q.plan_slug,
            "step_num": q.step_num,
            "step_title": q.step_title,
            "attempt": q.attempt,
            "question": q.question,
            "suggestions": q.suggestions,
            "asked_at": q.asked_at,
        });
        println!("{}", serde_json::to_string(&json)?);
        return Ok(());
    }

    println!(
        "[{num}] {} step {} ({})",
        q.plan_slug,
        q.step_num,
        q.step_title.trim()
    );
    println!("Asked: {} (attempt {})", q.asked_at, q.attempt);
    println!();
    println!("{}", q.question);
    if !q.suggestions.is_empty() {
        println!();
        println!("Suggestions:");
        for s in &q.suggestions {
            println!("  - {s}");
        }
    }
    Ok(())
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

    // -----------------------------------------------------------------
    // helpers for list/answer/show tests
    // -----------------------------------------------------------------

    fn quiet_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    /// Insert a `step_questions` row with an explicit `asked_at` so tests
    /// control the list ordering deterministically (the production code
    /// stamps via `strftime('now')` which would collapse adjacent calls).
    fn insert_question(
        conn: &Connection,
        id: &str,
        step_id: &str,
        attempt: i32,
        question: &str,
        suggestions: &[&str],
        asked_at: &str,
    ) {
        let suggestions_json =
            serde_json::to_string(&suggestions.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                .unwrap();
        conn.execute(
            "INSERT INTO step_questions (id, step_id, attempt, question, suggestions, asked_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![id, step_id, attempt, question, suggestions_json, asked_at],
        )
        .expect("insert step_questions");
    }

    // -----------------------------------------------------------------
    // `ralph question list`
    // -----------------------------------------------------------------

    #[test]
    fn list_returns_open_questions_sorted_by_asked_at() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-list";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p-list", project);

        // Out-of-order inserts but sorted asked_at — list output must be
        // ordered by asked_at, not insert order.
        insert_question(
            &conn,
            "q2",
            &step_id,
            1,
            "second",
            &[],
            "2026-01-02T00:00:00.000Z",
        );
        insert_question(
            &conn,
            "q1",
            &step_id,
            1,
            "first",
            &["A", "B"],
            "2026-01-01T00:00:00.000Z",
        );

        let qs = storage::list_open_questions(&conn, project, None).unwrap();
        assert_eq!(qs.len(), 2);
        assert_eq!(qs[0].id, "q1");
        assert_eq!(qs[0].question, "first");
        assert_eq!(qs[0].suggestions, vec!["A".to_string(), "B".to_string()]);
        assert_eq!(qs[0].plan_slug, "p-list");
        assert_eq!(qs[0].step_num, 1);
        assert_eq!(qs[1].id, "q2");

        // The command itself doesn't error on empty either; smoke test it.
        cmd_question_list(&conn, project, None, &quiet_out()).unwrap();
    }

    #[test]
    fn list_excludes_answered_questions() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-answered";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p-answered", project);

        insert_question(
            &conn,
            "q-open",
            &step_id,
            1,
            "open question",
            &[],
            "2026-01-01T00:00:00.000Z",
        );
        insert_question(
            &conn,
            "q-answered",
            &step_id,
            1,
            "already answered",
            &[],
            "2026-01-02T00:00:00.000Z",
        );
        // Mark q-answered as answered. Bypasses set_question_answer so we
        // exercise the storage filter directly.
        conn.execute(
            "UPDATE step_questions SET answer = 'done', answered_at = '2026-01-02T01:00:00.000Z'
             WHERE id = 'q-answered'",
            [],
        )
        .unwrap();

        let qs = storage::list_open_questions(&conn, project, None).unwrap();
        assert_eq!(qs.len(), 1, "answered rows must be filtered out");
        assert_eq!(qs[0].id, "q-open");
    }

    #[test]
    fn list_filters_by_plan_slug() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-multi";
        let (_a_id, a_step) = seed_plan_and_step(&conn, "plan-a", project);
        let (_b_id, b_step) = seed_plan_and_step(&conn, "plan-b", project);

        insert_question(
            &conn,
            "qa",
            &a_step,
            1,
            "from a",
            &[],
            "2026-01-01T00:00:00.000Z",
        );
        insert_question(
            &conn,
            "qb",
            &b_step,
            1,
            "from b",
            &[],
            "2026-01-02T00:00:00.000Z",
        );

        let all = storage::list_open_questions(&conn, project, None).unwrap();
        assert_eq!(all.len(), 2, "without filter, both plans' questions appear");

        let only_a = storage::list_open_questions(&conn, project, Some("plan-a")).unwrap();
        assert_eq!(only_a.len(), 1);
        assert_eq!(only_a[0].plan_slug, "plan-a");
    }

    #[test]
    fn list_excludes_other_projects() {
        let conn = db::open_memory().unwrap();
        let (_p1, s1) = seed_plan_and_step(&conn, "p1", "/proj-1");
        let (_p2, s2) = seed_plan_and_step(&conn, "p2", "/proj-2");

        insert_question(&conn, "q1", &s1, 1, "in 1", &[], "2026-01-01T00:00:00.000Z");
        insert_question(&conn, "q2", &s2, 1, "in 2", &[], "2026-01-02T00:00:00.000Z");

        let only_proj_1 = storage::list_open_questions(&conn, "/proj-1", None).unwrap();
        assert_eq!(only_proj_1.len(), 1);
        assert_eq!(only_proj_1[0].id, "q1");
    }

    #[test]
    fn list_step_num_matches_step_list_position() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-stepnum";
        let plan = storage::create_plan(&conn, "p-multi", project, "br", "d", None, None, &[])
            .expect("create_plan");
        let (s1, _) = storage::create_step(
            &conn, &plan.id, "first", "d", None, None, &[], None, None, None, None,
        )
        .unwrap();
        let (s2, _) = storage::create_step(
            &conn, &plan.id, "second", "d", None, None, &[], None, None, None, None,
        )
        .unwrap();

        insert_question(
            &conn,
            "q-on-2",
            &s2.id,
            1,
            "on second step",
            &[],
            "2026-01-02T00:00:00.000Z",
        );
        insert_question(
            &conn,
            "q-on-1",
            &s1.id,
            1,
            "on first step",
            &[],
            "2026-01-01T00:00:00.000Z",
        );

        let qs = storage::list_open_questions(&conn, project, None).unwrap();
        let by_id: std::collections::HashMap<&str, &storage::OpenQuestion> =
            qs.iter().map(|q| (q.id.as_str(), q)).collect();
        assert_eq!(by_id["q-on-1"].step_num, 1, "first step is num 1");
        assert_eq!(by_id["q-on-2"].step_num, 2, "second step is num 2");
    }

    // -----------------------------------------------------------------
    // `ralph question answer`
    // -----------------------------------------------------------------

    #[test]
    fn answer_writes_answer_and_answered_at_for_index() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-ans";
        let (_plan_id, step_id) = seed_plan_and_step(&conn, "p-ans", project);
        insert_question(
            &conn,
            "qX",
            &step_id,
            1,
            "the q",
            &["s1"],
            "2026-01-01T00:00:00.000Z",
        );

        cmd_question_answer(&conn, project, 1, "the answer", &quiet_out()).unwrap();

        let (answer, answered_at): (Option<String>, Option<String>) = conn
            .query_row(
                "SELECT answer, answered_at FROM step_questions WHERE id = 'qX'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(answer.as_deref(), Some("the answer"));
        assert!(
            answered_at.is_some(),
            "answered_at must be stamped on answer"
        );

        // Subsequent list must hide the now-answered row.
        let after = storage::list_open_questions(&conn, project, None).unwrap();
        assert!(after.is_empty(), "answered row drops out of the open list");
    }

    #[test]
    fn answer_out_of_range_index_errors_without_writing() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-oob";
        let (_p, step_id) = seed_plan_and_step(&conn, "p-oob", project);
        insert_question(
            &conn,
            "q-only",
            &step_id,
            1,
            "only",
            &[],
            "2026-01-01T00:00:00.000Z",
        );

        // 0 is invalid (1-based index).
        let err =
            cmd_question_answer(&conn, project, 0, "x", &quiet_out()).expect_err("out of range");
        assert!(err.to_string().contains("out of range"));

        // Beyond the end is invalid.
        let err =
            cmd_question_answer(&conn, project, 99, "x", &quiet_out()).expect_err("out of range");
        assert!(err.to_string().contains("out of range"));

        // The lone open question must be untouched.
        let answer: Option<String> = conn
            .query_row(
                "SELECT answer FROM step_questions WHERE id = 'q-only'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(answer.is_none(), "no DB write on out-of-range index");
    }

    #[test]
    fn answer_when_no_open_questions_errors_clearly() {
        let conn = db::open_memory().unwrap();
        // No questions inserted at all.
        let err = cmd_question_answer(&conn, "/proj-empty", 1, "x", &quiet_out())
            .expect_err("must error when list is empty");
        assert!(err.to_string().contains("No open questions"));
    }

    // -----------------------------------------------------------------
    // `ralph question show`
    // -----------------------------------------------------------------

    #[test]
    fn show_runs_for_valid_index() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-show";
        let (_p, step_id) = seed_plan_and_step(&conn, "p-show", project);
        insert_question(
            &conn,
            "q-show",
            &step_id,
            2,
            "What's the right approach?",
            &["Option A", "Option B"],
            "2026-01-01T00:00:00.000Z",
        );

        // Smoke-test: the command runs without error and the row is still
        // open (show is read-only).
        cmd_question_show(&conn, project, 1, &quiet_out()).unwrap();
        let still_open = storage::list_open_questions(&conn, project, None).unwrap();
        assert_eq!(still_open.len(), 1, "show must not mutate the row");
    }

    #[test]
    fn show_out_of_range_errors() {
        let conn = db::open_memory().unwrap();
        let project = "/proj-show-oob";
        let (_p, step_id) = seed_plan_and_step(&conn, "p-show-oob", project);
        insert_question(
            &conn,
            "q1",
            &step_id,
            1,
            "Q?",
            &[],
            "2026-01-01T00:00:00.000Z",
        );

        let err =
            cmd_question_show(&conn, project, 5, &quiet_out()).expect_err("out of range");
        assert!(err.to_string().contains("out of range"));
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
