// `ralph interruption list/show/resolve` — the human-side CLI for the
// unified interruption model (docs/dag-redesign.md §7). The TUI inbox is the
// primary path; this is the scriptable equivalent. All I/O is native against
// the `interruptions` table (no `step_questions`).

use anyhow::{Result, bail};
use rusqlite::Connection;

use crate::output::{OutputContext, OutputFormat};
use crate::plan::InterruptionKind;
use crate::storage::{self, OpenQuestion};

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

    storage::resolve_interruption(conn, &q.id, &resolution, comment)?;

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
}
