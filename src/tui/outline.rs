// Outline projection — pure DAG → topologically-ordered indented outline.
//
// docs/dag-redesign.md §12.1: the Phase-4 plan-detail view replaces the flat
// step list with an outline that is topologically ordered, indented by
// dependency depth, lists a join step's dependencies inline by short id, and
// marks reviewer-inserted corrective steps with `↳ corrects <short_id>`.
//
// This module is the *data model* for that view: a pure, fully
// unit-testable projection with **no rendering and no TUI wiring** (the
// outline view itself is a later step). Keeping it pure means transitions
// and ordering can be tested without spinning up a terminal — the project's
// hard invariant.
//
// Ordering is NOT redefined here. The single source of truth for scheduler
// order is `runner::step_schedule_cmp` over `runner::compute_step_depths`
// (the `(topological depth, sort_key, short_id)` tie-break of
// docs/dag-redesign.md §3.5). This module *calls* both so the drawn outline
// is, row-for-row, the execution order — there is deliberately no second,
// divergent sort.

use std::collections::HashMap;

use crate::plan::Step;
use crate::runner::{compute_step_depths, step_schedule_cmp};

/// One row of the projected outline.
///
/// (No `PartialEq` — the core [`Step`] model intentionally does not derive
/// it, and widening that model just for outline assertions would be out of
/// scope. Tests compare the projection's own fields — short_id, depth,
/// join_deps, corrects_short_id — which is all this projection promises.)
#[derive(Debug, Clone)]
pub struct OutlineEntry {
    /// The step this row represents.
    pub step: Step,
    /// Indent level = the step's topological depth in the DAG
    /// (`runner::compute_step_depths`): `0` for a root, `1 + max(dep
    /// depth)` otherwise. Drives how far the row is indented.
    pub depth: usize,
    /// For a **join** step (more than one dependency), the `short_id` of
    /// *every* dependency, in the same deterministic order the scheduler
    /// would consider them (`step_schedule_cmp`), for inline
    /// `deps: a1b2, c3d4` rendering. Empty for a root or a single-parent
    /// step (those need no inline dependency list — the indent + the
    /// immediately-preceding parent row already convey the edge).
    pub join_deps: Vec<String>,
    /// For a reviewer-inserted corrective step (§10,
    /// `Step::corrects_step_id` set), the `short_id` of the step it
    /// corrects, for the `↳ corrects <short_id>` marker. `None` for an
    /// ordinary step, or if the corrected step is not in the projected
    /// set (defensive — `ON DELETE CASCADE` makes a dangling pointer
    /// unreachable from the DB).
    pub corrects_short_id: Option<String>,
}

impl OutlineEntry {
    /// Whether this row is a join (multi-dependency) step — i.e. it should
    /// render an inline `deps: …` list.
    pub fn is_join(&self) -> bool {
        self.join_deps.len() > 1
    }
}

/// Project a plan's step set + dependency adjacency into a topologically
/// ordered, depth-indexed outline (docs/dag-redesign.md §12.1).
///
/// - `steps` is the plan's full step set (any order).
/// - `deps_of` maps `step_id -> [depends_on_step_id, ...]` exactly as
///   `storage::list_step_dependency_edges` returns it (a missing key means
///   "no dependencies"); pass the same map the runner schedules on so the
///   outline cannot diverge from execution order.
///
/// The returned `Vec` is ordered by `runner::step_schedule_cmp` over
/// `runner::compute_step_depths` — identical to the scheduler's runnable
/// tie-break — so outline row order == execution order. A linear (chain)
/// plan therefore projects to exactly its authored order with each row one
/// indent deeper than the last, and a multi-root plan interleaves roots by
/// `sort_key` then `short_id` exactly as the scheduler would pick them.
pub fn project_outline(
    steps: &[Step],
    deps_of: &HashMap<String, Vec<String>>,
) -> Vec<OutlineEntry> {
    let depths = compute_step_depths(steps, deps_of);

    // id -> short_id, for resolving dependency / corrects pointers (both
    // stored as internal UUIDs) into the user-facing short handle.
    let short_by_id: HashMap<&str, &str> = steps
        .iter()
        .map(|s| (s.id.as_str(), s.short_id.as_str()))
        .collect();
    // id -> Step, so dependency short_ids can be ordered by the SAME
    // scheduler comparator (no divergent sort for the inline deps list).
    let step_by_id: HashMap<&str, &Step> =
        steps.iter().map(|s| (s.id.as_str(), s)).collect();

    let mut ordered: Vec<&Step> = steps.iter().collect();
    ordered.sort_by(|a, b| step_schedule_cmp(a, b, &depths));

    ordered
        .into_iter()
        .map(|step| {
            let depth = depths.get(&step.id).copied().unwrap_or(0);

            // Inline dependency list — only for a genuine join (>1 dep).
            let mut deps: Vec<&Step> = deps_of
                .get(&step.id)
                .map(|ids| {
                    ids.iter()
                        .filter_map(|d| step_by_id.get(d.as_str()).copied())
                        .collect()
                })
                .unwrap_or_default();
            let join_deps = if deps.len() > 1 {
                // Same deterministic ordering the scheduler would use.
                deps.sort_by(|a, b| step_schedule_cmp(a, b, &depths));
                deps.into_iter().map(|d| d.short_id.clone()).collect()
            } else {
                Vec::new()
            };

            let corrects_short_id = step
                .corrects_step_id
                .as_deref()
                .and_then(|cid| short_by_id.get(cid).map(|s| s.to_string()));

            OutlineEntry {
                step: step.clone(),
                depth,
                join_deps,
                corrects_short_id,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{Step, StepStatus};
    use chrono::Utc;

    /// Minimal `Step` builder — `short_id` doubles as the test handle so
    /// assertions read cleanly.
    fn step(short_id: &str, sort_key: &str) -> Step {
        Step {
            id: format!("uuid-{short_id}"),
            short_id: short_id.to_string(),
            plan_id: "p1".to_string(),
            sort_key: sort_key.to_string(),
            title: format!("step {short_id}"),
            description: String::new(),
            agent: None,
            harness: None,
            acceptance_criteria: vec![],
            status: StepStatus::Pending,
            attempts: 0,
            max_retries: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
            model: None,
            skipped_reason: None,
            change_policy: Default::default(),
            tags: vec![],
            retry_strategy: None,
            review_enabled: None,
            review_status: None,
            corrects_step_id: None,
        }
    }

    fn ids(out: &[OutlineEntry]) -> Vec<&str> {
        out.iter().map(|e| e.step.short_id.as_str()).collect()
    }

    /// Edge helper: `step short_id` depends on each of `deps` short_ids.
    fn edge(
        deps_of: &mut HashMap<String, Vec<String>>,
        step_short: &str,
        deps: &[&str],
    ) {
        deps_of.insert(
            format!("uuid-{step_short}"),
            deps.iter().map(|d| format!("uuid-{d}")).collect(),
        );
    }

    #[test]
    fn linear_chain_projects_in_authored_order_with_increasing_depth() {
        // a -> b -> c -> d (each depends on the previous): the degenerate
        // DAG a migrated linear plan becomes. Order == authored sort_key
        // order; depth increases by exactly one each row; no joins, no
        // corrects markers.
        let steps = vec![
            step("aaaa1111", "a0"),
            step("bbbb2222", "a1"),
            step("cccc3333", "a2"),
            step("dddd4444", "a3"),
        ];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "bbbb2222", &["aaaa1111"]);
        edge(&mut deps_of, "cccc3333", &["bbbb2222"]);
        edge(&mut deps_of, "dddd4444", &["cccc3333"]);

        let out = project_outline(&steps, &deps_of);

        assert_eq!(
            ids(&out),
            vec!["aaaa1111", "bbbb2222", "cccc3333", "dddd4444"]
        );
        assert_eq!(out.iter().map(|e| e.depth).collect::<Vec<_>>(), vec![0, 1, 2, 3]);
        assert!(out.iter().all(|e| !e.is_join()));
        assert!(out.iter().all(|e| e.join_deps.is_empty()));
        assert!(out.iter().all(|e| e.corrects_short_id.is_none()));
    }

    #[test]
    fn diamond_join_lists_every_dependency_by_short_id() {
        // a is the root; b and c both depend on a; d depends on b AND c
        // (the join). d is depth 2; its row carries both b and c short_ids
        // in scheduler order. b/c are depth 1 and are NOT joins.
        let steps = vec![
            step("aaaaaaaa", "a0"),
            step("bbbbbbbb", "a1"),
            step("cccccccc", "a2"),
            step("dddddddd", "a3"),
        ];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "bbbbbbbb", &["aaaaaaaa"]);
        edge(&mut deps_of, "cccccccc", &["aaaaaaaa"]);
        edge(&mut deps_of, "dddddddd", &["cccccccc", "bbbbbbbb"]);

        let out = project_outline(&steps, &deps_of);

        assert_eq!(
            ids(&out),
            vec!["aaaaaaaa", "bbbbbbbb", "cccccccc", "dddddddd"]
        );
        assert_eq!(out.iter().map(|e| e.depth).collect::<Vec<_>>(), vec![0, 1, 1, 2]);

        let d = out.iter().find(|e| e.step.short_id == "dddddddd").unwrap();
        assert!(d.is_join());
        // Sorted by the scheduler comparator (depth equal → sort_key):
        // b (a1) before c (a2), regardless of the edge insertion order.
        assert_eq!(d.join_deps, vec!["bbbbbbbb", "cccccccc"]);

        // Single-parent / root rows are not joins and carry no inline list.
        for e in out.iter().filter(|e| e.step.short_id != "dddddddd") {
            assert!(!e.is_join());
            assert!(e.join_deps.is_empty());
        }
    }

    #[test]
    fn multi_root_orders_roots_by_sort_key_then_interleaves_by_depth() {
        // Two independent roots r1 (a0) and r2 (a1); r1 -> x, r2 -> y.
        // Depth 0 roots come first ordered by sort_key (r1, r2), then the
        // depth-1 children ordered by sort_key (x a2, y a3). This is
        // exactly the scheduler tie-break, so outline == execution order.
        let steps = vec![
            step("r1______", "a0"),
            step("r2______", "a1"),
            step("x_______", "a2"),
            step("y_______", "a3"),
        ];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "x_______", &["r1______"]);
        edge(&mut deps_of, "y_______", &["r2______"]);

        let out = project_outline(&steps, &deps_of);

        assert_eq!(ids(&out), vec!["r1______", "r2______", "x_______", "y_______"]);
        assert_eq!(out.iter().map(|e| e.depth).collect::<Vec<_>>(), vec![0, 0, 1, 1]);
        assert!(out.iter().all(|e| !e.is_join()));
    }

    #[test]
    fn corrective_step_carries_corrects_marker_by_short_id() {
        // a is reviewed and fails; a' (the corrective step) has
        // corrects_step_id == a.id and depends on a. The projected a' row
        // exposes the corrected step's *short_id* (resolved from the
        // internal UUID) for the `↳ corrects <short_id>` marker; ordinary
        // steps expose None.
        let mut a_prime = step("aprime00", "a1");
        a_prime.corrects_step_id = Some("uuid-aaaa1111".to_string());
        let steps = vec![step("aaaa1111", "a0"), a_prime];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "aprime00", &["aaaa1111"]);

        let out = project_outline(&steps, &deps_of);

        assert_eq!(ids(&out), vec!["aaaa1111", "aprime00"]);
        let a = out.iter().find(|e| e.step.short_id == "aaaa1111").unwrap();
        assert_eq!(a.corrects_short_id, None);
        let ap = out.iter().find(|e| e.step.short_id == "aprime00").unwrap();
        assert_eq!(ap.corrects_short_id, Some("aaaa1111".to_string()));
        // The corrective step is a single-parent step, not a join.
        assert!(!ap.is_join());
        assert_eq!(ap.depth, 1);
    }

    #[test]
    fn outline_order_equals_scheduler_pick_order() {
        // Cross-check the §12.1 hard invariant directly: the outline order
        // must equal the order the scheduler comparator imposes over the
        // same depths, with no divergent sort. (Diamond shape reused.)
        let steps = vec![
            step("aaaaaaaa", "a0"),
            step("bbbbbbbb", "a2"),
            step("cccccccc", "a1"),
            step("dddddddd", "a3"),
        ];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "bbbbbbbb", &["aaaaaaaa"]);
        edge(&mut deps_of, "cccccccc", &["aaaaaaaa"]);
        edge(&mut deps_of, "dddddddd", &["bbbbbbbb", "cccccccc"]);

        let out = project_outline(&steps, &deps_of);

        // Independently sort with the shared comparator and compare.
        let depths = compute_step_depths(&steps, &deps_of);
        let mut expected: Vec<&Step> = steps.iter().collect();
        expected.sort_by(|a, b| step_schedule_cmp(a, b, &depths));
        let expected_ids: Vec<&str> =
            expected.iter().map(|s| s.short_id.as_str()).collect();

        assert_eq!(ids(&out), expected_ids);
        // c (a1) sorts before b (a2) at the same depth; d is last (depth 2).
        assert_eq!(
            ids(&out),
            vec!["aaaaaaaa", "cccccccc", "bbbbbbbb", "dddddddd"]
        );
    }
}
