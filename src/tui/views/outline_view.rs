// Dependency-outline view model (docs/dag-redesign.md §12.1, §12.2, §12.5).
//
// This is the **pure state machine** behind the plan-detail step column. It
// replaces the flat positional step list with the topological
// [`crate::tui::outline::project_outline`] projection, layers the focus /
// re-root navigation (§12.2) on top as a pure view transform, and exposes a
// per-row presentation model (effective `Blocked` overlay, `review_status`
// badge, `↳ corrects <short_id>` marker, `deps:` join list) so the renderer
// stays a dumb projection.
//
// HARD INVARIANT (CLAUDE.md "TUI architecture"): no terminal, no DB. Every
// transition is a pure method tested below. Focus is a pure view filter — it
// never writes the DB and never affects the scheduler (§12.2): scheduling
// still spans the whole DAG; focus only narrows what is *drawn*.

use std::collections::{HashMap, HashSet};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::plan::{ReviewStatus, Step, StepStatus, effective_step_status};
use crate::tui::outline::{OutlineEntry, project_outline};

/// One drawable outline row: the projected [`OutlineEntry`] plus the derived
/// presentation fields the renderer needs. Computed purely from the step set,
/// dependency edges, and the open-interruption set — never re-queried.
#[derive(Debug, Clone)]
pub struct OutlineRow {
    /// Stable, plan-unique handle for this row's step.
    pub short_id: String,
    /// Internal UUID — for the dispatcher to open step-detail / resolve.
    pub step_id: String,
    /// Title, rendered after the indent + glyph.
    pub title: String,
    /// Topological depth (= indent level). Root = 0.
    pub depth: usize,
    /// `short_id`s of every dependency for a *join* step (>1 dep), in
    /// scheduler order; empty for roots / single-parent steps. Renders as
    /// `deps: a1b2 c3d4`.
    pub join_deps: Vec<String>,
    /// Set on a reviewer-inserted corrective step: the `short_id` of the
    /// step it corrects, for the `↳ corrects <short_id>` marker (§10/§12.1).
    pub corrects_short_id: Option<String>,
    /// The *effective* (presentation) status — the stored lifecycle with the
    /// derived `Blocked` overlay already applied via
    /// [`effective_step_status`] (§3.3). The renderer routes color through
    /// `theme::step_status_color(effective_status)` so the orange Blocked
    /// overlay reads identically to the plan-level Interrupted dot (§12.5).
    pub effective_status: StepStatus,
    /// The per-step review verdict (§3.3). `Pending` when never reviewed.
    /// Surfaced as a badge via `theme::step_status_color` mapping
    /// (Reviewing=blue etc. — §12.5).
    pub review_status: ReviewStatus,
}

impl OutlineRow {
    /// Whether this row should render an inline `deps: …` list (it is a
    /// genuine multi-dependency join).
    pub fn is_join(&self) -> bool {
        self.join_deps.len() > 1
    }
}

/// What the dispatcher should do after a key was handled. Pure — the
/// dispatcher owns the side effect (open step-detail, pop the view).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutlineOutcome {
    /// State changed in place (cursor moved, focus pushed/popped); redraw.
    Handled,
    /// Key wasn't ours — let the caller fall through to other handlers.
    Passthrough,
    /// Open step-detail for this step id (`enter`/`l`/`→`).
    OpenStep(String),
}

/// Pure outline state: the full step set + edges, the open-interruption set,
/// the cursor, and the **focus stack** (§12.2). The focus stack is a list of
/// step ids; an empty stack = the true root(s); pushing a step id re-roots
/// the drawn outline at that step's downstream-dependents cone; popping
/// unwinds one level.
#[derive(Debug, Clone)]
pub struct OutlineState {
    /// All steps in the plan (any order).
    steps: Vec<Step>,
    /// `step_id -> [depends_on_step_id]` exactly as
    /// `storage::list_step_dependency_edges` returns it.
    deps_of: HashMap<String, Vec<String>>,
    /// Step ids with ≥1 open interruption (question or blocker). Drives the
    /// derived `Blocked` overlay (§3.3) — the same "derived, never stored"
    /// mechanism as the plan-level `Interrupted` status.
    blocked_ids: HashSet<String>,
    /// Focus stack of step ids (newest last). Empty = whole DAG.
    focus_stack: Vec<String>,
    /// Cursor index into the *currently visible* rows.
    cursor: usize,
}

impl OutlineState {
    /// Build from the plan's step set + dependency edges + the open-question
    /// / blocker step-id set. All three come from a single DB poll in the
    /// dispatcher; this constructor is pure.
    pub fn new(
        steps: Vec<Step>,
        deps_of: HashMap<String, Vec<String>>,
        blocked_ids: HashSet<String>,
    ) -> Self {
        Self {
            steps,
            deps_of,
            blocked_ids,
            focus_stack: Vec::new(),
            cursor: 0,
        }
    }

    /// Replace the step set / edges / blocked set after a DB poll, preserving
    /// the cursor *by step id* and dropping any focus-stack entries whose
    /// step no longer exists (a deleted focus root pops to the true root).
    pub fn sync(
        &mut self,
        steps: Vec<Step>,
        deps_of: HashMap<String, Vec<String>>,
        blocked_ids: HashSet<String>,
    ) {
        let cursor_id = self.visible_rows().get(self.cursor).map(|r| r.step_id.clone());
        let valid: HashSet<&str> = steps.iter().map(|s| s.id.as_str()).collect();
        self.focus_stack.retain(|id| valid.contains(id.as_str()));
        self.steps = steps;
        self.deps_of = deps_of;
        self.blocked_ids = blocked_ids;
        // Restore cursor by id within the (possibly changed) visible set.
        let rows = self.visible_rows();
        if let Some(id) = cursor_id
            && let Some(idx) = rows.iter().position(|r| r.step_id == id)
        {
            self.cursor = idx;
        } else if rows.is_empty() {
            self.cursor = 0;
        } else if self.cursor >= rows.len() {
            self.cursor = rows.len() - 1;
        }
    }

    /// `step_id -> Step` index helper.
    fn step_by_id(&self) -> HashMap<&str, &Step> {
        self.steps.iter().map(|s| (s.id.as_str(), s)).collect()
    }

    /// The transitive **downstream dependents** cone of `root_id` (the step
    /// itself plus every step that depends on it, directly or transitively).
    /// This is the §12.2 confirmed focus direction: what flows *out* of the
    /// step. Pure graph walk over the reverse adjacency of `deps_of`.
    fn downstream_cone(&self, root_id: &str) -> HashSet<String> {
        // Reverse adjacency: dep -> [steps that depend on it].
        let mut dependents: HashMap<&str, Vec<&str>> = HashMap::new();
        for (step_id, deps) in &self.deps_of {
            for d in deps {
                dependents.entry(d.as_str()).or_default().push(step_id.as_str());
            }
        }
        let mut cone: HashSet<String> = HashSet::new();
        let mut stack = vec![root_id.to_string()];
        while let Some(cur) = stack.pop() {
            if !cone.insert(cur.clone()) {
                continue;
            }
            if let Some(children) = dependents.get(cur.as_str()) {
                for c in children {
                    stack.push((*c).to_string());
                }
            }
        }
        cone
    }

    /// The effective focus root id, if any (top of the stack).
    pub fn focus_root(&self) -> Option<&str> {
        self.focus_stack.last().map(|s| s.as_str())
    }

    /// The breadcrumb suffix carried by `chrome.rs` (§12.2): the focus
    /// path's short ids, e.g. `["c9d4", "f1a0"]` for nested focus. Empty
    /// when not focused. The renderer joins these as `focus: c9d4 › f1a0`.
    pub fn focus_breadcrumb(&self) -> Vec<String> {
        let by_id = self.step_by_id();
        self.focus_stack
            .iter()
            .filter_map(|id| by_id.get(id.as_str()).map(|s| s.short_id.clone()))
            .collect()
    }

    /// Project the currently-visible rows: the full topological outline,
    /// then filtered to the active focus cone (if focused). Order is always
    /// the shared scheduler order (`project_outline` guarantees outline row
    /// order == execution order — §12.1).
    pub fn visible_rows(&self) -> Vec<OutlineRow> {
        let entries: Vec<OutlineEntry> = project_outline(&self.steps, &self.deps_of);
        let cone = self.focus_root().map(|r| self.downstream_cone(r));
        entries
            .into_iter()
            .filter(|e| match &cone {
                Some(c) => c.contains(&e.step.id),
                None => true,
            })
            .map(|e| {
                let has_open = self.blocked_ids.contains(&e.step.id);
                OutlineRow {
                    short_id: e.step.short_id.clone(),
                    step_id: e.step.id.clone(),
                    title: e.step.title.clone(),
                    depth: e.depth,
                    join_deps: e.join_deps.clone(),
                    corrects_short_id: e.corrects_short_id.clone(),
                    effective_status: effective_step_status(e.step.status, has_open),
                    review_status: e.step.review_status.unwrap_or(ReviewStatus::Pending),
                }
            })
            .collect()
    }

    /// Cursor index into the visible rows.
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    /// The step id under the cursor, if any.
    pub fn selected_step_id(&self) -> Option<String> {
        self.visible_rows()
            .get(self.cursor)
            .map(|r| r.step_id.clone())
    }

    /// Move the cursor down one row (wraps), no-op on an empty outline.
    pub fn navigate_down(&mut self) {
        let n = self.visible_rows().len();
        if n == 0 {
            return;
        }
        self.cursor = (self.cursor + 1) % n;
    }

    /// Move the cursor up one row (wraps).
    pub fn navigate_up(&mut self) {
        let n = self.visible_rows().len();
        if n == 0 {
            return;
        }
        self.cursor = if self.cursor == 0 { n - 1 } else { self.cursor - 1 };
    }

    /// `z` — re-root the outline at the cursor's step: show only that step
    /// and its transitive downstream-dependents cone (§12.2). Focus *nests*:
    /// pressing `z` again on a step inside the current cone pushes another
    /// level. No-op on an empty outline or when the cursor's step is already
    /// the focus root. Pure view transform — no DB, no scheduler effect.
    pub fn focus_cursor(&mut self) -> bool {
        let Some(id) = self.selected_step_id() else {
            return false;
        };
        if self.focus_root() == Some(id.as_str()) {
            return false;
        }
        self.focus_stack.push(id);
        // The new root is always the first visible row of its own cone.
        self.cursor = 0;
        true
    }

    /// `Z` / `Esc` / breadcrumb-click — pop one focus level toward the true
    /// root(s). Returns `false` when already at the true root (nothing to
    /// pop). The cursor is parked back on the step we were focused on so the
    /// pop feels like "zoom out, keep your place".
    pub fn pop_focus(&mut self) -> bool {
        let Some(popped) = self.focus_stack.pop() else {
            return false;
        };
        let rows = self.visible_rows();
        self.cursor = rows
            .iter()
            .position(|r| r.step_id == popped)
            .unwrap_or(0);
        true
    }

    /// Top-level "back to root" jump: clear the entire focus stack in one
    /// step (§12.2 "with a top-level back-to-root jump"). Returns `false`
    /// when not focused. Cursor parks on the outermost (first) focus root.
    pub fn pop_focus_to_root(&mut self) -> bool {
        if self.focus_stack.is_empty() {
            return false;
        }
        let first_root = self.focus_stack.first().cloned();
        self.focus_stack.clear();
        let rows = self.visible_rows();
        self.cursor = first_root
            .and_then(|id| rows.iter().position(|r| r.step_id == id))
            .unwrap_or(0);
        true
    }

    /// Jump focus to the Nth breadcrumb crumb (0-based) — a breadcrumb click
    /// (§12.2). Crumb `k` keeps the focus stack truncated to `k+1` entries.
    pub fn focus_to_crumb(&mut self, crumb_index: usize) -> bool {
        if crumb_index + 1 >= self.focus_stack.len() {
            return false;
        }
        let keep = crumb_index + 1;
        let new_top = self.focus_stack[crumb_index].clone();
        self.focus_stack.truncate(keep);
        let rows = self.visible_rows();
        self.cursor = rows
            .iter()
            .position(|r| r.step_id == new_top)
            .unwrap_or(0);
        true
    }

    /// Pure key handler (§12.1 keeps the existing view/input/render split).
    /// Navigation + focus only; the dispatcher owns side effects via
    /// [`OutlineOutcome`]. Returns `Passthrough` for keys the outline does
    /// not own so the caller can fall through to its other bindings (skip,
    /// add, run, …).
    pub fn handle_key(&mut self, key: KeyEvent) -> OutlineOutcome {
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                self.navigate_down();
                OutlineOutcome::Handled
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.navigate_up();
                OutlineOutcome::Handled
            }
            KeyCode::Char('z') => {
                self.focus_cursor();
                // Always Handled — even a no-op press shouldn't fall through
                // to an unrelated `z` binding elsewhere.
                OutlineOutcome::Handled
            }
            KeyCode::Char('Z') => {
                self.pop_focus();
                OutlineOutcome::Handled
            }
            // Esc pops focus only when focused; otherwise it's the caller's
            // (clear-selection / dismiss-toast) — pass through.
            KeyCode::Esc if !self.focus_stack.is_empty() => {
                self.pop_focus();
                OutlineOutcome::Handled
            }
            KeyCode::Enter | KeyCode::Right | KeyCode::Char('l') => {
                match self.selected_step_id() {
                    Some(id) => OutlineOutcome::OpenStep(id),
                    None => OutlineOutcome::Passthrough,
                }
            }
            // Ctrl-C is the caller's pop-view; never ours.
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                OutlineOutcome::Passthrough
            }
            _ => OutlineOutcome::Passthrough,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

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

    fn edge(deps_of: &mut HashMap<String, Vec<String>>, s: &str, deps: &[&str]) {
        deps_of.insert(
            format!("uuid-{s}"),
            deps.iter().map(|d| format!("uuid-{d}")).collect(),
        );
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ids(rows: &[OutlineRow]) -> Vec<&str> {
        rows.iter().map(|r| r.short_id.as_str()).collect()
    }

    /// a -> b -> c -> d linear chain plus a diamond for focus tests.
    fn diamond() -> OutlineState {
        // a is root; b,c depend on a; d depends on b AND c.
        let steps = vec![
            step("aaaa", "a0"),
            step("bbbb", "a1"),
            step("cccc", "a2"),
            step("dddd", "a3"),
        ];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "bbbb", &["aaaa"]);
        edge(&mut deps_of, "cccc", &["aaaa"]);
        edge(&mut deps_of, "dddd", &["bbbb", "cccc"]);
        OutlineState::new(steps, deps_of, HashSet::new())
    }

    // -- STEP 50: outline render / selection model ------------------------

    #[test]
    fn visible_rows_match_topological_outline_order_and_depth() {
        let st = diamond();
        let rows = st.visible_rows();
        assert_eq!(ids(&rows), vec!["aaaa", "bbbb", "cccc", "dddd"]);
        assert_eq!(
            rows.iter().map(|r| r.depth).collect::<Vec<_>>(),
            vec![0, 1, 1, 2]
        );
    }

    #[test]
    fn join_step_lists_every_dependency_by_short_id() {
        let st = diamond();
        let rows = st.visible_rows();
        let d = rows.iter().find(|r| r.short_id == "dddd").unwrap();
        assert!(d.is_join());
        assert_eq!(d.join_deps, vec!["bbbb", "cccc"]);
        for r in rows.iter().filter(|r| r.short_id != "dddd") {
            assert!(!r.is_join());
            assert!(r.join_deps.is_empty());
        }
    }

    #[test]
    fn blocked_overlay_is_derived_like_question_status() {
        // b has an open interruption → it presents as Blocked though its
        // stored status is Pending; resolving (empty blocked set) un-shadows.
        let mut steps = vec![step("aaaa", "a0"), step("bbbb", "a1")];
        steps[1].status = StepStatus::InProgress;
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "bbbb", &["aaaa"]);
        let blocked: HashSet<String> = ["uuid-bbbb".to_string()].into_iter().collect();
        let st = OutlineState::new(steps.clone(), deps_of.clone(), blocked);
        let b = st
            .visible_rows()
            .into_iter()
            .find(|r| r.short_id == "bbbb")
            .unwrap();
        assert_eq!(b.effective_status, StepStatus::Blocked);

        let st2 = OutlineState::new(steps, deps_of, HashSet::new());
        let b2 = st2
            .visible_rows()
            .into_iter()
            .find(|r| r.short_id == "bbbb")
            .unwrap();
        assert_eq!(b2.effective_status, StepStatus::InProgress);
    }

    #[test]
    fn corrective_step_carries_corrects_marker() {
        let mut a_prime = step("apri", "a1");
        a_prime.corrects_step_id = Some("uuid-aaaa".to_string());
        let steps = vec![step("aaaa", "a0"), a_prime];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "apri", &["aaaa"]);
        let st = OutlineState::new(steps, deps_of, HashSet::new());
        let rows = st.visible_rows();
        let a = rows.iter().find(|r| r.short_id == "aaaa").unwrap();
        assert_eq!(a.corrects_short_id, None);
        let ap = rows.iter().find(|r| r.short_id == "apri").unwrap();
        assert_eq!(ap.corrects_short_id, Some("aaaa".to_string()));
    }

    #[test]
    fn review_status_defaults_to_pending_and_surfaces_verdict() {
        let mut steps = vec![step("aaaa", "a0"), step("bbbb", "a1")];
        steps[1].review_status = Some(ReviewStatus::InFlight);
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "bbbb", &["aaaa"]);
        let st = OutlineState::new(steps, deps_of, HashSet::new());
        let rows = st.visible_rows();
        assert_eq!(
            rows.iter().find(|r| r.short_id == "aaaa").unwrap().review_status,
            ReviewStatus::Pending
        );
        assert_eq!(
            rows.iter().find(|r| r.short_id == "bbbb").unwrap().review_status,
            ReviewStatus::InFlight
        );
    }

    #[test]
    fn navigation_wraps_and_tracks_cursor() {
        let mut st = diamond();
        assert_eq!(st.cursor(), 0);
        st.navigate_down();
        assert_eq!(st.selected_step_id().as_deref(), Some("uuid-bbbb"));
        st.navigate_up();
        st.navigate_up();
        // Wrapped to the last row (dddd).
        assert_eq!(st.selected_step_id().as_deref(), Some("uuid-dddd"));
    }

    #[test]
    fn handle_key_open_step_returns_cursor_step_id() {
        let mut st = diamond();
        st.navigate_down(); // bbbb
        assert_eq!(
            st.handle_key(key(KeyCode::Enter)),
            OutlineOutcome::OpenStep("uuid-bbbb".to_string())
        );
        assert_eq!(
            st.handle_key(key(KeyCode::Char('l'))),
            OutlineOutcome::OpenStep("uuid-bbbb".to_string())
        );
    }

    #[test]
    fn unrelated_key_passes_through() {
        let mut st = diamond();
        assert_eq!(
            st.handle_key(key(KeyCode::Char('d'))),
            OutlineOutcome::Passthrough
        );
        assert_eq!(
            st.handle_key(KeyEvent::new(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            OutlineOutcome::Passthrough
        );
    }

    // -- STEP 51: focus / re-root navigation ------------------------------

    #[test]
    fn focus_shows_only_downstream_dependents_cone() {
        // Focus on b: cone is {b, d} (d depends on b). a (upstream) and c
        // (unrelated branch) are hidden — confirmed downstream direction.
        let mut st = diamond();
        st.navigate_down(); // cursor on bbbb
        assert!(st.focus_cursor());
        let rows = st.visible_rows();
        let got: HashSet<&str> = ids(&rows).into_iter().collect();
        assert_eq!(
            got,
            ["bbbb", "dddd"].into_iter().collect::<HashSet<_>>(),
            "focus must show only the step + its downstream dependents"
        );
        assert!(!got.contains("aaaa"), "upstream is carried by breadcrumb");
        assert!(!got.contains("cccc"), "unrelated branch hidden");
    }

    #[test]
    fn focus_root_alone_when_no_dependents() {
        // Focus on d (a leaf): the cone is just {d}.
        let mut st = diamond();
        // Move cursor to dddd (index 3).
        for _ in 0..3 {
            st.navigate_down();
        }
        assert_eq!(st.selected_step_id().as_deref(), Some("uuid-dddd"));
        assert!(st.focus_cursor());
        assert_eq!(ids(&st.visible_rows()), vec!["dddd"]);
    }

    #[test]
    fn focus_nests_and_pop_unwinds_one_level() {
        // a -> b -> c -> d -> e: nest focus a then b; pop returns to a's cone.
        let steps = vec![
            step("aa", "a0"),
            step("bb", "a1"),
            step("cc", "a2"),
            step("dd", "a3"),
        ];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "bb", &["aa"]);
        edge(&mut deps_of, "cc", &["bb"]);
        edge(&mut deps_of, "dd", &["cc"]);
        let mut st = OutlineState::new(steps, deps_of, HashSet::new());

        // Focus on aa: cone = whole chain.
        assert!(st.focus_cursor());
        assert_eq!(ids(&st.visible_rows()), vec!["aa", "bb", "cc", "dd"]);
        // Cursor lands on the new root; move to bb and nest again.
        st.navigate_down();
        assert_eq!(st.selected_step_id().as_deref(), Some("uuid-bb"));
        assert!(st.focus_cursor());
        assert_eq!(ids(&st.visible_rows()), vec!["bb", "cc", "dd"]);
        assert_eq!(st.focus_breadcrumb(), vec!["aa", "bb"]);

        // Pop one level → back to aa's cone, cursor parked on bb.
        assert!(st.pop_focus());
        assert_eq!(ids(&st.visible_rows()), vec!["aa", "bb", "cc", "dd"]);
        assert_eq!(st.focus_breadcrumb(), vec!["aa"]);
        assert_eq!(st.selected_step_id().as_deref(), Some("uuid-bb"));

        // Pop again → true root, no focus.
        assert!(st.pop_focus());
        assert!(st.focus_breadcrumb().is_empty());
        assert!(st.focus_root().is_none());
        // Nothing to pop now.
        assert!(!st.pop_focus());
    }

    #[test]
    fn pop_focus_to_root_clears_whole_stack_in_one_jump() {
        let mut st = diamond();
        st.navigate_down(); // bbbb
        st.focus_cursor();
        // Only bbbb + dddd visible; nest on dddd.
        st.navigate_down(); // dddd within cone
        st.focus_cursor();
        assert_eq!(st.focus_breadcrumb(), vec!["bbbb", "dddd"]);
        assert!(st.pop_focus_to_root());
        assert!(st.focus_breadcrumb().is_empty());
        assert_eq!(ids(&st.visible_rows()), vec!["aaaa", "bbbb", "cccc", "dddd"]);
        assert!(!st.pop_focus_to_root());
    }

    #[test]
    fn focus_to_crumb_truncates_stack() {
        // Build a 3-deep nest then jump back to crumb 0.
        let steps = vec![step("aa", "a0"), step("bb", "a1"), step("cc", "a2")];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "bb", &["aa"]);
        edge(&mut deps_of, "cc", &["bb"]);
        let mut st = OutlineState::new(steps, deps_of, HashSet::new());
        st.focus_cursor(); // aa
        st.navigate_down();
        st.focus_cursor(); // bb
        st.navigate_down();
        st.focus_cursor(); // cc
        assert_eq!(st.focus_breadcrumb(), vec!["aa", "bb", "cc"]);
        // Click crumb 0 (aa) → stack truncated to [aa].
        assert!(st.focus_to_crumb(0));
        assert_eq!(st.focus_breadcrumb(), vec!["aa"]);
        // Out-of-range / last crumb is a no-op.
        assert!(!st.focus_to_crumb(5));
        assert!(!st.focus_to_crumb(0)); // already the only crumb
    }

    #[test]
    fn focus_is_pure_no_storage_no_scheduler_effect() {
        // Focusing must not mutate the step set, the edge set, or the
        // blocked set — it's purely a draw filter. We assert the underlying
        // data is byte-identical before/after a focus + pop round-trip.
        let st0 = diamond();
        let steps_before: Vec<String> =
            st0.steps.iter().map(|s| s.id.clone()).collect();
        let edges_before = st0.deps_of.clone();
        let mut st = st0;
        st.navigate_down();
        st.focus_cursor();
        st.pop_focus();
        let steps_after: Vec<String> =
            st.steps.iter().map(|s| s.id.clone()).collect();
        assert_eq!(steps_before, steps_after);
        assert_eq!(edges_before, st.deps_of);
        // visible_rows still spans the whole DAG (scheduler unaffected).
        assert_eq!(st.visible_rows().len(), 4);
    }

    #[test]
    fn esc_pops_focus_only_when_focused_else_passthrough() {
        let mut st = diamond();
        // Not focused: Esc passes through (caller owns clear-selection).
        assert_eq!(
            st.handle_key(key(KeyCode::Esc)),
            OutlineOutcome::Passthrough
        );
        st.navigate_down();
        st.focus_cursor();
        // Focused: Esc pops one level and is Handled.
        assert_eq!(st.handle_key(key(KeyCode::Esc)), OutlineOutcome::Handled);
        assert!(st.focus_root().is_none());
    }

    #[test]
    fn z_and_shift_z_keys_drive_focus() {
        let mut st = diamond();
        st.navigate_down(); // bbbb
        assert_eq!(st.handle_key(key(KeyCode::Char('z'))), OutlineOutcome::Handled);
        assert_eq!(st.focus_breadcrumb(), vec!["bbbb"]);
        assert_eq!(st.handle_key(key(KeyCode::Char('Z'))), OutlineOutcome::Handled);
        assert!(st.focus_breadcrumb().is_empty());
    }

    #[test]
    fn sync_preserves_cursor_by_id_and_drops_stale_focus() {
        let mut st = diamond();
        st.navigate_down(); // bbbb
        st.focus_cursor(); // focus bbbb
        // Re-poll with bbbb deleted → focus stack drops it, cursor clamps.
        let steps = vec![step("aaaa", "a0"), step("cccc", "a2")];
        let mut deps_of = HashMap::new();
        edge(&mut deps_of, "cccc", &["aaaa"]);
        st.sync(steps, deps_of, HashSet::new());
        assert!(st.focus_root().is_none(), "deleted focus root pops");
        let rows = st.visible_rows();
        assert_eq!(ids(&rows), vec!["aaaa", "cccc"]);
        assert!(st.cursor() < rows.len());
    }
}
