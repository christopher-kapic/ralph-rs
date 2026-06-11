// Interruptions inbox view (docs/dag-redesign.md §12.3).
//
// A cross-branch list of EVERY open question/blocker, decoupled from DAG
// navigation — reachable from anywhere with an open-count badge in chrome.
// Resolved items stay visible but dimmed for recent context. Submitting an
// answer auto-advances to the next open interruption (run-through), so the
// human clears the whole queue in one pass; `Esc` exits run-through back to
// the list.
//
// HARD INVARIANT (CLAUDE.md): pure state machine + separate render. No
// terminal, no DB. The dispatcher (`run_inbox_tui`) owns the alt-screen /
// raw-mode / event loop, the `storage::resolve_interruption` write, and the
// `$EDITOR` handoff; this module only decides *what* to do.

use chrono::{DateTime, Utc};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseEvent};

use crate::plan::{Interruption, InterruptionState};
use crate::tui::help::HelpState;
use crate::tui::toast::{ToastKind, ToastQueue};
use crate::tui::views::answer_modal::InterruptionModal;

/// One inbox row: an interruption plus the owning plan slug + step short id
/// for display. Resolved rows render dimmed (`theme::CHROME_DIM`) for recent
/// context (§12.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InboxItem {
    pub interruption: Interruption,
    pub plan_slug: String,
    pub step_short_id: String,
}

impl InboxItem {
    pub fn is_open(&self) -> bool {
        self.interruption.state == InterruptionState::Open
    }

    /// The stable run-through ordering key: `(asked_at, id)`, matching the
    /// `list_inbox_rows` open-branch `ORDER BY i.asked_at ASC, i.id ASC`.
    /// Run-through advancement keys off this — NOT a positional index — so a
    /// poll that re-buckets the list (open-first) can't skip an open item or
    /// finish early.
    fn run_through_key(&self) -> RunThroughKey {
        (self.interruption.asked_at, self.interruption.id.clone())
    }
}

/// `(asked_at, id)` — a stable, total ordering over interruptions that is
/// independent of in-memory list position and survives the open-first
/// re-bucketing on each poll.
type RunThroughKey = (DateTime<Utc>, String);

/// Run-through mode: after answering one interruption, auto-advance to the
/// next open one (§12.3). `Esc` exits back to the list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxMode {
    /// Browsing the cross-branch list.
    List,
    /// Resolving interruptions in one pass; the modal is the active surface.
    RunThrough,
}

/// What the dispatcher should do after a key was handled.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InboxOutcome {
    /// State changed; redraw.
    Handled,
    /// Pop the inbox back to the caller (`q`/`h`/`←`/Ctrl-C).
    Pop,
    /// Persist this resolution then advance to the next open interruption
    /// (`storage::resolve_interruption` — §8 bounded injection). The
    /// dispatcher calls [`InboxState::resolve_and_advance`] after the write.
    Resolve {
        interruption_id: String,
        resolution: String,
        comment: Option<String>,
    },
    /// Hand off to `$EDITOR` for the freeform answer of the active modal.
    EditFreeform,
    /// Hand off to `$EDITOR` for the comment of the active modal.
    EditComment,
}

/// Pure inbox state.
#[derive(Debug)]
pub struct InboxState {
    /// All items (open first, then resolved-dimmed for context). Ordering is
    /// the dispatcher's `list_open_interruptions` order with resolved items
    /// appended.
    items: Vec<InboxItem>,
    /// Cursor index into `items`.
    cursor: usize,
    mode: InboxMode,
    /// The active §12.4 modal while in run-through.
    modal: Option<InterruptionModal>,
    /// The current run-through target's stable `(asked_at, id)` key. Kept even
    /// after the target is resolved so advancement is "next open item strictly
    /// after THIS key in stable order" — robust against the open-first
    /// re-bucketing each poll applies (the §12.3 single-forward-pass guarantee).
    run_through_target: Option<RunThroughKey>,
    /// `?` help overlay state (per-view, per CLAUDE.md).
    pub help: HelpState,
    /// Transient user-visible messages for dispatcher-owned side effects
    /// such as `$EDITOR` handoff failures.
    pub toasts: ToastQueue,
    /// Pop request latch.
    should_pop: bool,
}

impl InboxState {
    /// Build from the cross-branch item list. Open items should precede
    /// resolved ones; the constructor parks the cursor on the first item.
    pub fn new(items: Vec<InboxItem>) -> Self {
        Self {
            items,
            cursor: 0,
            mode: InboxMode::List,
            modal: None,
            run_through_target: None,
            help: HelpState::new(),
            toasts: ToastQueue::new(),
            should_pop: false,
        }
    }

    pub fn push_toast(&mut self, text: impl Into<String>, kind: ToastKind) {
        self.toasts.push(text, kind, std::time::Instant::now());
    }

    /// Replace the item list after a DB poll, preserving the cursor by
    /// interruption id. If a run-through is active and its target is no longer
    /// open (resolved out-of-band by an external process), advance to the next
    /// open item by stable order — NOT finish early — so a poll that
    /// re-buckets the list can't drop a still-open interruption.
    pub fn sync(&mut self, items: Vec<InboxItem>) {
        let cursor_id = self
            .items
            .get(self.cursor)
            .map(|i| i.interruption.id.clone());
        self.items = items;
        if let Some(id) = cursor_id
            && let Some(idx) = self.items.iter().position(|i| i.interruption.id == id)
        {
            self.cursor = idx;
        } else if self.items.is_empty() {
            self.cursor = 0;
        } else if self.cursor >= self.items.len() {
            self.cursor = self.items.len() - 1;
        }
        // Advancement keys off `run_through_target` (the stable key), so it
        // remains correct even when the target row was resolved out-of-band
        // (its key is the boundary; `next_open_after` looks strictly past it).
        if self.mode == InboxMode::RunThrough {
            let target_still_open = self.modal.as_ref().is_some_and(|m| {
                self.items
                    .iter()
                    .any(|i| i.interruption.id == m.interruption_id && i.is_open())
            });
            if target_still_open {
                // Keep the visible cursor pinned to the modal target.
                if let Some(id) = self.modal.as_ref().map(|m| m.interruption_id.clone()) {
                    self.focus_cursor_on(&id);
                }
            } else {
                self.advance_or_finish();
            }
        }
    }

    /// Count of still-open interruptions — the chrome badge (`i (3)`).
    pub fn open_count(&self) -> usize {
        self.items.iter().filter(|i| i.is_open()).count()
    }

    /// All rows, in display order (open then resolved-dimmed).
    pub fn items(&self) -> &[InboxItem] {
        &self.items
    }

    pub fn cursor(&self) -> usize {
        self.cursor
    }

    pub fn mode(&self) -> &InboxMode {
        &self.mode
    }

    /// The active §12.4 modal, if in run-through.
    pub fn modal(&self) -> Option<&InterruptionModal> {
        self.modal.as_ref()
    }

    pub fn should_pop(&self) -> bool {
        self.should_pop
    }

    /// The item under the cursor, if any.
    pub fn selected(&self) -> Option<&InboxItem> {
        self.items.get(self.cursor)
    }

    fn navigate_down(&mut self) {
        if self.items.is_empty() {
            return;
        }
        self.cursor = (self.cursor + 1) % self.items.len();
    }

    fn navigate_up(&mut self) {
        if self.items.is_empty() {
            return;
        }
        self.cursor = if self.cursor == 0 {
            self.items.len() - 1
        } else {
            self.cursor - 1
        };
    }

    fn jump_top(&mut self) {
        if !self.items.is_empty() {
            self.cursor = 0;
        }
    }

    fn jump_bottom(&mut self) {
        if !self.items.is_empty() {
            self.cursor = self.items.len() - 1;
        }
    }

    /// The open item with the smallest stable `(asked_at, id)` key (the head
    /// of the run-through pass).
    fn first_open(&self) -> Option<&InboxItem> {
        self.items
            .iter()
            .filter(|i| i.is_open())
            .min_by(|a, b| a.run_through_key().cmp(&b.run_through_key()))
    }

    /// The open item with the smallest stable key strictly *after* `key`.
    /// Run-through advancement uses this so the next target is chosen by the
    /// stable `(asked_at, id)` order, never by current list position — a poll
    /// that re-buckets the list (open-first) can no longer skip an open item
    /// nor finish the pass while open items remain (§12.3 single forward pass).
    fn next_open_after(&self, key: &RunThroughKey) -> Option<&InboxItem> {
        self.items
            .iter()
            .filter(|i| i.is_open() && i.run_through_key() > *key)
            .min_by(|a, b| a.run_through_key().cmp(&b.run_through_key()))
    }

    /// Move the cursor onto the item with `id`, if present (run-through keeps
    /// the visible cursor on the modal's target).
    fn focus_cursor_on(&mut self, id: &str) {
        if let Some(idx) = self.items.iter().position(|i| i.interruption.id == id) {
            self.cursor = idx;
        }
    }

    /// Enter run-through starting at the cursor's item (or the first open
    /// item if the cursor is on a resolved one). No-op when nothing is open.
    pub fn start_run_through(&mut self) -> bool {
        let start_id = if self.selected().map(|i| i.is_open()).unwrap_or(false) {
            self.selected().map(|i| i.interruption.id.clone())
        } else {
            self.first_open().map(|i| i.interruption.id.clone())
        };
        match start_id {
            Some(id) => {
                self.enter_target(&id);
                true
            }
            None => false,
        }
    }

    /// Make `id` the active run-through target: open its modal, record its
    /// stable key, and park the cursor on it.
    fn enter_target(&mut self, id: &str) {
        let Some(item) = self.items.iter().find(|i| i.interruption.id == id) else {
            return;
        };
        self.run_through_target = Some(item.run_through_key());
        self.modal = Some(InterruptionModal::from_interruption(&item.interruption));
        self.mode = InboxMode::RunThrough;
        self.focus_cursor_on(id);
    }

    /// After a successful `storage::resolve_interruption`, mark the item
    /// resolved in-memory and auto-advance to the NEXT open interruption
    /// (§12.3 run-through). When none remain, exit run-through back to the
    /// list (the queue is cleared in one pass).
    pub fn resolve_and_advance(
        &mut self,
        interruption_id: &str,
        resolution: &str,
        comment: Option<&str>,
    ) {
        if let Some(item) = self
            .items
            .iter_mut()
            .find(|i| i.interruption.id == interruption_id)
        {
            item.interruption.state = InterruptionState::Resolved;
            item.interruption.resolution = Some(resolution.to_string());
            item.interruption.comment = comment.map(|c| c.to_string());
        }
        self.advance_or_finish();
    }

    /// Advance the run-through to the next still-open item whose stable key is
    /// strictly *after* the current target's, or finish (back to the list,
    /// modal cleared) when none remain. Keying off the stable `(asked_at, id)`
    /// order — not a list index — keeps the pass robust: resolving items out
    /// of order, and the open-first re-bucketing each poll applies, can no
    /// longer skip an open item or finish early (§12.3 single forward pass).
    fn advance_or_finish(&mut self) {
        // The boundary is the current target's key (kept even after the target
        // resolved — see `run_through_target`). With no target recorded fall
        // back to the stable-first open item.
        let next_id = match &self.run_through_target {
            Some(key) => self.next_open_after(key).map(|i| i.interruption.id.clone()),
            None => self.first_open().map(|i| i.interruption.id.clone()),
        };
        match next_id {
            Some(id) => self.enter_target(&id),
            None => self.exit_run_through(),
        }
    }

    /// Exit run-through back to the list WITHOUT resolving the current item
    /// (`Esc` mid-run-through — §12.3). Remaining items stay open.
    pub fn exit_run_through(&mut self) {
        self.mode = InboxMode::List;
        self.modal = None;
        self.run_through_target = None;
    }

    /// Pure key handler. In `List` mode: j/k navigate, `enter`/`a` start
    /// run-through, q/h/←/Ctrl-C pop. In `RunThrough` mode keys route to the
    /// §12.4 modal; its `Cancel` exits run-through (back to the list, items
    /// still open), its `Resolve` bubbles up for the dispatcher to persist.
    pub fn handle_key(&mut self, key: KeyEvent) -> InboxOutcome {
        match &self.mode {
            InboxMode::List => self.handle_list_key(key),
            InboxMode::RunThrough => self.handle_run_through_key(key),
        }
    }

    /// Mouse handler — currently only the scroll wheel does anything: in
    /// list mode it moves the cursor (mirroring `j`/`k`), matching the
    /// plan_list / archived_list / plan_detail convention. While the
    /// run-through modal is up the wheel is intentionally inert so a stray
    /// scroll while typing in `$EDITOR` doesn't shift a selection underneath
    /// the modal. All other mouse events are no-ops for now.
    pub fn handle_mouse(&mut self, event: MouseEvent) {
        use crossterm::event::MouseEventKind;

        if matches!(self.mode, InboxMode::RunThrough) {
            return;
        }
        match event.kind {
            MouseEventKind::ScrollDown => self.navigate_down(),
            MouseEventKind::ScrollUp => self.navigate_up(),
            _ => {}
        }
    }

    fn handle_list_key(&mut self, key: KeyEvent) -> InboxOutcome {
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                self.navigate_down();
                InboxOutcome::Handled
            }
            KeyCode::Char('k') | KeyCode::Up => {
                self.navigate_up();
                InboxOutcome::Handled
            }
            KeyCode::Char('g') | KeyCode::Home => {
                self.jump_top();
                InboxOutcome::Handled
            }
            KeyCode::Char('G') | KeyCode::End => {
                self.jump_bottom();
                InboxOutcome::Handled
            }
            KeyCode::Enter | KeyCode::Char('a') => {
                self.start_run_through();
                InboxOutcome::Handled
            }
            KeyCode::Char('q') | KeyCode::Char('h') | KeyCode::Left => {
                self.should_pop = true;
                InboxOutcome::Pop
            }
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                self.should_pop = true;
                InboxOutcome::Pop
            }
            _ => InboxOutcome::Handled,
        }
    }

    fn handle_run_through_key(&mut self, key: KeyEvent) -> InboxOutcome {
        use crate::tui::views::answer_modal::InterruptionModalAction;
        let Some(modal) = self.modal.as_mut() else {
            // Defensive: no modal but in run-through — fall back to list.
            self.exit_run_through();
            return InboxOutcome::Handled;
        };
        match modal.handle_key(key) {
            InterruptionModalAction::Pending => InboxOutcome::Handled,
            InterruptionModalAction::Cancel => {
                // §12.3: Esc exits run-through back to the list; remaining
                // items stay open.
                self.exit_run_through();
                InboxOutcome::Handled
            }
            InterruptionModalAction::EditFreeform => InboxOutcome::EditFreeform,
            InterruptionModalAction::EditComment => InboxOutcome::EditComment,
            InterruptionModalAction::Resolve {
                interruption_id,
                resolution,
                comment,
            } => InboxOutcome::Resolve {
                interruption_id,
                resolution,
                comment,
            },
        }
    }

    /// Apply editor-captured freeform text to the active modal (dispatcher
    /// calls this after the `$EDITOR` round-trip).
    pub fn set_modal_freeform(&mut self, text: String) {
        if let Some(m) = self.modal.as_mut() {
            m.freeform = text;
        }
    }

    /// Apply editor-captured comment text to the active modal.
    pub fn set_modal_comment(&mut self, text: String) {
        if let Some(m) = self.modal.as_mut() {
            m.comment = text;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::{InterruptionKind, InterruptionOption};
    use chrono::Utc;

    fn interruption(id: &str, open: bool) -> Interruption {
        Interruption {
            id: id.to_string(),
            step_id: format!("step-{id}"),
            attempt: 1,
            kind: InterruptionKind::Question,
            body: format!("Q {id}?"),
            options: vec![InterruptionOption {
                text: format!("ans-{id}"),
                priority: 1,
            }],
            resolution: None,
            comment: None,
            state: if open {
                InterruptionState::Open
            } else {
                InterruptionState::Resolved
            },
            asked_at: Utc::now(),
            resolved_at: None,
        }
    }

    fn item(id: &str, open: bool) -> InboxItem {
        InboxItem {
            interruption: interruption(id, open),
            plan_slug: "plan-a".to_string(),
            step_short_id: format!("s{id}"),
        }
    }

    /// Item with an explicit `asked_at` (seconds offset from a fixed epoch) so
    /// the stable `(asked_at, id)` run-through order is deterministic in tests
    /// that resolve out of `asked_at` order.
    fn item_at(id: &str, open: bool, asked_secs: i64) -> InboxItem {
        let mut it = item(id, open);
        it.interruption.asked_at = chrono::DateTime::<Utc>::from_timestamp(asked_secs, 0).unwrap();
        it
    }

    /// Re-bucket a slice into the `list_inbox_rows` shape the dispatcher feeds
    /// `sync`: open items first (oldest `asked_at` first), then resolved.
    fn rebucketed(items: &[InboxItem]) -> Vec<InboxItem> {
        let mut open: Vec<InboxItem> = items.iter().filter(|i| i.is_open()).cloned().collect();
        open.sort_by(|a, b| {
            (a.interruption.asked_at, &a.interruption.id)
                .cmp(&(b.interruption.asked_at, &b.interruption.id))
        });
        let resolved: Vec<InboxItem> = items.iter().filter(|i| !i.is_open()).cloned().collect();
        open.into_iter().chain(resolved).collect()
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    #[test]
    fn mouse_wheel_moves_cursor_in_list_mode() {
        use crossterm::event::{MouseEvent, MouseEventKind};
        fn wheel(kind: MouseEventKind) -> MouseEvent {
            MouseEvent {
                kind,
                column: 0,
                row: 0,
                modifiers: KeyModifiers::NONE,
            }
        }

        let items = vec![item("1", true), item("2", true), item("3", true)];
        let mut st = InboxState::new(items);
        assert_eq!(st.cursor(), 0);

        st.handle_mouse(wheel(MouseEventKind::ScrollDown));
        assert_eq!(st.cursor(), 1, "wheel-down advances cursor");
        st.handle_mouse(wheel(MouseEventKind::ScrollDown));
        assert_eq!(st.cursor(), 2);
        // Wraps (matches navigate_down's wrapping behavior — same as j/k).
        st.handle_mouse(wheel(MouseEventKind::ScrollDown));
        assert_eq!(st.cursor(), 0);
        st.handle_mouse(wheel(MouseEventKind::ScrollUp));
        assert_eq!(st.cursor(), 2, "wheel-up wraps to the end");

        // While the run-through modal is up the wheel must NOT move the
        // underlying cursor (the modal owns input; a stray scroll from the
        // user's mouse during $EDITOR text entry shouldn't shift selection).
        assert!(st.start_run_through());
        let before = st.cursor();
        st.handle_mouse(wheel(MouseEventKind::ScrollDown));
        assert_eq!(st.cursor(), before, "wheel is inert in run-through mode");

        // Non-scroll mouse events are no-ops everywhere.
        st.exit_run_through();
        let before = st.cursor();
        st.handle_mouse(wheel(MouseEventKind::Down(
            crossterm::event::MouseButton::Left,
        )));
        assert_eq!(st.cursor(), before);
    }

    // -- STEP 52: listing / selection / badge -----------------------------

    #[test]
    fn open_count_badge_counts_only_open_items() {
        let st = InboxState::new(vec![
            item("1", true),
            item("2", true),
            item("3", false), // resolved — dimmed, not counted
        ]);
        assert_eq!(st.open_count(), 2);
        assert_eq!(st.items().len(), 3, "resolved items stay visible");
    }

    #[test]
    fn navigation_wraps_across_open_and_resolved_rows() {
        let mut st = InboxState::new(vec![item("1", true), item("2", false)]);
        assert_eq!(st.cursor(), 0);
        st.handle_key(key(KeyCode::Char('j')));
        assert_eq!(st.selected().unwrap().interruption.id, "2");
        st.handle_key(key(KeyCode::Char('j')));
        assert_eq!(st.selected().unwrap().interruption.id, "1");
        st.handle_key(key(KeyCode::Char('k')));
        assert_eq!(st.selected().unwrap().interruption.id, "2");
    }

    #[test]
    fn g_and_capital_g_jump_to_top_and_bottom() {
        let mut st = InboxState::new(vec![item("1", true), item("2", false), item("3", true)]);
        st.handle_key(key(KeyCode::Char('G')));
        assert_eq!(st.selected().unwrap().interruption.id, "3");
        st.handle_key(key(KeyCode::Char('g')));
        assert_eq!(st.selected().unwrap().interruption.id, "1");
    }

    #[test]
    fn q_pops_the_inbox() {
        let mut st = InboxState::new(vec![item("1", true)]);
        assert_eq!(st.handle_key(key(KeyCode::Char('q'))), InboxOutcome::Pop);
        assert!(st.should_pop());
    }

    #[test]
    fn sync_preserves_cursor_by_interruption_id() {
        let mut st = InboxState::new(vec![item("1", true), item("2", true)]);
        st.handle_key(key(KeyCode::Char('j'))); // cursor on "2"
        // Re-poll with "1" resolved out-of-band; "2" still present.
        st.sync(vec![item("2", true), item("1", false)]);
        assert_eq!(st.selected().unwrap().interruption.id, "2");
        assert_eq!(st.open_count(), 1);
    }

    // -- STEP 53: run-through answering -----------------------------------

    #[test]
    fn answering_n_advances_through_all_then_ends() {
        let mut st = InboxState::new(vec![item("1", true), item("2", true), item("3", true)]);
        assert!(st.start_run_through());
        assert_eq!(*st.mode(), InboxMode::RunThrough);

        // Resolve #1 → auto-advance to #2.
        st.resolve_and_advance("1", "a1", None);
        assert_eq!(*st.mode(), InboxMode::RunThrough);
        assert_eq!(st.modal().unwrap().interruption_id, "2");

        // Resolve #2 → auto-advance to #3.
        st.resolve_and_advance("2", "a2", Some("note"));
        assert_eq!(st.modal().unwrap().interruption_id, "3");

        // Resolve #3 → queue cleared in one pass → back to the list.
        st.resolve_and_advance("3", "a3", None);
        assert_eq!(*st.mode(), InboxMode::List);
        assert!(st.modal().is_none());
        assert_eq!(st.open_count(), 0, "all N cleared in one run-through pass");
    }

    #[test]
    fn esc_mid_run_through_returns_to_list_with_remaining_open() {
        let mut st = InboxState::new(vec![item("1", true), item("2", true)]);
        st.start_run_through();
        // Resolve #1, advance to #2.
        st.resolve_and_advance("1", "a1", None);
        assert_eq!(st.modal().unwrap().interruption_id, "2");
        // Esc on #2's modal exits run-through; #2 stays open.
        let out = st.handle_key(key(KeyCode::Esc));
        assert_eq!(out, InboxOutcome::Handled);
        assert_eq!(*st.mode(), InboxMode::List);
        assert!(st.modal().is_none());
        assert_eq!(st.open_count(), 1, "#2 still open after Esc");
        assert!(
            st.items()
                .iter()
                .any(|i| i.interruption.id == "2" && i.is_open())
        );
    }

    #[test]
    fn resolve_outcome_bubbles_id_resolution_and_comment() {
        let mut st = InboxState::new(vec![item("1", true)]);
        st.start_run_through();
        // The modal opens focused on the priority-1 option, pre-selected.
        let out = st.handle_key(key(KeyCode::Enter));
        assert_eq!(
            out,
            InboxOutcome::Resolve {
                interruption_id: "1".to_string(),
                resolution: "ans-1".to_string(),
                comment: None,
            }
        );
    }

    #[test]
    fn run_through_skips_resolved_items_and_starts_at_first_open() {
        // Cursor parked on a resolved row; start_run_through jumps to the
        // first OPEN item rather than trying to answer a resolved one.
        let mut st = InboxState::new(vec![item("done", false), item("open1", true)]);
        assert_eq!(st.cursor(), 0); // on "done" (resolved)
        assert!(st.start_run_through());
        assert_eq!(st.modal().unwrap().interruption_id, "open1");
    }

    #[test]
    fn start_run_through_noop_when_nothing_open() {
        let mut st = InboxState::new(vec![item("1", false), item("2", false)]);
        assert!(!st.start_run_through());
        assert_eq!(*st.mode(), InboxMode::List);
        assert!(st.modal().is_none());
    }

    #[test]
    fn editor_handoff_outcomes_bubble_and_apply() {
        let mut st = InboxState::new(vec![item("1", true)]);
        st.start_run_through();
        // Tab to freeform, request the editor.
        st.handle_key(key(KeyCode::Tab));
        let out = st.handle_key(key(KeyCode::Char('f')));
        assert_eq!(out, InboxOutcome::EditFreeform);
        st.set_modal_freeform("custom answer".to_string());
        // Now Enter resolves with the typed freeform answer.
        assert_eq!(
            st.handle_key(key(KeyCode::Enter)),
            InboxOutcome::Resolve {
                interruption_id: "1".to_string(),
                resolution: "custom answer".to_string(),
                comment: None,
            }
        );
    }

    #[test]
    fn sync_ends_run_through_if_active_target_resolved_out_of_band() {
        let mut st = InboxState::new(vec![item("1", true), item("2", true)]);
        st.start_run_through();
        assert_eq!(st.modal().unwrap().interruption_id, "1");
        // Another process resolved #1 and #2.
        st.sync(vec![item("1", false), item("2", false)]);
        assert_eq!(*st.mode(), InboxMode::List);
        assert!(st.modal().is_none());
    }

    // -- FIX A: robust run-through under out-of-order resolution + re-bucketing.

    /// The run-through must visit every still-open item exactly once and not
    /// finish early, even when items resolve OUT OF `asked_at` order and the
    /// poll re-buckets the list (open-first) on every tick. The run-through
    /// resolves its own modal target; meanwhile an external process resolves
    /// a *higher-asked_at* item early — exactly the case that, with the old
    /// positional advance against the re-bucketed list, skipped a still-open
    /// item or finished early.
    #[test]
    fn run_through_visits_every_open_item_under_out_of_order_resolution() {
        // Stable order by asked_at: a(1) < b(2) < c(3) < d(4).
        let mut live = vec![
            item_at("a", true, 1),
            item_at("b", true, 2),
            item_at("c", true, 3),
            item_at("d", true, 4),
        ];
        let mut st = InboxState::new(rebucketed(&live));
        assert!(st.start_run_through());

        // BEFORE the user touches anything, an external process resolves `c`
        // (out of asked_at order — c comes after b). The next poll re-buckets:
        // open {a, b, d}, then resolved {c}. With positional advance this
        // shifted indices and could skip `b`.
        for it in live.iter_mut() {
            if it.interruption.id == "c" {
                it.interruption.state = InterruptionState::Resolved;
            }
        }
        st.sync(rebucketed(&live));
        // Target unchanged (a still open) — external resolution didn't derail.
        assert_eq!(st.modal().unwrap().interruption_id, "a");

        let mut visited: Vec<String> = Vec::new();
        // Now run the pass: each step resolves the CURRENT modal target.
        loop {
            let target = st.modal().unwrap().interruption_id.clone();
            assert!(
                !visited.contains(&target),
                "run-through re-visited {target}; visited={visited:?}"
            );
            visited.push(target.clone());
            for it in live.iter_mut() {
                if it.interruption.id == target {
                    it.interruption.state = InterruptionState::Resolved;
                }
            }
            st.resolve_and_advance(&target, "ans", None);
            // The dispatcher re-polls every tick: feed the re-bucketed list.
            st.sync(rebucketed(&live));
            if *st.mode() == InboxMode::List {
                break;
            }
        }

        // Visited exactly the three still-open items, each once, in stable
        // asked_at order — `c` was externally resolved so it's skipped, `b`
        // (which the positional bug dropped) is NOT skipped.
        assert_eq!(visited, vec!["a", "b", "d"]);
        assert!(st.modal().is_none());
        assert_eq!(st.open_count(), 0, "all open items cleared in one pass");
    }

    /// An item resolved by an EXTERNAL process (it appears resolved on the
    /// next `sync` WITHOUT going through `resolve_and_advance`) must advance
    /// the run-through to a remaining open item, not finish early.
    #[test]
    fn sync_advances_past_externally_resolved_target_to_remaining_open() {
        let all = vec![
            item_at("a", true, 1),
            item_at("b", true, 2),
            item_at("c", true, 3),
        ];
        let mut st = InboxState::new(rebucketed(&all));
        assert!(st.start_run_through());
        assert_eq!(st.modal().unwrap().interruption_id, "a");

        // Another process resolves the CURRENT target (a) out-of-band; b and c
        // remain open. The poll re-buckets (open-first: b, c; then a resolved).
        let after_external = vec![
            item_at("b", true, 2),
            item_at("c", true, 3),
            item_at("a", false, 1),
        ];
        st.sync(rebucketed(&after_external));

        // Must advance to the next stable-order open item (b), NOT finish.
        assert_eq!(*st.mode(), InboxMode::RunThrough);
        assert_eq!(
            st.modal().unwrap().interruption_id,
            "b",
            "advanced to remaining open item, not finished early"
        );

        // Finish the rest normally to confirm c is still reachable.
        st.resolve_and_advance("b", "ans", None);
        assert_eq!(st.modal().unwrap().interruption_id, "c");
    }
}
