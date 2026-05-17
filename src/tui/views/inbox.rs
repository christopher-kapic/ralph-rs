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

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

use crate::plan::{Interruption, InterruptionState};
use crate::tui::help::HelpState;
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
}

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
    /// `?` help overlay state (per-view, per CLAUDE.md).
    pub help: HelpState,
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
            help: HelpState::new(),
            should_pop: false,
        }
    }

    /// Replace the item list after a DB poll, preserving the cursor by
    /// interruption id. If a run-through modal is active and its target
    /// vanished (resolved out-of-band), the run-through ends gracefully.
    pub fn sync(&mut self, items: Vec<InboxItem>) {
        let cursor_id = self
            .items
            .get(self.cursor)
            .map(|i| i.interruption.id.clone());
        self.items = items;
        if let Some(id) = cursor_id
            && let Some(idx) = self
                .items
                .iter()
                .position(|i| i.interruption.id == id)
        {
            self.cursor = idx;
        } else if self.items.is_empty() {
            self.cursor = 0;
        } else if self.cursor >= self.items.len() {
            self.cursor = self.items.len() - 1;
        }
        if self.mode == InboxMode::RunThrough
            && let Some(m) = &self.modal
            && !self
                .items
                .iter()
                .any(|i| i.interruption.id == m.interruption_id && i.is_open())
        {
            self.advance_or_finish();
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

    /// Index of the first OPEN item at or after `from` (wrapping is *not*
    /// done — run-through is a single forward pass through the queue, §12.3).
    fn next_open_from(&self, from: usize) -> Option<usize> {
        self.items
            .iter()
            .enumerate()
            .skip(from)
            .find(|(_, i)| i.is_open())
            .map(|(idx, _)| idx)
    }

    /// Enter run-through starting at the cursor's item (or the first open
    /// item if the cursor is on a resolved one). No-op when nothing is open.
    pub fn start_run_through(&mut self) -> bool {
        let start = if self.selected().map(|i| i.is_open()).unwrap_or(false) {
            Some(self.cursor)
        } else {
            self.next_open_from(0)
        };
        match start {
            Some(idx) => {
                self.cursor = idx;
                self.mode = InboxMode::RunThrough;
                self.modal = Some(InterruptionModal::from_interruption(
                    &self.items[idx].interruption,
                ));
                true
            }
            None => false,
        }
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

    /// Advance the run-through to the next still-open item, or finish (back
    /// to the list, modal cleared) when the queue is empty.
    fn advance_or_finish(&mut self) {
        match self.next_open_from(0) {
            Some(idx) => {
                self.cursor = idx;
                self.modal = Some(InterruptionModal::from_interruption(
                    &self.items[idx].interruption,
                ));
            }
            None => {
                self.mode = InboxMode::List;
                self.modal = None;
            }
        }
    }

    /// Exit run-through back to the list WITHOUT resolving the current item
    /// (`Esc` mid-run-through — §12.3). Remaining items stay open.
    pub fn exit_run_through(&mut self) {
        self.mode = InboxMode::List;
        self.modal = None;
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

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
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
        let mut st = InboxState::new(vec![
            item("1", true),
            item("2", true),
            item("3", true),
        ]);
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
            st.items().iter().any(|i| i.interruption.id == "2" && i.is_open())
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
        let mut st = InboxState::new(vec![
            item("done", false),
            item("open1", true),
        ]);
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
}
