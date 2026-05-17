// Question answer modal + resume-implementation modal (TUI-plan.md §17).
//
// The answer modal is what step-detail surfaces when the user presses `a` on
// a focused open question: it shows the question text, the harness's
// suggestions numbered from 1, and lets the user pick a suggestion verbatim,
// open `$EDITOR` for a custom answer, or cancel. The resume modal pops after
// the *last* open question for the plan is answered, asking whether to kick
// off `ralph run` immediately. Both modals are pure state — the dispatcher
// owns the storage write and the runner spawn.
//
// Tests live in this module so the state-machine surface is exercised
// without a real terminal.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

/// State of a single open answer modal. Constructed by step-detail when the
/// user presses `a` on a focused question; consumed by the dispatcher via
/// [`AnswerModal::handle_key`] until it returns a non-Pending action.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnswerModal {
    /// The `interruptions.id` (question) row this modal is targeting. The
    /// dispatcher passes this back to `storage::set_question_answer`
    /// (→ `storage::resolve_interruption`) once the user commits an answer.
    pub question_id: String,
    /// Verbatim question text — rendered as the modal's title row.
    pub question: String,
    /// Suggestions exactly as the harness wrote them. The renderer prefixes
    /// each with `[N]` (1-based); selecting `[N]` submits the corresponding
    /// suggestion verbatim, with no further editing.
    pub suggestions: Vec<String>,
}

/// Outcome of one key event on the answer modal. Drives the dispatcher's
/// next move: persist the answer, hand off to `$EDITOR`, close the modal,
/// or keep waiting for input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnswerModalAction {
    /// Key was unrecognized — keep the modal open.
    Pending,
    /// User picked suggestion at this 1-based index — the dispatcher
    /// persists `suggestions[idx-1]` as the answer and closes the modal.
    Submit { index: usize },
    /// User pressed `c` — the dispatcher should suspend the TUI, open
    /// `$EDITOR` for free-form input, persist whatever comes back, and
    /// close the modal.
    EditCustom,
    /// User pressed Esc / Ctrl-C — the dispatcher closes the modal without
    /// writing anything.
    Cancel,
}

impl AnswerModal {
    /// Build a modal targeting the given question row. `suggestions` may be
    /// empty; in that case only the `[c]` (custom) and `[esc]` (cancel)
    /// rows are interactable.
    pub fn new(
        question_id: impl Into<String>,
        question: impl Into<String>,
        suggestions: Vec<String>,
    ) -> Self {
        Self {
            question_id: question_id.into(),
            question: question.into(),
            suggestions,
        }
    }

    /// Map a key event to an [`AnswerModalAction`]. Pure for tests.
    ///
    /// Number keys 1..=9 select the suggestion at that 1-based index — the
    /// caller is expected to bound-check (`index <= suggestions.len()`)
    /// before calling `storage::set_question_answer`. `c`/`C` requests the
    /// editor handoff. Esc / Ctrl-C cancels. Unrecognized keys return
    /// [`AnswerModalAction::Pending`].
    pub fn handle_key(&self, key: KeyEvent) -> AnswerModalAction {
        match key.code {
            KeyCode::Char(c) if c.is_ascii_digit() && c != '0' => {
                let idx = (c as u8 - b'0') as usize;
                if idx <= self.suggestions.len() {
                    AnswerModalAction::Submit { index: idx }
                } else {
                    // Pressed a number with no corresponding suggestion —
                    // treat as no-op so the user gets feedback by re-trying.
                    AnswerModalAction::Pending
                }
            }
            KeyCode::Char('c') | KeyCode::Char('C')
                if !key.modifiers.contains(KeyModifiers::CONTROL) =>
            {
                AnswerModalAction::EditCustom
            }
            KeyCode::Esc => AnswerModalAction::Cancel,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                AnswerModalAction::Cancel
            }
            _ => AnswerModalAction::Pending,
        }
    }

    /// Resolve a 1-based suggestion index to its text. Returns `None` when
    /// the index is out of range (defensively — `handle_key` already
    /// bound-checks before emitting `Submit`).
    pub fn suggestion_text(&self, index_1based: usize) -> Option<&str> {
        if index_1based == 0 {
            return None;
        }
        self.suggestions.get(index_1based - 1).map(|s| s.as_str())
    }
}

/// State of the resume-implementation prompt that pops after the user
/// answers the last open question for a plan (TUI-plan.md §17).
///
/// `current_branch` mirrors the previous run's branch mode so accepting the
/// prompt re-enters the run with the same flag. Stored on construction
/// rather than re-derived on accept so a DB blip between answer and accept
/// can't change the answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeModal {
    pub plan_slug: String,
    pub current_branch: bool,
}

/// Outcome of one key event on the resume modal. The dispatcher reacts by
/// spawning `ralph run` (Accept) or just closing the modal (Decline).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumeModalAction {
    Pending,
    Accept,
    Decline,
}

impl ResumeModal {
    /// Build a resume modal bound to a plan slug and the previous run's
    /// branch mode.
    pub fn new(plan_slug: impl Into<String>, current_branch: bool) -> Self {
        Self {
            plan_slug: plan_slug.into(),
            current_branch,
        }
    }

    /// Map a key event to a [`ResumeModalAction`]. Y / Enter accept (default
    /// is Yes per §17 `[Y/n]`); n / Esc / Ctrl-C decline. Pure for tests.
    pub fn handle_key(&self, key: KeyEvent) -> ResumeModalAction {
        match key.code {
            KeyCode::Char('y') | KeyCode::Char('Y') | KeyCode::Enter => ResumeModalAction::Accept,
            KeyCode::Char('n') | KeyCode::Char('N') | KeyCode::Esc => ResumeModalAction::Decline,
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                ResumeModalAction::Decline
            }
            _ => ResumeModalAction::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn ctrl(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::CONTROL)
    }

    fn modal_with_suggestions(n: usize) -> AnswerModal {
        let suggestions = (0..n).map(|i| format!("opt{}", i + 1)).collect();
        AnswerModal::new("q1", "Pick a logging crate", suggestions)
    }

    // -- AnswerModal.handle_key -----------------------------------------------

    #[test]
    fn digit_1_selects_first_suggestion() {
        let m = modal_with_suggestions(2);
        let action = m.handle_key(key(KeyCode::Char('1')));
        assert_eq!(action, AnswerModalAction::Submit { index: 1 });
    }

    #[test]
    fn digit_2_selects_second_suggestion() {
        let m = modal_with_suggestions(2);
        let action = m.handle_key(key(KeyCode::Char('2')));
        assert_eq!(action, AnswerModalAction::Submit { index: 2 });
    }

    #[test]
    fn digit_out_of_range_is_pending() {
        // Only 2 suggestions, but user pressed `3`.
        let m = modal_with_suggestions(2);
        let action = m.handle_key(key(KeyCode::Char('3')));
        assert_eq!(action, AnswerModalAction::Pending);
    }

    #[test]
    fn digit_zero_is_pending() {
        // 0 is not a valid suggestion index per the §17 grammar.
        let m = modal_with_suggestions(3);
        let action = m.handle_key(key(KeyCode::Char('0')));
        assert_eq!(action, AnswerModalAction::Pending);
    }

    #[test]
    fn digit_with_no_suggestions_is_pending() {
        // Open-ended question (no harness suggestions) — the only valid
        // submission is custom or cancel.
        let m = modal_with_suggestions(0);
        let action = m.handle_key(key(KeyCode::Char('1')));
        assert_eq!(action, AnswerModalAction::Pending);
    }

    #[test]
    fn lowercase_c_opens_custom_editor() {
        let m = modal_with_suggestions(2);
        let action = m.handle_key(key(KeyCode::Char('c')));
        assert_eq!(action, AnswerModalAction::EditCustom);
    }

    #[test]
    fn uppercase_c_opens_custom_editor() {
        let m = modal_with_suggestions(2);
        let action = m.handle_key(key(KeyCode::Char('C')));
        assert_eq!(action, AnswerModalAction::EditCustom);
    }

    #[test]
    fn esc_cancels_modal() {
        let m = modal_with_suggestions(2);
        assert_eq!(m.handle_key(key(KeyCode::Esc)), AnswerModalAction::Cancel);
    }

    #[test]
    fn ctrl_c_cancels_modal() {
        // Ctrl-C is treated as cancel — never as the custom-answer trigger,
        // matching the global `Ctrl-C = leave` convention used elsewhere in
        // the TUI.
        let m = modal_with_suggestions(2);
        assert_eq!(
            m.handle_key(ctrl(KeyCode::Char('c'))),
            AnswerModalAction::Cancel
        );
    }

    #[test]
    fn unrecognized_key_is_pending() {
        let m = modal_with_suggestions(2);
        assert_eq!(
            m.handle_key(key(KeyCode::Char('x'))),
            AnswerModalAction::Pending
        );
        assert_eq!(m.handle_key(key(KeyCode::Tab)), AnswerModalAction::Pending);
        assert_eq!(
            m.handle_key(key(KeyCode::Char(' '))),
            AnswerModalAction::Pending
        );
    }

    // -- AnswerModal.suggestion_text ------------------------------------------

    #[test]
    fn suggestion_text_returns_indexed_value() {
        let m = modal_with_suggestions(3);
        assert_eq!(m.suggestion_text(1), Some("opt1"));
        assert_eq!(m.suggestion_text(2), Some("opt2"));
        assert_eq!(m.suggestion_text(3), Some("opt3"));
    }

    #[test]
    fn suggestion_text_zero_is_none() {
        let m = modal_with_suggestions(3);
        assert_eq!(m.suggestion_text(0), None);
    }

    #[test]
    fn suggestion_text_out_of_range_is_none() {
        let m = modal_with_suggestions(2);
        assert_eq!(m.suggestion_text(3), None);
        assert_eq!(m.suggestion_text(99), None);
    }

    // -- ResumeModal.handle_key -----------------------------------------------

    #[test]
    fn resume_y_accepts() {
        let m = ResumeModal::new("plan", false);
        assert_eq!(
            m.handle_key(key(KeyCode::Char('y'))),
            ResumeModalAction::Accept
        );
        assert_eq!(
            m.handle_key(key(KeyCode::Char('Y'))),
            ResumeModalAction::Accept
        );
    }

    #[test]
    fn resume_enter_accepts_default() {
        // §17 default is Y, so Enter accepts.
        let m = ResumeModal::new("plan", false);
        assert_eq!(m.handle_key(key(KeyCode::Enter)), ResumeModalAction::Accept);
    }

    #[test]
    fn resume_n_declines() {
        let m = ResumeModal::new("plan", false);
        assert_eq!(
            m.handle_key(key(KeyCode::Char('n'))),
            ResumeModalAction::Decline
        );
        assert_eq!(
            m.handle_key(key(KeyCode::Char('N'))),
            ResumeModalAction::Decline
        );
    }

    #[test]
    fn resume_esc_declines() {
        let m = ResumeModal::new("plan", false);
        assert_eq!(m.handle_key(key(KeyCode::Esc)), ResumeModalAction::Decline);
    }

    #[test]
    fn resume_ctrl_c_declines() {
        let m = ResumeModal::new("plan", false);
        assert_eq!(
            m.handle_key(ctrl(KeyCode::Char('c'))),
            ResumeModalAction::Decline
        );
    }

    #[test]
    fn resume_unrecognized_is_pending() {
        let m = ResumeModal::new("plan", false);
        assert_eq!(
            m.handle_key(key(KeyCode::Char('x'))),
            ResumeModalAction::Pending
        );
        assert_eq!(m.handle_key(key(KeyCode::Tab)), ResumeModalAction::Pending);
    }

    #[test]
    fn resume_preserves_current_branch_flag() {
        let m1 = ResumeModal::new("plan", true);
        assert!(m1.current_branch);
        let m2 = ResumeModal::new("plan", false);
        assert!(!m2.current_branch);
    }
}
