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

// ---------------------------------------------------------------------------
// InterruptionModal — the §12.4 ranked-answer / blocker modal
// ---------------------------------------------------------------------------
//
// docs/dag-redesign.md §12.4: a richer answer modal used by the
// interruptions inbox (§12.3). It renders an interruption's proposed answers
// in **priority order** (1 = the agent's best) with the agent's #1
// **pre-selected**, plus an optional free-text comment field and a freeform
// escape-hatch answer. The blocker variant has **no options** — only
// resolve / resolve-with-comment. The chosen answer/option AND the comment
// flow into the Phase-2 bounded `resolve_interruption` injection.
//
// Deliberately NO "let the agent decide" shortcut (§12.4): resolving a
// question needs an explicit human answer (a ranked option or freeform).
// Abandoning a step remains `ralph skip`, not a one-key escape here.
//
// Pure state machine: the dispatcher owns the DB write
// (`storage::resolve_interruption`) and the `$EDITOR` handoff for the
// freeform / comment text.

use crate::plan::{Interruption, InterruptionKind, InterruptionOption};

/// Which field of the modal currently has keyboard focus.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InterruptionFocus {
    /// The ranked-option list (questions only; the agent's #1 is the
    /// initial selection per §12.4).
    Options,
    /// The freeform escape-hatch answer (always available — a question may
    /// be freeform-only, a blocker is always freeform).
    Freeform,
    /// The optional comment field (always injectable alongside the
    /// resolution — §3.4).
    Comment,
}

/// One key's outcome on the [`InterruptionModal`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InterruptionModalAction {
    /// Unrecognized key — keep the modal open.
    Pending,
    /// Commit the resolution. `resolution` is the chosen option text or the
    /// typed freeform answer; `comment` is the optional extra note. Both
    /// flow into `storage::resolve_interruption` (§8 bounded injection).
    Resolve {
        interruption_id: String,
        resolution: String,
        comment: Option<String>,
    },
    /// Hand off to `$EDITOR` to capture the freeform answer text.
    EditFreeform,
    /// Hand off to `$EDITOR` to capture the comment text.
    EditComment,
    /// Esc / Ctrl-C — close without resolving (in run-through, returns to
    /// the inbox list — §12.3).
    Cancel,
}

/// State of the §12.4 interruption modal. Pure; constructed from a Phase-2
/// [`Interruption`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterruptionModal {
    /// The `interruptions.id` this modal resolves.
    pub interruption_id: String,
    /// `Question` (ranked options + freeform) vs `Blocker` (no options;
    /// resolve / resolve-with-comment only).
    pub kind: InterruptionKind,
    /// The question text or blocker explanation — the modal's body.
    pub body: String,
    /// Proposed answers **sorted by priority ascending** (1 = agent best)
    /// so the renderer shows them top-to-bottom in the agent's ranking.
    /// Empty for a blocker or a freeform-only question.
    pub options: Vec<InterruptionOption>,
    /// Currently highlighted option index (into [`Self::options`]). Starts
    /// at 0 — i.e. the agent's #1 is **pre-selected** (§12.4).
    pub selected_option: usize,
    /// Where input focus currently is.
    pub focus: InterruptionFocus,
    /// Free-text escape-hatch answer captured via `$EDITOR`.
    pub freeform: String,
    /// Optional extra note captured via `$EDITOR`.
    pub comment: String,
}

impl InterruptionModal {
    /// Build a modal from a Phase-2 [`Interruption`]. Options are sorted by
    /// priority ascending so #1 (the agent's best) renders first and is
    /// pre-selected; a blocker / freeform-only question opens focused on the
    /// freeform field since it has no options.
    pub fn from_interruption(i: &Interruption) -> Self {
        let mut options = i.options.clone();
        options.sort_by_key(|o| o.priority);
        let focus = if options.is_empty() {
            InterruptionFocus::Freeform
        } else {
            InterruptionFocus::Options
        };
        Self {
            interruption_id: i.id.clone(),
            kind: i.kind,
            body: i.body.clone(),
            options,
            selected_option: 0,
            focus,
            freeform: String::new(),
            comment: String::new(),
        }
    }

    /// Whether this is a blocker (no options — resolve / resolve-with-comment
    /// only, §12.4).
    pub fn is_blocker(&self) -> bool {
        self.kind == InterruptionKind::Blocker
    }

    /// The currently-chosen resolution text: the highlighted option when the
    /// option list is focused and non-empty, otherwise the freeform answer.
    /// `None` when nothing has been chosen/typed yet (e.g. freeform empty).
    pub fn chosen_resolution(&self) -> Option<String> {
        match self.focus {
            InterruptionFocus::Options if !self.options.is_empty() => self
                .options
                .get(self.selected_option)
                .map(|o| o.text.clone()),
            _ => {
                let t = self.freeform.trim();
                if t.is_empty() {
                    None
                } else {
                    Some(t.to_string())
                }
            }
        }
    }

    /// The optional comment, trimmed; `None` when blank.
    pub fn comment_opt(&self) -> Option<String> {
        let t = self.comment.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    }

    /// Map a key event to an action. Pure for tests.
    ///
    /// - `j`/`k` (or ↑/↓) move within the option list when it's focused.
    /// - `Tab` cycles focus Options → Freeform → Comment → (Options|Freeform).
    /// - `f` opens `$EDITOR` for the freeform answer; `m` for the comment.
    /// - `Enter` resolves with the chosen resolution + comment (§8). It is
    ///   rejected (Pending) when there is no explicit human answer yet — no
    ///   "let the agent decide" shortcut (§12.4).
    /// - `Esc` / Ctrl-C cancels.
    pub fn handle_key(&mut self, key: KeyEvent) -> InterruptionModalAction {
        match key.code {
            KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                InterruptionModalAction::Cancel
            }
            KeyCode::Esc => InterruptionModalAction::Cancel,
            KeyCode::Char('j') | KeyCode::Down
                if self.focus == InterruptionFocus::Options && !self.options.is_empty() =>
            {
                self.selected_option = (self.selected_option + 1) % self.options.len();
                InterruptionModalAction::Pending
            }
            KeyCode::Char('k') | KeyCode::Up
                if self.focus == InterruptionFocus::Options && !self.options.is_empty() =>
            {
                self.selected_option = if self.selected_option == 0 {
                    self.options.len() - 1
                } else {
                    self.selected_option - 1
                };
                InterruptionModalAction::Pending
            }
            KeyCode::Tab => {
                self.focus = match self.focus {
                    InterruptionFocus::Options => InterruptionFocus::Freeform,
                    InterruptionFocus::Freeform => InterruptionFocus::Comment,
                    InterruptionFocus::Comment => {
                        if self.options.is_empty() {
                            InterruptionFocus::Freeform
                        } else {
                            InterruptionFocus::Options
                        }
                    }
                };
                InterruptionModalAction::Pending
            }
            KeyCode::Char('f') | KeyCode::Char('F') => InterruptionModalAction::EditFreeform,
            KeyCode::Char('m') | KeyCode::Char('M') => InterruptionModalAction::EditComment,
            KeyCode::Enter => match self.chosen_resolution() {
                Some(resolution) => InterruptionModalAction::Resolve {
                    interruption_id: self.interruption_id.clone(),
                    resolution,
                    comment: self.comment_opt(),
                },
                // §12.4: an explicit human answer is required — no implicit
                // "agent decides" path. Keep the modal open until one is
                // provided (pick an option or type a freeform answer).
                None => InterruptionModalAction::Pending,
            },
            _ => InterruptionModalAction::Pending,
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

    // -- InterruptionModal (§12.4) ---------------------------------------

    use crate::plan::{Interruption, InterruptionKind, InterruptionOption, InterruptionState};
    use chrono::Utc;

    fn question(opts: &[(&str, i32)]) -> Interruption {
        Interruption {
            id: "i1".to_string(),
            step_id: "s1".to_string(),
            attempt: 1,
            kind: InterruptionKind::Question,
            body: "Which logging crate?".to_string(),
            options: opts
                .iter()
                .map(|(t, p)| InterruptionOption {
                    text: (*t).to_string(),
                    priority: *p,
                })
                .collect(),
            resolution: None,
            comment: None,
            state: InterruptionState::Open,
            asked_at: Utc::now(),
            resolved_at: None,
        }
    }

    fn blocker() -> Interruption {
        Interruption {
            id: "b1".to_string(),
            step_id: "s2".to_string(),
            attempt: 1,
            kind: InterruptionKind::Blocker,
            body: "Need sudo to install deps".to_string(),
            options: vec![],
            resolution: None,
            comment: None,
            state: InterruptionState::Open,
            asked_at: Utc::now(),
            resolved_at: None,
        }
    }

    #[test]
    fn options_render_in_priority_order_with_number_one_preselected() {
        // Agent proposes them out of priority order; the modal must sort by
        // priority asc (1 = best) and pre-select index 0 (§12.4).
        let m = InterruptionModal::from_interruption(&question(&[
            ("third", 3),
            ("best", 1),
            ("second", 2),
        ]));
        assert_eq!(
            m.options
                .iter()
                .map(|o| o.text.as_str())
                .collect::<Vec<_>>(),
            vec!["best", "second", "third"]
        );
        assert_eq!(m.selected_option, 0);
        assert_eq!(m.focus, InterruptionFocus::Options);
        assert_eq!(m.chosen_resolution().as_deref(), Some("best"));
    }

    #[test]
    fn enter_resolves_with_chosen_option_and_comment() {
        let mut m = InterruptionModal::from_interruption(&question(&[("opt-a", 1), ("opt-b", 2)]));
        // Move to opt-b, set a comment via the editor handoff stub.
        assert_eq!(
            m.handle_key(key(KeyCode::Char('j'))),
            InterruptionModalAction::Pending
        );
        m.comment = "  also check perf  ".to_string();
        let action = m.handle_key(key(KeyCode::Enter));
        assert_eq!(
            action,
            InterruptionModalAction::Resolve {
                interruption_id: "i1".to_string(),
                resolution: "opt-b".to_string(),
                comment: Some("also check perf".to_string()),
            }
        );
    }

    #[test]
    fn freeform_path_resolves_with_typed_answer() {
        let mut m = InterruptionModal::from_interruption(&question(&[("o1", 1)]));
        // Tab to the freeform field, "type" via the editor handoff.
        assert_eq!(
            m.handle_key(key(KeyCode::Tab)),
            InterruptionModalAction::Pending
        );
        assert_eq!(m.focus, InterruptionFocus::Freeform);
        assert_eq!(
            m.handle_key(key(KeyCode::Char('f'))),
            InterruptionModalAction::EditFreeform
        );
        m.freeform = "use tracing".to_string();
        assert_eq!(
            m.handle_key(key(KeyCode::Enter)),
            InterruptionModalAction::Resolve {
                interruption_id: "i1".to_string(),
                resolution: "use tracing".to_string(),
                comment: None,
            }
        );
    }

    #[test]
    fn blocker_variant_has_no_options_and_resolves_freeform_only() {
        let mut m = InterruptionModal::from_interruption(&blocker());
        assert!(m.is_blocker());
        assert!(m.options.is_empty());
        // Opens focused on freeform since there are no options.
        assert_eq!(m.focus, InterruptionFocus::Freeform);
        // Enter with nothing typed must NOT resolve (explicit answer
        // required — no "agent decides" shortcut, §12.4).
        assert_eq!(
            m.handle_key(key(KeyCode::Enter)),
            InterruptionModalAction::Pending
        );
        m.freeform = "installed via admin".to_string();
        m.comment = "took 2 tries".to_string();
        assert_eq!(
            m.handle_key(key(KeyCode::Enter)),
            InterruptionModalAction::Resolve {
                interruption_id: "b1".to_string(),
                resolution: "installed via admin".to_string(),
                comment: Some("took 2 tries".to_string()),
            }
        );
    }

    #[test]
    fn no_agent_decide_shortcut_enter_is_pending_until_human_answers() {
        // A question with options but the user tabs to freeform and types
        // nothing: Enter must stay Pending (no implicit accept-#1, no
        // "agent decides").
        let mut m = InterruptionModal::from_interruption(&question(&[("a", 1)]));
        m.handle_key(key(KeyCode::Tab)); // → Freeform, empty
        assert_eq!(
            m.handle_key(key(KeyCode::Enter)),
            InterruptionModalAction::Pending
        );
    }

    #[test]
    fn esc_and_ctrl_c_cancel() {
        let mut m = InterruptionModal::from_interruption(&question(&[("a", 1)]));
        assert_eq!(
            m.handle_key(key(KeyCode::Esc)),
            InterruptionModalAction::Cancel
        );
        assert_eq!(
            m.handle_key(ctrl(KeyCode::Char('c'))),
            InterruptionModalAction::Cancel
        );
    }

    #[test]
    fn m_key_requests_comment_editor() {
        let mut m = InterruptionModal::from_interruption(&question(&[("a", 1)]));
        assert_eq!(
            m.handle_key(key(KeyCode::Char('m'))),
            InterruptionModalAction::EditComment
        );
    }
}
