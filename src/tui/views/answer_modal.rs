// Interruption answer modal + resume-implementation modal (TUI-plan.md §17,
// docs/dag-redesign.md §12.4).
//
// The answer flow step-detail surfaces when the user presses `a` on a focused
// open question is driven by the §12.4 `InterruptionModal` (the same ranked-
// options modal the inbox uses), built from the step's `storage::OpenQuestion`
// via [`InterruptionModal::from_open_question`]. The resume modal pops after
// the *last* open question for the plan is answered, asking whether to kick
// off `ralph run` immediately. Both modals are pure state — the dispatcher
// owns the storage write and the runner spawn.
//
// Tests live in this module so the state-machine surface is exercised
// without a real terminal.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

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
    /// Whether the human has explicitly engaged the option list (navigated
    /// it with j/k). The agent's #1 is *pre-selected* for convenience, but
    /// §12.4 forbids an implicit "agent decides": pressing Enter after
    /// Tab-ing away to an empty freeform must NOT silently accept #1. This
    /// records that the human actually interacted with the ranked list, so
    /// the option remains a valid submission even once focus moves off it
    /// (e.g. to add a Comment) — without turning the mere pre-selection into
    /// an auto-answer.
    pub option_touched: bool,
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
            option_touched: false,
        }
    }

    /// Build a modal from a step-detail [`storage::OpenQuestion`]. Step-detail
    /// has an `OpenQuestion` rather than a full [`Interruption`], but the modal
    /// surface is identical: `q.suggestions` are already in priority order, so
    /// they map to `options` with `priority = index + 1`. Mirrors the
    /// `focus`/field defaults of [`Self::from_interruption`] (Options focus when
    /// there are options, else Freeform).
    pub fn from_open_question(q: &crate::storage::OpenQuestion) -> Self {
        let options: Vec<InterruptionOption> = q
            .suggestions
            .iter()
            .enumerate()
            .map(|(i, text)| InterruptionOption {
                text: text.clone(),
                priority: (i + 1) as i32,
            })
            .collect();
        let focus = if options.is_empty() {
            InterruptionFocus::Freeform
        } else {
            InterruptionFocus::Options
        };
        Self {
            interruption_id: q.id.clone(),
            kind: q.kind,
            body: q.question.clone(),
            options,
            selected_option: 0,
            focus,
            freeform: String::new(),
            comment: String::new(),
            option_touched: false,
        }
    }

    /// Whether this is a blocker (no options — resolve / resolve-with-comment
    /// only, §12.4).
    pub fn is_blocker(&self) -> bool {
        self.kind == InterruptionKind::Blocker
    }

    /// The currently-chosen resolution text. A typed freeform answer is the
    /// deliberate §12.4 escape hatch and usually wins, but when focus is back
    /// on the option list the highlighted option is the active choice: a user
    /// who types something, then re-focuses the ranked list and presses Enter
    /// expects the selected option to win. Focus leaving `Options` still must
    /// not discard the selected option entirely — picking an option then
    /// Tab-ing to the Comment field to add a note is valid and should submit
    /// that option when freeform is blank. `None` only when there is genuinely
    /// nothing chosen (no options *and* empty freeform — e.g. a blocker the
    /// human hasn't written a resolution for).
    pub fn chosen_resolution(&self) -> Option<String> {
        if self.focus == InterruptionFocus::Options {
            return self
                .options
                .get(self.selected_option)
                .map(|o| o.text.clone());
        }

        let freeform = self.freeform.trim();
        if !freeform.is_empty() {
            return Some(freeform.to_string());
        }
        self.options
            .get(self.selected_option)
            .map(|o| o.text.clone())
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
                self.option_touched = true;
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
                self.option_touched = true;
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
            KeyCode::Enter => {
                // §12.4: an explicit human answer is required — no implicit
                // "agent decides" path. The pre-selected #1 alone is NOT a
                // decision; a submission is explicit only when the human
                // either typed a freeform answer, engaged the ranked list
                // (navigated it with j/k), or pressed Enter while focused on
                // the option list (accepting the highlight directly). This
                // gate is intentionally separate from `chosen_resolution()`
                // (which stays focus-independent so the option survives focus
                // moving to the Comment field, and so the renderer can show
                // the current highlight).
                let explicit = !self.freeform.trim().is_empty()
                    || self.option_touched
                    || self.focus == InterruptionFocus::Options;
                match self.chosen_resolution() {
                    Some(resolution) if explicit => InterruptionModalAction::Resolve {
                        interruption_id: self.interruption_id.clone(),
                        resolution,
                        comment: self.comment_opt(),
                    },
                    _ => InterruptionModalAction::Pending,
                }
            }
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
    fn from_open_question_maps_suggestions_to_priority_options() {
        // Step-detail builds the modal from an `OpenQuestion` whose
        // `suggestions` are already in priority order; they map to options
        // with `priority = index + 1`, Options focus, #1 pre-selected.
        let q = crate::storage::OpenQuestion {
            id: "q-step0-pick".to_string(),
            step_id: "step0".to_string(),
            plan_id: "p1".to_string(),
            plan_slug: "plan".to_string(),
            step_num: 1,
            step_title: "Step".to_string(),
            attempt: 1,
            question: "Pick a crate".to_string(),
            suggestions: vec!["tracing".to_string(), "log".to_string()],
            kind: InterruptionKind::Question,
            asked_at: "2026-05-04T00:00:00Z".to_string(),
        };
        let m = InterruptionModal::from_open_question(&q);
        assert_eq!(m.interruption_id, "q-step0-pick");
        assert_eq!(m.body, "Pick a crate");
        assert_eq!(m.kind, InterruptionKind::Question);
        assert_eq!(
            m.options
                .iter()
                .map(|o| (o.text.as_str(), o.priority))
                .collect::<Vec<_>>(),
            vec![("tracing", 1), ("log", 2)]
        );
        assert_eq!(m.selected_option, 0);
        assert_eq!(m.focus, InterruptionFocus::Options);
        assert_eq!(m.chosen_resolution().as_deref(), Some("tracing"));
    }

    #[test]
    fn from_open_question_with_no_suggestions_focuses_freeform() {
        let q = crate::storage::OpenQuestion {
            id: "q-step0-open".to_string(),
            step_id: "step0".to_string(),
            plan_id: "p1".to_string(),
            plan_slug: "plan".to_string(),
            step_num: 1,
            step_title: "Step".to_string(),
            attempt: 1,
            question: "Open-ended?".to_string(),
            suggestions: vec![],
            kind: InterruptionKind::Question,
            asked_at: "2026-05-04T00:00:00Z".to_string(),
        };
        let m = InterruptionModal::from_open_question(&q);
        assert!(m.options.is_empty());
        assert_eq!(m.focus, InterruptionFocus::Freeform);
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
    fn selected_option_survives_focus_moving_to_comment_field() {
        // Regression: the user picks option #2, then Tab-s off the option
        // list to the Comment field to add a note, then Enter. Previously
        // `chosen_resolution()` keyed off `focus`, so once focus left
        // `Options` it fell back to the (empty) freeform → `None` → Enter
        // rejected (Pending), silently discarding the selected option. The
        // resolution must be focus-independent.
        let mut m = InterruptionModal::from_interruption(&question(&[("opt-a", 1), ("opt-b", 2)]));
        assert_eq!(
            m.handle_key(key(KeyCode::Char('j'))),
            InterruptionModalAction::Pending,
            "navigate to opt-b"
        );
        // Tab Options -> Freeform -> Comment (focus now off the option list).
        m.handle_key(key(KeyCode::Tab));
        m.handle_key(key(KeyCode::Tab));
        assert_eq!(m.focus, InterruptionFocus::Comment);
        m.comment = "also check perf".to_string();
        assert_eq!(
            m.handle_key(key(KeyCode::Enter)),
            InterruptionModalAction::Resolve {
                interruption_id: "i1".to_string(),
                resolution: "opt-b".to_string(),
                comment: Some("also check perf".to_string()),
            },
            "selected option + comment must submit even with focus off Options"
        );
    }

    #[test]
    fn enter_on_focused_option_list_accepts_preselected_one() {
        // The intended fast path: a question opens focused on the ranked
        // list with #1 pre-selected; pressing Enter right there is an
        // explicit human accept of #1 (the human IS on the list). This is
        // distinct from Tab-ing away to freeform and pressing Enter (which
        // must stay Pending — see no_agent_decide_shortcut_*).
        let mut m = InterruptionModal::from_interruption(&question(&[("best", 1), ("other", 2)]));
        assert_eq!(m.focus, InterruptionFocus::Options);
        assert_eq!(
            m.handle_key(key(KeyCode::Enter)),
            InterruptionModalAction::Resolve {
                interruption_id: "i1".to_string(),
                resolution: "best".to_string(),
                comment: None,
            }
        );
    }

    #[test]
    fn focused_options_override_stale_freeform_text() {
        let mut m = InterruptionModal::from_interruption(&question(&[("best", 1), ("fail", 2)]));
        m.freeform = "stale hint".to_string();
        assert_eq!(m.focus, InterruptionFocus::Options);
        assert_eq!(
            m.handle_key(key(KeyCode::Enter)),
            InterruptionModalAction::Resolve {
                interruption_id: "i1".to_string(),
                resolution: "best".to_string(),
                comment: None,
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

    /// Phase C: the Phase B auto-raised retry-exhausted blocker is a
    /// **Blocker kind with two ranked options** (Retry / Fail). The modal
    /// must open focused on Options (so Enter immediately accepts #1 =
    /// "Retry step with parked changes") and the chosen resolution must be the
    /// priority-1 option text — the exact string the Phase C resolution
    /// helper matches against to reset attempts. Guards the §12.4 default-
    /// option UX from regressing for the auto-blocker shape.
    #[test]
    fn test_inbox_modal_renders_options_for_auto_blocker() {
        let auto_blocker = Interruption {
            id: "ab1".to_string(),
            step_id: "s1".to_string(),
            attempt: 3,
            kind: InterruptionKind::Blocker,
            body: "Step failed after 3 attempts.".to_string(),
            options: vec![
                InterruptionOption {
                    text: "Retry step with parked changes".to_string(),
                    priority: 1,
                },
                InterruptionOption {
                    text: "Mark step Failed".to_string(),
                    priority: 2,
                },
            ],
            resolution: None,
            comment: None,
            state: InterruptionState::Open,
            asked_at: Utc::now(),
            resolved_at: None,
        };

        let mut m = InterruptionModal::from_interruption(&auto_blocker);
        // A blocker that DOES carry options opens focused on Options
        // (priority 1 pre-selected) — not the empty-options Freeform
        // fallback path.
        assert!(m.is_blocker());
        assert_eq!(m.options.len(), 2);
        assert_eq!(
            m.focus,
            InterruptionFocus::Options,
            "auto-blocker has options → Options focus, not Freeform"
        );
        assert_eq!(m.selected_option, 0, "priority 1 pre-selected");
        // Pressing Enter on the Options focus immediately resolves with
        // the priority-1 option text — the contract the resolution helper
        // string-matches against.
        assert_eq!(
            m.handle_key(key(KeyCode::Enter)),
            InterruptionModalAction::Resolve {
                interruption_id: "ab1".to_string(),
                resolution: "Retry step with parked changes".to_string(),
                comment: None,
            }
        );
    }
}
