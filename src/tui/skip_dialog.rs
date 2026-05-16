// Skip-change-handling dialog (STEP 18).
//
// When the user presses `s` on a step in the plan-detail step list AND there
// is uncommitted work attributable to that step (a running step whose harness
// just got killed, or a `Failed` step that left a dirty tree behind), the
// dispatcher opens this dialog so the user chooses what to do with the work
// the killed/failed harness left behind:
//
//   Stash (recoverable)   [default]
//   Commit WIP
//   Discard
//
// Navigation/render are delegated to the generic [`Choice`] primitive (the
// same split `run_dialog.rs` uses): `j`/`k`/`↑`/`↓` move (clamped, no wrap),
// `Enter` confirms the focused row, `Esc`/`Ctrl-C` cancel.
//
// **Esc is not "do nothing".** Per the step-18 spec, cancelling the dialog
// while a step is *running* must roll the tree back, emit an
// `attempt_cancelled` NDJSON event, and re-enter the executor at the same
// attempt number — consuming no retry budget. The dialog itself only reports
// the user's decision; the caller maps a confirmed [`SkipChoice`] (or the
// `Cancelled` outcome) onto a [`crate::git::ParkStrategyKind`] and drives the
// skip plumbing.

use crossterm::event::KeyEvent;
use ratatui::Frame;
use ratatui::layout::Rect;

use super::choice::{Choice, ChoiceItem, ChoiceOutcome};
use crate::git::ParkStrategyKind;

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// The three rows of the skip dialog, top to bottom. Default-highlighted row
/// is [`SkipChoice::Stash`] (the recoverable, non-destructive option).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipChoice {
    /// `git stash push --include-untracked` — recoverable later.
    Stash,
    /// `git add -A && git commit` a WIP snapshot with a skip trailer.
    Commit,
    /// Throw the killed/failed harness's work away.
    Discard,
}

impl SkipChoice {
    /// Map a confirmed choice onto the registry-carried strategy kind the
    /// skip plumbing consumes. The TUI-only [`ParkStrategyKind::Cancel`] is
    /// **not** produced here — it corresponds to the dialog's `Esc`/cancel
    /// outcome, which the caller handles separately.
    pub fn to_park_kind(self) -> ParkStrategyKind {
        match self {
            SkipChoice::Stash => ParkStrategyKind::Stash,
            SkipChoice::Commit => ParkStrategyKind::Commit,
            SkipChoice::Discard => ParkStrategyKind::Discard,
        }
    }
}

impl ChoiceItem for SkipChoice {
    fn label(&self) -> String {
        match self {
            SkipChoice::Stash => "Stash (recoverable)".to_string(),
            SkipChoice::Commit => "Commit WIP".to_string(),
            SkipChoice::Discard => "Discard".to_string(),
        }
    }
}

/// Result of feeding one key to a [`SkipDialog`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SkipOutcome {
    /// Key consumed (navigation or unrecognized); dialog stays open.
    Pending,
    /// User pressed `Esc`/`Ctrl-C`. Per the step-18 spec this maps to
    /// [`ParkStrategyKind::Cancel`] for a *running* step (roll back +
    /// re-enter the executor at the same attempt, no budget consumed).
    Cancelled,
    /// User confirmed a row with `Enter`.
    Confirmed(SkipChoice),
}

/// Skip dialog state. Construct with [`SkipDialog::new`] (default focus =
/// Stash) and feed key events to [`SkipDialog::handle_key`] until it returns
/// a terminal [`SkipOutcome`]. The navigable list is the generic
/// [`Choice`] primitive; this struct only translates its outcome.
pub struct SkipDialog {
    /// The generic vertical single-select list backing the dialog.
    pub choice: Choice<SkipChoice>,
}

impl Default for SkipDialog {
    fn default() -> Self {
        Self::new()
    }
}

impl SkipDialog {
    /// Build the dialog with `Stash` (index 0) highlighted by default.
    pub fn new() -> Self {
        Self {
            choice: Choice::new(
                vec![SkipChoice::Stash, SkipChoice::Commit, SkipChoice::Discard],
                0,
            ),
        }
    }

    /// Process one key event, translating the generic [`ChoiceOutcome`] onto
    /// this dialog's [`SkipOutcome`].
    pub fn handle_key(&mut self, key: KeyEvent) -> SkipOutcome {
        match self.choice.handle_key(key) {
            ChoiceOutcome::Pending => SkipOutcome::Pending,
            ChoiceOutcome::Cancelled => SkipOutcome::Cancelled,
            ChoiceOutcome::Confirmed(c) => SkipOutcome::Confirmed(c),
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Draw the dialog as a centered overlay over `area`. The caller renders the
/// background view first; the generic primitive's `Clear` blanks just the
/// dialog rectangle.
pub fn render(frame: &mut Frame, area: Rect, dialog: &SkipDialog) {
    super::choice::render(frame, area, " Skip step — keep changes? ", &dialog.choice);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyCode, KeyModifiers};

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_with(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    #[test]
    fn default_focus_is_stash() {
        let d = SkipDialog::new();
        assert_eq!(d.choice.focused(), Some(&SkipChoice::Stash));
        assert_eq!(d.choice.focused_index(), 0);
    }

    #[test]
    fn rows_are_stash_commit_discard_in_order() {
        let d = SkipDialog::new();
        assert_eq!(
            d.choice.choices,
            vec![SkipChoice::Stash, SkipChoice::Commit, SkipChoice::Discard]
        );
    }

    #[test]
    fn enter_on_default_confirms_stash() {
        let mut d = SkipDialog::new();
        assert_eq!(
            d.handle_key(key(KeyCode::Enter)),
            SkipOutcome::Confirmed(SkipChoice::Stash)
        );
    }

    #[test]
    fn j_then_enter_confirms_commit() {
        let mut d = SkipDialog::new();
        assert_eq!(d.handle_key(key(KeyCode::Char('j'))), SkipOutcome::Pending);
        assert_eq!(
            d.handle_key(key(KeyCode::Enter)),
            SkipOutcome::Confirmed(SkipChoice::Commit)
        );
    }

    #[test]
    fn down_twice_then_enter_confirms_discard() {
        let mut d = SkipDialog::new();
        d.handle_key(key(KeyCode::Down));
        d.handle_key(key(KeyCode::Down));
        assert_eq!(
            d.handle_key(key(KeyCode::Enter)),
            SkipOutcome::Confirmed(SkipChoice::Discard)
        );
    }

    #[test]
    fn k_clamps_at_top_and_enter_still_confirms_stash() {
        let mut d = SkipDialog::new();
        // Already on the first row; k/↑ must not wrap to Discard.
        d.handle_key(key(KeyCode::Char('k')));
        d.handle_key(key(KeyCode::Up));
        assert_eq!(d.choice.focused(), Some(&SkipChoice::Stash));
        assert_eq!(
            d.handle_key(key(KeyCode::Enter)),
            SkipOutcome::Confirmed(SkipChoice::Stash)
        );
    }

    #[test]
    fn j_clamps_at_bottom() {
        let mut d = SkipDialog::new();
        for _ in 0..5 {
            d.handle_key(key(KeyCode::Char('j')));
        }
        assert_eq!(d.choice.focused(), Some(&SkipChoice::Discard));
    }

    #[test]
    fn esc_returns_cancelled() {
        let mut d = SkipDialog::new();
        assert_eq!(d.handle_key(key(KeyCode::Esc)), SkipOutcome::Cancelled);
    }

    #[test]
    fn ctrl_c_returns_cancelled() {
        let mut d = SkipDialog::new();
        assert_eq!(
            d.handle_key(key_with(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            SkipOutcome::Cancelled
        );
    }

    #[test]
    fn unrecognized_key_is_pending_and_does_not_move() {
        let mut d = SkipDialog::new();
        for code in [
            KeyCode::Char('x'),
            KeyCode::Tab,
            KeyCode::Left,
            KeyCode::Right,
        ] {
            assert_eq!(d.handle_key(key(code)), SkipOutcome::Pending);
            assert_eq!(d.choice.focused_index(), 0);
        }
    }

    #[test]
    fn confirmed_choices_map_to_park_kinds() {
        assert_eq!(SkipChoice::Stash.to_park_kind(), ParkStrategyKind::Stash);
        assert_eq!(SkipChoice::Commit.to_park_kind(), ParkStrategyKind::Commit);
        assert_eq!(
            SkipChoice::Discard.to_park_kind(),
            ParkStrategyKind::Discard
        );
    }

    #[test]
    fn render_smoke_writes_all_labels_and_title() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(50, 10);
        let mut terminal = Terminal::new(backend).unwrap();
        let dialog = SkipDialog::new();
        terminal
            .draw(|f| {
                let area = f.area();
                render(f, area, &dialog);
            })
            .unwrap();
        let buf = terminal.backend().buffer().clone();
        let dump: String = (0..buf.area().height)
            .map(|y| {
                (0..buf.area().width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(dump.contains("Skip step"), "title missing:\n{dump}");
        assert!(dump.contains("Stash"), "Stash missing:\n{dump}");
        assert!(dump.contains("Commit WIP"), "Commit missing:\n{dump}");
        assert!(dump.contains("Discard"), "Discard missing:\n{dump}");
    }
}
