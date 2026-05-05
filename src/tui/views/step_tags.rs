// Step-tag editor sub-view (TUI-plan.md §1, step 36).
//
// A focused sub-view over the step-detail screen that lets the user add and
// remove the free-form `step.tags` strings. Entered via the `/step edit
// --tags` palette command or a keybinding from step_detail.
//
// The state machine has two modes mirroring the spec from step 36:
//   * `List` shows the current working tag list as a `ratatui::Table`. The
//     keybindings `i` (add), `d` (remove), `Enter`/`q` (save), `Esc`
//     (discard) live here.
//   * `Input` shows a centered text-input modal triggered by `i`. Enter
//     accepts the typed tag (after trim + non-empty + dedup validation) and
//     returns to `List`; Esc abandons the input without committing it.
//
// The view is split into a pure state machine (`StepTagsApp`) and a renderer
// (`render`) so we can drive the state machine in tests without a real
// terminal. The actual write-through call to
// [`crate::storage::update_step_fields_ext`] lives in the dispatcher loop:
// the state machine surfaces the final tag list via [`Outcome::SaveAndPop`]
// so the caller can persist it and refresh the surrounding view. The
// alternative termination, [`Outcome::DiscardAndPop`], skips the storage
// write entirely and matches the user's `<esc>`-to-cancel mental model.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState};

use crate::tui::help::{self, HelpState};
use crate::tui::theme;
use crate::tui::toast::{ToastKind, ToastQueue};

// ---------------------------------------------------------------------------
// Sub-view state
// ---------------------------------------------------------------------------

/// Whether the sub-view is showing the working tag list or the add-tag input
/// overlay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mode {
    /// Default mode: tag table with `i`/`d`/`Enter`/`q`/`<esc>` keybindings.
    List,
    /// Add-tag overlay opened by `i`. Carries the in-progress text buffer so
    /// the renderer can show whatever the user has typed so far.
    Input { buffer: String },
}

/// What [`StepTagsApp::handle_key`] returns each turn. The dispatcher loop
/// runs the side effect (storage write, refresh, pop view) and keeps looping
/// on `Pending`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Key consumed; no side effect required.
    Pending,
    /// User pressed `Enter` / `q` in list mode — persist the working tag list
    /// via [`crate::storage::update_step_fields_ext`] and pop the sub-view.
    SaveAndPop { tags: Vec<String> },
    /// User pressed `<esc>` in list mode (or Ctrl-C anywhere) — pop without
    /// touching the database. Mirrors the universal "esc cancels" rule.
    DiscardAndPop,
}

/// Sub-view state.
pub struct StepTagsApp {
    /// The step whose tags are being edited. Carried so the dispatcher loop
    /// can wire the [`crate::storage::update_step_fields_ext`] call without
    /// re-resolving focus.
    pub step_id: String,
    /// Display slug of the parent plan, used in the title bar.
    pub plan_slug: String,
    /// Display label for the step (e.g. `#3 — Step title`), used in the
    /// title bar so the user always knows which step they're scoped to.
    pub step_label: String,
    /// Working tag list. Starts as a clone of `step.tags`; mutated locally
    /// until the user commits via Enter/q (in which case the dispatcher
    /// writes back) or cancels via Esc (in which case it's dropped).
    pub tags: Vec<String>,
    /// 0-based cursor in the tag list.
    pub list_cursor: usize,
    /// Current mode (List or Input { buffer }).
    pub mode: Mode,
    /// Toast queue rendered over the bottom hint row.
    pub toasts: ToastQueue,
    /// Help-overlay state. `?` toggles visibility; while visible the
    /// dispatcher routes input through [`HelpState::intercept_key`] before
    /// passing keys to the per-mode handler (TUI-plan.md §15).
    pub help: HelpState,
}

impl StepTagsApp {
    /// Build a new sub-view seeded with the step's current tag list.
    pub fn new(step_id: String, plan_slug: String, step_label: String, tags: Vec<String>) -> Self {
        Self {
            step_id,
            plan_slug,
            step_label,
            tags,
            list_cursor: 0,
            mode: Mode::List,
            toasts: ToastQueue::new(),
            help: HelpState::new(),
        }
    }

    /// Push a toast onto the queue using the system clock for `expires_at`.
    pub fn push_toast(&mut self, msg: impl Into<String>, kind: ToastKind) {
        self.toasts.push(msg, kind, std::time::Instant::now());
    }

    /// Pure key handler. Routes to the per-mode handler so tests can drive
    /// arbitrary key sequences without crossterm.
    pub fn handle_key(&mut self, key: KeyEvent) -> Outcome {
        // §15 help overlay: route `?` toggle / dismissal first. While the
        // overlay is up the sub-view's per-mode handlers are skipped.
        if self.help.intercept_key(key) != help::InterceptResult::Passthrough {
            return Outcome::Pending;
        }

        // Ctrl-C always discards and pops — matches the convention in
        // plan_hooks/step_hooks where Ctrl-C is the universal escape hatch.
        if let KeyCode::Char('c') = key.code
            && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            return Outcome::DiscardAndPop;
        }
        match &self.mode {
            Mode::List => self.handle_list_key(key),
            Mode::Input { .. } => self.handle_input_key(key),
        }
    }

    fn handle_list_key(&mut self, key: KeyEvent) -> Outcome {
        match key.code {
            // Navigation
            KeyCode::Char('j') | KeyCode::Down => {
                if !self.tags.is_empty() && self.list_cursor + 1 < self.tags.len() {
                    self.list_cursor += 1;
                }
                Outcome::Pending
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if self.list_cursor > 0 {
                    self.list_cursor -= 1;
                }
                Outcome::Pending
            }
            KeyCode::Char('g') | KeyCode::Home => {
                if !self.tags.is_empty() {
                    self.list_cursor = 0;
                }
                Outcome::Pending
            }
            KeyCode::Char('G') | KeyCode::End => {
                if !self.tags.is_empty() {
                    self.list_cursor = self.tags.len() - 1;
                }
                Outcome::Pending
            }

            // Open the add-tag input modal.
            KeyCode::Char('i') => {
                self.mode = Mode::Input {
                    buffer: String::new(),
                };
                Outcome::Pending
            }

            // Remove the highlighted tag and clamp the cursor back into
            // range. Empty list collapses to 0; off-the-end pulls back by
            // one so the cursor stays on a real row.
            KeyCode::Char('d') => {
                if self.list_cursor < self.tags.len() {
                    self.tags.remove(self.list_cursor);
                    if self.tags.is_empty() {
                        self.list_cursor = 0;
                    } else if self.list_cursor >= self.tags.len() {
                        self.list_cursor = self.tags.len() - 1;
                    }
                }
                Outcome::Pending
            }

            // Save the working tag list and pop.
            KeyCode::Enter | KeyCode::Char('q') => Outcome::SaveAndPop {
                tags: self.tags.clone(),
            },

            // Discard and pop.
            KeyCode::Esc => Outcome::DiscardAndPop,

            _ => Outcome::Pending,
        }
    }

    fn handle_input_key(&mut self, key: KeyEvent) -> Outcome {
        // Borrow the input buffer for the lifetime of this match. Because
        // every arm either commits (transitioning back to List) or stays in
        // Input mode, we mutate through a temporary `String` and reassign
        // the mode at the end when needed.
        match key.code {
            // Submit the buffer.
            KeyCode::Enter => {
                let Mode::Input { buffer } = &self.mode else {
                    return Outcome::Pending;
                };
                let candidate = buffer.trim().to_string();
                if candidate.is_empty() {
                    self.push_toast("Tag cannot be empty.", ToastKind::Error);
                    return Outcome::Pending;
                }
                if self.tags.iter().any(|t| t == &candidate) {
                    self.push_toast(
                        format!("Tag `{candidate}` already attached."),
                        ToastKind::Error,
                    );
                    return Outcome::Pending;
                }
                self.tags.push(candidate);
                self.list_cursor = self.tags.len() - 1;
                self.mode = Mode::List;
                Outcome::Pending
            }

            // Cancel the input without committing.
            KeyCode::Esc => {
                self.mode = Mode::List;
                Outcome::Pending
            }

            // Edit the buffer.
            KeyCode::Backspace => {
                if let Mode::Input { buffer } = &mut self.mode {
                    buffer.pop();
                }
                Outcome::Pending
            }
            KeyCode::Char(c) => {
                // Ignore Ctrl/Alt modifiers so e.g. Ctrl-A doesn't insert a
                // literal "a"; Ctrl-C is already handled in `handle_key`.
                if key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                {
                    return Outcome::Pending;
                }
                if let Mode::Input { buffer } = &mut self.mode {
                    buffer.push(c);
                }
                Outcome::Pending
            }

            _ => Outcome::Pending,
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

const LIST_HINT: &str = " [i] add   [d] remove   [Enter/q] save   [Esc] cancel ";
const INPUT_HINT: &str = " [Enter] add   [Esc] cancel ";

/// Draw the tag table over `area`. When the input modal is open, render the
/// modal overlay on top.
pub fn render(frame: &mut Frame, area: Rect, app: &mut StepTagsApp) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    // -- Title + hint row --------------------------------------------------
    let title = format!(" Step tags — {} / {} ", app.plan_slug, app.step_label);
    let block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));
    let inner = block.inner(area);
    frame.render_widget(block, area);

    if inner.width == 0 || inner.height == 0 {
        return;
    }

    // Reserve the bottom row for the hint / toast.
    let body_h = inner.height.saturating_sub(1);
    let body_area = Rect {
        x: inner.x,
        y: inner.y,
        width: inner.width,
        height: body_h,
    };
    let hint_area = Rect {
        x: inner.x,
        y: inner.y + body_h,
        width: inner.width,
        height: 1,
    };

    // -- Body: tag table --------------------------------------------------
    if app.tags.is_empty() {
        let para = Paragraph::new(Line::from(Span::styled(
            "(no tags — press `i` to add one)",
            Style::default().fg(theme::CHROME_DIM),
        )));
        frame.render_widget(para, body_area);
    } else {
        let header = Row::new(vec![Cell::from("#"), Cell::from("Tag")])
            .style(Style::default().add_modifier(Modifier::BOLD))
            .bottom_margin(0);
        let rows: Vec<Row> = app
            .tags
            .iter()
            .enumerate()
            .map(|(i, tag)| {
                Row::new(vec![
                    Cell::from(format!("{}", i + 1)),
                    Cell::from(tag.clone()),
                ])
            })
            .collect();
        let widths = [Constraint::Length(4), Constraint::Min(1)];
        let table = Table::new(rows, widths)
            .header(header)
            .row_highlight_style(
                Style::default()
                    .fg(theme::CURSOR)
                    .add_modifier(Modifier::REVERSED)
                    .add_modifier(Modifier::BOLD),
            )
            .highlight_symbol("");
        let mut state = TableState::default();
        state.select(Some(app.list_cursor.min(app.tags.len() - 1)));
        frame.render_stateful_widget(table, body_area, &mut state);
    }

    // -- Hint / toast row -------------------------------------------------
    let now = std::time::Instant::now();
    app.toasts.prune(now);
    let hint_line = if let Some(toast) = app.toasts.current() {
        Line::from(Span::styled(
            toast.text.clone(),
            Style::default()
                .fg(toast.color)
                .add_modifier(Modifier::BOLD),
        ))
    } else {
        let hint = match app.mode {
            Mode::List => LIST_HINT,
            Mode::Input { .. } => INPUT_HINT,
        };
        Line::from(Span::styled(hint, Style::default().fg(theme::CHROME_DIM)))
    };
    let hint = Paragraph::new(hint_line);
    frame.render_widget(hint, hint_area);

    // -- Input overlay ----------------------------------------------------
    if let Mode::Input { buffer } = &app.mode {
        render_input_modal(frame, area, buffer);
    }

    // -- Help overlay -----------------------------------------------------
    if app.help.is_visible() {
        help::render(frame, area, &help::for_step_tags());
    }
}

fn render_input_modal(frame: &mut Frame, area: Rect, buffer: &str) {
    let dialog = centered_input_rect(area);
    if dialog.width == 0 || dialog.height == 0 {
        return;
    }
    frame.render_widget(Clear, dialog);

    let block = Block::default()
        .title(" Add tag ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));

    let label_style = Style::default()
        .fg(theme::CURSOR)
        .add_modifier(Modifier::BOLD);
    let input_line = Line::from(vec![
        Span::styled("Tag: ", label_style),
        Span::raw(buffer.to_string()),
        Span::styled(
            "▌",
            Style::default()
                .fg(theme::CURSOR)
                .add_modifier(Modifier::SLOW_BLINK),
        ),
    ]);

    let lines = vec![
        input_line,
        Line::from(""),
        Line::styled(INPUT_HINT, Style::default().add_modifier(Modifier::BOLD)),
    ];

    let para = Paragraph::new(lines).block(block);
    frame.render_widget(para, dialog);
}

fn centered_input_rect(area: Rect) -> Rect {
    let desired_w = 60u16.min(area.width);
    let desired_h = 5u16.min(area.height);
    if desired_w == 0 || desired_h == 0 {
        return Rect {
            x: area.x,
            y: area.y,
            width: 0,
            height: 0,
        };
    }
    let [vert] = Layout::vertical([Constraint::Length(desired_h)])
        .flex(Flex::Center)
        .areas(area);
    let [horiz] = Layout::horizontal([Constraint::Length(desired_w)])
        .flex(Flex::Center)
        .areas(vert);
    horiz
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db;
    use crate::storage;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_with_mod(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    fn app_with(tags: Vec<&str>) -> StepTagsApp {
        StepTagsApp::new(
            "s1".into(),
            "parent".into(),
            "#1 — Step".into(),
            tags.into_iter().map(String::from).collect(),
        )
    }

    /// Type a string into the input buffer one character at a time.
    fn type_str(app: &mut StepTagsApp, s: &str) {
        for c in s.chars() {
            assert_eq!(app.handle_key(key(KeyCode::Char(c))), Outcome::Pending);
        }
    }

    // -- Pure state-machine tests ----------------------------------------

    #[test]
    fn new_starts_in_list_mode_with_zero_cursor() {
        let app = app_with(vec!["FIX", "review"]);
        assert_eq!(app.mode, Mode::List);
        assert_eq!(app.list_cursor, 0);
        assert_eq!(app.tags, vec!["FIX".to_string(), "review".to_string()]);
    }

    #[test]
    fn j_moves_list_cursor_down_clamped() {
        let mut app = app_with(vec!["a", "b"]);
        assert_eq!(app.handle_key(key(KeyCode::Char('j'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 1);
        assert_eq!(app.handle_key(key(KeyCode::Char('j'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 1);
    }

    #[test]
    fn k_moves_list_cursor_up_clamped() {
        let mut app = app_with(vec!["a", "b"]);
        app.list_cursor = 1;
        assert_eq!(app.handle_key(key(KeyCode::Char('k'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 0);
        assert_eq!(app.handle_key(key(KeyCode::Char('k'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 0);
    }

    #[test]
    fn g_jumps_to_top_capital_g_to_bottom() {
        let mut app = app_with(vec!["a", "b", "c"]);
        app.handle_key(key(KeyCode::Char('G')));
        assert_eq!(app.list_cursor, 2);
        app.handle_key(key(KeyCode::Char('g')));
        assert_eq!(app.list_cursor, 0);
    }

    // -- Add via input modal ---------------------------------------------

    #[test]
    fn i_opens_input_modal_with_empty_buffer() {
        let mut app = app_with(vec![]);
        assert_eq!(app.handle_key(key(KeyCode::Char('i'))), Outcome::Pending);
        match &app.mode {
            Mode::Input { buffer } => assert_eq!(buffer, ""),
            other => panic!("expected Input mode, got {other:?}"),
        }
    }

    #[test]
    fn input_typing_appends_to_buffer() {
        let mut app = app_with(vec![]);
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "FIX");
        match &app.mode {
            Mode::Input { buffer } => assert_eq!(buffer, "FIX"),
            other => panic!("expected Input mode, got {other:?}"),
        }
    }

    #[test]
    fn input_backspace_pops_last_char() {
        let mut app = app_with(vec![]);
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "abc");
        app.handle_key(key(KeyCode::Backspace));
        match &app.mode {
            Mode::Input { buffer } => assert_eq!(buffer, "ab"),
            other => panic!("expected Input mode, got {other:?}"),
        }
    }

    #[test]
    fn input_enter_appends_tag_returns_to_list() {
        let mut app = app_with(vec!["FIX"]);
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "review");
        assert_eq!(app.handle_key(key(KeyCode::Enter)), Outcome::Pending);
        assert_eq!(app.mode, Mode::List);
        assert_eq!(app.tags, vec!["FIX".to_string(), "review".to_string()]);
        // Cursor jumps to the newly-added tag.
        assert_eq!(app.list_cursor, 1);
    }

    #[test]
    fn input_enter_with_empty_buffer_toasts_and_stays_in_input() {
        let mut app = app_with(vec![]);
        app.handle_key(key(KeyCode::Char('i')));
        assert_eq!(app.handle_key(key(KeyCode::Enter)), Outcome::Pending);
        assert!(matches!(app.mode, Mode::Input { .. }));
        let toast = app.toasts.current().expect("toast pushed");
        assert!(toast.text.contains("empty"), "got: {}", toast.text);
        assert!(app.tags.is_empty());
    }

    #[test]
    fn input_enter_with_whitespace_only_buffer_is_rejected() {
        // The trim happens before the empty check, so all-whitespace input
        // takes the same "empty" path rather than slipping through.
        let mut app = app_with(vec![]);
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "   ");
        assert_eq!(app.handle_key(key(KeyCode::Enter)), Outcome::Pending);
        assert!(matches!(app.mode, Mode::Input { .. }));
        assert!(app.tags.is_empty());
    }

    #[test]
    fn input_enter_with_duplicate_toasts_and_stays_in_input() {
        let mut app = app_with(vec!["FIX"]);
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "FIX");
        assert_eq!(app.handle_key(key(KeyCode::Enter)), Outcome::Pending);
        assert!(matches!(app.mode, Mode::Input { .. }));
        let toast = app.toasts.current().expect("toast pushed");
        assert!(
            toast.text.contains("already attached"),
            "got: {}",
            toast.text
        );
        assert_eq!(app.tags, vec!["FIX".to_string()]);
    }

    #[test]
    fn input_trims_buffer_before_committing() {
        let mut app = app_with(vec![]);
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "  needs-review  ");
        app.handle_key(key(KeyCode::Enter));
        assert_eq!(app.tags, vec!["needs-review".to_string()]);
    }

    #[test]
    fn input_esc_cancels_returns_to_list_without_adding() {
        let mut app = app_with(vec!["FIX"]);
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "review");
        assert_eq!(app.handle_key(key(KeyCode::Esc)), Outcome::Pending);
        assert_eq!(app.mode, Mode::List);
        assert_eq!(app.tags, vec!["FIX".to_string()]);
    }

    #[test]
    fn input_ignores_ctrl_modified_chars() {
        // Ctrl-A shouldn't insert a literal "a" — protects callers from
        // having to special-case shortcut keys in their dispatcher loop.
        let mut app = app_with(vec![]);
        app.handle_key(key(KeyCode::Char('i')));
        app.handle_key(key_with_mod(KeyCode::Char('a'), KeyModifiers::CONTROL));
        match &app.mode {
            Mode::Input { buffer } => assert_eq!(buffer, ""),
            other => panic!("expected Input mode, got {other:?}"),
        }
    }

    // -- Remove via `d` --------------------------------------------------

    #[test]
    fn d_removes_highlighted_tag() {
        let mut app = app_with(vec!["a", "b", "c"]);
        app.list_cursor = 1;
        app.handle_key(key(KeyCode::Char('d')));
        assert_eq!(app.tags, vec!["a".to_string(), "c".to_string()]);
        // Cursor stays at index 1 (now pointing at "c").
        assert_eq!(app.list_cursor, 1);
    }

    #[test]
    fn d_at_last_position_clamps_cursor() {
        let mut app = app_with(vec!["a", "b", "c"]);
        app.list_cursor = 2;
        app.handle_key(key(KeyCode::Char('d')));
        assert_eq!(app.tags, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(app.list_cursor, 1);
    }

    #[test]
    fn d_on_singleton_collapses_to_zero_cursor() {
        let mut app = app_with(vec!["only"]);
        app.handle_key(key(KeyCode::Char('d')));
        assert!(app.tags.is_empty());
        assert_eq!(app.list_cursor, 0);
    }

    #[test]
    fn d_on_empty_list_is_pending_no_op() {
        let mut app = app_with(vec![]);
        assert_eq!(app.handle_key(key(KeyCode::Char('d'))), Outcome::Pending);
        assert!(app.tags.is_empty());
    }

    // -- Save / discard / cancel -----------------------------------------

    #[test]
    fn enter_in_list_emits_save_and_pop_with_current_tags() {
        let mut app = app_with(vec!["FIX", "review"]);
        let outcome = app.handle_key(key(KeyCode::Enter));
        assert_eq!(
            outcome,
            Outcome::SaveAndPop {
                tags: vec!["FIX".to_string(), "review".to_string()],
            }
        );
    }

    #[test]
    fn q_in_list_emits_save_and_pop() {
        let mut app = app_with(vec!["FIX"]);
        let outcome = app.handle_key(key(KeyCode::Char('q')));
        assert_eq!(
            outcome,
            Outcome::SaveAndPop {
                tags: vec!["FIX".to_string()],
            }
        );
    }

    #[test]
    fn enter_with_empty_list_emits_save_and_pop_with_empty_vec() {
        // Save still fires even if the user removed every tag — that's how
        // they clear the column. The dispatcher's `update_step_fields_ext`
        // call accepts an empty slice for exactly this case.
        let mut app = app_with(vec![]);
        let outcome = app.handle_key(key(KeyCode::Enter));
        assert_eq!(outcome, Outcome::SaveAndPop { tags: vec![] });
    }

    #[test]
    fn esc_in_list_emits_discard_and_pop() {
        let mut app = app_with(vec!["FIX"]);
        // Mutate the working list so we can prove it isn't returned.
        app.tags.push("uncommitted".to_string());
        assert_eq!(app.handle_key(key(KeyCode::Esc)), Outcome::DiscardAndPop);
    }

    #[test]
    fn ctrl_c_pops_in_list_mode() {
        let mut app = app_with(vec!["FIX"]);
        assert_eq!(
            app.handle_key(key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Outcome::DiscardAndPop
        );
    }

    #[test]
    fn ctrl_c_pops_in_input_mode() {
        let mut app = app_with(vec![]);
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "wip");
        assert_eq!(
            app.handle_key(key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Outcome::DiscardAndPop
        );
    }

    #[test]
    fn unknown_key_in_list_is_pending() {
        let mut app = app_with(vec!["FIX"]);
        assert_eq!(app.handle_key(key(KeyCode::Char('x'))), Outcome::Pending);
    }

    #[test]
    fn save_returns_tags_in_insertion_order() {
        // Adding via the modal preserves the order the user typed; saving
        // hands that order back to the dispatcher unchanged.
        let mut app = app_with(vec!["a"]);
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "b");
        app.handle_key(key(KeyCode::Enter));
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "c");
        app.handle_key(key(KeyCode::Enter));
        let outcome = app.handle_key(key(KeyCode::Enter));
        assert_eq!(
            outcome,
            Outcome::SaveAndPop {
                tags: vec!["a".to_string(), "b".to_string(), "c".to_string()],
            }
        );
    }

    // -- Render tests ----------------------------------------------------

    fn render_to_string(app: &mut StepTagsApp) -> String {
        let backend = TestBackend::new(80, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, f.area(), app)).unwrap();
        let buf = terminal.backend().buffer().clone();
        let mut out = String::new();
        for y in 0..buf.area.height {
            for x in 0..buf.area.width {
                out.push_str(buf[(x, y)].symbol());
            }
            out.push('\n');
        }
        out
    }

    #[test]
    fn render_list_shows_each_tag_with_index() {
        let mut app = StepTagsApp::new(
            "s1".into(),
            "demo".into(),
            "#3 — Build".into(),
            vec!["FIX".into(), "review".into(), "perf".into()],
        );
        let s = render_to_string(&mut app);
        assert!(s.contains("FIX"), "missing FIX in:\n{s}");
        assert!(s.contains("review"), "missing review in:\n{s}");
        assert!(s.contains("perf"), "missing perf in:\n{s}");
        assert!(s.contains("demo"), "title should include plan slug");
        assert!(s.contains("#3"), "title should include step label");
    }

    #[test]
    fn render_empty_list_shows_placeholder() {
        let mut app = app_with(vec![]);
        let s = render_to_string(&mut app);
        assert!(s.contains("no tags"), "got:\n{s}");
    }

    #[test]
    fn render_input_modal_shows_buffer_and_label() {
        let mut app = app_with(vec![]);
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "needs-review");
        let s = render_to_string(&mut app);
        assert!(s.contains("Add tag"), "missing modal title in:\n{s}");
        assert!(s.contains("needs-review"), "missing buffer in:\n{s}");
    }

    #[test]
    fn render_toast_overrides_hint_row() {
        let mut app = app_with(vec![]);
        app.handle_key(key(KeyCode::Char('i')));
        // Empty submit triggers the "empty" toast.
        app.handle_key(key(KeyCode::Enter));
        let s = render_to_string(&mut app);
        assert!(s.contains("empty"), "expected toast text in:\n{s}");
    }

    // -- End-to-end storage round-trip tests -----------------------------

    fn make_plan_and_step(
        conn: &rusqlite::Connection,
        slug: &str,
        project: &str,
        initial_tags: Vec<&str>,
    ) -> (String, String) {
        let plan_id = storage::create_plan(
            conn,
            slug,
            project,
            &format!("br-{slug}"),
            "d",
            None,
            None,
            &[],
        )
        .expect("create_plan")
        .id;
        let tags: Vec<String> = initial_tags.into_iter().map(String::from).collect();
        let tags_opt = if tags.is_empty() {
            None
        } else {
            Some(tags.as_slice())
        };
        let (step, _) = storage::create_step(
            conn,
            &plan_id,
            "Step",
            "",
            None,
            None,
            &[],
            None,
            None,
            None,
            tags_opt,
        )
        .expect("create_step");
        (plan_id, step.id)
    }

    #[test]
    fn end_to_end_save_persists_added_tag_through_storage() {
        let conn = db::open_memory().unwrap();
        let (_plan_id, step_id) = make_plan_and_step(&conn, "p", "/proj", vec!["FIX"]);

        let mut app = StepTagsApp::new(
            step_id.clone(),
            "p".into(),
            "#1 — Step".into(),
            vec!["FIX".into()],
        );
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "review");
        app.handle_key(key(KeyCode::Enter));
        let outcome = app.handle_key(key(KeyCode::Enter));
        let tags = match outcome {
            Outcome::SaveAndPop { tags } => tags,
            other => panic!("expected SaveAndPop, got {other:?}"),
        };
        assert_eq!(tags, vec!["FIX".to_string(), "review".to_string()]);

        storage::update_step_fields_ext(
            &conn,
            &step_id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&tags),
        )
        .unwrap();

        let after = storage::get_step(&conn, &step_id).unwrap();
        assert_eq!(after.tags, vec!["FIX".to_string(), "review".to_string()]);
    }

    #[test]
    fn end_to_end_save_persists_removed_tag_through_storage() {
        let conn = db::open_memory().unwrap();
        let (_plan_id, step_id) = make_plan_and_step(&conn, "p", "/proj", vec!["FIX", "review"]);

        let mut app = StepTagsApp::new(
            step_id.clone(),
            "p".into(),
            "#1 — Step".into(),
            vec!["FIX".into(), "review".into()],
        );
        app.list_cursor = 0;
        app.handle_key(key(KeyCode::Char('d')));
        let outcome = app.handle_key(key(KeyCode::Enter));
        let tags = match outcome {
            Outcome::SaveAndPop { tags } => tags,
            other => panic!("expected SaveAndPop, got {other:?}"),
        };
        assert_eq!(tags, vec!["review".to_string()]);

        storage::update_step_fields_ext(
            &conn,
            &step_id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&tags),
        )
        .unwrap();
        let after = storage::get_step(&conn, &step_id).unwrap();
        assert_eq!(after.tags, vec!["review".to_string()]);
    }

    #[test]
    fn end_to_end_save_with_empty_clears_all_tags() {
        let conn = db::open_memory().unwrap();
        let (_plan_id, step_id) = make_plan_and_step(&conn, "p", "/proj", vec!["FIX", "review"]);

        let mut app = StepTagsApp::new(
            step_id.clone(),
            "p".into(),
            "#1 — Step".into(),
            vec!["FIX".into(), "review".into()],
        );
        // Remove both tags then save.
        app.handle_key(key(KeyCode::Char('d')));
        app.handle_key(key(KeyCode::Char('d')));
        let outcome = app.handle_key(key(KeyCode::Enter));
        let tags = match outcome {
            Outcome::SaveAndPop { tags } => tags,
            other => panic!("expected SaveAndPop, got {other:?}"),
        };
        assert!(tags.is_empty());

        storage::update_step_fields_ext(
            &conn,
            &step_id,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            Some(&tags),
        )
        .unwrap();
        let after = storage::get_step(&conn, &step_id).unwrap();
        assert!(after.tags.is_empty());
    }

    #[test]
    fn end_to_end_discard_does_not_touch_storage() {
        // Drive the state machine to mutate the working list, then cancel
        // with Esc. The dispatcher loop is responsible for honoring
        // DiscardAndPop by skipping the storage write — this test proves
        // the sub-view never silently writes on cancel.
        let conn = db::open_memory().unwrap();
        let (_plan_id, step_id) = make_plan_and_step(&conn, "p", "/proj", vec!["FIX"]);

        let mut app = StepTagsApp::new(
            step_id.clone(),
            "p".into(),
            "#1 — Step".into(),
            vec!["FIX".into()],
        );
        app.handle_key(key(KeyCode::Char('i')));
        type_str(&mut app, "review");
        app.handle_key(key(KeyCode::Enter));
        assert_eq!(app.tags, vec!["FIX".to_string(), "review".to_string()]);

        // User changes their mind and cancels.
        let outcome = app.handle_key(key(KeyCode::Esc));
        assert_eq!(outcome, Outcome::DiscardAndPop);

        // Storage row is untouched — the dispatcher loop never called
        // update_step_fields_ext because the outcome was DiscardAndPop.
        let after = storage::get_step(&conn, &step_id).unwrap();
        assert_eq!(after.tags, vec!["FIX".to_string()]);
    }
}
