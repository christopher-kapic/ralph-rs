// Palette input bar widget (TUI-plan.md §9).
//
// Low-level UI primitive: a single-row input bar overlaid on the bottom
// chrome row plus an optional suggestion list (1–3 rows) drawn directly
// above it. The widget owns *editing* state — input buffer, cursor, and
// the index into a caller-supplied suggestion list — but does not compute
// the suggestions itself; callers populate `suggestions` from
// `palette::build_completion` (or any equivalent source) and re-render.
//
// The split lets the palette state machine in `super::super::palette` keep
// owning command parsing while this widget stays a pure presentational
// component that's easy to unit-test through a `TestBackend`.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph};

use crate::tui::chrome::display_width;
use crate::tui::theme;

/// Maximum number of suggestion rows rendered above the bar. Caller-side
/// suggestion vectors may be longer; the widget renders a sliding window.
pub const MAX_SUGGESTIONS_VISIBLE: usize = 3;

/// Outcome of feeding a single key event into the bar.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaletteBarOutcome {
    /// User pressed `<enter>`. Carries the current input verbatim (no
    /// trimming, no parsing — that's the caller's job).
    Submit(String),
    /// User pressed `<esc>` or `Ctrl-C`. Caller should close the bar
    /// without dispatching.
    Cancel,
    /// Key consumed; bar stays open with possibly mutated state.
    Pending,
}

/// Editable state of the palette input bar.
///
/// The fields are public so callers can populate `suggestions` after
/// recomputing them from the current input — there is no setter ceremony
/// because the widget is intentionally a value type.
#[derive(Debug, Clone)]
pub struct PaletteBarState {
    /// Visual prefix character (`/` or `:`) showing which key opened the
    /// palette. Cosmetic — parsing strips it.
    pub prefix: char,
    /// Text the user has typed (NOT including the prefix).
    pub input: String,
    /// Byte offset of the cursor within `input`. Always at a char boundary.
    pub cursor: usize,
    /// Suggestion candidates as full replacement strings (without prefix).
    /// Caller refreshes this whenever `input` changes.
    pub suggestions: Vec<String>,
    /// Index into `suggestions` of the currently-cycled candidate, or
    /// `None` when the user hasn't tabbed yet (or has typed since).
    pub suggestion_index: Option<usize>,
}

impl Default for PaletteBarState {
    fn default() -> Self {
        Self::new('/')
    }
}

impl PaletteBarState {
    /// Fresh bar state with the given prefix and an empty input.
    pub fn new(prefix: char) -> Self {
        Self {
            prefix,
            input: String::new(),
            cursor: 0,
            suggestions: Vec::new(),
            suggestion_index: None,
        }
    }

    /// Reset to a fresh state with the given prefix. Used when the bar is
    /// reopened — keeps allocation around but clears the buffer.
    pub fn reset(&mut self, prefix: char) {
        self.prefix = prefix;
        self.input.clear();
        self.cursor = 0;
        self.suggestions.clear();
        self.suggestion_index = None;
    }

    /// Handle one key event. The widget consumes every key and returns
    /// [`PaletteBarOutcome::Pending`] for keys that don't terminate input.
    pub fn on_key(&mut self, key: KeyEvent) -> PaletteBarOutcome {
        // Ctrl-W: word erase (handled before the generic Char branch so
        // it isn't treated as a literal 'w' insert).
        if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('w')) {
            self.word_erase();
            return PaletteBarOutcome::Pending;
        }
        // Ctrl-C cancels regardless of which key code rides on it.
        if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
            return PaletteBarOutcome::Cancel;
        }

        match key.code {
            KeyCode::Esc => PaletteBarOutcome::Cancel,
            KeyCode::Enter => PaletteBarOutcome::Submit(self.input.clone()),
            KeyCode::Tab => {
                self.cycle_forward();
                PaletteBarOutcome::Pending
            }
            KeyCode::BackTab => {
                self.cycle_backward();
                PaletteBarOutcome::Pending
            }
            KeyCode::Backspace => {
                self.backspace();
                PaletteBarOutcome::Pending
            }
            KeyCode::Left => {
                self.move_cursor_left();
                self.suggestion_index = None;
                PaletteBarOutcome::Pending
            }
            KeyCode::Right => {
                self.move_cursor_right();
                self.suggestion_index = None;
                PaletteBarOutcome::Pending
            }
            KeyCode::Home => {
                self.cursor = 0;
                self.suggestion_index = None;
                PaletteBarOutcome::Pending
            }
            KeyCode::End => {
                self.cursor = self.input.len();
                self.suggestion_index = None;
                PaletteBarOutcome::Pending
            }
            KeyCode::Char(c) => {
                // Suppress modifier-decorated chars (Ctrl-A etc.) so they
                // don't insert literal letters. Plain Shift produces the
                // already-uppercased char via the OS layer.
                if key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                {
                    return PaletteBarOutcome::Pending;
                }
                self.insert_char(c);
                PaletteBarOutcome::Pending
            }
            _ => PaletteBarOutcome::Pending,
        }
    }

    /// If a suggestion is currently selected, replace `input` with it and
    /// move the cursor to the end. No-op when `suggestion_index` is `None`
    /// or out of bounds.
    pub fn apply_completion(&mut self) {
        if let Some(i) = self.suggestion_index
            && let Some(s) = self.suggestions.get(i)
        {
            self.input = s.clone();
            self.cursor = self.input.len();
        }
    }

    /// Insert a character at the cursor and clear any active completion
    /// cycle so the next `<tab>` recomputes from the new buffer.
    pub fn insert_char(&mut self, c: char) {
        self.input.insert(self.cursor, c);
        self.cursor += c.len_utf8();
        self.suggestion_index = None;
    }

    /// Delete the char immediately before the cursor. No-op at column 0.
    pub fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let mut prev = self.cursor - 1;
        while !self.input.is_char_boundary(prev) {
            prev -= 1;
        }
        self.input.replace_range(prev..self.cursor, "");
        self.cursor = prev;
        self.suggestion_index = None;
    }

    /// Erase the word before the cursor — readline-style `Ctrl-W`. Skips
    /// trailing whitespace, then deletes back through the next run of
    /// non-whitespace characters.
    pub fn word_erase(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let prefix = &self.input[..self.cursor];
        let trimmed = prefix.trim_end();
        let mut new_cursor = trimmed.len();
        for (idx, c) in trimmed.char_indices().rev() {
            if c.is_whitespace() {
                new_cursor = idx + c.len_utf8();
                break;
            }
            new_cursor = idx;
        }
        self.input.replace_range(new_cursor..self.cursor, "");
        self.cursor = new_cursor;
        self.suggestion_index = None;
    }

    /// Move the cursor one char left (UTF-8 safe).
    pub fn move_cursor_left(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let mut prev = self.cursor - 1;
        while !self.input.is_char_boundary(prev) {
            prev -= 1;
        }
        self.cursor = prev;
    }

    /// Move the cursor one char right (UTF-8 safe).
    pub fn move_cursor_right(&mut self) {
        if self.cursor >= self.input.len() {
            return;
        }
        let mut next = self.cursor + 1;
        while next < self.input.len() && !self.input.is_char_boundary(next) {
            next += 1;
        }
        self.cursor = next;
    }

    fn cycle_forward(&mut self) {
        if self.suggestions.is_empty() {
            return;
        }
        self.suggestion_index = Some(match self.suggestion_index {
            Some(i) => (i + 1) % self.suggestions.len(),
            None => 0,
        });
        self.apply_completion();
    }

    fn cycle_backward(&mut self) {
        if self.suggestions.is_empty() {
            return;
        }
        self.suggestion_index = Some(match self.suggestion_index {
            Some(0) | None => self.suggestions.len() - 1,
            Some(i) => i - 1,
        });
        self.apply_completion();
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Draw the palette bar (and any suggestions) into `area`.
///
/// `area` is the full rectangle the widget may claim — its bottom row is
/// reserved for the input bar, and up to [`MAX_SUGGESTIONS_VISIBLE`] rows
/// directly above are used for the suggestion list. Caller is expected to
/// render this AFTER the host view so it overlays the bottom hint bar.
pub fn render(frame: &mut Frame, area: Rect, state: &PaletteBarState) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    frame.render_widget(Clear, area);

    let bar_y = area.bottom().saturating_sub(1);
    let bar_area = Rect::new(area.x, bar_y, area.width, 1);

    let max_rows_avail = (area.height as usize).saturating_sub(1);
    let n_visible = state
        .suggestions
        .len()
        .min(MAX_SUGGESTIONS_VISIBLE)
        .min(max_rows_avail);

    if n_visible > 0 {
        // Window the suggestion slice so the active candidate (if any)
        // stays visible — with the window anchored to the bottom of the
        // visible region when the active index is past it.
        let start = match state.suggestion_index {
            Some(i) if i >= n_visible => i + 1 - n_visible,
            _ => 0,
        };
        let suggestions_y = bar_y - n_visible as u16;
        for (offset, abs_idx) in (start..start + n_visible).enumerate() {
            let Some(text) = state.suggestions.get(abs_idx) else {
                break;
            };
            let row_y = suggestions_y + offset as u16;
            let row_area = Rect::new(area.x, row_y, area.width, 1);
            let style = if Some(abs_idx) == state.suggestion_index {
                Style::default()
                    .bg(theme::CURSOR)
                    .fg(Color::Black)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default().fg(theme::CHROME_DIM)
            };
            frame.render_widget(Paragraph::new(Span::styled(text.clone(), style)), row_area);
        }
    }

    render_bar_row(frame, bar_area, state);
}

fn render_bar_row(frame: &mut Frame, area: Rect, state: &PaletteBarState) {
    if area.width == 0 {
        return;
    }
    let prefix_str = format!("{} ", state.prefix);
    let prefix_width = display_width(&prefix_str);

    let before = &state.input[..state.cursor];
    let after_full = &state.input[state.cursor..];

    // Render the cell at the cursor with REVERSED so terminals that hide
    // the hardware cursor (or test backends that don't surface it) still
    // show a visible block. When the cursor is past the end of `input`,
    // synthesize a single-space cell to highlight.
    let (at_cursor, after_rest) = match after_full.chars().next() {
        Some(c) => {
            let n = c.len_utf8();
            (c.to_string(), &after_full[n..])
        }
        None => (" ".to_string(), after_full),
    };

    let prefix_style = Style::default()
        .fg(theme::CURSOR)
        .add_modifier(Modifier::BOLD);
    let cursor_style = Style::default().add_modifier(Modifier::REVERSED);

    let spans = vec![
        Span::styled(prefix_str, prefix_style),
        Span::raw(before.to_string()),
        Span::styled(at_cursor, cursor_style),
        Span::raw(after_rest.to_string()),
    ];
    frame.render_widget(Paragraph::new(Line::from(spans)), area);

    // Also report the hardware cursor so well-behaved terminals draw it.
    let cursor_col = area.x + prefix_width as u16 + display_width(before) as u16;
    if cursor_col < area.x + area.width {
        frame.set_cursor_position((cursor_col, area.y));
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyEvent, KeyModifiers};
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;
    use ratatui::buffer::Buffer;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_with(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    fn render_to_buffer(state: &PaletteBarState, w: u16, h: u16) -> Buffer {
        let backend = TestBackend::new(w, h);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                let area = Rect::new(0, 0, w, h);
                render(frame, area, state);
            })
            .unwrap();
        terminal.backend().buffer().clone()
    }

    fn row_text(buffer: &Buffer, y: u16) -> String {
        (0..buffer.area().width)
            .map(|x| buffer[(x, y)].symbol())
            .collect()
    }

    // -- on_key terminal outcomes -----------------------------------------

    #[test]
    fn enter_submits_current_input() {
        let mut s = PaletteBarState::new('/');
        s.input = "run main".to_string();
        s.cursor = s.input.len();
        let out = s.on_key(key(KeyCode::Enter));
        assert_eq!(out, PaletteBarOutcome::Submit("run main".to_string()));
    }

    #[test]
    fn esc_returns_cancel() {
        let mut s = PaletteBarState::new('/');
        s.input = "anything".to_string();
        let out = s.on_key(key(KeyCode::Esc));
        assert_eq!(out, PaletteBarOutcome::Cancel);
    }

    #[test]
    fn ctrl_c_returns_cancel() {
        let mut s = PaletteBarState::new('/');
        let out = s.on_key(key_with(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert_eq!(out, PaletteBarOutcome::Cancel);
    }

    // -- character entry --------------------------------------------------

    #[test]
    fn typing_inserts_chars_and_clears_cycle() {
        let mut s = PaletteBarState::new('/');
        s.suggestions = vec!["run".into()];
        s.suggestion_index = Some(0);
        for c in "ru".chars() {
            assert_eq!(s.on_key(key(KeyCode::Char(c))), PaletteBarOutcome::Pending);
        }
        assert_eq!(s.input, "ru");
        assert_eq!(s.cursor, 2);
        assert_eq!(s.suggestion_index, None);
    }

    #[test]
    fn ctrl_modified_chars_are_swallowed() {
        let mut s = PaletteBarState::new('/');
        s.on_key(key_with(KeyCode::Char('a'), KeyModifiers::CONTROL));
        assert_eq!(s.input, "");
        s.on_key(key_with(KeyCode::Char('z'), KeyModifiers::ALT));
        assert_eq!(s.input, "");
    }

    // -- backspace / word_erase -------------------------------------------

    #[test]
    fn backspace_removes_one_char() {
        let mut s = PaletteBarState::new('/');
        for c in "run".chars() {
            s.on_key(key(KeyCode::Char(c)));
        }
        s.on_key(key(KeyCode::Backspace));
        assert_eq!(s.input, "ru");
        assert_eq!(s.cursor, 2);
        for _ in 0..5 {
            s.on_key(key(KeyCode::Backspace));
        }
        assert_eq!(s.input, "");
        assert_eq!(s.cursor, 0);
    }

    #[test]
    fn word_erase_removes_trailing_word_and_whitespace() {
        let mut s = PaletteBarState::new('/');
        s.input = "hello world".to_string();
        s.cursor = s.input.len();
        s.on_key(key_with(KeyCode::Char('w'), KeyModifiers::CONTROL));
        assert_eq!(s.input, "hello ");
        assert_eq!(s.cursor, 6);
        // A second Ctrl-W eats "hello " (whitespace + word) entirely.
        s.on_key(key_with(KeyCode::Char('w'), KeyModifiers::CONTROL));
        assert_eq!(s.input, "");
        assert_eq!(s.cursor, 0);
    }

    #[test]
    fn word_erase_in_middle_of_buffer() {
        let mut s = PaletteBarState::new('/');
        s.input = "alpha beta gamma".to_string();
        // Cursor sits right after "beta".
        s.cursor = "alpha beta".len();
        s.on_key(key_with(KeyCode::Char('w'), KeyModifiers::CONTROL));
        assert_eq!(s.input, "alpha  gamma");
        assert_eq!(s.cursor, "alpha ".len());
    }

    #[test]
    fn word_erase_at_column_zero_is_noop() {
        let mut s = PaletteBarState::new('/');
        s.on_key(key_with(KeyCode::Char('w'), KeyModifiers::CONTROL));
        assert_eq!(s.input, "");
        assert_eq!(s.cursor, 0);
    }

    // -- cursor movement --------------------------------------------------

    #[test]
    fn left_right_arrows_move_cursor() {
        let mut s = PaletteBarState::new('/');
        for c in "abc".chars() {
            s.on_key(key(KeyCode::Char(c)));
        }
        assert_eq!(s.cursor, 3);
        s.on_key(key(KeyCode::Left));
        assert_eq!(s.cursor, 2);
        s.on_key(key(KeyCode::Left));
        s.on_key(key(KeyCode::Left));
        s.on_key(key(KeyCode::Left));
        assert_eq!(s.cursor, 0);
        s.on_key(key(KeyCode::Right));
        assert_eq!(s.cursor, 1);
        s.on_key(key(KeyCode::End));
        assert_eq!(s.cursor, 3);
        s.on_key(key(KeyCode::Home));
        assert_eq!(s.cursor, 0);
    }

    #[test]
    fn cursor_movement_clears_pending_completion() {
        let mut s = PaletteBarState::new('/');
        s.suggestions = vec!["run".into()];
        s.suggestion_index = Some(0);
        s.on_key(key(KeyCode::Left));
        assert_eq!(s.suggestion_index, None);
    }

    // -- tab cycling / apply_completion -----------------------------------

    #[test]
    fn tab_cycles_suggestions_and_applies_them() {
        let mut s = PaletteBarState::new('/');
        s.input = "p".to_string();
        s.cursor = 1;
        s.suggestions = vec!["plan harness".into(), "plan hooks".into()];
        s.on_key(key(KeyCode::Tab));
        assert_eq!(s.suggestion_index, Some(0));
        assert_eq!(s.input, "plan harness");
        assert_eq!(s.cursor, "plan harness".len());
        s.on_key(key(KeyCode::Tab));
        assert_eq!(s.suggestion_index, Some(1));
        assert_eq!(s.input, "plan hooks");
        s.on_key(key(KeyCode::Tab));
        // Wraps around.
        assert_eq!(s.suggestion_index, Some(0));
        assert_eq!(s.input, "plan harness");
    }

    #[test]
    fn back_tab_cycles_in_reverse() {
        let mut s = PaletteBarState::new('/');
        s.suggestions = vec!["a".into(), "b".into(), "c".into()];
        s.on_key(key(KeyCode::BackTab));
        assert_eq!(s.suggestion_index, Some(2));
        assert_eq!(s.input, "c");
        s.on_key(key(KeyCode::BackTab));
        assert_eq!(s.suggestion_index, Some(1));
        assert_eq!(s.input, "b");
    }

    #[test]
    fn tab_with_no_suggestions_is_a_noop() {
        let mut s = PaletteBarState::new('/');
        s.input = "zzz".to_string();
        s.cursor = 3;
        s.on_key(key(KeyCode::Tab));
        assert_eq!(s.input, "zzz");
        assert_eq!(s.suggestion_index, None);
    }

    #[test]
    fn apply_completion_replaces_input_with_indexed_suggestion() {
        let mut s = PaletteBarState::new('/');
        s.input = "old".into();
        s.suggestions = vec!["alpha".into(), "beta".into()];
        s.suggestion_index = Some(1);
        s.apply_completion();
        assert_eq!(s.input, "beta");
        assert_eq!(s.cursor, 4);
    }

    #[test]
    fn apply_completion_with_no_index_is_noop() {
        let mut s = PaletteBarState::new('/');
        s.input = "old".into();
        s.suggestions = vec!["alpha".into()];
        s.suggestion_index = None;
        s.apply_completion();
        assert_eq!(s.input, "old");
    }

    #[test]
    fn reset_clears_buffer_and_suggestions() {
        let mut s = PaletteBarState::new('/');
        s.input = "stuff".into();
        s.cursor = 5;
        s.suggestions = vec!["a".into()];
        s.suggestion_index = Some(0);
        s.reset(':');
        assert_eq!(s.prefix, ':');
        assert_eq!(s.input, "");
        assert_eq!(s.cursor, 0);
        assert!(s.suggestions.is_empty());
        assert_eq!(s.suggestion_index, None);
    }

    // -- rendering --------------------------------------------------------

    #[test]
    fn render_draws_prefix_then_input_on_bottom_row() {
        let mut s = PaletteBarState::new(':');
        s.input = "plan harness".to_string();
        s.cursor = s.input.len();
        let buffer = render_to_buffer(&s, 40, 4);
        // Bottom row is index 3 (0-based) in a 4-row terminal.
        let bottom = row_text(&buffer, 3);
        assert!(bottom.starts_with(": plan harness"), "got {bottom:?}");
    }

    #[test]
    fn render_uses_slash_prefix_when_state_says_so() {
        let mut s = PaletteBarState::new('/');
        s.input = "run".to_string();
        s.cursor = s.input.len();
        let buffer = render_to_buffer(&s, 30, 2);
        let bottom = row_text(&buffer, 1);
        assert!(bottom.starts_with("/ run"), "got {bottom:?}");
    }

    #[test]
    fn render_shows_suggestion_rows_above_the_bar() {
        let mut s = PaletteBarState::new('/');
        s.input = "p".into();
        s.cursor = 1;
        s.suggestions = vec!["plan harness".into(), "plan hooks".into()];
        let buffer = render_to_buffer(&s, 40, 4);
        // Bar row = 3. Two suggestions render at rows 1 and 2.
        let s1 = row_text(&buffer, 1);
        let s2 = row_text(&buffer, 2);
        let bar = row_text(&buffer, 3);
        assert!(s1.starts_with("plan harness"), "row1 {s1:?}");
        assert!(s2.starts_with("plan hooks"), "row2 {s2:?}");
        assert!(bar.starts_with("/ p"), "bar {bar:?}");
    }

    #[test]
    fn render_highlights_the_active_suggestion_with_cursor_bg() {
        let mut s = PaletteBarState::new('/');
        s.input = "p".into();
        s.cursor = 1;
        s.suggestions = vec!["plan harness".into(), "plan hooks".into()];
        s.suggestion_index = Some(1);
        // Tab would have applied the suggestion; emulate that here so the
        // bar reflects the active candidate, while the active row carries
        // the highlight bg.
        s.apply_completion();
        let buffer = render_to_buffer(&s, 40, 4);
        // Row 2 is the active suggestion. Its first cell should carry the
        // cursor (yellow) background.
        let cell = &buffer[(0, 2)];
        assert_eq!(
            cell.style().bg,
            Some(theme::CURSOR),
            "active suggestion row should use theme::CURSOR background"
        );
        // Row 1 is the inactive candidate — no highlight bg.
        let inactive = &buffer[(0, 1)];
        assert_ne!(
            inactive.style().bg,
            Some(theme::CURSOR),
            "inactive suggestion row must not carry the highlight bg"
        );
    }

    #[test]
    fn render_caps_visible_suggestions_at_three() {
        let mut s = PaletteBarState::new('/');
        s.suggestions = (0..7).map(|i| format!("verb{i}")).collect();
        let buffer = render_to_buffer(&s, 40, 6);
        // Bar row = 5. The three suggestion rows are 2..=4. Earlier rows
        // (0, 1) must be empty since no completion is active and the
        // window starts at 0.
        assert!(row_text(&buffer, 2).starts_with("verb0"));
        assert!(row_text(&buffer, 3).starts_with("verb1"));
        assert!(row_text(&buffer, 4).starts_with("verb2"));
        // verb3..verb6 should NOT be drawn — only the first 3 fit.
        for y in [0u16, 1] {
            let row = row_text(&buffer, y);
            assert!(
                !row.contains("verb"),
                "row {y} should be blank, got {row:?}"
            );
        }
    }

    #[test]
    fn render_windows_to_keep_active_suggestion_visible() {
        let mut s = PaletteBarState::new('/');
        s.suggestions = (0..6).map(|i| format!("v{i}")).collect();
        // Highlight v4 — outside the default window of [0..3].
        s.suggestion_index = Some(4);
        s.apply_completion();
        let buffer = render_to_buffer(&s, 40, 5);
        // Bar row = 4. Suggestion rows are 1..=3. Window anchored to put
        // v4 at the bottom, so rows show v2, v3, v4.
        assert!(row_text(&buffer, 1).starts_with("v2"));
        assert!(row_text(&buffer, 2).starts_with("v3"));
        assert!(row_text(&buffer, 3).starts_with("v4"));
        // The active row carries the highlight bg.
        assert_eq!(buffer[(0, 3)].style().bg, Some(theme::CURSOR));
    }

    #[test]
    fn render_omits_suggestions_when_empty() {
        let mut s = PaletteBarState::new('/');
        s.input = "run".to_string();
        s.cursor = 3;
        let buffer = render_to_buffer(&s, 20, 3);
        // Only the bar row should have visible content; rows above blank.
        assert!(row_text(&buffer, 0).trim().is_empty());
        assert!(row_text(&buffer, 1).trim().is_empty());
        assert!(row_text(&buffer, 2).starts_with("/ run"));
    }

    #[test]
    fn render_visible_cursor_uses_reversed_modifier() {
        let mut s = PaletteBarState::new('/');
        s.input = "abc".to_string();
        s.cursor = 1; // Cursor sits on 'b'.
        let buffer = render_to_buffer(&s, 20, 1);
        // Prefix "/" + " " takes 2 cells. Then 'a' at col 2, cursor on 'b'
        // at col 3.
        let cell = &buffer[(3, 0)];
        assert_eq!(cell.symbol(), "b");
        assert!(
            cell.style().add_modifier.contains(Modifier::REVERSED),
            "cursor cell should carry the REVERSED modifier"
        );
    }

    #[test]
    fn render_cursor_past_end_renders_a_block_after_input() {
        let mut s = PaletteBarState::new('/');
        s.input = "ab".to_string();
        s.cursor = 2;
        let buffer = render_to_buffer(&s, 10, 1);
        // Prefix takes col 0..2. Input "ab" at cols 2 and 3. Cursor cell
        // at col 4 should be a synthesized space with REVERSED.
        let cell = &buffer[(4, 0)];
        assert_eq!(cell.symbol(), " ");
        assert!(cell.style().add_modifier.contains(Modifier::REVERSED));
    }

    #[test]
    fn render_zero_area_is_noop() {
        let backend = TestBackend::new(20, 3);
        let mut terminal = Terminal::new(backend).unwrap();
        let s = PaletteBarState::default();
        terminal
            .draw(|frame| {
                render(frame, Rect::new(0, 0, 0, 0), &s);
            })
            .unwrap();
        // No assertion beyond "didn't panic"; cells stay default.
    }

    #[test]
    fn render_does_not_panic_on_tiny_area() {
        let mut s = PaletteBarState::new('/');
        s.suggestions = vec!["a".into(), "b".into()];
        let _ = render_to_buffer(&s, 3, 1);
        let _ = render_to_buffer(&s, 1, 5);
    }
}
