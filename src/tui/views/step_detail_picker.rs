// Bottom-row pickers for the step-detail view (TUI-plan.md §8 "Bottom-row
// inline editors").
//
// `c` on the focused bottom-row sub-cell opens one of four pickers:
// Harness, Model, Agent, Change policy. Each picker is a centered modal
// list; `j`/`k` moves the highlight, `Enter` confirms, `Esc` cancels.
//
// The Model picker also exposes a synthetic "Custom..." entry that flips the
// modal into a free-text input mode so users can type a model identifier
// not present in the harness's known list. `Esc` from the input mode falls
// back to list mode (rather than cancelling the whole picker outright) so a
// mistyped Custom doesn't lose the list selection.
//
// State and event handling live here as a self-contained module so the
// dispatcher only has to drive a `handle_key` → `PickerOutcome` loop, and
// tests can exercise selection / cancellation paths without a real terminal
// or any DB writes — `apply_*` on `StepDetailApp` is the DB-write surface.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use crate::plan::ChangePolicy;
use crate::tui::chrome::display_width;
use crate::tui::theme;

// ---------------------------------------------------------------------------
// Bottom-row sub-cell focus
// ---------------------------------------------------------------------------

/// Which cell of the bottom row currently has focus. `h`/`l` walks between
/// cells when [`super::step_detail::Pane::BottomRow`] is the focused pane;
/// `c` opens the corresponding picker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BottomCell {
    Harness,
    Model,
    Agent,
    ChangePolicy,
}

impl BottomCell {
    /// Display order — left-to-right in the rendered bottom row, matching
    /// the §8 sketch.
    pub const ORDER: [BottomCell; 4] = [
        BottomCell::Harness,
        BottomCell::Model,
        BottomCell::Agent,
        BottomCell::ChangePolicy,
    ];

    fn index(self) -> usize {
        Self::ORDER
            .iter()
            .position(|c| *c == self)
            .expect("cell in ORDER")
    }

    /// Move one cell to the left. Stays put at the leftmost cell — the
    /// outer `handle_left` falls through to popping the view in that case
    /// per §8.
    pub fn move_left(self) -> Option<Self> {
        let i = self.index();
        if i == 0 {
            None
        } else {
            Some(Self::ORDER[i - 1])
        }
    }

    /// Move one cell to the right. Stays put at the rightmost cell — `l`
    /// is a no-op past the right edge.
    pub fn move_right(self) -> Option<Self> {
        let i = self.index();
        if i + 1 >= Self::ORDER.len() {
            None
        } else {
            Some(Self::ORDER[i + 1])
        }
    }
}

// ---------------------------------------------------------------------------
// Picker state
// ---------------------------------------------------------------------------

/// Discriminator that survives the picker-open → submit round trip so the
/// dispatcher can tell the four picker flavors apart when applying the
/// chosen value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PickerKind {
    Harness,
    Model,
    Agent,
    ChangePolicy,
}

impl PickerKind {
    /// Title shown on the modal's bordered block.
    pub fn title(self) -> &'static str {
        match self {
            PickerKind::Harness => "Pick harness",
            PickerKind::Model => "Pick model",
            PickerKind::Agent => "Pick agent",
            PickerKind::ChangePolicy => "Pick change policy",
        }
    }
}

/// One row in the picker list. `Value` rows write the contained string
/// through to the steps row on confirm; `Custom` rows flip the picker into
/// free-text input mode (Model picker only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PickerItem {
    Inherit,
    Value(String),
    Custom,
}

impl PickerItem {
    pub fn label(&self) -> &str {
        match self {
            PickerItem::Inherit => "(inherit)",
            PickerItem::Value(s) => s,
            PickerItem::Custom => "Custom…",
        }
    }
}

/// Whether the picker is showing the option list or the free-text Custom
/// input. The state machine flips between modes; cancellation paths differ
/// (Esc in CustomInput returns to List rather than fully cancelling, so a
/// fat-finger doesn't drop the list selection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PickerMode {
    List,
    CustomInput { buffer: String },
}

/// One open picker. The dispatcher calls [`Self::handle_key`] until it
/// returns [`PickerOutcome::Cancelled`] or [`PickerOutcome::Submit`], then
/// closes / applies as appropriate.
#[derive(Debug, Clone)]
pub struct PickerState {
    pub kind: PickerKind,
    pub items: Vec<PickerItem>,
    pub selected: usize,
    pub mode: PickerMode,
}

/// What [`PickerState::handle_key`] returns each turn — `Pending` means
/// "keep looping", the other variants drive the dispatcher's exit paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PickerOutcome {
    Pending,
    Cancelled,
    /// User confirmed a value. The dispatcher wires this through one of the
    /// `apply_*` methods on [`super::step_detail::StepDetailApp`].
    Submit {
        kind: PickerKind,
        value: Option<String>,
    },
}

impl PickerState {
    /// Build the Harness picker. `harnesses` is the deduplicated, sorted
    /// list of `Config.harnesses` keys; `current` (if present) is
    /// pre-selected so Enter without movement is a no-op confirmation of
    /// the existing value.
    pub fn for_harness(harnesses: &[String], current: Option<&str>) -> Self {
        let mut items: Vec<PickerItem> = vec![PickerItem::Inherit];
        items.extend(harnesses.iter().cloned().map(PickerItem::Value));
        let selected = current
            .and_then(|c| {
                items
                    .iter()
                    .position(|i| matches!(i, PickerItem::Value(s) if s == c))
            })
            .unwrap_or(0);
        Self {
            kind: PickerKind::Harness,
            items,
            selected,
            mode: PickerMode::List,
        }
    }

    /// Build the Model picker. List entries: the harness's `default_model`
    /// (if any), the step's current `model` override (if any and distinct
    /// from the default), and a synthetic "Custom…" entry. Pre-selects the
    /// step's current model when present, otherwise the harness default.
    pub fn for_model(harness_default: Option<&str>, current: Option<&str>) -> Self {
        let mut items: Vec<PickerItem> = vec![PickerItem::Inherit];
        if let Some(d) = harness_default
            && !d.is_empty()
        {
            items.push(PickerItem::Value(d.to_string()));
        }
        if let Some(c) = current
            && !c.is_empty()
            && !items
                .iter()
                .any(|i| matches!(i, PickerItem::Value(s) if s == c))
        {
            items.push(PickerItem::Value(c.to_string()));
        }
        items.push(PickerItem::Custom);

        let selected = current
            .and_then(|c| {
                items
                    .iter()
                    .position(|i| matches!(i, PickerItem::Value(s) if s == c))
            })
            .unwrap_or(0);
        Self {
            kind: PickerKind::Model,
            items,
            selected,
            mode: PickerMode::List,
        }
    }

    /// Build the Agent picker. `agents` is the sorted, deduplicated list of
    /// agent filenames (without the `.md` extension); `current` (if any) is
    /// pre-selected. When the agents directory is empty, `agents` is empty
    /// and the modal shows a single dim "(no agents — run `ralph agents
    /// create <name>`)" placeholder. Confirming with no items does nothing
    /// (no submit, no cancel).
    pub fn for_agent(agents: &[String], current: Option<&str>) -> Self {
        let mut items: Vec<PickerItem> = vec![PickerItem::Inherit];
        items.extend(agents.iter().cloned().map(PickerItem::Value));
        let selected = current
            .and_then(|c| {
                items
                    .iter()
                    .position(|i| matches!(i, PickerItem::Value(s) if s == c))
            })
            .unwrap_or(0);
        Self {
            kind: PickerKind::Agent,
            items,
            selected,
            mode: PickerMode::List,
        }
    }

    /// Build the Change-policy picker. Two-item list (Required / Optional)
    /// with the step's current policy pre-selected.
    pub fn for_change_policy(current: ChangePolicy) -> Self {
        let items = vec![
            PickerItem::Value(ChangePolicy::Required.as_str().to_string()),
            PickerItem::Value(ChangePolicy::Optional.as_str().to_string()),
        ];
        let selected = match current {
            ChangePolicy::Required => 0,
            ChangePolicy::Optional => 1,
        };
        Self {
            kind: PickerKind::ChangePolicy,
            items,
            selected,
            mode: PickerMode::List,
        }
    }

    /// Pure key handler. Splits responsibility with the event loop the same
    /// way [`super::create_plan::CreatePlanModal::handle_key`] does — tests
    /// drive arbitrary key sequences without crossterm.
    pub fn handle_key(&mut self, key: KeyEvent) -> PickerOutcome {
        // Ctrl-C always cancels regardless of mode.
        if let KeyCode::Char('c') = key.code
            && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            return PickerOutcome::Cancelled;
        }

        let in_input = matches!(self.mode, PickerMode::CustomInput { .. });
        if in_input {
            self.handle_custom_input_key(key)
        } else {
            Self::handle_list_key(self.kind, &self.items, &mut self.selected, key)
        }
    }

    fn handle_list_key(
        kind: PickerKind,
        items: &[PickerItem],
        selected: &mut usize,
        key: KeyEvent,
    ) -> PickerOutcome {
        match key.code {
            KeyCode::Esc => PickerOutcome::Cancelled,
            KeyCode::Char('q') => PickerOutcome::Cancelled,
            KeyCode::Up | KeyCode::Char('k') => {
                if !items.is_empty() && *selected > 0 {
                    *selected -= 1;
                }
                PickerOutcome::Pending
            }
            KeyCode::Down | KeyCode::Char('j') => {
                if !items.is_empty() && *selected + 1 < items.len() {
                    *selected += 1;
                }
                PickerOutcome::Pending
            }
            KeyCode::Home | KeyCode::Char('g') => {
                if !items.is_empty() {
                    *selected = 0;
                }
                PickerOutcome::Pending
            }
            KeyCode::End | KeyCode::Char('G') => {
                if !items.is_empty() {
                    *selected = items.len() - 1;
                }
                PickerOutcome::Pending
            }
            KeyCode::Enter => {
                let Some(item) = items.get(*selected) else {
                    // Empty list — Enter is a no-op until the user cancels.
                    return PickerOutcome::Pending;
                };
                match item {
                    PickerItem::Inherit => PickerOutcome::Submit { kind, value: None },
                    PickerItem::Value(s) => PickerOutcome::Submit {
                        kind,
                        value: Some(s.clone()),
                    },
                    PickerItem::Custom => {
                        // The picker stays open; the caller observes the
                        // mode transition on the next render via
                        // `is_in_custom_input`.
                        PickerOutcome::Pending
                    }
                }
            }
            _ => PickerOutcome::Pending,
        }
    }

    fn handle_custom_input_key(&mut self, key: KeyEvent) -> PickerOutcome {
        let kind = self.kind;
        match key.code {
            KeyCode::Esc => {
                // Drop back to the list rather than cancel — `Esc` in input
                // mode shouldn't strand the user without their list-pick.
                self.mode = PickerMode::List;
                PickerOutcome::Pending
            }
            KeyCode::Enter => {
                let buffer = match &self.mode {
                    PickerMode::CustomInput { buffer } => buffer.clone(),
                    PickerMode::List => return PickerOutcome::Pending,
                };
                let value = buffer.trim().to_string();
                if value.is_empty() {
                    // Refuse to submit an empty Custom — bounce back so the
                    // user can either type something or `Esc` back to the list.
                    return PickerOutcome::Pending;
                }
                PickerOutcome::Submit {
                    kind,
                    value: Some(value),
                }
            }
            KeyCode::Backspace => {
                if let PickerMode::CustomInput { buffer } = &mut self.mode {
                    buffer.pop();
                }
                PickerOutcome::Pending
            }
            KeyCode::Char(c) => {
                if key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                {
                    return PickerOutcome::Pending;
                }
                if let PickerMode::CustomInput { buffer } = &mut self.mode {
                    buffer.push(c);
                }
                PickerOutcome::Pending
            }
            _ => PickerOutcome::Pending,
        }
    }

    /// Force-flip the picker into Custom-input mode. Called by the
    /// dispatcher when the user pressed Enter on the synthetic `Custom…`
    /// row (which `handle_key` reports as `Pending`); the dispatcher
    /// performs the transition so the state-machine's mutable-self borrow
    /// tracking stays simple.
    pub fn enter_custom_input(&mut self) {
        if matches!(self.mode, PickerMode::List) {
            self.mode = PickerMode::CustomInput {
                buffer: String::new(),
            };
        }
    }

    /// `true` when the user just confirmed the synthetic `Custom…` row —
    /// callers use this to decide whether to call `enter_custom_input`
    /// before re-rendering.
    pub fn is_custom_row_selected(&self) -> bool {
        matches!(self.items.get(self.selected), Some(PickerItem::Custom))
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Hint shown along the bottom of the modal in list mode.
const LIST_HINT: &str = " [j/k] move   [Enter] select   [Esc] cancel ";

/// Hint shown when the Custom-input field is active.
const INPUT_HINT: &str = " [Enter] save   [Esc] back ";

/// Draw the picker as a centered modal over `area`. Caller is expected to
/// render the underlying step-detail view immediately prior — `Clear` blanks
/// only the modal rectangle.
pub fn render(frame: &mut Frame, area: Rect, picker: &PickerState) {
    let dialog = centered_rect(area, picker);
    if dialog.width == 0 || dialog.height == 0 {
        return;
    }
    frame.render_widget(Clear, dialog);

    let block = Block::default()
        .title(format!(" {} ", picker.kind.title()))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));

    let mut lines: Vec<Line> = Vec::new();
    match &picker.mode {
        PickerMode::List => {
            if picker.items.is_empty() {
                lines.push(Line::from(Span::styled(
                    empty_placeholder(picker.kind),
                    Style::default().fg(theme::CHROME_DIM),
                )));
            } else {
                for (i, item) in picker.items.iter().enumerate() {
                    let style = if i == picker.selected {
                        Style::default()
                            .fg(theme::CURSOR)
                            .add_modifier(Modifier::REVERSED)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default()
                    };
                    lines.push(Line::from(Span::styled(
                        format!(" {} ", item.label()),
                        style,
                    )));
                }
            }
            lines.push(Line::from(""));
            lines.push(Line::styled(
                LIST_HINT,
                Style::default().add_modifier(Modifier::BOLD),
            ));
        }
        PickerMode::CustomInput { buffer } => {
            lines.push(Line::from(Span::styled(
                "Custom model:",
                Style::default()
                    .fg(theme::CURSOR)
                    .add_modifier(Modifier::BOLD),
            )));
            lines.push(Line::from(vec![
                Span::raw(buffer.clone()),
                Span::styled(
                    "▌",
                    Style::default()
                        .fg(theme::CURSOR)
                        .add_modifier(Modifier::SLOW_BLINK),
                ),
            ]));
            lines.push(Line::from(""));
            lines.push(Line::styled(
                INPUT_HINT,
                Style::default().add_modifier(Modifier::BOLD),
            ));
        }
    }

    let para = Paragraph::new(lines)
        .block(block)
        .wrap(Wrap { trim: false });
    frame.render_widget(para, dialog);
}

/// Dim placeholder text when a picker has no candidates to show. Gives the
/// user a hint about how to populate the list rather than a silent empty box.
fn empty_placeholder(kind: PickerKind) -> &'static str {
    match kind {
        PickerKind::Agent => "(no agents — run `ralph agents create <name>`)",
        PickerKind::Harness => "(no harnesses configured)",
        PickerKind::Model => "(no models — pick Custom… to type one)",
        PickerKind::ChangePolicy => "(no change policies)",
    }
}

fn centered_rect(area: Rect, picker: &PickerState) -> Rect {
    // Width: longest label (+ surrounding pad), title, hint — clamped to fit.
    let label_w = picker
        .items
        .iter()
        .map(|i| display_width(i.label()))
        .max()
        .unwrap_or(0);
    let title_w = display_width(picker.kind.title()) + 2;
    let hint_w = display_width(LIST_HINT).max(display_width(INPUT_HINT));
    let body_w = label_w.max(title_w).max(hint_w) + 4;
    let desired_w = (body_w as u16).max(40).min(area.width.max(20));

    // Height: 2 borders + items (or 2 lines for input) + spacer + hint.
    let body_h: u16 = match &picker.mode {
        PickerMode::List => picker.items.len().max(1) as u16 + 4,
        PickerMode::CustomInput { .. } => 6,
    };
    let desired_h = body_h.min(area.height).max(6.min(area.height));

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
    use crossterm::event::KeyEvent;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_with_mod(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    // -- BottomCell movement ----------------------------------------------

    #[test]
    fn bottom_cell_order_is_left_to_right() {
        assert_eq!(
            BottomCell::ORDER,
            [
                BottomCell::Harness,
                BottomCell::Model,
                BottomCell::Agent,
                BottomCell::ChangePolicy,
            ]
        );
    }

    #[test]
    fn move_left_walks_to_previous_cell() {
        assert_eq!(
            BottomCell::ChangePolicy.move_left(),
            Some(BottomCell::Agent)
        );
        assert_eq!(BottomCell::Agent.move_left(), Some(BottomCell::Model));
        assert_eq!(BottomCell::Model.move_left(), Some(BottomCell::Harness));
    }

    #[test]
    fn move_left_returns_none_at_leftmost() {
        // The outer handle_left falls through to popping the view in this
        // case, so returning None lets the caller distinguish.
        assert_eq!(BottomCell::Harness.move_left(), None);
    }

    #[test]
    fn move_right_walks_to_next_cell() {
        assert_eq!(BottomCell::Harness.move_right(), Some(BottomCell::Model));
        assert_eq!(BottomCell::Model.move_right(), Some(BottomCell::Agent));
        assert_eq!(
            BottomCell::Agent.move_right(),
            Some(BottomCell::ChangePolicy)
        );
    }

    #[test]
    fn move_right_returns_none_at_rightmost() {
        // l from the rightmost is a no-op per §8 keybinding table.
        assert_eq!(BottomCell::ChangePolicy.move_right(), None);
    }

    // -- Picker construction ----------------------------------------------

    #[test]
    fn for_harness_preselects_current() {
        let picker = PickerState::for_harness(
            &["claude".into(), "codex".into(), "goose".into()],
            Some("codex"),
        );
        assert_eq!(picker.kind, PickerKind::Harness);
        assert_eq!(picker.selected, 2);
        assert_eq!(picker.items.len(), 4);
        assert!(matches!(picker.items[0], PickerItem::Inherit));
        assert!(matches!(picker.mode, PickerMode::List));
    }

    #[test]
    fn for_harness_defaults_to_zero_when_current_unknown() {
        let picker = PickerState::for_harness(&["claude".into(), "codex".into()], Some("missing"));
        assert_eq!(picker.selected, 0);
    }

    #[test]
    fn for_harness_defaults_to_zero_when_current_unset() {
        let picker = PickerState::for_harness(&["claude".into(), "codex".into()], None);
        assert_eq!(picker.selected, 0);
    }

    #[test]
    fn for_model_includes_custom_entry() {
        let picker = PickerState::for_model(Some("claude-sonnet-4-6"), None);
        // [(inherit), default, Custom…]
        assert_eq!(picker.items.len(), 3);
        assert!(matches!(picker.items[0], PickerItem::Inherit));
        assert!(matches!(picker.items[1], PickerItem::Value(_)));
        assert!(matches!(picker.items[2], PickerItem::Custom));
        assert_eq!(picker.selected, 0);
    }

    #[test]
    fn for_model_with_no_default_has_inherit_and_custom() {
        let picker = PickerState::for_model(None, None);
        assert_eq!(picker.items.len(), 2);
        assert!(matches!(picker.items[0], PickerItem::Inherit));
        assert!(matches!(picker.items[1], PickerItem::Custom));
    }

    #[test]
    fn for_model_appends_step_override_distinct_from_default() {
        let picker = PickerState::for_model(Some("default-A"), Some("override-B"));
        // [(inherit), default-A, override-B, Custom…]
        assert_eq!(picker.items.len(), 4);
        // Step's current override is preselected.
        assert_eq!(picker.selected, 2);
    }

    #[test]
    fn for_model_does_not_duplicate_when_override_matches_default() {
        let picker = PickerState::for_model(Some("same"), Some("same"));
        // [(inherit), same, Custom…]
        assert_eq!(picker.items.len(), 3);
        assert_eq!(picker.selected, 1);
    }

    #[test]
    fn for_agent_preselects_current() {
        let picker = PickerState::for_agent(
            &["alpha".into(), "beta".into(), "gamma".into()],
            Some("beta"),
        );
        assert_eq!(picker.selected, 2);
        assert_eq!(picker.items.len(), 4);
        assert!(matches!(picker.items[0], PickerItem::Inherit));
    }

    #[test]
    fn for_agent_with_empty_list_still_offers_inherit() {
        let picker = PickerState::for_agent(&[], None);
        assert_eq!(picker.items.len(), 1);
        assert!(matches!(picker.items[0], PickerItem::Inherit));
    }

    #[test]
    fn for_change_policy_preselects_current_required() {
        let picker = PickerState::for_change_policy(ChangePolicy::Required);
        assert_eq!(picker.items.len(), 2);
        assert_eq!(picker.selected, 0);
    }

    #[test]
    fn for_change_policy_preselects_current_optional() {
        let picker = PickerState::for_change_policy(ChangePolicy::Optional);
        assert_eq!(picker.selected, 1);
    }

    // -- List-mode key handling -------------------------------------------

    #[test]
    fn esc_in_list_mode_cancels() {
        let mut picker = PickerState::for_change_policy(ChangePolicy::Required);
        assert_eq!(
            picker.handle_key(key(KeyCode::Esc)),
            PickerOutcome::Cancelled
        );
    }

    #[test]
    fn q_in_list_mode_cancels() {
        // Vim users expect q to back out of a modal; it's also consistent
        // with the rest of the TUI's cancellation paths.
        let mut picker = PickerState::for_change_policy(ChangePolicy::Required);
        assert_eq!(
            picker.handle_key(key(KeyCode::Char('q'))),
            PickerOutcome::Cancelled
        );
    }

    #[test]
    fn ctrl_c_in_list_mode_cancels() {
        let mut picker = PickerState::for_change_policy(ChangePolicy::Required);
        assert_eq!(
            picker.handle_key(key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            PickerOutcome::Cancelled
        );
    }

    #[test]
    fn j_and_k_navigate_selection() {
        let mut picker =
            PickerState::for_harness(&["claude".into(), "codex".into(), "goose".into()], None);
        assert_eq!(picker.selected, 0);
        assert_eq!(
            picker.handle_key(key(KeyCode::Char('j'))),
            PickerOutcome::Pending
        );
        assert_eq!(picker.selected, 1);
        assert_eq!(
            picker.handle_key(key(KeyCode::Char('j'))),
            PickerOutcome::Pending
        );
        assert_eq!(picker.selected, 2);
        assert_eq!(
            picker.handle_key(key(KeyCode::Char('j'))),
            PickerOutcome::Pending
        );
        assert_eq!(picker.selected, 3);
        // At the bottom — j is a no-op.
        assert_eq!(
            picker.handle_key(key(KeyCode::Char('j'))),
            PickerOutcome::Pending
        );
        assert_eq!(picker.selected, 3);
        assert_eq!(
            picker.handle_key(key(KeyCode::Char('k'))),
            PickerOutcome::Pending
        );
        assert_eq!(picker.selected, 2);
    }

    #[test]
    fn arrows_navigate_selection() {
        let mut picker = PickerState::for_harness(&["a".into(), "b".into(), "c".into()], None);
        picker.handle_key(key(KeyCode::Down));
        assert_eq!(picker.selected, 1);
        picker.handle_key(key(KeyCode::Up));
        assert_eq!(picker.selected, 0);
    }

    #[test]
    fn home_g_jumps_to_top_and_end_to_bottom() {
        let mut picker = PickerState::for_harness(&["a".into(), "b".into(), "c".into()], Some("c"));
        assert_eq!(picker.selected, 3);
        picker.handle_key(key(KeyCode::Char('g')));
        assert_eq!(picker.selected, 0);
        picker.handle_key(key(KeyCode::Char('G')));
        assert_eq!(picker.selected, 3);
    }

    #[test]
    fn enter_on_value_row_submits() {
        let mut picker =
            PickerState::for_harness(&["claude".into(), "codex".into()], Some("claude"));
        picker.handle_key(key(KeyCode::Char('j')));
        match picker.handle_key(key(KeyCode::Enter)) {
            PickerOutcome::Submit { kind, value } => {
                assert_eq!(kind, PickerKind::Harness);
                assert_eq!(value.as_deref(), Some("codex"));
            }
            other => panic!("expected Submit, got {other:?}"),
        }
    }

    #[test]
    fn enter_on_custom_row_returns_pending() {
        // The dispatcher transitions to CustomInput mode itself by calling
        // enter_custom_input — the state machine just reports Pending so
        // there's no submit racing the mode flip.
        let mut picker = PickerState::for_model(Some("default"), None);
        // [(inherit), default, Custom…] — move to Custom.
        picker.handle_key(key(KeyCode::Char('G')));
        assert!(picker.is_custom_row_selected());
        assert_eq!(
            picker.handle_key(key(KeyCode::Enter)),
            PickerOutcome::Pending
        );
    }

    #[test]
    fn enter_custom_input_flips_mode_when_on_custom_row() {
        let mut picker = PickerState::for_model(Some("default"), None);
        picker.handle_key(key(KeyCode::Char('G')));
        picker.enter_custom_input();
        assert!(matches!(
            picker.mode,
            PickerMode::CustomInput { ref buffer } if buffer.is_empty()
        ));
    }

    #[test]
    fn enter_custom_input_is_noop_when_already_in_input() {
        let mut picker = PickerState::for_model(Some("default"), None);
        picker.handle_key(key(KeyCode::Char('G')));
        picker.enter_custom_input();
        if let PickerMode::CustomInput { ref mut buffer } = picker.mode {
            buffer.push_str("anthropic-9999");
        }
        // Idempotent — does not wipe the typed buffer.
        picker.enter_custom_input();
        assert!(matches!(
            picker.mode,
            PickerMode::CustomInput { ref buffer } if buffer == "anthropic-9999"
        ));
    }

    #[test]
    fn enter_on_inherit_row_submits_none() {
        let mut picker = PickerState::for_agent(&[], None);
        match picker.handle_key(key(KeyCode::Enter)) {
            PickerOutcome::Submit { kind, value } => {
                assert_eq!(kind, PickerKind::Agent);
                assert_eq!(value, None);
            }
            other => panic!("expected inherit Submit, got {other:?}"),
        }
    }

    // -- CustomInput-mode key handling ------------------------------------

    fn make_custom_input(seed: &str) -> PickerState {
        let mut picker = PickerState::for_model(Some("default"), None);
        // Move to "Custom…" and flip to input mode.
        picker.handle_key(key(KeyCode::Char('G')));
        picker.enter_custom_input();
        if let PickerMode::CustomInput { ref mut buffer } = picker.mode {
            buffer.push_str(seed);
        }
        picker
    }

    #[test]
    fn typing_appends_to_custom_buffer() {
        let mut picker = make_custom_input("");
        for c in "claude-opus-4-7".chars() {
            assert_eq!(
                picker.handle_key(key(KeyCode::Char(c))),
                PickerOutcome::Pending
            );
        }
        match &picker.mode {
            PickerMode::CustomInput { buffer } => assert_eq!(buffer, "claude-opus-4-7"),
            other => panic!("expected CustomInput, got {other:?}"),
        }
    }

    #[test]
    fn backspace_pops_custom_buffer() {
        let mut picker = make_custom_input("abc");
        picker.handle_key(key(KeyCode::Backspace));
        match &picker.mode {
            PickerMode::CustomInput { buffer } => assert_eq!(buffer, "ab"),
            other => panic!("expected CustomInput, got {other:?}"),
        }
        // Extra backspace at the start is a no-op.
        picker.handle_key(key(KeyCode::Backspace));
        picker.handle_key(key(KeyCode::Backspace));
        picker.handle_key(key(KeyCode::Backspace));
        match &picker.mode {
            PickerMode::CustomInput { buffer } => assert!(buffer.is_empty()),
            other => panic!("expected CustomInput, got {other:?}"),
        }
    }

    #[test]
    fn enter_in_custom_input_with_text_submits_trimmed() {
        let mut picker = make_custom_input("  claude-opus-4-7  ");
        match picker.handle_key(key(KeyCode::Enter)) {
            PickerOutcome::Submit { kind, value } => {
                assert_eq!(kind, PickerKind::Model);
                assert_eq!(value.as_deref(), Some("claude-opus-4-7"));
            }
            other => panic!("expected Submit, got {other:?}"),
        }
    }

    #[test]
    fn enter_in_custom_input_with_empty_buffer_is_pending() {
        let mut picker = make_custom_input("");
        assert_eq!(
            picker.handle_key(key(KeyCode::Enter)),
            PickerOutcome::Pending
        );
    }

    #[test]
    fn enter_in_custom_input_with_only_whitespace_is_pending() {
        let mut picker = make_custom_input("    ");
        assert_eq!(
            picker.handle_key(key(KeyCode::Enter)),
            PickerOutcome::Pending
        );
    }

    #[test]
    fn esc_in_custom_input_returns_to_list_mode() {
        // Esc shouldn't strand the user — they fall back to the list and can
        // try a different option (or Esc again to fully cancel).
        let mut picker = make_custom_input("typo");
        assert_eq!(picker.handle_key(key(KeyCode::Esc)), PickerOutcome::Pending);
        assert!(matches!(picker.mode, PickerMode::List));
    }

    #[test]
    fn ctrl_c_in_custom_input_cancels() {
        // Hard cancel — Ctrl-C bypasses the list-fallback intent of Esc.
        let mut picker = make_custom_input("typo");
        assert_eq!(
            picker.handle_key(key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            PickerOutcome::Cancelled
        );
    }

    #[test]
    fn ctrl_modified_chars_ignored_in_custom_input() {
        let mut picker = make_custom_input("");
        let outcome = picker.handle_key(key_with_mod(KeyCode::Char('a'), KeyModifiers::CONTROL));
        assert_eq!(outcome, PickerOutcome::Pending);
        match &picker.mode {
            PickerMode::CustomInput { buffer } => assert!(buffer.is_empty()),
            other => panic!("expected CustomInput, got {other:?}"),
        }
    }

    // -- Render smoke -----------------------------------------------------

    fn render_to_string(width: u16, height: u16, picker: &PickerState) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal.draw(|f| render(f, f.area(), picker)).unwrap();
        let buf = terminal.backend().buffer().clone();
        (0..buf.area().height)
            .map(|y| {
                (0..buf.area().width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn render_shows_title_and_items() {
        let picker = PickerState::for_harness(
            &["claude".into(), "codex".into(), "goose".into()],
            Some("codex"),
        );
        let out = render_to_string(80, 20, &picker);
        assert!(out.contains("Pick harness"), "title missing:\n{out}");
        assert!(out.contains("claude"), "item missing:\n{out}");
        assert!(out.contains("codex"), "item missing:\n{out}");
        assert!(out.contains("goose"), "item missing:\n{out}");
        assert!(out.contains("[Enter] select"), "hint missing:\n{out}");
    }

    #[test]
    fn render_shows_custom_label_for_model_picker() {
        let picker = PickerState::for_model(Some("claude-sonnet-4-6"), None);
        let out = render_to_string(80, 20, &picker);
        assert!(out.contains("Custom"), "Custom row missing:\n{out}");
    }

    #[test]
    fn render_shows_input_mode_after_custom_chosen() {
        let mut picker = PickerState::for_model(Some("default"), None);
        picker.handle_key(key(KeyCode::Char('G')));
        picker.enter_custom_input();
        if let PickerMode::CustomInput { ref mut buffer } = picker.mode {
            buffer.push_str("typed");
        }
        let out = render_to_string(80, 20, &picker);
        assert!(out.contains("Custom model"), "input prompt missing:\n{out}");
        assert!(out.contains("typed"), "buffer missing:\n{out}");
        assert!(out.contains("[Enter] save"), "input hint missing:\n{out}");
    }

    #[test]
    fn render_shows_inherit_for_empty_agent_list() {
        let picker = PickerState::for_agent(&[], None);
        let out = render_to_string(80, 20, &picker);
        assert!(
            out.contains("(inherit)"),
            "inherit row missing for empty agent list:\n{out}"
        );
    }

    #[test]
    fn render_does_not_panic_on_tiny_terminal() {
        let picker = PickerState::for_harness(&["claude".into()], Some("claude"));
        let _ = render_to_string(8, 4, &picker);
    }

    #[test]
    fn render_change_policy_picker_lists_both_options() {
        let picker = PickerState::for_change_policy(ChangePolicy::Required);
        let out = render_to_string(80, 20, &picker);
        assert!(out.contains("required"), "required row missing:\n{out}");
        assert!(out.contains("optional"), "optional row missing:\n{out}");
    }
}
