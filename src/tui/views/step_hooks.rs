// Step-hook attachment sub-view (TUI-plan.md §1, step 35).
//
// A focused sub-view over the step-detail screen that lets the user inspect,
// add, and remove per-step hook attachments. Entered via the `/step
// set-hook|unset-hook` palette commands or a keybinding from step_detail.
//
// State machine: three modes mirroring `plan_hooks::Mode`. `List` shows the
// step's current attachments grouped by lifecycle as a `ratatui::Table`. `a`
// opens `LifecyclePicker` which steps through the four
// `hook_library::Lifecycle` variants; selecting one transitions to
// `HookPicker { lifecycle }` listing every hook in the library that isn't
// already attached at that lifecycle. `d` in `List` mode requests removal of
// the highlighted edge.
//
// Like `plan_hooks`, the view is split into a pure state machine
// (`StepHooksApp`) and a renderer (`render`) so we can drive the state
// machine in tests without a real terminal. All write-throughs (attach via
// `storage::attach_hook_to_step` / detach via `storage::detach_hook` with
// `step_id == Some(...)`) live in the dispatcher loop alongside the entry
// point; this module only exposes the user's intent through `Outcome` and
// surfaces toasts for feedback.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Cell, Clear, Paragraph, Row, Table, TableState};

use crate::hook_library::Lifecycle;
use crate::tui::theme;
use crate::tui::toast::{ToastKind, ToastQueue};

// ---------------------------------------------------------------------------
// Sub-view state
// ---------------------------------------------------------------------------

/// One step-scoped hook attachment row, projected from
/// [`crate::storage::StepHookRow`] after filtering out the plan-wide rows
/// (those are managed by `plan_hooks.rs`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepHookRef {
    pub lifecycle: Lifecycle,
    pub hook_name: String,
}

/// A hook available in the library. The dispatcher builds these from
/// `hook_library::load_all()` filtered by project scope before calling
/// [`StepHooksApp::new`] / [`StepHooksApp::refresh`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HookCandidate {
    pub name: String,
    pub description: String,
}

/// Whether the sub-view is showing the current attachment list or one of the
/// two picker overlays for adding a new attachment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// Default mode: attachment table with `a`/`d`/`q`/`<esc>` keybindings.
    List,
    /// First step of the add flow: pick a lifecycle. `j`/`k` moves the
    /// cursor, `Enter` advances to [`Mode::HookPicker`], `<esc>`/`q` falls
    /// back to `List`.
    LifecyclePicker,
    /// Second step of the add flow: pick a hook name. `j`/`k` moves the
    /// cursor, `Enter` requests an add, `<esc>`/`q` falls back to
    /// [`Mode::LifecyclePicker`].
    HookPicker { lifecycle: Lifecycle },
}

/// What [`StepHooksApp::handle_key`] returns each turn. The dispatcher loop
/// runs the side effects (storage writes, refresh, toast push) and keeps
/// looping on `Pending`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Key consumed; no side effect required.
    Pending,
    /// User pressed `q` / `<esc>` / Ctrl-C in `List` mode — pop the sub-view.
    Pop,
    /// User pressed `Enter` on a hook-picker row — request the new
    /// attachment via [`crate::storage::attach_hook_to_step`].
    AddRequested {
        lifecycle: Lifecycle,
        hook_name: String,
    },
    /// User pressed `d` on a row — request detachment via
    /// [`crate::storage::detach_hook`] with `step_id == Some(...)`.
    RemoveRequested {
        lifecycle: Lifecycle,
        hook_name: String,
    },
}

/// The four lifecycles in display order (matches `hook_library::Lifecycle`'s
/// declaration order).
pub const LIFECYCLES: [Lifecycle; 4] = [
    Lifecycle::PreStep,
    Lifecycle::PostStep,
    Lifecycle::PreTest,
    Lifecycle::PostTest,
];

/// Sub-view state.
pub struct StepHooksApp {
    /// Plan that owns the step being edited. Carried so the dispatcher loop
    /// can wire the storage call without re-resolving the parent.
    pub plan_id: String,
    /// The step whose hooks are being edited.
    pub step_id: String,
    /// Display slug of the parent plan, used in the title bar.
    pub plan_slug: String,
    /// Display label for the step (e.g. "#3 — Step title"), used in the
    /// title bar so the user always knows which step they're scoped to.
    pub step_label: String,
    /// Current per-step attachments, sorted by `(lifecycle, hook_name)` so
    /// the table is stable across refreshes.
    pub attachments: Vec<StepHookRef>,
    /// Every hook in the library applicable to this project. The hook picker
    /// further filters to those not already attached at the chosen lifecycle.
    pub all_hooks: Vec<HookCandidate>,
    /// 0-based cursor in the attachments table.
    pub list_cursor: usize,
    /// 0-based cursor in the lifecycle picker (always 0..LIFECYCLES.len()).
    pub lifecycle_cursor: usize,
    /// 0-based cursor in the hook picker (clamped against the filtered list).
    pub hook_cursor: usize,
    /// Current mode (List / LifecyclePicker / HookPicker).
    pub mode: Mode,
    /// Toast queue rendered over the bottom hint row.
    pub toasts: ToastQueue,
}

impl StepHooksApp {
    /// Build a new sub-view with the given attachments / hook-library
    /// snapshots. Attachments are sorted into a stable display order.
    pub fn new(
        plan_id: String,
        step_id: String,
        plan_slug: String,
        step_label: String,
        attachments: Vec<StepHookRef>,
        all_hooks: Vec<HookCandidate>,
    ) -> Self {
        let mut attachments = attachments;
        sort_attachments(&mut attachments);
        Self {
            plan_id,
            step_id,
            plan_slug,
            step_label,
            attachments,
            all_hooks,
            list_cursor: 0,
            lifecycle_cursor: 0,
            hook_cursor: 0,
            mode: Mode::List,
            toasts: ToastQueue::new(),
        }
    }

    /// Replace the attachment / library snapshots after a successful write,
    /// clamping cursors so they don't dangle past the new list lengths.
    pub fn refresh(&mut self, attachments: Vec<StepHookRef>, all_hooks: Vec<HookCandidate>) {
        let mut attachments = attachments;
        sort_attachments(&mut attachments);
        self.attachments = attachments;
        self.all_hooks = all_hooks;
        if self.list_cursor >= self.attachments.len() {
            self.list_cursor = self.attachments.len().saturating_sub(1);
        }
        // Reset picker cursors — refresh implies the picker has done its job.
        self.lifecycle_cursor = self.lifecycle_cursor.min(LIFECYCLES.len() - 1);
        self.hook_cursor = 0;
    }

    /// Push a toast onto the queue using the system clock for `expires_at`.
    pub fn push_toast(&mut self, msg: impl Into<String>, kind: ToastKind) {
        self.toasts.push(msg, kind, std::time::Instant::now());
    }

    /// Hooks available to attach at `lifecycle` — i.e. every library hook not
    /// already attached at that lifecycle for this step. The cursor in
    /// `HookPicker` indexes into this slice.
    pub fn candidates_for(&self, lifecycle: Lifecycle) -> Vec<&HookCandidate> {
        self.all_hooks
            .iter()
            .filter(|h| {
                !self
                    .attachments
                    .iter()
                    .any(|a| a.lifecycle == lifecycle && a.hook_name == h.name)
            })
            .collect()
    }

    /// Pure key handler. Routes to the per-mode handler so tests can drive
    /// arbitrary key sequences without crossterm.
    pub fn handle_key(&mut self, key: KeyEvent) -> Outcome {
        // Ctrl-C always pops the sub-view, mirroring the plan-detail view.
        if let KeyCode::Char('c') = key.code
            && key.modifiers.contains(KeyModifiers::CONTROL)
        {
            return Outcome::Pop;
        }
        match self.mode {
            Mode::List => self.handle_list_key(key),
            Mode::LifecyclePicker => self.handle_lifecycle_key(key),
            Mode::HookPicker { lifecycle } => self.handle_hook_key(key, lifecycle),
        }
    }

    fn handle_list_key(&mut self, key: KeyEvent) -> Outcome {
        match key.code {
            // Navigation
            KeyCode::Char('j') | KeyCode::Down => {
                if !self.attachments.is_empty()
                    && self.list_cursor + 1 < self.attachments.len()
                {
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
                if !self.attachments.is_empty() {
                    self.list_cursor = 0;
                }
                Outcome::Pending
            }
            KeyCode::Char('G') | KeyCode::End => {
                if !self.attachments.is_empty() {
                    self.list_cursor = self.attachments.len() - 1;
                }
                Outcome::Pending
            }

            // Open lifecycle picker for adding a new attachment.
            KeyCode::Char('a') => {
                if self.all_hooks.is_empty() {
                    self.push_toast(
                        "No hooks in library. Run `ralph hooks add` first.",
                        ToastKind::Info,
                    );
                    return Outcome::Pending;
                }
                self.lifecycle_cursor = 0;
                self.mode = Mode::LifecyclePicker;
                Outcome::Pending
            }

            // Remove the highlighted attachment.
            KeyCode::Char('d') => {
                if let Some(target) = self.attachments.get(self.list_cursor) {
                    Outcome::RemoveRequested {
                        lifecycle: target.lifecycle,
                        hook_name: target.hook_name.clone(),
                    }
                } else {
                    Outcome::Pending
                }
            }

            // Pop the sub-view.
            KeyCode::Char('q') | KeyCode::Esc | KeyCode::Char('h') | KeyCode::Left => {
                Outcome::Pop
            }

            _ => Outcome::Pending,
        }
    }

    fn handle_lifecycle_key(&mut self, key: KeyEvent) -> Outcome {
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                if self.lifecycle_cursor + 1 < LIFECYCLES.len() {
                    self.lifecycle_cursor += 1;
                }
                Outcome::Pending
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if self.lifecycle_cursor > 0 {
                    self.lifecycle_cursor -= 1;
                }
                Outcome::Pending
            }
            KeyCode::Char('g') | KeyCode::Home => {
                self.lifecycle_cursor = 0;
                Outcome::Pending
            }
            KeyCode::Char('G') | KeyCode::End => {
                self.lifecycle_cursor = LIFECYCLES.len() - 1;
                Outcome::Pending
            }

            KeyCode::Enter => {
                let lifecycle = LIFECYCLES[self.lifecycle_cursor];
                let candidates = self.candidates_for(lifecycle);
                if candidates.is_empty() {
                    self.push_toast(
                        format!("No remaining hooks to attach at {lifecycle}."),
                        ToastKind::Info,
                    );
                    return Outcome::Pending;
                }
                self.hook_cursor = 0;
                self.mode = Mode::HookPicker { lifecycle };
                Outcome::Pending
            }

            // Cancel back to list mode.
            KeyCode::Esc | KeyCode::Char('q') => {
                self.mode = Mode::List;
                Outcome::Pending
            }

            _ => Outcome::Pending,
        }
    }

    fn handle_hook_key(&mut self, key: KeyEvent, lifecycle: Lifecycle) -> Outcome {
        let candidates = self.candidates_for(lifecycle);
        match key.code {
            KeyCode::Char('j') | KeyCode::Down => {
                if !candidates.is_empty() && self.hook_cursor + 1 < candidates.len() {
                    self.hook_cursor += 1;
                }
                Outcome::Pending
            }
            KeyCode::Char('k') | KeyCode::Up => {
                if self.hook_cursor > 0 {
                    self.hook_cursor -= 1;
                }
                Outcome::Pending
            }
            KeyCode::Char('g') | KeyCode::Home => {
                if !candidates.is_empty() {
                    self.hook_cursor = 0;
                }
                Outcome::Pending
            }
            KeyCode::Char('G') | KeyCode::End => {
                if !candidates.is_empty() {
                    self.hook_cursor = candidates.len() - 1;
                }
                Outcome::Pending
            }

            KeyCode::Enter => {
                if let Some(candidate) = candidates.get(self.hook_cursor) {
                    Outcome::AddRequested {
                        lifecycle,
                        hook_name: candidate.name.clone(),
                    }
                } else {
                    Outcome::Pending
                }
            }

            // Cancel back to lifecycle picker, NOT to list.
            KeyCode::Esc | KeyCode::Char('q') => {
                self.mode = Mode::LifecyclePicker;
                Outcome::Pending
            }

            _ => Outcome::Pending,
        }
    }
}

/// Stable display order: lifecycle (declaration order), then hook name
/// (lexicographic). Keeps row positions consistent across refreshes so the
/// `list_cursor` lands where the user expects.
fn sort_attachments(rows: &mut [StepHookRef]) {
    rows.sort_by(|a, b| {
        lifecycle_index(a.lifecycle)
            .cmp(&lifecycle_index(b.lifecycle))
            .then_with(|| a.hook_name.cmp(&b.hook_name))
    });
}

fn lifecycle_index(l: Lifecycle) -> usize {
    LIFECYCLES.iter().position(|x| *x == l).unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

const LIST_HINT: &str = " [a] add   [d] remove   [j/k] move   [q/Esc] back ";
const LIFECYCLE_HINT: &str = " [Enter] choose   [j/k] move   [Esc] cancel ";
const HOOK_HINT: &str = " [Enter] attach   [j/k] move   [Esc] back ";

/// Draw the attachment table over `area`. When a picker is open, render the
/// picker overlay on top.
pub fn render(frame: &mut Frame, area: Rect, app: &mut StepHooksApp) {
    if area.width == 0 || area.height == 0 {
        return;
    }

    // -- Title + hint row --------------------------------------------------
    let title = format!(" Step hooks — {} / {} ", app.plan_slug, app.step_label);
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

    // -- Body: attachment table -------------------------------------------
    if app.attachments.is_empty() {
        let para = Paragraph::new(Line::from(Span::styled(
            "(no per-step hooks — press `a` to attach one)",
            Style::default().fg(theme::CHROME_DIM),
        )));
        frame.render_widget(para, body_area);
    } else {
        let header = Row::new(vec![
            Cell::from("#"),
            Cell::from("Lifecycle"),
            Cell::from("Hook"),
        ])
        .style(Style::default().add_modifier(Modifier::BOLD))
        .bottom_margin(0);
        let rows: Vec<Row> = app
            .attachments
            .iter()
            .enumerate()
            .map(|(i, att)| {
                Row::new(vec![
                    Cell::from(format!("{}", i + 1)),
                    Cell::from(att.lifecycle.as_str()),
                    Cell::from(att.hook_name.clone()),
                ])
            })
            .collect();
        let widths = [
            Constraint::Length(4),
            Constraint::Length(11),
            Constraint::Min(1),
        ];
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
        if !app.attachments.is_empty() {
            state.select(Some(app.list_cursor.min(app.attachments.len() - 1)));
        }
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
            Mode::LifecyclePicker => LIFECYCLE_HINT,
            Mode::HookPicker { .. } => HOOK_HINT,
        };
        Line::from(Span::styled(
            hint,
            Style::default().fg(theme::CHROME_DIM),
        ))
    };
    let hint = Paragraph::new(hint_line);
    frame.render_widget(hint, hint_area);

    // -- Picker overlays --------------------------------------------------
    match app.mode {
        Mode::List => {}
        Mode::LifecyclePicker => render_lifecycle_picker(frame, area, app),
        Mode::HookPicker { lifecycle } => render_hook_picker(frame, area, app, lifecycle),
    }
}

fn render_lifecycle_picker(frame: &mut Frame, area: Rect, app: &StepHooksApp) {
    let dialog = centered_picker_rect(area, 24, LIFECYCLES.len());
    if dialog.width == 0 || dialog.height == 0 {
        return;
    }
    frame.render_widget(Clear, dialog);

    let block = Block::default()
        .title(" Choose lifecycle ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));

    let mut lines: Vec<Line> = Vec::new();
    for (i, lifecycle) in LIFECYCLES.iter().enumerate() {
        let style = if i == app.lifecycle_cursor {
            Style::default()
                .fg(theme::CURSOR)
                .add_modifier(Modifier::REVERSED)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        lines.push(Line::from(Span::styled(
            format!(" {} ", lifecycle.as_str()),
            style,
        )));
    }
    lines.push(Line::from(""));
    lines.push(Line::styled(
        LIFECYCLE_HINT,
        Style::default().add_modifier(Modifier::BOLD),
    ));

    let para = Paragraph::new(lines).block(block);
    frame.render_widget(para, dialog);
}

fn render_hook_picker(
    frame: &mut Frame,
    area: Rect,
    app: &StepHooksApp,
    lifecycle: Lifecycle,
) {
    let candidates = app.candidates_for(lifecycle);
    let max_label = candidates
        .iter()
        .map(|c| c.name.chars().count())
        .max()
        .unwrap_or(0);
    let body_w = max_label.max(HOOK_HINT.chars().count()) + 4;
    let row_count = candidates.len().max(1);
    let dialog = centered_picker_rect(area, body_w as u16, row_count);
    if dialog.width == 0 || dialog.height == 0 {
        return;
    }
    frame.render_widget(Clear, dialog);

    let block = Block::default()
        .title(format!(" Attach hook at {} ", lifecycle.as_str()))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));

    let mut lines: Vec<Line> = Vec::new();
    if candidates.is_empty() {
        lines.push(Line::from(Span::styled(
            "(no candidates)",
            Style::default().fg(theme::CHROME_DIM),
        )));
    } else {
        for (i, candidate) in candidates.iter().enumerate() {
            let style = if i == app.hook_cursor {
                Style::default()
                    .fg(theme::CURSOR)
                    .add_modifier(Modifier::REVERSED)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            };
            lines.push(Line::from(Span::styled(
                format!(" {} ", candidate.name),
                style,
            )));
        }
    }
    lines.push(Line::from(""));
    lines.push(Line::styled(
        HOOK_HINT,
        Style::default().add_modifier(Modifier::BOLD),
    ));

    let para = Paragraph::new(lines).block(block);
    frame.render_widget(para, dialog);
}

fn centered_picker_rect(area: Rect, body_w: u16, row_count: usize) -> Rect {
    use ratatui::layout::{Flex, Layout};

    let desired_w = body_w.max(40).min(area.width);
    let desired_h = ((row_count + 4) as u16).min(area.height);
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

    fn att(lifecycle: Lifecycle, name: &str) -> StepHookRef {
        StepHookRef {
            lifecycle,
            hook_name: name.to_string(),
        }
    }

    fn cand(name: &str) -> HookCandidate {
        HookCandidate {
            name: name.to_string(),
            description: String::new(),
        }
    }

    fn app_with(
        attachments: Vec<StepHookRef>,
        all_hooks: Vec<HookCandidate>,
    ) -> StepHooksApp {
        StepHooksApp::new(
            "p1".into(),
            "s1".into(),
            "parent".into(),
            "#1 — Step".into(),
            attachments,
            all_hooks,
        )
    }

    // -- Pure state-machine tests ----------------------------------------

    #[test]
    fn new_starts_in_list_mode_with_zero_cursor() {
        let app = app_with(
            vec![att(Lifecycle::PreStep, "fmt")],
            vec![cand("fmt"), cand("clippy")],
        );
        assert_eq!(app.mode, Mode::List);
        assert_eq!(app.list_cursor, 0);
        assert_eq!(app.lifecycle_cursor, 0);
        assert_eq!(app.hook_cursor, 0);
    }

    #[test]
    fn new_sorts_attachments_by_lifecycle_then_name() {
        let app = app_with(
            vec![
                att(Lifecycle::PostTest, "z"),
                att(Lifecycle::PreStep, "b"),
                att(Lifecycle::PreStep, "a"),
                att(Lifecycle::PostStep, "m"),
            ],
            vec![],
        );
        assert_eq!(app.attachments[0], att(Lifecycle::PreStep, "a"));
        assert_eq!(app.attachments[1], att(Lifecycle::PreStep, "b"));
        assert_eq!(app.attachments[2], att(Lifecycle::PostStep, "m"));
        assert_eq!(app.attachments[3], att(Lifecycle::PostTest, "z"));
    }

    #[test]
    fn j_moves_list_cursor_down_clamped() {
        let mut app = app_with(
            vec![
                att(Lifecycle::PreStep, "a"),
                att(Lifecycle::PostStep, "b"),
            ],
            vec![],
        );
        assert_eq!(app.handle_key(key(KeyCode::Char('j'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 1);
        assert_eq!(app.handle_key(key(KeyCode::Char('j'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 1);
    }

    #[test]
    fn k_moves_list_cursor_up_clamped() {
        let mut app = app_with(
            vec![
                att(Lifecycle::PreStep, "a"),
                att(Lifecycle::PostStep, "b"),
            ],
            vec![],
        );
        app.list_cursor = 1;
        assert_eq!(app.handle_key(key(KeyCode::Char('k'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 0);
        assert_eq!(app.handle_key(key(KeyCode::Char('k'))), Outcome::Pending);
        assert_eq!(app.list_cursor, 0);
    }

    #[test]
    fn g_jumps_to_top_capital_g_to_bottom() {
        let mut app = app_with(
            vec![
                att(Lifecycle::PreStep, "a"),
                att(Lifecycle::PreStep, "b"),
                att(Lifecycle::PreStep, "c"),
            ],
            vec![],
        );
        app.handle_key(key(KeyCode::Char('G')));
        assert_eq!(app.list_cursor, 2);
        app.handle_key(key(KeyCode::Char('g')));
        assert_eq!(app.list_cursor, 0);
    }

    #[test]
    fn a_opens_lifecycle_picker_when_library_has_hooks() {
        let mut app = app_with(vec![], vec![cand("fmt")]);
        assert_eq!(app.handle_key(key(KeyCode::Char('a'))), Outcome::Pending);
        assert_eq!(app.mode, Mode::LifecyclePicker);
        assert_eq!(app.lifecycle_cursor, 0);
    }

    #[test]
    fn a_with_empty_library_toasts_and_stays_in_list() {
        let mut app = app_with(vec![], vec![]);
        assert_eq!(app.handle_key(key(KeyCode::Char('a'))), Outcome::Pending);
        assert_eq!(app.mode, Mode::List);
        let toast = app.toasts.current().expect("toast pushed");
        assert!(toast.text.contains("No hooks in library"));
    }

    #[test]
    fn d_emits_remove_request_for_cursor_row_each_lifecycle() {
        // Cover all four lifecycles in a single attachment list to prove the
        // remove-cursor is wired correctly for each variant.
        let attachments = vec![
            att(Lifecycle::PreStep, "p1"),
            att(Lifecycle::PostStep, "p2"),
            att(Lifecycle::PreTest, "p3"),
            att(Lifecycle::PostTest, "p4"),
        ];
        for (i, expected) in attachments.iter().enumerate() {
            let mut app = app_with(attachments.clone(), vec![]);
            app.list_cursor = i;
            let outcome = app.handle_key(key(KeyCode::Char('d')));
            assert_eq!(
                outcome,
                Outcome::RemoveRequested {
                    lifecycle: expected.lifecycle,
                    hook_name: expected.hook_name.clone(),
                }
            );
        }
    }

    #[test]
    fn d_with_empty_attachments_is_pending() {
        let mut app = app_with(vec![], vec![cand("fmt")]);
        assert_eq!(app.handle_key(key(KeyCode::Char('d'))), Outcome::Pending);
    }

    #[test]
    fn q_pops_in_list_mode() {
        let mut app = app_with(vec![att(Lifecycle::PreStep, "fmt")], vec![]);
        assert_eq!(app.handle_key(key(KeyCode::Char('q'))), Outcome::Pop);
    }

    #[test]
    fn esc_pops_in_list_mode() {
        let mut app = app_with(vec![], vec![]);
        assert_eq!(app.handle_key(key(KeyCode::Esc)), Outcome::Pop);
    }

    #[test]
    fn ctrl_c_pops_in_any_mode() {
        let mut app = app_with(vec![], vec![cand("fmt")]);
        // List mode.
        assert_eq!(
            app.handle_key(key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Outcome::Pop
        );
        // Lifecycle picker.
        app.handle_key(key(KeyCode::Char('a')));
        assert_eq!(app.mode, Mode::LifecyclePicker);
        assert_eq!(
            app.handle_key(key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Outcome::Pop
        );
        // Hook picker.
        app.handle_key(key(KeyCode::Char('a')));
        app.handle_key(key(KeyCode::Enter));
        assert!(matches!(app.mode, Mode::HookPicker { .. }));
        assert_eq!(
            app.handle_key(key_with_mod(KeyCode::Char('c'), KeyModifiers::CONTROL)),
            Outcome::Pop
        );
    }

    #[test]
    fn lifecycle_picker_navigates_clamped() {
        let mut app = app_with(vec![], vec![cand("fmt")]);
        app.handle_key(key(KeyCode::Char('a')));
        assert_eq!(app.mode, Mode::LifecyclePicker);

        // Advance through all four lifecycles.
        for i in 1..LIFECYCLES.len() {
            app.handle_key(key(KeyCode::Char('j')));
            assert_eq!(app.lifecycle_cursor, i);
        }
        // Clamp at the bottom.
        app.handle_key(key(KeyCode::Char('j')));
        assert_eq!(app.lifecycle_cursor, LIFECYCLES.len() - 1);
        // Clamp at the top.
        for _ in 0..LIFECYCLES.len() {
            app.handle_key(key(KeyCode::Char('k')));
        }
        assert_eq!(app.lifecycle_cursor, 0);
    }

    #[test]
    fn lifecycle_enter_with_candidates_opens_hook_picker() {
        let mut app = app_with(vec![], vec![cand("fmt"), cand("clippy")]);
        app.handle_key(key(KeyCode::Char('a')));
        assert_eq!(app.handle_key(key(KeyCode::Enter)), Outcome::Pending);
        assert_eq!(
            app.mode,
            Mode::HookPicker {
                lifecycle: Lifecycle::PreStep,
            }
        );
        assert_eq!(app.hook_cursor, 0);
    }

    #[test]
    fn lifecycle_enter_with_no_remaining_candidates_toasts() {
        // Library has only one hook ("fmt"), already attached at PreStep.
        let mut app = app_with(
            vec![att(Lifecycle::PreStep, "fmt")],
            vec![cand("fmt")],
        );
        app.handle_key(key(KeyCode::Char('a')));
        assert_eq!(app.mode, Mode::LifecyclePicker);
        // PreStep cursor — Enter should toast since fmt is already attached.
        assert_eq!(app.handle_key(key(KeyCode::Enter)), Outcome::Pending);
        assert_eq!(app.mode, Mode::LifecyclePicker);
        let toast = app.toasts.current().expect("toast pushed");
        assert!(toast.text.contains("No remaining hooks"));
    }

    #[test]
    fn lifecycle_picker_esc_falls_back_to_list_mode() {
        let mut app = app_with(vec![], vec![cand("fmt")]);
        app.handle_key(key(KeyCode::Char('a')));
        assert_eq!(app.handle_key(key(KeyCode::Esc)), Outcome::Pending);
        assert_eq!(app.mode, Mode::List);
    }

    #[test]
    fn hook_picker_navigates_clamped() {
        let mut app = app_with(
            vec![],
            vec![cand("a"), cand("b"), cand("c")],
        );
        app.handle_key(key(KeyCode::Char('a')));
        app.handle_key(key(KeyCode::Enter));
        assert!(matches!(app.mode, Mode::HookPicker { .. }));
        // j moves down, clamps at len-1.
        app.handle_key(key(KeyCode::Char('j')));
        assert_eq!(app.hook_cursor, 1);
        app.handle_key(key(KeyCode::Char('j')));
        assert_eq!(app.hook_cursor, 2);
        app.handle_key(key(KeyCode::Char('j')));
        assert_eq!(app.hook_cursor, 2);
        // k moves up, clamps at 0.
        app.handle_key(key(KeyCode::Char('k')));
        assert_eq!(app.hook_cursor, 1);
        app.handle_key(key(KeyCode::Char('k')));
        assert_eq!(app.hook_cursor, 0);
        app.handle_key(key(KeyCode::Char('k')));
        assert_eq!(app.hook_cursor, 0);
    }

    #[test]
    fn hook_enter_emits_add_request_for_each_lifecycle() {
        for (i, lifecycle) in LIFECYCLES.iter().enumerate() {
            let mut app = app_with(vec![], vec![cand("fmt")]);
            app.handle_key(key(KeyCode::Char('a')));
            // Move to the i-th lifecycle.
            for _ in 0..i {
                app.handle_key(key(KeyCode::Char('j')));
            }
            app.handle_key(key(KeyCode::Enter));
            assert_eq!(app.mode, Mode::HookPicker { lifecycle: *lifecycle });
            let outcome = app.handle_key(key(KeyCode::Enter));
            assert_eq!(
                outcome,
                Outcome::AddRequested {
                    lifecycle: *lifecycle,
                    hook_name: "fmt".into(),
                }
            );
        }
    }

    #[test]
    fn hook_picker_esc_falls_back_to_lifecycle_picker() {
        let mut app = app_with(vec![], vec![cand("fmt")]);
        app.handle_key(key(KeyCode::Char('a')));
        app.handle_key(key(KeyCode::Enter));
        assert!(matches!(app.mode, Mode::HookPicker { .. }));
        assert_eq!(app.handle_key(key(KeyCode::Esc)), Outcome::Pending);
        assert_eq!(app.mode, Mode::LifecyclePicker);
    }

    #[test]
    fn candidates_for_filters_already_attached() {
        let app = app_with(
            vec![
                att(Lifecycle::PreStep, "fmt"),
                att(Lifecycle::PostStep, "clippy"),
            ],
            vec![cand("fmt"), cand("clippy"), cand("review")],
        );
        // PreStep: fmt is attached, so candidates are clippy + review.
        let pre = app.candidates_for(Lifecycle::PreStep);
        assert_eq!(pre.len(), 2);
        assert_eq!(pre[0].name, "clippy");
        assert_eq!(pre[1].name, "review");

        // PostStep: clippy is attached, so candidates are fmt + review.
        let post = app.candidates_for(Lifecycle::PostStep);
        assert_eq!(post.len(), 2);
        assert_eq!(post[0].name, "fmt");
        assert_eq!(post[1].name, "review");

        // PreTest / PostTest: nothing attached, all three candidates remain.
        assert_eq!(app.candidates_for(Lifecycle::PreTest).len(), 3);
        assert_eq!(app.candidates_for(Lifecycle::PostTest).len(), 3);
    }

    #[test]
    fn refresh_clamps_cursors_to_new_lengths() {
        let mut app = app_with(
            vec![
                att(Lifecycle::PreStep, "a"),
                att(Lifecycle::PostStep, "b"),
                att(Lifecycle::PostStep, "c"),
            ],
            vec![cand("a"), cand("b"), cand("c"), cand("d")],
        );
        app.list_cursor = 2;
        app.hook_cursor = 3;
        app.refresh(vec![att(Lifecycle::PreStep, "a")], vec![cand("a")]);
        assert_eq!(app.list_cursor, 0);
        assert_eq!(app.hook_cursor, 0);
    }

    #[test]
    fn refresh_with_empty_lists_zeroes_cursors() {
        let mut app = app_with(
            vec![att(Lifecycle::PreStep, "a")],
            vec![cand("a")],
        );
        app.refresh(vec![], vec![]);
        assert_eq!(app.list_cursor, 0);
        assert_eq!(app.hook_cursor, 0);
    }

    #[test]
    fn unknown_key_in_list_is_pending() {
        let mut app = app_with(vec![att(Lifecycle::PreStep, "fmt")], vec![]);
        assert_eq!(app.handle_key(key(KeyCode::Char('x'))), Outcome::Pending);
    }

    // -- Render tests ----------------------------------------------------

    /// Run `render` against an in-memory backend and return the textual
    /// buffer joined with newlines so callers can grep for substrings.
    fn render_to_string(app: &mut StepHooksApp) -> String {
        let backend = TestBackend::new(80, 20);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|f| render(f, f.area(), app))
            .unwrap();
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
    fn render_list_shows_lifecycle_label_for_each_variant() {
        // One attachment per lifecycle proves all four labels are rendered.
        let mut app = StepHooksApp::new(
            "p1".into(),
            "s1".into(),
            "demo".into(),
            "#3 — Build".into(),
            vec![
                att(Lifecycle::PreStep, "fmt"),
                att(Lifecycle::PostStep, "clippy"),
                att(Lifecycle::PreTest, "setup"),
                att(Lifecycle::PostTest, "teardown"),
            ],
            vec![],
        );
        let s = render_to_string(&mut app);
        assert!(s.contains("pre-step"), "missing pre-step in:\n{s}");
        assert!(s.contains("post-step"), "missing post-step in:\n{s}");
        assert!(s.contains("pre-test"), "missing pre-test in:\n{s}");
        assert!(s.contains("post-test"), "missing post-test in:\n{s}");
        assert!(s.contains("fmt"));
        assert!(s.contains("clippy"));
        assert!(s.contains("setup"));
        assert!(s.contains("teardown"));
        assert!(s.contains("demo"), "title should include plan slug");
        assert!(s.contains("#3"), "title should include step label");
    }

    #[test]
    fn render_empty_list_shows_placeholder() {
        let mut app = app_with(vec![], vec![cand("fmt")]);
        let s = render_to_string(&mut app);
        assert!(s.contains("no per-step hooks"), "got:\n{s}");
    }

    #[test]
    fn render_lifecycle_picker_lists_all_four_lifecycles() {
        let mut app = app_with(vec![], vec![cand("fmt")]);
        app.handle_key(key(KeyCode::Char('a')));
        let s = render_to_string(&mut app);
        for lc in LIFECYCLES.iter() {
            assert!(s.contains(lc.as_str()), "missing {} in:\n{s}", lc.as_str());
        }
        assert!(s.contains("Choose lifecycle"));
    }

    #[test]
    fn render_hook_picker_titles_with_chosen_lifecycle() {
        let mut app = app_with(vec![], vec![cand("fmt"), cand("clippy")]);
        app.handle_key(key(KeyCode::Char('a')));
        // Move to PostStep (index 1).
        app.handle_key(key(KeyCode::Char('j')));
        app.handle_key(key(KeyCode::Enter));
        let s = render_to_string(&mut app);
        assert!(s.contains("Attach hook at post-step"), "got:\n{s}");
        assert!(s.contains("fmt"));
        assert!(s.contains("clippy"));
    }

    // -- End-to-end storage round-trip tests -----------------------------

    fn make_plan_and_step(
        conn: &rusqlite::Connection,
        slug: &str,
        project: &str,
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
        let (step, _) = storage::create_step(
            conn, &plan_id, "Step", "", None, None, &[], None, None, None, None,
        )
        .expect("create_step");
        (plan_id, step.id)
    }

    #[test]
    fn end_to_end_add_persists_through_storage_each_lifecycle() {
        // For every lifecycle, drive the state machine to completion and
        // confirm the storage layer accepts the resulting attach call.
        for lifecycle in LIFECYCLES {
            let conn = db::open_memory().unwrap();
            let project = "/proj";
            let (plan_id, step_id) = make_plan_and_step(&conn, "p", project);

            let mut app = StepHooksApp::new(
                plan_id.clone(),
                step_id.clone(),
                "p".into(),
                "#1 — Step".into(),
                vec![],
                vec![cand("fmt")],
            );
            app.handle_key(key(KeyCode::Char('a')));
            // Move to the chosen lifecycle.
            let target_idx = lifecycle_index(lifecycle);
            for _ in 0..target_idx {
                app.handle_key(key(KeyCode::Char('j')));
            }
            app.handle_key(key(KeyCode::Enter));
            let outcome = app.handle_key(key(KeyCode::Enter));
            let (got_lifecycle, got_name) = match outcome {
                Outcome::AddRequested {
                    lifecycle,
                    hook_name,
                } => (lifecycle, hook_name),
                other => panic!("expected AddRequested, got {other:?}"),
            };
            assert_eq!(got_lifecycle, lifecycle);
            assert_eq!(got_name, "fmt");

            storage::attach_hook_to_step(
                &conn,
                &plan_id,
                &step_id,
                got_lifecycle.as_str(),
                &got_name,
            )
            .unwrap();
            let rows = storage::list_all_hooks_for_plan(&conn, &plan_id).unwrap();
            assert_eq!(rows.len(), 1, "{lifecycle:?}");
            assert_eq!(rows[0].lifecycle, lifecycle.as_str());
            assert_eq!(rows[0].hook_name, "fmt");
            assert_eq!(rows[0].step_id.as_deref(), Some(step_id.as_str()));
        }
    }

    #[test]
    fn end_to_end_remove_persists_through_storage_each_lifecycle() {
        for lifecycle in LIFECYCLES {
            let conn = db::open_memory().unwrap();
            let project = "/proj";
            let (plan_id, step_id) = make_plan_and_step(&conn, "p", project);
            storage::attach_hook_to_step(
                &conn,
                &plan_id,
                &step_id,
                lifecycle.as_str(),
                "fmt",
            )
            .unwrap();

            let mut app = StepHooksApp::new(
                plan_id.clone(),
                step_id.clone(),
                "p".into(),
                "#1 — Step".into(),
                vec![att(lifecycle, "fmt")],
                vec![],
            );
            let outcome = app.handle_key(key(KeyCode::Char('d')));
            let (got_lifecycle, got_name) = match outcome {
                Outcome::RemoveRequested {
                    lifecycle,
                    hook_name,
                } => (lifecycle, hook_name),
                other => panic!("expected RemoveRequested, got {other:?}"),
            };
            assert_eq!(got_lifecycle, lifecycle);
            assert_eq!(got_name, "fmt");

            let removed = storage::detach_hook(
                &conn,
                &plan_id,
                Some(&step_id),
                got_lifecycle.as_str(),
                &got_name,
            )
            .unwrap();
            assert_eq!(removed, 1, "{lifecycle:?}");
            let rows = storage::list_all_hooks_for_plan(&conn, &plan_id).unwrap();
            assert!(rows.is_empty(), "{lifecycle:?}");
        }
    }

    #[test]
    fn end_to_end_attach_already_attached_returns_unique_violation() {
        // Defense-in-depth: if the user somehow bypasses the candidate
        // filter (e.g., racy refresh), the storage layer still rejects with
        // a UNIQUE violation that the dispatcher loop can surface as a toast.
        let conn = db::open_memory().unwrap();
        let project = "/proj";
        let (plan_id, step_id) = make_plan_and_step(&conn, "p", project);
        storage::attach_hook_to_step(&conn, &plan_id, &step_id, "pre-step", "fmt").unwrap();

        let err =
            storage::attach_hook_to_step(&conn, &plan_id, &step_id, "pre-step", "fmt")
                .unwrap_err();
        assert!(format!("{err}").contains("already attached"));
    }

    #[test]
    fn end_to_end_remove_does_not_touch_plan_wide_hook() {
        // Sanity check: the per-step detach helper takes `step_id == Some` and
        // therefore must not delete a plan-wide row with the same lifecycle +
        // hook_name. This is the defensive complement to plan_hooks.rs's
        // mirror test.
        let conn = db::open_memory().unwrap();
        let project = "/proj";
        let (plan_id, step_id) = make_plan_and_step(&conn, "p", project);

        // Plan-wide hook.
        storage::attach_hook_to_plan(&conn, &plan_id, "pre-step", "fmt").unwrap();
        // No step-scoped attachment to remove, so detach reports 0 affected.
        let removed =
            storage::detach_hook(&conn, &plan_id, Some(&step_id), "pre-step", "fmt").unwrap();
        assert_eq!(removed, 0);
        // Plan-wide attachment still in the table.
        let rows = storage::list_all_hooks_for_plan(&conn, &plan_id).unwrap();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].step_id.is_none());
    }
}
