// Branch-choice dialog for the `/run` palette command (TUI-plan.md §9.1).
//
// Two-phase state machine:
//   1. `Choosing` — the 3-button choice
//        Use current branch [Enter] | New branch [n] | Cancel [esc]
//   2. `NamingBranch { buffer }` — text-input prompt for a branch name,
//      pre-populated with `default_branch` (the plan's `branch_name` for a
//      single target, or the first selected plan's `branch_name` for
//      multi-select).
//
// The state machine is decoupled from the runner-spawn side effect: callers
// drive `handle_key` until it returns a terminal `Outcome`, then translate
// that outcome into one or more `RunSpawner::spawn_run` calls via
// [`dispatch_outcome`]. The spawner is a trait so unit tests can record
// arguments without forking a real subprocess. A real-spawn smoke test
// covers [`ProcessRunSpawner`] using `/bin/true` as a stand-in binary.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{Context, Result};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::{Constraint, Flex, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph, Wrap};

use super::theme;

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// Where the dialog is in its two-phase flow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DialogState {
    /// Showing the 3-button choice.
    Choosing,
    /// Text-input prompt for a new branch name. `buffer` is the user-edited
    /// string; it starts equal to `default_branch` and the user can edit it
    /// freely (ASCII-only edits — Backspace removes the trailing char).
    NamingBranch { buffer: String },
}

/// Result of one key press.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Key consumed; dialog stays open.
    Pending,
    /// User cancelled (`Esc` or `Ctrl-C` in either phase).
    Cancelled,
    /// User picked "Use current branch" — runs in cwd's currently-checked-out
    /// branch with `--current-branch` for every target.
    Current,
    /// User confirmed a new-branch name. Caller is responsible for switching
    /// (creating if needed) before spawning the runners. For single-plan
    /// callers, an empty string here is treated as "use default" — but the
    /// state machine bounces empty submissions, so this variant always
    /// carries a non-empty trimmed name.
    NewBranch(String),
}

/// Branch-choice dialog state. Construct with `new` and feed key events to
/// [`Self::handle_key`] until it returns a terminal `Outcome`.
pub struct RunDialog {
    pub state: DialogState,
    /// Branch name pre-loaded into the NamingBranch buffer when the user
    /// picks "New branch" without first typing one.
    pub default_branch: String,
    /// Number of plans the user is about to run. Only affects rendering
    /// (the title says "Run 1 plan" vs. "Run N plans"). Multi-plan callers
    /// always force `--current-branch`; that decision lives in
    /// [`dispatch_outcome`], not in the dialog itself.
    pub plan_count: usize,
}

impl RunDialog {
    pub fn new(default_branch: impl Into<String>, plan_count: usize) -> Self {
        Self {
            state: DialogState::Choosing,
            default_branch: default_branch.into(),
            plan_count,
        }
    }

    /// Process one key event.
    pub fn handle_key(&mut self, key: KeyEvent) -> Outcome {
        // Ctrl-C cancels in any state.
        if key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('c'))
        {
            return Outcome::Cancelled;
        }

        // Take the current state out so we can pattern-match on owned data
        // and rebuild the new state without aliasing borrows.
        let state = std::mem::replace(&mut self.state, DialogState::Choosing);
        match state {
            DialogState::Choosing => self.handle_choosing(key),
            DialogState::NamingBranch { buffer } => self.handle_naming(key, buffer),
        }
    }

    fn handle_choosing(&mut self, key: KeyEvent) -> Outcome {
        match key.code {
            KeyCode::Esc => Outcome::Cancelled,
            // Default action: use current branch.
            KeyCode::Enter => Outcome::Current,
            // 'n' / 'N': switch to NamingBranch with the default pre-filled.
            KeyCode::Char('n') | KeyCode::Char('N') => {
                self.state = DialogState::NamingBranch {
                    buffer: self.default_branch.clone(),
                };
                Outcome::Pending
            }
            // Anything else stays in Choosing — preserve state.
            _ => {
                self.state = DialogState::Choosing;
                Outcome::Pending
            }
        }
    }

    fn handle_naming(&mut self, key: KeyEvent, mut buffer: String) -> Outcome {
        match key.code {
            KeyCode::Esc => Outcome::Cancelled,
            KeyCode::Enter => {
                let trimmed = buffer.trim().to_string();
                if trimmed.is_empty() {
                    // Empty submission keeps the user in NamingBranch so they
                    // can fix it. Restore the buffer.
                    self.state = DialogState::NamingBranch { buffer };
                    return Outcome::Pending;
                }
                Outcome::NewBranch(trimmed)
            }
            KeyCode::Backspace => {
                buffer.pop();
                self.state = DialogState::NamingBranch { buffer };
                Outcome::Pending
            }
            KeyCode::Char(c) => {
                if key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                {
                    self.state = DialogState::NamingBranch { buffer };
                    return Outcome::Pending;
                }
                buffer.push(c);
                self.state = DialogState::NamingBranch { buffer };
                Outcome::Pending
            }
            _ => {
                self.state = DialogState::NamingBranch { buffer };
                Outcome::Pending
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Runner spawn abstraction
// ---------------------------------------------------------------------------

/// One target plan to run. The `default_branch` is the plan's
/// `branch_name` — used by [`dispatch_outcome`] to decide whether the user's
/// `NewBranch(name)` matches the plan's natural branch (in which case the
/// runner can do its own switch via no `--current-branch` flag) or differs
/// (in which case the caller must pre-switch and the runner uses
/// `--current-branch`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunTarget {
    pub slug: String,
    pub default_branch: String,
}

/// Build the `Command` that would launch a runner subprocess. Pure (no
/// spawn), so tests can inspect the args without forking a real process.
///
/// The shape mirrors the existing `R`-keybinding spawner in
/// `commands::run::plan_detail_apply_run` (TUI-plan.md §7) plus a flag for
/// `--current-branch`.
pub fn build_run_command(
    exe: &Path,
    project: &Path,
    slug: &str,
    current_branch: bool,
) -> Command {
    let mut cmd = Command::new(exe);
    cmd.arg("-C").arg(project).arg("--non-interactive").arg("run");
    if current_branch {
        cmd.arg("--current-branch");
    }
    cmd.arg(slug);
    // Dropping the runner's stdio prevents its output from corrupting the
    // TUI's alternate-screen render. Step 37 of tui-v1 swaps this for an
    // NDJSON pipe so the TUI can stream events.
    cmd.stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    cmd
}

/// Abstraction over "spawn a runner subprocess for plan X". The dispatcher
/// holds a `&mut dyn RunSpawner`; tests inject [`StubRunSpawner`] to verify
/// arg ordering without forking.
pub trait RunSpawner {
    fn spawn_run(
        &mut self,
        project: &Path,
        slug: &str,
        current_branch: bool,
    ) -> Result<()>;
}

/// Production spawner — forks an actual `ralph` subprocess.
pub struct ProcessRunSpawner {
    /// Path to the `ralph` binary to fork. Defaults to
    /// `std::env::current_exe()`.
    pub exe: PathBuf,
}

impl ProcessRunSpawner {
    /// Construct a spawner that forks the currently-running binary
    /// (`std::env::current_exe`). Returns an error if the OS doesn't expose
    /// the current-exe path (e.g. some sandbox environments).
    pub fn new() -> Result<Self> {
        let exe = std::env::current_exe().context("locate ralph binary")?;
        Ok(Self { exe })
    }
}

impl RunSpawner for ProcessRunSpawner {
    fn spawn_run(
        &mut self,
        project: &Path,
        slug: &str,
        current_branch: bool,
    ) -> Result<()> {
        let mut cmd = build_run_command(&self.exe, project, slug, current_branch);
        cmd.spawn()
            .with_context(|| format!("spawn ralph run for plan {slug}"))?;
        Ok(())
    }
}

/// Dispatch a terminal `Outcome` against a list of targets. Issues one
/// `spawn_run` call per target and returns the slugs that were spawned in
/// order so the caller can decide which plan-detail view to push first.
///
/// `force_current_branch` is set by multi-select callers — per TUI-plan.md
/// §9.1, multi-plan runs always pass `--current-branch` regardless of which
/// branch button the user pressed (the caller pre-switches branches before
/// invoking this for `NewBranch`).
///
/// For single-plan callers (`force_current_branch = false`), the
/// `NewBranch(name)` case passes `--current-branch` only when `name` differs
/// from the plan's `default_branch`. When they match, the runner does its
/// own branch switch — matching the §9.1 paragraph "spawn runner without
/// --current-branch (it switches to plan.branch_name)".
pub fn dispatch_outcome(
    outcome: &Outcome,
    targets: &[RunTarget],
    project: &Path,
    spawner: &mut dyn RunSpawner,
    force_current_branch: bool,
) -> Result<Vec<String>> {
    match outcome {
        Outcome::Pending | Outcome::Cancelled => Ok(Vec::new()),
        Outcome::Current => {
            let mut spawned = Vec::with_capacity(targets.len());
            for target in targets {
                spawner.spawn_run(project, &target.slug, true)?;
                spawned.push(target.slug.clone());
            }
            Ok(spawned)
        }
        Outcome::NewBranch(name) => {
            let mut spawned = Vec::with_capacity(targets.len());
            for target in targets {
                let use_current = force_current_branch || name != &target.default_branch;
                spawner.spawn_run(project, &target.slug, use_current)?;
                spawned.push(target.slug.clone());
            }
            Ok(spawned)
        }
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Draw the dialog as a centered overlay over `area`. The caller is
/// expected to render the background view first; `Clear` blanks just the
/// dialog rectangle.
pub fn render(frame: &mut Frame, area: Rect, dialog: &RunDialog) {
    let title = if dialog.plan_count <= 1 {
        " Run plan ".to_string()
    } else {
        format!(" Run {} plans ", dialog.plan_count)
    };
    match &dialog.state {
        DialogState::Choosing => render_choosing(frame, area, &title),
        DialogState::NamingBranch { buffer } => render_naming(frame, area, &title, buffer),
    }
}

fn render_choosing(frame: &mut Frame, area: Rect, title: &str) {
    let body = "Use current branch  [Enter]\nNew branch          [n]\nCancel              [Esc]";
    draw_box(frame, area, title, body);
}

fn render_naming(frame: &mut Frame, area: Rect, title: &str, buffer: &str) {
    let body = format!(
        "Branch name:\n  {buffer}\n\n[Enter] confirm   [Esc] cancel"
    );
    draw_box(frame, area, title, &body);
}

fn draw_box(frame: &mut Frame, area: Rect, title: &str, body: &str) {
    let lines: Vec<Line> = body.lines().map(Line::from).collect();
    let body_lines = lines.len().max(1) as u16;
    let max_w = body
        .lines()
        .map(|l| l.chars().count())
        .max()
        .unwrap_or(0)
        .max(title.chars().count())
        .max(20) as u16;
    let height = (body_lines + 2).min(area.height).max(3.min(area.height));
    let width = (max_w + 4).min(area.width).max(20.min(area.width));

    let [vert] = Layout::vertical([Constraint::Length(height)])
        .flex(Flex::Center)
        .areas(area);
    let [horiz] = Layout::horizontal([Constraint::Length(width)])
        .flex(Flex::Center)
        .areas(vert);

    frame.render_widget(Clear, horiz);
    let block = Block::default()
        .title(Span::styled(
            title.to_string(),
            Style::default().add_modifier(Modifier::BOLD),
        ))
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme::CURSOR));
    let para = Paragraph::new(lines).block(block).wrap(Wrap { trim: false });
    frame.render_widget(para, horiz);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyEvent, KeyModifiers};
    use std::cell::RefCell;
    use std::path::PathBuf;

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_with(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    // -- State machine ----------------------------------------------------

    #[test]
    fn enter_picks_current_branch() {
        let mut d = RunDialog::new("tui-v1", 1);
        let out = d.handle_key(key(KeyCode::Enter));
        assert_eq!(out, Outcome::Current);
    }

    #[test]
    fn esc_cancels_in_choosing() {
        let mut d = RunDialog::new("tui-v1", 1);
        let out = d.handle_key(key(KeyCode::Esc));
        assert_eq!(out, Outcome::Cancelled);
    }

    #[test]
    fn ctrl_c_cancels_in_choosing() {
        let mut d = RunDialog::new("tui-v1", 1);
        let out = d.handle_key(key_with(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert_eq!(out, Outcome::Cancelled);
    }

    #[test]
    fn n_enters_naming_branch_with_default() {
        let mut d = RunDialog::new("tui-v1", 1);
        let out = d.handle_key(key(KeyCode::Char('n')));
        assert_eq!(out, Outcome::Pending);
        match &d.state {
            DialogState::NamingBranch { buffer } => assert_eq!(buffer, "tui-v1"),
            other => panic!("unexpected state {other:?}"),
        }
    }

    #[test]
    fn capital_n_also_enters_naming_branch() {
        let mut d = RunDialog::new("main", 2);
        let out = d.handle_key(key(KeyCode::Char('N')));
        assert_eq!(out, Outcome::Pending);
        assert!(matches!(d.state, DialogState::NamingBranch { .. }));
    }

    #[test]
    fn unrecognized_key_in_choosing_is_pending() {
        let mut d = RunDialog::new("main", 1);
        let out = d.handle_key(key(KeyCode::Char('x')));
        assert_eq!(out, Outcome::Pending);
        assert_eq!(d.state, DialogState::Choosing);
    }

    #[test]
    fn naming_backspace_removes_last_char() {
        let mut d = RunDialog::new("main", 1);
        d.handle_key(key(KeyCode::Char('n'))); // → NamingBranch { "main" }
        d.handle_key(key(KeyCode::Backspace));
        match &d.state {
            DialogState::NamingBranch { buffer } => assert_eq!(buffer, "mai"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn naming_typing_appends_to_buffer() {
        let mut d = RunDialog::new("main", 1);
        d.handle_key(key(KeyCode::Char('n'))); // → NamingBranch { "main" }
        d.handle_key(key(KeyCode::Char('-')));
        d.handle_key(key(KeyCode::Char('x')));
        match &d.state {
            DialogState::NamingBranch { buffer } => assert_eq!(buffer, "main-x"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn naming_ctrl_modified_chars_are_ignored() {
        let mut d = RunDialog::new("main", 1);
        d.handle_key(key(KeyCode::Char('n')));
        // Ctrl-A would otherwise insert literal 'a'.
        d.handle_key(key_with(KeyCode::Char('a'), KeyModifiers::CONTROL));
        match &d.state {
            DialogState::NamingBranch { buffer } => assert_eq!(buffer, "main"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn naming_enter_emits_new_branch() {
        let mut d = RunDialog::new("main", 1);
        d.handle_key(key(KeyCode::Char('n')));
        let out = d.handle_key(key(KeyCode::Enter));
        assert_eq!(out, Outcome::NewBranch("main".to_string()));
    }

    #[test]
    fn naming_esc_cancels() {
        let mut d = RunDialog::new("main", 1);
        d.handle_key(key(KeyCode::Char('n')));
        let out = d.handle_key(key(KeyCode::Esc));
        assert_eq!(out, Outcome::Cancelled);
    }

    #[test]
    fn naming_ctrl_c_cancels() {
        let mut d = RunDialog::new("main", 1);
        d.handle_key(key(KeyCode::Char('n')));
        let out = d.handle_key(key_with(KeyCode::Char('c'), KeyModifiers::CONTROL));
        assert_eq!(out, Outcome::Cancelled);
    }

    #[test]
    fn naming_empty_enter_is_pending_and_preserves_buffer() {
        let mut d = RunDialog::new("", 1);
        d.handle_key(key(KeyCode::Char('n'))); // → NamingBranch { "" }
        let out = d.handle_key(key(KeyCode::Enter));
        assert_eq!(out, Outcome::Pending);
        assert_eq!(d.state, DialogState::NamingBranch { buffer: String::new() });
    }

    #[test]
    fn naming_whitespace_only_enter_is_pending() {
        // The state machine bounces "    " just like ""; the spec wants a
        // real branch name.
        let mut d = RunDialog::new("   ", 1);
        d.handle_key(key(KeyCode::Char('n')));
        let out = d.handle_key(key(KeyCode::Enter));
        assert_eq!(out, Outcome::Pending);
    }

    #[test]
    fn user_can_edit_default_branch_then_submit() {
        // Realistic flow: open dialog with default "feature-x", clear the
        // buffer, type "hotfix", press Enter.
        let mut d = RunDialog::new("feature-x", 1);
        assert_eq!(d.handle_key(key(KeyCode::Char('n'))), Outcome::Pending);
        for _ in 0..10 {
            d.handle_key(key(KeyCode::Backspace));
        }
        for c in "hotfix".chars() {
            d.handle_key(key(KeyCode::Char(c)));
        }
        let out = d.handle_key(key(KeyCode::Enter));
        assert_eq!(out, Outcome::NewBranch("hotfix".to_string()));
    }

    // -- Spawner abstraction ---------------------------------------------

    /// Records every spawn_run call so tests can verify ordering / args.
    struct StubRunSpawner {
        calls: RefCell<Vec<(PathBuf, String, bool)>>,
    }

    impl StubRunSpawner {
        fn new() -> Self {
            Self {
                calls: RefCell::new(Vec::new()),
            }
        }
    }

    impl RunSpawner for StubRunSpawner {
        fn spawn_run(
            &mut self,
            project: &Path,
            slug: &str,
            current_branch: bool,
        ) -> Result<()> {
            self.calls
                .borrow_mut()
                .push((project.to_path_buf(), slug.to_string(), current_branch));
            Ok(())
        }
    }

    fn target(slug: &str, branch: &str) -> RunTarget {
        RunTarget {
            slug: slug.to_string(),
            default_branch: branch.to_string(),
        }
    }

    #[test]
    fn dispatch_pending_or_cancelled_spawns_nothing() {
        let mut s = StubRunSpawner::new();
        let project = PathBuf::from("/proj");
        let targets = vec![target("a", "b")];

        let _ = dispatch_outcome(&Outcome::Pending, &targets, &project, &mut s, false).unwrap();
        let _ = dispatch_outcome(&Outcome::Cancelled, &targets, &project, &mut s, false).unwrap();
        assert!(s.calls.borrow().is_empty());
    }

    #[test]
    fn dispatch_current_passes_current_branch_for_each_target() {
        let mut s = StubRunSpawner::new();
        let project = PathBuf::from("/proj");
        let targets = vec![target("alpha", "main"), target("beta", "develop")];

        let spawned =
            dispatch_outcome(&Outcome::Current, &targets, &project, &mut s, false).unwrap();
        assert_eq!(spawned, vec!["alpha".to_string(), "beta".to_string()]);
        let calls = s.calls.borrow();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0], (PathBuf::from("/proj"), "alpha".to_string(), true));
        assert_eq!(calls[1], (PathBuf::from("/proj"), "beta".to_string(), true));
    }

    #[test]
    fn dispatch_new_branch_default_omits_current_branch_for_single_plan() {
        // Single plan: NewBranch(name) where name == default_branch ⇒ runner
        // does its own branch switch (no --current-branch).
        let mut s = StubRunSpawner::new();
        let project = PathBuf::from("/proj");
        let targets = vec![target("alpha", "feature-x")];

        let spawned = dispatch_outcome(
            &Outcome::NewBranch("feature-x".to_string()),
            &targets,
            &project,
            &mut s,
            false,
        )
        .unwrap();
        assert_eq!(spawned, vec!["alpha".to_string()]);
        let calls = s.calls.borrow();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], (PathBuf::from("/proj"), "alpha".to_string(), false));
    }

    #[test]
    fn dispatch_new_branch_custom_uses_current_branch_for_single_plan() {
        // Single plan: NewBranch(name) where name != default_branch ⇒ caller
        // pre-switched, so runner uses --current-branch.
        let mut s = StubRunSpawner::new();
        let project = PathBuf::from("/proj");
        let targets = vec![target("alpha", "feature-x")];

        let spawned = dispatch_outcome(
            &Outcome::NewBranch("hotfix".to_string()),
            &targets,
            &project,
            &mut s,
            false,
        )
        .unwrap();
        assert_eq!(spawned, vec!["alpha".to_string()]);
        let calls = s.calls.borrow();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0], (PathBuf::from("/proj"), "alpha".to_string(), true));
    }

    #[test]
    fn dispatch_multi_plan_forces_current_branch_even_on_new_branch() {
        // Multi-plan: --current-branch is forced regardless of the user's
        // pick. Spec §9.1.
        let mut s = StubRunSpawner::new();
        let project = PathBuf::from("/proj");
        let targets = vec![
            target("alpha", "feature-a"),
            target("beta", "feature-b"),
            target("gamma", "feature-c"),
        ];

        let spawned = dispatch_outcome(
            &Outcome::NewBranch("integration".to_string()),
            &targets,
            &project,
            &mut s,
            true,
        )
        .unwrap();
        assert_eq!(
            spawned,
            vec!["alpha".to_string(), "beta".to_string(), "gamma".to_string()]
        );
        let calls = s.calls.borrow();
        assert!(
            calls.iter().all(|(_, _, cb)| *cb),
            "every spawn must pass --current-branch in multi-plan mode: {:?}",
            *calls
        );
    }

    #[test]
    fn dispatch_multi_plan_current_outcome_passes_current_branch() {
        // Sanity: multi-plan + Outcome::Current also passes --current-branch.
        let mut s = StubRunSpawner::new();
        let project = PathBuf::from("/proj");
        let targets = vec![target("a", "b1"), target("c", "b2")];
        dispatch_outcome(&Outcome::Current, &targets, &project, &mut s, true).unwrap();
        assert!(s.calls.borrow().iter().all(|(_, _, cb)| *cb));
    }

    // -- build_run_command -----------------------------------------------

    fn cmd_args(cmd: &Command) -> Vec<String> {
        cmd.get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect()
    }

    #[test]
    fn build_run_command_with_current_branch() {
        let cmd = build_run_command(
            Path::new("/usr/bin/ralph"),
            Path::new("/some/proj"),
            "my-plan",
            true,
        );
        assert_eq!(cmd.get_program(), "/usr/bin/ralph");
        assert_eq!(
            cmd_args(&cmd),
            vec![
                "-C".to_string(),
                "/some/proj".to_string(),
                "--non-interactive".to_string(),
                "run".to_string(),
                "--current-branch".to_string(),
                "my-plan".to_string(),
            ]
        );
    }

    #[test]
    fn build_run_command_without_current_branch() {
        let cmd = build_run_command(
            Path::new("/usr/bin/ralph"),
            Path::new("/proj"),
            "alpha",
            false,
        );
        assert_eq!(
            cmd_args(&cmd),
            vec![
                "-C".to_string(),
                "/proj".to_string(),
                "--non-interactive".to_string(),
                "run".to_string(),
                "alpha".to_string(),
            ]
        );
    }

    // -- ProcessRunSpawner real-spawn smoke test -------------------------
    //
    // Verifies the end-to-end fork path on Unix using `/bin/true` as a
    // stand-in `ralph` binary. The key check: spawn_run returns Ok and the
    // child process is reaped without panicking. We don't assert on `ralph
    // run` semantics — that's covered by the runner's own tests.

    #[cfg(unix)]
    #[test]
    fn process_run_spawner_actually_spawns() {
        let mut spawner = ProcessRunSpawner {
            exe: PathBuf::from("/bin/true"),
        };
        let result = spawner.spawn_run(Path::new("/tmp"), "any-slug", true);
        assert!(
            result.is_ok(),
            "spawn_run failed: {:?}",
            result.err().map(|e| e.to_string())
        );
        // Give the child a moment to exit; we don't have a handle to wait
        // on it explicitly. /bin/true exits immediately so the OS will reap
        // it in short order.
    }

    #[cfg(unix)]
    #[test]
    fn process_run_spawner_surfaces_spawn_error_for_missing_binary() {
        let mut spawner = ProcessRunSpawner {
            exe: PathBuf::from("/nonexistent/path/to/ralph-doesnt-exist-xyzzy"),
        };
        let result = spawner.spawn_run(Path::new("/tmp"), "slug", false);
        assert!(result.is_err(), "expected spawn to fail for missing binary");
    }

    // -- Render smoke ----------------------------------------------------

    #[test]
    fn render_choosing_writes_three_buttons() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(60, 12);
        let mut terminal = Terminal::new(backend).unwrap();
        let dialog = RunDialog::new("tui-v1", 1);
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
        assert!(dump.contains("Run plan"), "title missing:\n{dump}");
        assert!(dump.contains("Use current branch"), "current button:\n{dump}");
        assert!(dump.contains("New branch"), "new button:\n{dump}");
        assert!(dump.contains("Cancel"), "cancel button:\n{dump}");
    }

    #[test]
    fn render_naming_writes_buffer() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(60, 12);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut dialog = RunDialog::new("default-branch", 1);
        dialog.state = DialogState::NamingBranch {
            buffer: "feature/x".to_string(),
        };
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
        assert!(dump.contains("Branch name"), "label missing:\n{dump}");
        assert!(dump.contains("feature/x"), "buffer missing:\n{dump}");
    }

    #[test]
    fn render_uses_plural_title_for_multi_plan() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(60, 12);
        let mut terminal = Terminal::new(backend).unwrap();
        let dialog = RunDialog::new("main", 3);
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
        assert!(dump.contains("Run 3 plans"), "plural title missing:\n{dump}");
    }
}
