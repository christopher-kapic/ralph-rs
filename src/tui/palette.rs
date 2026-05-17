// Slash/colon command palette (TUI-plan.md §9).
//
// Both `:` and `/` open a single-line input bar at the bottom of the screen
// and submit through the same parser. This module is the parser, the input-
// bar state machine, the tab-completion engine, and the renderer. Wiring the
// recognized commands to real behavior lands in steps 29–31 of the tui-v1
// plan; for now any successful parse is reported back to the caller and
// dispatchers toast "<verb> not yet implemented".

use std::path::Path;
use std::process::Command;

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, Paragraph};

use super::theme;

// ---------------------------------------------------------------------------
// Recognized commands
// ---------------------------------------------------------------------------

/// One palette command per row of TUI-plan.md §9. Optional `Option<…>` slots
/// hold arguments that are optional in the grammar (`/plan archive [<slug>]`,
/// for instance).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaletteCommand {
    /// `/run [<branch>]`. `Some(branch)` means run on `<branch>` with
    /// `--current-branch`; `None` opens the branch-choice dialog described in
    /// §9.1.
    Run(Option<String>),
    /// `/plan harness [<name>]`.
    PlanHarness(Option<String>),
    /// `/plan show [<slug>]`.
    PlanShow(Option<String>),
    /// `/plan archive [<slug>]`.
    PlanArchive(Option<String>),
    /// `/plan unarchive <slug>`.
    PlanUnarchive(String),
    /// `/plan delete <slug>`.
    PlanDelete(String),
    /// `/plan approve [<slug>]`.
    PlanApprove(Option<String>),
    /// `/plan questions on [<slug>]`.
    PlanQuestionsOn(Option<String>),
    /// `/plan questions off [<slug>]`.
    PlanQuestionsOff(Option<String>),
    /// `/plan dependency add` — routes to the plan-dependency sub-view.
    PlanDependencyAdd,
    /// `/plan dependency remove` — routes to the plan-dependency sub-view.
    PlanDependencyRemove,
    /// `/plan dependency list` — routes to the plan-dependency sub-view.
    PlanDependencyList,
    /// `/plan set-hook` — routes to the plan-hook sub-view.
    PlanSetHook,
    /// `/plan unset-hook` — routes to the plan-hook sub-view.
    PlanUnsetHook,
    /// `/plan hooks` — routes to the plan-hook sub-view.
    PlanHooks,
    /// `/step add <title>`.
    StepAdd(String),
    /// `/step skip [<num>]`.
    StepSkip(Option<u32>),
    /// `/step move <num> --to <m>`.
    StepMove { num: u32, to: u32 },
    /// `/step set-hook` — routes to the step-hook sub-view.
    StepSetHook,
    /// `/step unset-hook` — routes to the step-hook sub-view.
    StepUnsetHook,
    /// `/step edit --tags` — routes to the step-tag sub-view.
    StepEditTags,
    /// `/cancel`.
    Cancel,
    /// `/export <slug> [-o <path>]`. `output` is `None` when the user
    /// omitted `-o`; the dispatcher falls back to `<slug>.ralph.json` in
    /// the cwd.
    Export {
        slug: String,
        output: Option<String>,
    },
    /// `/import <path>`.
    Import(String),
    /// `/quit` or `/q`.
    Quit,
    /// `/help`.
    Help,
    /// `/inbox` — open the cross-branch interruptions inbox
    /// (docs/dag-redesign.md §12.3).
    Inbox,
    /// `/focus <short_id>` — re-root the plan-detail outline on a step's
    /// downstream-dependents cone (docs/dag-redesign.md §12.2). `None`
    /// focuses the cursor's step; `Some(id)` focuses an explicit short id.
    Focus(Option<String>),
}

impl PaletteCommand {
    /// Canonical user-facing label (with leading slash). Used by the
    /// dispatcher's "<verb> not yet implemented" toast and by the help
    /// overlay (§44) once it lands.
    pub fn label(&self) -> &'static str {
        match self {
            PaletteCommand::Run(_) => "/run",
            PaletteCommand::PlanHarness(_) => "/plan harness",
            PaletteCommand::PlanShow(_) => "/plan show",
            PaletteCommand::PlanArchive(_) => "/plan archive",
            PaletteCommand::PlanUnarchive(_) => "/plan unarchive",
            PaletteCommand::PlanDelete(_) => "/plan delete",
            PaletteCommand::PlanApprove(_) => "/plan approve",
            PaletteCommand::PlanQuestionsOn(_) => "/plan questions on",
            PaletteCommand::PlanQuestionsOff(_) => "/plan questions off",
            PaletteCommand::PlanDependencyAdd => "/plan dependency add",
            PaletteCommand::PlanDependencyRemove => "/plan dependency remove",
            PaletteCommand::PlanDependencyList => "/plan dependency list",
            PaletteCommand::PlanSetHook => "/plan set-hook",
            PaletteCommand::PlanUnsetHook => "/plan unset-hook",
            PaletteCommand::PlanHooks => "/plan hooks",
            PaletteCommand::StepAdd(_) => "/step add",
            PaletteCommand::StepSkip(_) => "/step skip",
            PaletteCommand::StepMove { .. } => "/step move",
            PaletteCommand::StepSetHook => "/step set-hook",
            PaletteCommand::StepUnsetHook => "/step unset-hook",
            PaletteCommand::StepEditTags => "/step edit --tags",
            PaletteCommand::Cancel => "/cancel",
            PaletteCommand::Export { .. } => "/export",
            PaletteCommand::Import(_) => "/import",
            PaletteCommand::Quit => "/quit",
            PaletteCommand::Help => "/help",
            PaletteCommand::Inbox => "/inbox",
            PaletteCommand::Focus(_) => "/focus",
        }
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

/// Why a palette input failed to parse.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// Input was empty or only whitespace / a bare `/`. The dispatcher
    /// should treat this as a silent close.
    Empty,
    /// First token (or the full token path) didn't match any known verb.
    /// Carries the raw user input minus the optional leading slash so the
    /// dispatcher can render `Unknown command: <verb>`.
    Unknown(String),
    /// The matched verb requires an argument that wasn't supplied.
    MissingArgument {
        command: &'static str,
        arg: &'static str,
    },
    /// The matched verb's argument was supplied but couldn't be parsed.
    InvalidArgument {
        command: &'static str,
        arg: String,
        reason: &'static str,
    },
}

/// Parse a palette input line into a [`PaletteCommand`].
///
/// The leading `/` is optional; both `:run` (typed after the prefix key) and
/// `:/run` resolve identically. Whitespace is collapsed, so `  /plan   show `
/// parses the same as `/plan show`.
pub fn parse(input: &str) -> Result<PaletteCommand, ParseError> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        return Err(ParseError::Empty);
    }
    let body = trimmed.strip_prefix('/').unwrap_or(trimmed).trim();
    if body.is_empty() {
        return Err(ParseError::Empty);
    }
    let tokens: Vec<&str> = body.split_whitespace().collect();

    match tokens.as_slice() {
        // /run [<branch>]
        ["run"] => Ok(PaletteCommand::Run(None)),
        ["run", branch] => Ok(PaletteCommand::Run(Some((*branch).to_string()))),

        // /plan harness [<name>]
        ["plan", "harness"] => Ok(PaletteCommand::PlanHarness(None)),
        ["plan", "harness", name] => Ok(PaletteCommand::PlanHarness(Some((*name).to_string()))),

        // /plan show [<slug>]
        ["plan", "show"] => Ok(PaletteCommand::PlanShow(None)),
        ["plan", "show", slug] => Ok(PaletteCommand::PlanShow(Some((*slug).to_string()))),

        // /plan archive [<slug>]
        ["plan", "archive"] => Ok(PaletteCommand::PlanArchive(None)),
        ["plan", "archive", slug] => Ok(PaletteCommand::PlanArchive(Some((*slug).to_string()))),

        // /plan unarchive <slug>
        ["plan", "unarchive"] => Err(ParseError::MissingArgument {
            command: "/plan unarchive",
            arg: "<slug>",
        }),
        ["plan", "unarchive", slug] => Ok(PaletteCommand::PlanUnarchive((*slug).to_string())),

        // /plan delete <slug>
        ["plan", "delete"] => Err(ParseError::MissingArgument {
            command: "/plan delete",
            arg: "<slug>",
        }),
        ["plan", "delete", slug] => Ok(PaletteCommand::PlanDelete((*slug).to_string())),

        // /plan approve [<slug>]
        ["plan", "approve"] => Ok(PaletteCommand::PlanApprove(None)),
        ["plan", "approve", slug] => Ok(PaletteCommand::PlanApprove(Some((*slug).to_string()))),

        // /plan questions on|off [<slug>]
        ["plan", "questions", "on"] => Ok(PaletteCommand::PlanQuestionsOn(None)),
        ["plan", "questions", "on", slug] => {
            Ok(PaletteCommand::PlanQuestionsOn(Some((*slug).to_string())))
        }
        ["plan", "questions", "off"] => Ok(PaletteCommand::PlanQuestionsOff(None)),
        ["plan", "questions", "off", slug] => {
            Ok(PaletteCommand::PlanQuestionsOff(Some((*slug).to_string())))
        }

        // /plan dependency add|remove|list
        ["plan", "dependency", "add"] => Ok(PaletteCommand::PlanDependencyAdd),
        ["plan", "dependency", "remove"] => Ok(PaletteCommand::PlanDependencyRemove),
        ["plan", "dependency", "list"] => Ok(PaletteCommand::PlanDependencyList),

        // /plan set-hook|unset-hook|hooks
        ["plan", "set-hook"] => Ok(PaletteCommand::PlanSetHook),
        ["plan", "unset-hook"] => Ok(PaletteCommand::PlanUnsetHook),
        ["plan", "hooks"] => Ok(PaletteCommand::PlanHooks),

        // /step add <title>
        ["step", "add"] => Err(ParseError::MissingArgument {
            command: "/step add",
            arg: "<title>",
        }),
        ["step", "add", ..] => {
            // Preserve multi-word titles by re-joining whitespace-collapsed
            // tokens. Quoted-string handling is out of scope for v1.
            let title = tokens[2..].join(" ");
            Ok(PaletteCommand::StepAdd(title))
        }

        // /step skip [<num>]
        ["step", "skip"] => Ok(PaletteCommand::StepSkip(None)),
        ["step", "skip", num] => {
            let n: u32 = num.parse().map_err(|_| ParseError::InvalidArgument {
                command: "/step skip",
                arg: (*num).to_string(),
                reason: "expected step number",
            })?;
            Ok(PaletteCommand::StepSkip(Some(n)))
        }

        // /step move <num> --to <m>
        ["step", "move", num, "--to", to] => {
            let n: u32 = num.parse().map_err(|_| ParseError::InvalidArgument {
                command: "/step move",
                arg: (*num).to_string(),
                reason: "expected step number",
            })?;
            let m: u32 = to.parse().map_err(|_| ParseError::InvalidArgument {
                command: "/step move",
                arg: (*to).to_string(),
                reason: "expected target position",
            })?;
            Ok(PaletteCommand::StepMove { num: n, to: m })
        }

        // /step set-hook|unset-hook
        ["step", "set-hook"] => Ok(PaletteCommand::StepSetHook),
        ["step", "unset-hook"] => Ok(PaletteCommand::StepUnsetHook),

        // /step edit --tags
        ["step", "edit", "--tags"] => Ok(PaletteCommand::StepEditTags),

        // /cancel
        ["cancel"] => Ok(PaletteCommand::Cancel),

        // /export <slug> [-o <path>]
        ["export"] => Err(ParseError::MissingArgument {
            command: "/export",
            arg: "<slug>",
        }),
        ["export", slug] => Ok(PaletteCommand::Export {
            slug: (*slug).to_string(),
            output: None,
        }),
        ["export", slug, "-o", path] => Ok(PaletteCommand::Export {
            slug: (*slug).to_string(),
            output: Some((*path).to_string()),
        }),

        // /import <path>
        ["import"] => Err(ParseError::MissingArgument {
            command: "/import",
            arg: "<path>",
        }),
        ["import", path] => Ok(PaletteCommand::Import((*path).to_string())),

        // /quit and its alias /q
        ["quit"] | ["q"] => Ok(PaletteCommand::Quit),

        // /help
        ["help"] => Ok(PaletteCommand::Help),

        // /inbox — cross-branch interruptions inbox (§12.3)
        ["inbox"] => Ok(PaletteCommand::Inbox),

        // /focus [<short_id>] — re-root the outline (§12.2)
        ["focus"] => Ok(PaletteCommand::Focus(None)),
        ["focus", short_id] => Ok(PaletteCommand::Focus(Some((*short_id).to_string()))),

        _ => Err(ParseError::Unknown(body.to_string())),
    }
}

// ---------------------------------------------------------------------------
// Tab completion
// ---------------------------------------------------------------------------

/// Where a verb's positional argument values come from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ArgSource {
    /// Free-form text or no completable arguments — tab proposes nothing
    /// once the verb is fully typed.
    None,
    /// Harness names from `Config.harnesses`.
    Harness,
    /// Plan slugs from `storage::list_plans` (filtered by current project).
    PlanSlug,
    /// Branch names from `git branch --list`.
    Branch,
}

/// One row in the static verb table that drives both completion and the
/// help overlay. The order here is the order tab cycles through verbs that
/// share a common prefix (e.g. `plan h` cycles to `plan harness` then
/// `plan hooks`).
struct VerbSpec {
    /// Whitespace-separated tokens that make up this verb (sans leading
    /// slash). E.g. `&["plan", "harness"]`.
    tokens: &'static [&'static str],
    /// Source for the next argument after the verb's tokens. `None` means
    /// the verb either takes no args or its args aren't enumerable.
    arg: ArgSource,
}

/// Master list of completable verbs. Order is significant for tab cycling:
/// candidates are returned in the order they appear here when multiple
/// match the user's prefix.
///
/// `/q` is intentionally absent — `quit` is the canonical label and
/// completion shouldn't surface both. The parser still accepts `/q`.
const VERB_SPECS: &[VerbSpec] = &[
    VerbSpec {
        tokens: &["run"],
        arg: ArgSource::Branch,
    },
    VerbSpec {
        tokens: &["plan", "harness"],
        arg: ArgSource::Harness,
    },
    VerbSpec {
        tokens: &["plan", "show"],
        arg: ArgSource::PlanSlug,
    },
    VerbSpec {
        tokens: &["plan", "archive"],
        arg: ArgSource::PlanSlug,
    },
    VerbSpec {
        tokens: &["plan", "unarchive"],
        arg: ArgSource::PlanSlug,
    },
    VerbSpec {
        tokens: &["plan", "delete"],
        arg: ArgSource::PlanSlug,
    },
    VerbSpec {
        tokens: &["plan", "approve"],
        arg: ArgSource::PlanSlug,
    },
    VerbSpec {
        tokens: &["plan", "questions", "on"],
        arg: ArgSource::PlanSlug,
    },
    VerbSpec {
        tokens: &["plan", "questions", "off"],
        arg: ArgSource::PlanSlug,
    },
    VerbSpec {
        tokens: &["plan", "dependency", "add"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["plan", "dependency", "remove"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["plan", "dependency", "list"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["plan", "set-hook"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["plan", "unset-hook"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["plan", "hooks"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["step", "add"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["step", "skip"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["step", "move"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["step", "set-hook"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["step", "unset-hook"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["step", "edit", "--tags"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["cancel"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["export"],
        arg: ArgSource::PlanSlug,
    },
    VerbSpec {
        tokens: &["import"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["quit"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["help"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["inbox"],
        arg: ArgSource::None,
    },
    VerbSpec {
        tokens: &["focus"],
        arg: ArgSource::None,
    },
];

/// Argument-source data the completer reads from. The dispatcher refreshes
/// this on demand (`harnesses` from the loaded `Config`, `plan_slugs` from
/// `storage::list_plans`, `branches` from `list_git_branches`).
#[derive(Debug, Clone, Default)]
pub struct CompletionContext {
    pub harnesses: Vec<String>,
    pub plan_slugs: Vec<String>,
    pub branches: Vec<String>,
}

/// A computed tab-completion cycle. `stem` is the buffer text preserved
/// across cycle steps; `candidates` are appended one at a time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Completion {
    pub stem: String,
    pub candidates: Vec<String>,
}

/// Compute completion candidates for `buffer`. Returns `None` when nothing
/// matches (no candidates means tab is a no-op rather than an error).
pub fn build_completion(buffer: &str, ctx: &CompletionContext) -> Option<Completion> {
    let (slash, body) = match buffer.strip_prefix('/') {
        Some(rest) => ("/", rest),
        None => ("", buffer),
    };
    let trailing_ws = body.ends_with(|c: char| c.is_whitespace());
    let tokens: Vec<&str> = body.split_whitespace().collect();

    // Try to find the longest verb spec whose tokens fully match a prefix
    // of `tokens`. A "full match" requires either trailing whitespace
    // (the user just typed a space and wants argument suggestions) or
    // extra tokens past the verb (the user is editing the argument).
    let mut best: Option<&VerbSpec> = None;
    for spec in VERB_SPECS {
        let n = spec.tokens.len();
        if tokens.len() >= n
            && tokens[..n].iter().zip(spec.tokens).all(|(a, b)| a == b)
            && (tokens.len() > n || trailing_ws)
            && best.is_none_or(|b| b.tokens.len() < n)
        {
            best = Some(spec);
        }
    }

    if let Some(spec) = best {
        return complete_argument(spec, &tokens, trailing_ws, slash, ctx);
    }

    // Otherwise, complete the verb itself. The user's prefix is everything
    // after the leading slash, with whitespace collapsed by `split_whitespace`
    // and re-joined with single spaces — so "plan  h" matches "plan harness".
    let prefix = tokens.join(" ");
    let prefix_with_trailing = if trailing_ws && !prefix.is_empty() {
        format!("{prefix} ")
    } else {
        prefix
    };
    let mut candidates: Vec<String> = Vec::new();
    for spec in VERB_SPECS {
        let label = spec.tokens.join(" ");
        if label.starts_with(&prefix_with_trailing) {
            candidates.push(label);
        }
    }
    if candidates.is_empty() {
        return None;
    }
    Some(Completion {
        stem: slash.to_string(),
        candidates,
    })
}

fn complete_argument(
    spec: &VerbSpec,
    tokens: &[&str],
    trailing_ws: bool,
    slash: &str,
    ctx: &CompletionContext,
) -> Option<Completion> {
    let n = spec.tokens.len();
    // Determine the partial argument the user is editing, and how many
    // arg-tokens already exist (everything past the verb's tokens).
    let arg_tokens = &tokens[n..];
    let (partial, fixed_args) = if trailing_ws || arg_tokens.is_empty() {
        ("", arg_tokens)
    } else {
        let split = arg_tokens.len() - 1;
        (arg_tokens[split], &arg_tokens[..split])
    };

    let pool: &[String] = match spec.arg {
        ArgSource::Harness => &ctx.harnesses,
        ArgSource::PlanSlug => &ctx.plan_slugs,
        ArgSource::Branch => &ctx.branches,
        ArgSource::None => return None,
    };
    let candidates: Vec<String> = pool
        .iter()
        .filter(|c| c.starts_with(partial))
        .cloned()
        .collect();
    if candidates.is_empty() {
        return None;
    }

    // Build the stem. It's everything up to (but not including) the partial
    // arg the user is replacing — verb tokens + any earlier fixed args, all
    // single-space-separated, with a trailing space.
    let mut stem = String::from(slash);
    let mut first = true;
    for tok in spec.tokens.iter().chain(fixed_args.iter()) {
        if !first {
            stem.push(' ');
        }
        stem.push_str(tok);
        first = false;
    }
    if !first {
        stem.push(' ');
    }
    Some(Completion { stem, candidates })
}

/// Run `git branch --list` in `workdir` and return the branch names. Any
/// error (no git, not a repo, malformed output) yields an empty list — the
/// palette degrades to "no branch suggestions" rather than blowing up.
///
/// Output format pinned via `--format=%(refname:short)` so we don't have to
/// strip the `* ` current-branch marker that `git branch` would otherwise
/// prepend.
pub fn list_git_branches(workdir: &Path) -> Vec<String> {
    let output = Command::new("git")
        .args(["branch", "--list", "--format=%(refname:short)"])
        .current_dir(workdir)
        .output();
    let output = match output {
        Ok(o) if o.status.success() => o,
        _ => return Vec::new(),
    };
    String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

// ---------------------------------------------------------------------------
// Palette state
// ---------------------------------------------------------------------------

/// Outcome of a single key event delivered to the palette while it is open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyOutcome {
    /// Key consumed; palette stays open with possibly mutated state.
    None,
    /// User pressed Esc / Ctrl-C — palette should close without dispatching.
    Cancel,
    /// User pressed Enter — `result` is the parsed command (or a parse
    /// error for the dispatcher to surface as a toast).
    Submit(Result<PaletteCommand, ParseError>),
}

/// Tab-cycle state. Stored on the palette across consecutive `<tab>`
/// presses; cleared on any other key so a fresh tab cycle recomputes
/// candidates from the new buffer.
#[derive(Debug, Clone)]
struct CycleState {
    stem: String,
    candidates: Vec<String>,
    index: usize,
}

/// Single-line input bar at the bottom of the screen. Created once per TUI
/// session and toggled on/off via [`Palette::open`] / [`Palette::close`].
#[derive(Debug, Clone)]
pub struct Palette {
    /// True while the input bar is rendered and consuming key events.
    pub active: bool,
    /// Visual prefix character (`/` or `:`) showing which key opened the
    /// palette. Purely cosmetic; the parser ignores it.
    pub prefix: char,
    /// Text the user has typed (NOT including the prefix).
    pub buffer: String,
    /// Byte offset of the cursor within `buffer`. Always at a char boundary.
    pub cursor: usize,
    /// Active tab-completion cycle, if one is in progress.
    cycle: Option<CycleState>,
}

impl Default for Palette {
    fn default() -> Self {
        Self::new()
    }
}

impl Palette {
    pub fn new() -> Self {
        Self {
            active: false,
            prefix: '/',
            buffer: String::new(),
            cursor: 0,
            cycle: None,
        }
    }

    /// Open the palette with the given prefix character. Resets the buffer,
    /// cursor, and any pending cycle.
    pub fn open(&mut self, prefix: char) {
        self.active = true;
        self.prefix = prefix;
        self.buffer.clear();
        self.cursor = 0;
        self.cycle = None;
    }

    /// Close the palette. Discards any in-flight buffer / cycle.
    pub fn close(&mut self) {
        self.active = false;
        self.buffer.clear();
        self.cursor = 0;
        self.cycle = None;
    }

    /// Handle one key event. Returns the outcome the dispatcher should act
    /// on (cancel = call `close`, submit = parse + dispatch + close).
    pub fn handle_key(&mut self, key: KeyEvent, ctx: &CompletionContext) -> KeyOutcome {
        // Ctrl-C cancels regardless of which key code rides on it.
        if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('c')) {
            return KeyOutcome::Cancel;
        }

        match key.code {
            KeyCode::Esc => KeyOutcome::Cancel,
            KeyCode::Enter => KeyOutcome::Submit(parse(&self.buffer)),
            KeyCode::Tab => {
                self.tab(ctx);
                KeyOutcome::None
            }
            KeyCode::Backspace => {
                self.backspace();
                KeyOutcome::None
            }
            KeyCode::Left => {
                self.move_cursor_left();
                self.cycle = None;
                KeyOutcome::None
            }
            KeyCode::Right => {
                self.move_cursor_right();
                self.cycle = None;
                KeyOutcome::None
            }
            KeyCode::Home => {
                self.cursor = 0;
                self.cycle = None;
                KeyOutcome::None
            }
            KeyCode::End => {
                self.cursor = self.buffer.len();
                self.cycle = None;
                KeyOutcome::None
            }
            KeyCode::Char(c) => {
                // Suppress modifier-decorated chars (Ctrl-A etc.) so they
                // don't insert literal letters. Plain Shift is fine — it
                // already produces uppercased chars via the OS layer.
                if key
                    .modifiers
                    .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT)
                {
                    return KeyOutcome::None;
                }
                self.insert_char(c);
                KeyOutcome::None
            }
            _ => KeyOutcome::None,
        }
    }

    fn insert_char(&mut self, c: char) {
        self.buffer.insert(self.cursor, c);
        self.cursor += c.len_utf8();
        self.cycle = None;
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        // Walk back to the previous char boundary so multi-byte chars are
        // removed cleanly.
        let mut prev = self.cursor - 1;
        while !self.buffer.is_char_boundary(prev) {
            prev -= 1;
        }
        self.buffer.replace_range(prev..self.cursor, "");
        self.cursor = prev;
        self.cycle = None;
    }

    fn move_cursor_left(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let mut prev = self.cursor - 1;
        while !self.buffer.is_char_boundary(prev) {
            prev -= 1;
        }
        self.cursor = prev;
    }

    fn move_cursor_right(&mut self) {
        if self.cursor >= self.buffer.len() {
            return;
        }
        let mut next = self.cursor + 1;
        while next < self.buffer.len() && !self.buffer.is_char_boundary(next) {
            next += 1;
        }
        self.cursor = next;
    }

    fn tab(&mut self, ctx: &CompletionContext) {
        // Continue a cycle if we already have one and the buffer still
        // matches what the cycle produced last time.
        if let Some(cycle) = &mut self.cycle
            && !cycle.candidates.is_empty()
        {
            cycle.index = (cycle.index + 1) % cycle.candidates.len();
            self.buffer = format!("{}{}", cycle.stem, cycle.candidates[cycle.index]);
            self.cursor = self.buffer.len();
            return;
        }

        let Some(completion) = build_completion(&self.buffer, ctx) else {
            return;
        };
        if completion.candidates.is_empty() {
            return;
        }
        self.buffer = format!("{}{}", completion.stem, completion.candidates[0]);
        self.cursor = self.buffer.len();
        self.cycle = Some(CycleState {
            stem: completion.stem,
            candidates: completion.candidates,
            index: 0,
        });
    }
}

// ---------------------------------------------------------------------------
// Rendering
// ---------------------------------------------------------------------------

/// Draw the palette as a single bordered row over `area`. Caller is
/// expected to render it AFTER the host view, so it overlays the bottom
/// hint bar without disturbing the rest of the screen.
pub fn render(frame: &mut Frame, area: Rect, palette: &Palette) {
    if !palette.active || area.height == 0 || area.width == 0 {
        return;
    }
    frame.render_widget(Clear, area);

    let mut spans: Vec<Span<'_>> = Vec::with_capacity(2);
    let prefix = format!("{} ", palette.prefix);
    spans.push(Span::styled(
        prefix,
        Style::default()
            .fg(theme::CURSOR)
            .add_modifier(Modifier::BOLD),
    ));
    spans.push(Span::raw(palette.buffer.clone()));

    let block = Block::default()
        .borders(Borders::TOP)
        .border_style(Style::default().fg(theme::CURSOR));
    let para = Paragraph::new(Line::from(spans)).block(block);
    frame.render_widget(para, area);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::{KeyEvent, KeyModifiers};

    fn ctx(harnesses: &[&str], plans: &[&str], branches: &[&str]) -> CompletionContext {
        CompletionContext {
            harnesses: harnesses.iter().map(|s| (*s).to_string()).collect(),
            plan_slugs: plans.iter().map(|s| (*s).to_string()).collect(),
            branches: branches.iter().map(|s| (*s).to_string()).collect(),
        }
    }

    fn key(code: KeyCode) -> KeyEvent {
        KeyEvent::new(code, KeyModifiers::NONE)
    }

    fn key_with(code: KeyCode, mods: KeyModifiers) -> KeyEvent {
        KeyEvent::new(code, mods)
    }

    // -- parser: every recognized verb ------------------------------------

    #[test]
    fn parses_run_no_branch() {
        assert_eq!(parse("/run"), Ok(PaletteCommand::Run(None)));
        assert_eq!(parse("run"), Ok(PaletteCommand::Run(None)));
    }

    #[test]
    fn parses_run_with_branch() {
        assert_eq!(
            parse("/run feature-x"),
            Ok(PaletteCommand::Run(Some("feature-x".to_string())))
        );
    }

    #[test]
    fn parses_plan_harness_optional() {
        assert_eq!(
            parse("/plan harness"),
            Ok(PaletteCommand::PlanHarness(None))
        );
        assert_eq!(
            parse("/plan harness claude"),
            Ok(PaletteCommand::PlanHarness(Some("claude".to_string())))
        );
    }

    #[test]
    fn parses_plan_show() {
        assert_eq!(parse("/plan show"), Ok(PaletteCommand::PlanShow(None)));
        assert_eq!(
            parse("/plan show my-plan"),
            Ok(PaletteCommand::PlanShow(Some("my-plan".to_string())))
        );
    }

    #[test]
    fn parses_plan_archive() {
        assert_eq!(
            parse("/plan archive"),
            Ok(PaletteCommand::PlanArchive(None))
        );
        assert_eq!(
            parse("/plan archive p"),
            Ok(PaletteCommand::PlanArchive(Some("p".to_string())))
        );
    }

    #[test]
    fn parses_plan_unarchive_requires_slug() {
        assert!(matches!(
            parse("/plan unarchive"),
            Err(ParseError::MissingArgument {
                command: "/plan unarchive",
                ..
            })
        ));
        assert_eq!(
            parse("/plan unarchive p"),
            Ok(PaletteCommand::PlanUnarchive("p".to_string()))
        );
    }

    #[test]
    fn parses_plan_delete_requires_slug() {
        assert!(matches!(
            parse("/plan delete"),
            Err(ParseError::MissingArgument { .. })
        ));
        assert_eq!(
            parse("/plan delete p"),
            Ok(PaletteCommand::PlanDelete("p".to_string()))
        );
    }

    #[test]
    fn parses_plan_approve() {
        assert_eq!(
            parse("/plan approve"),
            Ok(PaletteCommand::PlanApprove(None))
        );
        assert_eq!(
            parse("/plan approve p"),
            Ok(PaletteCommand::PlanApprove(Some("p".to_string())))
        );
    }

    #[test]
    fn parses_plan_questions_on_off() {
        assert_eq!(
            parse("/plan questions on"),
            Ok(PaletteCommand::PlanQuestionsOn(None))
        );
        assert_eq!(
            parse("/plan questions off p"),
            Ok(PaletteCommand::PlanQuestionsOff(Some("p".to_string())))
        );
    }

    #[test]
    fn parses_plan_dependency_subcommands() {
        assert_eq!(
            parse("/plan dependency add"),
            Ok(PaletteCommand::PlanDependencyAdd)
        );
        assert_eq!(
            parse("/plan dependency remove"),
            Ok(PaletteCommand::PlanDependencyRemove)
        );
        assert_eq!(
            parse("/plan dependency list"),
            Ok(PaletteCommand::PlanDependencyList)
        );
    }

    #[test]
    fn parses_plan_hook_subcommands() {
        assert_eq!(parse("/plan set-hook"), Ok(PaletteCommand::PlanSetHook));
        assert_eq!(parse("/plan unset-hook"), Ok(PaletteCommand::PlanUnsetHook));
        assert_eq!(parse("/plan hooks"), Ok(PaletteCommand::PlanHooks));
    }

    #[test]
    fn parses_step_add_with_multi_word_title() {
        assert_eq!(
            parse("/step add do the thing"),
            Ok(PaletteCommand::StepAdd("do the thing".to_string()))
        );
    }

    #[test]
    fn parses_step_add_requires_title() {
        assert!(matches!(
            parse("/step add"),
            Err(ParseError::MissingArgument {
                command: "/step add",
                ..
            })
        ));
    }

    #[test]
    fn parses_step_skip() {
        assert_eq!(parse("/step skip"), Ok(PaletteCommand::StepSkip(None)));
        assert_eq!(parse("/step skip 7"), Ok(PaletteCommand::StepSkip(Some(7))));
    }

    #[test]
    fn parses_step_skip_rejects_non_integer() {
        assert!(matches!(
            parse("/step skip foo"),
            Err(ParseError::InvalidArgument {
                command: "/step skip",
                ..
            })
        ));
    }

    #[test]
    fn parses_step_move() {
        assert_eq!(
            parse("/step move 3 --to 5"),
            Ok(PaletteCommand::StepMove { num: 3, to: 5 })
        );
    }

    #[test]
    fn parses_step_move_rejects_malformed() {
        assert!(matches!(parse("/step move"), Err(ParseError::Unknown(_))));
        assert!(matches!(parse("/step move 3"), Err(ParseError::Unknown(_))));
        assert!(matches!(
            parse("/step move 3 5"),
            Err(ParseError::Unknown(_))
        ));
        assert!(matches!(
            parse("/step move foo --to 5"),
            Err(ParseError::InvalidArgument {
                command: "/step move",
                ..
            })
        ));
    }

    #[test]
    fn parses_step_hook_subcommands() {
        assert_eq!(parse("/step set-hook"), Ok(PaletteCommand::StepSetHook));
        assert_eq!(parse("/step unset-hook"), Ok(PaletteCommand::StepUnsetHook));
    }

    #[test]
    fn parses_step_edit_tags() {
        assert_eq!(parse("/step edit --tags"), Ok(PaletteCommand::StepEditTags));
    }

    #[test]
    fn parses_cancel_quit_help() {
        assert_eq!(parse("/cancel"), Ok(PaletteCommand::Cancel));
        assert_eq!(parse("/quit"), Ok(PaletteCommand::Quit));
        assert_eq!(parse("/q"), Ok(PaletteCommand::Quit));
        assert_eq!(parse("/help"), Ok(PaletteCommand::Help));
    }

    #[test]
    fn parses_inbox_and_focus() {
        // docs/dag-redesign.md §12.3 / §12.2 palette wiring. The `:` prefix
        // key is consumed by the palette bar before `parse`, so the parser
        // only ever sees the slash-or-bare form (mirrors every other verb).
        assert_eq!(parse("/inbox"), Ok(PaletteCommand::Inbox));
        assert_eq!(parse("inbox"), Ok(PaletteCommand::Inbox));
        assert_eq!(parse("/focus"), Ok(PaletteCommand::Focus(None)));
        assert_eq!(
            parse("/focus c9d4a1b2"),
            Ok(PaletteCommand::Focus(Some("c9d4a1b2".to_string())))
        );
        // Labels for the help/toast surfaces.
        assert_eq!(PaletteCommand::Inbox.label(), "/inbox");
        assert_eq!(PaletteCommand::Focus(None).label(), "/focus");
    }

    #[test]
    fn parses_export_import() {
        assert_eq!(
            parse("/export my-plan"),
            Ok(PaletteCommand::Export {
                slug: "my-plan".to_string(),
                output: None,
            })
        );
        assert_eq!(
            parse("/export my-plan -o /tmp/out.json"),
            Ok(PaletteCommand::Export {
                slug: "my-plan".to_string(),
                output: Some("/tmp/out.json".to_string()),
            })
        );
        assert!(matches!(
            parse("/export"),
            Err(ParseError::MissingArgument {
                command: "/export",
                ..
            })
        ));
        assert_eq!(
            parse("/import /tmp/p.json"),
            Ok(PaletteCommand::Import("/tmp/p.json".to_string()))
        );
        assert!(matches!(
            parse("/import"),
            Err(ParseError::MissingArgument {
                command: "/import",
                ..
            })
        ));
    }

    // -- parser: edge cases -----------------------------------------------

    #[test]
    fn parser_treats_leading_slash_as_optional() {
        assert_eq!(parse("run"), parse("/run"));
        assert_eq!(parse("plan show"), parse("/plan show"));
    }

    #[test]
    fn parser_collapses_extra_whitespace() {
        assert_eq!(
            parse("  /plan   show  "),
            Ok(PaletteCommand::PlanShow(None))
        );
    }

    #[test]
    fn parser_returns_empty_for_blank_input() {
        assert_eq!(parse(""), Err(ParseError::Empty));
        assert_eq!(parse("   "), Err(ParseError::Empty));
        assert_eq!(parse("/"), Err(ParseError::Empty));
        assert_eq!(parse("  /  "), Err(ParseError::Empty));
    }

    #[test]
    fn parser_returns_unknown_for_unrecognized_verb() {
        assert_eq!(
            parse("/foobar"),
            Err(ParseError::Unknown("foobar".to_string()))
        );
        assert_eq!(
            parse("/plan harnes typo"),
            Err(ParseError::Unknown("plan harnes typo".to_string()))
        );
    }

    // -- completion: verb cycling -----------------------------------------

    #[test]
    fn completion_empty_buffer_lists_all_verbs() {
        let c = build_completion("", &ctx(&[], &[], &[])).expect("candidates");
        assert_eq!(c.stem, "");
        // Every spec contributes a candidate; check a few representatives.
        assert!(c.candidates.iter().any(|s| s == "run"));
        assert!(c.candidates.iter().any(|s| s == "plan harness"));
        assert!(c.candidates.iter().any(|s| s == "help"));
        assert_eq!(c.candidates.len(), VERB_SPECS.len());
    }

    #[test]
    fn completion_keeps_user_slash_in_stem() {
        let c = build_completion("/r", &ctx(&[], &[], &[])).expect("candidates");
        assert_eq!(c.stem, "/");
        assert_eq!(c.candidates, vec!["run".to_string()]);
    }

    #[test]
    fn completion_omits_slash_when_user_didnt_type_it() {
        let c = build_completion("r", &ctx(&[], &[], &[])).expect("candidates");
        assert_eq!(c.stem, "");
        assert_eq!(c.candidates, vec!["run".to_string()]);
    }

    #[test]
    fn completion_filters_by_prefix() {
        let c = build_completion("/plan h", &ctx(&[], &[], &[])).expect("candidates");
        assert_eq!(c.stem, "/");
        assert_eq!(
            c.candidates,
            vec!["plan harness".to_string(), "plan hooks".to_string()]
        );
    }

    #[test]
    fn completion_returns_none_when_nothing_matches() {
        assert!(build_completion("/foo", &ctx(&[], &[], &[])).is_none());
        assert!(build_completion("plan zzz", &ctx(&[], &[], &[])).is_none());
    }

    // -- completion: argument cycling -------------------------------------

    #[test]
    fn completion_after_run_lists_branches() {
        let c = build_completion("/run ", &ctx(&[], &[], &["main", "feature-x", "tui-v1"]))
            .expect("candidates");
        assert_eq!(c.stem, "/run ");
        assert_eq!(
            c.candidates,
            vec![
                "main".to_string(),
                "feature-x".to_string(),
                "tui-v1".to_string()
            ]
        );
    }

    #[test]
    fn completion_after_plan_harness_lists_harnesses() {
        let c = build_completion("/plan harness ", &ctx(&["claude", "codex", "pi"], &[], &[]))
            .expect("candidates");
        assert_eq!(c.stem, "/plan harness ");
        assert_eq!(c.candidates.len(), 3);
    }

    #[test]
    fn completion_after_plan_archive_lists_plan_slugs() {
        let c = build_completion(
            "/plan archive ",
            &ctx(&[], &["alpha", "beta", "gamma"], &[]),
        )
        .expect("candidates");
        assert_eq!(c.stem, "/plan archive ");
        assert_eq!(c.candidates.len(), 3);
    }

    #[test]
    fn completion_filters_argument_by_partial_prefix() {
        let c = build_completion(
            "/plan archive be",
            &ctx(&[], &["alpha", "beta", "berlin", "gamma"], &[]),
        )
        .expect("candidates");
        assert_eq!(c.stem, "/plan archive ");
        assert_eq!(c.candidates, vec!["beta".to_string(), "berlin".to_string()]);
    }

    #[test]
    fn completion_returns_none_when_argsource_is_none() {
        // /step add takes a free-form title; tab proposes nothing.
        assert!(build_completion("/step add ", &ctx(&[], &[], &[])).is_none());
    }

    #[test]
    fn completion_returns_none_when_arg_pool_empty() {
        assert!(build_completion("/run ", &ctx(&[], &[], &[])).is_none());
    }

    // -- completion: longest-prefix match --------------------------------

    #[test]
    fn completion_prefers_longer_verb_match() {
        // "/plan questions on " should complete plan slugs (the deeper
        // verb) — NOT cycle through verbs starting with "plan questions".
        let c = build_completion("/plan questions on ", &ctx(&[], &["alpha", "beta"], &[]))
            .expect("candidates");
        assert_eq!(c.stem, "/plan questions on ");
        assert_eq!(c.candidates.len(), 2);
    }

    // -- Palette state machine --------------------------------------------

    #[test]
    fn open_then_close_resets_buffer() {
        let mut p = Palette::new();
        assert!(!p.active);
        p.open(':');
        assert!(p.active);
        assert_eq!(p.prefix, ':');
        assert_eq!(p.buffer, "");
        assert_eq!(p.cursor, 0);

        p.handle_key(key(KeyCode::Char('r')), &CompletionContext::default());
        p.handle_key(key(KeyCode::Char('u')), &CompletionContext::default());
        assert_eq!(p.buffer, "ru");
        assert_eq!(p.cursor, 2);

        p.close();
        assert!(!p.active);
        assert_eq!(p.buffer, "");
    }

    #[test]
    fn esc_cancels() {
        let mut p = Palette::new();
        p.open('/');
        p.handle_key(key(KeyCode::Char('r')), &CompletionContext::default());
        let out = p.handle_key(key(KeyCode::Esc), &CompletionContext::default());
        assert_eq!(out, KeyOutcome::Cancel);
    }

    #[test]
    fn ctrl_c_cancels() {
        let mut p = Palette::new();
        p.open('/');
        let out = p.handle_key(
            key_with(KeyCode::Char('c'), KeyModifiers::CONTROL),
            &CompletionContext::default(),
        );
        assert_eq!(out, KeyOutcome::Cancel);
    }

    #[test]
    fn enter_submits_parsed_command() {
        let mut p = Palette::new();
        p.open('/');
        for c in "run main".chars() {
            p.handle_key(key(KeyCode::Char(c)), &CompletionContext::default());
        }
        let out = p.handle_key(key(KeyCode::Enter), &CompletionContext::default());
        assert_eq!(
            out,
            KeyOutcome::Submit(Ok(PaletteCommand::Run(Some("main".to_string()))))
        );
    }

    #[test]
    fn enter_on_unknown_returns_parse_error() {
        let mut p = Palette::new();
        p.open(':');
        for c in "nonsense".chars() {
            p.handle_key(key(KeyCode::Char(c)), &CompletionContext::default());
        }
        let out = p.handle_key(key(KeyCode::Enter), &CompletionContext::default());
        assert!(matches!(
            out,
            KeyOutcome::Submit(Err(ParseError::Unknown(_)))
        ));
    }

    #[test]
    fn backspace_removes_char_before_cursor() {
        let mut p = Palette::new();
        p.open('/');
        for c in "run".chars() {
            p.handle_key(key(KeyCode::Char(c)), &CompletionContext::default());
        }
        p.handle_key(key(KeyCode::Backspace), &CompletionContext::default());
        assert_eq!(p.buffer, "ru");
        assert_eq!(p.cursor, 2);
        // Backspace at empty buffer is a safe no-op.
        for _ in 0..5 {
            p.handle_key(key(KeyCode::Backspace), &CompletionContext::default());
        }
        assert_eq!(p.buffer, "");
        assert_eq!(p.cursor, 0);
    }

    #[test]
    fn ctrl_modified_chars_are_ignored() {
        let mut p = Palette::new();
        p.open('/');
        // Ctrl-A would otherwise insert literal 'a'.
        p.handle_key(
            key_with(KeyCode::Char('a'), KeyModifiers::CONTROL),
            &CompletionContext::default(),
        );
        assert_eq!(p.buffer, "");
    }

    // -- tab cycling integration ------------------------------------------

    #[test]
    fn tab_completes_verb_prefix() {
        let mut p = Palette::new();
        p.open('/');
        for c in "ru".chars() {
            p.handle_key(key(KeyCode::Char(c)), &CompletionContext::default());
        }
        p.handle_key(key(KeyCode::Tab), &CompletionContext::default());
        assert_eq!(p.buffer, "run");
        assert_eq!(p.cursor, p.buffer.len());
    }

    #[test]
    fn tab_cycles_with_wrap_around() {
        let mut p = Palette::new();
        p.open('/');
        for c in "plan h".chars() {
            p.handle_key(key(KeyCode::Char(c)), &CompletionContext::default());
        }
        let ctx = CompletionContext::default();
        // First tab → first candidate, "plan harness".
        p.handle_key(key(KeyCode::Tab), &ctx);
        assert_eq!(p.buffer, "plan harness");
        // Second tab → second candidate, "plan hooks".
        p.handle_key(key(KeyCode::Tab), &ctx);
        assert_eq!(p.buffer, "plan hooks");
        // Third tab wraps back to "plan harness".
        p.handle_key(key(KeyCode::Tab), &ctx);
        assert_eq!(p.buffer, "plan harness");
    }

    #[test]
    fn tab_cycles_branch_arguments() {
        let mut p = Palette::new();
        p.open('/');
        for c in "/run ".chars() {
            p.handle_key(key(KeyCode::Char(c)), &CompletionContext::default());
        }
        let context = ctx(&[], &[], &["main", "tui-v1"]);
        p.handle_key(key(KeyCode::Tab), &context);
        assert_eq!(p.buffer, "/run main");
        p.handle_key(key(KeyCode::Tab), &context);
        assert_eq!(p.buffer, "/run tui-v1");
        p.handle_key(key(KeyCode::Tab), &context);
        assert_eq!(p.buffer, "/run main");
    }

    #[test]
    fn typing_after_tab_resets_cycle() {
        let mut p = Palette::new();
        p.open('/');
        for c in "plan h".chars() {
            p.handle_key(key(KeyCode::Char(c)), &CompletionContext::default());
        }
        let context = CompletionContext::default();
        p.handle_key(key(KeyCode::Tab), &context);
        assert_eq!(p.buffer, "plan harness");

        // User types 'x' — cycle resets so the next tab recomputes.
        p.handle_key(key(KeyCode::Char('x')), &context);
        assert_eq!(p.buffer, "plan harnessx");
        // Nothing matches "plan harnessx" — tab must be a no-op.
        p.handle_key(key(KeyCode::Tab), &context);
        assert_eq!(p.buffer, "plan harnessx");
    }

    #[test]
    fn tab_with_no_matches_is_a_noop() {
        let mut p = Palette::new();
        p.open('/');
        for c in "zzz".chars() {
            p.handle_key(key(KeyCode::Char(c)), &CompletionContext::default());
        }
        p.handle_key(key(KeyCode::Tab), &CompletionContext::default());
        assert_eq!(p.buffer, "zzz");
    }

    // -- label() ----------------------------------------------------------

    #[test]
    fn every_command_has_a_distinct_label() {
        // The dispatcher uses `label()` to format the "<verb> not yet
        // implemented" toast — labels must be both stable and include the
        // leading slash.
        let labels: Vec<&str> = [
            PaletteCommand::Run(None),
            PaletteCommand::PlanHarness(None),
            PaletteCommand::PlanShow(None),
            PaletteCommand::PlanArchive(None),
            PaletteCommand::PlanUnarchive(String::new()),
            PaletteCommand::PlanDelete(String::new()),
            PaletteCommand::PlanApprove(None),
            PaletteCommand::PlanQuestionsOn(None),
            PaletteCommand::PlanQuestionsOff(None),
            PaletteCommand::PlanDependencyAdd,
            PaletteCommand::PlanDependencyRemove,
            PaletteCommand::PlanDependencyList,
            PaletteCommand::PlanSetHook,
            PaletteCommand::PlanUnsetHook,
            PaletteCommand::PlanHooks,
            PaletteCommand::StepAdd(String::new()),
            PaletteCommand::StepSkip(None),
            PaletteCommand::StepMove { num: 0, to: 0 },
            PaletteCommand::StepSetHook,
            PaletteCommand::StepUnsetHook,
            PaletteCommand::StepEditTags,
            PaletteCommand::Cancel,
            PaletteCommand::Export {
                slug: String::new(),
                output: None,
            },
            PaletteCommand::Import(String::new()),
            PaletteCommand::Quit,
            PaletteCommand::Help,
        ]
        .iter()
        .map(PaletteCommand::label)
        .collect();
        for l in &labels {
            assert!(l.starts_with('/'), "label should start with '/': {l}");
        }
        let mut sorted = labels.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), labels.len(), "labels must be unique");
    }

    // -- render: doesn't panic, contains buffer ---------------------------

    #[test]
    fn render_writes_prefix_and_buffer() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(40, 4);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut palette = Palette::new();
        palette.open(':');
        palette.buffer = "plan harness".to_string();
        palette.cursor = palette.buffer.len();

        terminal
            .draw(|f| {
                let area = Rect::new(0, 2, 40, 2);
                render(f, area, &palette);
            })
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        let rendered: String = (0..buffer.area().height)
            .map(|y| {
                (0..buffer.area().width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        assert!(rendered.contains(": "), "prefix missing:\n{rendered}");
        assert!(
            rendered.contains("plan harness"),
            "buffer missing:\n{rendered}"
        );
    }

    #[test]
    fn render_inactive_palette_is_a_noop() {
        use ratatui::Terminal;
        use ratatui::backend::TestBackend;

        let backend = TestBackend::new(20, 3);
        let mut terminal = Terminal::new(backend).unwrap();
        let palette = Palette::new();
        terminal
            .draw(|f| {
                let area = Rect::new(0, 1, 20, 1);
                render(f, area, &palette);
            })
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        // The cell at (0, 1) should be the default (' ') since render bailed
        // before drawing anything.
        assert_eq!(buffer[(0, 1)].symbol(), " ");
    }

    // -- list_git_branches ------------------------------------------------

    #[test]
    fn list_git_branches_in_non_repo_returns_empty() {
        let tmp = tempfile::tempdir().expect("tempdir");
        // Not a git repository — git exits non-zero, helper degrades to [].
        assert!(list_git_branches(tmp.path()).is_empty());
    }
}
