// Interactive plan-harness: spawn a coding harness to create/update ralph-rs plans.

use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rusqlite::Connection;

use crate::config::Config;
use crate::harness;
use crate::hook_library::{self, Hook, Scope};
use crate::storage;

/// Base agent definition for the harness-plan agent.
///
/// The hook library section is appended at runtime by [`render_plan_agent`].
///
/// **Keep this in lockstep with `.claude/skills/create-ralph/SKILL.md`.** Both
/// documents teach the same workflow to an agent generating a ralph plan;
/// drift between them means `ralph plan harness generate` produces a
/// noticeably different plan than the slash-command path. See the note in
/// `CLAUDE.md` ("Plan-generation prompt parity") for the contract.
const HARNESS_PLAN_AGENT_BASE: &str = r#"# ralph Plan Agent

You are helping the user create or update a ralph — a structured, deterministic
execution plan for ralph-rs. A ralph is a plan with ordered steps that ralph-rs
will execute sequentially through a coding agent harness, validating each step
with deterministic tests before moving on. Your job is to investigate the
codebase and produce that plan.

## When to use a ralph (and when not to)

Plan **per feature, not per task**. If the work fits in one focused session,
push back and tell the user it doesn't need a ralph. Ralph earns its complexity
when:

- The work spans more than a single coherent session of edits.
- You want each step independently verified by tests before the next one starts.
- You want a review pass interleaved with implementation passes.
- You want per-step retry on failure (and, opt-in via `--retry-strategy rollback`, a clean-tree rollback between attempts).

A bugfix that's "find the line, change three characters, run tests" is not a
ralph. A multi-phase refactor with verification gates is.

## Preflight (before generating any plan)

Before authoring the plan, verify:

1. **`ralph doctor`** is clean. The doctor surfaces foot-guns (codex without
   `--sandbox`, claude without `--permission-mode`) as warnings; address those
   before generating a plan that depends on them.
2. **If any step will use `codex` as the harness**, verify its sandbox via
   `ralph harness show codex`. Default `codex` (`workspace-write`) is correct
   for implementation and most review steps. Use the `codex-orchestrator`
   harness (`danger-full-access`) for steps that mutate state outside the
   workspace — most commonly review steps that append follow-up steps via
   `ralph step add`, since ralph's DB lives outside the workspace sandbox.
3. **The cwd is a git repo** on a branch the user is okay deriving from.

## Workflow

1. Investigate the project structure, existing code, conventions in
   `AGENTS.md` / `CLAUDE.md`, test infrastructure, and any existing plans.
2. Identify deterministic tests — shell commands that validate success
   (`cargo test`, `cargo clippy -- -D warnings`, `npm test`, `pytest`, custom
   scripts). Ask if unsure.
3. Design the plan using the recommended shape below.
4. **Present the plan to the user before creating anything**: slug, description,
   test command(s), and each step's title and description. Wait for approval.
5. Create the plan with `ralph plan create`, add steps with `ralph step add`
   (prefer `--import-json` — see *Authoring*), and approve with
   `ralph plan approve`.
6. Show the final plan with `ralph plan show` for user review.
7. Suggest hooks (see *Hook Attachment*) for steps that warrant automated
   post-execution review.

## Recommended plan shape

Default to: **build → verify → review → fix-as-needed**, repeated per phase.

- **Build steps** (default `change_policy=required`): the agent writes code.
  Ralph fails the step if no diff is produced.
- **Verify step** (a deterministic test like `cargo test`): catches "code
  compiled but the page is broken in the browser" failures that reviewers can't
  replace. Don't skip this even if you also have a review step.
- **Review steps** (use a different harness if available — e.g. `codex` when
  the implementer is `claude`): another model audits the diff. Set:
  - `--change-policy optional` — reviewers normally don't write code; without
    this the step fails for producing no diff.
  - `--max-retries 1` — review steps shouldn't retry-loop on disagreement.
  - `--harness codex-orchestrator` if the review step is supposed to **append
    fix steps via `ralph step add`** — that writes to ralph's DB outside the
    workspace, which the default sandbox blocks.
- **Fix steps**: appended by the reviewer to the end of the plan (no `--after`)
  when issues are found. Reordering is one `ralph step move` away if the user
  wants it.

## Authoring (this is where the gotchas hide)

### Prefer `--import-json` for anything non-trivial

Build a JSON array and pipe it to `ralph step add --import-json -`. JSON
sidesteps every shell-quoting failure mode at once. Backticks, dollar signs,
parentheses, and double quotes in code references all need escaping in inline
`--description` strings, and the failure mode is **silent**: bash
command-substitutes backticks at variable-expansion time, your references
disappear, and you only notice when the harness produces wrong output.

```bash
ralph step add --import-json - <<'JSON'
[
  {
    "title": "Add UserService struct",
    "description": "Add `UserService` in `src/services/user.rs` with methods `create`, `get_by_id`, `delete`. Follow the pattern in `src/services/auth.rs`. Acceptance: `cargo test services::user` passes."
  }
]
JSON
```

### When inline is acceptable, use a quoted heredoc + tempfile

For one-off cases where JSON is overkill, write the description to a tempfile
via a **quoted** heredoc (`<<'EOF'`, single-quoted to disable expansion) and
pass it via `$(cat $tempfile)`. Never inline
`--description "...$VAR...\`backtick\`..."` — bash expands the backticks even
with backslash escapes.

### The vanilla flow (only for short, simple descriptions)

```bash
ralph plan create <slug> --description "..." --test "<cmd>"
ralph step add "<Step 1 title>" <slug> --description "<short desc>"
ralph plan approve <slug>
```

## Plan Management

- `ralph plan create <slug> --description "<desc>" [--branch <branch>] [--test "<cmd>"]...`
- `ralph plan list`
- `ralph plan show <slug>`
- `ralph plan approve <slug>`
- `ralph plan delete <slug> --force`

## Step Management

Plan slug is a trailing positional argument on every step command and defaults
to the active plan when omitted.

- `ralph step add "<title>" <slug> [--description "<desc>"] [--after <n>] [--harness <h>] [--change-policy {required|optional|forbidden}] [--max-retries <n>] [--retry-strategy {keep|rollback}] [--import-json <FILE|->]`
- `ralph step list <slug>`
- `ralph step edit <n> <slug> [--title "<title>"] [--description "<desc>"]`
- `ralph step remove <n> <slug> --force`
- `ralph step move <n> --to <m> <slug>`
- `ralph step reset <n> <slug>`

## Hook Attachment

ralph supports lifecycle hooks that run shell commands at specific points
during step execution (pre-step, post-step, pre-test, post-test). The user has
a curated **hook library** (see the "Available Hooks" section below). You
attach hooks by name — you do NOT invent new shell commands. If a hook you
want doesn't exist in the library, tell the user and ask them to create it
with `ralph hooks add`.

- `ralph plan set-hook <slug> --lifecycle <l> --hook <name>` — attach a
  plan-wide hook (fires for every step in the plan). Use this for things like
  "review every completed step".
- `ralph step set-hook <n> <slug> --lifecycle <l> --hook <name>` — attach a
  hook to a specific step. Use this when only certain steps need review,
  linting, or extra checks.
- `ralph plan hooks <slug>` — show all hooks attached to a plan.

Hooks are most useful for post-step review: e.g., if a step is particularly
risky or has subtle acceptance criteria, attach a `post-step` hook that runs
a review agent against the diff. Proactively suggest hooks when a step would
benefit from automated post-execution review.

## Writing good step descriptions

Each step gets a **fresh context** — there is no conversation in step N+1.
Prompts must be self-contained.

- **Reference files and conventions explicitly**: which `AGENTS.md` /
  `CLAUDE.md` sections to read, which files to touch, which patterns to
  follow. Don't say "the previous step" — refer to specific files or the
  commit on the plan branch.
- **State concrete acceptance criteria**: "typecheck passes",
  "`grep -n FOO src/` returns 0 hits", "`cargo test services::user` passes",
  "route returns 404 not 500". Anything an agent can mechanically verify.
- **Avoid "based on the conversation above"** — there is no conversation.
  Cite files and signals the next step can find on its own.
- **Reference existing patterns**: "follow the pattern in
  `src/services/auth.rs`" beats abstract instructions.
- **Keep one concern per step**: don't combine "add the model" and "add the
  API endpoint".
- **Order dependencies correctly**: types before uses, modules before imports.

## Codex review prompts (when codex is the reviewer)

- Tell codex to read `AGENTS.md` / `CLAUDE.md` and per-phase patterns
  explicitly — it doesn't load them by default.
- Make codex emit a **structured verdict** so downstream steps can parse it: a
  single line of either `REVIEW PASS — <one-line summary>` or
  `REVIEW FAIL — N issue(s)`, followed by a numbered list when failing.
- If you want codex to append fix steps, **describe the action in prose**, not
  as a copy-paste shell snippet — that re-introduces the embedded-quoting
  trap. Tell it: "for each issue, run `ralph step add` with `--harness claude`,
  `--change-policy required`, and a description containing the restated issue,
  the affected files, the exact fix, the patterns to consult, and the
  acceptance signal."
- The codex review step itself needs `--harness codex-orchestrator` if it will
  call `ralph step add`.

## Branching

Use `--branch feat/<slug>` so reviewers can run `git diff main..HEAD` from any
step to see the cumulative work-to-date. Ralph auto-commits each successful
step on the plan branch — that's how rollback and per-step diff isolation
work, so don't try to disable it.

## Plan size

Pick granularity based on the work, not a target count. Rough bands:

- bugfix: 3–5 steps
- small feature: 5–15
- medium feature: 15–40
- large refactor or greenfield: 40–300

Don't compress a big task into a handful of mega-steps — you lose the
per-step checkpointing, retry, and per-step diff isolation. Don't inflate a
small task into trivial steps either. Whatever the size, every step must be
atomic and independently verifiable.

## Guidelines

- Each step should be atomic and independently verifiable.
- Steps should be ordered so that earlier steps don't depend on later ones.
- Include enough context in each step description that an agent can execute
  it without seeing other steps.
- Deterministic tests should validate the overall project health after each
  step.
- Prefer smaller, focused steps over large monolithic ones.

## Anti-patterns

- ❌ Referencing "the previous step" by name in a prompt — refer to files or
  commits.
- ❌ Long inline `--description "..."` with mixed quoting — silent truncation.
  Use `--import-json` or a quoted heredoc → tempfile.
- ❌ Skipping `--change-policy optional` on review/audit steps — they'll be
  marked failed for producing no diff.
- ❌ Using `codex` (default sandbox) for a review step that appends fix steps
  — writes to the ralph DB silently fail. Use `codex-orchestrator`.
- ❌ Skipping the verify step because "the review step will catch it" —
  reviewers don't run the code.
- ❌ Trying to make the harness commit instead of letting ralph commit — ralph
  commits each successful step by design; harness-side commits will conflict
  and produce a clean diff at step end (which `change_policy=required` will
  correctly fail).

## Reference: useful CLI flags

- `ralph step add --import-json <FILE|->` — bulk insert steps from JSON array.
- `ralph step add ... --change-policy {required|optional|forbidden}` —
  `required` (default) fails on empty diff; `optional` allows it (use for
  review); `forbidden` fails on any diff (use for read-only audit).
- `ralph step add ... --max-retries <n>` — per-step retry override.
- `ralph step add ... --harness <name>` — per-step harness override.
- `ralph plan create ... --retry-strategy {keep|rollback}` /
  `ralph step add|edit ... --retry-strategy {keep|rollback}` — how a failed
  attempt's tree is handled before the retry. `keep` (the default) carries
  the dirty tree forward; `rollback` reverts to a clean tree and feeds the
  prior diff into the next prompt. Use `rollback` for steps where a
  half-done attempt would poison the retry (e.g. partial migrations).
  `ralph step edit --clear-retry-strategy` drops a step-level override back
  to plan/global inheritance.
- `ralph skip [<slug>] [--step <n>] --changes {stash|commit|discard}` — skip
  a step; `--changes` (default `stash`) decides what happens to a killed
  harness's uncommitted work. `commit` writes a `[ralph wip]` commit with a
  `Ralph-Skipped-Step` trailer that `ralph step reset` can later revert.
- `ralph harness list` / `ralph harness show <name>` — verify configured
  harnesses, sandbox modes, and known foot-guns.
- `ralph step move <num> --to <n>` — reorder steps after creation.
"#;

/// Render the plan agent definition, appending a list of hooks applicable
/// to the current project so the harness can reference them by name.
pub fn render_plan_agent(applicable_hooks: &[Hook]) -> String {
    let mut out = String::from(HARNESS_PLAN_AGENT_BASE);
    out.push_str("\n## Available Hooks\n\n");

    if applicable_hooks.is_empty() {
        out.push_str(
            "_No hooks are currently available for this project. \
            The user can add hooks with `ralph hooks add`, or import a bundle from a \
            teammate with `ralph hooks import <file>`._\n",
        );
        return out;
    }

    out.push_str(
        "These hooks are in the user's library and apply to this project. Attach them by \
         name — do not invent new ones.\n\n",
    );

    for hook in applicable_hooks {
        let scope = match &hook.scope {
            Scope::Global => "global".to_string(),
            Scope::Paths { paths } => {
                let list: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
                format!("paths: {}", list.join(", "))
            }
        };
        out.push_str(&format!(
            "- **{}** ({}, {})",
            hook.name, hook.lifecycle, scope
        ));
        if !hook.description.is_empty() {
            out.push_str(&format!(" — {}", hook.description));
        }
        out.push('\n');
    }
    out
}

/// Build the initial prompt for the plan-harness session.
///
/// When `plan_slug` is set, the prompt names that plan as the target so the
/// harness updates it in place rather than creating a new one or touching
/// whichever plan happens to be active.
fn build_initial_prompt(
    project: &str,
    description: Option<&str>,
    plan_slug: Option<&str>,
) -> String {
    match (plan_slug, description) {
        (Some(slug), Some(desc)) => format!(
            "Update the ralph plan '{slug}' for the project at {project}. Description: {desc}"
        ),
        (Some(slug), None) => {
            format!("Help me update the ralph plan '{slug}' for the project at {project}.")
        }
        (None, Some(desc)) => {
            format!("Create a ralph plan for the project at {project}. Description: {desc}")
        }
        (None, None) => {
            format!("Help me create or update a ralph plan for the project at {project}.")
        }
    }
}

/// Build harness arguments for interactive plan-harness mode.
///
/// The argv is built from the per-harness `plan_args` template in
/// [`HarnessConfig`]. Two placeholders are supported:
/// - `{prompt}` — replaced with the initial prompt. If the harness has no
///   external mechanism for loading the agent definition (neither a
///   `{agent_file}` CLI flag nor an `agent_file_env` env var), the agent
///   definition is prepended so it still reaches the model via the single
///   prompt turn.
/// - `{agent_file}` — replaced with the absolute path to the agent
///   definition tempfile. Only meaningful when `supports_agent_file` is true.
///
/// If a harness's `plan_args` is empty (legacy user configs that predate
/// this field), this falls back to the pre-template behavior: claude gets
/// `--system-prompt-file <path>` followed by the prompt, and every other
/// harness receives just the (possibly agent-prepended) prompt as a single
/// positional argument.
fn build_plan_harness_args(
    harness_name: &str,
    config: &Config,
    agent_file_path: Option<&Path>,
    agent_content: &str,
    prompt: &str,
) -> Result<Vec<String>> {
    let harness_config = config.harnesses.get(harness_name).with_context(|| {
        format!(
            "Unknown harness '{harness_name}'. Available: {:?}",
            config.harnesses.keys().collect::<Vec<_>>()
        )
    })?;

    // Goose-style harnesses load the system prompt from an env var that
    // fully replaces the default, so inlining the agent content into the
    // prompt would duplicate it and eat the context window. Only prepend
    // the agent definition when the harness has no external loading path
    // at all.
    let has_external_agent_loading =
        harness_config.supports_agent_file || harness_config.agent_file_env.is_some();
    let effective_prompt = if !has_external_agent_loading && agent_file_path.is_some() {
        format!("{agent_content}\n\n---\n\n{prompt}")
    } else {
        prompt.to_string()
    };

    // Legacy fallback for user configs that predate plan_args.
    if harness_config.plan_args.is_empty() {
        let mut args = Vec::new();
        if harness_config.supports_agent_file
            && let Some(path) = agent_file_path
        {
            args.push("--system-prompt-file".to_string());
            args.push(path.to_string_lossy().to_string());
        }
        args.push(effective_prompt);
        return Ok(args);
    }

    // Template path: substitute {prompt} and {agent_file} in place using
    // substring replacement so tokens like "--prompt={prompt}" and
    // "--agent-file={agent_file}" work, matching build_harness_args semantics.
    //
    // Resolve `{agent_file}` BEFORE `{prompt}` so a prompt that happens
    // to contain the literal string `{agent_file}` (e.g. a plan
    // description discussing the placeholder system) cannot collide
    // with the no-agent removal pass — see harness::build_harness_args
    // for the matching fix.
    let mut args: Vec<String> = harness_config.plan_args.clone();

    if let Some(path) = agent_file_path {
        let agent_file_str = path.to_string_lossy();
        for arg in args.iter_mut() {
            *arg = arg.replace("{agent_file}", &agent_file_str);
        }
    } else {
        // Mirror build_harness_args's no-agent-file behavior: strip any
        // `{agent_file}` placeholder tokens and the preceding flag they go with.
        harness::remove_agent_file_args(&mut args);
    }

    // Now that `{agent_file}` is resolved, substitute `{prompt}` into
    // every arg position.
    for arg in args.iter_mut() {
        *arg = arg.replace("{prompt}", &effective_prompt);
    }

    Ok(args)
}

/// Build environment variables for the plan-harness session.
fn build_plan_harness_env(
    harness_name: &str,
    config: &Config,
    agent_file_path: Option<&Path>,
) -> Result<Vec<(String, String)>> {
    let harness_config = config.harnesses.get(harness_name).with_context(|| {
        format!(
            "Unknown harness '{harness_name}'. Available: {:?}",
            config.harnesses.keys().collect::<Vec<_>>()
        )
    })?;
    let mut env_vars = Vec::new();

    // Goose uses an env var for the system prompt file
    if let Some(ref env_name) = harness_config.agent_file_env
        && !harness_config.supports_agent_file
        && let Some(path) = agent_file_path
    {
        env_vars.push((env_name.clone(), path.to_string_lossy().to_string()));
    }

    Ok(env_vars)
}

/// Refuse to start the planner when a `ralph run` is in progress on this
/// project.
///
/// The planner mutates plan/step rows the executor is about to read, so
/// running them concurrently can corrupt plan state (steps reordered or
/// deleted out from under the executor). The check is strict by default:
/// if a `run_locks` row exists for `project`, we bail — even if the
/// recorded pid is no longer alive. Liveness probing lives in
/// `ralph run --force` (reclaim path) and `ralph cancel` (clears dead
/// locks); duplicating that heuristic here would invite split-brain
/// behavior where the planner and executor disagree about whether a run
/// is live.
///
/// The bail message names the escape hatches so a user who hits a stale
/// row knows exactly how to recover.
pub fn preflight_no_live_run(conn: &Connection, project: &str) -> Result<()> {
    let live = storage::get_live_run(conn, project)?;
    if let Some(lr) = live {
        let plan_label = lr
            .plan_slug
            .as_deref()
            .map(|s| format!("plan {s}"))
            .unwrap_or_else(|| "<all plans>".to_string());
        bail!(
            "`ralph run` is active in this project (pid {pid}, {plan_label}, started {started_at}).\n\
             Refusing to start the planner while a run is in progress.\n\n\
             Cancel the run with `ralph cancel` first, or wait for it to finish.",
            pid = lr.pid,
            plan_label = plan_label,
            started_at = lr.started_at,
        );
    }
    Ok(())
}

/// Run the interactive plan-harness: spawn a harness with the plan agent definition
/// and wait for it to exit.
///
/// Returns the harness exit code.
pub async fn run_plan_harness(
    config: &Config,
    harness_name: &str,
    project: &str,
    description: Option<&str>,
    plan_slug: Option<&str>,
) -> Result<i32> {
    let harness_config = config.harnesses.get(harness_name).with_context(|| {
        format!(
            "Unknown harness '{harness_name}'. Available: {:?}",
            config.harnesses.keys().collect::<Vec<_>>()
        )
    })?;

    // Build the plan agent content, injecting the list of hooks applicable
    // to the current project so the harness can reference them by name.
    let project_path = std::path::Path::new(project);
    let hooks = hook_library::load_all()?;
    let applicable = hook_library::filter_by_project(hooks, project_path);
    let agent_content = render_plan_agent(&applicable);

    // Write the agent definition to a temporary file.
    // This file lives for the duration of the harness process.
    let agent_temp_file = write_agent_temp_file(&agent_content)?;
    let agent_file_path = agent_temp_file.path();

    // Build the initial prompt
    let prompt = build_initial_prompt(project, description, plan_slug);

    // Build per-harness args and env
    let args = build_plan_harness_args(
        harness_name,
        config,
        Some(agent_file_path),
        &agent_content,
        &prompt,
    )?;
    let env_vars = build_plan_harness_env(harness_name, config, Some(agent_file_path))?;

    // Spawn the harness interactively
    let cwd = std::path::Path::new(project);
    let mut child = harness::spawn_harness_interactive(harness_config, &args, &env_vars, cwd)
        .await
        .with_context(|| format!("Failed to spawn plan-harness '{harness_name}'"))?;

    // Wait for the harness to exit
    let status = child
        .wait()
        .await
        .context("Failed to wait for plan-harness process")?;

    // The temp file is cleaned up when agent_temp_file is dropped
    Ok(status.code().unwrap_or(1))
}

/// A temporary file that is cleaned up on drop.
pub struct TempAgentFile {
    path: PathBuf,
}

impl TempAgentFile {
    /// Returns the path to the temporary agent file.
    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempAgentFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

/// Write the given agent definition content to a temporary file. The filename
/// combines the process id with a uuid to prevent collisions between
/// concurrent callers in the same process (notably parallel tests).
fn write_agent_temp_file(content: &str) -> Result<TempAgentFile> {
    let file_name = format!(
        "ralph-rs-plan-agent-{}-{}.md",
        std::process::id(),
        uuid::Uuid::new_v4(),
    );
    let path = std::env::temp_dir().join(file_name);

    let mut file = std::fs::File::create(&path).with_context(|| {
        format!(
            "Failed to create temporary agent file at {}",
            path.display()
        )
    })?;
    file.write_all(content.as_bytes())
        .context("Failed to write agent definition to temp file")?;
    file.flush().context("Failed to flush agent temp file")?;

    Ok(TempAgentFile { path })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn test_build_initial_prompt_with_description() {
        let prompt = build_initial_prompt("/tmp/project", Some("Add authentication"), None);
        assert!(prompt.contains("/tmp/project"));
        assert!(prompt.contains("Add authentication"));
        assert!(prompt.starts_with("Create a ralph plan"));
    }

    #[test]
    fn test_build_initial_prompt_without_description() {
        let prompt = build_initial_prompt("/tmp/project", None, None);
        assert!(prompt.contains("/tmp/project"));
        assert!(prompt.starts_with("Help me create or update"));
    }

    #[test]
    fn test_build_initial_prompt_with_plan_slug_and_description() {
        // Regression: `ralph plan harness generate <desc> my-plan` used to
        // discard the slug. The prompt must now name the target plan so the
        // harness updates it instead of touching the active plan.
        let prompt = build_initial_prompt("/tmp/project", Some("Wire up the API"), Some("my-plan"));
        assert!(prompt.contains("/tmp/project"));
        assert!(prompt.contains("Wire up the API"));
        assert!(prompt.contains("'my-plan'"));
        assert!(prompt.starts_with("Update the ralph plan"));
    }

    #[test]
    fn test_build_initial_prompt_with_plan_slug_only() {
        let prompt = build_initial_prompt("/tmp/project", None, Some("my-plan"));
        assert!(prompt.contains("/tmp/project"));
        assert!(prompt.contains("'my-plan'"));
        assert!(prompt.starts_with("Help me update the ralph plan"));
    }

    fn test_agent_content() -> String {
        render_plan_agent(&[])
    }

    #[test]
    fn test_build_plan_harness_args_claude() {
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "claude",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        )
        .unwrap();

        // Claude should get --system-prompt-file and the prompt as separate args
        assert!(args.contains(&"--system-prompt-file".to_string()));
        assert!(args.contains(&"Create a plan".to_string()));
        // Agent content should NOT be in the prompt
        assert!(!args.iter().any(|a| a.contains("ralph Plan Agent")));
        // {agent_file} placeholder should have been substituted with the real path
        let path_str = agent_file.path().to_string_lossy().into_owned();
        assert!(args.contains(&path_str));
        assert!(!args.iter().any(|a| a.contains("{agent_file}")));
    }

    #[test]
    fn test_build_plan_harness_args_prompt_containing_agent_file_token() {
        // Regression test mirroring harness::build_harness_args: if the
        // prompt text contains the literal string `{agent_file}` (e.g. a
        // plan description discussing the placeholder system itself), the
        // no-agent removal pass must run BEFORE prompt substitution so
        // the preceding flag (here, claude's `--system-prompt-file` or a
        // harness's `-p` / `--prompt`) is not stripped along with the
        // prompt arg.
        let config = Config::default();
        let agent_content = test_agent_content();
        let prompt_with_placeholder =
            "Plan that discusses the {agent_file} placeholder collision bug.";

        // No agent file: hits the remove_agent_file_args branch for
        // any harness whose plan_args contain a `{agent_file}` token.
        let claude_args = build_plan_harness_args(
            "claude",
            &config,
            None,
            &agent_content,
            prompt_with_placeholder,
        )
        .unwrap();
        // Claude's plan_args include `--permission-mode bypassPermissions`
        // which must survive unconditionally — and the prompt itself must
        // appear verbatim with its literal `{agent_file}` intact.
        assert!(
            claude_args.iter().any(|a| a == "--permission-mode"),
            "--permission-mode was stripped; got args: {claude_args:?}"
        );
        assert!(
            claude_args.iter().any(|a| a == "bypassPermissions"),
            "bypassPermissions was stripped; got args: {claude_args:?}"
        );
        assert!(
            claude_args.iter().any(|a| a == prompt_with_placeholder),
            "prompt was stripped or mangled; got args: {claude_args:?}"
        );
        // No residual `{agent_file}` token in args (outside the prompt).
        // Every arg equal to just "{agent_file}" on its own would indicate
        // a failed removal; the prompt-as-a-whole containing the substring
        // is fine and expected.
        assert!(
            !claude_args.iter().any(|a| a == "{agent_file}"),
            "raw {{agent_file}} token leaked through; got args: {claude_args:?}"
        );
    }

    #[test]
    fn test_build_plan_harness_args_copilot_uses_interactive_flag() {
        // The whole point of the plan_args template: copilot's run-mode -p
        // is one-shot and rejects a seeded positional prompt, so plan-harness
        // mode must invoke it with -i instead.
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "copilot",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Plan this",
        )
        .unwrap();

        assert!(
            args.contains(&"-i".to_string()),
            "copilot plan_args must use -i, got: {args:?}"
        );
        assert!(
            !args.contains(&"-p".to_string()),
            "copilot plan_args must NOT use -p (one-shot, non-interactive): {args:?}"
        );
        assert!(args.contains(&"--allow-all".to_string()));
        assert!(args.contains(&"--allow-all-paths".to_string()));
        // The prompt (with prepended agent content) should still be present.
        assert!(args.iter().any(|a| a.contains("Plan this")));
        assert!(args.iter().any(|a| a.contains("ralph Plan Agent")));
    }

    #[test]
    fn test_build_plan_harness_args_legacy_empty_template() {
        // A user config that predates plan_args ships an empty Vec. The
        // builder must fall back to the pre-template behavior (claude gets
        // --system-prompt-file + prompt; everything else gets the prepended
        // prompt as a bare positional).
        let mut config = Config::default();
        for harness in config.harnesses.values_mut() {
            harness.plan_args.clear();
        }
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();

        let claude_args = build_plan_harness_args(
            "claude",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Plan",
        )
        .unwrap();
        assert_eq!(claude_args[0], "--system-prompt-file");
        assert_eq!(claude_args[1], agent_file.path().to_string_lossy());
        assert_eq!(claude_args[2], "Plan");
        assert_eq!(claude_args.len(), 3);

        let codex_args = build_plan_harness_args(
            "codex",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Plan",
        )
        .unwrap();
        assert_eq!(codex_args.len(), 1);
        assert!(codex_args[0].contains("ralph Plan Agent"));
        assert!(codex_args[0].contains("Plan"));
    }

    #[test]
    fn test_build_plan_harness_args_codex_prepends_agent() {
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "codex",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        )
        .unwrap();

        // Codex doesn't support agent files, so agent content should be prepended
        assert!(!args.iter().any(|a| a == "--system-prompt-file"));
        assert!(args.iter().any(|a| a.contains("ralph Plan Agent")));
        assert!(args.iter().any(|a| a.contains("Create a plan")));
    }

    #[test]
    fn test_build_plan_harness_args_pi_prepends_agent() {
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "pi",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Help me plan",
        )
        .unwrap();

        assert!(!args.iter().any(|a| a == "--system-prompt-file"));
        assert!(args.iter().any(|a| a.contains("ralph Plan Agent")));
        assert!(args.iter().any(|a| a.contains("Help me plan")));
    }

    #[test]
    fn test_build_plan_harness_args_goose_does_not_inline_agent() {
        // Goose loads the agent definition via GOOSE_SYSTEM_PROMPT_FILE_PATH,
        // which fully replaces the default system prompt. If we ALSO inline
        // the definition into the -t prompt (as the old behavior did), goose
        // sees it twice: once as system, once as user. Pin the fix so any
        // future change to the prepend logic has to re-justify itself.
        let config = Config::default();
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "goose",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Help me plan",
        )
        .unwrap();

        assert!(
            args.iter().any(|a| a.contains("Help me plan")),
            "user prompt missing from argv: {args:?}"
        );
        assert!(
            !args.iter().any(|a| a.contains("ralph Plan Agent")),
            "agent definition was inlined into goose argv (double-load bug): {args:?}"
        );
    }

    #[test]
    fn test_build_plan_harness_env_goose() {
        // Goose's default config sets `agent_file_env` to
        // GOOSE_SYSTEM_PROMPT_FILE_PATH, so an agent file should be exported
        // as that env var to the subprocess.
        let config = Config::default();
        let agent_file = write_agent_temp_file(&test_agent_content()).unwrap();
        let env = build_plan_harness_env("goose", &config, Some(agent_file.path())).unwrap();

        assert_eq!(env.len(), 1);
        assert_eq!(env[0].0, "GOOSE_SYSTEM_PROMPT_FILE_PATH");
        assert_eq!(env[0].1, agent_file.path().to_string_lossy());
    }

    #[test]
    fn test_build_plan_harness_env_goose_no_agent_file() {
        // With no agent file, nothing should be exported even if the env
        // var is configured.
        let config = Config::default();
        let env = build_plan_harness_env("goose", &config, None).unwrap();
        assert!(env.is_empty());
    }

    #[test]
    fn test_build_plan_harness_env_claude_no_env() {
        let config = Config::default();
        let agent_file = write_agent_temp_file(&test_agent_content()).unwrap();
        let env = build_plan_harness_env("claude", &config, Some(agent_file.path())).unwrap();

        // Claude supports agent file natively, so env var should NOT be set
        assert!(env.is_empty());
    }

    #[test]
    fn test_build_plan_harness_args_unknown_harness_returns_err() {
        // Regression: indexing into config.harnesses with an unknown key
        // used to panic. An unknown harness name must surface as a
        // descriptive Err that names the missing harness and lists the
        // available ones, not a process abort.
        let config = Config::default();
        let agent_content = test_agent_content();
        let result =
            build_plan_harness_args("does-not-exist", &config, None, &agent_content, "Plan");
        let err = result.expect_err("expected Err for unknown harness");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("does-not-exist"),
            "error should name the missing harness: {msg}"
        );
    }

    #[test]
    fn test_build_plan_harness_env_unknown_harness_returns_err() {
        let config = Config::default();
        let result = build_plan_harness_env("does-not-exist", &config, None);
        let err = result.expect_err("expected Err for unknown harness");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("does-not-exist"),
            "error should name the missing harness: {msg}"
        );
    }

    #[test]
    fn test_write_agent_temp_file() {
        let content = test_agent_content();
        let agent_file = write_agent_temp_file(&content).unwrap();
        let read = std::fs::read_to_string(agent_file.path()).unwrap();
        assert!(read.contains("ralph Plan Agent"));
        assert!(read.contains("ralph plan create"));
        assert!(read.contains("ralph step add"));
    }

    #[test]
    fn test_harness_plan_agent_content() {
        // Verify the rendered agent definition has key sections.
        let content = render_plan_agent(&[]);
        assert!(content.contains("Plan Management"));
        assert!(content.contains("Step Management"));
        assert!(content.contains("Hook Attachment"));
        assert!(content.contains("Workflow"));
        assert!(content.contains("Guidelines"));
        assert!(content.contains("ralph plan create"));
        assert!(content.contains("ralph step add"));
        assert!(content.contains("ralph plan approve"));
        assert!(content.contains("ralph step set-hook"));
        assert!(content.contains("Available Hooks"));
    }

    #[test]
    fn test_render_plan_agent_lists_hooks() {
        use crate::hook_library::{Hook, Lifecycle, Scope};
        let hooks = vec![
            Hook {
                name: "claude-review".to_string(),
                description: "Review with Claude".to_string(),
                lifecycle: Lifecycle::PostStep,
                scope: Scope::Global,
                command: "claude -p 'review'".to_string(),
            },
            Hook {
                name: "rust-clippy".to_string(),
                description: String::new(),
                lifecycle: Lifecycle::PostStep,
                scope: Scope::Paths {
                    paths: vec![std::path::PathBuf::from("/home/me/rust")],
                },
                command: "cargo clippy".to_string(),
            },
        ];
        let content = render_plan_agent(&hooks);
        assert!(content.contains("**claude-review**"));
        assert!(content.contains("Review with Claude"));
        assert!(content.contains("**rust-clippy**"));
        assert!(content.contains("/home/me/rust"));
    }

    #[test]
    fn test_render_plan_agent_no_hooks_message() {
        let content = render_plan_agent(&[]);
        assert!(content.contains("No hooks are currently available"));
        assert!(content.contains("ralph hooks add"));
    }

    /// Synthetic harness config builder used by the substring-replacement
    /// tests below. `HarnessConfig` doesn't derive `Default`, so we spell out
    /// all fields here to keep the tests independent of `Config::default()`.
    fn synthetic_harness(plan_args: Vec<String>, supports_agent_file: bool) -> Config {
        use crate::config::HarnessConfig;
        use std::collections::HashMap;
        let mut harnesses = HashMap::new();
        harnesses.insert(
            "synth".to_string(),
            HarnessConfig {
                command: "synth".to_string(),
                args: vec![],
                plan_args,
                supports_agent_file,
                supports_json_output: false,
                json_output_args: vec![],
                agent_file_env: None,
                agent_file_args: vec![],
                model_args: vec![],
                default_model: None,
                auth_env_vars: vec![],
                auth_probe_args: vec![],
                prompt_input: crate::config::PromptInputMode::Stdin,
                argv_overflow: crate::config::ArgvOverflowBehavior::SpillToTempFile,
                color: None,
            },
        );
        Config {
            default_harness: "synth".to_string(),
            max_retries_per_step: 0,
            timeout_secs: None,
            hook_timeout_secs: 120,
            auto_stash: true,
            prompt: None,
            min_free_disk_mb: 1024,
            display_timezone: "UTC".to_string(),
            harness_chunk_max_bytes: 4096,
            review: crate::config::ReviewConfig::default(),
            harnesses,
        }
    }

    #[test]
    fn test_plan_harness_args_substring_replacement() {
        // A plan_args template that embeds {prompt} inside a larger token
        // (e.g. `--prompt={prompt}`) must be substring-substituted, not
        // matched as a whole token — otherwise users copying run-mode
        // patterns into plan_args get the literal placeholder in argv.
        let config = synthetic_harness(vec!["--prompt={prompt}".to_string()], false);
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "synth",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        )
        .unwrap();

        assert_eq!(args.len(), 1, "expected a single arg, got {args:?}");
        // Because supports_agent_file=false, the agent content is prepended
        // to the effective prompt — so the arg begins with "--prompt=" and
        // contains both the agent content and the original prompt.
        assert!(args[0].starts_with("--prompt="));
        assert!(args[0].contains("Create a plan"));
        assert!(
            !args[0].contains("{prompt}"),
            "literal placeholder leaked into argv: {args:?}"
        );
    }

    #[test]
    fn test_plan_harness_args_agent_file_substring() {
        // Same story for {agent_file}: a token like `--agent-file={agent_file}`
        // must substring-substitute to the real path, not leak the literal.
        let config = synthetic_harness(
            vec![
                "--agent-file={agent_file}".to_string(),
                "{prompt}".to_string(),
            ],
            true,
        );
        let agent_content = test_agent_content();
        let agent_file = write_agent_temp_file(&agent_content).unwrap();
        let args = build_plan_harness_args(
            "synth",
            &config,
            Some(agent_file.path()),
            &agent_content,
            "Create a plan",
        )
        .unwrap();

        let path_str = agent_file.path().to_string_lossy().into_owned();
        assert_eq!(args.len(), 2, "expected two args, got {args:?}");
        assert!(args[0].starts_with("--agent-file="));
        assert!(
            args[0].contains(&path_str),
            "expected path substring in first arg: {args:?}"
        );
        assert!(
            !args[0].contains("{agent_file}"),
            "literal {{agent_file}} leaked into argv: {args:?}"
        );
        assert_eq!(args[1], "Create a plan");
    }

    // -- preflight_no_live_run tests --

    fn setup_conn() -> Connection {
        crate::db::open_memory().expect("open_memory")
    }

    #[test]
    fn test_planner_refuses_when_run_lock_present() {
        // A live `ralph run` row for this project must block the planner.
        // The bail message should explain the situation in terms the user
        // can act on.
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO run_locks (project, pid, plan_id, plan_slug) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["/proj-live", 4242i64, "plan-id-x", "my-plan"],
        )
        .unwrap();

        let err = preflight_no_live_run(&conn, "/proj-live")
            .expect_err("expected Err when a run lock row exists");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("active") || msg.contains("Refusing"),
            "bail message should signal the run is active; got: {msg}"
        );
        // Observability: pid and plan slug should surface so the user can
        // identify the blocking run.
        assert!(msg.contains("4242"), "pid missing from message: {msg}");
        assert!(
            msg.contains("my-plan"),
            "plan slug missing from message: {msg}"
        );
        // The escape hatches must be named.
        assert!(
            msg.contains("ralph cancel"),
            "escape-hatch hint missing: {msg}"
        );
    }

    #[test]
    fn test_planner_proceeds_when_no_run_lock() {
        // No row → preflight returns Ok, so the caller proceeds to spawn
        // the planner harness. We don't drive the full planner path here;
        // the preflight function is the unit under test.
        let conn = setup_conn();
        let res = preflight_no_live_run(&conn, "/proj-empty");
        assert!(
            res.is_ok(),
            "expected Ok when no run_locks row present, got: {res:?}"
        );
    }

    #[test]
    fn test_planner_refuses_even_when_plan_slug_is_null() {
        // A run row without a plan_slug (e.g. `ralph run --all`) still
        // blocks the planner. The bail message should fall back to the
        // "<all plans>" label instead of omitting the plan field entirely.
        let conn = setup_conn();
        conn.execute(
            "INSERT INTO run_locks (project, pid) VALUES (?1, ?2)",
            rusqlite::params!["/proj-all", 7777i64],
        )
        .unwrap();

        let err = preflight_no_live_run(&conn, "/proj-all")
            .expect_err("expected Err when a run lock row exists");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("<all plans>"),
            "missing all-plans label: {msg}"
        );
        assert!(msg.contains("7777"), "missing pid: {msg}");
    }
}
