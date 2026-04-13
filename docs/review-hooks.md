# Post-Step Review with Agent Harnesses

Ralph has no built-in review agent. Instead, it ships a **lifecycle hook system** that lets you invoke any external agent harness (Claude Code, Codex, Goose, etc.) — or any shell command — at well-defined points during plan execution. The `post-step` lifecycle is the natural integration point for review.

Unlike traditional filesystem hooks, ralph stores hooks in two places:

1. **The hook library** — `~/.config/ralph-rs/hooks/*.md` — a shared, reusable catalog of hook definitions with scope metadata. One file per hook, edited by hand or via `ralph hooks add`. Nothing in your working directory.
2. **Per-plan / per-step associations** — rows in ralph's SQLite database that link a plan or step to a hook by name. These are created by you (`ralph plan set-hook`) or by the plan agent while it builds a plan.

This separation means your project directory stays clean, hooks are reusable across plans and projects, and a plan agent can pick from a curated library instead of inventing arbitrary shell commands.

## The Hook Library

Each hook is a markdown file with frontmatter and a shell command body:

```markdown
---
name: claude-review
description: Review completed steps with Claude Code
lifecycle: post-step
scope: global
---
claude -p "Review the diff: $(git diff HEAD~1)" --allowedTools Write Read Glob Grep
```

### Frontmatter fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | optional | Hook name. Defaults to the filename stem. Used when attaching via `--hook <name>`. |
| `description` | optional | One-line description shown in `ralph hooks list`. |
| `lifecycle` | **required** | One of `pre-step`, `post-step`, `pre-test`, `post-test`. |
| `scope` | optional | `global` (default) or a `paths:` list of absolute path prefixes. |

### Scope

A hook's scope controls which projects can use it:

- **`scope: global`** — applies anywhere. Use for harness invocations, linters, and generic review commands.
- **`scope: paths: [...]`** — applies only when the project directory starts with one of the listed prefixes. Paths must be **absolute** (no `~` expansion). Use for project-specific review commands you don't want leaking into unrelated projects.

```markdown
---
name: rust-clippy
lifecycle: post-step
scope:
  paths:
    - /home/me/projects/rust
    - /home/me/work/backend
---
cargo clippy -- -D warnings
```

### Managing the library

```bash
ralph hooks list              # hooks applicable to the current project
ralph hooks list --all        # every hook in the library
ralph hooks show <name>       # show a hook's full definition
ralph hooks add <name> \
    --lifecycle post-step \
    --command "claude -p 'review'" \
    [--description "..."] \
    [--scope-path /abs/path]... \
    [--force]
ralph hooks remove <name>
```

## Attaching Hooks to a Plan

A hook in the library doesn't do anything until it's attached to a plan or step. Attachments are stored in ralph's database:

```bash
# Attach a hook to every step in a plan ("plan-wide")
ralph plan set-hook <slug> --lifecycle post-step --hook claude-review

# Attach a hook to a single step
ralph step set-hook 3 --plan <slug> --lifecycle post-step --hook claude-review

# Detach
ralph plan unset-hook <slug> --lifecycle post-step --hook claude-review
ralph step unset-hook 3 --plan <slug> --lifecycle post-step --hook claude-review

# See what's attached
ralph plan hooks <slug>
```

When ralph executes a step, it looks up every hook associated with that step at each lifecycle event — plan-wide hooks first, then per-step hooks. Both fire (additive, not override).

## The Plan Agent and Hooks

When you run `ralph plan:harness` (a harness-driven planning session), ralph injects the list of applicable hooks (filtered by the current project's path scope) into the plan agent's system prompt. The agent can reference hooks **by name** but cannot invent new shell commands — if it wants a hook that doesn't exist, it has to ask you to create one first.

A well-designed plan agent will:

1. Read the "Available Hooks" section at the bottom of its agent definition.
2. Identify steps that would benefit from post-step review (risky diffs, subtle acceptance criteria, security-sensitive changes).
3. Attach the appropriate hook via `ralph step set-hook` or `ralph plan set-hook`.

This keeps review behavior declarative and auditable: looking at `ralph plan hooks <slug>` tells you exactly what will run and when.

## Sharing Hooks with a Teammate

Hooks live in your user config, not in the repo. To share them with a teammate, export to a JSON bundle:

```bash
# Export hooks applicable to the current project
ralph hooks export -o my-hooks.json

# Export every hook in your library
ralph hooks export --all -o all-hooks.json

# Export hooks applicable to a specific path (useful if you're not
# currently in that directory)
ralph hooks export --path /home/me/projects/rust -o rust-hooks.json
```

Your teammate imports:

```bash
ralph hooks import my-hooks.json
```

**Collision policy:** by default, import **errors out** if any hook in the bundle already exists in the teammate's library — they won't silently overwrite their own work. Pass `--force` to overwrite on purpose.

**Missing-hook policy at runtime:** if a plan references a hook by name that isn't in the local library (e.g., teammate hasn't imported the bundle yet), ralph **warns and skips** at run time. The plan still runs, just without that hook. Import the bundle and the hooks will fire on subsequent runs.

## Hook Environment Variables

Every hook receives these environment variables:

| Variable | Description |
|----------|-------------|
| `RALPH_PLAN_SLUG` | Plan slug (e.g., `add-auth`) |
| `RALPH_PLAN_ID` | Plan UUID |
| `RALPH_STEP_TITLE` | Current step title |
| `RALPH_STEP_ID` | Step UUID |
| `RALPH_STEP_ATTEMPT` | Attempt number (1-based) |
| `RALPH_PROJECT_DIR` | Absolute path to the project |
| `RALPH_HOOK_NAME` | The hook's library name (useful for logging) |

Lifecycle-specific:

| Variable | Lifecycle | Values |
|----------|-----------|--------|
| `RALPH_STEP_STATUS` | `post-step` | `complete`, `failed`, `timeout`, `aborted` |
| `RALPH_TEST_PASSED` | `post-test` | `true`, `false` |

## What Context Can Review Hooks Use?

A review hook typically wants three things: the diff, the plan context, and the step metadata. All three are already available from the hook's working directory (the project) and from ralph itself:

| Source | What you get |
|--------|--------------|
| `git diff HEAD~1` | The diff the just-completed step produced |
| `git diff main...HEAD` | Cumulative diff for the whole plan so far |
| `ralph log --plan $RALPH_PLAN_SLUG --step N --full` | Per-step execution log: attempts, duration, test results, commit hash, cost/tokens, harness stdout/stderr |
| `ralph status --plan $RALPH_PLAN_SLUG --verbose` | Plan progress and per-step state |
| `ralph export $RALPH_PLAN_SLUG` | Full plan JSON: descriptions, acceptance criteria, deterministic tests |

## Example Hooks

### Claude Code review

```markdown
---
name: claude-review
description: Review completed steps with Claude Code
lifecycle: post-step
scope: global
---
[ "$RALPH_STEP_STATUS" = "complete" ] || exit 0

DIFF=$(git diff HEAD~1)
PLAN=$(ralph export "$RALPH_PLAN_SLUG" 2>/dev/null)

mkdir -p .ralph-review

claude -p "Review step '$RALPH_STEP_TITLE' of plan '$RALPH_PLAN_SLUG'.

## Diff
$DIFF

## Full Plan
$PLAN

Check correctness against the step's acceptance criteria, flag regressions and quality issues.
Write findings to .ralph-review/${RALPH_PLAN_SLUG}-step-${RALPH_STEP_ID}.md" \
  --allowedTools Write Read Glob Grep
```

Attach it to a plan:

```bash
ralph plan set-hook my-feature --lifecycle post-step --hook claude-review
```

### Codex review (path-scoped)

```markdown
---
name: codex-review-backend
description: Codex review for backend service steps
lifecycle: post-step
scope:
  paths:
    - /home/me/work/backend
---
[ "$RALPH_STEP_STATUS" = "complete" ] || exit 0

DIFF=$(git diff HEAD~1)
mkdir -p .ralph-review
codex -q --approval-mode full-auto \
  "Review this diff for step '$RALPH_STEP_TITLE'. Flag correctness issues, regressions, or quality problems. Write findings to .ralph-review/${RALPH_PLAN_SLUG}-step-${RALPH_STEP_ID}.md

$DIFF"
```

### Lightweight non-LLM review

```markdown
---
name: rust-clippy-check
description: Run clippy on changed Rust files
lifecycle: post-step
scope: global
---
[ "$RALPH_STEP_STATUS" = "complete" ] || exit 0

CHANGED=$(git diff --name-only HEAD~1 -- '*.rs')
if [ -n "$CHANGED" ]; then
  mkdir -p .ralph-review
  cargo clippy -- -D warnings > .ralph-review/clippy-${RALPH_STEP_ID}.txt 2>&1
fi
```

### Blocking review via pre-step gate

Post-step failures are logged as warnings but don't block execution. If you want a review that can halt the plan, have the review write a `BLOCKING` marker and check for it in a `pre-step` hook on the following step:

```markdown
---
name: block-on-unresolved-review
description: Halt the next step if the previous review flagged BLOCKING findings
lifecycle: pre-step
scope: global
---
REVIEW_DIR=".ralph-review"
for f in "$REVIEW_DIR"/*.md; do
  [ -f "$f" ] || continue
  if grep -q "BLOCKING" "$f"; then
    echo "Blocking review finding in $f — resolve before continuing"
    exit 1
  fi
done
```

A non-zero exit from `pre-step` fails the current attempt; ralph retries up to `max_retries` then marks the step failed, giving you a chance to address the finding.

## Tips

- Add `.ralph-review/` to `.gitignore` — review output is ephemeral, not part of the codebase.
- `ralph log --full` truncates stdout/stderr at 50 lines in the terminal. For the complete output, read the SQLite database at `~/.local/share/ralph-rs/ralph.db` (Linux) or `~/Library/Application Support/ralph-rs/ralph.db` (macOS).
- The `ralph export` JSON includes acceptance criteria for every step — the most useful reference when checking whether a diff actually satisfies the step.
- If your review harness is slow, gate it on `RALPH_STEP_ATTEMPT` or `RALPH_STEP_STATUS = complete` so you're not reviewing intermediate failures that ralph will retry anyway.
- When a plan agent attaches hooks to a plan, run `ralph plan hooks <slug>` before approving to confirm what will fire.
