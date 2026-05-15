// Prompt commands.
//
// The four-layer prompt model has ONE content blob per layer. Two scopes
// share a single CLI noun (`ralph prompt ...`): the Global layer lives in
// config.json, the Project layer lives in `project_settings`. Both read/write
// paths share the same `PromptScope` enum dispatched here.

use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::cli::PromptScope;
use crate::config::{self, Config};
use crate::output::{self, OutputContext, OutputFormat};
use crate::storage;

/// Serializable view of a single scope's prompt for JSON output.
#[derive(Debug, serde::Serialize)]
struct ScopeView<'a> {
    scope: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt: Option<&'a str>,
}

/// Composed (fully-layered) prompt for `--resolved` output.
#[derive(Debug, serde::Serialize)]
struct ResolvedView {
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt: Option<String>,
}

/// `ralph prompt show` — display configured prompts.
pub fn cmd_prompt_show(
    conn: &Connection,
    config: &Config,
    project: &str,
    scope: Option<PromptScope>,
    resolved: bool,
    out: &OutputContext,
) -> Result<()> {
    let project_settings = storage::get_project_settings(conn, project)?;

    if resolved {
        // Compose exactly how build_step_prompt stacks the layers, but
        // without a step body — the user sees the leading text a harness
        // receives. There's no plan in this command's context, so only the
        // global and project layers participate.
        let composed = join_layers([config.prompt.as_deref(), project_settings.prompt.as_deref()]);
        let view = ResolvedView { prompt: composed };
        if out.format == OutputFormat::Json {
            println!("{}", serde_json::to_string(&view)?);
        } else {
            match &view.prompt {
                Some(p) => println!("prompt:\n{}", indent(p, "  ")),
                None => println!("prompt: <none>"),
            }
        }
        return Ok(());
    }

    let all_views = [
        ScopeView {
            scope: "global",
            prompt: config.prompt.as_deref(),
        },
        ScopeView {
            scope: "project",
            prompt: project_settings.prompt.as_deref(),
        },
    ];

    let filtered: Vec<&ScopeView<'_>> = match scope {
        None => all_views.iter().collect(),
        Some(s) => all_views
            .iter()
            .filter(|v| v.scope == scope_name(s))
            .collect(),
    };

    if out.format == OutputFormat::Json {
        println!("{}", serde_json::to_string(&filtered)?);
    } else {
        for view in &filtered {
            print_scope_plain(view);
        }
    }
    Ok(())
}

/// `ralph prompt set` — replace the prompt at one scope. An empty string
/// clears it (stored as "unset" so the layer contributes nothing).
pub fn cmd_prompt_set(
    conn: &Connection,
    config_path: &std::path::Path,
    project: &str,
    scope: PromptScope,
    content: &str,
    out: &OutputContext,
) -> Result<()> {
    let value = if content.is_empty() {
        None
    } else {
        Some(content)
    };

    match scope {
        PromptScope::Global => {
            // Load from disk (not the preloaded `Config`) so we only rewrite
            // the field we own — preserving any manual edits the user made
            // between this process starting and the set call.
            let mut cfg = config::load_or_create_config()?;
            cfg.prompt = value.map(str::to_string);
            write_config(&cfg, config_path)?;
        }
        PromptScope::Project => {
            storage::set_project_prompt(conn, project, value)?;
        }
    }

    if !out.quiet {
        let icon = output::check_icon(out.color);
        let verb = if value.is_some() { "Updated" } else { "Cleared" };
        eprintln!("{icon} {verb} {} prompt", scope_name(scope));
    }
    Ok(())
}

/// `ralph prompt clear` — null out the prompt at one scope.
pub fn cmd_prompt_clear(
    conn: &Connection,
    config_path: &std::path::Path,
    project: &str,
    scope: PromptScope,
    out: &OutputContext,
) -> Result<()> {
    match scope {
        PromptScope::Global => {
            let mut cfg = config::load_or_create_config()?;
            cfg.prompt = None;
            write_config(&cfg, config_path)?;
        }
        PromptScope::Project => {
            storage::set_project_prompt(conn, project, None)?;
        }
    }

    if !out.quiet {
        let icon = output::check_icon(out.color);
        eprintln!("{icon} Cleared {} prompt", scope_name(scope));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn scope_name(s: PromptScope) -> &'static str {
    match s {
        PromptScope::Global => "global",
        PromptScope::Project => "project",
    }
}

fn write_config(cfg: &Config, path: &std::path::Path) -> Result<()> {
    let json = serde_json::to_string_pretty(cfg)?;
    std::fs::write(path, json)
        .with_context(|| format!("Failed to write config to {}", path.display()))?;
    Ok(())
}

fn print_scope_plain(view: &ScopeView<'_>) {
    println!("[{}]", view.scope);
    match view.prompt {
        Some(p) => println!("  prompt:\n{}", indent(p, "    ")),
        None => println!("  prompt: <unset>"),
    }
    println!();
}

fn join_layers<const N: usize>(layers: [Option<&str>; N]) -> Option<String> {
    let pieces: Vec<&str> = layers
        .into_iter()
        .filter_map(|s| s.filter(|v| !v.is_empty()))
        .collect();
    if pieces.is_empty() {
        None
    } else {
        Some(pieces.join("\n\n"))
    }
}

fn indent(text: &str, prefix: &str) -> String {
    text.lines()
        .map(|l| format!("{prefix}{l}"))
        .collect::<Vec<_>>()
        .join("\n")
}
