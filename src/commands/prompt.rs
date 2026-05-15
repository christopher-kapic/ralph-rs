// Prompt prefix/suffix commands.
//
// Two scopes share a single CLI noun (`ralph prompt ...`): global lives in
// config.json, project lives in `project_settings`. Both read/write paths
// share the same `PromptScope` enum dispatched here.

use anyhow::{Context, Result, bail};
use rusqlite::Connection;

use crate::cli::PromptScope;
use crate::config::{self, Config};
use crate::output::{self, OutputContext, OutputFormat};
use crate::prompt::{PromptWrap, PromptWraps};
use crate::storage;

/// Serializable view of a single scope's prefix/suffix pair for JSON output.
#[derive(Debug, serde::Serialize)]
struct ScopeView<'a> {
    scope: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    prefix: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    suffix: Option<&'a str>,
}

/// Composed (fully-layered) wrap for `--resolved` output.
#[derive(Debug, serde::Serialize)]
struct ResolvedView {
    #[serde(skip_serializing_if = "Option::is_none")]
    prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    suffix: Option<String>,
}

/// `ralph prompt show` — display configured prompt wraps.
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
        let wraps = PromptWraps {
            global: PromptWrap::from_opts(
                config.prompt_prefix.as_ref(),
                config.prompt_suffix.as_ref(),
            ),
            project: PromptWrap::from_opts(
                project_settings.prompt_prefix.as_ref(),
                project_settings.prompt_suffix.as_ref(),
            ),
            plan: PromptWrap::default(),
        };
        return print_resolved(&wraps, out);
    }

    let all_views = [
        ScopeView {
            scope: "global",
            prefix: config.prompt_prefix.as_deref(),
            suffix: config.prompt_suffix.as_deref(),
        },
        ScopeView {
            scope: "project",
            prefix: project_settings.prompt_prefix.as_deref(),
            suffix: project_settings.prompt_suffix.as_deref(),
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

/// `ralph prompt set` — upsert prefix and/or suffix at one scope.
pub fn cmd_prompt_set(
    conn: &Connection,
    config_path: &std::path::Path,
    project: &str,
    scope: PromptScope,
    prefix: Option<&str>,
    suffix: Option<&str>,
    out: &OutputContext,
) -> Result<()> {
    if prefix.is_none() && suffix.is_none() {
        bail!("Provide at least one of --prefix / --suffix");
    }

    match scope {
        PromptScope::Global => {
            // Load from disk (not the preloaded `Config`) so we only rewrite
            // the fields we own — preserving any manual edits the user made
            // between this process starting and the set call.
            let mut cfg = config::load_or_create_config()?;
            if let Some(p) = prefix {
                cfg.prompt_prefix = Some(p.to_string());
            }
            if let Some(s) = suffix {
                cfg.prompt_suffix = Some(s.to_string());
            }
            write_config(&cfg, config_path)?;
        }
        PromptScope::Project => {
            if let Some(p) = prefix {
                storage::set_project_prompt_prefix(conn, project, Some(p))?;
            }
            if let Some(s) = suffix {
                storage::set_project_prompt_suffix(conn, project, Some(s))?;
            }
        }
    }

    if !out.quiet {
        let icon = output::check_icon(out.color);
        eprintln!(
            "{icon} Updated {} prompt wrap{}{}",
            scope_name(scope),
            prefix.map_or("", |_| " (prefix)"),
            suffix.map_or("", |_| " (suffix)"),
        );
    }
    Ok(())
}

/// `ralph prompt clear` — null out prefix and/or suffix at one scope.
pub fn cmd_prompt_clear(
    conn: &Connection,
    config_path: &std::path::Path,
    project: &str,
    scope: PromptScope,
    clear_prefix: bool,
    clear_suffix: bool,
    out: &OutputContext,
) -> Result<()> {
    if !clear_prefix && !clear_suffix {
        bail!("Pass at least one of --prefix / --suffix to specify what to clear");
    }

    match scope {
        PromptScope::Global => {
            let mut cfg = config::load_or_create_config()?;
            if clear_prefix {
                cfg.prompt_prefix = None;
            }
            if clear_suffix {
                cfg.prompt_suffix = None;
            }
            write_config(&cfg, config_path)?;
        }
        PromptScope::Project => {
            if clear_prefix {
                storage::set_project_prompt_prefix(conn, project, None)?;
            }
            if clear_suffix {
                storage::set_project_prompt_suffix(conn, project, None)?;
            }
        }
    }

    if !out.quiet {
        let icon = output::check_icon(out.color);
        eprintln!(
            "{icon} Cleared {} prompt wrap{}{}",
            scope_name(scope),
            if clear_prefix { " (prefix)" } else { "" },
            if clear_suffix { " (suffix)" } else { "" },
        );
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
    match view.prefix {
        Some(p) => println!("  prefix:\n{}", indent(p, "    ")),
        None => println!("  prefix: <unset>"),
    }
    match view.suffix {
        Some(s) => println!("  suffix:\n{}", indent(s, "    ")),
        None => println!("  suffix: <unset>"),
    }
    println!();
}

fn print_resolved(wraps: &PromptWraps<'_>, out: &OutputContext) -> Result<()> {
    // Compose prefix/suffix exactly how build_step_prompt would, but without
    // the body in between — the user sees the actual text a harness receives.
    let prefix = join_layers([wraps.global.prefix, wraps.project.prefix, wraps.plan.prefix]);
    let suffix = join_layers([wraps.plan.suffix, wraps.project.suffix, wraps.global.suffix]);
    let view = ResolvedView { prefix, suffix };
    if out.format == OutputFormat::Json {
        println!("{}", serde_json::to_string(&view)?);
    } else {
        match &view.prefix {
            Some(p) => println!("prefix:\n{}\n", indent(p, "  ")),
            None => println!("prefix: <none>\n"),
        }
        match &view.suffix {
            Some(s) => println!("suffix:\n{}", indent(s, "  ")),
            None => println!("suffix: <none>"),
        }
    }
    Ok(())
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
