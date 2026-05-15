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
use crate::storage::{self, ProjectPromptSource};

/// Serializable view of a single scope's prompt for JSON output.
#[derive(Debug, serde::Serialize)]
struct ScopeView<'a> {
    scope: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    prompt: Option<&'a str>,
    /// Active source for the Project scope: `"file"` or `"db"`. Omitted for
    /// the Global scope (always config.json).
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<&'static str>,
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
    let (project_settings, project_source) = storage::resolve_project_prompt(conn, project)?;

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

    let project_source_label = match &project_source {
        ProjectPromptSource::File(_) => "file",
        ProjectPromptSource::Db => "db",
    };

    let all_views = [
        ScopeView {
            scope: "global",
            prompt: config.prompt.as_deref(),
            source: None,
        },
        ScopeView {
            scope: "project",
            prompt: project_settings.prompt.as_deref(),
            source: Some(project_source_label),
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
            // The checked-in file, when it already exists, is the source of
            // truth — write through to it so a shared file stays canonical.
            // Otherwise fall back to the per-machine DB column (solo users
            // aren't forced onto the file path).
            let (_, source) = storage::resolve_project_prompt(conn, project)?;
            match source {
                ProjectPromptSource::File(_) => {
                    storage::write_project_prompt_file(project, value.unwrap_or(""))?;
                }
                ProjectPromptSource::Db => {
                    storage::set_project_prompt(conn, project, value)?;
                }
            }
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
            // Empty the project layer in one shot. When the file is the
            // active source we delete it AND null the DB row: otherwise a
            // stale `project_settings.prompt` value would resurface on the
            // very next read (file gone → DB fallback), so a single
            // `prompt clear` wouldn't actually empty the project layer.
            // When no file is active there's nothing to delete and we just
            // clear the DB row (the existing DB-only path).
            let (_, source) = storage::resolve_project_prompt(conn, project)?;
            if let ProjectPromptSource::File(_) = source {
                storage::delete_project_prompt_file(project)?;
            }
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

/// Persist `cfg` to `path` atomically (tmp-file + rename) via the shared
/// `Config::save_at` helper, so a crash mid-write can't truncate
/// `config.json`. `path` is always `<config_dir>/config.json`, so its
/// parent is the directory `save_at` writes into.
fn write_config(cfg: &Config, path: &std::path::Path) -> Result<()> {
    let dir = path.parent().with_context(|| {
        format!("Config path {} has no parent directory", path.display())
    })?;
    cfg.save_at(dir)
        .with_context(|| format!("Failed to write config to {}", path.display()))
}

fn print_scope_plain(view: &ScopeView<'_>) {
    match view.source {
        // Project scope: surface which source is active so users know
        // whether `prompt set`/`clear` will touch the checked-in file or
        // the per-machine DB row.
        Some("file") => println!("[{}] (file: .ralph/prompt.md)", view.scope),
        Some(_) => println!("[{}] (db)", view.scope),
        None => println!("[{}]", view.scope),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage;

    fn quiet_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    /// `prompt set --scope project` writes to the DB when no file exists.
    #[test]
    fn project_set_targets_db_when_file_absent() {
        let conn = crate::db::open_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();
        let cfg_path = dir.path().join("config.json");

        cmd_prompt_set(
            &conn,
            &cfg_path,
            &project,
            PromptScope::Project,
            "db content",
            &quiet_out(),
        )
        .unwrap();

        assert_eq!(
            storage::get_project_settings_db(&conn, &project)
                .unwrap()
                .prompt
                .as_deref(),
            Some("db content")
        );
        assert!(storage::read_project_prompt_file(&project).unwrap().is_none());
    }

    /// `prompt set --scope project` writes through to the file when the
    /// file already exists (a team's checked-in shared prompt stays canonical).
    #[test]
    fn project_set_targets_file_when_file_present() {
        let conn = crate::db::open_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();
        let cfg_path = dir.path().join("config.json");

        // File pre-exists with some content → it's the active source.
        storage::write_project_prompt_file(&project, "original").unwrap();

        cmd_prompt_set(
            &conn,
            &cfg_path,
            &project,
            PromptScope::Project,
            "updated via file",
            &quiet_out(),
        )
        .unwrap();

        assert_eq!(
            storage::read_project_prompt_file(&project).unwrap().as_deref(),
            Some("updated via file")
        );
        // DB column untouched.
        assert_eq!(
            storage::get_project_settings_db(&conn, &project)
                .unwrap()
                .prompt,
            None
        );
    }

    /// `prompt clear --scope project` with the file active deletes the file
    /// AND nulls the DB row, so the project layer is genuinely empty after
    /// one clear — a stale DB value must not resurface on the next read.
    #[test]
    fn project_clear_deletes_file_and_clears_db_when_file_active() {
        let conn = crate::db::open_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();
        let cfg_path = dir.path().join("config.json");

        // Both a stale DB value and the active file are present; the file
        // is the active source.
        storage::set_project_prompt(&conn, &project, Some("stale db value")).unwrap();
        storage::write_project_prompt_file(&project, "shared").unwrap();
        assert!(matches!(
            storage::resolve_project_prompt(&conn, &project).unwrap().1,
            ProjectPromptSource::File(_)
        ));

        cmd_prompt_clear(
            &conn,
            &cfg_path,
            &project,
            PromptScope::Project,
            &quiet_out(),
        )
        .unwrap();

        // File gone.
        assert!(!storage::project_prompt_file_path(&project).exists());
        // DB row cleared too — no resurrection.
        assert_eq!(
            storage::get_project_settings_db(&conn, &project)
                .unwrap()
                .prompt,
            None
        );
        // The effective project layer is genuinely empty.
        let (settings, _) = storage::resolve_project_prompt(&conn, &project).unwrap();
        assert_eq!(settings.prompt, None);
    }

    /// `--scope universal` (clap alias for `global`) drives the exact same
    /// global config.json path as `--scope global` through `cmd_prompt_set`.
    /// We parse the CLI so the alias-resolution is exercised end to end, then
    /// assert the handler wrote `config.prompt`.
    #[test]
    fn universal_alias_sets_global_prompt_like_global() {
        use crate::cli::{Cli, Command, PromptCommand};
        use clap::Parser;

        let conn = crate::db::open_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();

        // Resolve the scope through clap's alias machinery, exactly as the
        // real CLI dispatch does.
        let cli = Cli::try_parse_from([
            "ralph-rs",
            "prompt",
            "set",
            "--scope",
            "universal",
            "from universal alias",
        ])
        .unwrap();
        let scope = match cli.command.unwrap() {
            Command::Prompt(PromptCommand::Set { scope, .. }) => scope,
            _ => panic!("expected prompt set"),
        };
        assert_eq!(scope, PromptScope::Global);

        // Point config loading at the isolated temp dir so we don't touch
        // the user's real config. The Global path in `cmd_prompt_set`
        // round-trips through `config::load_or_create_config()` /
        // `config::config_dir()`, so the config_path we pass must be the
        // XDG-resolved one (not an arbitrary tempfile).
        let _xdg = set_xdg(dir.path());
        let cfg_path = crate::config::config_dir().unwrap().join("config.json");

        cmd_prompt_set(
            &conn,
            &cfg_path,
            &project,
            scope,
            "from universal alias",
            &quiet_out(),
        )
        .unwrap();

        let cfg = crate::config::load_or_create_config().unwrap();
        assert_eq!(cfg.prompt.as_deref(), Some("from universal alias"));
    }

    use std::sync::{Mutex, MutexGuard};

    /// Serialize tests that mutate `$XDG_CONFIG_HOME` (process-wide env).
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct XdgGuard {
        _lock: MutexGuard<'static, ()>,
        prev: Option<std::ffi::OsString>,
    }
    impl Drop for XdgGuard {
        fn drop(&mut self) {
            match self.prev.take() {
                Some(v) => unsafe { std::env::set_var("XDG_CONFIG_HOME", v) },
                None => unsafe { std::env::remove_var("XDG_CONFIG_HOME") },
            }
        }
    }
    fn set_xdg(path: &std::path::Path) -> XdgGuard {
        let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var_os("XDG_CONFIG_HOME");
        // SAFETY: guarded by ENV_LOCK for the lifetime of the returned guard.
        unsafe { std::env::set_var("XDG_CONFIG_HOME", path) };
        XdgGuard { _lock: lock, prev }
    }

    /// `prompt clear --scope project` clears the DB row when no file exists.
    #[test]
    fn project_clear_clears_db_when_file_absent() {
        let conn = crate::db::open_memory().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let project = dir.path().to_string_lossy().into_owned();
        let cfg_path = dir.path().join("config.json");

        storage::set_project_prompt(&conn, &project, Some("db value")).unwrap();

        cmd_prompt_clear(
            &conn,
            &cfg_path,
            &project,
            PromptScope::Project,
            &quiet_out(),
        )
        .unwrap();

        assert_eq!(
            storage::get_project_settings_db(&conn, &project)
                .unwrap()
                .prompt,
            None
        );
    }
}
