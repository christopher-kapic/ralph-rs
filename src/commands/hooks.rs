// Hooks CLI command implementations

use anyhow::{Context, Result, bail};
use std::path::Path;

use crate::hook_library::{self, Hook, HookBundle, Lifecycle, Scope};
use crate::output::{self, OutputContext, OutputFormat};
use crate::validate::validate_name;

// ---------------------------------------------------------------------------
// Hooks commands
// ---------------------------------------------------------------------------

pub fn cmd_hooks_list(project: &str, all: bool, out: &OutputContext) -> Result<()> {
    let hooks = hook_library::load_all()?;

    let filtered: Vec<Hook> = if all {
        hooks
    } else {
        hook_library::filter_by_project(hooks, Path::new(project))
    };

    if out.format == OutputFormat::Json {
        let infos: Vec<output::HookInfo> = filtered
            .iter()
            .map(|h| {
                let scope_str = match &h.scope {
                    Scope::Global => "global".to_string(),
                    Scope::Paths { paths } => {
                        let list: Vec<String> =
                            paths.iter().map(|p| p.display().to_string()).collect();
                        format!("paths: {}", list.join(", "))
                    }
                };
                output::HookInfo {
                    name: h.name.clone(),
                    lifecycle: h.lifecycle.to_string(),
                    scope: scope_str,
                    description: h.description.clone(),
                }
            })
            .collect();
        println!("{}", serde_json::to_string(&infos)?);
        return Ok(());
    }

    if filtered.is_empty() {
        if all {
            eprintln!("No hooks found in {}", hook_library::hooks_dir()?.display());
        } else {
            eprintln!(
                "No hooks applicable to {project}. Use `ralph hooks list --all` to see all hooks."
            );
        }
        return Ok(());
    }

    for hook in &filtered {
        let scope_str = match &hook.scope {
            Scope::Global => "global".to_string(),
            Scope::Paths { paths } => {
                let list: Vec<String> = paths.iter().map(|p| p.display().to_string()).collect();
                format!("paths: {}", list.join(", "))
            }
        };
        let desc = if hook.description.is_empty() {
            String::new()
        } else {
            format!(" — {}", hook.description)
        };
        println!(
            "  {name:<24} [{lifecycle}] ({scope}){desc}",
            name = hook.name,
            lifecycle = hook.lifecycle,
            scope = scope_str,
        );
    }

    Ok(())
}

pub fn cmd_hooks_show(name: &str, _out: &OutputContext) -> Result<()> {
    validate_name(name)?;
    let path = hook_library::hooks_dir()?.join(format!("{name}.md"));
    if !path.exists() {
        bail!("Hook not found: {name}");
    }
    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    println!("{contents}");
    Ok(())
}

pub fn cmd_hooks_add(
    name: &str,
    lifecycle: Lifecycle,
    command: &str,
    description: Option<&str>,
    scope_paths: &[std::path::PathBuf],
    force: bool,
    _out: &OutputContext,
) -> Result<()> {
    validate_name(name)?;
    let scope = if scope_paths.is_empty() {
        Scope::Global
    } else {
        for p in scope_paths {
            if !p.is_absolute() {
                bail!(
                    "Scope path '{}' must be absolute (no '~' expansion)",
                    p.display()
                );
            }
        }
        Scope::Paths {
            paths: scope_paths.to_vec(),
        }
    };

    let hook = Hook {
        name: name.to_string(),
        description: description.unwrap_or("").to_string(),
        lifecycle,
        scope,
        command: command.to_string(),
    };

    let path = hook_library::save(&hook, force)?;
    eprintln!("Created hook '{name}' at {}", path.display());
    Ok(())
}

pub fn cmd_hooks_remove(name: &str, _out: &OutputContext) -> Result<()> {
    validate_name(name)?;
    hook_library::delete(name)?;
    eprintln!("Deleted hook '{name}'");
    Ok(())
}

pub fn cmd_hooks_export(
    project: &str,
    output: Option<&Path>,
    all: bool,
    path: Option<&Path>,
    _out: &OutputContext,
) -> Result<()> {
    let hooks = hook_library::load_all()?;

    let filtered: Vec<Hook> = if all {
        hooks
    } else {
        let scope_path = path
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| std::path::PathBuf::from(project));
        hook_library::filter_by_project(hooks, &scope_path)
    };

    let bundle = HookBundle::new(filtered);
    let json = serde_json::to_string_pretty(&bundle)?;

    match output {
        Some(p) => {
            std::fs::write(p, format!("{json}\n"))
                .with_context(|| format!("Failed to write {}", p.display()))?;
            eprintln!("Exported {} hook(s) to {}", bundle.hooks.len(), p.display());
        }
        None => println!("{json}"),
    }
    Ok(())
}

pub fn cmd_hooks_import(file: &Path, force: bool, _out: &OutputContext) -> Result<()> {
    let contents = std::fs::read_to_string(file)
        .with_context(|| format!("Failed to read bundle {}", file.display()))?;
    let bundle: HookBundle = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse bundle {}", file.display()))?;

    if bundle.hooks.is_empty() {
        eprintln!("Bundle contains no hooks.");
        return Ok(());
    }

    let mut imported = 0usize;
    let mut collisions: Vec<String> = Vec::new();

    for hook in &bundle.hooks {
        let existed = hook_library::try_load(&hook.name)?.is_some();
        if existed && !force {
            collisions.push(hook.name.clone());
            continue;
        }
        hook_library::save(hook, true)?;
        imported += 1;
    }

    finalize_import(imported, &collisions, force)
}

fn finalize_import(imported: usize, collisions: &[String], force: bool) -> Result<()> {
    let skipped = collisions.len();
    if skipped > 0 {
        eprintln!(
            "Skipped {skipped} hook(s) due to collisions: {}",
            collisions.join(", ")
        );
    }
    eprintln!("Imported {imported} hook(s), skipped {skipped}.");
    if imported == 0 && skipped > 0 {
        bail!("No hooks imported; all {skipped} collided. Use --force to overwrite.");
    }
    if skipped > 0 && !force {
        eprintln!("Re-run with --force to overwrite existing hooks.");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::finalize_import;

    #[test]
    fn partial_success_returns_ok_and_reports_collisions() {
        let collisions = vec!["a".to_string(), "b".to_string()];
        let result = finalize_import(3, &collisions, false);
        assert!(
            result.is_ok(),
            "partial import should exit 0; got: {result:?}"
        );
    }

    #[test]
    fn all_collided_returns_err() {
        let collisions = vec!["a".to_string()];
        let result = finalize_import(0, &collisions, false);
        let err = result.expect_err("all-collided import should error");
        assert!(err.to_string().contains("No hooks imported"), "got: {err}");
    }

    #[test]
    fn clean_import_returns_ok() {
        assert!(finalize_import(2, &[], false).is_ok());
    }

    #[test]
    fn force_with_overwrites_returns_ok() {
        let collisions: Vec<String> = Vec::new();
        assert!(finalize_import(2, &collisions, true).is_ok());
    }
}
