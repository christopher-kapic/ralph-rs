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
            out.status(format!(
                "No hooks found in {}",
                hook_library::hooks_dir()?.display()
            ));
        } else {
            out.status(format!(
                "No hooks applicable to {project}. Use `ralph hooks list --all` to see all hooks."
            ));
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
    out: &OutputContext,
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
    out.status(format!("Created hook '{name}' at {}", path.display()));
    Ok(())
}

pub fn cmd_hooks_remove(name: &str, out: &OutputContext) -> Result<()> {
    validate_name(name)?;
    hook_library::delete(name)?;
    out.status(format!("Deleted hook '{name}'"));
    Ok(())
}

pub fn cmd_hooks_export(
    project: &str,
    output: Option<&Path>,
    all: bool,
    path: Option<&Path>,
    out: &OutputContext,
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
            out.status(format!(
                "Exported {} hook(s) to {}",
                bundle.hooks.len(),
                p.display()
            ));
        }
        None => println!("{json}"),
    }
    Ok(())
}

pub fn cmd_hooks_import(file: &Path, force: bool, trust: bool, out: &OutputContext) -> Result<()> {
    let contents = std::fs::read_to_string(file)
        .with_context(|| format!("Failed to read bundle {}", file.display()))?;
    let bundle: HookBundle = serde_json::from_str(&contents)
        .with_context(|| format!("Failed to parse bundle {}", file.display()))?;

    if bundle.hooks.is_empty() {
        out.status("Bundle contains no hooks.");
        return Ok(());
    }

    if !out.quiet && out.format == OutputFormat::Plain {
        eprintln!("Hook bundle contains shell commands:");
        for hook in &bundle.hooks {
            eprintln!("  - {}: {}", hook.name, hook.command);
        }
    }
    if !trust {
        bail!("Refusing to install hook commands from an imported bundle without --trust");
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

    finalize_import(imported, &collisions, force, out)
}

fn finalize_import(
    imported: usize,
    collisions: &[String],
    force: bool,
    out: &OutputContext,
) -> Result<()> {
    let skipped = collisions.len();
    if skipped > 0 {
        out.status(format!(
            "Skipped {skipped} hook(s) due to collisions: {}",
            collisions.join(", ")
        ));
    }
    out.status(format!("Imported {imported} hook(s), skipped {skipped}."));
    if imported == 0 && skipped > 0 {
        bail!("No hooks imported; all {skipped} collided. Use --force to overwrite.");
    }
    if skipped > 0 && !force {
        out.status("Re-run with --force to overwrite existing hooks.");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{cmd_hooks_import, finalize_import};
    use crate::hook_library::{Hook, HookBundle, Lifecycle, Scope};
    use crate::output::{OutputContext, OutputFormat};
    use std::path::Path;
    use std::sync::MutexGuard;

    fn quiet_out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    /// Shared crate-wide so it serializes `$XDG_CONFIG_HOME` mutation across
    /// test modules, not just within this one.
    use crate::config::XDG_ENV_LOCK as ENV_LOCK;

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

    fn set_xdg(path: &Path) -> XdgGuard {
        let lock = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var_os("XDG_CONFIG_HOME");
        // SAFETY: guarded by ENV_LOCK for the duration of the returned guard.
        unsafe { std::env::set_var("XDG_CONFIG_HOME", path) };
        XdgGuard { _lock: lock, prev }
    }

    #[test]
    fn partial_success_returns_ok_and_reports_collisions() {
        let collisions = vec!["a".to_string(), "b".to_string()];
        let result = finalize_import(3, &collisions, false, &quiet_out());
        assert!(
            result.is_ok(),
            "partial import should exit 0; got: {result:?}"
        );
    }

    #[test]
    fn all_collided_returns_err() {
        let collisions = vec!["a".to_string()];
        let result = finalize_import(0, &collisions, false, &quiet_out());
        let err = result.expect_err("all-collided import should error");
        assert!(err.to_string().contains("No hooks imported"), "got: {err}");
    }

    #[test]
    fn clean_import_returns_ok() {
        assert!(finalize_import(2, &[], false, &quiet_out()).is_ok());
    }

    #[test]
    fn force_with_overwrites_returns_ok() {
        let collisions: Vec<String> = Vec::new();
        assert!(finalize_import(2, &collisions, true, &quiet_out()).is_ok());
    }

    #[test]
    fn import_requires_trust_before_writing_hooks() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let _xdg = set_xdg(tmp.path());
        let bundle_path = tmp.path().join("hooks.json");
        let bundle = HookBundle::new(vec![Hook {
            name: "dangerous".to_string(),
            description: "test hook".to_string(),
            lifecycle: Lifecycle::PostStep,
            scope: Scope::Global,
            command: "echo owned".to_string(),
        }]);
        std::fs::write(
            &bundle_path,
            serde_json::to_string_pretty(&bundle).expect("serialize hook bundle"),
        )
        .expect("write hook bundle");

        let err = cmd_hooks_import(&bundle_path, false, false, &quiet_out())
            .expect_err("hook import must require --trust");
        assert!(
            err.to_string().contains("without --trust"),
            "unexpected error: {err}"
        );
        assert!(
            !tmp.path()
                .join("ralph-rs")
                .join("hooks")
                .join("dangerous.md")
                .exists(),
            "untrusted hook bundle must not be installed"
        );
    }
}
