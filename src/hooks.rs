// Lifecycle hooks: shell scripts in `<project>/.ralph/hooks/`
//
// Hook names: pre-step, post-step, pre-test, post-test
// Convention-based discovery — missing hooks are silently skipped.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result, bail};

use crate::plan::{Plan, Step};

/// Directory within the project that holds hook scripts.
const HOOKS_DIR: &str = ".ralph/hooks";

/// Known hook names.
pub const PRE_STEP: &str = "pre-step";
pub const POST_STEP: &str = "post-step";
pub const PRE_TEST: &str = "pre-test";
pub const POST_TEST: &str = "post-test";

/// Find a hook script by name. Returns `None` if the file doesn't exist.
fn find_hook(workdir: &Path, name: &str) -> Option<PathBuf> {
    let path = workdir.join(HOOKS_DIR).join(name);
    if path.is_file() { Some(path) } else { None }
}

/// Run a hook script with the given environment variables.
///
/// The hook is executed via `sh <path>` in the workdir. Environment variables
/// are set on the child process.
///
/// Returns `Ok(())` if the hook doesn't exist or exits 0.
/// Returns `Err` if the hook exits non-zero.
fn run_hook(workdir: &Path, name: &str, env: &[(&str, &str)]) -> Result<()> {
    let path = match find_hook(workdir, name) {
        Some(p) => p,
        None => return Ok(()),
    };

    let mut cmd = Command::new("sh");
    cmd.arg(&path).current_dir(workdir);

    for &(key, val) in env {
        cmd.env(key, val);
    }

    let output = cmd
        .output()
        .with_context(|| format!("Failed to execute hook '{name}'"))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Hook '{name}' failed (exit {}): {}",
            output.status,
            stderr.trim()
        );
    }

    Ok(())
}

/// Build the common environment variables for step hooks.
fn step_env<'a>(
    plan: &'a Plan,
    step: &'a Step,
    attempt: i32,
    workdir: &'a Path,
) -> Vec<(&'a str, String)> {
    vec![
        ("RALPH_PLAN_SLUG", plan.slug.clone()),
        ("RALPH_PLAN_ID", plan.id.clone()),
        ("RALPH_STEP_TITLE", step.title.clone()),
        ("RALPH_STEP_ID", step.id.clone()),
        ("RALPH_STEP_ATTEMPT", attempt.to_string()),
        ("RALPH_PROJECT_DIR", workdir.to_string_lossy().into_owned()),
    ]
}

/// Run the pre-step hook. Returns `Err` if it fails (caller should treat as failed attempt).
pub fn run_pre_step(plan: &Plan, step: &Step, attempt: i32, workdir: &Path) -> Result<()> {
    let env_owned = step_env(plan, step, attempt, workdir);
    let env: Vec<(&str, &str)> = env_owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
    run_hook(workdir, PRE_STEP, &env)
}

/// Run the post-step hook. Logs a warning on failure but does not return an error.
pub fn run_post_step(plan: &Plan, step: &Step, attempt: i32, status: &str, workdir: &Path) {
    let mut env_owned = step_env(plan, step, attempt, workdir);
    env_owned.push(("RALPH_STEP_STATUS", status.to_string()));
    let env: Vec<(&str, &str)> = env_owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
    if let Err(e) = run_hook(workdir, POST_STEP, &env) {
        eprintln!("Warning: post-step hook failed: {e}");
    }
}

/// Run the pre-test hook. Returns `Err` if it fails.
pub fn run_pre_test(plan: &Plan, step: &Step, attempt: i32, workdir: &Path) -> Result<()> {
    let env_owned = step_env(plan, step, attempt, workdir);
    let env: Vec<(&str, &str)> = env_owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
    run_hook(workdir, PRE_TEST, &env)
}

/// Run the post-test hook. Logs a warning on failure but does not return an error.
pub fn run_post_test(plan: &Plan, step: &Step, attempt: i32, test_passed: bool, workdir: &Path) {
    let mut env_owned = step_env(plan, step, attempt, workdir);
    env_owned.push((
        "RALPH_TEST_PASSED",
        if test_passed { "true" } else { "false" }.to_string(),
    ));
    let env: Vec<(&str, &str)> = env_owned.iter().map(|(k, v)| (*k, v.as_str())).collect();
    if let Err(e) = run_hook(workdir, POST_TEST, &env) {
        eprintln!("Warning: post-test hook failed: {e}");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn setup_hook(dir: &Path, name: &str, script: &str) {
        let hooks_dir = dir.join(HOOKS_DIR);
        fs::create_dir_all(&hooks_dir).unwrap();
        let path = hooks_dir.join(name);
        fs::write(&path, script).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    #[test]
    fn test_find_hook_missing() {
        let tmp = TempDir::new().unwrap();
        assert!(find_hook(tmp.path(), "pre-step").is_none());
    }

    #[test]
    fn test_find_hook_exists() {
        let tmp = TempDir::new().unwrap();
        setup_hook(tmp.path(), "pre-step", "#!/bin/sh\ntrue");
        assert!(find_hook(tmp.path(), "pre-step").is_some());
    }

    #[test]
    fn test_run_hook_missing_is_ok() {
        let tmp = TempDir::new().unwrap();
        assert!(run_hook(tmp.path(), "pre-step", &[]).is_ok());
    }

    #[test]
    fn test_run_hook_success() {
        let tmp = TempDir::new().unwrap();
        setup_hook(tmp.path(), "pre-step", "#!/bin/sh\ntrue");
        assert!(run_hook(tmp.path(), "pre-step", &[]).is_ok());
    }

    #[test]
    fn test_run_hook_failure() {
        let tmp = TempDir::new().unwrap();
        setup_hook(tmp.path(), "pre-step", "#!/bin/sh\nexit 1");
        assert!(run_hook(tmp.path(), "pre-step", &[]).is_err());
    }

    #[test]
    fn test_run_hook_receives_env() {
        let tmp = TempDir::new().unwrap();
        let marker = tmp.path().join("marker.txt");
        let script = format!("#!/bin/sh\necho $RALPH_PLAN_SLUG > {}", marker.display());
        setup_hook(tmp.path(), "pre-step", &script);
        run_hook(tmp.path(), "pre-step", &[("RALPH_PLAN_SLUG", "test-plan")]).unwrap();
        let content = fs::read_to_string(&marker).unwrap();
        assert_eq!(content.trim(), "test-plan");
    }
}
