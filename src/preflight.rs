// Preflight checks
//
// Pre-run checks before plan execution:
// - Package manager: warn if package.json exists without node_modules
// - Test binary availability: extract binary from test commands, check via `which`
// - Harness authentication: check GH_TOKEN for copilot
// - Git dirty state: auto-commit with a descriptive message
#![allow(dead_code)]

use std::path::Path;

use anyhow::Result;

use crate::config::{Config, HarnessConfig};
use crate::git;
use crate::plan::Plan;

// ---------------------------------------------------------------------------
// Check result types
// ---------------------------------------------------------------------------

/// Severity of a preflight check result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckSeverity {
    Pass,
    Warning,
    Error,
}

/// A single preflight check result.
#[derive(Debug, Clone)]
pub struct CheckResult {
    pub name: String,
    pub severity: CheckSeverity,
    pub message: String,
}

/// Aggregated preflight results.
#[derive(Debug, Clone)]
pub struct PreflightResults {
    pub checks: Vec<CheckResult>,
}

impl PreflightResults {
    /// Returns true if there are no errors (warnings are OK).
    pub fn is_ok(&self) -> bool {
        !self.checks.iter().any(|c| c.severity == CheckSeverity::Error)
    }

    /// Returns true if every check passed with no warnings or errors.
    pub fn all_passed(&self) -> bool {
        self.checks.iter().all(|c| c.severity == CheckSeverity::Pass)
    }

    /// Print all check results to stderr.
    pub fn print_report(&self) {
        for check in &self.checks {
            let icon = match check.severity {
                CheckSeverity::Pass => "\x1b[32m\u{2714}\x1b[0m",    // green check
                CheckSeverity::Warning => "\x1b[33m\u{26a0}\x1b[0m", // yellow warning
                CheckSeverity::Error => "\x1b[31m\u{2718}\x1b[0m",   // red X
            };
            eprintln!("  {} {}: {}", icon, check.name, check.message);
        }
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Run all preflight checks for a plan execution.
///
/// This does NOT auto-commit git state; that is a separate step called
/// via [`auto_stash_dirty_state`].
pub fn run_preflight_checks(
    plan: &Plan,
    config: &Config,
    workdir: &Path,
) -> Result<PreflightResults> {
    let mut checks = Vec::new();

    // 1. Package manager check
    checks.push(check_package_manager(workdir));

    // 2. Test binary availability
    for result in check_test_binaries(&plan.deterministic_tests) {
        checks.push(result);
    }

    // 3. Harness authentication
    let harness_name = plan
        .harness
        .as_deref()
        .unwrap_or(&config.default_harness);
    if let Some(harness_config) = config.harnesses.get(harness_name) {
        checks.push(check_harness_auth(harness_name, harness_config));
    }

    // 4. Git dirty state (informational only; auto-commit is separate)
    checks.push(check_git_state(workdir));

    Ok(PreflightResults { checks })
}

/// Auto-commit any dirty git state before a plan run.
///
/// If the working tree is clean, this is a no-op.
pub fn auto_stash_dirty_state(workdir: &Path) -> Result<bool> {
    if git::has_uncommitted_changes(workdir)? {
        git::commit_changes(workdir, "ralph-rs: [skip] auto-stash before run")?;
        Ok(true)
    } else {
        Ok(false)
    }
}

// ---------------------------------------------------------------------------
// Individual checks
// ---------------------------------------------------------------------------

/// Check if package.json exists without node_modules.
fn check_package_manager(workdir: &Path) -> CheckResult {
    let pkg_json = workdir.join("package.json");
    let node_modules = workdir.join("node_modules");

    if pkg_json.exists() && !node_modules.exists() {
        CheckResult {
            name: "package-manager".to_string(),
            severity: CheckSeverity::Warning,
            message: "package.json found but node_modules missing; run `npm install` or `yarn`"
                .to_string(),
        }
    } else {
        CheckResult {
            name: "package-manager".to_string(),
            severity: CheckSeverity::Pass,
            message: "OK".to_string(),
        }
    }
}

/// Extract the binary name from a test command and check if it's available via `which`.
fn check_test_binaries(test_commands: &[String]) -> Vec<CheckResult> {
    let mut results = Vec::new();

    for cmd in test_commands {
        let binary = extract_binary_from_command(cmd);
        if binary.is_empty() {
            continue;
        }

        let available = is_binary_available(&binary);
        if available {
            results.push(CheckResult {
                name: format!("test-binary:{binary}"),
                severity: CheckSeverity::Pass,
                message: format!("`{binary}` found"),
            });
        } else {
            results.push(CheckResult {
                name: format!("test-binary:{binary}"),
                severity: CheckSeverity::Warning,
                message: format!("`{binary}` not found in PATH"),
            });
        }
    }

    results
}

/// Check harness-specific authentication requirements.
fn check_harness_auth(harness_name: &str, _harness_config: &HarnessConfig) -> CheckResult {
    // Copilot (gh copilot) requires GH_TOKEN or GITHUB_TOKEN
    if harness_name == "copilot" {
        let has_token = std::env::var("GH_TOKEN").is_ok()
            || std::env::var("GITHUB_TOKEN").is_ok();

        if has_token {
            CheckResult {
                name: "harness-auth".to_string(),
                severity: CheckSeverity::Pass,
                message: format!("{harness_name}: GitHub token found"),
            }
        } else {
            CheckResult {
                name: "harness-auth".to_string(),
                severity: CheckSeverity::Warning,
                message: format!(
                    "{harness_name}: GH_TOKEN or GITHUB_TOKEN not set; `gh` may prompt for login"
                ),
            }
        }
    } else {
        CheckResult {
            name: "harness-auth".to_string(),
            severity: CheckSeverity::Pass,
            message: format!("{harness_name}: no special auth required"),
        }
    }
}

/// Check git working tree state.
fn check_git_state(workdir: &Path) -> CheckResult {
    match git::has_uncommitted_changes(workdir) {
        Ok(true) => CheckResult {
            name: "git-state".to_string(),
            severity: CheckSeverity::Warning,
            message: "uncommitted changes detected; will auto-commit before run".to_string(),
        },
        Ok(false) => CheckResult {
            name: "git-state".to_string(),
            severity: CheckSeverity::Pass,
            message: "working tree clean".to_string(),
        },
        Err(_) => CheckResult {
            name: "git-state".to_string(),
            severity: CheckSeverity::Warning,
            message: "not a git repository or git not available".to_string(),
        },
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract the first word (binary name) from a shell command string.
///
/// Handles common patterns like `cargo test`, `npm run test`, `sh -c "..."`.
fn extract_binary_from_command(cmd: &str) -> String {
    let trimmed = cmd.trim();
    // Split on whitespace and take the first token.
    trimmed
        .split_whitespace()
        .next()
        .unwrap_or("")
        .to_string()
}

/// Check if a binary is available on PATH using `which`.
fn is_binary_available(binary: &str) -> bool {
    std::process::Command::new("which")
        .arg(binary)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Doctor checks
// ---------------------------------------------------------------------------

/// Run doctor checks: verify config, database, harness binaries, agents dir.
pub fn run_doctor_checks(config: &Config) -> Vec<CheckResult> {
    let mut checks = Vec::new();

    // 1. Config exists and parses (if we got here, it parsed).
    checks.push(CheckResult {
        name: "config".to_string(),
        severity: CheckSeverity::Pass,
        message: "configuration loaded successfully".to_string(),
    });

    // 2. Database check
    match crate::db::open() {
        Ok(_) => {
            checks.push(CheckResult {
                name: "database".to_string(),
                severity: CheckSeverity::Pass,
                message: "database opens and migrations applied".to_string(),
            });
        }
        Err(e) => {
            checks.push(CheckResult {
                name: "database".to_string(),
                severity: CheckSeverity::Error,
                message: format!("database error: {e}"),
            });
        }
    }

    // 3. Check each configured harness binary.
    for (name, harness_config) in &config.harnesses {
        let binary = &harness_config.command;
        if is_binary_available(binary) {
            checks.push(CheckResult {
                name: format!("harness:{name}"),
                severity: CheckSeverity::Pass,
                message: format!("`{binary}` found"),
            });
        } else {
            checks.push(CheckResult {
                name: format!("harness:{name}"),
                severity: CheckSeverity::Warning,
                message: format!("`{binary}` not found in PATH"),
            });
        }
    }

    // 4. Agents directory
    match crate::config::agents_dir() {
        Ok(dir) => {
            if dir.exists() {
                checks.push(CheckResult {
                    name: "agents-dir".to_string(),
                    severity: CheckSeverity::Pass,
                    message: format!("{}", dir.display()),
                });
            } else {
                checks.push(CheckResult {
                    name: "agents-dir".to_string(),
                    severity: CheckSeverity::Warning,
                    message: format!("{} does not exist", dir.display()),
                });
            }
        }
        Err(e) => {
            checks.push(CheckResult {
                name: "agents-dir".to_string(),
                severity: CheckSeverity::Error,
                message: format!("could not determine agents directory: {e}"),
            });
        }
    }

    checks
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_extract_binary_simple() {
        assert_eq!(extract_binary_from_command("cargo test"), "cargo");
        assert_eq!(extract_binary_from_command("npm run test"), "npm");
        assert_eq!(extract_binary_from_command("  pytest  "), "pytest");
        assert_eq!(extract_binary_from_command(""), "");
    }

    #[test]
    fn test_extract_binary_with_path() {
        assert_eq!(
            extract_binary_from_command("cargo clippy -- -D warnings"),
            "cargo"
        );
    }

    #[test]
    fn test_check_package_manager_no_pkg_json() {
        let tmp = tempfile::tempdir().unwrap();
        let result = check_package_manager(tmp.path());
        assert_eq!(result.severity, CheckSeverity::Pass);
    }

    #[test]
    fn test_check_package_manager_with_node_modules() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("package.json"), "{}").unwrap();
        std::fs::create_dir(tmp.path().join("node_modules")).unwrap();
        let result = check_package_manager(tmp.path());
        assert_eq!(result.severity, CheckSeverity::Pass);
    }

    #[test]
    fn test_check_package_manager_missing_node_modules() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(tmp.path().join("package.json"), "{}").unwrap();
        let result = check_package_manager(tmp.path());
        assert_eq!(result.severity, CheckSeverity::Warning);
        assert!(result.message.contains("node_modules"));
    }

    #[test]
    fn test_check_test_binaries_found() {
        // `sh` should always be available on Unix-like systems.
        let results = check_test_binaries(&["sh -c 'echo hello'".to_string()]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].severity, CheckSeverity::Pass);
    }

    #[test]
    fn test_check_test_binaries_not_found() {
        let results = check_test_binaries(&["nonexistent_binary_xyz --test".to_string()]);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].severity, CheckSeverity::Warning);
    }

    #[test]
    fn test_check_test_binaries_empty() {
        let results = check_test_binaries(&[]);
        assert!(results.is_empty());
    }

    #[test]
    fn test_check_harness_auth_copilot_no_token() {
        // Temporarily ensure neither token is set.
        let gh = std::env::var("GH_TOKEN").ok();
        let github = std::env::var("GITHUB_TOKEN").ok();
        // SAFETY: This test runs single-threaded; no concurrent env access.
        unsafe {
            std::env::remove_var("GH_TOKEN");
            std::env::remove_var("GITHUB_TOKEN");
        }

        let harness = crate::config::HarnessConfig {
            command: "gh".to_string(),
            args: vec!["copilot".to_string()],
            supports_agent_file: false,
            supports_json_output: false,
            json_output_args: vec![],
            agent_file_env: None,
        };
        let result = check_harness_auth("copilot", &harness);
        assert_eq!(result.severity, CheckSeverity::Warning);

        // Restore
        unsafe {
            if let Some(v) = gh {
                std::env::set_var("GH_TOKEN", v);
            }
            if let Some(v) = github {
                std::env::set_var("GITHUB_TOKEN", v);
            }
        }
    }

    #[test]
    fn test_check_harness_auth_non_copilot() {
        let harness = crate::config::HarnessConfig {
            command: "claude".to_string(),
            args: vec![],
            supports_agent_file: true,
            supports_json_output: true,
            json_output_args: vec![],
            agent_file_env: None,
        };
        let result = check_harness_auth("claude", &harness);
        assert_eq!(result.severity, CheckSeverity::Pass);
    }

    #[test]
    fn test_check_git_state_not_git_repo() {
        let tmp = tempfile::tempdir().unwrap();
        let result = check_git_state(tmp.path());
        // Not a git repo -> warning
        assert_eq!(result.severity, CheckSeverity::Warning);
    }

    #[test]
    fn test_preflight_results_is_ok() {
        let results = PreflightResults {
            checks: vec![
                CheckResult {
                    name: "a".to_string(),
                    severity: CheckSeverity::Pass,
                    message: "ok".to_string(),
                },
                CheckResult {
                    name: "b".to_string(),
                    severity: CheckSeverity::Warning,
                    message: "warn".to_string(),
                },
            ],
        };
        assert!(results.is_ok()); // warnings are OK
        assert!(!results.all_passed());
    }

    #[test]
    fn test_preflight_results_has_error() {
        let results = PreflightResults {
            checks: vec![CheckResult {
                name: "a".to_string(),
                severity: CheckSeverity::Error,
                message: "bad".to_string(),
            }],
        };
        assert!(!results.is_ok());
    }

    #[test]
    fn test_preflight_results_all_passed() {
        let results = PreflightResults {
            checks: vec![CheckResult {
                name: "a".to_string(),
                severity: CheckSeverity::Pass,
                message: "ok".to_string(),
            }],
        };
        assert!(results.is_ok());
        assert!(results.all_passed());
    }

    #[test]
    fn test_doctor_checks_returns_results() {
        let config = Config {
            default_harness: "claude".to_string(),
            max_retries_per_step: 3,
            timeout_secs: 300,
            harnesses: HashMap::new(),
        };
        let checks = run_doctor_checks(&config);
        // Should have at least config, database, agents-dir checks
        assert!(checks.len() >= 3);

        // Config should always pass
        let config_check = checks.iter().find(|c| c.name == "config").unwrap();
        assert_eq!(config_check.severity, CheckSeverity::Pass);
    }

    #[test]
    fn test_is_binary_available_sh() {
        // `sh` should always be available
        assert!(is_binary_available("sh"));
    }

    #[test]
    fn test_is_binary_not_available() {
        assert!(!is_binary_available("definitely_not_a_real_binary_xyz"));
    }
}
