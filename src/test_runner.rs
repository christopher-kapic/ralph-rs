#![allow(dead_code)]
// Test validation runner
//
// Executes deterministic test commands (shell commands) and collects structured results.

use std::path::Path;
use std::process::Command;

/// Result of a single test command execution.
#[derive(Debug, Clone)]
pub struct TestResult {
    /// The shell command that was executed.
    pub command: String,
    /// Process exit code (`None` if terminated by signal).
    pub exit_code: Option<i32>,
    /// Last ~50 lines of combined stdout+stderr.
    pub output_tail: String,
    /// Whether the command succeeded (exit code 0).
    pub passed: bool,
}

/// Aggregated results from running a suite of test commands.
#[derive(Debug, Clone)]
pub struct TestResults {
    /// Individual results for each executed test (may be fewer than the input
    /// list if short-circuiting occurred).
    pub results: Vec<TestResult>,
    /// `true` only when every test command passed.
    pub all_passed: bool,
    /// Index into `results` of the first failing test, if any.
    pub first_failure_index: Option<usize>,
}

/// Maximum number of output lines to retain per test command.
const TAIL_LINES: usize = 50;

/// Execute each test command sequentially via `sh -c`, short-circuiting on the
/// first failure.
///
/// # Arguments
/// * `tests` – slice of shell command strings to run.
/// * `cwd` – working directory for each command.
///
/// # Returns
/// A [`TestResults`] summarising the run.
pub fn run_tests(tests: &[String], cwd: &Path) -> TestResults {
    let mut results: Vec<TestResult> = Vec::with_capacity(tests.len());
    let mut all_passed = true;
    let mut first_failure_index: Option<usize> = None;

    for (i, cmd) in tests.iter().enumerate() {
        let result = run_single_test(cmd, cwd);
        let passed = result.passed;
        results.push(result);

        if !passed {
            all_passed = false;
            first_failure_index = Some(i);
            break; // short-circuit
        }
    }

    TestResults {
        results,
        all_passed,
        first_failure_index,
    }
}

/// Run a single shell command and capture its output.
fn run_single_test(cmd: &str, cwd: &Path) -> TestResult {
    let output = Command::new("sh")
        .arg("-c")
        .arg(cmd)
        .current_dir(cwd)
        .output();

    match output {
        Ok(out) => {
            let exit_code = out.status.code();
            let passed = out.status.success();

            // Combine stdout and stderr, then keep only the last TAIL_LINES lines.
            let combined = {
                let mut buf = String::from_utf8_lossy(&out.stdout).into_owned();
                let stderr = String::from_utf8_lossy(&out.stderr);
                if !stderr.is_empty() {
                    if !buf.is_empty() && !buf.ends_with('\n') {
                        buf.push('\n');
                    }
                    buf.push_str(&stderr);
                }
                buf
            };

            let output_tail = tail_lines(&combined, TAIL_LINES);

            TestResult {
                command: cmd.to_string(),
                exit_code,
                output_tail,
                passed,
            }
        }
        Err(e) => {
            // Could not even spawn the process.
            TestResult {
                command: cmd.to_string(),
                exit_code: None,
                output_tail: format!("failed to execute command: {e}"),
                passed: false,
            }
        }
    }
}

/// Return the last `n` lines of `text` as a single string.
fn tail_lines(text: &str, n: usize) -> String {
    let lines: Vec<&str> = text.lines().collect();
    if lines.len() <= n {
        text.to_string()
    } else {
        lines[lines.len() - n..].join("\n")
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn cwd() -> PathBuf {
        std::env::current_dir().expect("current dir")
    }

    #[test]
    fn test_all_pass() {
        let tests = vec!["true".to_string(), "echo hello".to_string()];
        let res = run_tests(&tests, &cwd());
        assert!(res.all_passed);
        assert_eq!(res.results.len(), 2);
        assert!(res.first_failure_index.is_none());
        for r in &res.results {
            assert!(r.passed);
            assert_eq!(r.exit_code, Some(0));
        }
    }

    #[test]
    fn test_first_failure_short_circuits() {
        let tests = vec![
            "true".to_string(),
            "false".to_string(), // fails
            "echo should_not_run".to_string(),
        ];
        let res = run_tests(&tests, &cwd());
        assert!(!res.all_passed);
        // Only first two tests should have run.
        assert_eq!(res.results.len(), 2);
        assert_eq!(res.first_failure_index, Some(1));
        assert!(res.results[0].passed);
        assert!(!res.results[1].passed);
    }

    #[test]
    fn test_captures_output() {
        let tests = vec!["echo hello_world".to_string()];
        let res = run_tests(&tests, &cwd());
        assert!(res.all_passed);
        assert!(res.results[0].output_tail.contains("hello_world"));
    }

    #[test]
    fn test_captures_stderr() {
        let tests = vec!["echo err_output >&2".to_string()];
        let res = run_tests(&tests, &cwd());
        assert!(res.all_passed);
        assert!(res.results[0].output_tail.contains("err_output"));
    }

    #[test]
    fn test_exit_code_nonzero() {
        let tests = vec!["exit 42".to_string()];
        let res = run_tests(&tests, &cwd());
        assert!(!res.all_passed);
        assert_eq!(res.results[0].exit_code, Some(42));
        assert!(!res.results[0].passed);
    }

    #[test]
    fn test_tail_lines_truncation() {
        // Generate 100 lines; tail should keep only last 50.
        let many_lines: String = (0..100)
            .map(|i| format!("line{i}"))
            .collect::<Vec<_>>()
            .join("\n");
        let tail = tail_lines(&many_lines, 50);
        let lines: Vec<&str> = tail.lines().collect();
        assert_eq!(lines.len(), 50);
        assert!(lines[0].contains("line50"));
        assert!(lines[49].contains("line99"));
    }

    #[test]
    fn test_empty_tests() {
        let tests: Vec<String> = vec![];
        let res = run_tests(&tests, &cwd());
        assert!(res.all_passed);
        assert!(res.results.is_empty());
        assert!(res.first_failure_index.is_none());
    }

    #[test]
    fn test_respects_cwd() {
        let tests = vec!["pwd".to_string()];
        let dir = std::env::temp_dir();
        let res = run_tests(&tests, &dir);
        assert!(res.all_passed);
        // The output should contain the temp dir path (canonicalize to handle symlinks).
        let canonical = dir.canonicalize().unwrap();
        let output_canonical = PathBuf::from(res.results[0].output_tail.trim())
            .canonicalize()
            .unwrap_or_default();
        assert_eq!(output_canonical, canonical);
    }
}
