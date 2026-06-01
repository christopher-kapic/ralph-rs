//! End-to-end regression tests for two `ralph step add` DAG-authoring bugs
//! found in review of the DAG-redesign branch:
//!
//!   P1 — `--import-json` reported the wrong `short_id` when the payload
//!        pinned one. `storage::create_step` returns a `Step` carrying a
//!        freshly-minted throwaway `short_id`; `set_step_short_id` updated
//!        only the DB row, so the success/JSON output handed the user an id
//!        that did not exist. An explicitly supplied (well-shaped) short_id
//!        is still honored, so this is user-visible. (Also: `StepSummary`
//!        now carries `short_id`, so the `--json` surface exposes the handle
//!        at all — previously it omitted it entirely.)
//!
//!   P2 — `--after X --before Y` is documented to reroute the *specific*
//!        X→Y edge through the new step. When no such edge existed the
//!        splice silently succeeded, inventing `new→X` / `Y→new` ordering
//!        constraints and serializing two unrelated branches. It must now
//!        bail unless Y directly depends on X.
//!
//!   P3 — `--import-json` overloaded `short_id` as both the persisted
//!        handle and the intra-payload `depends_on` wiring key, and the
//!        docs taught readable values (`"parser"`). `resolve_step` only
//!        accepts an 8-char base-62 token, so a readable `short_id` was
//!        created-but-unselectable and a numeric one shadowed a step
//!        position. The split: a batch-local `id` wires `depends_on`
//!        (never persisted); an explicit `short_id` is now validated
//!        `is_short_id_shaped` (omit it ⇒ ralph mints a selectable one).
//!        The same shape guard is enforced for full `ralph import`
//!        bundles (a hand-edit/tamper guard — real exports always pass).
//!
//! These drive the real binary so they cover the actual CLI/JSON surface a
//! plan-authoring agent or script sees.

use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};

fn ralph_bin() -> &'static str {
    env!("CARGO_BIN_EXE_ralph")
}

fn env_home(proj: &Path) -> std::path::PathBuf {
    proj.parent().unwrap().join("home")
}
fn env_xdg_config(proj: &Path) -> std::path::PathBuf {
    env_home(proj).join(".config")
}
fn env_xdg_data(proj: &Path) -> std::path::PathBuf {
    env_home(proj).join(".local").join("share")
}

fn run_check(cmd: &mut Command, label: &str) {
    let out = cmd
        .output()
        .unwrap_or_else(|e| panic!("{label}: spawn: {e}"));
    assert!(
        out.status.success(),
        "{label} failed: status={:?}\nstdout: {}\nstderr: {}",
        out.status,
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr),
    );
}

/// `ralph <args>` against the project with HOME/XDG redirected into the
/// tempdir. Returns the completed output (caller asserts status/streams).
fn ralph(proj: &Path, args: &[&str]) -> std::process::Output {
    Command::new(ralph_bin())
        .args(["-C"])
        .arg(proj)
        .args(args)
        .env("HOME", env_home(proj))
        .env("XDG_CONFIG_HOME", env_xdg_config(proj))
        .env("XDG_DATA_HOME", env_xdg_data(proj))
        .output()
        .expect("spawn ralph")
}

/// Fresh ralph project with one empty (unapproved) plan `p`. `step add`
/// does not need an approved plan, so we skip `plan approve`.
fn setup_project() -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().expect("create tempdir");
    let home = dir.path().join("home");
    let proj = dir.path().join("proj");
    std::fs::create_dir_all(&home).unwrap();
    std::fs::create_dir_all(&proj).unwrap();

    run_check(
        Command::new("git")
            .args(["init", "-q", "-b", "main"])
            .arg(&proj),
        "git init",
    );
    run_check(
        Command::new("git")
            .args(["config", "user.email", "ralph-test@example.com"])
            .current_dir(&proj),
        "git config email",
    );
    run_check(
        Command::new("git")
            .args(["config", "user.name", "ralph-test"])
            .current_dir(&proj),
        "git config name",
    );
    std::fs::write(proj.join("README"), "test").unwrap();
    run_check(
        Command::new("git").args(["add", "."]).current_dir(&proj),
        "git add",
    );
    run_check(
        Command::new("git")
            .args(["commit", "-q", "-m", "init"])
            .current_dir(&proj),
        "git commit",
    );

    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["init", "--non-interactive", "--default-harness", "claude"])
            .env("HOME", home.clone())
            .env("XDG_CONFIG_HOME", home.join(".config"))
            .env("XDG_DATA_HOME", home.join(".local").join("share")),
        "ralph init",
    );
    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["plan", "create", "p", "-d", "test"])
            .env("HOME", home.clone())
            .env("XDG_CONFIG_HOME", home.join(".config"))
            .env("XDG_DATA_HOME", home.join(".local").join("share")),
        "plan create",
    );

    (dir, proj)
}

/// `ralph step list p --json` → the short_id of the step whose title matches.
fn short_id_of(proj: &Path, title: &str) -> String {
    let out = ralph(proj, &["step", "list", "p", "--json"]);
    assert!(out.status.success(), "step list --json failed");
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("step list json");
    v.as_array()
        .expect("array")
        .iter()
        .find(|s| s["title"] == title)
        .unwrap_or_else(|| panic!("no step titled {title}"))["short_id"]
        .as_str()
        .expect("short_id string")
        .to_string()
}

// --------------------------------------------------------------------------
// P1
// --------------------------------------------------------------------------

#[test]
fn import_json_reports_pinned_short_id() {
    let (_dir, proj) = setup_project();

    // Two steps, both pinning a caller-chosen short_id, the child wired to
    // the parent by that pinned id (the documented DAG-authoring path).
    let payload = r#"[
        {"title": "Root step",  "short_id": "rootaaaa"},
        {"title": "Child step", "short_id": "childbbb", "depends_on": ["rootaaaa"]}
    ]"#;

    let mut child = Command::new(ralph_bin())
        .args(["-C"])
        .arg(&proj)
        .args(["step", "add", "--import-json", "-", "p", "--json"])
        .env("HOME", env_home(&proj))
        .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
        .env("XDG_DATA_HOME", env_xdg_data(&proj))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn step add --import-json");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(payload.as_bytes())
        .unwrap();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.success(),
        "import failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );

    // The emitted JSON summaries must report the *pinned* ids — the P1 bug
    // reported `create_step`'s throwaway minted id here instead.
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).expect("summaries json");
    let arr = v.as_array().expect("array of summaries");
    let sid = |title: &str| -> &str {
        arr.iter()
            .find(|s| s["title"] == title)
            .unwrap_or_else(|| panic!("missing {title}"))["short_id"]
            .as_str()
            .expect("short_id")
    };
    assert_eq!(sid("Root step"), "rootaaaa");
    assert_eq!(sid("Child step"), "childbbb");

    // And the pinned handles must actually resolve in the DB (so a follow-up
    // `step add --after rootaaaa` works as the reported id implies).
    assert_eq!(short_id_of(&proj, "Root step"), "rootaaaa");
    assert_eq!(short_id_of(&proj, "Child step"), "childbbb");
}

// --------------------------------------------------------------------------
// P2
// --------------------------------------------------------------------------

#[test]
fn splice_without_xy_edge_is_rejected() {
    let (_dir, proj) = setup_project();

    // Two independent roots (Beta does NOT depend on Alpha).
    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["step", "add", "Alpha", "p"])
            .env("HOME", env_home(&proj))
            .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
            .env("XDG_DATA_HOME", env_xdg_data(&proj)),
        "add Alpha",
    );
    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["step", "add", "Beta", "p", "--root"])
            .env("HOME", env_home(&proj))
            .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
            .env("XDG_DATA_HOME", env_xdg_data(&proj)),
        "add Beta",
    );

    let alpha = short_id_of(&proj, "Alpha");
    let beta = short_id_of(&proj, "Beta");

    // Splicing across a non-existent Alpha→Beta edge must fail loudly.
    let out = ralph(
        &proj,
        &[
            "step", "add", "Gamma", "p", "--after", &alpha, "--before", &beta,
        ],
    );
    assert!(
        !out.status.success(),
        "splice without an X→Y edge must fail, but it succeeded"
    );
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("does not depend on") && stderr.contains("edge to reroute"),
        "unexpected error message: {stderr}"
    );

    // Nothing was added — still exactly Alpha and Beta.
    let list = ralph(&proj, &["step", "list", "p", "--json"]);
    let v: serde_json::Value = serde_json::from_slice(&list.stdout).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 2, "Gamma must not be added");
}

#[test]
fn valid_splice_reroutes_xy_edge() {
    let (_dir, proj) = setup_project();

    // Alpha (root) ← Beta (Beta depends on Alpha), so the Alpha→Beta edge
    // exists and the splice is legal.
    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["step", "add", "Alpha", "p"])
            .env("HOME", env_home(&proj))
            .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
            .env("XDG_DATA_HOME", env_xdg_data(&proj)),
        "add Alpha",
    );
    let alpha = short_id_of(&proj, "Alpha");
    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["step", "add", "Beta", "p", "--after", &alpha])
            .env("HOME", env_home(&proj))
            .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
            .env("XDG_DATA_HOME", env_xdg_data(&proj)),
        "add Beta",
    );
    let beta = short_id_of(&proj, "Beta");

    // Splice Gamma between Alpha and Beta.
    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args([
                "step", "add", "Gamma", "p", "--after", &alpha, "--before", &beta,
            ])
            .env("HOME", env_home(&proj))
            .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
            .env("XDG_DATA_HOME", env_xdg_data(&proj)),
        "splice Gamma",
    );
    let gamma = short_id_of(&proj, "Gamma");

    // Beta now depends on Gamma (not Alpha); Gamma depends on Alpha.
    let beta_deps = ralph(&proj, &["step", "dependency", "list", &beta, "p", "--json"]);
    let bd: serde_json::Value = serde_json::from_slice(&beta_deps.stdout).unwrap();
    let bd: Vec<String> = bd["depends_on"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect();
    assert_eq!(bd, vec![gamma.clone()], "Beta should depend only on Gamma");

    let gamma_deps = ralph(
        &proj,
        &["step", "dependency", "list", &gamma, "p", "--json"],
    );
    let gd: serde_json::Value = serde_json::from_slice(&gamma_deps.stdout).unwrap();
    let gd: Vec<String> = gd["depends_on"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect();
    assert_eq!(gd, vec![alpha], "Gamma should depend on Alpha");
}

// --------------------------------------------------------------------------
// P3
// --------------------------------------------------------------------------

/// `ralph step add --import-json -` with `payload` on stdin.
fn import_json(proj: &Path, payload: &str) -> std::process::Output {
    let mut child = Command::new(ralph_bin())
        .args(["-C"])
        .arg(proj)
        .args(["step", "add", "--import-json", "-", "p", "--json"])
        .env("HOME", env_home(proj))
        .env("XDG_CONFIG_HOME", env_xdg_config(proj))
        .env("XDG_DATA_HOME", env_xdg_data(proj))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn step add --import-json");
    child
        .stdin
        .take()
        .unwrap()
        .write_all(payload.as_bytes())
        .unwrap();
    child.wait_with_output().unwrap()
}

fn step_count(proj: &Path) -> usize {
    let out = ralph(proj, &["step", "list", "p", "--json"]);
    let v: serde_json::Value = serde_json::from_slice(&out.stdout).unwrap();
    v.as_array().unwrap().len()
}

/// The documented path: each step carries a readable batch-local `id`,
/// `depends_on` wires by those `id`s, no `short_id` is supplied. The DAG
/// must be built, the `id`s must NOT leak as the persisted handle, and the
/// minted `short_id`s must actually be selectable afterwards.
#[test]
fn import_json_id_wires_dag_and_mints_selectable_short_id() {
    let (_dir, proj) = setup_project();

    // `integrate` forward-references `codegen` (defined after it) to also
    // cover forward `id` references resolving across the whole payload.
    let payload = r#"[
        {"id": "parser",    "title": "Parser"},
        {"id": "integrate", "title": "Integrate", "depends_on": ["parser", "codegen"]},
        {"id": "codegen",   "title": "Codegen"}
    ]"#;

    let out = import_json(&proj, payload);
    assert!(
        out.status.success(),
        "id-wired import must succeed: stderr={}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(step_count(&proj), 3);

    // The readable `id` is a wiring label only — it must NOT become the
    // persisted short_id. Every minted short_id is `is_short_id_shaped`
    // (8 base-62 chars) and therefore selectable.
    for title in ["Parser", "Integrate", "Codegen"] {
        let sid = short_id_of(&proj, title);
        assert_ne!(sid, title.to_lowercase(), "id leaked as short_id");
        assert_eq!(sid.len(), 8, "minted short_id must be 8 chars: {sid:?}");
        assert!(
            sid.bytes().all(|b| b.is_ascii_alphanumeric()),
            "minted short_id must be base-62: {sid:?}"
        );
        // Selectable by the minted handle (the bug was that the advertised
        // handle could not be resolved by any later CLI command).
        let dep = ralph(&proj, &["step", "dependency", "list", &sid, "p", "--json"]);
        assert!(
            dep.status.success(),
            "minted short_id {sid} must resolve: stderr={}",
            String::from_utf8_lossy(&dep.stderr)
        );
    }

    // Edges wired by `id` landed on the minted handles: Integrate depends
    // on both Parser and Codegen.
    let integrate = short_id_of(&proj, "Integrate");
    let parser = short_id_of(&proj, "Parser");
    let codegen = short_id_of(&proj, "Codegen");
    let deps = ralph(
        &proj,
        &["step", "dependency", "list", &integrate, "p", "--json"],
    );
    let v: serde_json::Value = serde_json::from_slice(&deps.stdout).unwrap();
    let mut got: Vec<String> = v["depends_on"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x.as_str().unwrap().to_string())
        .collect();
    got.sort();
    let mut want = vec![parser, codegen];
    want.sort();
    assert_eq!(got, want, "Integrate must depend on Parser+Codegen");
}

/// A readable explicit `short_id` (the old advertised value) is now
/// rejected pre-DB with a message pointing at `id`, and nothing is written.
#[test]
fn import_json_readable_short_id_rejected() {
    let (_dir, proj) = setup_project();
    let out = import_json(&proj, r#"[{"title": "X", "short_id": "parser"}]"#);
    assert!(!out.status.success(), "readable short_id must be rejected");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("invalid `short_id`")
            && stderr.contains("8 base-62")
            && stderr.contains("`id`"),
        "message must explain the rule and point at `id`: {stderr}"
    );
    assert_eq!(step_count(&proj), 0, "nothing must be written");
}

/// A numeric explicit `short_id` is the dangerous case (it would shadow a
/// step position) and must also be rejected.
#[test]
fn import_json_numeric_short_id_rejected() {
    let (_dir, proj) = setup_project();
    let out = import_json(&proj, r#"[{"title": "X", "short_id": "1"}]"#);
    assert!(!out.status.success(), "numeric short_id must be rejected");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("invalid `short_id`"),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(step_count(&proj), 0);
}

/// Even a well-shaped 8-digit `short_id` must be rejected: persisted short ids
/// cannot shadow positional step selectors.
#[test]
fn import_json_eight_digit_short_id_rejected() {
    let (_dir, proj) = setup_project();
    let out = import_json(&proj, r#"[{"title": "X", "short_id": "00000001"}]"#);
    assert!(!out.status.success(), "8-digit short_id must be rejected");
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("invalid `short_id`"),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(step_count(&proj), 0);
}

/// Batch-local `id` labels are authoring-only handles and must not look like
/// real step selectors, or `depends_on` could silently shadow existing steps.
#[test]
fn import_json_selector_shaped_id_rejected() {
    let (_dir, proj) = setup_project();
    let out = import_json(&proj, r#"[{"title": "X", "id": "00000001"}]"#);
    assert!(
        !out.status.success(),
        "selector-shaped batch id must be rejected"
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("invalid `id`"),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(step_count(&proj), 0);
}

/// Full-bundle `ralph import` enforces the same shape guard: a real export
/// round-trips, but the same bundle with a hand-tampered readable short_id
/// is rejected (and imports no partial plan).
#[test]
fn full_import_rejects_tampered_readable_short_id() {
    let (_dir, proj) = setup_project();

    // Two real steps so the export is a genuine DAG-aware bundle.
    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["step", "add", "First", "p"])
            .env("HOME", env_home(&proj))
            .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
            .env("XDG_DATA_HOME", env_xdg_data(&proj)),
        "add First",
    );
    let first = short_id_of(&proj, "First");
    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["step", "add", "Second", "p", "--after", &first])
            .env("HOME", env_home(&proj))
            .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
            .env("XDG_DATA_HOME", env_xdg_data(&proj)),
        "add Second",
    );

    let bundle = proj.join("bundle.json");
    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["export", "p", "-o"])
            .arg(&bundle)
            .env("HOME", env_home(&proj))
            .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
            .env("XDG_DATA_HOME", env_xdg_data(&proj)),
        "export",
    );

    // Clean re-import works (sanity: the export is well-shaped).
    run_check(
        Command::new(ralph_bin())
            .args(["-C"])
            .arg(&proj)
            .args(["import"])
            .arg(&bundle)
            .args(["--slug", "clean"])
            .env("HOME", env_home(&proj))
            .env("XDG_CONFIG_HOME", env_xdg_config(&proj))
            .env("XDG_DATA_HOME", env_xdg_data(&proj)),
        "clean import",
    );

    // Tamper a leaf step's short_id to a non-shaped value and re-import.
    // (Second is a leaf — nothing depends on it — so this isolates the
    // shape guard from the dangling-edge rule. `"bad"` is < 8 chars, so
    // `is_short_id_shaped` rejects it.)
    let mut data: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&bundle).unwrap()).unwrap();
    data["steps"][1]["short_id"] = serde_json::json!("bad");
    let tampered = proj.join("tampered.json");
    std::fs::write(&tampered, serde_json::to_string(&data).unwrap()).unwrap();

    let out = ralph(
        &proj,
        &["import", tampered.to_str().unwrap(), "--slug", "tampered"],
    );
    assert!(
        !out.status.success(),
        "a bundle with a non-shaped short_id must be rejected"
    );
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("invalid short_id"),
        "stderr: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    // The rejected slug must not exist.
    let plans = ralph(&proj, &["plan", "list", "--all", "--json"]);
    let pv: serde_json::Value = serde_json::from_slice(&plans.stdout).unwrap();
    let has_tampered = pv
        .as_array()
        .map(|a| a.iter().any(|p| p["slug"] == "tampered"))
        .unwrap_or(false);
    assert!(!has_tampered, "no partial plan must be written");
}
