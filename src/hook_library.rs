// Hook library: user-curated hook definitions loaded from disk.
//
// Hooks live at `<config_dir>/hooks/<name>.md`, one file per hook.
// Each file has YAML-ish frontmatter followed by the shell command body.
//
//     ---
//     name: claude-review
//     description: Review completed steps with Claude Code
//     lifecycle: post-step
//     scope: global
//     ---
//     claude -p "Review: $(git diff HEAD~1)"
//
// `scope` is either `global` or a list of absolute path prefixes:
//
//     scope:
//       paths:
//         - /home/me/projects/rust
//         - /home/me/work/backend
#![allow(dead_code)]

use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

use crate::config;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

/// A lifecycle event at which a hook can fire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Lifecycle {
    PreStep,
    PostStep,
    PreTest,
    PostTest,
}

impl Lifecycle {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::PreStep => "pre-step",
            Self::PostStep => "post-step",
            Self::PreTest => "pre-test",
            Self::PostTest => "post-test",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "pre-step" => Ok(Self::PreStep),
            "post-step" => Ok(Self::PostStep),
            "pre-test" => Ok(Self::PreTest),
            "post-test" => Ok(Self::PostTest),
            other => bail!("Unknown lifecycle '{other}' (expected pre-step, post-step, pre-test, post-test)"),
        }
    }
}

impl std::fmt::Display for Lifecycle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A hook's scope controls which project directories it applies to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case")]
pub enum Scope {
    /// Applies to every project.
    Global,
    /// Applies only when the project directory starts with one of these prefixes.
    Paths { paths: Vec<PathBuf> },
}

impl Scope {
    /// Returns true if this scope covers the given absolute project directory.
    pub fn matches(&self, project_dir: &Path) -> bool {
        match self {
            Self::Global => true,
            Self::Paths { paths } => paths.iter().any(|p| project_dir.starts_with(p)),
        }
    }
}

/// A single hook definition loaded from disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hook {
    pub name: String,
    #[serde(default)]
    pub description: String,
    pub lifecycle: Lifecycle,
    pub scope: Scope,
    /// The shell command to execute. Can be a single command or a multi-line
    /// shell script. Ralph runs it via `sh -c`.
    pub command: String,
}

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

/// Directory where hook files are stored.
pub fn hooks_dir() -> Result<PathBuf> {
    Ok(config::config_dir()?.join("hooks"))
}

fn hook_path(name: &str) -> Result<PathBuf> {
    Ok(hooks_dir()?.join(format!("{name}.md")))
}

// ---------------------------------------------------------------------------
// Parsing and serialization
// ---------------------------------------------------------------------------

/// Parse a hook file's contents into a Hook struct.
///
/// The file format is simple frontmatter delimited by `---` lines, followed
/// by the shell command body. Supported frontmatter keys:
///
/// ```text
/// name: <string>
/// description: <string>
/// lifecycle: pre-step | post-step | pre-test | post-test
/// scope: global
///   -- OR --
/// scope:
///   paths:
///     - /absolute/path/a
///     - /absolute/path/b
/// ```
///
/// The `name` field takes precedence over the filename if present.
pub fn parse_hook(contents: &str, fallback_name: &str) -> Result<Hook> {
    let trimmed = contents.trim_start();
    let rest = trimmed
        .strip_prefix("---")
        .context("Hook file must start with '---' frontmatter delimiter")?;
    let rest = rest.strip_prefix('\n').unwrap_or(rest);

    let end = rest
        .find("\n---")
        .context("Hook file missing closing '---' frontmatter delimiter")?;
    let frontmatter_str = &rest[..end];
    let body = rest[end + 4..].trim_start_matches('\n').trim_end();

    let mut name: Option<String> = None;
    let mut description = String::new();
    let mut lifecycle_str: Option<String> = None;
    let mut scope_kind: Option<String> = None;
    let mut scope_paths: Vec<PathBuf> = Vec::new();
    let mut in_scope_paths = false;

    for raw_line in frontmatter_str.lines() {
        // Strip comments.
        let line = match raw_line.find('#') {
            Some(i) => &raw_line[..i],
            None => raw_line,
        };

        if line.trim().is_empty() {
            continue;
        }

        // List item under `scope.paths`.
        if in_scope_paths {
            let t = line.trim_start();
            if let Some(item) = t.strip_prefix("- ") {
                let p = PathBuf::from(item.trim());
                if !p.is_absolute() {
                    bail!(
                        "Scope path '{}' must be absolute (no '~' expansion, no relative paths)",
                        p.display()
                    );
                }
                scope_paths.push(p);
                continue;
            }
            // Any non-list-item line ends the paths block.
            in_scope_paths = false;
        }

        // `  paths:` marker inside a block `scope:` value.
        let trimmed_line = line.trim_start();
        if trimmed_line.starts_with("paths:")
            && line.starts_with(' ')
            && scope_kind.as_deref() == Some("paths")
        {
            in_scope_paths = true;
            continue;
        }

        // Top-level `key: value` lines.
        let Some(colon) = line.find(':') else {
            continue;
        };
        let key = line[..colon].trim();
        let value = line[colon + 1..].trim();

        match key {
            "name" => name = Some(value.to_string()),
            "description" => description = unquote(value).to_string(),
            "lifecycle" => lifecycle_str = Some(value.to_string()),
            "scope" => {
                if value.is_empty() {
                    // Block scalar: expect `paths:` on next indented line.
                    scope_kind = Some("paths".to_string());
                } else if value == "global" {
                    scope_kind = Some("global".to_string());
                } else {
                    bail!(
                        "Unknown scope value '{value}' (expected 'global' or a paths block)"
                    );
                }
            }
            _ => {
                // Unknown top-level keys are ignored for forward compatibility.
            }
        }
    }

    let lifecycle_str = lifecycle_str.context("Hook frontmatter missing 'lifecycle'")?;
    let lifecycle = Lifecycle::parse(&lifecycle_str)?;

    let scope = match scope_kind.as_deref() {
        None | Some("global") => Scope::Global,
        Some("paths") => {
            if scope_paths.is_empty() {
                bail!("Hook scope 'paths' block is empty");
            }
            Scope::Paths { paths: scope_paths }
        }
        Some(other) => bail!("Unknown scope kind '{other}'"),
    };

    Ok(Hook {
        name: name.unwrap_or_else(|| fallback_name.to_string()),
        description,
        lifecycle,
        scope,
        command: body.to_string(),
    })
}

/// Strip surrounding single or double quotes from a YAML-ish scalar.
fn unquote(s: &str) -> &str {
    if s.len() >= 2 {
        let bytes = s.as_bytes();
        if (bytes[0] == b'"' && bytes[s.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[s.len() - 1] == b'\'')
        {
            return &s[1..s.len() - 1];
        }
    }
    s
}

/// Serialize a Hook back to the file format used by the library.
pub fn serialize_hook(hook: &Hook) -> String {
    let mut out = String::new();
    out.push_str("---\n");
    out.push_str(&format!("name: {}\n", hook.name));
    if !hook.description.is_empty() {
        out.push_str(&format!(
            "description: {}\n",
            yaml_escape(&hook.description)
        ));
    }
    out.push_str(&format!("lifecycle: {}\n", hook.lifecycle));
    match &hook.scope {
        Scope::Global => out.push_str("scope: global\n"),
        Scope::Paths { paths } => {
            out.push_str("scope:\n  paths:\n");
            for p in paths {
                out.push_str(&format!("    - {}\n", p.display()));
            }
        }
    }
    out.push_str("---\n");
    out.push_str(&hook.command);
    if !hook.command.ends_with('\n') {
        out.push('\n');
    }
    out
}

/// Minimal escaping for YAML scalars — wraps in double quotes if the string
/// contains anything that would confuse a YAML parser.
fn yaml_escape(s: &str) -> String {
    if s.chars()
        .any(|c| c == ':' || c == '#' || c == '\n' || c == '"' || c == '\'')
    {
        format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""))
    } else {
        s.to_string()
    }
}

// ---------------------------------------------------------------------------
// Library operations
// ---------------------------------------------------------------------------

/// Load every hook in the library. Invalid files are skipped with a warning
/// on stderr so one bad file doesn't take down the whole library.
pub fn load_all() -> Result<Vec<Hook>> {
    let dir = hooks_dir()?;
    if !dir.exists() {
        return Ok(Vec::new());
    }

    let mut hooks = Vec::new();
    let mut entries: Vec<_> = fs::read_dir(&dir)
        .with_context(|| format!("Failed to read hooks directory {}", dir.display()))?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "md"))
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let path = entry.path();
        let fallback = path
            .file_stem()
            .map(|s| s.to_string_lossy().into_owned())
            .unwrap_or_default();
        match fs::read_to_string(&path) {
            Ok(contents) => match parse_hook(&contents, &fallback) {
                Ok(hook) => hooks.push(hook),
                Err(e) => eprintln!("Warning: skipping hook {}: {e}", path.display()),
            },
            Err(e) => eprintln!("Warning: could not read hook {}: {e}", path.display()),
        }
    }

    Ok(hooks)
}

/// Load a single hook by name.
pub fn load(name: &str) -> Result<Hook> {
    let path = hook_path(name)?;
    if !path.exists() {
        bail!("Hook not found: {name}");
    }
    let contents = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read hook {}", path.display()))?;
    parse_hook(&contents, name)
}

/// Try to load a hook by name, returning `None` if it doesn't exist.
pub fn try_load(name: &str) -> Result<Option<Hook>> {
    let path = hook_path(name)?;
    if !path.exists() {
        return Ok(None);
    }
    let contents = fs::read_to_string(&path)
        .with_context(|| format!("Failed to read hook {}", path.display()))?;
    Ok(Some(parse_hook(&contents, name)?))
}

/// Save a hook to disk. Fails if a hook with the same name already exists
/// and `force` is false.
pub fn save(hook: &Hook, force: bool) -> Result<PathBuf> {
    let dir = hooks_dir()?;
    fs::create_dir_all(&dir)
        .with_context(|| format!("Failed to create hooks directory {}", dir.display()))?;
    let path = dir.join(format!("{}.md", hook.name));
    if path.exists() && !force {
        bail!(
            "Hook '{}' already exists at {}. Use --force to overwrite.",
            hook.name,
            path.display()
        );
    }
    fs::write(&path, serialize_hook(hook))
        .with_context(|| format!("Failed to write hook {}", path.display()))?;
    Ok(path)
}

/// Delete a hook from the library.
pub fn delete(name: &str) -> Result<()> {
    let path = hook_path(name)?;
    if !path.exists() {
        bail!("Hook not found: {name}");
    }
    fs::remove_file(&path)
        .with_context(|| format!("Failed to delete hook {}", path.display()))?;
    Ok(())
}

/// Return only the hooks whose scope matches the given project directory.
pub fn filter_by_project(hooks: Vec<Hook>, project_dir: &Path) -> Vec<Hook> {
    hooks
        .into_iter()
        .filter(|h| h.scope.matches(project_dir))
        .collect()
}

// ---------------------------------------------------------------------------
// Bundle (export/import)
// ---------------------------------------------------------------------------

/// The on-disk format for an exported bundle of hooks.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HookBundle {
    pub ralph_rs_version: String,
    pub exported_at: String,
    pub hooks: Vec<Hook>,
}

impl HookBundle {
    pub fn new(hooks: Vec<Hook>) -> Self {
        Self {
            ralph_rs_version: env!("CARGO_PKG_VERSION").to_string(),
            exported_at: chrono::Utc::now().to_rfc3339(),
            hooks,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_global_hook() {
        let src = "---\nname: claude-review\ndescription: Review with Claude\nlifecycle: post-step\nscope: global\n---\nclaude -p \"review this\"\n";
        let hook = parse_hook(src, "fallback").unwrap();
        assert_eq!(hook.name, "claude-review");
        assert_eq!(hook.description, "Review with Claude");
        assert_eq!(hook.lifecycle, Lifecycle::PostStep);
        assert_eq!(hook.scope, Scope::Global);
        assert_eq!(hook.command, "claude -p \"review this\"");
    }

    #[test]
    fn test_parse_path_scoped_hook() {
        let src = "---\nname: rust-clippy\nlifecycle: post-step\nscope:\n  paths:\n    - /home/me/projects/rust\n    - /home/me/work/backend\n---\ncargo clippy -- -D warnings\n";
        let hook = parse_hook(src, "fallback").unwrap();
        assert_eq!(hook.name, "rust-clippy");
        match hook.scope {
            Scope::Paths { paths } => {
                assert_eq!(paths.len(), 2);
                assert_eq!(paths[0], PathBuf::from("/home/me/projects/rust"));
                assert_eq!(paths[1], PathBuf::from("/home/me/work/backend"));
            }
            _ => panic!("expected path scope"),
        }
    }

    #[test]
    fn test_parse_rejects_relative_paths() {
        let src = "---\nname: bad\nlifecycle: post-step\nscope:\n  paths:\n    - ~/projects\n---\necho hi\n";
        let result = parse_hook(src, "bad");
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("absolute"), "got: {err}");
    }

    #[test]
    fn test_parse_uses_fallback_name() {
        let src = "---\nlifecycle: pre-step\nscope: global\n---\necho hi\n";
        let hook = parse_hook(src, "from-filename").unwrap();
        assert_eq!(hook.name, "from-filename");
    }

    #[test]
    fn test_parse_rejects_unknown_lifecycle() {
        let src = "---\nname: x\nlifecycle: middle-step\nscope: global\n---\necho hi\n";
        assert!(parse_hook(src, "x").is_err());
    }

    #[test]
    fn test_parse_rejects_missing_frontmatter() {
        let src = "claude -p 'hi'\n";
        assert!(parse_hook(src, "x").is_err());
    }

    #[test]
    fn test_scope_matches_global() {
        let scope = Scope::Global;
        assert!(scope.matches(Path::new("/any/path")));
    }

    #[test]
    fn test_scope_matches_prefix() {
        let scope = Scope::Paths {
            paths: vec![
                PathBuf::from("/home/me/projects/rust"),
                PathBuf::from("/tmp/foo"),
            ],
        };
        assert!(scope.matches(Path::new("/home/me/projects/rust/my-app")));
        assert!(scope.matches(Path::new("/home/me/projects/rust")));
        assert!(scope.matches(Path::new("/tmp/foo/bar")));
        assert!(!scope.matches(Path::new("/home/me/projects/js/my-app")));
        assert!(!scope.matches(Path::new("/tmp/other")));
    }

    #[test]
    fn test_roundtrip_global() {
        let hook = Hook {
            name: "test".to_string(),
            description: "A test hook".to_string(),
            lifecycle: Lifecycle::PostStep,
            scope: Scope::Global,
            command: "echo hello\n".to_string(),
        };
        let serialized = serialize_hook(&hook);
        let parsed = parse_hook(&serialized, "test").unwrap();
        assert_eq!(parsed.name, hook.name);
        assert_eq!(parsed.description, hook.description);
        assert_eq!(parsed.lifecycle, hook.lifecycle);
        assert_eq!(parsed.scope, hook.scope);
        assert_eq!(parsed.command.trim(), hook.command.trim());
    }

    #[test]
    fn test_roundtrip_paths() {
        let hook = Hook {
            name: "scoped".to_string(),
            description: String::new(),
            lifecycle: Lifecycle::PreStep,
            scope: Scope::Paths {
                paths: vec![PathBuf::from("/a/b"), PathBuf::from("/c/d")],
            },
            command: "ls -la".to_string(),
        };
        let serialized = serialize_hook(&hook);
        let parsed = parse_hook(&serialized, "scoped").unwrap();
        assert_eq!(parsed.scope, hook.scope);
    }

    #[test]
    fn test_yaml_escape_plain() {
        assert_eq!(yaml_escape("hello world"), "hello world");
    }

    #[test]
    fn test_yaml_escape_with_colon() {
        assert_eq!(yaml_escape("a: b"), "\"a: b\"");
    }

    #[test]
    fn test_filter_by_project() {
        let hooks = vec![
            Hook {
                name: "global-one".to_string(),
                description: String::new(),
                lifecycle: Lifecycle::PostStep,
                scope: Scope::Global,
                command: "x".to_string(),
            },
            Hook {
                name: "rust-only".to_string(),
                description: String::new(),
                lifecycle: Lifecycle::PostStep,
                scope: Scope::Paths {
                    paths: vec![PathBuf::from("/home/me/rust")],
                },
                command: "y".to_string(),
            },
        ];
        let filtered = filter_by_project(hooks.clone(), Path::new("/home/me/rust/project"));
        assert_eq!(filtered.len(), 2);

        let filtered = filter_by_project(hooks, Path::new("/home/me/js/project"));
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].name, "global-one");
    }

    #[test]
    fn test_lifecycle_parse() {
        assert_eq!(Lifecycle::parse("pre-step").unwrap(), Lifecycle::PreStep);
        assert_eq!(Lifecycle::parse("post-step").unwrap(), Lifecycle::PostStep);
        assert_eq!(Lifecycle::parse("pre-test").unwrap(), Lifecycle::PreTest);
        assert_eq!(Lifecycle::parse("post-test").unwrap(), Lifecycle::PostTest);
        assert!(Lifecycle::parse("garbage").is_err());
    }
}
