// `ralph config` subcommand family.
//
// Small CLI surface for viewing and mutating the global config file
// (`~/.config/ralph-rs/config.json`). Add mutators here as needed; the
// `Config::save` helper handles atomic pretty-printed writes.

use anyhow::{Context, Result, anyhow};
use std::str::FromStr;

use crate::config::{self, Config};
use crate::output::OutputContext;

/// Print the canonical config path and the effective values of the fields
/// users most commonly care about.
///
/// Intentionally narrow: we don't dump harnesses here because they are
/// verbose and already printable via `ralph init` / the raw file. If a
/// future diagnostic needs the full blob, add a `--json` branch.
pub fn config_show(out: &OutputContext) -> Result<()> {
    let config_path = config::config_dir()?.join("config.json");
    let config = config::load_or_create_config()?;

    if matches!(out.format, crate::output::OutputFormat::Json) {
        // When `--json` is active, return the full config as JSON so
        // tooling can scrape any field without a parser per field.
        let payload = serde_json::json!({
            "config_path": config_path.display().to_string(),
            "config": config,
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    println!("Config file: {}", config_path.display());
    println!();
    println!("  default_harness:     {}", config.default_harness);
    println!("  display_timezone:    {}", config.display_timezone);
    println!("  max_retries_per_step: {}", config.max_retries_per_step);
    println!(
        "  timeout_secs:        {}",
        config
            .timeout_secs
            .map(|n| n.to_string())
            .unwrap_or_else(|| "<none>".to_string())
    );
    println!("  hook_timeout_secs:   {}", config.hook_timeout_secs);
    println!("  auto_stash:          {}", config.auto_stash);
    println!("  min_free_disk_mb:    {}", config.min_free_disk_mb);
    let mut harness_names: Vec<&str> =
        config.harnesses.keys().map(String::as_str).collect();
    harness_names.sort_unstable();
    println!("  harnesses:           {}", harness_names.join(", "));

    Ok(())
}

/// Set the `display_timezone` field to `tz` and write the config back to
/// disk. Rejects invalid IANA names up front so a typo never corrupts the
/// on-disk config.
pub fn config_set_timezone(tz: &str) -> Result<()> {
    // Validate the IANA name first — we don't want to persist garbage.
    chrono_tz::Tz::from_str(tz).map_err(|e| {
        anyhow!(
            "'{tz}' is not a valid IANA timezone name: {e}. \
             Examples: America/New_York, Europe/London, Asia/Tokyo, UTC."
        )
    })?;

    let mut config: Config = config::load_or_create_config()?;
    config.display_timezone = tz.to_string();
    config
        .save()
        .context("Failed to persist updated config to disk")?;

    eprintln!("display_timezone set to '{tz}'.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{self, Config};
    use std::sync::{Mutex, MutexGuard};

    /// Serialize tests that mutate `$XDG_CONFIG_HOME`. Parallel mutation of a
    /// process-wide env var would race; a mutex keeps these tests honest
    /// without forcing the entire test binary to run single-threaded.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Point the config loader at `path` for the duration of the returned guard
    /// and restore the previous `XDG_CONFIG_HOME` on drop.
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
        // SAFETY: guarded by ENV_LOCK for the duration of the returned guard.
        unsafe { std::env::set_var("XDG_CONFIG_HOME", path) };
        XdgGuard { _lock: lock, prev }
    }

    #[test]
    fn test_set_timezone_persists_to_file() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let _guard = set_xdg(tmp.path());

        // First call creates the file with the default config.
        config_set_timezone("America/New_York").expect("set_timezone ok");

        let reloaded: Config = config::load_or_create_config().expect("reload");
        assert_eq!(reloaded.display_timezone, "America/New_York");

        // Verify pretty-printing survives the round trip (the file is
        // human-readable, not a blob).
        let config_path = tmp.path().join("ralph-rs").join("config.json");
        let contents = std::fs::read_to_string(&config_path).unwrap();
        assert!(contents.contains("\"display_timezone\": \"America/New_York\""));
        // Pretty-printed JSON has newlines; minified would be a single line.
        assert!(contents.contains('\n'));
    }

    #[test]
    fn test_set_timezone_rejects_invalid() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let _guard = set_xdg(tmp.path());

        let err = config_set_timezone("Not/A_Real_Zone")
            .expect_err("must reject invalid IANA name");
        let msg = format!("{err}");
        assert!(msg.contains("Not/A_Real_Zone"), "{msg}");
        assert!(msg.contains("IANA"), "{msg}");

        // On rejection, no config file should have been written — the
        // error must fire before any disk mutation.
        let config_path = tmp.path().join("ralph-rs").join("config.json");
        assert!(
            !config_path.exists(),
            "rejected set must not leave a config file behind"
        );
    }

    #[test]
    fn test_show_prints_config_path_and_effective_values() {
        // We don't intercept stdout here — just exercise the code path to
        // confirm it doesn't panic or fail on a freshly-created config,
        // and that the underlying load surfaces default values.
        let tmp = tempfile::tempdir().expect("tempdir");
        let _guard = set_xdg(tmp.path());

        let out = OutputContext {
            format: crate::output::OutputFormat::Plain,
            quiet: true,
            color: false,
        };
        config_show(&out).expect("config_show must succeed on a fresh config");

        // The show path loads-or-creates, so a config file now exists.
        let config_path = tmp.path().join("ralph-rs").join("config.json");
        assert!(config_path.exists(), "show must materialize a config");

        let loaded: Config = config::load_or_create_config().expect("reload");
        // Default is UTC — if that ever changes, this test breaks loudly.
        assert_eq!(loaded.display_timezone, "UTC");
    }
}
