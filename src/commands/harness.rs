// `ralph harness …` subcommand family.
//
// Read-only inspection surface for the configured harnesses. Distinct from
// `ralph plan harness …`, which manages the plan-generation harness for a
// specific plan.

use anyhow::{Result, anyhow};

use crate::config::{self, Config, HarnessConfig};
use crate::output::OutputContext;
use crate::preflight;

/// Print a table of all configured harnesses with on-PATH status, a one-line
/// safety summary (sandbox value for codex, permission mode for claude, etc.)
/// and a footgun count. Designed to be the first thing a user runs when
/// asking "is my codex going to actually be able to write files?".
pub fn harness_list(config: &Config, json: bool, _out: &OutputContext) -> Result<()> {
    let mut names: Vec<&str> = config.harnesses.keys().map(String::as_str).collect();
    names.sort_unstable();

    if json {
        let payload: Vec<serde_json::Value> = names
            .iter()
            .map(|name| {
                let hc = &config.harnesses[*name];
                let on_path = preflight::is_binary_available(&hc.command);
                let footguns = config::harness_footguns(name, hc);
                serde_json::json!({
                    "name": name,
                    "command": hc.command,
                    "on_path": on_path,
                    "default_harness": *name == config.default_harness,
                    "safety": config::harness_safety_summary(hc),
                    "footguns": footguns,
                })
            })
            .collect();
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    // Plain-text table. Width-aware enough for the common case; heavy
    // stylistic alignment isn't worth a tabwriter dep.
    println!(
        "{:<22} {:<14} {:<8} {:<22} NOTES",
        "NAME", "COMMAND", "ON-PATH", "SAFETY"
    );
    for name in &names {
        let hc = &config.harnesses[*name];
        let on_path = if preflight::is_binary_available(&hc.command) {
            "yes"
        } else {
            "no"
        };
        let safety = config::harness_safety_summary(hc);
        let footguns = config::harness_footguns(name, hc);
        let mut notes = Vec::new();
        if *name == config.default_harness {
            notes.push("default".to_string());
        }
        if !footguns.is_empty() {
            notes.push(format!("⚠ {} footgun(s)", footguns.len()));
        }
        let notes_str = notes.join(", ");
        println!(
            "{:<22} {:<14} {:<8} {:<22} {}",
            name, hc.command, on_path, safety, notes_str
        );
    }

    // Print any footgun details after the table so the user sees the
    // remediation text inline. `ralph harness list` is the discovery path
    // for "why is my run silently doing nothing?" — burying these details
    // behind another command would defeat that.
    let mut any_footgun = false;
    for name in &names {
        let issues = config::harness_footguns(name, &config.harnesses[*name]);
        if !issues.is_empty() && !any_footgun {
            println!();
            any_footgun = true;
        }
        for issue in &issues {
            println!("  ⚠ {issue}");
        }
    }

    Ok(())
}

/// Pretty-print the full configuration of a single harness, plus on-PATH
/// status and any footgun warnings. JSON form just dumps the underlying
/// `HarnessConfig` as-stored.
pub fn harness_show(
    config: &Config,
    name: &str,
    json: bool,
    _out: &OutputContext,
) -> Result<()> {
    let hc: &HarnessConfig = config.harnesses.get(name).ok_or_else(|| {
        let mut available: Vec<&str> = config.harnesses.keys().map(String::as_str).collect();
        available.sort_unstable();
        anyhow!(
            "harness `{name}` is not configured (available: {})",
            available.join(", ")
        )
    })?;

    if json {
        let payload = serde_json::json!({
            "name": name,
            "default_harness": name == config.default_harness,
            "on_path": preflight::is_binary_available(&hc.command),
            "safety": config::harness_safety_summary(hc),
            "footguns": config::harness_footguns(name, hc),
            "config": hc,
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    println!("Harness: {name}");
    if name == config.default_harness {
        println!("  (default harness)");
    }
    println!();
    println!("  command:              {}", hc.command);
    println!(
        "  on PATH:              {}",
        if preflight::is_binary_available(&hc.command) {
            "yes"
        } else {
            "no"
        }
    );
    println!("  safety:               {}", config::harness_safety_summary(hc));
    println!("  args:                 {}", format_arg_vec(&hc.args));
    println!("  plan_args:            {}", format_arg_vec(&hc.plan_args));
    println!(
        "  prompt_input:         {}",
        format!("{:?}", hc.prompt_input).to_lowercase()
    );
    println!("  supports_agent_file:  {}", hc.supports_agent_file);
    if let Some(env) = &hc.agent_file_env {
        println!("  agent_file_env:       {env}");
    }
    if !hc.agent_file_args.is_empty() {
        println!(
            "  agent_file_args:      {}",
            format_arg_vec(&hc.agent_file_args)
        );
    }
    println!("  supports_json_output: {}", hc.supports_json_output);
    if !hc.json_output_args.is_empty() {
        println!(
            "  json_output_args:     {}",
            format_arg_vec(&hc.json_output_args)
        );
    }
    if !hc.model_args.is_empty() {
        println!("  model_args:           {}", format_arg_vec(&hc.model_args));
    }
    if let Some(model) = &hc.default_model {
        println!("  default_model:        {model}");
    }
    if !hc.auth_env_vars.is_empty() {
        println!(
            "  auth_env_vars:        {}",
            hc.auth_env_vars.join(", ")
        );
    }
    if !hc.auth_probe_args.is_empty() {
        println!(
            "  auth_probe_args:      {}",
            format_arg_vec(&hc.auth_probe_args)
        );
    }
    if let Some(color) = &hc.color {
        println!("  color:                {color}");
    }

    let footguns = config::harness_footguns(name, hc);
    if !footguns.is_empty() {
        println!();
        for issue in &footguns {
            println!("  ⚠ {issue}");
        }
    }

    Ok(())
}

fn format_arg_vec(v: &[String]) -> String {
    if v.is_empty() {
        "[]".to_string()
    } else {
        v.iter()
            .map(|a| {
                if a.contains(char::is_whitespace) {
                    format!("\"{a}\"")
                } else {
                    a.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::output::OutputFormat;

    fn out() -> OutputContext {
        OutputContext {
            format: OutputFormat::Plain,
            quiet: true,
            color: false,
        }
    }

    #[test]
    fn list_succeeds_on_default_config() {
        let config = Config::default();
        harness_list(&config, false, &out()).expect("list ok");
        harness_list(&config, true, &out()).expect("list json ok");
    }

    #[test]
    fn show_succeeds_on_known_harness() {
        let config = Config::default();
        harness_show(&config, "codex", false, &out()).expect("show plain ok");
        harness_show(&config, "codex-orchestrator", true, &out()).expect("show json ok");
    }

    #[test]
    fn show_errors_on_unknown_harness() {
        let config = Config::default();
        let err = harness_show(&config, "no-such-harness", false, &out())
            .expect_err("unknown harness must error");
        let msg = format!("{err}");
        assert!(msg.contains("no-such-harness"), "{msg}");
        assert!(msg.contains("available"), "{msg}");
    }
}
