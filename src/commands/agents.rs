// Agents CLI command implementations

use anyhow::{Context, Result};

use crate::config;
use crate::output::{self, OutputContext, OutputFormat};
use crate::validate::validate_name;

// ---------------------------------------------------------------------------
// Agents commands
// ---------------------------------------------------------------------------

pub fn cmd_agents_list(out: &OutputContext) -> Result<()> {
    let agents_dir = config::agents_dir()?;

    if !agents_dir.exists() {
        if out.format == OutputFormat::Json {
            println!("[]");
        } else {
            eprintln!("Agents directory not found: {}", agents_dir.display());
            eprintln!("Run `ralph init` to create it.");
        }
        return Ok(());
    }

    let mut entries: Vec<_> = std::fs::read_dir(&agents_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "md"))
        .collect();

    entries.sort_by_key(|e| e.file_name());

    if out.format == OutputFormat::Json {
        let infos: Vec<output::AgentInfo> = entries
            .iter()
            .map(|entry| {
                let name = entry
                    .file_name()
                    .to_string_lossy()
                    .trim_end_matches(".md")
                    .to_string();
                let size = entry.metadata().ok().map(|m| m.len()).unwrap_or(0);
                output::AgentInfo {
                    name,
                    size_bytes: size,
                }
            })
            .collect();
        println!("{}", serde_json::to_string(&infos)?);
        return Ok(());
    }

    let mut found = false;
    for entry in &entries {
        let name = entry
            .file_name()
            .to_string_lossy()
            .trim_end_matches(".md")
            .to_string();
        let metadata = entry.metadata().ok();
        let size = metadata.map(|m| m.len()).unwrap_or(0);
        println!("  {} ({} bytes)", name, size);
        found = true;
    }

    if !found {
        eprintln!("No agent files found in {}", agents_dir.display());
    }

    Ok(())
}

pub fn cmd_agents_show(name: &str, _out: &OutputContext) -> Result<()> {
    validate_name(name)?;
    let agents_dir = config::agents_dir()?;
    let path = agents_dir.join(format!("{name}.md"));

    if !path.exists() {
        anyhow::bail!("Agent file not found: {}", path.display());
    }

    let contents = std::fs::read_to_string(&path)
        .with_context(|| format!("Failed to read {}", path.display()))?;
    println!("{contents}");
    Ok(())
}

pub fn cmd_agents_create(
    name: &str,
    file: Option<&std::path::Path>,
    _out: &OutputContext,
) -> Result<()> {
    validate_name(name)?;
    let agents_dir = config::agents_dir()?;
    std::fs::create_dir_all(&agents_dir)?;
    let path = agents_dir.join(format!("{name}.md"));

    if path.exists() {
        anyhow::bail!("Agent file already exists: {}", path.display());
    }

    let contents = if let Some(src) = file {
        std::fs::read_to_string(src).with_context(|| format!("Failed to read {}", src.display()))?
    } else {
        default_agent_scaffold(name)
    };

    std::fs::write(&path, &contents)
        .with_context(|| format!("Failed to write {}", path.display()))?;
    eprintln!("Created agent file: {}", path.display());
    Ok(())
}

/// Default scaffold for a freshly-created agent file. Gives the author a
/// handful of prompts to fill in rather than a blank page.
fn default_agent_scaffold(name: &str) -> String {
    format!(
        "# {name}\n\
         \n\
         ## Role\n\
         \n\
         Describe who this agent is and what perspective it brings. \
         One or two sentences.\n\
         \n\
         ## Responsibilities\n\
         \n\
         - What should this agent do on every step?\n\
         - What decisions is it empowered to make?\n\
         \n\
         ## Constraints\n\
         \n\
         - What should this agent never do?\n\
         - Any files, directories, or tools that are off-limits?\n\
         \n\
         ## Output Expectations\n\
         \n\
         Describe the shape of a good result: tone, commit style, \
         how to surface uncertainty, when to ask for help.\n\
         \n\
         ## Notes\n\
         \n\
         Free-form context, links, or reminders for future runs.\n"
    )
}

pub fn cmd_agents_delete(name: &str, _out: &OutputContext) -> Result<()> {
    validate_name(name)?;
    let agents_dir = config::agents_dir()?;
    let path = agents_dir.join(format!("{name}.md"));

    if !path.exists() {
        anyhow::bail!("Agent file not found: {}", path.display());
    }

    std::fs::remove_file(&path).with_context(|| format!("Failed to delete {}", path.display()))?;
    eprintln!("Deleted agent file: {name}");
    Ok(())
}
