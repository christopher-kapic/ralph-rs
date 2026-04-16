use anyhow::{Result, bail};

/// Reject names that could escape the intended directory when used in
/// `format!("{name}.md")` path construction.
pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty() {
        bail!("Name must not be empty");
    }

    if name.contains('/') || name.contains('\\') {
        bail!("Name must not contain path separators ('/' or '\\'): {name}");
    }

    if name.contains("..") {
        bail!("Name must not contain '..': {name}");
    }

    if name.starts_with('.') {
        bail!("Name must not start with '.': {name}");
    }

    if name.contains('\0') {
        bail!("Name must not contain NUL bytes: {name}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_names() {
        assert!(validate_name("my-agent").is_ok());
        assert!(validate_name("claude_review").is_ok());
        assert!(validate_name("hook1").is_ok());
    }

    #[test]
    fn rejects_path_traversal() {
        assert!(validate_name("../evil").is_err());
        assert!(validate_name("..\\evil").is_err());
        assert!(validate_name("foo/../bar").is_err());
    }

    #[test]
    fn rejects_absolute_path() {
        assert!(validate_name("/abs/path").is_err());
    }

    #[test]
    fn rejects_backslash_path() {
        assert!(validate_name("foo\\bar").is_err());
    }

    #[test]
    fn rejects_leading_dot() {
        assert!(validate_name(".hidden").is_err());
    }

    #[test]
    fn rejects_nul_byte() {
        assert!(validate_name("bad\0name").is_err());
    }

    #[test]
    fn rejects_empty() {
        assert!(validate_name("").is_err());
    }
}
