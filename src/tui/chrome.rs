// Persistent TUI chrome
//
// Top breadcrumb bar and bottom hint+cwd/version bar drawn on every TUI view
// (TUI-plan.md §4). A single `render` function is called from each view's
// drawing code; it reserves the top and bottom rows and returns the inner
// body Rect for the view to draw into.

use std::path::Path;

use ratatui::Frame;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::Paragraph;

use crate::tui::theme;

const VERSION: &str = env!("CARGO_PKG_VERSION");
const ELLIPSIS: char = '…';
const CRUMB_SEP: &str = " › ";

/// Per-frame chrome inputs. `breadcrumbs` are joined with ` › ` for the top
/// bar; `hint` is left-aligned on the bottom bar; `cwd` is shortened with `~`
/// substitution and rendered bottom-right alongside `ralph v<version>`.
///
/// `banner` is the read-only lockdown banner (TUI-plan.md §13.2). When
/// `Some`, it takes the bottom-bar slot in place of the per-view hint and is
/// rendered in red so the lockdown state is visually prominent across every
/// view; the cwd/version still renders on the right edge.
///
/// `running_indicator` (step #29) is a compact one-line "▶ Running step N
/// (phase) MM:SS" string surfaced just left of the cwd/version when a runner
/// is bound to the plan. Lives in chrome so the user keeps seeing live
/// progress regardless of which step the cursor is on (or which view they
/// pushed onto the stack).
pub struct Chrome<'a> {
    pub breadcrumbs: &'a [&'a str],
    pub hint: &'a str,
    pub cwd: &'a Path,
    pub banner: Option<&'a str>,
    pub running_indicator: Option<&'a str>,
}

impl<'a> Chrome<'a> {
    /// Convenience constructor: chrome with no read-only banner.
    pub fn new(breadcrumbs: &'a [&'a str], hint: &'a str, cwd: &'a Path) -> Self {
        Self {
            breadcrumbs,
            hint,
            cwd,
            banner: None,
            running_indicator: None,
        }
    }
}

/// Render the chrome and return the inner Rect a view should draw into.
pub fn render(frame: &mut Frame, chrome: &Chrome<'_>) -> Rect {
    let area = frame.area();
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Min(0),
            Constraint::Length(1),
        ])
        .split(area);

    render_breadcrumb(frame, chunks[0], chrome.breadcrumbs);
    render_bottom(
        frame,
        chunks[2],
        chrome.hint,
        chrome.cwd,
        chrome.banner,
        chrome.running_indicator,
    );

    chunks[1]
}

fn render_breadcrumb(frame: &mut Frame, area: Rect, crumbs: &[&str]) {
    if area.width == 0 {
        return;
    }
    let joined = crumbs.join(CRUMB_SEP);
    let text = right_truncate(&joined, area.width as usize);
    let para = Paragraph::new(Span::styled(
        text,
        Style::default().add_modifier(Modifier::BOLD),
    ));
    frame.render_widget(para, area);
}

fn render_bottom(
    frame: &mut Frame,
    area: Rect,
    hint: &str,
    cwd: &Path,
    banner: Option<&str>,
    running_indicator: Option<&str>,
) {
    if area.width == 0 {
        return;
    }
    let cwd_full = format_cwd_version(cwd);
    let cwd_text = left_truncate(&cwd_full, area.width as usize);
    let cwd_width = display_width(&cwd_text);

    // Reserve room for the running indicator (when present) just left of the
    // cwd/version; pad with two spaces so it doesn't visually fuse into the
    // version string. If there isn't enough horizontal room (after the cwd),
    // drop the indicator silently rather than truncate it into nonsense.
    let running_text = running_indicator.unwrap_or("");
    let running_width = if running_indicator.is_some() {
        display_width(running_text)
    } else {
        0
    };
    let running_slot_width =
        if running_width > 0 && (area.width as usize) > cwd_width + running_width + 3 {
            running_width as u16 + 2
        } else {
            0
        };

    // Reserve the right side for cwd/version (and optionally the running
    // indicator); the left side gets whatever's left after a 1-column gap.
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Min(0),
            Constraint::Length(running_slot_width),
            Constraint::Length(cwd_width as u16),
        ])
        .split(area);

    let left_max = (chunks[0].width as usize).saturating_sub(1);
    // The lockdown banner takes the hint slot when present, rendered in red
    // with a bold modifier so it's visually distinct from the regular dim
    // gray hint line (TUI-plan.md §13.2).
    let (left_text, left_style) = match banner {
        Some(b) => (
            right_truncate(b, left_max),
            Style::default().fg(Color::Red).add_modifier(Modifier::BOLD),
        ),
        None => (
            right_truncate(hint, left_max),
            Style::default().fg(theme::CHROME_DIM),
        ),
    };
    frame.render_widget(
        Paragraph::new(Span::styled(left_text, left_style)),
        chunks[0],
    );
    if running_slot_width > 0 {
        frame.render_widget(
            Paragraph::new(Span::styled(
                running_text,
                // The running indicator means "this plan is executing" —
                // the derived in-progress plan status. Route it through the
                // single §12.5 mapping so chrome can't drift from the
                // plan-list dot / step glyph (docs/dag-redesign.md §12.5).
                Style::default()
                    .fg(theme::plan_status_color(crate::plan::PlanStatus::InProgress))
                    .add_modifier(Modifier::BOLD),
            ))
            .alignment(Alignment::Right),
            chunks[1],
        );
    }
    frame.render_widget(
        Paragraph::new(Span::styled(
            cwd_text,
            Style::default().fg(theme::CHROME_DIM),
        ))
        .alignment(Alignment::Right),
        chunks[2],
    );
}

/// Build the bottom-right text: `<cwd-with-~>  ralph v<version>`.
pub fn format_cwd_version(cwd: &Path) -> String {
    format!("{}  ralph v{VERSION}", shorten_with_home(cwd))
}

/// Substitute the user's home directory with `~` in `path`. If `path` is not
/// inside HOME (or HOME cannot be determined), return the path unchanged.
pub fn shorten_with_home(path: &Path) -> String {
    shorten_with_home_impl(path, dirs::home_dir().as_deref())
}

fn shorten_with_home_impl(path: &Path, home: Option<&Path>) -> String {
    let s = path.to_string_lossy();
    let Some(home) = home else {
        return s.into_owned();
    };
    let home_str = home.to_string_lossy();
    if home_str.is_empty() {
        return s.into_owned();
    }
    if s == home_str {
        return "~".to_string();
    }
    if let Some(rest) = s.strip_prefix(home_str.as_ref())
        && (rest.starts_with('/') || rest.starts_with('\\'))
    {
        return format!("~{rest}");
    }
    s.into_owned()
}

/// Truncate `s` from the left with `…` so its column width is at most `max`.
pub fn left_truncate(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if display_width(s) <= max {
        return s.to_string();
    }
    if max == 1 {
        return ELLIPSIS.to_string();
    }
    let target = max - 1;
    let mut start_byte = s.len();
    for (i, _) in s.char_indices() {
        if display_width(&s[i..]) <= target {
            start_byte = i;
            break;
        }
    }
    let mut out = String::with_capacity(s.len() - start_byte + ELLIPSIS.len_utf8());
    out.push(ELLIPSIS);
    out.push_str(&s[start_byte..]);
    out
}

/// Truncate `s` from the right with `…` so its column width is at most `max`.
pub fn right_truncate(s: &str, max: usize) -> String {
    if max == 0 {
        return String::new();
    }
    if display_width(s) <= max {
        return s.to_string();
    }
    if max == 1 {
        return ELLIPSIS.to_string();
    }
    let target = max - 1;
    let mut end_byte = 0;
    for (i, _) in s.char_indices() {
        if display_width(&s[..i]) <= target {
            end_byte = i;
        } else {
            break;
        }
    }
    let mut out = String::with_capacity(end_byte + ELLIPSIS.len_utf8());
    out.push_str(&s[..end_byte]);
    out.push(ELLIPSIS);
    out
}

fn display_width(s: &str) -> usize {
    s.chars().count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::Terminal;
    use ratatui::backend::TestBackend;

    fn render_to_string(width: u16, height: u16, chrome: &Chrome<'_>) -> String {
        let backend = TestBackend::new(width, height);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                let _body = render(frame, chrome);
            })
            .unwrap();
        let buffer = terminal.backend().buffer().clone();
        (0..buffer.area().height)
            .map(|y| {
                (0..buffer.area().width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    // -- left_truncate ----------------------------------------------------

    #[test]
    fn left_truncate_fits_unchanged() {
        assert_eq!(left_truncate("abc", 5), "abc");
        assert_eq!(left_truncate("abc", 3), "abc");
    }

    #[test]
    fn left_truncate_replaces_left_with_ellipsis() {
        assert_eq!(left_truncate("abcdef", 4), "…def");
        // The result must be exactly `max` columns wide (1 ellipsis + 9 chars).
        let out = left_truncate("/home/user/projects/foo", 10);
        assert_eq!(out, "…jects/foo");
        assert_eq!(display_width(&out), 10);
    }

    #[test]
    fn left_truncate_zero_and_one_max() {
        assert_eq!(left_truncate("abcdef", 0), "");
        assert_eq!(left_truncate("abcdef", 1), "…");
    }

    // -- right_truncate ---------------------------------------------------

    #[test]
    fn right_truncate_fits_unchanged() {
        assert_eq!(right_truncate("abc", 5), "abc");
        assert_eq!(right_truncate("abc", 3), "abc");
    }

    #[test]
    fn right_truncate_replaces_right_with_ellipsis() {
        assert_eq!(right_truncate("abcdef", 4), "abc…");
        assert_eq!(right_truncate("ralph › slug › step", 10), "ralph › s…");
    }

    #[test]
    fn right_truncate_zero_and_one_max() {
        assert_eq!(right_truncate("abcdef", 0), "");
        assert_eq!(right_truncate("abcdef", 1), "…");
    }

    // -- shorten_with_home -----------------------------------------------

    #[test]
    fn shorten_with_home_substitutes_prefix() {
        let home = Path::new("/home/alice");
        assert_eq!(
            shorten_with_home_impl(Path::new("/home/alice/projects/foo"), Some(home)),
            "~/projects/foo"
        );
    }

    #[test]
    fn shorten_with_home_exact_home() {
        assert_eq!(
            shorten_with_home_impl(Path::new("/home/alice"), Some(Path::new("/home/alice"))),
            "~"
        );
    }

    #[test]
    fn shorten_with_home_does_not_match_partial_segment() {
        // /home/alice2 should not collapse against home /home/alice.
        assert_eq!(
            shorten_with_home_impl(
                Path::new("/home/alice2/foo"),
                Some(Path::new("/home/alice"))
            ),
            "/home/alice2/foo"
        );
    }

    #[test]
    fn shorten_with_home_unrelated_path() {
        assert_eq!(
            shorten_with_home_impl(Path::new("/etc/passwd"), Some(Path::new("/home/alice"))),
            "/etc/passwd"
        );
    }

    #[test]
    fn shorten_with_home_no_home_known() {
        assert_eq!(
            shorten_with_home_impl(Path::new("/etc/passwd"), None),
            "/etc/passwd"
        );
    }

    // -- format_cwd_version ----------------------------------------------

    #[test]
    fn format_cwd_version_includes_version_and_path() {
        let out = format_cwd_version(Path::new("/tmp/proj"));
        assert!(out.contains("ralph v"), "missing version: {out}");
        assert!(out.contains("/tmp/proj"), "missing cwd: {out}");
        // Two-space gap between cwd and "ralph".
        assert!(out.contains("  ralph v"), "expected two spaces: {out}");
    }

    // -- render integration ----------------------------------------------

    #[test]
    fn render_returns_inner_body_rect() {
        let backend = TestBackend::new(80, 24);
        let mut terminal = Terminal::new(backend).unwrap();
        let mut body_rect: Option<Rect> = None;
        terminal
            .draw(|frame| {
                let body = render(
                    frame,
                    &Chrome {
                        breadcrumbs: &["ralph", "my-plan"],
                        hint: "[q] quit",
                        cwd: Path::new("/tmp"),
                        banner: None,
                        running_indicator: None,
                    },
                );
                body_rect = Some(body);
            })
            .unwrap();
        let body = body_rect.unwrap();
        assert_eq!(body.y, 1);
        assert_eq!(body.height, 22);
        assert_eq!(body.width, 80);
    }

    #[test]
    fn render_draws_breadcrumb_and_version() {
        let rendered = render_to_string(
            80,
            5,
            &Chrome {
                breadcrumbs: &["ralph", "tui-v1"],
                hint: "[j/k] nav  [q] quit",
                cwd: Path::new("/tmp/proj"),
                banner: None,
                running_indicator: None,
            },
        );
        assert!(
            rendered.contains("ralph › tui-v1"),
            "breadcrumb missing:\n{rendered}"
        );
        assert!(
            rendered.contains(&format!("ralph v{VERSION}")),
            "version missing:\n{rendered}"
        );
        assert!(rendered.contains("[j/k] nav"), "hint missing:\n{rendered}");
        assert!(rendered.contains("/tmp/proj"), "cwd missing:\n{rendered}");
    }

    #[test]
    fn render_left_truncates_cwd_in_narrow_terminal() {
        // 30 columns is too narrow for a deep path + "  ralph vX.Y.Z";
        // the cwd portion gets eaten by `…` from the left.
        let rendered = render_to_string(
            30,
            3,
            &Chrome {
                breadcrumbs: &["ralph", "p"],
                hint: "h",
                cwd: Path::new("/very/deeply/nested/project/path"),
                banner: None,
                running_indicator: None,
            },
        );
        // The bottom row must still end with "ralph v<version>".
        let bottom = rendered.lines().last().unwrap();
        assert!(
            bottom.trim_end().ends_with(&format!("ralph v{VERSION}")),
            "bottom should end with version: {bottom:?}"
        );
        // Truncation marker present somewhere on the bottom row.
        assert!(
            bottom.contains('…'),
            "expected ellipsis on truncated bottom row: {bottom:?}"
        );
        // Bottom row must not exceed terminal width.
        assert_eq!(
            display_width(bottom),
            30,
            "bottom row width should equal terminal width"
        );
    }

    #[test]
    fn render_banner_replaces_hint_on_bottom_row() {
        let rendered = render_to_string(
            120,
            5,
            &Chrome {
                breadcrumbs: &["ralph", "tui-v1"],
                hint: "[j/k] nav  [q] quit",
                cwd: Path::new("/tmp/proj"),
                banner: Some("🔒 Read-only — run in progress (PID 4242). [S] cancel  [q] quit"),
                running_indicator: None,
            },
        );
        let bottom = rendered.lines().last().unwrap();
        assert!(
            bottom.contains("Read-only"),
            "banner missing on bottom row: {bottom:?}"
        );
        assert!(
            bottom.contains("PID 4242"),
            "banner pid missing: {bottom:?}"
        );
        // The regular hint must NOT render when the banner is set; the
        // banner replaces it.
        assert!(
            !bottom.contains("[j/k] nav"),
            "hint should be hidden under banner: {bottom:?}"
        );
        // cwd/version still on the right.
        assert!(
            bottom.contains(&format!("ralph v{VERSION}")),
            "version missing under banner: {bottom:?}"
        );
    }

    #[test]
    fn render_does_not_panic_on_tiny_terminal() {
        // Very narrow / very short terminals must still render without panic.
        let _ = render_to_string(
            5,
            3,
            &Chrome {
                breadcrumbs: &["ralph", "plan"],
                hint: "[q] quit",
                cwd: Path::new("/some/long/path"),
                banner: None,
                running_indicator: None,
            },
        );
        let _ = render_to_string(
            10,
            2,
            &Chrome {
                breadcrumbs: &["ralph"],
                hint: "h",
                cwd: Path::new("/x"),
                banner: None,
                running_indicator: None,
            },
        );
    }
}
