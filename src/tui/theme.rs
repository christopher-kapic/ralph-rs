// Centralized color palette for the TUI (TUI-plan.md §12).
//
// All colors live as truecolor `Color::Rgb` constants. Terminals that don't
// advertise truecolor degrade to the nearest 256-color match via ratatui's
// default rendering path; we don't do explicit fallback here.

use ratatui::style::Color;

/// Highlight border / cursor row.
pub const CURSOR: Color = Color::Rgb(0xf7, 0xd1, 0x35);

/// Multi-select border.
pub const SELECTION: Color = Color::Rgb(0x56, 0xd0, 0xd9);

/// Plan dot + step glyph when complete.
pub const STATUS_COMPLETE: Color = Color::Rgb(0x34, 0xd0, 0x58);

/// Plan dot + step glyph when running.
pub const STATUS_IN_PROGRESS: Color = Color::Rgb(0xf7, 0xd1, 0x35);

/// Plan dot when never run.
pub const STATUS_PENDING: Color = Color::Rgb(0x3b, 0x82, 0xf6);

/// Plan dot + archived tile border.
pub const STATUS_FAILED: Color = Color::Rgb(0xef, 0x44, 0x44);

/// Plan dot when paused for question (§17).
pub const STATUS_QUESTION: Color = Color::Rgb(0xa8, 0x55, 0xf7);

/// Default tile borders, idle bottom bar text.
pub const CHROME_DIM: Color = Color::Gray;

/// Error toast background/text accent.
pub const TOAST_ERROR: Color = Color::Rgb(0xef, 0x44, 0x44);

/// Info toast background/text accent.
pub const TOAST_INFO: Color = Color::Rgb(0x3b, 0x82, 0xf6);

/// Success toast background/text accent (e.g. "Saved.").
pub const TOAST_SUCCESS: Color = Color::Rgb(0x34, 0xd0, 0x58);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truecolor_constants_match_palette() {
        assert_eq!(CURSOR, Color::Rgb(0xf7, 0xd1, 0x35));
        assert_eq!(SELECTION, Color::Rgb(0x56, 0xd0, 0xd9));
        assert_eq!(STATUS_COMPLETE, Color::Rgb(0x34, 0xd0, 0x58));
        assert_eq!(STATUS_IN_PROGRESS, Color::Rgb(0xf7, 0xd1, 0x35));
        assert_eq!(STATUS_PENDING, Color::Rgb(0x3b, 0x82, 0xf6));
        assert_eq!(STATUS_FAILED, Color::Rgb(0xef, 0x44, 0x44));
        assert_eq!(STATUS_QUESTION, Color::Rgb(0xa8, 0x55, 0xf7));
        assert_eq!(TOAST_ERROR, Color::Rgb(0xef, 0x44, 0x44));
        assert_eq!(TOAST_INFO, Color::Rgb(0x3b, 0x82, 0xf6));
        assert_eq!(TOAST_SUCCESS, Color::Rgb(0x34, 0xd0, 0x58));
    }

    #[test]
    fn in_progress_matches_cursor() {
        // Both highlight states share #f7d135 per the palette table.
        assert_eq!(STATUS_IN_PROGRESS, CURSOR);
    }
}
