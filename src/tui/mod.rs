// TUI module
//
// Interactive terminal UI for plan execution, built on ratatui + crossterm.
// Provides step list navigation, live status, inline step insertion, and
// graceful shutdown — as the interactive counterpart to the non-interactive
// runner.

pub mod chrome;
pub mod dialog;
pub mod editor;
pub mod events;
pub mod palette;
pub mod palette_dispatch;
pub mod read_only;
pub mod run_dialog;
pub mod selection;
pub mod theme;
pub mod toast;
pub mod view;
pub mod views;
