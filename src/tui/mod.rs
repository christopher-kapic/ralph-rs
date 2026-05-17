// TUI module
//
// Interactive terminal UI for plan execution, built on ratatui + crossterm.
// Provides step list navigation, live status, inline step insertion, and
// graceful shutdown — as the interactive counterpart to the non-interactive
// runner.

pub mod choice;
pub mod chrome;
pub mod dialog;
pub mod editor;
pub mod events;
pub mod help;
// Pure DAG → outline projection (docs/dag-redesign.md §12.1). Data model
// only — not yet wired into any view (the outline view is a later step);
// declared here so its unit tests run and the later view can consume it.
pub mod outline;
pub mod palette;
pub mod palette_dispatch;
pub mod read_only;
pub mod run_dialog;
pub mod selection;
pub mod skip_dialog;
pub mod theme;
pub mod toast;
pub mod view;
pub mod views;
pub mod widgets;
