// Reusable ratatui widget helpers shared across multiple TUI views.
//
// Widgets here are deliberately presentation-only — they take an immutable
// data slice plus enough state (cursor / selection / list scroll) to render
// and have no knowledge of the surrounding `App` struct. That keeps them
// reusable from both plan-list (read-only previews) and plan-detail
// (interactive primary view).

pub mod step_list;
