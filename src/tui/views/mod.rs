// View modules
//
// Each top-level view of the TUI lives in its own module here. Per
// TUI-plan.md §15, the multi-view shell routes rendering and input handling
// through these modules so each view's state is self-contained.

pub mod plan_detail;
pub mod plan_detail_input;
pub mod plan_detail_ui;
