// View modules
//
// Each top-level view of the TUI lives in its own module here. Per
// TUI-plan.md §15, the multi-view shell routes rendering and input handling
// through these modules so each view's state is self-contained.

pub mod archived_list;
pub mod create_plan;
pub mod plan_dependencies;
pub mod plan_detail;
pub mod plan_detail_input;
pub mod plan_detail_ui;
pub mod plan_list;
pub mod step_detail;
pub mod step_detail_picker;
