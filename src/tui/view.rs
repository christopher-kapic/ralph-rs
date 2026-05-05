// TUI top-level view enum
//
// Identifies which top-level view the TUI is currently rendering. The
// multi-view shell (added in TUI-plan.md §15) routes input handling and
// rendering through this enum so each view's state lives in its own module.

/// The top-level view currently being rendered by the TUI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum View {
    PlanList,
    ArchivedList,
    PlanDetail,
    StepDetail,
}
