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

/// Plan dot + step glyph when complete (docs/dag-redesign.md §12.5 —
/// **unchanged**: complete stays green so the remap is small + additive).
pub const STATUS_COMPLETE: Color = Color::Rgb(0x34, 0xd0, 0x58);

/// Plan dot + step glyph when implementing / in progress
/// (docs/dag-redesign.md §12.5 — **unchanged** yellow; intentionally the
/// same hex as [`CURSOR`], see the `colors_are_distinct` note below).
pub const STATUS_IN_PROGRESS: Color = Color::Rgb(0xf7, 0xd1, 0x35);

/// Plan dot + step glyph when *reviewing* (awaiting / in review)
/// (docs/dag-redesign.md §12.5). Blue `#3b82f6` — this is the exact value
/// formerly carried by the retired `STATUS_PENDING`. It deliberately
/// shares its hex with [`TOAST_INFO`] but is kept as a SEPARATE named
/// token (a transient info toast and a persistent step glyph are
/// different surfaces — a future palette edit must not couple them).
pub const STATUS_REVIEWING: Color = Color::Rgb(0x3b, 0x82, 0xf6);

/// Plan dot + step glyph when waiting-for-turn / pending / never-run
/// (docs/dag-redesign.md §12.5). A deliberately *bright* white —
/// distinct from default body text so a waiting row still stands out on
/// the dark theme (the old pending blue was reused as
/// [`STATUS_REVIEWING`]).
pub const STATUS_WAITING: Color = Color::Rgb(0xf5, 0xf7, 0xfa);

/// Plan dot + archived tile border; failed / review-failed
/// (docs/dag-redesign.md §12.5 — **unchanged** red).
pub const STATUS_FAILED: Color = Color::Rgb(0xef, 0x44, 0x44);

/// Plan dot + step glyph when blocked — a question *or* a blocker
/// (docs/dag-redesign.md §12.5). Orange `#db6d28`. Retires the old
/// purple `STATUS_QUESTION`: the step glyph AND the derived plan-level
/// "interrupted" status both go orange so questions and blockers read
/// identically everywhere (`#db6d28` checked for separation against
/// implementing-yellow and failed-red).
pub const STATUS_BLOCKED: Color = Color::Rgb(0xdb, 0x6d, 0x28);

/// Default tile borders, idle bottom bar text.
pub const CHROME_DIM: Color = Color::Gray;

/// Error toast background/text accent.
pub const TOAST_ERROR: Color = Color::Rgb(0xef, 0x44, 0x44);

/// Info toast background/text accent.
pub const TOAST_INFO: Color = Color::Rgb(0x3b, 0x82, 0xf6);

/// Success toast background/text accent (e.g. "Saved.").
pub const TOAST_SUCCESS: Color = Color::Rgb(0x34, 0xd0, 0x58);

// ---------------------------------------------------------------------------
// §12.5 status → color: the SINGLE source of truth, TUI-wide
// ---------------------------------------------------------------------------
//
// docs/dag-redesign.md §12.5 mandates "one state, one color" applied
// TUI-wide so the same concept never shows two colors across screens. Both
// the step DAG/glyph surfaces and the plan-list / chrome plan-status
// surfaces funnel through these two adapters; no surface picks a
// `STATUS_*` token directly anymore. `StepStatus`/`PlanStatus` are the only
// two status enums; the derived `Blocked` (step) and `Interrupted` (plan)
// overlays are *already folded into those enums* by the time they reach the
// renderer (`effective_step_status` / `plan_effective_status`), so this
// single table covers every derived state too.

use crate::plan::{PlanStatus, StepStatus};

/// §12.5 color for a (possibly-derived) [`StepStatus`].
///
/// The caller is expected to pass the *effective* status (i.e. the
/// `Blocked` overlay already applied via
/// [`crate::plan::effective_step_status`]); `Blocked` maps to the new
/// orange [`STATUS_BLOCKED`] (questions and blockers read identically).
/// Pending/waiting-for-turn is the deliberately-bright [`STATUS_WAITING`]
/// white. `Skipped` stays dim gray ([`CHROME_DIM`]) per §12.5.
pub fn step_status_color(status: StepStatus) -> Color {
    match status {
        StepStatus::Complete => STATUS_COMPLETE,
        StepStatus::InProgress => STATUS_IN_PROGRESS,
        StepStatus::Pending => STATUS_WAITING,
        StepStatus::Failed | StepStatus::Aborted => STATUS_FAILED,
        StepStatus::Skipped => CHROME_DIM,
        StepStatus::Blocked => STATUS_BLOCKED,
    }
}

/// §12.5 color for a (possibly-derived) [`PlanStatus`].
///
/// `Interrupted` is the derived plan-level overlay (an open interruption
/// — question *or* blocker — exists for some step); per §12.5 it goes
/// orange ([`STATUS_BLOCKED`]) so it reads identically to a blocked step
/// glyph. `Planning`/`Ready` (not-yet-run) is waiting-for-turn white;
/// `Archived` joins failed/aborted on red (an archived tile border, as
/// before). There is no plan-level "reviewing" status, so
/// [`STATUS_REVIEWING`] is only reachable via [`step_status_color`].
pub fn plan_status_color(status: PlanStatus) -> Color {
    match status {
        PlanStatus::Complete => STATUS_COMPLETE,
        PlanStatus::InProgress => STATUS_IN_PROGRESS,
        PlanStatus::Planning | PlanStatus::Ready => STATUS_WAITING,
        PlanStatus::Failed | PlanStatus::Aborted | PlanStatus::Archived => STATUS_FAILED,
        PlanStatus::Interrupted => STATUS_BLOCKED,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truecolor_constants_match_palette() {
        // docs/dag-redesign.md §12.5 status-color table. Three tokens are
        // UNCHANGED (complete/in-progress/failed); the old pending-blue is
        // reused verbatim as STATUS_REVIEWING; STATUS_WAITING + STATUS_BLOCKED
        // are new; the purple STATUS_QUESTION is retired.
        assert_eq!(CURSOR, Color::Rgb(0xf7, 0xd1, 0x35));
        assert_eq!(SELECTION, Color::Rgb(0x56, 0xd0, 0xd9));
        assert_eq!(STATUS_COMPLETE, Color::Rgb(0x34, 0xd0, 0x58));
        assert_eq!(STATUS_IN_PROGRESS, Color::Rgb(0xf7, 0xd1, 0x35));
        assert_eq!(STATUS_REVIEWING, Color::Rgb(0x3b, 0x82, 0xf6));
        assert_eq!(STATUS_WAITING, Color::Rgb(0xf5, 0xf7, 0xfa));
        assert_eq!(STATUS_FAILED, Color::Rgb(0xef, 0x44, 0x44));
        assert_eq!(STATUS_BLOCKED, Color::Rgb(0xdb, 0x6d, 0x28));
        assert_eq!(TOAST_ERROR, Color::Rgb(0xef, 0x44, 0x44));
        assert_eq!(TOAST_INFO, Color::Rgb(0x3b, 0x82, 0xf6));
        assert_eq!(TOAST_SUCCESS, Color::Rgb(0x34, 0xd0, 0x58));
    }

    #[test]
    fn reviewing_and_toast_info_share_hex_but_are_separate_tokens() {
        // docs/dag-redesign.md §12.5 implementation note: reviewing shares
        // its hex with TOAST_INFO (#3b82f6). That is acceptable — different
        // surfaces — but they MUST stay separate named tokens so a future
        // palette edit cannot couple a persistent step glyph to a transient
        // info toast. This test documents (does not forbid) the shared value.
        assert_eq!(STATUS_REVIEWING, TOAST_INFO);
    }

    #[test]
    fn cursor_is_a_highlight_not_a_status_recolor() {
        // docs/dag-redesign.md §12.5 cursor note. The cursor color #f7d135
        // is *intentionally* identical to implementing-yellow
        // (STATUS_IN_PROGRESS). This is fine ONLY because the cursor is
        // expressed as a row/border highlight + `→` glyph, never as a text
        // recolor: a cursored non-implementing row stays distinguishable via
        // the highlight/glyph, not via its status color. This replaces the
        // old `in_progress_matches_cursor` assertion, which had implied the
        // coupling was the *mechanism* of distinction rather than an
        // incidental shared hue. We still pin the shared value so an
        // accidental palette divergence is caught.
        assert_eq!(STATUS_IN_PROGRESS, CURSOR);
    }

    #[test]
    fn status_colors_are_distinct() {
        // docs/dag-redesign.md §12.5: one state, one color. Every *status*
        // token must be visually distinguishable from every other status
        // token (CURSOR is deliberately excluded — it is a highlight, not a
        // status; see `cursor_is_a_highlight_not_a_status_recolor`). White
        // (STATUS_WAITING) in particular must differ from the others so a
        // waiting row stands out on the dark theme.
        let tokens = [
            ("complete", STATUS_COMPLETE),
            ("in_progress", STATUS_IN_PROGRESS),
            ("reviewing", STATUS_REVIEWING),
            ("waiting", STATUS_WAITING),
            ("failed", STATUS_FAILED),
            ("blocked", STATUS_BLOCKED),
        ];
        for (i, (na, a)) in tokens.iter().enumerate() {
            for (nb, b) in tokens.iter().skip(i + 1) {
                assert_ne!(a, b, "status colors {na} and {nb} must differ");
            }
        }
    }

    #[test]
    fn step_status_color_maps_to_exact_125_hex() {
        // docs/dag-redesign.md §12.5 status-color table, step side.
        assert_eq!(
            step_status_color(StepStatus::Complete),
            Color::Rgb(0x34, 0xd0, 0x58)
        );
        assert_eq!(
            step_status_color(StepStatus::InProgress),
            Color::Rgb(0xf7, 0xd1, 0x35)
        );
        assert_eq!(
            step_status_color(StepStatus::Pending),
            Color::Rgb(0xf5, 0xf7, 0xfa)
        );
        assert_eq!(
            step_status_color(StepStatus::Failed),
            Color::Rgb(0xef, 0x44, 0x44)
        );
        assert_eq!(
            step_status_color(StepStatus::Aborted),
            Color::Rgb(0xef, 0x44, 0x44)
        );
        assert_eq!(step_status_color(StepStatus::Skipped), CHROME_DIM);
        // Retired purple #a855f7 → orange #db6d28 (question == blocker).
        assert_eq!(
            step_status_color(StepStatus::Blocked),
            Color::Rgb(0xdb, 0x6d, 0x28)
        );
    }

    #[test]
    fn plan_status_color_maps_to_exact_125_hex() {
        // docs/dag-redesign.md §12.5 status-color table, plan side.
        assert_eq!(
            plan_status_color(PlanStatus::Complete),
            Color::Rgb(0x34, 0xd0, 0x58)
        );
        assert_eq!(
            plan_status_color(PlanStatus::InProgress),
            Color::Rgb(0xf7, 0xd1, 0x35)
        );
        assert_eq!(
            plan_status_color(PlanStatus::Planning),
            Color::Rgb(0xf5, 0xf7, 0xfa)
        );
        assert_eq!(
            plan_status_color(PlanStatus::Ready),
            Color::Rgb(0xf5, 0xf7, 0xfa)
        );
        assert_eq!(
            plan_status_color(PlanStatus::Failed),
            Color::Rgb(0xef, 0x44, 0x44)
        );
        assert_eq!(
            plan_status_color(PlanStatus::Aborted),
            Color::Rgb(0xef, 0x44, 0x44)
        );
        assert_eq!(
            plan_status_color(PlanStatus::Archived),
            Color::Rgb(0xef, 0x44, 0x44)
        );
        // Derived plan-level interrupted → same orange as a blocked step.
        assert_eq!(
            plan_status_color(PlanStatus::Interrupted),
            Color::Rgb(0xdb, 0x6d, 0x28)
        );
    }

    #[test]
    fn same_concept_one_color_across_step_and_plan_surfaces() {
        // docs/dag-redesign.md §12.5: "the same concept never shows two
        // colors across screens." A blocked step (DAG glyph, plan_detail)
        // and a derived-interrupted plan (plan_list dot, chrome) are the
        // same concept — an open interruption — and MUST render identically.
        assert_eq!(
            step_status_color(StepStatus::Blocked),
            plan_status_color(PlanStatus::Interrupted),
            "blocked step and interrupted plan must share one color"
        );
        // Likewise the shared lifecycle states must agree across the two
        // surfaces (the whole point of a single mapping helper).
        assert_eq!(
            step_status_color(StepStatus::Complete),
            plan_status_color(PlanStatus::Complete)
        );
        assert_eq!(
            step_status_color(StepStatus::InProgress),
            plan_status_color(PlanStatus::InProgress)
        );
        assert_eq!(
            step_status_color(StepStatus::Pending),
            plan_status_color(PlanStatus::Planning)
        );
        assert_eq!(
            step_status_color(StepStatus::Failed),
            plan_status_color(PlanStatus::Failed)
        );
    }
}
