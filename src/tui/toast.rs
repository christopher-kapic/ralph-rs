// Transient toast bar for the TUI bottom row (TUI-plan.md §4 / §12).
//
// `Toast` is a single message with an expiry deadline; `ToastQueue` is the
// collection rendered by the chrome layer. Auto-expiry is computed against an
// `Instant` passed in by the caller — the TUI event loop hands in
// `Instant::now()` on each tick, and tests inject a synthetic clock.

use std::time::{Duration, Instant};

use ratatui::style::Color;

use super::theme;

/// How long a toast remains visible before auto-expiring.
pub const DEFAULT_TTL: Duration = Duration::from_secs(3);

/// Semantic kind of a toast — chooses the accent color from `theme`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToastKind {
    Info,
    Success,
    Error,
}

impl ToastKind {
    pub fn color(self) -> Color {
        match self {
            ToastKind::Info => theme::TOAST_INFO,
            ToastKind::Success => theme::TOAST_SUCCESS,
            ToastKind::Error => theme::TOAST_ERROR,
        }
    }
}

/// One transient message displayed in the bottom toast slot.
#[derive(Debug, Clone)]
pub struct Toast {
    pub text: String,
    pub color: Color,
    pub expires_at: Instant,
}

/// Queue of pending toasts. The most recently pushed toast is "current" and
/// renders on top; popping it (auto-expiry or manual `<esc>` dismiss) reveals
/// the next-most-recent.
#[derive(Debug)]
pub struct ToastQueue {
    toasts: Vec<Toast>,
    ttl: Duration,
}

impl Default for ToastQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl ToastQueue {
    pub fn new() -> Self {
        Self::with_ttl(DEFAULT_TTL)
    }

    pub fn with_ttl(ttl: Duration) -> Self {
        Self {
            toasts: Vec::new(),
            ttl,
        }
    }

    /// Push a semantically-typed toast. Color is taken from `kind`.
    pub fn push(&mut self, text: impl Into<String>, kind: ToastKind, now: Instant) {
        self.push_with_color(text, kind.color(), now);
    }

    /// Push a toast with an explicit color (escape hatch for callers that want
    /// to use a non-toast palette token, e.g. cursor yellow for a hint).
    pub fn push_with_color(&mut self, text: impl Into<String>, color: Color, now: Instant) {
        self.toasts.push(Toast {
            text: text.into(),
            color,
            expires_at: now + self.ttl,
        });
    }

    /// Drop any toasts whose `expires_at` is at or before `now`. Called once
    /// per render tick by the event loop.
    pub fn prune(&mut self, now: Instant) {
        self.toasts.retain(|t| t.expires_at > now);
    }

    /// Manually dismiss the current (most recent) toast — bound to `<esc>`.
    /// Returns true if a toast was dismissed.
    pub fn dismiss(&mut self) -> bool {
        self.toasts.pop().is_some()
    }

    /// Drop every queued toast.
    pub fn clear(&mut self) {
        self.toasts.clear();
    }

    /// The toast currently shown to the user (most recently pushed, if any).
    pub fn current(&self) -> Option<&Toast> {
        self.toasts.last()
    }

    pub fn is_empty(&self) -> bool {
        self.toasts.is_empty()
    }

    pub fn len(&self) -> usize {
        self.toasts.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fixed reference instant; tests advance with `+ Duration::from_*` to
    /// simulate elapsed time. `Instant` has no public constructor, so we
    /// anchor on a real `now()` and only compare relative offsets afterward.
    fn t0() -> Instant {
        Instant::now()
    }

    #[test]
    fn empty_queue_has_no_current() {
        let q = ToastQueue::new();
        assert!(q.is_empty());
        assert_eq!(q.len(), 0);
        assert!(q.current().is_none());
    }

    #[test]
    fn push_makes_toast_current() {
        let now = t0();
        let mut q = ToastQueue::new();
        q.push("Saved.", ToastKind::Success, now);
        let cur = q.current().expect("toast should be current");
        assert_eq!(cur.text, "Saved.");
        assert_eq!(cur.color, theme::TOAST_SUCCESS);
        assert_eq!(cur.expires_at, now + DEFAULT_TTL);
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn most_recent_push_wins() {
        let now = t0();
        let mut q = ToastQueue::new();
        q.push("first", ToastKind::Info, now);
        q.push("second", ToastKind::Error, now);
        let cur = q.current().unwrap();
        assert_eq!(cur.text, "second");
        assert_eq!(cur.color, theme::TOAST_ERROR);
        assert_eq!(q.len(), 2);
    }

    #[test]
    fn kind_maps_to_palette_color() {
        assert_eq!(ToastKind::Info.color(), theme::TOAST_INFO);
        assert_eq!(ToastKind::Success.color(), theme::TOAST_SUCCESS);
        assert_eq!(ToastKind::Error.color(), theme::TOAST_ERROR);
    }

    #[test]
    fn push_with_color_overrides_kind_color() {
        let now = t0();
        let mut q = ToastQueue::new();
        q.push_with_color("custom", theme::CURSOR, now);
        assert_eq!(q.current().unwrap().color, theme::CURSOR);
    }

    #[test]
    fn prune_drops_expired_only() {
        let now = t0();
        let mut q = ToastQueue::with_ttl(Duration::from_secs(3));
        q.push("a", ToastKind::Info, now);
        q.push("b", ToastKind::Info, now + Duration::from_secs(2));

        // 1s in: nothing expired yet.
        q.prune(now + Duration::from_secs(1));
        assert_eq!(q.len(), 2);

        // 3s in: 'a' is exactly at its deadline (expires_at = now+3) and
        // should be dropped; 'b' (expires at now+5) survives.
        q.prune(now + Duration::from_secs(3));
        assert_eq!(q.len(), 1);
        assert_eq!(q.current().unwrap().text, "b");

        // 6s in: 'b' is past its deadline.
        q.prune(now + Duration::from_secs(6));
        assert!(q.is_empty());
    }

    #[test]
    fn prune_uses_strict_inequality_at_deadline() {
        let now = t0();
        let mut q = ToastQueue::with_ttl(Duration::from_secs(3));
        q.push("x", ToastKind::Info, now);
        // At exactly the deadline, the toast should be considered expired.
        q.prune(now + Duration::from_secs(3));
        assert!(q.is_empty(), "toast at deadline should be pruned");
    }

    #[test]
    fn dismiss_pops_current_toast() {
        let now = t0();
        let mut q = ToastQueue::new();
        q.push("a", ToastKind::Info, now);
        q.push("b", ToastKind::Info, now);
        assert_eq!(q.current().unwrap().text, "b");

        assert!(q.dismiss());
        assert_eq!(q.current().unwrap().text, "a");

        assert!(q.dismiss());
        assert!(q.is_empty());

        // Dismissing an empty queue is a no-op that returns false.
        assert!(!q.dismiss());
    }

    #[test]
    fn clear_drops_all_toasts() {
        let now = t0();
        let mut q = ToastQueue::new();
        q.push("a", ToastKind::Info, now);
        q.push("b", ToastKind::Info, now);
        q.push("c", ToastKind::Info, now);
        q.clear();
        assert!(q.is_empty());
    }

    #[test]
    fn custom_ttl_is_honored() {
        let now = t0();
        let mut q = ToastQueue::with_ttl(Duration::from_millis(500));
        q.push("quick", ToastKind::Info, now);
        q.prune(now + Duration::from_millis(499));
        assert_eq!(q.len(), 1);
        q.prune(now + Duration::from_millis(500));
        assert!(q.is_empty());
    }

    #[test]
    fn default_ttl_is_three_seconds() {
        assert_eq!(DEFAULT_TTL, Duration::from_secs(3));
    }
}
