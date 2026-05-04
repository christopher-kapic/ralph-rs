// Cross-view multi-selection state (TUI-plan.md §5/§7).
//
// Plan list and plan detail share the same multi-select semantics: `space`
// toggles selection on the highlighted item, and the order in which items
// are selected drives the `[N]` badge rendered on each tile. This module
// owns that state generically so both views can store the same struct
// without duplicating logic.
//
// Keys are typically string IDs (`Plan.id`, `Step.id`) which survive list
// re-sorts after a refresh, so a selection made before a refresh keeps
// pointing at the same items afterwards. The generic parameter is left
// open so callers can use other key types in tests.

/// Ordered set of selected keys.
///
/// Insertion order is preserved so the i-th element of `keys` corresponds
/// to the `[N]` badge for that key (1-based: first selected = `[1]`).
/// Toggling a key that is already selected removes it; the remaining keys
/// shift down so the badge numbers stay contiguous.
#[derive(Debug, Clone, Default)]
pub struct Selection<K> {
    keys: Vec<K>,
}

impl<K: Clone + PartialEq> Selection<K> {
    /// Create an empty selection.
    pub fn new() -> Self {
        Self { keys: Vec::new() }
    }

    /// True when nothing is selected.
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Number of selected keys.
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// True if `key` is currently selected.
    pub fn is_selected(&self, key: &K) -> bool {
        self.keys.iter().any(|k| k == key)
    }

    /// 1-based selection-order index of `key`, or `None` if unselected.
    /// This is the value rendered as the `[N]` badge.
    pub fn index_of(&self, key: &K) -> Option<usize> {
        self.keys.iter().position(|k| k == key).map(|i| i + 1)
    }

    /// Toggle `key`: if already selected, remove it (later keys shift down);
    /// otherwise append it to preserve the order in which the user picked
    /// items. Returns `true` when the key is now selected.
    pub fn toggle(&mut self, key: K) -> bool {
        if let Some(pos) = self.keys.iter().position(|k| *k == key) {
            self.keys.remove(pos);
            false
        } else {
            self.keys.push(key);
            true
        }
    }

    /// Clear all selections.
    pub fn clear(&mut self) {
        self.keys.clear();
    }

    /// Read-only view of selected keys in selection order.
    pub fn as_slice(&self) -> &[K] {
        &self.keys
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let s: Selection<String> = Selection::new();
        assert!(s.is_empty());
        assert_eq!(s.len(), 0);
        assert_eq!(s.as_slice(), &[] as &[String]);
    }

    #[test]
    fn toggle_adds_then_removes() {
        let mut s: Selection<&str> = Selection::new();
        assert!(s.toggle("a"));
        assert!(s.is_selected(&"a"));
        assert_eq!(s.len(), 1);

        assert!(!s.toggle("a"));
        assert!(!s.is_selected(&"a"));
        assert!(s.is_empty());
    }

    #[test]
    fn toggle_preserves_insertion_order() {
        let mut s: Selection<&str> = Selection::new();
        s.toggle("x");
        s.toggle("y");
        s.toggle("z");
        assert_eq!(s.as_slice(), &["x", "y", "z"]);
        assert_eq!(s.index_of(&"x"), Some(1));
        assert_eq!(s.index_of(&"y"), Some(2));
        assert_eq!(s.index_of(&"z"), Some(3));
    }

    #[test]
    fn re_toggling_a_middle_key_renumbers_the_rest() {
        // Selecting [a, b, c] then toggling b off should leave [a, c]
        // with c re-numbered to 2 (not 3).
        let mut s: Selection<&str> = Selection::new();
        s.toggle("a");
        s.toggle("b");
        s.toggle("c");
        s.toggle("b");
        assert_eq!(s.as_slice(), &["a", "c"]);
        assert_eq!(s.index_of(&"a"), Some(1));
        assert_eq!(s.index_of(&"c"), Some(2));
        assert_eq!(s.index_of(&"b"), None);
    }

    #[test]
    fn re_toggling_after_remove_appends_at_the_end() {
        // After a key is removed and re-toggled, it should land at the
        // end of the order — re-selection is "fresh", not a return to its
        // prior slot.
        let mut s: Selection<&str> = Selection::new();
        s.toggle("a");
        s.toggle("b");
        s.toggle("a");
        s.toggle("a");
        assert_eq!(s.as_slice(), &["b", "a"]);
        assert_eq!(s.index_of(&"b"), Some(1));
        assert_eq!(s.index_of(&"a"), Some(2));
    }

    #[test]
    fn clear_empties_selection() {
        let mut s: Selection<&str> = Selection::new();
        s.toggle("a");
        s.toggle("b");
        assert!(!s.is_empty());
        s.clear();
        assert!(s.is_empty());
        assert_eq!(s.index_of(&"a"), None);
    }

    #[test]
    fn index_of_unselected_is_none() {
        let s: Selection<&str> = Selection::new();
        assert_eq!(s.index_of(&"missing"), None);
    }

    #[test]
    fn works_with_owned_string_keys() {
        // The expected production usage: tracking selection by Plan.id /
        // Step.id strings.
        let mut s: Selection<String> = Selection::new();
        s.toggle("p1".to_string());
        s.toggle("p2".to_string());
        assert!(s.is_selected(&"p1".to_string()));
        assert_eq!(s.index_of(&"p2".to_string()), Some(2));
    }
}
