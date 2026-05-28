// DAG utilities shared across the codebase.
//
// Right now this module owns one thing: the reachability DFS that backs
// every "would this edge close a cycle?" check we run (plan dependencies,
// step dependencies, in-memory import-bundle DAGs). Three structurally
// identical copies of the same DFS used to live in `storage` and `import`;
// they now all funnel through [`would_create_cycle_generic`], keeping the
// algorithm in one place and the call sites at five lines each.
//
// The function is intentionally *pure*: it has no DB knowledge, no
// connection handle, no opinion about how dependencies are stored. Callers
// pass a closure that, given an id, returns that id's direct dependencies.
// That closure is allowed to fail (e.g. a SQL query against rusqlite), so
// the dependency accessor returns `Result<Vec<String>>` rather than
// `Vec<String>`.

use anyhow::Result;
use std::collections::HashSet;

/// Does adding the edge `source -> target` close a cycle in the dependency
/// graph reachable through `get_deps`?
///
/// Walks the transitive dependencies of `target` (the would-be new edge's
/// head); if `source` shows up in that walk, the edge would close a cycle.
/// A self-edge (`source == target`) is treated as a cycle, matching the
/// invariant the DB-level `CHECK` constraints enforce — we never want a
/// step or plan depending on itself.
///
/// `get_deps` is called once per visited node and returns that node's
/// direct dependencies (the heads of its outgoing edges). It may fail (the
/// closures used by `storage::would_create_cycle` /
/// `storage::would_create_step_cycle` issue SQL queries that can error);
/// the first failure short-circuits and propagates out of the DFS.
///
/// Stack/visited DFS, iterative (no recursion bound), `HashSet`-deduped so
/// a node is expanded at most once even in dense graphs. Cycle reachable
/// through `target`'s existing transitive deps is harmless to walking — we
/// stop as soon as either `source` is found or the frontier drains.
pub fn would_create_cycle_generic<F>(
    source: &str,
    target: &str,
    mut get_deps: F,
) -> Result<bool>
where
    F: FnMut(&str) -> Result<Vec<String>>,
{
    if source == target {
        return Ok(true);
    }

    let mut stack: Vec<String> = vec![target.to_string()];
    let mut visited: HashSet<String> = HashSet::new();

    while let Some(current) = stack.pop() {
        if !visited.insert(current.clone()) {
            continue;
        }
        if current == source {
            return Ok(true);
        }
        for d in get_deps(&current)? {
            if !visited.contains(&d) {
                stack.push(d);
            }
        }
    }

    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Wrap a static `HashMap<&str, Vec<&str>>` adjacency in the `Result`
    /// shape `would_create_cycle_generic` expects — pure in-memory, never
    /// fails. Mirrors the in-memory `built` lookup that
    /// `import::find_imported_cycle` performs (just with owned `String`s
    /// for the closure return shape).
    fn adj_closure<'a>(
        adj: &'a HashMap<&'a str, Vec<&'a str>>,
    ) -> impl FnMut(&str) -> Result<Vec<String>> + 'a {
        move |id: &str| {
            Ok(adj
                .get(id)
                .map(|v| v.iter().map(|s| s.to_string()).collect())
                .unwrap_or_default())
        }
    }

    #[test]
    fn self_edge_is_a_cycle() {
        let adj: HashMap<&str, Vec<&str>> = HashMap::new();
        assert!(would_create_cycle_generic("a", "a", adj_closure(&adj)).unwrap());
    }

    #[test]
    fn direct_edge_back_is_a_cycle() {
        // b -> a already; adding a -> b would close a cycle.
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        adj.insert("b", vec!["a"]);
        assert!(would_create_cycle_generic("a", "b", adj_closure(&adj)).unwrap());
    }

    #[test]
    fn transitive_edge_back_is_a_cycle() {
        // c -> b -> a; adding a -> c would close a cycle (a transitively
        // already reachable from c via b).
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        adj.insert("c", vec!["b"]);
        adj.insert("b", vec!["a"]);
        assert!(would_create_cycle_generic("a", "c", adj_closure(&adj)).unwrap());
    }

    #[test]
    fn unrelated_edge_is_not_a_cycle() {
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        adj.insert("a", vec!["b"]);
        adj.insert("c", vec!["d"]);
        assert!(!would_create_cycle_generic("a", "c", adj_closure(&adj)).unwrap());
    }

    #[test]
    fn fan_in_does_not_falsely_report_a_cycle() {
        // Two independent edges into `x`. Adding `x -> y` (a sibling root)
        // does not close a cycle: y is not reachable from x.
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        adj.insert("x", vec!["a", "b"]);
        adj.insert("y", vec!["c"]);
        assert!(!would_create_cycle_generic("x", "y", adj_closure(&adj)).unwrap());
    }

    #[test]
    fn closure_error_propagates() {
        let result: Result<bool> = would_create_cycle_generic("a", "b", |_id| {
            Err(anyhow::anyhow!("boom"))
        });
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("boom"));
    }

    #[test]
    fn pre_existing_cycle_in_graph_does_not_loop_forever() {
        // The graph itself already has a cycle (b <-> c). Asking whether
        // adding `a -> b` closes a cycle should still terminate (visited
        // dedupe) and return `false` (a is unreachable from b).
        let mut adj: HashMap<&str, Vec<&str>> = HashMap::new();
        adj.insert("b", vec!["c"]);
        adj.insert("c", vec!["b"]);
        assert!(!would_create_cycle_generic("a", "b", adj_closure(&adj)).unwrap());
    }
}
