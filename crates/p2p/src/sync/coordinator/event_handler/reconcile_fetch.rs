//! Which of a session's discovered heads this node actually has to fetch.
//!
//! A session reports every head the peer holds that this node's snapshot did not
//! contain. Two things make that list larger than the work behind it, and both
//! grow with how many sessions a scenario runs:
//!
//! - the same head can be named by more than one entry, and
//! - the snapshot is taken when the session opens, so a head that arrived while
//!   the session was running is already local by the time the session ends.
//!
//! Filtering here makes the fan-out the number of heads still missing rather
//! than the number of heads discovered. The presence rule is BranchableSync's:
//! holding the head block does not imply holding its ancestors, so a head is
//! only skipped when its whole DAG is local *and* merged.

use std::collections::HashSet;

use blockstore::Blockstore;
use cid::Cid;

use crate::reconcile::ItemId;
use crate::sync::manager::links::find_all_missing_links;

/// The distinct head CIDs from `need` this node cannot already satisfy locally,
/// in the order they were first named.
pub(super) async fn heads_to_fetch<B: Blockstore>(blockstore: &B, need: &[ItemId]) -> Vec<Cid> {
    let mut seen = HashSet::new();
    let mut wanted = Vec::new();
    for id in need {
        let Ok(cid) = Cid::try_from(id.as_bytes()) else {
            continue;
        };
        if !seen.insert(cid) {
            continue;
        }
        if satisfied_locally(blockstore, &cid).await {
            continue;
        }
        wanted.push(cid);
    }
    wanted
}

/// Whether the DAG rooted at `cid` is already complete and merged here.
///
/// Anything unknown reads as not satisfied, so an unreadable block or a walk
/// that fails costs a fetch rather than a silently skipped head.
async fn satisfied_locally<B: Blockstore>(blockstore: &B, cid: &Cid) -> bool {
    let Ok(Some(data)) = blockstore.get(cid).await else {
        return false;
    };
    match find_all_missing_links(blockstore, &data).await {
        Ok(missing) if missing.is_empty() => matches!(blockstore.is_merged(cid).await, Ok(true)),
        _ => false,
    }
}

#[cfg(test)]
#[path = "reconcile_fetch_tests.rs"]
mod reconcile_fetch_tests;
