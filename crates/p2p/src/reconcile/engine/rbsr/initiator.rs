//! The initiator's per-round transform.
//!
//! The initiator never changes the tiling: every incoming range maps to exactly
//! one outgoing range over the same bounds. It answers a matching fingerprint
//! with a skip, a mismatching one by echoing its own, and an ID list by
//! resolving the leaf into its need and have sets and then skipping. Refinement
//! is the responder's job alone, which is what keeps the initiator's outgoing
//! message the same shape as the one it received and makes an all-skip message
//! an unambiguous convergence signal.

use std::collections::HashSet;

use super::caps::MAX_IDS_PER_RANGE;
use super::message::{Mode, Range, RbsrMessage};
use super::segment_tree::SegmentTree;
use crate::reconcile::engine::Diff;
use crate::reconcile::error::{ReconcileError, Result};
use crate::reconcile::source::{Bound, ItemId, ItemSource};

/// The opening message: one full-range fingerprint over the whole local set.
pub(super) fn initiate<S: ItemSource>(index: &SegmentTree<S>) -> RbsrMessage {
    RbsrMessage::new(vec![Range::fingerprint(
        Bound::Max,
        index.fingerprint(0, index.len()),
    )])
}

/// Maps the responder's message to the next outgoing message, recording any
/// resolved leaves into `diff`.
pub(super) fn reconcile<S: ItemSource>(
    index: &SegmentTree<S>,
    diff: &mut Diff,
    incoming: &RbsrMessage,
) -> Result<RbsrMessage> {
    let mut out = Vec::with_capacity(incoming.ranges().len());
    let mut lo = Bound::Min;

    for range in incoming.ranges() {
        let hi = &range.upper_bound;
        let (start, end) = index.window(&lo, hi)?;

        match &range.mode {
            Mode::Skip => out.push(Range::skip(hi.clone())),
            Mode::Fingerprint(theirs) => {
                let mine = index.fingerprint(start, end);
                if mine == *theirs {
                    out.push(Range::skip(hi.clone()));
                } else {
                    out.push(Range::fingerprint(hi.clone(), mine));
                }
            }
            Mode::IdList(ids) => {
                resolve_leaf(index, diff, ids, start, end)?;
                out.push(Range::skip(hi.clone()));
            }
        }

        lo = hi.clone();
    }

    Ok(RbsrMessage::new(out))
}

/// Records the difference a listed range settles: identities the peer listed but
/// the local set lacks are needed, and local identities the peer omitted are
/// held.
fn resolve_leaf<S: ItemSource>(
    index: &SegmentTree<S>,
    diff: &mut Diff,
    remote_ids: &[ItemId],
    start: usize,
    end: usize,
) -> Result<()> {
    if remote_ids.len() > MAX_IDS_PER_RANGE {
        return Err(ReconcileError::IdListTooLarge {
            size: remote_ids.len(),
            max: MAX_IDS_PER_RANGE,
        });
    }

    let source = index.source();
    let remote: HashSet<&ItemId> = remote_ids.iter().collect();
    let local: HashSet<&ItemId> = (start..end).map(|i| source.id(i)).collect();

    for id in remote_ids {
        if !local.contains(id) {
            diff.record_need(id.clone());
        }
    }
    for i in start..end {
        let id = source.id(i);
        if !remote.contains(id) {
            diff.record_have(id.clone());
        }
    }
    Ok(())
}
