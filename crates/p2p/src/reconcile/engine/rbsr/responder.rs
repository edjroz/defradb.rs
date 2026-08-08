//! The responder's refinement, which carries no per-session state.
//!
//! For each incoming range the responder recomputes its own fingerprint over the
//! bound-derived window and answers:
//!
//! - skip or ID-list range — already settled by the initiator, mirror a skip;
//! - fingerprint match — skip;
//! - mismatch over at most [`ID_LIST_THRESHOLD`] items — list the window out;
//! - larger mismatch — split into [`BRANCHING_FACTOR`] sub-ranges.
//!
//! The output tiles the same keyspace as the input. Once
//! [`MAX_RANGES_PER_MESSAGE`] ranges have been produced, refinement of the rest
//! is deferred to a later round by echoing an unsplit fingerprint, so no single
//! message grows without bound.

use super::caps::{BRANCHING_FACTOR, ID_LIST_THRESHOLD, MAX_IDS_PER_RANGE, MAX_RANGES_PER_MESSAGE};
use super::message::{Mode, Range, RbsrMessage};
use super::segment_tree::SegmentTree;
use crate::reconcile::error::{ReconcileError, Result};
use crate::reconcile::source::{Bound, ItemSource};

/// Answers an incoming message from the local index.
pub(super) fn respond<S: ItemSource>(
    index: &SegmentTree<S>,
    incoming: &RbsrMessage,
) -> Result<RbsrMessage> {
    let mut out: Vec<Range> = Vec::with_capacity(incoming.ranges().len());
    let mut lo = Bound::Min;

    for range in incoming.ranges() {
        let hi = &range.upper_bound;
        let (start, end) = index.window(&lo, hi)?;

        match &range.mode {
            Mode::Skip => out.push(Range::skip(hi.clone())),
            Mode::IdList(ids) => {
                if ids.len() > MAX_IDS_PER_RANGE {
                    return Err(ReconcileError::IdListTooLarge {
                        size: ids.len(),
                        max: MAX_IDS_PER_RANGE,
                    });
                }
                out.push(Range::skip(hi.clone()));
            }
            Mode::Fingerprint(theirs) => {
                let mine = index.fingerprint(start, end);
                if mine == *theirs {
                    out.push(Range::skip(hi.clone()));
                } else if out.len() >= MAX_RANGES_PER_MESSAGE {
                    out.push(Range::fingerprint(hi.clone(), mine));
                } else if end - start <= ID_LIST_THRESHOLD {
                    out.push(id_list(index, hi, start, end));
                } else {
                    split(index, hi, start, end, &mut out);
                }
            }
        }

        lo = hi.clone();
    }

    Ok(RbsrMessage::new(out))
}

/// Lists the window out. Only reached when the window holds at most
/// [`ID_LIST_THRESHOLD`] items, so the list never exceeds
/// [`MAX_IDS_PER_RANGE`].
fn id_list<S: ItemSource>(index: &SegmentTree<S>, hi: &Bound, start: usize, end: usize) -> Range {
    let source = index.source();
    let ids = (start..end).map(|i| source.id(i).clone()).collect();
    Range::id_list(hi.clone(), ids)
}

/// Divides the window into up to [`BRANCHING_FACTOR`] contiguous buckets.
///
/// Bucket boundaries are taken from real item sort keys so the peer's `seek`
/// derives an identical window from the same bound; the final bucket inherits
/// the original upper bound. Splitting by index rather than by keyspace is what
/// makes the buckets evenly sized regardless of how the keys cluster.
fn split<S: ItemSource>(
    index: &SegmentTree<S>,
    hi: &Bound,
    start: usize,
    end: usize,
    out: &mut Vec<Range>,
) {
    let span = end - start;
    let buckets = BRANCHING_FACTOR.min(span);

    let mut bucket_start = start;
    for bucket in 1..=buckets {
        let bucket_end = start + (span * bucket) / buckets;
        let upper = if bucket < buckets {
            Bound::Key(index.source().key(bucket_end).clone())
        } else {
            hi.clone()
        };
        out.push(Range::fingerprint(
            upper,
            index.fingerprint(bucket_start, bucket_end),
        ));
        bucket_start = bucket_end;
    }
}
