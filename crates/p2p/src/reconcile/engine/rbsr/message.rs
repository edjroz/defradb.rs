//! The RBSR wire message: an ordered tiling of the keyspace.

use serde::{Deserialize, Serialize};

use super::fingerprint::Fingerprint;
use crate::reconcile::source::{Bound, ItemId};

/// How one [`Range`] is to be reconciled.
///
/// The Go reference carries this as a tag byte beside three optional fields;
/// folding the payload into the variant makes the invalid combinations
/// unrepresentable. The tag values and their meanings are unchanged.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mode {
    /// Both peers agree on this range: nothing to do.
    Skip,
    /// A fingerprint to compare; a mismatch is split or listed.
    Fingerprint(Fingerprint),
    /// The explicit identities the sender holds in this range. An empty list
    /// means "I hold nothing here".
    IdList(Vec<ItemId>),
}

/// One contiguous slice of the keyspace.
///
/// The lower bound is the previous range's [`Range::upper_bound`]; the first
/// range's lower bound is [`Bound::Min`]. The upper bound is exclusive, and the
/// final range's upper bound is [`Bound::Max`], so a message's ranges tile
/// `[Min, Max)` completely and in ascending order.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Range {
    /// Exclusive upper bound of this range.
    pub upper_bound: Bound,
    /// How the range is handled.
    pub mode: Mode,
}

impl Range {
    /// A range both peers agree on.
    pub fn skip(upper_bound: Bound) -> Self {
        Self {
            upper_bound,
            mode: Mode::Skip,
        }
    }

    /// A range summarized by a fingerprint.
    pub fn fingerprint(upper_bound: Bound, fingerprint: Fingerprint) -> Self {
        Self {
            upper_bound,
            mode: Mode::Fingerprint(fingerprint),
        }
    }

    /// A range listed out identity by identity.
    pub fn id_list(upper_bound: Bound, ids: Vec<ItemId>) -> Self {
        Self {
            upper_bound,
            mode: Mode::IdList(ids),
        }
    }
}

/// An ordered list of contiguous ranges tiling `[Min, Max)`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RbsrMessage {
    ranges: Vec<Range>,
}

impl RbsrMessage {
    /// Wraps an ascending, contiguous range tiling.
    pub fn new(ranges: Vec<Range>) -> Self {
        Self { ranges }
    }

    /// Borrows the ranges in ascending order.
    pub fn ranges(&self) -> &[Range] {
        &self.ranges
    }

    /// Whether every range is [`Mode::Skip`] — the convergence sentinel. An
    /// empty message is vacuously all-skip.
    pub fn is_all_skip(&self) -> bool {
        self.ranges.iter().all(|range| range.mode == Mode::Skip)
    }
}
