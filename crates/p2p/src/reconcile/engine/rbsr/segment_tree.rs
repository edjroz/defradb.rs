//! An `O(log n)` range-fingerprint index over an [`ItemSource`].
//!
//! Each leaf accumulates one item's identity and each internal node is the merge
//! of its children, so a range fold touches `O(log n)` nodes. That is sound only
//! because [`Accumulator::merge`] is associative *and* commutative, which lets
//! the fold visit nodes in whatever order the iterative walk reaches them.
//!
//! The tree is built once per session in `O(n)` and then answers every range
//! query in the session's many rounds in `O(log n)`. Building it fresh per
//! session — rather than maintaining it across writes — matches the Go reference
//! and is deliberate for this phase: a standing index only pays for itself once
//! it sits on the real headstore, and its write-path cost has to be measured
//! rather than assumed.

use super::fingerprint::{Accumulator, Fingerprint};
use crate::reconcile::error::{ReconcileError, Result};
use crate::reconcile::source::{Bound, ItemSource};

/// A sealed fingerprint index over a sealed [`ItemSource`].
pub struct SegmentTree<S: ItemSource> {
    source: S,
    /// One-indexed iterative segment tree: leaf `i` lives at `size + i`, and
    /// internal node `i` is `merge(node 2i, node 2i+1)`.
    nodes: Vec<Accumulator>,
    size: usize,
}

impl<S: ItemSource> SegmentTree<S> {
    /// Builds the accumulator tree over the source in `O(n)`.
    pub fn build(source: S) -> Self {
        let size = source.len();
        if size == 0 {
            return Self {
                source,
                nodes: Vec::new(),
                size,
            };
        }

        let mut nodes = vec![Accumulator::default(); 2 * size];
        for index in 0..size {
            nodes[size + index].add(source.id(index));
        }
        for index in (1..size).rev() {
            let mut node = nodes[2 * index];
            node.merge(&nodes[2 * index + 1]);
            nodes[index] = node;
        }

        Self {
            source,
            nodes,
            size,
        }
    }

    /// The indexed source.
    pub fn source(&self) -> &S {
        &self.source
    }

    /// Number of indexed items.
    pub fn len(&self) -> usize {
        self.size
    }

    /// Whether the indexed source is empty.
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    /// Maps a `[lo, hi)` bound pair onto a half-open index window.
    ///
    /// Both peers run this over the same bounds, which is why split boundaries
    /// must land on real item sort keys. Fails on an inverted pair.
    pub fn window(&self, lo: &Bound, hi: &Bound) -> Result<(usize, usize)> {
        let start = self.source.seek(lo);
        let end = self.source.seek(hi);
        if start > end {
            return Err(ReconcileError::MalformedBound);
        }
        Ok((start, end))
    }

    /// Summarizes the items in the half-open index range `[lo, hi)`.
    pub fn fingerprint(&self, lo: usize, hi: usize) -> Fingerprint {
        self.accumulate(lo, hi).finalize()
    }

    /// The combinable partial state for `[lo, hi)`, folded from `O(log n)` cached
    /// nodes.
    pub fn accumulate(&self, lo: usize, hi: usize) -> Accumulator {
        let mut acc = Accumulator::default();
        if lo >= hi || self.size == 0 {
            return acc;
        }

        let mut left = lo + self.size;
        let mut right = hi + self.size;
        while left < right {
            if left & 1 == 1 {
                acc.merge(&self.nodes[left]);
                left += 1;
            }
            if right & 1 == 1 {
                right -= 1;
                acc.merge(&self.nodes[right]);
            }
            left >>= 1;
            right >>= 1;
        }
        acc
    }
}

#[cfg(test)]
#[path = "segment_tree_tests.rs"]
mod segment_tree_tests;
