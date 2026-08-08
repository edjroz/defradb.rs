//! The ordered `(key, id)` view a reconciliation engine reads its local set from.
//!
//! [`ItemSource`] is the only seam between the protocol engines and where the
//! items actually live. Phase 1 ships [`MemorySource`]; the headstore-backed
//! adapter arrives with the transport wiring.

use serde::{Deserialize, Serialize};

use super::error::{ReconcileError, Result};

/// Length of the big-endian height prefix of a [`SortKey`].
pub const HEIGHT_PREFIX_LEN: usize = 8;

/// Raw bytes identifying one reconcilable item — a block CID's bytes in
/// production, opaque to the protocol.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ItemId(#[serde(with = "serde_bytes")] Vec<u8>);

impl ItemId {
    /// Wraps raw identity bytes.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Borrows the raw identity bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// The total order the keyspace is tiled along: an 8-byte big-endian block
/// height followed by the item's identity bytes.
///
/// Ordering by height groups items into causal layers while the identity bytes
/// break ties into a strict total order. Because those bytes are a content hash
/// in production, the keyspace within a layer is hash-ordered, which is what
/// makes the responder's even index splits land on evenly sized sub-ranges.
/// Callers with no meaningful height pass `0`, yielding plain identity order.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SortKey(#[serde(with = "serde_bytes")] Vec<u8>);

impl SortKey {
    /// Builds the sort key for an item at the given block height.
    pub fn new(height: u64, id: &ItemId) -> Self {
        let mut key = Vec::with_capacity(HEIGHT_PREFIX_LEN + id.0.len());
        key.extend_from_slice(&height.to_be_bytes());
        key.extend_from_slice(&id.0);
        Self(key)
    }

    /// Borrows the raw key bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A range boundary in the reconciled keyspace.
///
/// The derived ordering is exactly the protocol's: `Min` precedes every real
/// key and `Max` follows every real key. The Go reference encodes these two
/// sentinels as an empty and a nil byte slice respectively and warns that the
/// distinction may not survive a CBOR round trip; representing them as variants
/// removes that hazard.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum Bound {
    /// The inclusive low end of the keyspace.
    Min,
    /// A real sort key.
    Key(SortKey),
    /// The exclusive "+infinity" high end of the keyspace.
    Max,
}

/// One element of a reconcilable set.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Item {
    key: SortKey,
    id: ItemId,
}

impl Item {
    /// Builds an item at the given block height.
    pub fn new(height: u64, id: ItemId) -> Self {
        Self {
            key: SortKey::new(height, &id),
            id,
        }
    }

    /// The item's position in the reconciled keyspace.
    pub fn key(&self) -> &SortKey {
        &self.key
    }

    /// The item's identity.
    pub fn id(&self) -> &ItemId {
        &self.id
    }
}

/// A sealed, ordered, duplicate-free view of a local set.
///
/// Implementations MUST present items in strictly ascending [`SortKey`] order
/// and MUST NOT change for the lifetime of a session: both peers derive index
/// windows from the same bounds, so a source that shifted mid-session would
/// desynchronize the tiling. The Go reference makes the same "sealed" promise.
pub trait ItemSource {
    /// Number of items in the source.
    fn len(&self) -> usize;

    /// The sort key of the item at `index`, which must be less than [`Self::len`].
    fn key(&self, index: usize) -> &SortKey;

    /// The identity of the item at `index`, which must be less than [`Self::len`].
    fn id(&self, index: usize) -> &ItemId;

    /// Whether the source holds no items.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The smallest index whose key is at or above `bound` — the lower-bound
    /// insertion point. [`Bound::Min`] maps to `0` and [`Bound::Max`] to
    /// [`Self::len`].
    fn seek(&self, bound: &Bound) -> usize {
        let target = match bound {
            Bound::Min => return 0,
            Bound::Max => return self.len(),
            Bound::Key(key) => key,
        };
        let (mut lo, mut hi) = (0usize, self.len());
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.key(mid) < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }
}

/// An in-memory [`ItemSource`] over a sorted, sealed item vector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemorySource {
    items: Vec<Item>,
}

impl MemorySource {
    /// Sorts the given items by sort key and seals them into a source.
    ///
    /// Returns [`ReconcileError::DuplicateItem`] if two items share a sort key:
    /// the fingerprint's set semantics depend on there being none.
    pub fn new(items: impl IntoIterator<Item = Item>) -> Result<Self> {
        let mut items: Vec<Item> = items.into_iter().collect();
        items.sort_by(|a, b| a.key.cmp(&b.key));
        if items.windows(2).any(|pair| pair[0].key == pair[1].key) {
            return Err(ReconcileError::DuplicateItem);
        }
        Ok(Self { items })
    }
}

impl ItemSource for MemorySource {
    fn len(&self) -> usize {
        self.items.len()
    }

    fn key(&self, index: usize) -> &SortKey {
        &self.items[index].key
    }

    fn id(&self, index: usize) -> &ItemId {
        &self.items[index].id
    }
}

impl<S: ItemSource + ?Sized> ItemSource for &S {
    fn len(&self) -> usize {
        (**self).len()
    }

    fn key(&self, index: usize) -> &SortKey {
        (**self).key(index)
    }

    fn id(&self, index: usize) -> &ItemId {
        (**self).id(index)
    }

    fn seek(&self, bound: &Bound) -> usize {
        (**self).seek(bound)
    }
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod source_tests;
