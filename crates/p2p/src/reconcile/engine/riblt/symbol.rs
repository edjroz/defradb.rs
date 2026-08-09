//! The XOR group the sketch is built over, and one cell of that sketch.
//!
//! A source symbol is the item's identity bytes, unmodified — for a CIDv1
//! `dag-cbor/sha2-256` item that is a uniform 36 bytes, so the identity can *be*
//! the symbol and a decoded symbol is directly fetchable without a resolve
//! round. All symbols in one session share a width, because XOR over differing
//! widths is not a group operation; [`super::engine`] enforces that.
//!
//! The checksum is the reference's `Hash`: a `u64` that must not be homomorphic
//! over XOR, so that a cell claiming to hold one symbol can be checked. The
//! reference's own tests use SipHash with arbitrary keys; here it is the first
//! eight bytes of SHA-256 read little-endian, which the RBSR engine's hash
//! already brings in and which a Go driver reproduces in three lines. A false
//! peel is therefore a `2^-64` event per check, the width the recovered design
//! doc asked for.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Direction of an [`CodedSymbol::apply`] that adds a symbol to a cell.
pub(super) const ADD: i64 = 1;

/// Direction of an [`CodedSymbol::apply`] that removes one.
pub(super) const REMOVE: i64 = -1;

/// The checksum of one source symbol.
pub(super) fn symbol_hash(symbol: &[u8]) -> u64 {
    let digest = Sha256::digest(symbol);
    u64::from_le_bytes(digest[..8].try_into().expect("SHA-256 is 32 bytes"))
}

/// A source symbol bundled with its checksum, so the checksum is computed once.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct HashedSymbol {
    symbol: Vec<u8>,
    hash: u64,
}

impl HashedSymbol {
    /// Hashes and bundles a source symbol.
    pub(super) fn new(symbol: Vec<u8>) -> Self {
        let hash = symbol_hash(&symbol);
        Self { symbol, hash }
    }

    /// The symbol's checksum, which is also its mapping seed.
    pub(super) fn hash(&self) -> u64 {
        self.hash
    }

    /// The raw symbol bytes.
    pub(super) fn symbol(&self) -> &[u8] {
        &self.symbol
    }
}

/// One cell of a rateless sketch: the XOR of its members, the XOR of their
/// checksums, and how many members it has.
///
/// The count is signed because a decoder subtracts the peer's cell from its own:
/// `+1` then means "the peer holds it and I do not" and `-1` the reverse, which
/// is how one stream yields both halves of the difference.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CodedSymbol {
    #[serde(with = "serde_bytes")]
    sum: Vec<u8>,
    checksum: u64,
    count: i64,
}

impl CodedSymbol {
    /// The identity cell of the given symbol width.
    pub(super) fn zero(width: usize) -> Self {
        Self {
            sum: vec![0; width],
            checksum: 0,
            count: 0,
        }
    }

    /// Builds a cell from its parts, for decoding one off the wire.
    pub fn new(sum: Vec<u8>, checksum: u64, count: i64) -> Self {
        Self {
            sum,
            checksum,
            count,
        }
    }

    /// The XOR of the cell's member symbols.
    pub fn sum(&self) -> &[u8] {
        &self.sum
    }

    /// The XOR of the cell's member checksums.
    pub fn checksum(&self) -> u64 {
        self.checksum
    }

    /// The signed number of members.
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Adds or removes one source symbol.
    ///
    /// The XOR runs over the shorter of the two widths. Widths are equal for
    /// anything a session accepts; tolerating the difference here means a
    /// mismatched frame is answered with an error from the engine rather than a
    /// panic from the arithmetic.
    pub(super) fn apply(&mut self, symbol: &HashedSymbol, direction: i64) {
        for (cell, byte) in self.sum.iter_mut().zip(symbol.symbol.iter()) {
            *cell ^= byte;
        }
        self.checksum ^= symbol.hash;
        self.count = self.count.saturating_add(direction);
    }

    /// Whether the cell holds exactly one symbol, which its checksum confirms.
    ///
    /// A zero-width cell is never pure however its checksum reads. Symbols are
    /// item identities and the empty identity is not an item, so a session that
    /// reconciles nothing must not be able to peel something out of it.
    pub(super) fn is_pure(&self) -> bool {
        !self.sum.is_empty()
            && (self.count == 1 || self.count == -1)
            && self.checksum == symbol_hash(&self.sum)
    }

    /// Whether the cell is the identity — nothing left in it to explain.
    ///
    /// All three fields, not just the count and the checksum. Two of the three
    /// are the peer's to choose freely, so a cell carrying an arbitrary sum with
    /// a zeroed count and checksum would otherwise read as a fully explained
    /// residual, which is a decoder concluding it is in sync from a frame that
    /// told it nothing.
    pub(super) fn is_empty(&self) -> bool {
        self.count == 0 && self.checksum == 0 && self.sum.iter().all(|byte| *byte == 0)
    }
}
