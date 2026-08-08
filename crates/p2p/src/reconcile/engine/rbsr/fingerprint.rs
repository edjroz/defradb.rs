//! Combinable range fingerprints.
//!
//! A range is summarized by
//!
//! ```text
//! fp = SHA256( (Σ SHA256(id)) mod 2^256 || uvarint(count) )[..16]
//! ```
//!
//! matching the Go reference byte for byte. The inner sum is associative and
//! commutative, so adjacent ranges combine in O(1) via [`Accumulator::merge`] —
//! that is what lets a segment tree answer a range fingerprint in O(log n). The
//! count is folded into the outer hash so two distinct sets that happen to share
//! an additive sum still differ, which a plain additive or XOR sum would not.

use std::convert::TryInto;

use serde::de::{Deserialize, Deserializer, Error as _};
use serde::{Serialize, Serializer};
use sha2::{Digest, Sha256};

use crate::reconcile::source::ItemId;

/// Truncated digest length of a [`Fingerprint`].
pub const FINGERPRINT_LEN: usize = 16;

/// A 16-byte digest summarizing the set of item identities in a range.
///
/// Two ranges holding the same identities have equal fingerprints regardless of
/// order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Fingerprint([u8; FINGERPRINT_LEN]);

impl Fingerprint {
    /// The fingerprint of an empty range: `SHA256(0^32 || uvarint(0))[..16]`.
    ///
    /// Pinned to the Go reference's value; `fingerprint_tests.rs` checks it
    /// against a freshly computed [`Accumulator::finalize`].
    pub const EMPTY: Self = Self([
        0x7f, 0x9c, 0x9e, 0x31, 0xac, 0x82, 0x56, 0xca, 0x2f, 0x25, 0x85, 0x83, 0xdf, 0x26, 0x2d,
        0xbc,
    ]);

    /// Borrows the digest bytes.
    pub fn as_bytes(&self) -> &[u8; FINGERPRINT_LEN] {
        &self.0
    }

    /// Folds the given identities into a fresh accumulator and finalizes it.
    pub fn of<'a, I>(ids: I) -> Self
    where
        I: IntoIterator<Item = &'a ItemId>,
    {
        let mut acc = Accumulator::default();
        for id in ids {
            acc.add(id);
        }
        acc.finalize()
    }
}

impl Serialize for Fingerprint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Fingerprint {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = serde_bytes::ByteBuf::deserialize(deserializer)?;
        let digest: [u8; FINGERPRINT_LEN] = bytes
            .as_ref()
            .try_into()
            .map_err(|_| D::Error::invalid_length(bytes.len(), &"16 fingerprint bytes"))?;
        Ok(Self(digest))
    }
}

/// A running `Σ SHA256(id) mod 2^256` plus the item count.
///
/// The sum is held as four 64-bit limbs, most significant first, rather than the
/// Go reference's 32 bytes; the arithmetic is identical modulo 2^256 and
/// `fingerprint_tests.rs` pins the two against each other.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Accumulator {
    sum: [u64; 4],
    count: u64,
}

impl Accumulator {
    /// Folds one item identity into the accumulator.
    pub fn add(&mut self, id: &ItemId) {
        let digest: [u8; 32] = Sha256::digest(id.as_bytes()).into();
        let mut limbs = [0u64; 4];
        for (limb, chunk) in limbs.iter_mut().zip(digest.chunks_exact(8)) {
            *limb = u64::from_be_bytes(chunk.try_into().expect("8-byte chunk"));
        }
        add_limbs(&mut self.sum, &limbs);
        self.count += 1;
    }

    /// Folds another accumulator in, so a range fingerprint can be composed from
    /// its sub-ranges without rescanning either.
    pub fn merge(&mut self, other: &Self) {
        add_limbs(&mut self.sum, &other.sum);
        self.count += other.count;
    }

    /// Number of identities folded in.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Produces the range fingerprint.
    pub fn finalize(&self) -> Fingerprint {
        let mut hasher = Sha256::new();
        hasher.update(self.sum_be_bytes());
        hasher.update(uvarint(self.count).as_slice());
        let digest = hasher.finalize();
        Fingerprint(
            digest[..FINGERPRINT_LEN]
                .try_into()
                .expect("sha256 is longer than a fingerprint"),
        )
    }

    fn sum_be_bytes(&self) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        for (chunk, limb) in bytes.chunks_exact_mut(8).zip(self.sum) {
            chunk.copy_from_slice(&limb.to_be_bytes());
        }
        bytes
    }

    #[cfg(test)]
    fn set_sum_be_bytes(&mut self, bytes: [u8; 32]) {
        for (limb, chunk) in self.sum.iter_mut().zip(bytes.chunks_exact(8)) {
            *limb = u64::from_be_bytes(chunk.try_into().expect("8-byte chunk"));
        }
    }

    #[cfg(test)]
    fn set_count(&mut self, count: u64) {
        self.count = count;
    }
}

/// Adds `src` into `dst` modulo 2^256, discarding the carry out of the most
/// significant limb — the wraparound that makes the sum combinable.
fn add_limbs(dst: &mut [u64; 4], src: &[u64; 4]) {
    let mut carry = 0u64;
    for index in (0..4).rev() {
        let (partial, overflow_a) = dst[index].overflowing_add(src[index]);
        let (total, overflow_b) = partial.overflowing_add(carry);
        dst[index] = total;
        carry = u64::from(overflow_a || overflow_b);
    }
}

/// A stack-allocated LEB128 unsigned varint, byte-identical to Go's
/// `binary.PutUvarint`.
struct Uvarint {
    buf: [u8; 10],
    len: usize,
}

impl Uvarint {
    fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

fn uvarint(value: u64) -> Uvarint {
    let mut buf = unsigned_varint::encode::u64_buffer();
    let len = unsigned_varint::encode::u64(value, &mut buf).len();
    Uvarint { buf, len }
}

#[cfg(test)]
#[path = "fingerprint_tests.rs"]
mod fingerprint_tests;
