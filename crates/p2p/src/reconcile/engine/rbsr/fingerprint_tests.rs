use super::*;
use crate::reconcile::source::ItemId;
use sha2::{Digest, Sha256};

fn id(n: u64) -> ItemId {
    ItemId::new(Sha256::digest(n.to_be_bytes()).to_vec())
}

/// The Go reference's byte-wise `acc256.addInto`: add two 32-byte big-endian
/// values modulo 2^256, discarding the carry out of the top byte. The Rust
/// accumulator uses 64-bit limbs instead; these tests pin the two to the same
/// arithmetic so phase 3's cross-node comparison is meaningful.
fn go_add_into(dst: &mut [u8; 32], src: &[u8; 32]) {
    let mut carry: u16 = 0;
    for i in (0..32).rev() {
        let sum = u16::from(dst[i]) + u16::from(src[i]) + carry;
        dst[i] = sum as u8;
        carry = sum >> 8;
    }
}

fn go_sum(ids: &[ItemId]) -> [u8; 32] {
    let mut acc = [0u8; 32];
    for item in ids {
        let digest: [u8; 32] = Sha256::digest(item.as_bytes()).into();
        go_add_into(&mut acc, &digest);
    }
    acc
}

#[test]
fn limb_addition_matches_the_go_byte_wise_accumulator() {
    for n in [0usize, 1, 2, 7, 64, 257] {
        let ids: Vec<ItemId> = (0..n as u64).map(id).collect();
        let mut acc = Accumulator::default();
        for item in &ids {
            acc.add(item);
        }
        assert_eq!(acc.sum_be_bytes(), go_sum(&ids), "n={n}");
        assert_eq!(acc.count(), n as u64);
    }
}

#[test]
fn limb_addition_wraps_at_2_pow_256() {
    let mut acc = Accumulator::default();
    acc.set_sum_be_bytes([0xff; 32]);
    let mut other = Accumulator::default();
    other.set_sum_be_bytes({
        let mut v = [0u8; 32];
        v[31] = 1;
        v
    });
    acc.merge(&other);
    assert_eq!(
        acc.sum_be_bytes(),
        [0u8; 32],
        "carry out of the top is discarded"
    );
}

#[test]
fn empty_fingerprint_matches_the_pinned_go_vector() {
    assert_eq!(
        hex::encode(Fingerprint::EMPTY.as_bytes()),
        "7f9c9e31ac8256ca2f258583df262dbc",
        "SHA256(0^32 || uvarint(0))[:16]"
    );
    assert_eq!(Accumulator::default().finalize(), Fingerprint::EMPTY);
}

/// Cross-implementation vectors, produced by running the Go reference's own
/// `FingerprintOf` over `SHA256(bigEndian64(i))` for `i` in `0..n`. If the
/// fingerprint ever stops matching these, two nodes stop agreeing on what a
/// range contains and the phase 3 comparison is measuring different protocols.
#[test]
fn fingerprints_match_the_go_reference_vectors() {
    const VECTORS: [(u64, &str); 6] = [
        (1, "2c993803505a339bf219b76f3c2ff4c7"),
        (2, "bf33a8abd7dfe04feef8335ddfa0092e"),
        (7, "553a45ecd10a595bc5efa8057f53fec9"),
        (64, "fc0b851ff0ee6a1391f487270c7f890b"),
        (257, "abc4b96501bf2308ccdbbe96576ac0d1"),
        (1000, "e868e4f7cc2d5f18b2f01f417f02e0fe"),
    ];

    for (n, expected) in VECTORS {
        let ids: Vec<ItemId> = (0..n).map(id).collect();
        assert_eq!(
            hex::encode(Fingerprint::of(ids.iter()).as_bytes()),
            expected,
            "n={n}"
        );
    }
}

#[test]
fn fingerprint_is_order_independent() {
    let ids: Vec<ItemId> = (0..32u64).map(id).collect();
    let forward = Fingerprint::of(ids.iter());
    let reversed = Fingerprint::of(ids.iter().rev());
    assert_eq!(forward, reversed);
}

#[test]
fn merge_of_adjacent_ranges_equals_the_whole_range() {
    let ids: Vec<ItemId> = (0..100u64).map(id).collect();

    let mut left = Accumulator::default();
    for item in &ids[..37] {
        left.add(item);
    }
    let mut right = Accumulator::default();
    for item in &ids[37..] {
        right.add(item);
    }
    left.merge(&right);

    assert_eq!(left.finalize(), Fingerprint::of(ids.iter()));
    assert_eq!(left.count(), 100);
}

#[test]
fn the_count_fold_defeats_additive_cancellation() {
    // Two accumulators contrived to share a sum but not a count: the sum of the
    // empty set equals the sum of a set whose digests cancel to zero mod 2^256.
    let mut cancelling = Accumulator::default();
    cancelling.set_sum_be_bytes([0u8; 32]);
    cancelling.set_count(4);

    assert_ne!(
        cancelling.finalize(),
        Fingerprint::EMPTY,
        "identical sums with different counts must not collide"
    );
}

#[test]
fn distinct_sets_produce_distinct_fingerprints() {
    let a: Vec<ItemId> = (0..16u64).map(id).collect();
    let b: Vec<ItemId> = (1..17u64).map(id).collect();
    assert_ne!(Fingerprint::of(a.iter()), Fingerprint::of(b.iter()));
}

#[test]
fn uvarint_matches_the_go_put_uvarint_encoding() {
    assert_eq!(uvarint(0).as_slice(), &[0x00]);
    assert_eq!(uvarint(1).as_slice(), &[0x01]);
    assert_eq!(uvarint(127).as_slice(), &[0x7f]);
    assert_eq!(uvarint(128).as_slice(), &[0x80, 0x01]);
    assert_eq!(uvarint(300).as_slice(), &[0xac, 0x02]);
    assert_eq!(uvarint(16384).as_slice(), &[0x80, 0x80, 0x01]);
}
