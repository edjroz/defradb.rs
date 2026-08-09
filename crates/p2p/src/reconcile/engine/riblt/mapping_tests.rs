//! Pins the index mapping and the symbol hash against the reference.
//!
//! # Provenance of the vectors
//!
//! `github.com/yangl1996/riblt` was cloned read-only and driven by a small Go
//! program using only its exported API. A singleton set exposes the mapping
//! directly: coded symbol `i` of a one-element set has count 1 exactly when the
//! element maps to `i`, so `Encoder.ProduceNextCodedSymbol` over a one-element
//! set enumerates the mapping without reaching into the package's unexported
//! `randomMapping`. The driver's symbols are
//! `SHA-256("riblt-vector-<n>") || SHA-256("riblt-vector-<n>")[..4]`, padded to
//! the 36-byte CID width, and its `Hash` method is [`symbol_hash`] transcribed
//! into Go — so a mismatch here is a mismatch in the hash, the PRNG, or the
//! decay formula, and nothing else.

use super::mapping::RandomMapping;
use super::symbol::symbol_hash;
use sha2::{Digest, Sha256};

/// The `item(n)` of the vector driver.
pub(super) fn vector_item(n: usize) -> Vec<u8> {
    let digest = Sha256::digest(format!("riblt-vector-{n}").as_bytes());
    let mut symbol = digest.to_vec();
    symbol.extend_from_slice(&digest[..4]);
    symbol
}

/// `item n -> (hash, the mapped indices below 64)`, straight from the driver.
const SINGLETON_MAPPING: [(u64, &[u64]); 4] = [
    (0xa7eb_adfd_fc10_5ad7, &[0, 1, 6, 11, 17, 26, 47]),
    (0xcef6_cfff_f515_e87a, &[0, 1, 2, 7, 8, 22, 57]),
    (0xc8af_0b7c_dd32_0d21, &[0, 3, 4, 5, 7, 22, 26, 27, 50, 61]),
    (0x8377_8720_c4c3_14d1, &[0, 51, 62]),
];

#[test]
fn the_symbol_hash_matches_the_reference_drivers_hash() {
    for (n, (hash, _)) in SINGLETON_MAPPING.iter().enumerate() {
        assert_eq!(symbol_hash(&vector_item(n)), *hash, "item {n}");
    }
}

#[test]
fn the_index_mapping_matches_the_reference() {
    for (n, (hash, expected)) in SINGLETON_MAPPING.iter().enumerate() {
        let mut mapping = RandomMapping::new(*hash);
        let mut indices = vec![mapping.index()];
        loop {
            let next = mapping.next_index();
            if next >= 64 {
                break;
            }
            indices.push(next);
        }
        assert_eq!(indices, *expected, "item {n}");
    }
}

/// Every symbol is in coded symbol 0, which is what makes the whole shared
/// prefix of two sets cancel in the very first cell.
#[test]
fn every_symbol_starts_at_index_zero() {
    for seed in [0u64, 1, 42, u64::MAX] {
        assert_eq!(RandomMapping::new(seed).index(), 0);
    }
}

/// The indices a symbol maps to must ascend, or the encoder's priority queue
/// would revisit a coded symbol it has already emitted.
#[test]
fn indices_ascend() {
    for seed in [1u64, 7, 0x5555_5555_5555_5555, u64::MAX] {
        let mut mapping = RandomMapping::new(seed);
        let mut previous = mapping.index();
        for _ in 0..1000 {
            let next = mapping.next_index();
            assert!(next >= previous, "seed {seed}: {next} < {previous}");
            previous = next;
        }
    }
}

/// The `1/(1+i/2)` participation decay is the whole reason a prefix of the
/// stream is a well-proportioned sketch. Over many seeds, the count of symbols
/// mapped to index `i` must track `n/(1+i/2)`.
#[test]
fn participation_decays_as_one_over_index() {
    const SEEDS: u64 = 20_000;
    let mut hits = [0u64; 8];
    for seed in 1..=SEEDS {
        let mut mapping = RandomMapping::new(symbol_hash(&seed.to_be_bytes()));
        let mut index = mapping.index();
        while index < hits.len() as u64 {
            hits[index as usize] += 1;
            index = mapping.next_index();
        }
    }

    assert_eq!(hits[0], SEEDS, "index 0 holds every symbol");
    for (index, hit) in hits.iter().enumerate().skip(1) {
        let expected = SEEDS as f64 / (1.0 + index as f64 / 2.0);
        let ratio = *hit as f64 / expected;
        assert!(
            (0.9..1.1).contains(&ratio),
            "index {index}: {hit} hits against an expected {expected:.0}"
        );
    }
}
