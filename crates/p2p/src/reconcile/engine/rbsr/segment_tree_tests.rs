use super::*;
use crate::reconcile::source::{Item, ItemId, MemorySource};
use sha2::{Digest, Sha256};

fn id(n: u64) -> ItemId {
    ItemId::new(Sha256::digest(n.to_be_bytes()).to_vec())
}

fn source(n: u64) -> MemorySource {
    MemorySource::new((0..n).map(|i| Item::new(0, id(i)))).expect("distinct items")
}

fn scan(source: &MemorySource, lo: usize, hi: usize) -> Fingerprint {
    Fingerprint::of((lo..hi).map(|i| source.id(i)))
}

#[test]
fn every_range_matches_a_linear_scan() {
    for n in [0u64, 1, 2, 3, 7, 8, 9, 16, 17, 64, 129] {
        let src = source(n);
        let tree = SegmentTree::build(&src);
        let len = n as usize;
        for lo in 0..=len {
            for hi in lo..=len {
                assert_eq!(
                    tree.fingerprint(lo, hi),
                    scan(&src, lo, hi),
                    "n={n} range=[{lo},{hi})"
                );
            }
        }
    }
}

#[test]
fn an_empty_range_is_the_empty_fingerprint() {
    let src = source(32);
    let tree = SegmentTree::build(&src);
    assert_eq!(tree.fingerprint(0, 0), Fingerprint::EMPTY);
    assert_eq!(tree.fingerprint(9, 9), Fingerprint::EMPTY);
    assert_eq!(tree.fingerprint(32, 32), Fingerprint::EMPTY);
}

#[test]
fn an_empty_source_fingerprints_empty() {
    let src = source(0);
    let tree = SegmentTree::build(&src);
    assert_eq!(tree.len(), 0);
    assert!(tree.is_empty());
    assert_eq!(tree.fingerprint(0, 0), Fingerprint::EMPTY);
}

#[test]
fn adjacent_accumulations_compose_into_the_whole() {
    let src = source(500);
    let tree = SegmentTree::build(&src);
    let mut composed = tree.accumulate(0, 173);
    composed.merge(&tree.accumulate(173, 500));
    assert_eq!(composed.finalize(), tree.fingerprint(0, 500));
    assert_eq!(composed.count(), 500);
}

#[test]
fn a_large_tree_matches_the_scan_on_sampled_ranges() {
    let src = source(4096);
    let tree = SegmentTree::build(&src);
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for _ in 0..200 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let a = (state % 4097) as usize;
        let b = ((state >> 20) % 4097) as usize;
        let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
        assert_eq!(tree.fingerprint(lo, hi), scan(&src, lo, hi), "[{lo},{hi})");
    }
}
