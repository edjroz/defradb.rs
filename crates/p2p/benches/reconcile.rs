//! Micro-benchmarks for the set-reconciliation engine's two hot costs.
//!
//! The set sizes and the shape of each measurement mirror the Go reference's
//! `BenchmarkSegmentTreeFingerprint`, `BenchmarkSegmentTreeBuild`, and
//! `BenchmarkVectorFingerprintScan`, so the two sets of numbers can be read side
//! by side. Run with `cargo bench -p p2p --bench reconcile`.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use p2p::reconcile::engine::rbsr::{Fingerprint, SegmentTree};
use p2p::reconcile::source::{Item, ItemId, ItemSource, MemorySource};
use sha2::{Digest, Sha256};
use std::hint::black_box;

const SIZES: [usize; 4] = [100, 1_000, 10_000, 100_000];

/// A deterministic 32-byte identity, matching the Go bench's `tid`.
fn id(seed: usize) -> ItemId {
    ItemId::new(Sha256::digest((seed as u64).to_be_bytes()).to_vec())
}

fn source(n: usize) -> MemorySource {
    MemorySource::new((0..n).map(|seed| Item::new(0, id(seed)))).expect("distinct seeds")
}

/// Full-range fingerprint over a prebuilt tree: the per-round cost a session
/// pays many times. Expected to be O(log n).
fn fingerprint_over_tree(c: &mut Criterion) {
    let mut group = c.benchmark_group("reconcile/segment_tree_fingerprint");
    for n in SIZES {
        let source = source(n);
        let tree = SegmentTree::build(&source);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| black_box(tree.fingerprint(0, n)));
        });
    }
    group.finish();
}

/// The O(n) scan the tree replaces, kept as the comparison baseline the Go
/// reference's `Vector` provides.
fn fingerprint_by_scan(c: &mut Criterion) {
    let mut group = c.benchmark_group("reconcile/linear_fingerprint_scan");
    for n in SIZES {
        let source = source(n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| black_box(Fingerprint::of((0..n).map(|i| source.id(i)))));
        });
    }
    group.finish();
}

/// The once-per-session index build: derive identities, seal the ordered source,
/// and fold the accumulator tree. Matches the Go bench, which likewise derives
/// each identity with a hash inside the timed loop.
fn per_session_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("reconcile/session_build");
    for n in SIZES {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let source =
                    MemorySource::new((0..n).map(|seed| Item::new(0, id(seed)))).expect("distinct");
                black_box(SegmentTree::build(source).len())
            });
        });
    }
    group.finish();
}

/// The same build with identities already materialized, isolating the sort,
/// seal, and tree fold from the hashing the Go bench folds into its number.
fn per_session_build_without_id_derivation(c: &mut Criterion) {
    let mut group = c.benchmark_group("reconcile/session_build_precomputed_ids");
    for n in SIZES {
        let ids: Vec<ItemId> = (0..n).map(id).collect();
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let source = MemorySource::new(ids.iter().map(|id| Item::new(0, id.clone())))
                    .expect("distinct");
                black_box(SegmentTree::build(source).len())
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    fingerprint_over_tree,
    fingerprint_by_scan,
    per_session_build,
    per_session_build_without_id_derivation
);
criterion_main!(benches);
