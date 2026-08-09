//! Micro-benchmarks for the rateless engine's two costs.
//!
//! They are on different axes, which is the whole point of the protocol: the
//! encoder's cost is a function of the set size `n` because the accepted design
//! enumerates the set once per session, while the decoder's cost is a function
//! of the difference `d` alone. Read alongside `benches/reconcile.rs`, whose
//! per-session index build is the range engine's counterpart to the load below.
//!
//! Run with `cargo bench -p p2p --bench riblt`.

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use p2p::reconcile::engine::riblt::{Decoder, Encoder};
use sha2::{Digest, Sha256};
use std::hint::black_box;

/// Set sizes, matching the range engine's bench so the two tables line up.
const SIZES: [usize; 4] = [100, 1_000, 10_000, 100_000];

/// Difference sizes the decoder is measured over.
const DIFFERENCES: [usize; 5] = [1, 10, 100, 1_000, 10_000];

/// A deterministic 36-byte identity, the width of a CIDv1 `dag-cbor/sha2-256`
/// item.
fn symbol(seed: usize) -> Vec<u8> {
    let digest = Sha256::digest((seed as u64).to_be_bytes());
    let mut bytes = digest.to_vec();
    bytes.extend_from_slice(&digest[..4]);
    bytes
}

fn symbols(range: std::ops::Range<usize>) -> Vec<Vec<u8>> {
    range.map(symbol).collect()
}

/// Loading the set into an encoder: the per-session enumeration the design
/// deliberately pays instead of a sketch maintained on the write path.
fn encoder_load(c: &mut Criterion) {
    let mut group = c.benchmark_group("riblt/encoder_load");
    for n in SIZES {
        let set = symbols(0..n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let mut encoder = Encoder::new(36);
                for item in &set {
                    encoder.add(item.clone());
                }
                black_box(encoder.produce_next())
            });
        });
    }
    group.finish();
}

/// The first coded symbol alone, which folds every item in the set — the `O(n)`
/// term of a session, isolated from the load.
fn first_coded_symbol(c: &mut Criterion) {
    let mut group = c.benchmark_group("riblt/first_coded_symbol");
    for n in SIZES {
        let set = symbols(0..n);
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let mut encoder = Encoder::new(36);
                    for item in &set {
                        encoder.add(item.clone());
                    }
                    encoder
                },
                |mut encoder| black_box(encoder.produce_next()),
                criterion::BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

/// Producing a run of cells past the first, which is where the stream's
/// steady-state `O(log m)` per cell shows.
fn steady_state_stream(c: &mut Criterion) {
    let mut group = c.benchmark_group("riblt/stream_64_cells");
    for n in SIZES {
        let set = symbols(0..n);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter_batched(
                || {
                    let mut encoder = Encoder::new(36);
                    for item in &set {
                        encoder.add(item.clone());
                    }
                    encoder.produce_next();
                    encoder
                },
                |mut encoder| {
                    for _ in 0..64 {
                        black_box(encoder.produce_next());
                    }
                },
                criterion::BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

/// Subtract and peel a difference of `d` out of a large shared set: the cost
/// that should track `d` and ignore `n` entirely.
fn decode_by_difference(c: &mut Criterion) {
    let mut group = c.benchmark_group("riblt/decode");
    let shared = symbols(0..10_000);
    for d in DIFFERENCES {
        let extra = symbols(10_000..10_000 + d);
        group.throughput(Throughput::Elements(d as u64));
        group.bench_with_input(BenchmarkId::from_parameter(d), &d, |b, _| {
            b.iter(|| {
                let mut encoder = Encoder::new(36);
                for item in shared.iter().chain(extra.iter()) {
                    encoder.add(item.clone());
                }
                let mut decoder = Decoder::new(36);
                for item in &shared {
                    decoder.add(item.clone());
                }
                while !decoder.is_decoded() {
                    decoder
                        .add_coded_symbol(encoder.produce_next())
                        .expect("one width");
                    decoder.try_decode();
                }
                black_box(decoder.remote().count())
            });
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    encoder_load,
    first_coded_symbol,
    steady_state_stream,
    decode_by_difference
);
criterion_main!(benches);
