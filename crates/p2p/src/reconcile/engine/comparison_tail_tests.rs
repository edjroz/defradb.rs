//! What the rateless engine costs on a bad draw, at node scale.
//!
//! R1 measured the coding tail on a bare decoder fed one symbol at a time. This
//! asks the same question of a whole session above the listing boundary, which
//! is what a node pays: the decoder pulls in doubling batches, so the coding
//! overshoot is quantised before it reaches the wire and the session figure is
//! coarser than the coding one.
//!
//! The range engine's draws are recorded beside them, because the contrast is
//! the claim: its bytes move with where the difference lands in the keyspace,
//! its round count does not move at all, and the round count is what a latent
//! link multiplies.
//!
//! ```text
//! cargo test --release -p p2p --features iroh-transport --lib \
//!   reconcile::engine::comparison_tail_tests -- --ignored --nocapture
//! ```

use super::comparison::{measure, row, Measurement, LISTING_BOUNDARY, ROW_HEADER};

/// Seeds a tail claim is drawn over. Thirty is what a nearest-rank p95 can
/// resolve and a p99 cannot, which is the point: the p99 column below is a
/// maximum wearing a percentile's name and says so.
const TAIL_SEEDS: usize = 30;

/// The seeds themselves, spread across the identity space rather than
/// consecutive, because a seed both scatters the difference and moves the whole
/// identity space.
fn tail_seed(index: usize) -> u64 {
    (index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0x5EED_C0FFEE
}

/// The value at a percentile of a sorted sample, by nearest rank.
fn percentile(sorted: &[usize], fraction: f64) -> usize {
    let rank = ((sorted.len() as f64 * fraction).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

/// Whether `samples` draws can tell the given percentile from the maximum.
fn resolves(samples: usize, fraction: f64) -> bool {
    ((samples as f64 * fraction).ceil() as usize) < samples
}

#[test]
fn a_percentile_past_what_the_sample_resolves_is_the_maximum() {
    let sorted: Vec<usize> = (1..=20).collect();
    assert_eq!(percentile(&sorted, 0.50), 10);
    assert_eq!(percentile(&sorted, 0.95), 19);
    assert_eq!(percentile(&sorted, 0.99), 20);
    assert!(resolves(20, 0.95));
    assert!(
        !resolves(20, 0.99),
        "a twentieth of a sample is its maximum"
    );
    assert!(resolves(TAIL_SEEDS, 0.95));
    assert!(!resolves(TAIL_SEEDS, 0.99));
}

/// The contrast chart 19 and the tail requirement both rest on: the range
/// engine's round count is a property of the *shape* of the difference, and the
/// rateless engine's is a draw.
///
/// Measured rather than assumed, because "RBSR is deterministic" is a claim
/// about this implementation's round schedule, not a theorem — and it is the
/// baseline every RIBLT tail figure is reported against.
#[test]
fn the_range_engine_spends_the_same_rounds_on_every_draw() {
    const N: usize = 2 * LISTING_BOUNDARY;
    const D: usize = 64;

    let mut ranges = Vec::new();
    let mut rateless = Vec::new();
    for index in 0..8 {
        let point = measure(N, D, tail_seed(index));
        ranges.push(point.rbsr.expect("the range engine converges").rounds);
        rateless.push(point.riblt.expect("the rateless engine converges").rounds);
    }

    assert_eq!(
        ranges
            .iter()
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        1,
        "the range engine spent {ranges:?} rounds across eight draws of the same (n, d)"
    );
    assert!(
        rateless.iter().all(|rounds| rounds > &ranges[0]),
        "at d={D} the rateless engine spent {rateless:?} rounds against the range \
         engine's {} — the inversion the latency study rests on is gone",
        ranges[0]
    );
}

/// **Every draw is printed, not only its percentiles.** A percentile computed
/// from thirty draws cannot be re-cut later, so a later reader who wants a
/// different cut needs the draws; and the p99 column here is the maximum of
/// thirty, which `p99Resolves` says on every row.
#[test]
#[ignore = "benchmark: thirty draws per point over sets of up to a hundred thousand items"]
fn the_tail_of_a_rateless_session() {
    println!("# raw draws");
    println!("{ROW_HEADER}");
    let points = [
        (10 * LISTING_BOUNDARY, 1usize),
        (10 * LISTING_BOUNDARY, 10),
        (10 * LISTING_BOUNDARY, 100),
        (100_000, 1),
        (100_000, 10),
        (100_000, 100),
        (100_000, 1_000),
    ];

    let mut summaries = Vec::new();
    for (n, d) in points {
        let mut draws = Vec::new();
        for index in 0..TAIL_SEEDS {
            let seed = tail_seed(index);
            let point = measure(n, d, seed);
            row(n, d, seed, &point);
            draws.push(point);
        }
        summaries.push((n, d, draws));
    }

    println!("# summary");
    println!(
        "n,d,samples,engine,quantity,p50,p95,p99,max,p50PerDiffItem,p99PerDiffItem,p99Resolves"
    );
    for (n, d, draws) in &summaries {
        for (engine, rateless) in [("riblt", true), ("ranges", false)] {
            for (quantity, of) in [
                (
                    "symbols",
                    (|m: &Measurement| m.symbols) as fn(&Measurement) -> usize,
                ),
                ("bytes", |m| m.bytes),
                ("rounds", |m| m.rounds),
            ] {
                let mut sample: Vec<usize> = draws
                    .iter()
                    .filter_map(|point| {
                        let result = if rateless { &point.riblt } else { &point.rbsr };
                        result.as_ref().ok().map(of)
                    })
                    .collect();
                if sample.is_empty() {
                    continue;
                }
                sample.sort_unstable();
                let per = |value: usize| value as f64 / *d as f64;
                println!(
                    "{n},{d},{},{engine},{quantity},{},{},{},{},{:.3},{:.3},{}",
                    sample.len(),
                    percentile(&sample, 0.50),
                    percentile(&sample, 0.95),
                    percentile(&sample, 0.99),
                    sample[sample.len() - 1],
                    per(percentile(&sample, 0.50)),
                    per(percentile(&sample, 0.99)),
                    resolves(sample.len(), 0.99),
                );
            }
        }
    }
}
