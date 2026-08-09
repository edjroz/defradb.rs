//! Where the rateless engine beats the range engine, and where it stops.
//!
//! The modelled charts claim a byte advantage that decays from 77x at small
//! differences to 1.1x and **never crosses parity**. Phase 3 could not test that
//! claim: its difference sweep sat at n = 500, below
//! `BRANCHING_FACTOR * ID_LIST_THRESHOLD = 1024`, where any mismatching range is
//! listed in full and the range engine is in its listing regime rather than its
//! `O(d log n)` one. A crossover claim made there would be about the wrong
//! regime.
//!
//! So this study runs above that boundary, on set sizes a two-node harness
//! cannot seed, with both engines driven by [`super::comparison`] so they share
//! an identity width and a codec.
//!
//! Both caps are cliffs rather than degradations and both are recorded as
//! outcomes: the range engine fails on
//! [`MAX_ROUNDS`](crate::reconcile::session::MAX_ROUNDS), and the rateless one
//! on [`MAX_CODED_SYMBOLS`](super::riblt::caps::MAX_CODED_SYMBOLS), which at the
//! measured overhead binds first — around `d ≈ 190,000`.
//!
//! ```text
//! cargo test --release -p p2p --features iroh-transport --lib \
//!   reconcile::engine::comparison_tests -- --ignored --nocapture
//! ```

use super::comparison::{diverged, id, rbsr, riblt, Measurement, IDENTITY_WIDTH};
use super::rbsr::caps::{BRANCHING_FACTOR, ID_LIST_THRESHOLD};
use crate::reconcile::source::ItemSource;

/// Where the range engine's listing regime ends and `O(d log n)` begins.
const LISTING_BOUNDARY: usize = BRANCHING_FACTOR * ID_LIST_THRESHOLD;

/// Seeds every decision point is repeated over. One seed reports a single draw
/// as if it were the boundary.
const SEEDS: [u64; 3] = [0x5EED_C0FFEE, 0x1234_5678, 0xDEAD_BEEF];

/// One point's result for both engines, or the reason an engine gave up.
struct Point {
    rbsr: Result<Measurement, String>,
    riblt: Result<Measurement, String>,
}

fn measure(n: usize, d: usize, seed: u64) -> Point {
    let (local, remote) = diverged(n, d, seed);
    Point {
        rbsr: rbsr(&local, &remote).map_err(|error| error.to_string()),
        riblt: riblt(&local, &remote).map_err(|error| error.to_string()),
    }
}

fn row(n: usize, d: usize, seed: u64, point: &Point) {
    for (engine, result) in [("ranges", &point.rbsr), ("riblt", &point.riblt)] {
        match result {
            Ok(m) => println!(
                "{n},{d},{seed:#x},{engine},{},{},{},{},{},converged",
                m.bytes, m.rounds, m.symbols, m.need, m.have
            ),
            Err(error) => println!("{n},{d},{seed:#x},{engine},,,,,,{error}"),
        }
    }
}

/// Both engines must be handed the same set, or the comparison measures the
/// fixtures. This is cheap and always on because it is the assumption every
/// number below rests on.
#[test]
fn both_engines_reconcile_the_same_items_at_the_same_width() {
    let (local, remote) = diverged(64, 8, SEEDS[0]);
    for source in [&local, &remote] {
        for index in 0..source.len() {
            assert_eq!(
                source.id(index).as_bytes().len(),
                IDENTITY_WIDTH,
                "every identity must be the CID width both engines are charged for"
            );
        }
    }

    let ranges = rbsr(&local, &remote).expect("converges");
    let rateless = riblt(&local, &remote).expect("converges");
    assert_eq!(
        (ranges.need, ranges.have),
        (rateless.need, rateless.have),
        "the two engines must find the same difference, or they are not comparable"
    );
    assert_eq!(
        ranges.need + ranges.have,
        8,
        "the fixture must produce the difference it was asked for"
    );
}

/// The identity generator is the fairness guarantee; pin its width and its
/// determinism directly.
#[test]
fn identities_are_deterministic_and_exactly_the_cid_width() {
    assert_eq!(id(7), id(7));
    assert_ne!(id(7), id(8));
    assert_eq!(id(u64::MAX).as_bytes().len(), IDENTITY_WIDTH);
}

/// The tail table's subject is the coded cells a decoder consumes, so the
/// driver's count of them is pinned against the one figure that is arithmetic
/// rather than a draw: agreement always costs exactly the opening batch.
#[test]
fn the_driver_counts_the_cells_the_decoder_consumed() {
    use super::riblt::caps::INITIAL_SYMBOL_BATCH;

    let (local, remote) = diverged(2 * LISTING_BOUNDARY, 0, SEEDS[0]);
    let agreed = riblt(&local, &remote).expect("converges");
    assert_eq!(
        agreed.symbols, INITIAL_SYMBOL_BATCH,
        "a decoder cannot know the sets agree without pulling and decoding a batch"
    );

    let (local, remote) = diverged(2 * LISTING_BOUNDARY, 100, SEEDS[0]);
    let differing = riblt(&local, &remote).expect("converges");
    assert!(
        differing.symbols > agreed.symbols,
        "a difference of a hundred cost {} cells, no more than agreement's {}",
        differing.symbols,
        agreed.symbols
    );

    let ranges = rbsr(&local, &remote).expect("converges");
    assert_eq!(
        ranges.symbols, 0,
        "the range engine codes nothing, and a zero here is that and not a miscount"
    );
}

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
        !rateless.is_empty(),
        "the rateless draws are the thing the tail table reports"
    );
}

/// What the rateless engine costs on a bad draw, at node scale.
///
/// R1 measured the coding tail on a bare decoder fed one symbol at a time. This
/// is the same question asked of a whole session at set sizes above the listing
/// boundary, which is what a node pays: the batches overshoot, so the session
/// figure is coarser than the coding figure and is the one a capacity decision
/// reads.
///
/// **Every draw is printed, not only its percentiles.** A percentile computed
/// from thirty draws cannot be re-cut later, and the p99 column here is the
/// maximum of thirty — `p99Resolves` is `false` on every row and says so.
///
/// The range engine's draws are printed beside them for the contrast: its cost
/// varies with where the difference falls in the keyspace, but its round count
/// does not vary at all, which is the property the latency study turns on.
#[test]
#[ignore = "benchmark: thirty draws per point over sets of up to a hundred thousand items"]
fn the_tail_of_a_rateless_session() {
    println!("# raw draws");
    println!("n,d,seed,engine,bytes,rounds,symbols,need,have,outcome");
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
        for (engine, take) in [("riblt", true), ("ranges", false)] {
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
                        let result = if take { &point.riblt } else { &point.rbsr };
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

/// Bytes against set size, at the difference sizes a live node actually sees.
///
/// `d = 0` is in the sweep and is not a formality: it is the regime a node that
/// reconciles on a timer is in almost every time, the range engine answers it
/// with one fingerprint and one skip, and the rateless decoder cannot know the
/// sets agree until it has pulled and decoded a batch.
#[test]
#[ignore = "benchmark: reconciles sets of up to a hundred thousand items"]
fn bytes_against_set_size_above_the_listing_boundary() {
    println!("# listing boundary at n = {LISTING_BOUNDARY}");
    println!("n,d,seed,engine,bytes,rounds,symbols,need,have,outcome");
    for n in [
        LISTING_BOUNDARY,
        2 * LISTING_BOUNDARY,
        10 * LISTING_BOUNDARY,
        25_000,
        50_000,
        100_000,
    ] {
        for d in [0, 1, 8, 100] {
            for seed in SEEDS {
                row(n, d, seed, &measure(n, d, seed));
            }
        }
    }
}

/// Bytes against difference size, at two set sizes above the boundary.
///
/// This is the sweep phase 3's n = 500 could not be: the range engine is in its
/// `O(d log n)` regime here, so the decay of the rateless advantage is the
/// modelled decay rather than an artifact of full-set listing.
#[test]
#[ignore = "benchmark: reconciles a hundred thousand items, repeatedly"]
fn bytes_against_difference_size_above_the_listing_boundary() {
    println!("n,d,seed,engine,bytes,rounds,symbols,need,have,outcome");
    for n in [10 * LISTING_BOUNDARY, 100_000] {
        let mut d = 0;
        loop {
            for seed in SEEDS {
                row(n, d, seed, &measure(n, d, seed));
            }
            if d >= n {
                break;
            }
            d = if d == 0 { 1 } else { (d * 2).min(n) };
        }
    }
}

/// The difference size at which the rateless engine stops being cheaper.
///
/// **A crossing, not a minimum.** Neither engine's cost is monotone in `d` —
/// the range engine's depends on where the difference lands in the keyspace and
/// the rateless engine's is a draw from a distribution — so a bisection is only
/// entitled to say "this `d` lost and this smaller one won". Three seeds show
/// how far apart two such crossings sit; a single one would read as a boundary.
///
/// The low end is reported too, because there is a second crossing there and it
/// runs the other way: at `d = 0` the rateless engine loses outright, since it
/// pays a whole opening batch to learn there was nothing to find.
#[test]
#[ignore = "benchmark: bisects over sets of up to a hundred thousand items"]
fn where_the_rateless_advantage_crosses_parity() {
    println!("n,seed,dZero,winnerAtZero,crossingD,lastWinningD,ratioAtOne");
    for n in [LISTING_BOUNDARY, 10 * LISTING_BOUNDARY, 25_000, 100_000] {
        for seed in SEEDS {
            let zero = measure(n, 0, seed);
            let (zero_ranges, zero_rateless) = (bytes(&zero.rbsr), bytes(&zero.riblt));
            let winner_at_zero = match (zero_ranges, zero_rateless) {
                (Some(r), Some(t)) if t < r => "riblt",
                (Some(_), Some(_)) => "ranges",
                _ => "?",
            };

            let one = measure(n, 1, seed);
            let ratio_at_one = match (bytes(&one.rbsr), bytes(&one.riblt)) {
                (Some(r), Some(t)) if t > 0 => format!("{:.2}", r as f64 / t as f64),
                _ => String::new(),
            };

            let (crossing, last_winning) = crossing_above(n, seed);
            let zero_pair = format!(
                "{}/{}",
                zero_ranges.map(|b| b.to_string()).unwrap_or_default(),
                zero_rateless.map(|b| b.to_string()).unwrap_or_default()
            );
            println!(
                "{n},{seed:#x},{zero_pair},{winner_at_zero},{},{},{ratio_at_one}",
                crossing.map(|d| d.to_string()).unwrap_or_default(),
                last_winning.map(|d| d.to_string()).unwrap_or_default(),
            );
        }
    }
}

fn bytes(result: &Result<Measurement, String>) -> Option<usize> {
    result.as_ref().ok().map(|m| m.bytes)
}

/// Whether the rateless engine is cheaper at `(n, d)`. A point where either
/// engine gives up counts as not cheaper, so a cap hit shows up as a crossing
/// rather than being searched past.
fn rateless_wins(n: usize, d: usize, seed: u64) -> bool {
    let point = measure(n, d, seed);
    match (bytes(&point.rbsr), bytes(&point.riblt)) {
        (Some(ranges), Some(rateless)) => rateless < ranges,
        _ => false,
    }
}

/// Bisects between a `d` the rateless engine wins at and one it loses at.
fn crossing_above(n: usize, seed: u64) -> (Option<usize>, Option<usize>) {
    if !rateless_wins(n, 1, seed) {
        return (Some(1), None);
    }
    if rateless_wins(n, n, seed) {
        return (None, Some(n));
    }

    let (mut winning, mut losing) = (1usize, n);
    while losing - winning > 1 {
        let midpoint = winning + (losing - winning) / 2;
        if rateless_wins(n, midpoint, seed) {
            winning = midpoint;
        } else {
            losing = midpoint;
        }
    }
    (Some(losing), Some(winning))
}
