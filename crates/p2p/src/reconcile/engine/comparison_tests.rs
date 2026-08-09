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

use super::comparison::{
    diverged, id, measure, rbsr, riblt, row, Measurement, IDENTITY_WIDTH, LISTING_BOUNDARY,
    ROW_HEADER,
};
use crate::reconcile::source::ItemSource;

/// Seeds every decision point is repeated over. One seed reports a single draw
/// as if it were the boundary.
const SEEDS: [u64; 3] = [0x5EED_C0FFEE, 0x1234_5678, 0xDEAD_BEEF];

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
    println!("{ROW_HEADER}");
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
    println!("{ROW_HEADER}");
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
