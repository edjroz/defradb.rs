//! Where the session round cap bites, and where the range regime begins.
//!
//! [`MAX_ROUNDS`](crate::reconcile::session::MAX_ROUNDS) is a cliff, not a
//! degradation: a session that needs one round more than the cap fails outright.
//! A measurement campaign that quietly stays inside the cap has not learned
//! where it is, so this maps it directly, over a grid of set sizes and
//! difference sizes, and prints the map.
//!
//! It runs at the engine, not over nodes: the cap counts peer messages, which
//! the engines decide between themselves, and the set sizes needed to approach
//! it are far beyond what a two-node harness can seed in reasonable time.
//!
//! Nothing here establishes a *minimum* failing difference. Whether a given
//! `(n, d)` converges depends on how the difference happens to land in the
//! keyspace, so the predicate is not monotone in `d` and a search over it finds
//! a crossing, not the first one. Several seeds are probed so the spread between
//! crossings is visible rather than assumed away.
//!
//! ```text
//! cargo test --release -p p2p --features iroh-transport --lib \
//!   reconcile::engine::rbsr::round_cap_tests -- --ignored --nocapture
//! ```

use super::caps::{BRANCHING_FACTOR, ID_LIST_THRESHOLD};
use super::simulate;
use crate::reconcile::session::MAX_ROUNDS;

const SEED: u64 = 0x5EED_C0FFEE;

/// Seeds the crossing search repeats over. One seed would report a single draw
/// as if it were the boundary.
const SEEDS: [u64; 3] = [0x5EED_C0FFEE, 0x1234_5678, 0xDEAD_BEEF];

/// Rounds and bytes for one `(n, d)` point, or the reason it failed.
fn probe_seeded(n: usize, d: usize, seed: u64) -> Result<(usize, usize), String> {
    let (local, remote) = simulate::diverged(n, d, seed);
    match simulate::run(&simulate::source(local), &simulate::source(remote)) {
        Ok(outcome) => Ok((outcome.rounds, outcome.bytes)),
        Err(error) => Err(error.to_string()),
    }
}

fn probe(n: usize, d: usize) -> Result<(usize, usize), String> {
    probe_seeded(n, d, SEED)
}

/// The grid, printed as CSV so a report can quote it without re-deriving it.
#[test]
#[ignore = "benchmark: reconciles sets of up to a million items"]
fn map_the_round_cap_boundary() {
    println!("n,d,rounds,bytes,outcome");
    let mut worst = 0;
    for n in [1_000, 10_000, 100_000, 1_000_000] {
        for divisor in [n, 100, 10, 2, 1] {
            let d = (n / divisor).max(1);
            match probe(n, d) {
                Ok((rounds, bytes)) => {
                    worst = worst.max(rounds);
                    println!("{n},{d},{rounds},{bytes},converged");
                }
                Err(error) => println!("{n},{d},,,{error}"),
            }
        }
    }
    println!("# deepest session observed: {worst} rounds against a cap of {MAX_ROUNDS}");
}

/// The cliff is real; this finds a difference size that falls off it.
///
/// It is not tree depth that gets there. Refinement is logarithmic with a
/// branching factor of 16, and the grid above shows a wholly disjoint hundred
/// thousand items converging in four rounds. The **hypothesis** is that what
/// reaches the cap is the message caps: a response that would exceed
/// [`MAX_RANGES_PER_MESSAGE`](super::caps::MAX_RANGES_PER_MESSAGE) or
/// [`MAX_LISTED_ID_BYTES`](super::caps::MAX_LISTED_ID_BYTES) defers the rest of
/// its refinement to a later round, and past some difference size there is more
/// to defer than 32 rounds can carry. That is consistent with what is printed
/// here and with how the caps are documented to behave, but nothing below
/// isolates the deferral itself, which would mean instrumenting the responder.
///
/// **What the search returns is a crossing, not a minimum.** Convergence is not
/// monotone in `d` — it depends on where the difference lands in the keyspace —
/// so a bisection is only entitled to say "this `d` failed and this smaller one
/// did not". Repeating it over several seeds shows how far apart two such
/// crossings can be, which is the only honest way to read the spread.
#[test]
#[ignore = "benchmark: reconciles up to a million items, repeatedly"]
fn a_round_cap_crossing_exists_above_a_quarter_million_items() {
    println!("n,seed,failingD,convergingD");
    for n in [100_000, 250_000, 500_000, 1_000_000] {
        for seed in SEEDS {
            if probe_seeded(n, n, seed).is_ok() {
                println!("{n},{seed:#x},,{n}");
                continue;
            }

            let (mut converging, mut failing) = (1, n);
            while failing - converging > 1 {
                let midpoint = converging + (failing - converging) / 2;
                if probe_seeded(n, midpoint, seed).is_ok() {
                    converging = midpoint;
                } else {
                    failing = midpoint;
                }
            }
            println!("{n},{seed:#x},{failing},{converging}");
        }
    }
}

/// Below `BRANCHING_FACTOR * ID_LIST_THRESHOLD` items, a responder that has to
/// refine at all lists the whole set rather than narrowing.
///
/// This is the shape of the phase 3 difference sweep: at n = 500 the first
/// refinement produces 16 ranges of about 31 items each, every one of them under
/// the 64-item listing threshold, so any range that mismatches is listed in
/// full. Once a scattered difference touches most ranges, the session costs what
/// a full-set exchange costs. It is not a defect and not a Rust-specific
/// behaviour — the caps are the Go reference's — but it does mean a sweep at
/// n = 500 measures the listing regime, not the `O(d log n)` one, and the report
/// must say which.
#[test]
#[ignore = "benchmark: reconciles ten thousand items"]
fn the_listing_regime_ends_where_the_caps_say_it_does() {
    let regime_limit = BRANCHING_FACTOR * ID_LIST_THRESHOLD;

    let (_, small_at_1) = probe(500, 1).expect("converges");
    let (_, small_at_50) = probe(500, 50).expect("converges");
    let (_, small_at_all) = probe(500, 500).expect("converges");
    let (_, large_at_50) = probe(10 * regime_limit, 50).expect("converges");
    let (_, large_at_all) = probe(10 * regime_limit, 10 * regime_limit).expect("converges");

    println!(
        "n=500: d=1 {small_at_1} B, d=50 {small_at_50} B, d=n {small_at_all} B\n\
         n={}: d=50 {large_at_50} B, d=n {large_at_all} B\n\
         # listing regime holds below n = BRANCHING_FACTOR * ID_LIST_THRESHOLD = {regime_limit}",
        10 * regime_limit
    );

    assert!(
        small_at_50 as f64 > 0.5 * small_at_all as f64,
        "below the regime limit a scattered difference should already cost most of a \
         full exchange: d=50 cost {small_at_50} B against {small_at_all} B for d=n"
    );
    assert!(
        (large_at_50 as f64) < 0.25 * large_at_all as f64,
        "above the regime limit the same difference should cost a small fraction of a \
         full exchange: d=50 cost {large_at_50} B against {large_at_all} B for d=n"
    );
}
