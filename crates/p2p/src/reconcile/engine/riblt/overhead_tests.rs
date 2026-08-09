//! How many coded symbols a difference of size `d` actually costs.
//!
//! The number is a distribution, not a constant: peeling is probabilistic, so a
//! given difference sometimes decodes off a shorter prefix and sometimes needs a
//! longer one. The recovered design work models it as `≈1.35 · d`, and every
//! bandwidth claim built on that model inherits whatever the real distribution
//! is — including its tail, which is what a mobile sync decision reads.
//!
//! Two quantities are measured, because they are different questions:
//!
//! - **Code overhead**, symbols consumed when they arrive one at a time. This
//!   is the coding constant, directly comparable to the model's 1.35.
//! - **Session overshoot**, symbols the protocol actually pulls, which is larger
//!   because the decoder asks in doubling batches and the last batch is mostly
//!   wasted. This is what crosses the wire — including at `d = 0`, where the
//!   decoder still pays a whole batch to learn there was nothing to find, and
//!   where the range engine wins outright.
//!
//! `cargo test --release -p p2p --lib riblt::overhead -- --ignored --nocapture`.

use super::caps;
use super::decoder::Decoder;
use super::encoder::Encoder;
use super::simulate::{diverged, id, run, source};

/// Differences the study sweeps, spanning the regimes the model's asymptotic
/// constant is least trustworthy in (very small `d`) and most (large `d`).
const DIFFERENCES: [usize; 8] = [1, 2, 5, 10, 50, 100, 1_000, 10_000];

/// Seeds per difference size, traded against how long a run takes.
fn seeds_for(d: usize) -> usize {
    match d {
        0..=100 => 1_000,
        101..=1_000 => 200,
        _ => 20,
    }
}

/// Symbols consumed to decode a difference of `d` over `common` shared items,
/// feeding the decoder one symbol at a time.
///
/// The seed moves the whole identity space as well as which items diverge. It
/// has to: the coded stream is a function of the *set*, not of the order it was
/// built in, so a seed that only reshuffled would draw the same sample every
/// time — which is exactly what a first version of this study did, and it read
/// as a distribution with no variance at all.
fn symbols_to_decode(common: usize, d: usize, seed: u64) -> usize {
    let (local, remote) = diverged(common, d, seed);
    let offset = seed.wrapping_mul(0x0010_0000_0000_0001);
    let mut encoder = Encoder::new(36);
    let mut decoder = Decoder::new(36);
    for value in remote {
        encoder.add(id(value.wrapping_add(offset)).as_bytes().to_vec());
    }
    for value in local {
        decoder.add(id(value.wrapping_add(offset)).as_bytes().to_vec());
    }

    let mut consumed = 0;
    while !decoder.is_decoded() {
        decoder
            .add_coded_symbol(encoder.produce_next())
            .expect("one width");
        consumed += 1;
        decoder.try_decode();
        assert!(consumed < 1_000_000, "d={d} seed={seed} did not decode");
    }
    consumed
}

/// The value at a percentile of a sorted sample, by nearest rank.
///
/// Nearest rank cannot resolve a percentile finer than `1/samples`: at twenty
/// draws the "99th percentile" is the largest of the twenty and nothing more.
/// [`resolves`] says whether a given row's tail figure is a percentile or a
/// maximum wearing one's name, and every table that prints one prints that too.
fn percentile(sorted: &[usize], fraction: f64) -> usize {
    let rank = ((sorted.len() as f64 * fraction).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

/// Whether `samples` draws can distinguish the given percentile from the
/// maximum at all.
fn resolves(samples: usize, fraction: f64) -> bool {
    ((samples as f64 * fraction).ceil() as usize) < samples
}

/// The sorted symbol counts over `samples` seeded runs.
fn distribution(common: usize, d: usize, samples: usize) -> Vec<usize> {
    let mut counts: Vec<usize> = (0..samples as u64)
        .map(|seed| symbols_to_decode(common, d, 0x0e40_0000u64.wrapping_add(seed)))
        .collect();
    counts.sort_unstable();
    counts
}

#[test]
#[ignore = "prints a measurement table rather than asserting"]
fn symbols_to_decode_distribution() {
    println!("common,d,samples,p50,p95,p99,max,p50_ratio,p95_ratio,p99_ratio,p99_resolves");
    for common in [0usize, 10_000] {
        for d in DIFFERENCES {
            let samples = seeds_for(d);
            let counts = distribution(common, d, samples);
            retain_samples(common, d, &counts);
            let ratio = |value: usize| value as f64 / d as f64;
            println!(
                "{common},{d},{samples},{},{},{},{},{:.3},{:.3},{:.3},{}",
                percentile(&counts, 0.50),
                percentile(&counts, 0.95),
                percentile(&counts, 0.99),
                counts[counts.len() - 1],
                ratio(percentile(&counts, 0.50)),
                ratio(percentile(&counts, 0.95)),
                ratio(percentile(&counts, 0.99)),
                resolves(samples, 0.99),
            );
        }
    }
}

/// Writes every draw, not just its percentiles, when `DEFRA_RIBLT_SAMPLES_OUT`
/// names a directory.
///
/// A later comparison campaign has to be able to re-derive a tail claim, or
/// re-cut it at a percentile this table did not print. Percentiles alone throw
/// that away, and re-running is not the same thing: the runs are seeded, but the
/// seeds only reproduce given the same sweep.
fn retain_samples(common: usize, d: usize, counts: &[usize]) {
    let Ok(directory) = std::env::var("DEFRA_RIBLT_SAMPLES_OUT") else {
        return;
    };
    let path = std::path::Path::new(&directory).join(format!("samples-{common}-{d}.csv"));
    let body: String = counts
        .iter()
        .map(|count| format!("{count}\n"))
        .collect::<Vec<_>>()
        .concat();
    std::fs::create_dir_all(&directory).expect("sample directory");
    std::fs::write(path, body).expect("write samples");
}

#[test]
#[ignore = "prints a measurement table rather than asserting"]
fn session_overshoot_table() {
    println!("n,d,rounds,symbols,bytes,symbols_per_diff_item,bytes_per_diff_item");
    for n in [1_000usize, 10_000, 100_000] {
        for d in [0usize, 1, 8, 100, 1_000] {
            let (local, remote) = diverged(n, d, 0x5eed_c057);
            let outcome = run(&source(local), &source(remote)).expect("converges");
            let per_item = |value: f64| if d == 0 { f64::NAN } else { value / d as f64 };
            println!(
                "{n},{d},{},{},{},{:.2},{:.1}",
                outcome.rounds,
                outcome.symbols,
                outcome.bytes,
                per_item(outcome.symbols as f64),
                per_item(outcome.bytes as f64),
            );
        }
    }
}

/// What agreement costs, which is the regime a live node is in almost always.
///
/// This is the one number where the rateless engine *loses* to the range engine
/// outright, and it loses by a lot: the decoder cannot know the sets agree until
/// it has asked for and decoded a batch, so it always pays the opening batch of
/// [`caps::INITIAL_SYMBOL_BATCH`] cells. The range engine answers the same
/// question with one fingerprint and one all-skip, which phase 1 measured at a
/// flat 143 bytes at every set size.
///
/// Pinned rather than merely printed because it is the number most likely to be
/// quietly improved away by a change to the batch schedule, and a comparison
/// campaign needs to know when it moves.
#[test]
fn agreement_costs_the_opening_batch_at_every_set_size() {
    const RBSR_AGREEMENT_BYTES: usize = 143;

    for n in [1_000usize, 10_000, 100_000] {
        let (local, remote) = diverged(n, 0, 0x5eed_c057);
        let outcome = run(&source(local), &source(remote)).expect("converges");

        assert_eq!(outcome.rounds, 1, "n={n}");
        assert_eq!(
            outcome.symbols,
            caps::INITIAL_SYMBOL_BATCH,
            "n={n}: a decoder cannot know the sets agree without decoding a batch"
        );
        assert!(
            (600..700).contains(&outcome.bytes),
            "n={n}: agreement cost {} bytes, against the range engine's {RBSR_AGREEMENT_BYTES}",
            outcome.bytes
        );
    }
}

/// A guard rather than a measurement: the printed tables carry the verdict on
/// the model constant, but a mapping or peel that regressed would blow past any
/// plausible constant, and that must fail rather than quietly change a table.
///
/// The bands are per difference size because the model's constant is
/// asymptotic. A difference of ten decodes off sixteen symbols at the median
/// and past thirty in its tail — three times the model — and that is the code
/// behaving correctly, not a regression. Only the large-`d` rows are tight
/// enough to be a useful alarm.
#[test]
fn the_coding_overhead_stays_within_a_plausible_band() {
    for (d, samples, band) in [(100usize, 100usize, 2.0f64), (1_000, 20, 1.8)] {
        let counts = distribution(0, d, samples);
        let worst = counts[counts.len() - 1] as f64 / d as f64;
        assert!(
            worst < band,
            "d={d}: the worst of {samples} draws overshot by {worst:.2}, past the \
             {band} band any rateless coding constant should hold"
        );
    }
}

/// The whole point of a sketch: the coding overhead is a function of the
/// difference and not of how much the two peers already agree on.
#[test]
fn the_overhead_does_not_move_with_the_shared_set() {
    const SAMPLES: usize = 40;
    let alone = percentile(&distribution(0, 50, SAMPLES), 0.50);
    let buried = percentile(&distribution(2_000, 50, SAMPLES), 0.50);
    assert!(
        alone.abs_diff(buried) * 4 <= alone,
        "a difference of fifty cost {alone} symbols alone and {buried} buried \
         in two thousand shared items"
    );
}
