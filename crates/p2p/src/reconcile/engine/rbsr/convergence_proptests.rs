//! Convergence is the invariant, so it is property-tested rather than
//! exemplified: for arbitrary set pairs, a session must terminate with the
//! initiator's need and have sets *exactly* equal to the true symmetric
//! difference — no misses, no spurious entries.
//!
//! Two generators cover different risks. Arbitrary small sets shrink a failure
//! to a minimal counterexample. Seeded structured pairs reach the sizes and
//! difference shapes that matter (`d` at 0, 1, small, about half, and all of
//! `n`) at set sizes a `proptest` generator could not produce affordably; a
//! failure there shrinks to a single reproducible seed.

use std::collections::BTreeSet;

use proptest::prelude::*;

use super::simulate::{ids, run, source};

/// Asserts a session reproduces the naive set difference exactly.
fn check(local_seeds: &[u64], remote_seeds: &[u64]) -> Result<(), TestCaseError> {
    let local = source(local_seeds.iter().copied());
    let remote = source(remote_seeds.iter().copied());

    let outcome = run(&local, &remote).map_err(|error| TestCaseError::fail(error.to_string()))?;

    let local_set: BTreeSet<u64> = local_seeds.iter().copied().collect();
    let remote_set: BTreeSet<u64> = remote_seeds.iter().copied().collect();

    prop_assert_eq!(
        outcome.need,
        ids(remote_set.difference(&local_set).copied()),
        "need must equal remote \\ local"
    );
    prop_assert_eq!(
        outcome.have,
        ids(local_set.difference(&remote_set).copied()),
        "have must equal local \\ remote"
    );
    Ok(())
}

/// Builds a set pair of about `n` items whose symmetric difference is exactly
/// `d`, with the divergent items scattered through the keyspace rather than
/// clustered — the harder case for a range protocol.
fn diverged(n: usize, d: usize, seed: u64) -> (Vec<u64>, Vec<u64>) {
    let mut universe: Vec<u64> = (0..(n + d) as u64).collect();
    shuffle(&mut universe, seed);

    let (divergent, shared) = universe.split_at(d);
    let local_only = d.div_ceil(2);

    let mut local = shared.to_vec();
    local.extend_from_slice(&divergent[..local_only]);
    let mut remote = shared.to_vec();
    remote.extend_from_slice(&divergent[local_only..]);

    (local, remote)
}

fn shuffle(values: &mut [u64], seed: u64) {
    let mut state = seed | 1;
    for index in (1..values.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        values.swap(index, (state % (index as u64 + 1)) as usize);
    }
}

/// The difference sizes the acceptance criteria call for at a given set size.
fn difference_sizes(n: usize) -> [usize; 5] {
    [0, 1, 8, n / 2, n]
}

fn check_tier(n: usize, seed: u64) {
    for d in difference_sizes(n) {
        let (local, remote) = diverged(n, d, seed ^ d as u64);
        check(&local, &remote).unwrap_or_else(|error| panic!("n={n} d={d}: {error}"));
    }
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 96, ..ProptestConfig::default() })]

    /// Arbitrary overlapping sets, including empty and identical ones, with real
    /// shrinking to a minimal counterexample.
    #[test]
    fn arbitrary_set_pairs_converge_to_the_symmetric_difference(
        local in prop::collection::hash_set(0u64..300, 0..120),
        remote in prop::collection::hash_set(0u64..300, 0..120),
    ) {
        let local: Vec<u64> = local.into_iter().collect();
        let remote: Vec<u64> = remote.into_iter().collect();
        check(&local, &remote)?;
    }

    /// Sets far larger than the ID-list threshold, so every session exercises
    /// the split path many times over.
    #[test]
    fn seeded_structured_differences_converge(
        seed in any::<u64>(),
        class in 0usize..5,
    ) {
        let n = 1_000;
        let d = difference_sizes(n)[class];
        let (local, remote) = diverged(n, d, seed);
        check(&local, &remote)?;
    }
}

#[test]
fn identical_sets_yield_an_empty_difference() {
    let seeds: Vec<u64> = (0..500).collect();
    check(&seeds, &seeds).expect("identical sets converge to nothing");
}

#[test]
fn two_empty_sets_yield_an_empty_difference() {
    check(&[], &[]).expect("empty sets converge to nothing");
}

#[test]
fn an_empty_set_against_a_full_one_yields_the_whole_set() {
    let seeds: Vec<u64> = (0..500).collect();
    check(&[], &seeds).expect("the initiator needs everything");
    check(&seeds, &[]).expect("the initiator has everything");
}

#[test]
fn tier_1k() {
    check_tier(1_000, 0x5eed_1000);
}

#[test]
fn tier_10k() {
    check_tier(10_000, 0x5eed_a000);
}

/// The 100k tier is opt-in so the default suite stays fast:
/// `cargo test -p p2p --lib reconcile -- --ignored`.
#[test]
#[ignore = "n=100k costs seconds rather than milliseconds; run explicitly"]
fn tier_100k() {
    check_tier(100_000, 0x5eed_f000);
}
