//! Example-based protocol tests: convergence on hand-picked set pairs, the
//! observable split and list heuristics, the byte-scaling claim, and the caps
//! that keep a misbehaving peer bounded.

use super::caps::{BRANCHING_FACTOR, ID_LIST_THRESHOLD, MAX_IDS_PER_RANGE};
use super::engine::{RbsrEngine, Role};
use super::message::{Mode, Range, RbsrMessage};
use super::segment_tree::SegmentTree;
use super::simulate::{difference_sizes, diverged, id, ids, run, source};
use super::{responder, Fingerprint};
use crate::reconcile::engine::{Engine, Progress};
use crate::reconcile::error::ReconcileError;
use crate::reconcile::session::{Session, MAX_ROUNDS};
use crate::reconcile::source::{Bound, ItemSource, MemorySource};

fn full_range_mismatch() -> RbsrMessage {
    RbsrMessage::new(vec![Range::fingerprint(
        Bound::Max,
        Fingerprint::of(std::iter::once(&id(u64::MAX))),
    )])
}

fn respond_to_mismatch(local: &MemorySource) -> RbsrMessage {
    let index = SegmentTree::build(local);
    responder::respond(&index, &full_range_mismatch()).expect("responds")
}

#[test]
fn empty_sets_converge_with_nothing_to_do() {
    let outcome = run(&source([]), &source([])).expect("converges");
    assert!(outcome.need.is_empty());
    assert!(outcome.have.is_empty());
}

#[test]
fn an_empty_initiator_needs_everything() {
    let outcome = run(&source([]), &source(1..=10)).expect("converges");
    assert_eq!(outcome.need, ids(1..=10));
    assert!(outcome.have.is_empty());
}

#[test]
fn an_empty_responder_means_the_initiator_has_everything() {
    let outcome = run(&source(1..=10), &source([])).expect("converges");
    assert!(outcome.need.is_empty());
    assert_eq!(outcome.have, ids(1..=10));
}

#[test]
fn disjoint_sets_converge_to_a_full_two_way_difference() {
    let outcome = run(&source(1..=10), &source(11..=20)).expect("converges");
    assert_eq!(outcome.need, ids(11..=20));
    assert_eq!(outcome.have, ids(1..=10));
}

#[test]
fn a_single_element_difference_is_found_in_a_large_set() {
    let outcome = run(&source(1..=1000), &source(1..=1001)).expect("converges");
    assert_eq!(outcome.need, ids([1001]));
    assert!(outcome.have.is_empty());
}

#[test]
fn identical_sets_converge_in_constant_bytes_regardless_of_size() {
    let small = run(&source(1..=50), &source(1..=50)).expect("converges");
    let large = run(&source(1..=5000), &source(1..=5000)).expect("converges");
    assert_eq!(
        small.bytes, large.bytes,
        "an agreed set costs the same however big it is"
    );
    assert_eq!(small.rounds, large.rounds);
}

#[test]
fn bytes_scale_with_the_symmetric_difference_not_the_union() {
    const N: u64 = 1000;

    let agreed = run(&source(1..=N), &source(1..=N)).expect("converges");

    let local: Vec<u64> = (1..=N).chain([N + 1]).collect();
    let remote: Vec<u64> = (1..=N).chain([N + 2]).collect();
    let tiny = run(&source(local), &source(remote)).expect("converges");
    assert_eq!(tiny.need, ids([N + 2]));
    assert_eq!(tiny.have, ids([N + 1]));

    let disjoint = run(&source(1..=N), &source(N + 1..=2 * N)).expect("converges");

    assert!(
        agreed.bytes < tiny.bytes,
        "no difference is cheaper than a two-item difference"
    );
    assert!(
        tiny.bytes < disjoint.bytes / 2,
        "a two-item difference is far cheaper than transferring the union: \
         {} vs {}",
        tiny.bytes,
        disjoint.bytes
    );
}

#[test]
fn rounds_stay_logarithmic_in_the_set_size() {
    for n in [100u64, 1_000, 10_000] {
        let outcome = run(&source(1..=n), &source(2..=n)).expect("converges");
        assert!(
            outcome.rounds <= MAX_ROUNDS,
            "n={n} took {} rounds",
            outcome.rounds
        );
    }
}

#[test]
fn a_mismatching_range_at_the_threshold_is_listed_rather_than_split() {
    let listed = respond_to_mismatch(&source(1..=ID_LIST_THRESHOLD as u64));
    assert_eq!(listed.ranges().len(), 1);
    match &listed.ranges()[0].mode {
        Mode::IdList(entries) => assert_eq!(entries.len(), ID_LIST_THRESHOLD),
        other => panic!("expected an ID list, got {other:?}"),
    }
}

#[test]
fn a_mismatching_range_above_the_threshold_splits_by_the_branching_factor() {
    let split = respond_to_mismatch(&source(1..=ID_LIST_THRESHOLD as u64 + 1));
    assert_eq!(split.ranges().len(), BRANCHING_FACTOR);
    assert!(split
        .ranges()
        .iter()
        .all(|range| matches!(range.mode, Mode::Fingerprint(_))));
    assert_eq!(
        split.ranges()[BRANCHING_FACTOR - 1].upper_bound,
        Bound::Max,
        "the last bucket inherits the original upper bound"
    );
}

#[test]
fn split_buckets_are_evenly_sized() {
    let local = source(1..=1600);
    let split = respond_to_mismatch(&local);
    let index = SegmentTree::build(&local);

    let mut lo = Bound::Min;
    for range in split.ranges() {
        let (start, end) = index.window(&lo, &range.upper_bound).expect("valid window");
        assert_eq!(end - start, 100, "1600 items across 16 buckets");
        lo = range.upper_bound.clone();
    }
}

#[test]
fn an_oversized_id_list_from_a_peer_is_rejected() {
    let oversized = RbsrMessage::new(vec![Range::id_list(
        Bound::Max,
        (0..=MAX_IDS_PER_RANGE as u64).map(id).collect(),
    )]);

    let local = source(1..=10);
    let index = SegmentTree::build(&local);
    assert_eq!(
        responder::respond(&index, &oversized),
        Err(ReconcileError::IdListTooLarge {
            size: MAX_IDS_PER_RANGE + 1,
            max: MAX_IDS_PER_RANGE,
        })
    );

    let mut initiator = RbsrEngine::initiator(&local);
    let _ = initiator.next_outbound().expect("opens");
    assert_eq!(
        initiator.ingest(oversized),
        Err(ReconcileError::IdListTooLarge {
            size: MAX_IDS_PER_RANGE + 1,
            max: MAX_IDS_PER_RANGE,
        })
    );
}

#[test]
fn descending_range_bounds_are_rejected() {
    let local = source(1..=100);
    let index = SegmentTree::build(&local);
    let high = index.source().key(90).clone();
    let low = index.source().key(10).clone();

    let inverted = RbsrMessage::new(vec![
        Range::skip(Bound::Key(high)),
        Range::skip(Bound::Key(low)),
        Range::skip(Bound::Max),
    ]);

    assert_eq!(
        responder::respond(&index, &inverted),
        Err(ReconcileError::MalformedBound)
    );
}

#[test]
fn a_peer_whose_fingerprints_never_agree_hits_the_round_cap() {
    let local = source(1..=1000);
    let mut session = Session::new(RbsrEngine::initiator(&local));
    let _ = session.next_outbound().expect("opens");

    let mut last = Ok(Progress::Continue);
    for round in 0..MAX_ROUNDS + 1 {
        let garbage = RbsrMessage::new(vec![Range::fingerprint(
            Bound::Max,
            Fingerprint::of(std::iter::once(&id(u64::MAX - round as u64))),
        )]);
        last = session.ingest(garbage);
        if last.is_err() {
            break;
        }
    }

    assert_eq!(
        last,
        Err(ReconcileError::RoundCapExceeded { max: MAX_ROUNDS }),
        "a peer that never agrees must terminate the session, not hang it"
    );
    assert_eq!(session.rounds(), MAX_ROUNDS);
}

/// Prints what a session actually costs across set sizes and difference sizes.
/// This is the evidence behind keeping the Go engine's branching factor and
/// ID-list threshold: it shows where the bytes go before anyone tunes them.
/// Measurement, not assertion:
/// `cargo test -p p2p --lib session_cost_table -- --ignored --nocapture`.
#[test]
#[ignore = "prints a measurement table rather than asserting"]
fn session_cost_table() {
    println!("n,d,rounds,bytes,bytes_per_diff_item");
    for n in [1_000usize, 10_000, 100_000] {
        for d in difference_sizes(n) {
            let (local, remote) = diverged(n, d, 0x5eed_c057);
            let outcome = run(&source(local), &source(remote)).expect("converges");
            let per_item = if d == 0 {
                0.0
            } else {
                outcome.bytes as f64 / d as f64
            };
            println!("{n},{d},{},{},{per_item:.1}", outcome.rounds, outcome.bytes);
        }
    }
}

#[test]
fn the_responder_learns_nothing() {
    let remote = source(1..=100);
    let mut engine = RbsrEngine::responder(&remote);
    assert_eq!(engine.role(), Role::Responder);
    assert!(engine.next_outbound().expect("no error").is_none());

    engine.ingest(full_range_mismatch()).expect("responds");
    assert!(engine.next_outbound().expect("no error").is_some());
    assert!(engine.diff().is_empty(), "the responder stays stateless");
}
