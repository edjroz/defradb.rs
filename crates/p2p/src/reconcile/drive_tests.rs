use std::collections::BTreeSet;

use super::*;
use crate::reconcile::engine::rbsr::{Range, RbsrEngine, RbsrMessage};
use crate::reconcile::source::{Bound, Item, ItemId, MemorySource};
use crate::reconcile::stream::MemoryStream;

fn id(seed: u64) -> ItemId {
    ItemId::new(seed.to_be_bytes().to_vec())
}

fn source(seeds: impl IntoIterator<Item = u64>) -> MemorySource {
    MemorySource::new(seeds.into_iter().map(|seed| Item::new(seed % 5, id(seed)))).unwrap()
}

fn ids(seeds: impl IntoIterator<Item = u64>) -> BTreeSet<ItemId> {
    seeds.into_iter().map(id).collect()
}

/// Runs both roles over one in-memory pipe, as the transport does.
async fn reconcile(local: Vec<u64>, remote: Vec<u64>) -> (Diff, SessionCost) {
    let (mut initiator_side, mut responder_side) = MemoryStream::pair();

    let responder = tokio::spawn(async move {
        let session = Session::new(RbsrEngine::responder(source(remote)));
        drive_responder(session, &mut responder_side).await
    });

    let session = Session::new(RbsrEngine::initiator(source(local)));
    let outcome = drive_initiator(session, &mut initiator_side)
        .await
        .expect("session must converge");
    responder
        .await
        .unwrap()
        .expect("responder must end cleanly");
    outcome
}

/// Most tests only care about what was discovered, not what it cost.
async fn reconcile_diff(local: Vec<u64>, remote: Vec<u64>) -> Diff {
    reconcile(local, remote).await.0
}

#[tokio::test]
async fn identical_sets_converge_with_an_empty_diff() {
    let diff = reconcile_diff((0..200).collect(), (0..200).collect()).await;
    assert!(diff.is_empty(), "no divergence must yield no diff");
}

#[tokio::test]
async fn empty_sets_converge() {
    let diff = reconcile_diff(Vec::new(), Vec::new()).await;
    assert!(diff.is_empty());
}

#[tokio::test]
async fn the_discovered_diff_is_exactly_the_true_diff() {
    let local: Vec<u64> = (0..500).filter(|seed| seed % 7 != 0).collect();
    let remote: Vec<u64> = (0..500).filter(|seed| seed % 11 != 0).collect();

    let diff = reconcile_diff(local.clone(), remote.clone()).await;

    let local_set = ids(local);
    let remote_set = ids(remote);
    assert_eq!(
        diff.need().iter().cloned().collect::<BTreeSet<_>>(),
        remote_set.difference(&local_set).cloned().collect(),
        "need must be exactly what the peer holds and we lack"
    );
    assert_eq!(
        diff.have().iter().cloned().collect::<BTreeSet<_>>(),
        local_set.difference(&remote_set).cloned().collect(),
        "have must be exactly what we hold and the peer lacks"
    );
}

#[tokio::test]
async fn a_one_sided_set_is_discovered_whole() {
    let diff = reconcile_diff(Vec::new(), (0..100).collect()).await;
    assert_eq!(diff.need().len(), 100);
    assert!(diff.have().is_empty());
}

#[tokio::test]
async fn an_initiator_whose_peer_vanishes_errors_rather_than_hanging() {
    let (mut initiator_side, responder_side) = MemoryStream::pair();
    drop(responder_side);

    let session = Session::new(RbsrEngine::initiator(source(0..100)));
    let error = drive_initiator(session, &mut initiator_side)
        .await
        .expect_err("a vanished peer must fail the session");
    assert!(matches!(error, ReconcileError::Transport(_)), "{error:?}");
}

#[tokio::test]
async fn an_initiator_rejects_an_undecodable_frame() {
    let (mut initiator_side, mut responder_side) = MemoryStream::pair();

    let peer = tokio::spawn(async move {
        responder_side.recv_frame().await.unwrap();
        responder_side.send_frame(b"not a frame").await.unwrap();
    });

    let session = Session::new(RbsrEngine::initiator(source(0..100)));
    let error = drive_initiator(session, &mut initiator_side)
        .await
        .expect_err("garbage must fail the session");
    assert!(matches!(error, ReconcileError::Codec(_)), "{error:?}");
    peer.await.unwrap();
}

#[tokio::test]
async fn a_responder_stops_a_peer_that_never_converges() {
    let (mut peer_side, mut responder_side) = MemoryStream::pair();

    let responder = tokio::spawn(async move {
        let session = Session::new(RbsrEngine::responder(source(0..300)));
        drive_responder(session, &mut responder_side).await
    });

    // A hostile initiator that answers every refinement with the same
    // never-matching full-range fingerprint.
    let provocation = RbsrMessage::new(vec![Range::fingerprint(
        Bound::Max,
        crate::reconcile::engine::rbsr::Fingerprint::of(std::iter::once(&id(u64::MAX))),
    )]);
    let frame = codec::encode(&provocation).unwrap();
    for _ in 0..=super::super::session::MAX_ROUNDS {
        if peer_side.send_frame(&frame).await.is_err() {
            break;
        }
        let _ = peer_side.recv_frame().await;
    }

    let error = responder
        .await
        .unwrap()
        .expect_err("the round cap must end the session");
    assert!(
        matches!(error, ReconcileError::RoundCapExceeded { .. }),
        "{error:?}"
    );
}

#[tokio::test]
async fn a_responder_ends_cleanly_when_the_initiator_closes() {
    let (mut initiator_side, mut responder_side) = MemoryStream::pair();

    let responder = tokio::spawn(async move {
        let session = Session::new(RbsrEngine::responder(source(0..50)));
        drive_responder(session, &mut responder_side).await
    });

    initiator_side.finish().await.unwrap();
    drop(initiator_side);

    assert_eq!(
        responder
            .await
            .unwrap()
            .expect("clean close is not an error")
            .rounds,
        0,
        "a session closed before its first round served none"
    );
}

#[tokio::test(start_paused = true)]
async fn an_initiator_whose_peer_goes_silent_times_out() {
    // The peer stays alive and holds the stream open but never answers, which a
    // closed-stream check cannot catch: only a clock can.
    let (mut initiator_side, _peer_side) = MemoryStream::pair();

    let session = Session::new(RbsrEngine::initiator(source(0..100)));
    let error = drive_initiator(session, &mut initiator_side)
        .await
        .expect_err("a silent peer must fail the session");
    assert!(matches!(error, ReconcileError::Transport(_)), "{error:?}");
}

#[tokio::test(start_paused = true)]
async fn a_responder_whose_peer_goes_silent_times_out() {
    // The exact shape the coordinator sees: a peer opens a session, sends
    // nothing, and would otherwise pin the responder's task and its snapshot
    // for as long as it liked.
    let (_peer_side, mut responder_side) = MemoryStream::pair();

    let session = Session::new(RbsrEngine::responder(source(0..100)));
    let error = drive_responder(session, &mut responder_side)
        .await
        .expect_err("a silent peer must fail the session");
    assert!(matches!(error, ReconcileError::Transport(_)), "{error:?}");
}

#[tokio::test]
async fn a_session_reports_what_it_cost() {
    let (diff, cost) = reconcile((0..300).collect(), (0..300).collect()).await;

    assert!(diff.is_empty());
    assert!(cost.rounds > 0, "a converged session consumed messages");
    assert!(
        cost.bytes_sent > 0 && cost.bytes_received > 0,
        "both directions must be counted, got {cost:?}"
    );
}

#[tokio::test]
async fn agreeing_costs_far_less_than_diverging() {
    // The property that makes reconciliation worth having: cost tracks the
    // difference, not the size of the sets. Asserted through the counters so it
    // stays true rather than being taken on trust.
    let identical: Vec<u64> = (0..500).collect();
    let (_, agreed) = reconcile(identical.clone(), identical.clone()).await;
    let (_, diverged) = reconcile(identical, (0..500).filter(|n| n % 3 != 0).collect()).await;

    assert!(
        agreed.bytes_sent + agreed.bytes_received
            < (diverged.bytes_sent + diverged.bytes_received) / 4,
        "a zero-diff session must be far cheaper: agreed {agreed:?} vs diverged {diverged:?}"
    );
}
