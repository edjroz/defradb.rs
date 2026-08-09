//! Engine selection over a live stream, including the ways it is refused.
//!
//! The engines are proved separately in [`crate::reconcile`]; what is proved
//! here is the seam — that the opening frame carries the choice, that both
//! roles derive the same one from it, and that every way of disagreeing about
//! it ends in an error rather than in two peers waiting for each other.

use serde::Serialize;
use sha2::Digest;

use super::{accept, initiate, serve};
use crate::error::Result;
use crate::reconcile::codec::{MessageKind, WireMessage};
use crate::reconcile::stream::MemoryStream;
use crate::reconcile::{
    codec, Diff, EngineKind, Item, ItemId, MemorySource, ReconcileStream, SessionCost,
};

fn id(seed: u64) -> ItemId {
    let digest = sha2::Sha256::digest(seed.to_be_bytes());
    let mut bytes = digest.to_vec();
    bytes.extend_from_slice(&digest[..4]);
    ItemId::new(bytes)
}

fn source(seeds: std::ops::Range<u64>) -> MemorySource {
    MemorySource::new(seeds.map(|seed| Item::new(0, id(seed)))).expect("distinct seeds")
}

/// One full session over a pipe, with the responder taking the engine from the
/// opening frame exactly as the coordinator does.
async fn session(
    local: MemorySource,
    remote: MemorySource,
    engine: EngineKind,
) -> (Result<(Diff, SessionCost)>, Result<SessionCost>) {
    let (mut left, mut right) = MemoryStream::pair();
    let responder = async move {
        let (collection, engine) = accept(&mut right).await?;
        assert_eq!(collection, "Note");
        serve(&mut right, remote, engine).await
    };
    tokio::join!(initiate(&mut left, "Note", local, engine), responder)
}

#[tokio::test]
async fn a_riblt_session_learns_the_difference_in_both_directions() {
    let (initiated, served) = session(source(0..100), source(20..120), EngineKind::Riblt).await;

    let (diff, cost) = initiated.expect("the session converges");
    served.expect("the responder finishes cleanly");

    assert_eq!(diff.need().len(), 20, "what only the peer holds");
    assert_eq!(diff.have().len(), 20, "what only this node holds");
    assert!(cost.bytes_sent > 0 && cost.bytes_received > 0, "{cost:?}");
    assert!(cost.rounds >= 1, "{cost:?}");
}

/// The engine choice must not disturb the engine that was already there.
#[tokio::test]
async fn an_rbsr_session_still_learns_the_same_difference() {
    let (initiated, served) = session(source(0..100), source(20..120), EngineKind::Rbsr).await;

    let (diff, _) = initiated.expect("the session converges");
    served.expect("the responder finishes cleanly");
    assert_eq!(diff.need().len(), 20);
    assert_eq!(diff.have().len(), 20);
}

/// A peer from a build that runs an engine this one does not: same envelope
/// version, same frame kind, an engine tag with no meaning here.
#[derive(Serialize)]
struct FutureOpen {
    collection: String,
    engine: u8,
}

impl WireMessage for FutureOpen {
    const KIND: MessageKind = MessageKind::SessionOpen;
}

impl<'de> serde::Deserialize<'de> for FutureOpen {
    fn deserialize<D: serde::Deserializer<'de>>(_: D) -> std::result::Result<Self, D::Error> {
        unreachable!("this stands in for a peer's encoder, never a decoder")
    }
}

/// The refusal is the point: an engine this build does not implement is
/// answered before a snapshot is taken, not left to the session deadline.
#[tokio::test]
async fn an_unknown_engine_is_refused_at_the_opening_frame() {
    let (mut left, mut right) = MemoryStream::pair();
    let frame = codec::encode(&FutureOpen {
        collection: "Note".to_string(),
        engine: 200,
    })
    .expect("encode");
    left.send_frame(&frame).await.expect("send");

    let error = accept(&mut right).await.expect_err("engine 200 is unknown");
    assert!(
        error
            .to_string()
            .contains("unsupported reconciliation engine 200"),
        "{error}"
    );
}

/// Two peers running different engines is the failure the opening frame exists
/// to prevent; if it happens anyway, the mismatch must surface as an error on
/// the first message rather than as two sides waiting on each other.
#[tokio::test]
async fn peers_running_different_engines_fail_rather_than_hang() {
    let (mut left, mut right) = MemoryStream::pair();
    let remote = source(20..120);

    let responder = async move {
        accept(&mut right).await?;
        serve(&mut right, remote, EngineKind::Rbsr).await
    };
    let (initiated, served) = tokio::join!(
        initiate(&mut left, "Note", source(0..100), EngineKind::Riblt),
        responder
    );

    assert!(initiated.is_err(), "the initiator must not converge");
    assert!(served.is_err(), "the responder must not pretend to serve");
}
