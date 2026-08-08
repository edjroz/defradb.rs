//! Ingress authorization for served reconciliation sessions.
//!
//! A session hands the peer the collection's entire head set, so it is gated
//! exactly like its two siblings: the any-collection check DocSync applies
//! before it will answer at all, and the collection-scoped check BranchableSync
//! applies once it knows which collection is being asked for. These tests pin
//! both, because the two refusals are observed differently — the first fails the
//! call, the second closes the stream without answering, since the collection is
//! only known after the peer's opening frame has been read on the session's own
//! task.

use std::sync::Arc;

use async_trait::async_trait;

use super::access_tests::create_test_coordinator_with_sync_config;
use crate::bitswap::{AccessMode, ReplicatorRegistry};
use crate::error::Error;
use crate::reconcile::engine::rbsr::RbsrEngine;
use crate::reconcile::stream::{MemoryStream, ReconcileStream};
use crate::reconcile::{codec, Engine, Item, ItemId, MemorySource, SessionOpen};
use crate::sync::manager::SyncConfig;
use crate::sync::peer_state::PeerStateTracker;
use crate::sync::reconcile::ReconcileSourceProvider;
use crate::transport::PeerId;
use crate::{ReplicationFilters, ReplicatorInfo};

const COLLECTION: &str = "Note";
const OTHER_COLLECTION: &str = "Other";
const PEER: &str = "peer-asking";

/// A source with a handful of items, so a served session has something to say.
struct FixedSource;

#[async_trait]
impl ReconcileSourceProvider for FixedSource {
    async fn snapshot(&self, _collection_id: &str) -> crate::error::Result<MemorySource> {
        let items = (0..8u64).map(|seed| Item::new(seed, ItemId::new(seed.to_be_bytes().to_vec())));
        MemorySource::new(items).map_err(|error| Error::Transport(error.to_string()))
    }
}

fn reconcile_config() -> SyncConfig {
    SyncConfig {
        reconcile_enabled: true,
        ..Default::default()
    }
}

fn replicator_for(collection_id: &str) -> Arc<ReplicatorRegistry> {
    let registry = Arc::new(ReplicatorRegistry::new());
    registry.set_replicator_info(ReplicatorInfo::from_raw_with_filters(
        PEER.to_string(),
        vec![collection_id.to_string()],
        Vec::new(),
        ReplicationFilters::new(),
    ));
    registry
}

/// Runs one served session against a coordinator built from the given access
/// state, returning the call's result and whatever the peer got back.
async fn serve_session(
    replicators: Arc<ReplicatorRegistry>,
    peer_state: Arc<PeerStateTracker>,
    collection_id: &str,
) -> (crate::error::Result<()>, Option<Vec<u8>>) {
    let (coordinator, _events) = create_test_coordinator_with_sync_config(
        AccessMode::Controlled,
        replicators,
        peer_state,
        reconcile_config(),
    );
    coordinator.install_reconcile_source(Arc::new(FixedSource));

    let (mut peer_side, node_side) = MemoryStream::pair();
    peer_side
        .send_frame(&codec::encode(&SessionOpen::new(collection_id)).unwrap())
        .await
        .unwrap();

    // A real initiator follows its opening frame with the engine's first range
    // message; without it a served session has nothing to answer.
    let opening = RbsrEngine::initiator(FixedSource.snapshot(collection_id).await.unwrap())
        .next_outbound()
        .unwrap()
        .unwrap();
    peer_side
        .send_frame(&codec::encode(&opening).unwrap())
        .await
        .unwrap();

    let result = coordinator
        .handle_reconcile_session(PeerId::new(PEER.to_string()), Box::new(node_side))
        .await;

    // The session runs on its own task; give it a chance to answer or hang up.
    let answer = tokio::time::timeout(std::time::Duration::from_secs(5), peer_side.recv_frame())
        .await
        .expect("the served session must settle, not hang")
        .expect("the stream must not error");

    (result, answer)
}

#[tokio::test]
async fn a_peer_authorized_for_nothing_is_refused_before_the_session_starts() {
    let (result, answer) = serve_session(
        Arc::new(ReplicatorRegistry::new()),
        Arc::new(PeerStateTracker::new()),
        COLLECTION,
    )
    .await;

    assert!(
        matches!(result, Err(Error::AccessDenied { .. })),
        "an unauthorized peer must be refused, got {result:?}"
    );
    assert!(
        answer.is_none(),
        "a refused peer must not receive a reconciliation message"
    );
}

#[tokio::test]
async fn a_connected_peer_is_served() {
    let peer_state = Arc::new(PeerStateTracker::new());
    peer_state.peer_connected(PEER);

    let (result, answer) =
        serve_session(Arc::new(ReplicatorRegistry::new()), peer_state, COLLECTION).await;

    assert!(
        result.is_ok(),
        "a connected peer must be served: {result:?}"
    );
    assert!(
        answer.is_some(),
        "a served session must answer the peer's opening frame"
    );
}

#[tokio::test]
async fn a_replicator_for_the_named_collection_is_served() {
    let (result, answer) = serve_session(
        replicator_for(COLLECTION),
        Arc::new(PeerStateTracker::new()),
        COLLECTION,
    )
    .await;

    assert!(result.is_ok(), "a replicator must be served: {result:?}");
    assert!(answer.is_some(), "a served session must answer");
}

#[tokio::test]
async fn a_replicator_for_another_collection_gets_no_answer() {
    // Authorized for *something*, so the any-collection gate lets it in, but not
    // for the collection it then names. The stream closes unanswered.
    let (result, answer) = serve_session(
        replicator_for(OTHER_COLLECTION),
        Arc::new(PeerStateTracker::new()),
        COLLECTION,
    )
    .await;

    assert!(
        result.is_ok(),
        "the collection is unknown until the session's own task reads it"
    );
    assert!(
        answer.is_none(),
        "a peer unauthorized for the named collection must get no head set"
    );
}
