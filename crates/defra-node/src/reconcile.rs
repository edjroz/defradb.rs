//! The in-process handle that starts a reconciliation session.
//!
//! Reconciliation has no scheduler: something has to ask for a session. This is
//! that something, and it is the whole trigger surface — no HTTP route and no
//! CLI command reaches it.
//!
//! Gated behind [`P2PConfig::reconcile_enabled`](crate::P2PConfig), which also
//! decides whether the node offers the reconciliation ALPN at all, so a node
//! that has not opted in can neither start a session nor serve one.

use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use blockstore::Blockstore;
use cid::Cid;
use p2p::sync::IrohSyncCoordinator;
use p2p::transport::PeerId;

/// What one reconciliation session found and what it cost.
///
/// The cost fields are not diagnostics: round count and control bytes are the
/// quantities a reconciliation protocol is judged on, so a caller that cannot
/// read them cannot tell a working session from one that degenerated into
/// exchanging whole sets.
///
/// One session reconciles one direction. `need` is what this node will pull;
/// `have` is what the peer is missing and will not receive from this session.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileOutcome {
    /// Head CIDs the peer holds and this node lacked; a fetch is already under
    /// way for each.
    pub need: Vec<Cid>,
    /// Head CIDs this node holds and the peer lacked.
    pub have: Vec<Cid>,
    /// Peer messages this session consumed.
    pub rounds: usize,
    /// Encoded frame bytes sent to the peer.
    pub bytes_sent: u64,
    /// Encoded frame bytes received from the peer.
    pub bytes_received: u64,
}

/// Starts reconciliation sessions against peers.
#[async_trait]
pub trait ReconcileTrigger: Send + Sync {
    /// Reconciles one collection against one peer.
    async fn reconcile_collection(
        &self,
        peer_id: &str,
        collection: &str,
    ) -> Result<ReconcileOutcome>;
}

/// The iroh sync coordinator behind the handle.
pub(crate) struct CoordinatorTrigger<B: Blockstore + 'static> {
    coordinator: Arc<IrohSyncCoordinator<B>>,
}

impl<B: Blockstore + 'static> CoordinatorTrigger<B> {
    pub(crate) fn new(coordinator: Arc<IrohSyncCoordinator<B>>) -> Self {
        Self { coordinator }
    }
}

#[async_trait]
impl<B: Blockstore + 'static> ReconcileTrigger for CoordinatorTrigger<B> {
    async fn reconcile_collection(
        &self,
        peer_id: &str,
        collection: &str,
    ) -> Result<ReconcileOutcome> {
        let (diff, cost) = self
            .coordinator
            .reconcile_collection(&PeerId::new(peer_id.to_string()), collection)
            .await
            .map_err(|error| anyhow!("reconciliation failed: {error}"))?;

        Ok(ReconcileOutcome {
            need: cids(diff.need()),
            have: cids(diff.have()),
            rounds: cost.rounds,
            bytes_sent: cost.bytes_sent,
            bytes_received: cost.bytes_received,
        })
    }
}

fn cids(ids: &[p2p::reconcile::ItemId]) -> Vec<Cid> {
    ids.iter()
        .filter_map(|id| Cid::try_from(id.as_bytes()).ok())
        .collect()
}
