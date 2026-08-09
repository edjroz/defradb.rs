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
use p2p::sync::IrohSyncCoordinator;
use p2p::transport::PeerId;

pub use p2p::reconcile::EngineKind;
pub use p2p::sync::ReconcileOutcome;

/// Starts reconciliation sessions against peers.
#[async_trait]
pub trait ReconcileTrigger: Send + Sync {
    /// Reconciles one collection against one peer with the named engine.
    async fn reconcile_collection(
        &self,
        peer_id: &str,
        collection: &str,
        engine: EngineKind,
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
        engine: EngineKind,
    ) -> Result<ReconcileOutcome> {
        let (diff, cost) = self
            .coordinator
            .reconcile_collection(&PeerId::new(peer_id.to_string()), collection, engine)
            .await
            .map_err(|error| anyhow!("reconciliation failed: {error}"))?;

        Ok(ReconcileOutcome::new(&diff, cost))
    }
}
