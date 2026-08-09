//! The handle that starts a reconciliation session.
//!
//! Reconciliation has no scheduler by design: something has to ask for it. This
//! is that something — the smallest surface that lets a caller name a peer and a
//! collection and get back what the session discovered, without exposing the
//! whole sync coordinator.

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
pub trait ReconcileOperations: Send + Sync {
    /// Reconciles one collection against one peer with the named engine.
    async fn reconcile_collection(
        &self,
        peer_id: &str,
        collection: &str,
        engine: EngineKind,
    ) -> Result<ReconcileOutcome>;
}

/// The iroh coordinator behind the handle.
pub struct CoordinatorReconciler<B: Blockstore + 'static> {
    coordinator: Arc<IrohSyncCoordinator<B>>,
}

impl<B: Blockstore + 'static> CoordinatorReconciler<B> {
    /// Wraps a coordinator as a reconciliation handle.
    pub fn new(coordinator: Arc<IrohSyncCoordinator<B>>) -> Self {
        Self { coordinator }
    }
}

#[async_trait]
impl<B: Blockstore + 'static> ReconcileOperations for CoordinatorReconciler<B> {
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
