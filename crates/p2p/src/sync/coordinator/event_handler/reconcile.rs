//! Set reconciliation sessions and the handoff of what they discover.
//!
//! A session ends with a set of head CIDs the peer holds and this node does not.
//! Those go straight into the DAG fetch path that BranchableSync already uses
//! for exactly this shape of input — head CIDs with a collection but no document
//! identity, which merge recovers from the genesis composite. Reconciliation
//! therefore adds a way to *discover* what to fetch and nothing else.

use std::sync::Arc;

use blockstore::Blockstore;
use cid::Cid;

use super::super::dag_context::DagFetchContext;
use super::super::SyncCoordinator;
use crate::error::{Error, Result};
use crate::reconcile::{Diff, ItemId, ReconcileStream, SessionCost};
use crate::sync::reconcile;
use crate::transport::{P2PTransport, PeerId};

impl<B: Blockstore + 'static, T: P2PTransport> SyncCoordinator<B, T> {
    /// Reconciles one collection against a peer and fetches whatever the
    /// session says is missing locally.
    ///
    /// Returns the difference the session discovered together with what it
    /// cost, so a caller can assert on what was found and on how much traffic
    /// finding it took, rather than only on what eventually arrived.
    ///
    /// One session reconciles one direction: this node learns what it needs.
    /// Making both peers whole means running a session from each side.
    pub async fn reconcile_collection(
        &self,
        peer_id: &PeerId,
        collection_id: &str,
    ) -> Result<(Diff, SessionCost)> {
        self.ensure_reconcile_enabled()?;

        let local = self.reconcile_source()?.snapshot(collection_id).await?;
        let mut stream = self
            .runtime
            .transport
            .open_reconcile_session(peer_id)
            .await?;

        let (diff, cost) = reconcile::initiate(stream.as_mut(), collection_id, local).await?;
        tracing::info!(
            peer_id = %peer_id,
            collection_id = %collection_id,
            need = diff.need().len(),
            have = diff.have().len(),
            rounds = cost.rounds,
            bytes_sent = cost.bytes_sent,
            bytes_received = cost.bytes_received,
            "Reconciliation session converged"
        );

        self.fetch_reconciled_heads(peer_id, collection_id, diff.need());
        Ok((diff, cost))
    }

    /// Serves a session a peer opened.
    ///
    /// The whole session, including reading the peer's opening frame, runs on
    /// its own task. Reading even that first frame on the transport's event
    /// loop would let one peer that opens a stream and says nothing stall every
    /// other event the node has to handle.
    pub(crate) async fn handle_reconcile_session(
        &self,
        peer_id: PeerId,
        mut stream: Box<dyn ReconcileStream>,
    ) -> Result<()> {
        self.ensure_reconcile_enabled()?;
        let source = self.reconcile_source()?;

        self.spawn_background_task("reconcile_serve_session", async move {
            let served = async {
                let collection_id = reconcile::accept(stream.as_mut()).await?;
                let local = source.snapshot(&collection_id).await?;
                let cost = reconcile::serve(stream.as_mut(), local).await?;
                Ok::<_, Error>((collection_id, cost))
            }
            .await;

            match served {
                Ok((collection_id, cost)) => tracing::info!(
                    peer_id = %peer_id,
                    collection_id = %collection_id,
                    rounds = cost.rounds,
                    bytes_sent = cost.bytes_sent,
                    bytes_received = cost.bytes_received,
                    "Served reconciliation session"
                ),
                Err(error) => tracing::debug!(
                    peer_id = %peer_id,
                    error = %error,
                    "Reconciliation session ended"
                ),
            }
        });
        Ok(())
    }

    fn ensure_reconcile_enabled(&self) -> Result<()> {
        if self.runtime.reconcile_enabled {
            return Ok(());
        }
        Err(Error::Transport(
            "set reconciliation is not enabled on this node".to_string(),
        ))
    }

    fn reconcile_source(&self) -> Result<Arc<dyn crate::sync::reconcile::ReconcileSourceProvider>> {
        self.reconcile_source.get().cloned().ok_or_else(|| {
            Error::Transport("no reconciliation source is installed on this node".to_string())
        })
    }

    /// Hands the need set to the DAG fetch path, one fetch per head.
    fn fetch_reconciled_heads(&self, peer_id: &PeerId, collection_id: &str, need: &[ItemId]) {
        let heads: Vec<Cid> = need
            .iter()
            .filter_map(|id| Cid::try_from(id.as_bytes()).ok())
            .collect();
        if heads.is_empty() {
            return;
        }

        let is_explicit_replicator = self.is_registered_replicator(peer_id.as_str(), collection_id);
        for root_cid in heads {
            let transport = self.runtime.transport.clone();
            let blockstore = self.manager.blockstore().clone();
            let event_tx = self.manager.event_sender();
            let limiter = self.runtime.dag_fetch_limiter.clone();
            let source_peer = peer_id.clone();
            let collection_id = collection_id.to_string();

            self.spawn_background_task("reconcile_fetch_dag", async move {
                let alternate_providers =
                    super::super::dag_fetcher::connected_alternate_providers(&transport, &root_cid)
                        .await;
                super::super::dag_fetcher::poll_fetch_dag(
                    transport,
                    blockstore,
                    event_tx,
                    root_cid,
                    DagFetchContext::new(String::new(), collection_id, String::new(), source_peer)
                        .with_alternate_providers(alternate_providers)
                        .with_explicit_replicator(is_explicit_replicator),
                    limiter,
                )
                .await;
            });
        }
    }
}
