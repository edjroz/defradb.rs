//! The loop that hands an iroh endpoint's transport events to its coordinator.

use std::sync::Arc;

use p2p::iroh::IrohTransport;
use p2p::P2PTransport;

/// Dispatch every transport event to the coordinator, re-arming a peer's
/// pending retries when it connects.
pub async fn run_iroh_event_handler<B: blockstore::Blockstore + 'static>(
    events: tokio::sync::mpsc::Receiver<
        p2p::TransportEvent<<IrohTransport as P2PTransport>::ResponseToken>,
    >,
    coordinator: Arc<p2p::sync::IrohSyncCoordinator<B>>,
    store: Arc<impl storage::corekv::Store + 'static>,
) {
    let handler_coordinator = coordinator.clone();
    coordinator.run_event_dispatcher(events, move |event, admission| {
        let coordinator = handler_coordinator.clone();
        let store = store.clone();
        async move {
            let event_kind = event.kind();
            if let p2p::TransportEvent::PeerConnected(peer_id) = &event {
                crate::activate_retry_peer(store, peer_id).await;
            }

            if let Err(e) = coordinator
                .handle_transport_event_with_admission(event, admission)
                .await
            {
                if e.is_rate_limited() {
                    tracing::debug!(event_kind, error = %e, "P2P rate-limited");
                } else if e.is_retriable() {
                    tracing::warn!(event_kind, error = %e, "P2P transport event failed after retries");
                } else {
                    tracing::error!(event_kind, error = %e, "P2P event handler error");
                }
            }
        }
    })
    .await;
}
