//! Assembling and tearing down a browser peer.
//!
//! This mirrors `defra-node`'s `setup_p2p`: the same coordinator, replication
//! stack, event dispatch, retry loop, and adapter, over a relay-only endpoint.
//! Encryption key distribution is not wired here; a browser peer does not yet
//! hold or serve DEKs.

use std::sync::Arc;

use db::event::emission::TxnBroadcaster;
use db::DB;
use defra_p2p_adapter::P2POperations;
use n0_future::task::JoinHandle;
use p2p::iroh::{IrohEndpointConfig, IrohTransport};
use p2p::P2PTransport;
use storage::RegolithStore;

use crate::error::{Result, WasmError};

use super::config::P2PConfig;

type Blockstore = blockstore::DefraBlockstore<RegolithStore>;

/// A running peer. Dropping it without [`P2PRuntime::shutdown`] leaves its
/// tasks running until the page unloads.
pub(crate) struct P2PRuntime {
    transport: IrohTransport,
    coordinator: p2p::sync::SyncShutdownHandle,
    endpoint_task: JoinHandle<()>,
    tasks: Vec<JoinHandle<()>>,
    pub(crate) ops: Arc<dyn P2POperations>,
    pub(crate) mutator: Arc<dyn query::DocMutator>,
    pub(crate) txn_broadcaster: Arc<dyn TxnBroadcaster>,
}

impl P2PRuntime {
    // Nothing here is Send on wasm32, and nothing needs to be: the browser has
    // one thread.
    #[allow(clippy::arc_with_non_send_sync)]
    pub(crate) async fn start(
        database: Arc<DB<RegolithStore>>,
        event_bus: Arc<dyn events::Bus>,
        document_acp: Arc<dyn acp::DocumentACP>,
        identity: Option<Arc<identity::RawIdentity>>,
        config: &P2PConfig,
    ) -> Result<Self> {
        let store = Arc::clone(database.store());
        let secret_key = config.secret_key(identity.as_deref())?;

        let (command_tx, events, replicators, endpoint_task) =
            p2p::iroh::spawn_endpoint(IrohEndpointConfig {
                secret_key: secret_key.clone(),
                node_identity: identity,
                relay_mode: config.relay_mode(),
                discovery: config.discovery(),
                ..IrohEndpointConfig::default()
            })
            .await
            .map_err(p2p_error("failed to bind iroh endpoint"))?;
        let transport = IrohTransport::new(command_tx, secret_key);

        let blockstore = Arc::new(Blockstore::new(Arc::clone(&store), true));
        let serve_acp = Arc::new(p2p::bitswap::LateBoundServeAcp::new());
        let (mut coordinator, sync_events) =
            p2p::sync::SyncCoordinator::with_head_provider_and_serve_gate(
                transport.clone(),
                Arc::clone(&blockstore),
                p2p::sync::SyncConfig::default(),
                p2p::AccessMode::Controlled,
                replicators,
                Arc::new(p2p::sync::P2PCollectionStore::new(Arc::clone(&store))),
                Arc::new(db::merge::create_head_provider(Arc::clone(&database))),
                Arc::new(replication_filter::QueryReplicationFilterMatcher::new()),
                defra_p2p_adapter::DbBlockClassifier::new_arc(Arc::clone(&database)),
                Arc::clone(&serve_acp),
            )
            .await
            .map_err(p2p_error("failed to create sync coordinator"))?;

        let failure_rx = db::merge::attach_failure_channel(&mut coordinator, 1024);
        let coordinator = Arc::new(coordinator);
        coordinator
            .install_pending_dag_store(Arc::new(p2p::sync::PendingDagStore::new(Arc::clone(
                &store,
            ))))
            .await;
        if let Err(error) = db::merge::load_persisted_collections(&coordinator).await {
            warn(&format!(
                "failed to load persisted P2P collections: {error}"
            ));
        }

        let replication = db::merge::create_replication_stack(
            Arc::clone(&database),
            Arc::clone(&blockstore),
            Arc::clone(&coordinator),
        );

        let doc_pusher = Arc::new(defra_p2p_adapter::DbTransportDocPusher::new(
            Arc::clone(&database),
            transport.clone(),
            coordinator.head_hint_car_authority(),
        ));

        serve_acp.set(p2p::bitswap::ServeAcp {
            resolver: Arc::new(p2p::IrohPeerIdentityResolver::new(transport.clone())),
            gate: defra_p2p_adapter::DbBlockReadGate::new_arc(Arc::clone(&document_acp)),
        });
        replication
            .merge_handler
            .set_document_acp(Arc::clone(&document_acp));
        doc_pusher.set_document_acp(Arc::clone(&document_acp));
        replication.broadcast_mutator.set_document_acp(document_acp);

        let mut tasks = Vec::new();
        let replication_coordinator = Arc::clone(&coordinator);
        let merge_handler = Arc::clone(&replication.merge_handler);
        tasks.push(n0_future::task::spawn(async move {
            p2p::sync::ReplicationLoop::run(
                replication_coordinator,
                sync_events,
                merge_handler,
                p2p::sync::ReplicationConfig::default(),
                |_| {},
            )
            .await;
        }));

        let resync_coordinator = Arc::clone(&coordinator);
        coordinator.spawn_background_task("pending_dag_resync", async move {
            resync_coordinator
                .run_pending_dag_resync(std::time::Duration::from_secs(60))
                .await;
        });
        let retry_clock_coordinator = Arc::clone(&coordinator);
        coordinator.spawn_background_task("pending_dag_retry_clock", async move {
            retry_clock_coordinator
                .run_pending_dag_retry_clock(std::time::Duration::from_secs(2))
                .await;
        });

        let event_coordinator = Arc::clone(&coordinator);
        let event_store = Arc::clone(&store);
        tasks.push(n0_future::task::spawn(async move {
            defra_p2p_adapter::run_iroh_event_handler(events, event_coordinator, event_store).await;
        }));

        tasks.push(defra_p2p_adapter::spawn_failure_recorder(
            storage::stores::Peerstore::new(Arc::clone(&store)),
            failure_rx,
        ));
        let doc_pusher: Arc<dyn defra_p2p_adapter::TransportDocPusher> = doc_pusher;
        tasks.push(defra_p2p_adapter::spawn_retry_loop(
            storage::stores::Peerstore::new(Arc::clone(&store)),
            transport.clone(),
            Arc::clone(&doc_pusher),
            None,
        ));

        let version_syncer = defra_p2p_adapter::DbTransportVersionSyncer::new_arc(
            blockstore,
            Arc::clone(&replication.merge_handler_inner),
            Arc::clone(&database),
            transport.clone(),
        );
        let restored_doc_ids =
            defra_p2p_adapter::restore_iroh_p2p_state(store, &transport, &coordinator).await;
        let adapter = defra_p2p_adapter::IrohP2PAdapter::with_full_context(
            transport.clone(),
            Arc::clone(&coordinator),
            doc_pusher,
            event_bus,
            Some(version_syncer),
            db::node_access_checker(database),
        );
        adapter.set_initial_tracked_documents(restored_doc_ids);

        Ok(Self {
            transport,
            coordinator: coordinator.shutdown_handle(),
            endpoint_task,
            tasks,
            ops: Arc::new(adapter),
            mutator: replication.broadcast_mutator,
            txn_broadcaster: replication.txn_broadcaster,
        })
    }

    pub(crate) fn endpoint_id(&self) -> String {
        self.transport.local_peer_id().to_string()
    }

    /// Stop the network in the order a native node does: no new retries, then
    /// the coordinator's own work, then the endpoint, then everything that
    /// only consumed what those produced.
    pub(crate) async fn shutdown(self) {
        let Self {
            transport,
            coordinator,
            endpoint_task,
            tasks,
            ops,
            mutator,
            txn_broadcaster,
        } = self;
        drop((ops, mutator, txn_broadcaster));

        coordinator.shutdown().await;
        if let Err(error) = transport.shutdown().await {
            warn(&format!("iroh transport shutdown failed: {error}"));
        }
        drop(transport);
        drop(coordinator);

        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        let _ = n0_future::time::timeout(std::time::Duration::from_secs(5), endpoint_task).await;
    }
}

fn p2p_error(context: &'static str) -> impl Fn(p2p::Error) -> WasmError {
    move |error| WasmError::P2P(format!("{context}: {error}"))
}

fn warn(message: &str) {
    web_sys::console::warn_1(&message.into());
}
