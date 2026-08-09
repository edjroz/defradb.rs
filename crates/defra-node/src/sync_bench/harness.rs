//! A metered two-node pair and the primitives a scenario drives it with.

use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use p2p::metrics::TransportCounters;

use super::csv::MeasurementRow;
use super::documents::{block_stats, doc_values, COLLECTION, SDL};
use super::run::RunOutcome;
use crate::{EmbeddedNode, P2PConfig};

/// Overall budget for one scenario's convergence, across all rounds.
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(900);

/// A single `sync_documents` call is bounded by its own deadline, so the reader
/// is driven to a fixpoint over rounds — the same shape the Go harness used.
const MAX_ROUNDS: u32 = 80;

/// How long the divergent set may stand still before the driver gives up.
///
/// This is a duration and not a round count on purpose. A receiving node holds
/// rejected pending-DAG registrations until they expire, and only then can the
/// next batch be admitted, so progress arrives in waves separated by that
/// expiry. A round-count budget silently shrinks the wait whenever rounds get
/// cheaper; a duration does not.
const NO_PROGRESS_BUDGET: Duration = Duration::from_secs(400);

/// Upper bound on how long one round waits for the reader to catch up.
const ROUND_SETTLE: Duration = Duration::from_secs(20);

/// How long the divergent set must hold steady before a round is considered
/// finished. `sync_documents` already waits for its own merges, so this only
/// has to cover the lag between a merge and the query seeing it.
const ROUND_STABLE: Duration = Duration::from_secs(2);

/// Convergence poll interval. Bounds the resolution of every `wallMs` here: a
/// reported time can overshoot the true one by up to this much.
const POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Route node logs to the test writer so a stalled run can be diagnosed with
/// `RUST_LOG`.
fn init_tracing() {
    static INIT: std::sync::Once = std::sync::Once::new();
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .with_test_writer()
            .try_init();
    });
}

fn bench_p2p_config(counters: Arc<TransportCounters>, reconcile_enabled: bool) -> P2PConfig {
    P2PConfig {
        port: 0,
        bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        relay_mode: p2p::iroh::IrohRelayModeConfig::Disabled,
        discovery: p2p::iroh::IrohDiscoveryConfig::Disabled,
        max_concurrent_multipath_paths: None,
        secret_key_path: None,
        load_persisted_collections: false,
        max_concurrent_dag_fetches: p2p::sync::DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
        max_concurrent_push_tasks: p2p::sync::DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
        max_doc_sync_request_doc_ids: p2p::sync::DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
        rate_limit_burst: p2p::sync::DEFAULT_RATE_LIMIT_BURST,
        rate_limit_rate: p2p::sync::DEFAULT_RATE_LIMIT_RATE,
        max_pending_dags: p2p::sync::DEFAULT_MAX_PENDING_DAGS,
        counters: Some(counters),
        reconcile_enabled,
    }
}

/// Node 0 writes, node 1 pulls. Each carries its own counter set, so traffic
/// is attributable per node exactly as in the Go crossdevice harness.
pub(crate) struct NodePair {
    pub writer: EmbeddedNode,
    pub reader: EmbeddedNode,
    writer_counters: Arc<TransportCounters>,
    reader_counters: Arc<TransportCounters>,
}

impl NodePair {
    /// Two nodes that can see each other's addresses but have not been
    /// introduced. Scenarios seed divergence first and call [`Self::connect`]
    /// afterwards, so seeding never crosses the wire — the same ordering the
    /// Go harness used, and the only way the measured traffic is the
    /// reconciliation traffic.
    pub(crate) async fn isolated() -> Self {
        Self::isolated_with_reconcile(false).await
    }

    /// As [`Self::isolated`], but both nodes offer and accept reconciliation
    /// sessions. Both sides must opt in: the responder only advertises the
    /// reconciliation ALPN when it has.
    pub(crate) async fn isolated_with_reconcile(reconcile_enabled: bool) -> Self {
        init_tracing();
        let writer_counters = TransportCounters::new();
        let reader_counters = TransportCounters::new();
        let writer = build_node(Arc::clone(&writer_counters), reconcile_enabled).await;
        let reader = build_node(Arc::clone(&reader_counters), reconcile_enabled).await;

        Self {
            writer,
            reader,
            writer_counters,
            reader_counters,
        }
    }

    /// Introduce the two nodes without subscribing either to the collection
    /// topic, so [`Self::measure_pull`] measures the pull and nothing else.
    ///
    /// Subscribing here would put a gossip broadcast on the wire for every
    /// document the writer touches, concurrently with the pull, and the
    /// counters cannot tell the two apart: the recorded cost would be a pull
    /// racing a push rather than the cost of reconciling. Documents do reach
    /// an unsubscribed reader over `sync_documents` alone — that is what
    /// `p2p_pull_tests::doc_sync_pull_delivers_documents_without_a_subscription`
    /// pins.
    pub(crate) async fn connect(&self) {
        let writer_addr = dialable_addr(&self.writer).await;
        let reader_p2p = self.reader.p2p().expect("reader p2p");

        reader_p2p
            .connect_peer(&writer_addr)
            .await
            .expect("connect reader -> writer");
        wait_for_peer(&self.writer).await;
        wait_for_peer(&self.reader).await;
    }

    pub(crate) fn reader_snapshot(&self) -> p2p::metrics::CountersSnapshot {
        self.reader_counters.snapshot()
    }

    pub(crate) fn writer_snapshot(&self) -> p2p::metrics::CountersSnapshot {
        self.writer_counters.snapshot()
    }

    pub(crate) fn reset_counters(&self) {
        self.writer_counters.reset();
        self.reader_counters.reset();
    }

    /// Drive the reader to a fixpoint against the writer and report one row
    /// per node.
    ///
    /// Convergence is judged by comparing the two nodes live, not against a
    /// snapshot: a merge can move either side, so a stale expectation would
    /// mistake a converged pair for a divergent one.
    pub(crate) async fn measure_pull(
        &self,
        scenario: &str,
        doc_ids: &[String],
    ) -> Vec<MeasurementRow> {
        let reader_p2p = self.reader.p2p().expect("reader p2p");

        let started = Instant::now();
        let mut rounds = 0;
        let mut converged = false;
        let mut pending: Vec<String> = doc_ids.to_vec();
        let mut last_progress = Instant::now();
        while rounds < MAX_ROUNDS && started.elapsed() < CONVERGENCE_TIMEOUT {
            rounds += 1;
            let before = pending.len();
            reader_p2p
                .sync_documents(COLLECTION, pending.clone())
                .await
                .expect("sync_documents");
            pending = self.settle(doc_ids).await;
            println!(
                "  {scenario} round {rounds}: {} of {} documents still divergent",
                pending.len(),
                doc_ids.len()
            );
            if pending.is_empty() {
                converged = true;
                break;
            }
            if pending.len() < before {
                last_progress = Instant::now();
            } else if last_progress.elapsed() >= NO_PROGRESS_BUDGET {
                println!(
                    "  {scenario}: no progress for {}s, stopping",
                    last_progress.elapsed().as_secs()
                );
                break;
            }
        }
        let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
        let state_match = converged && self.divergent(doc_ids).await.is_empty();

        self.rows(&RunOutcome {
            scenario: scenario.to_string(),
            mode: "default",
            wall_ms,
            rounds,
            converged,
            state_match,
        })
        .await
    }

    /// The peer ID the reader must name to reach the writer.
    pub(crate) async fn writer_peer_id(&self) -> String {
        self.writer
            .p2p()
            .expect("writer p2p")
            .local_peer_id()
            .await
            .expect("local_peer_id")
    }

    /// One row per node for a run that has already finished.
    pub(crate) async fn rows(&self, outcome: &RunOutcome) -> Vec<MeasurementRow> {
        let mut rows = Vec::with_capacity(2);
        for (node_id, node, counters) in [
            (0, &self.writer, &self.writer_counters),
            (1, &self.reader, &self.reader_counters),
        ] {
            let snapshot = counters.snapshot();
            let (blocks, block_bytes) = block_stats(node).await;
            rows.push(MeasurementRow {
                topology: "pair".to_string(),
                n: 2,
                scenario: outcome.scenario.clone(),
                mode: outcome.mode.to_string(),
                node_id,
                ctrl_bytes_sent: snapshot.control_bytes_sent(),
                ctrl_bytes_recv: snapshot.control_bytes_recv(),
                ctrl_msgs: snapshot.control_msgs(),
                blocks,
                block_bytes,
                wall_ms: outcome.wall_ms,
                rounds: outcome.rounds,
                converged: outcome.converged,
                state_match: outcome.state_match,
            });
        }
        rows
    }

    /// Documents the two nodes do not agree on right now.
    pub(crate) async fn divergent(&self, doc_ids: &[String]) -> Vec<String> {
        let writer = doc_values(&self.writer).await;
        let reader = doc_values(&self.reader).await;
        doc_ids
            .iter()
            .filter(|id| writer.get(*id) != reader.get(*id))
            .cloned()
            .collect()
    }

    /// Wait out one round, returning the documents the nodes still disagree on.
    /// An empty result means converged.
    ///
    /// Returns as soon as the divergent set holds steady for [`ROUND_STABLE`],
    /// so a sweep of many chunked rounds does not pay the full settle window
    /// each time.
    pub(crate) async fn settle(&self, doc_ids: &[String]) -> Vec<String> {
        let deadline = Instant::now() + ROUND_SETTLE;
        let mut pending = self.divergent(doc_ids).await;
        let mut steady_since = Instant::now();
        while !pending.is_empty() && Instant::now() < deadline {
            tokio::time::sleep(POLL_INTERVAL).await;
            let current = self.divergent(doc_ids).await;
            if current.len() != pending.len() {
                steady_since = Instant::now();
            }
            pending = current;
            if steady_since.elapsed() >= ROUND_STABLE {
                break;
            }
        }
        pending
    }

    pub(crate) async fn shutdown(self) {
        self.writer.shutdown().await;
        self.reader.shutdown().await;
    }
}

async fn build_node(counters: Arc<TransportCounters>, reconcile_enabled: bool) -> EmbeddedNode {
    let node = EmbeddedNode::builder()
        .with_p2p(bench_p2p_config(counters, reconcile_enabled))
        .build()
        .await
        .expect("build bench node");
    node.add_schema(SDL).await.expect("add bench schema");
    node
}

/// An address of `node` that a peer can actually dial.
///
/// Deliberately not `listen_addresses().first()`. That list is ordered direct
/// addresses first, and which direct address leads is whichever one iroh
/// enumerated first; the host's LAN address is discovered about half a second
/// after start and sorts ahead of loopback. These nodes bind loopback only, so
/// once that happens the first entry names a socket nothing is listening on and
/// every dial to it burns the full timeout. `shareable_address` is the accessor
/// that answers this question — it prefers a ticket carrying a dialable address
/// — and its own documentation says callers should not have to guess which
/// entry was meant for sharing.
async fn dialable_addr(node: &EmbeddedNode) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let addr = node
            .p2p()
            .expect("p2p enabled")
            .shareable_address()
            .await
            .expect("shareable_address");
        if let Some(addr) = addr {
            return addr;
        }
        assert!(Instant::now() < deadline, "node never listened");
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

async fn wait_for_peer(node: &EmbeddedNode) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let peers = node
            .p2p()
            .expect("p2p enabled")
            .connected_peers()
            .await
            .expect("connected_peers");
        if !peers.is_empty() {
            return;
        }
        assert!(Instant::now() < deadline, "node never saw a peer");
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}
