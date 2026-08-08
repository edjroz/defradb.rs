//! A metered two-node pair and the primitives a scenario drives it with.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::{Duration, Instant};

use p2p::metrics::TransportCounters;
use serde_json::Value as JsonValue;

use super::csv::MeasurementRow;
use super::scenario::DivergenceFixture;
use crate::{EmbeddedNode, P2PConfig};

const SDL: &str = "type BenchDoc { name: String value: Int }";
const COLLECTION: &str = "BenchDoc";

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

fn bench_p2p_config(counters: Arc<TransportCounters>) -> P2PConfig {
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
        init_tracing();
        let writer_counters = TransportCounters::new();
        let reader_counters = TransportCounters::new();
        let writer = build_node(Arc::clone(&writer_counters)).await;
        let reader = build_node(Arc::clone(&reader_counters)).await;

        Self {
            writer,
            reader,
            writer_counters,
            reader_counters,
        }
    }

    pub(crate) async fn connect(&self) {
        let writer_addr = listen_addr(&self.writer).await;
        let reader_p2p = self.reader.p2p().expect("reader p2p");
        let writer_p2p = self.writer.p2p().expect("writer p2p");

        reader_p2p
            .connect_peer(&writer_addr)
            .await
            .expect("connect reader -> writer");
        wait_for_peer(&self.writer).await;
        wait_for_peer(&self.reader).await;

        for p2p in [writer_p2p, reader_p2p] {
            p2p.add_collections(vec![COLLECTION.to_string()])
                .await
                .expect("subscribe collection");
        }
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
                scenario: scenario.to_string(),
                mode: "default".to_string(),
                node_id,
                ctrl_bytes_sent: snapshot.control_bytes_sent(),
                ctrl_bytes_recv: snapshot.control_bytes_recv(),
                ctrl_msgs: snapshot.control_msgs(),
                blocks,
                block_bytes,
                wall_ms,
                rounds,
                converged,
                state_match,
            });
        }
        rows
    }

    /// Documents the two nodes do not agree on right now.
    async fn divergent(&self, doc_ids: &[String]) -> Vec<String> {
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
    async fn settle(&self, doc_ids: &[String]) -> Vec<String> {
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

async fn build_node(counters: Arc<TransportCounters>) -> EmbeddedNode {
    let node = EmbeddedNode::builder()
        .with_p2p(bench_p2p_config(counters))
        .build()
        .await
        .expect("build bench node");
    node.add_schema(SDL).await.expect("add bench schema");
    node
}

async fn listen_addr(node: &EmbeddedNode) -> String {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let addrs = node
            .p2p()
            .expect("p2p enabled")
            .listen_addresses()
            .await
            .expect("listen_addresses");
        if let Some(addr) = addrs.first() {
            return addr.clone();
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

/// Write the fixture's seed documents and return their doc IDs in fixture
/// order.
pub(crate) async fn seed_docs(node: &EmbeddedNode, fixture: &DivergenceFixture) -> Vec<String> {
    let mut doc_ids = Vec::with_capacity(fixture.docs.len());
    for doc in &fixture.docs {
        let response = node
            .execute(&format!(
                r#"mutation {{ add_BenchDoc(input: {{name: "{}", value: {}}}) {{ _docID }} }}"#,
                doc.name, doc.value
            ))
            .await;
        doc_ids.push(created_doc_id(&response.data, &response.errors));
    }
    doc_ids
}

/// Apply the fixture's writer-only updates.
pub(crate) async fn apply_updates(
    node: &EmbeddedNode,
    doc_ids: &[String],
    fixture: &DivergenceFixture,
) {
    for update in &fixture.updates {
        let response = node
            .execute(&format!(
                r#"mutation {{ update_BenchDoc(docID: "{}", input: {{value: {}}}) {{ _docID }} }}"#,
                doc_ids[update.doc_index], update.value
            ))
            .await;
        assert!(
            response.errors.is_empty(),
            "update failed: {:?}",
            response.errors
        );
    }
}

fn created_doc_id(data: &Option<JsonValue>, errors: &[impl std::fmt::Debug]) -> String {
    assert!(errors.is_empty(), "mutation failed: {errors:?}");
    data.as_ref()
        .and_then(|d| d.get("add_BenchDoc"))
        .and_then(|v| v.as_array())
        .and_then(|docs| docs.first())
        .and_then(|doc| doc.get("_docID"))
        .and_then(|id| id.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| panic!("mutation returned no _docID: {data:?}"))
}

/// The node's document set as `docID -> value`, the basis for `stateMatch`.
async fn doc_values(node: &EmbeddedNode) -> BTreeMap<String, i64> {
    let response = node
        .execute(&format!("query {{ {COLLECTION} {{ _docID value }} }}"))
        .await;
    assert!(
        response.errors.is_empty(),
        "state query failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get(COLLECTION))
        .and_then(|docs| docs.as_array())
        .map(|docs| {
            docs.iter()
                .filter_map(|doc| {
                    let id = doc.get("_docID")?.as_str()?.to_string();
                    let value = doc.get("value")?.as_i64()?;
                    Some((id, value))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Converged blockstore size, the same state measure the Go harness reported.
async fn block_stats(node: &EmbeddedNode) -> (u64, u64) {
    let Some(blockstore) = node.p2p_blockstore() else {
        return (0, 0);
    };
    let cids = blockstore.all_cids().await.expect("all_cids");
    let mut bytes = 0u64;
    for cid in &cids {
        bytes += blockstore
            .get_size(cid)
            .await
            .expect("get_size")
            .unwrap_or(0) as u64;
    }
    (cids.len() as u64, bytes)
}
