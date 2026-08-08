//! Default-path baselines: two real in-process iroh nodes converging over the
//! shipped DocSync + DAG-fill path.
//!
//! These are `#[ignore]`d because each one builds two nodes and waits on real
//! network convergence. Run them explicitly, e.g.
//!
//! ```text
//! DEFRA_SYNC_BENCH_OUT=/tmp/baselines \
//!   cargo test -p defra-node --features p2p sync_bench::baseline -- --ignored --nocapture
//! ```
//!
//! Each scenario writes `<out>/<name>.csv` in the Go crossdevice schema and
//! then asserts convergence as identical document sets — recording first, so a
//! run that does not converge leaves a `converged=False` row rather than none.

use std::path::PathBuf;

use super::csv::{self, MeasurementRow};
use super::documents::{apply_updates, seed_docs};
use super::harness::NodePair;
use super::scenario::DivergenceFixture;

/// Fixed so every recorded row is reproducible.
const SEED: u64 = 0x5EED_C0FFEE;

fn out_dir() -> PathBuf {
    std::env::var("DEFRA_SYNC_BENCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/sync-bench"))
}

fn record(name: &str, rows: &[MeasurementRow]) {
    let path = out_dir().join(format!("{name}.csv"));
    csv::write(&path, rows).expect("write measurement csv");
    println!("{}\n{}", path.display(), csv::render(rows));
}

/// Every row is recorded before it is judged, so a scenario that fails to
/// converge still leaves an honest `converged=False` row behind rather than
/// no row at all.
fn assert_converged(rows: &[MeasurementRow]) {
    for row in rows {
        assert!(
            row.converged && row.state_match,
            "{} node {} did not reach an identical document set",
            row.scenario,
            row.node_id
        );
    }
}

/// Cold start: the reader holds nothing and pulls the writer's whole
/// collection. Divergence is total, so this is the `d == n` corner.
async fn cold_start(docs: usize) -> Vec<MeasurementRow> {
    let fixture = DivergenceFixture::new(SEED, docs, 0);
    let pair = NodePair::isolated().await;
    let doc_ids = seed_docs(&pair.writer, &fixture).await;

    pair.connect().await;
    pair.reset_counters();
    let rows = pair
        .measure_pull(&format!("coldstart_docs{docs}"), &doc_ids)
        .await;
    pair.shutdown().await;
    rows
}

/// Both nodes share the same `docs` documents *and the same history*; the
/// writer then changes `diverged` of them and the reader pulls.
///
/// The shared base is established by syncing, not by writing the same content
/// on both nodes: each node signs its own commits, so independent writes of
/// identical content produce sibling DAGs and turn a one-sided update into a
/// concurrent-write merge. Syncing first is also the ordering the Go harness
/// used, and it keeps the measured traffic to the divergence alone.
async fn tiny_diff(docs: usize, diverged: usize) -> Vec<MeasurementRow> {
    let fixture = DivergenceFixture::new(SEED, docs, diverged);
    let pair = NodePair::isolated().await;
    let doc_ids = seed_docs(&pair.writer, &fixture).await;

    pair.connect().await;
    let scenario = format!("diff{diverged}_docs{docs}");
    let base = pair
        .measure_pull(&format!("{scenario}_base"), &doc_ids)
        .await;
    if base.iter().any(|row| !row.converged || !row.state_match) {
        // Measuring divergence on top of a base the nodes never agreed on
        // would report a number for something that did not happen.
        pair.shutdown().await;
        return base;
    }

    apply_updates(&pair.writer, &doc_ids, &fixture).await;
    pair.reset_counters();
    let rows = pair.measure_pull(&scenario, &doc_ids).await;
    pair.shutdown().await;
    rows
}

/// M1: per-document pull of a 10-document collection from a cold reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark: builds two iroh nodes and waits for real convergence"]
async fn baseline_m1_per_doc() {
    let rows = cold_start(10).await;
    record("m1_perdoc_docs10", &rows);
    assert_converged(&rows);
}

/// M2: whole-collection pull of the same 10 documents.
///
/// On the default path M1 and M2 are the same request shape — the split only
/// becomes meaningful once a per-collection reconciliation mode exists — so
/// this row is the `mode=default` reference both are compared against.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark: builds two iroh nodes and waits for real convergence"]
async fn baseline_m2_per_collection() {
    let rows = cold_start(10).await;
    record("m2_collection_docs10", &rows);
    assert_converged(&rows);
}

/// M5: collection-size sweep with exactly one changed document. The whole
/// point of set reconciliation is that this cost should not grow with `n`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark: five two-node runs, minutes not seconds"]
async fn baseline_m5_size_sweep() {
    let mut rows = Vec::new();
    for docs in [50, 100, 200, 500, 1000] {
        let run = tiny_diff(docs, 1).await;
        record(&format!("m5_onedocchanged_docs{docs}"), &run);
        rows.extend(run);
    }
    record("m5_size_sweep", &rows);
    assert_converged(&rows);
}

/// Difference sweep at a fixed collection size: how the default path's cost
/// responds to how much actually differs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark: six two-node runs over 500-document collections"]
async fn baseline_diffsweep_docs500() {
    let mut rows = Vec::new();
    for diverged in [1, 10, 50, 100, 250, 500] {
        let run = tiny_diff(500, diverged).await;
        record(&format!("diffsweep_docs500_diff{diverged}"), &run);
        rows.extend(run);
    }
    record("diffsweep_docs500", &rows);
    assert_converged(&rows);
}
