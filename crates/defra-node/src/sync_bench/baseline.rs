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

use super::csv::MeasurementRow;
use super::documents::{apply_updates, seed_docs};
use super::harness::NodePair;
use super::output::{assert_converged, record};
use super::scenario::DivergenceFixture;

/// Fixed so every recorded row is reproducible.
pub(super) const SEED: u64 = 0x5EED_C0FFEE;

/// Cold start: the reader holds nothing and pulls the writer's whole
/// collection. Divergence is total, so this is the `d == n` corner.
pub(super) async fn cold_start(docs: usize) -> Vec<MeasurementRow> {
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
pub(super) async fn tiny_diff(docs: usize, diverged: usize) -> Vec<MeasurementRow> {
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

/// One sweep point per test, because a node pair does not fully release its
/// iroh endpoint when the pair shuts down: the third or fourth pair built in
/// the same process cannot be dialled any more and the run dies part-way,
/// taking the points already measured with it. A test each keeps every point
/// in its own process, so one failure costs one row.
macro_rules! sweep_point {
    ($name:ident, $file:literal, $docs:literal, $diverged:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore = "benchmark: builds two iroh nodes and waits for real convergence"]
        async fn $name() {
            let rows = tiny_diff($docs, $diverged).await;
            record($file, &rows);
            assert_converged(&rows);
        }
    };
}

// M5: collection-size sweep with exactly one changed document. The whole point
// of set reconciliation is that this cost should not grow with `n`.
sweep_point!(baseline_m5_docs50, "m5_onedocchanged_docs50", 50, 1);
sweep_point!(baseline_m5_docs100, "m5_onedocchanged_docs100", 100, 1);
sweep_point!(baseline_m5_docs200, "m5_onedocchanged_docs200", 200, 1);
sweep_point!(baseline_m5_docs500, "m5_onedocchanged_docs500", 500, 1);
sweep_point!(baseline_m5_docs1000, "m5_onedocchanged_docs1000", 1000, 1);

// Difference sweep at a fixed collection size: how the default path's cost
// responds to how much actually differs.
sweep_point!(baseline_diffsweep_diff1, "diffsweep_docs500_diff1", 500, 1);
sweep_point!(
    baseline_diffsweep_diff10,
    "diffsweep_docs500_diff10",
    500,
    10
);
sweep_point!(
    baseline_diffsweep_diff50,
    "diffsweep_docs500_diff50",
    500,
    50
);
sweep_point!(
    baseline_diffsweep_diff100,
    "diffsweep_docs500_diff100",
    500,
    100
);
sweep_point!(
    baseline_diffsweep_diff250,
    "diffsweep_docs500_diff250",
    500,
    250
);
sweep_point!(
    baseline_diffsweep_diff500,
    "diffsweep_docs500_diff500",
    500,
    500
);
