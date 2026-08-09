//! The three-way measurement matrix, run against real iroh nodes.
//!
//! Each point runs the same fixture three times in one process — over the
//! shipped DocSync path, over RBSR, and over RIBLT — records all three modes'
//! rows in one CSV, and asserts that only the discovery cost changed. Recording
//! them in the same process is deliberate: a comparison between a row taken
//! today and a row taken on another day carries every difference between the
//! two days, and this campaign's whole claim is that only the protocol differs.
//!
//! These are `#[ignore]`d because each one builds six iroh nodes and waits on
//! real convergence. Run them explicitly, e.g.
//!
//! ```text
//! DEFRA_SYNC_BENCH_OUT=/tmp/ranges \
//!   cargo test -p defra-node --features p2p --lib \
//!   sync_bench::ranges_matrix::matrix_m5_docs50 -- --ignored --nocapture
//! ```

use p2p::reconcile::EngineKind;

use super::baseline::{self, SEED};
use super::csv::MeasurementRow;
use super::documents::{apply_updates, quiesce, seed_docs};
use super::harness::NodePair;
use super::output::{
    assert_converged, assert_payload_identity, flag_payload_divergence, record, record_payload,
    record_sessions,
};
use super::ranges::RangesRun;
use super::scenario::DivergenceFixture;

/// Both engines, in the order their rows are recorded.
const ENGINES: [EngineKind; 2] = [EngineKind::Rbsr, EngineKind::Riblt];

/// Cold start over reconciliation: the reader holds nothing, so the session's
/// need set is the writer's whole head set. The `d == n` corner.
async fn ranges_cold_start(docs: usize, scenario: &str, engine: EngineKind) -> RangesRun {
    let fixture = DivergenceFixture::new(SEED, docs, 0);
    let pair = NodePair::isolated_with_reconcile(true).await;
    let doc_ids = seed_docs(&pair.writer, &fixture).await;
    quiesce(&pair.writer).await;

    pair.connect().await;
    pair.reset_counters();
    let run = pair.measure_reconcile(scenario, &doc_ids, engine).await;
    pair.shutdown().await;
    run
}

/// The shared base is established with reconciliation as well, so the measured
/// session is the only thing that differs between a base and a diverged run and
/// the setup does not pay the default path's timeout ladder.
///
/// The base is established on the **same engine** the measured session runs on:
/// a base reached by one protocol and measured by another would put a
/// difference in the setup of the comparison.
async fn ranges_tiny_diff(
    docs: usize,
    diverged: usize,
    scenario: &str,
    engine: EngineKind,
) -> RangesRun {
    let fixture = DivergenceFixture::new(SEED, docs, diverged);
    let pair = NodePair::isolated_with_reconcile(true).await;
    let doc_ids = seed_docs(&pair.writer, &fixture).await;
    quiesce(&pair.writer).await;

    pair.connect().await;
    let base = pair
        .measure_reconcile(&format!("{scenario}_base"), &doc_ids, engine)
        .await;
    if base
        .rows
        .iter()
        .any(|row| !row.converged || !row.state_match)
    {
        // Measuring divergence on top of a base the nodes never agreed on would
        // report a number for something that did not happen.
        pair.shutdown().await;
        return base;
    }

    apply_updates(&pair.writer, &doc_ids, &fixture).await;
    quiesce(&pair.writer).await;
    pair.reset_counters();
    let run = pair.measure_reconcile(scenario, &doc_ids, engine).await;
    pair.shutdown().await;
    run
}

/// Print what the sessions reported next to what the transport counted, and
/// insist the two agree once the handshake is accounted for.
///
/// They are not the same quantity. `SessionCost` is the engine's accounting of
/// the frames its rounds exchanged; the counters see everything on the ALPN,
/// including the one `SessionOpen` frame per session that names the collection
/// before any round happens. So the receive sides must match exactly, and the
/// send side must exceed the session's own count by a whole number of identical
/// opening frames. Anything else means one of the two is not measuring what it
/// claims.
fn report_session_cost(scenario: &str, run: &RangesRun) {
    println!(
        "  {scenario} sessions={} rounds={} outcome_bytes={}/{} wire_initiator={}/{} ({} msgs) wire_responder={}/{} ({} msgs) heads_needed={}",
        run.cost.sessions,
        run.cost.rounds,
        run.cost.bytes_sent,
        run.cost.bytes_received,
        run.initiator_wire.bytes_sent,
        run.initiator_wire.bytes_recv,
        run.initiator_wire.msgs,
        run.responder_wire.bytes_sent,
        run.responder_wire.bytes_recv,
        run.responder_wire.msgs,
        run.cost.heads_needed,
    );
    if let Some(error) = &run.error {
        println!("  {scenario} ended with: {error}");
    }

    assert_eq!(
        run.cost.bytes_received, run.initiator_wire.bytes_recv,
        "{scenario}: the sessions received a different number of bytes than the counters saw"
    );
    let handshake = run
        .initiator_wire
        .bytes_sent
        .checked_sub(run.cost.bytes_sent)
        .unwrap_or_else(|| {
            panic!("{scenario}: the counters saw fewer sent bytes than the sessions reported")
        });
    assert_eq!(
        handshake % u64::from(run.cost.sessions),
        0,
        "{scenario}: {handshake} unaccounted sent bytes over {} sessions is not a whole number of opening frames",
        run.cost.sessions
    );
    println!(
        "  {scenario} session-open overhead: {} bytes per session",
        handshake / u64::from(run.cost.sessions)
    );

    assert_eq!(
        (run.initiator_wire.bytes_sent, run.initiator_wire.bytes_recv),
        (run.responder_wire.bytes_recv, run.responder_wire.bytes_sent),
        "{scenario}: the two ends of the session counted different traffic"
    );
}

/// Record all three modes' rows for one scenario and judge them.
///
/// Everything is recorded before anything is asserted, so a point that fails
/// its convergence check still leaves an honest row on disk rather than none.
fn record_comparison(name: &str, default_rows: &[MeasurementRow], runs: &[RangesRun]) {
    let mut rows = default_rows.to_vec();
    for run in runs {
        rows.extend(run.rows.iter().cloned());
    }
    record(name, &rows);
    record_payload(name, &rows);
    for run in runs {
        let scenario = run
            .rows
            .first()
            .map(|row| row.scenario.clone())
            .unwrap_or_else(|| name.to_string());
        let mode = super::ranges::mode_for(run.engine);
        record_sessions(&format!("{name}_{mode}"), &scenario, run);
        report_session_cost(&format!("{name} [{mode}]"), run);
        flag_payload_divergence(name, default_rows, &run.rows);
    }
    if let [first, rest @ ..] = runs {
        for run in rest {
            flag_payload_divergence(name, &first.rows, &run.rows);
        }
    }
    for run in runs {
        assert_converged(&run.rows);
        assert_payload_identity(default_rows, &run.rows);
    }
}

macro_rules! cold_start_point {
    ($name:ident, $file:literal, $scenario:literal, $docs:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore = "benchmark: builds six iroh nodes and waits for real convergence"]
        async fn $name() {
            let default_rows = baseline::cold_start($docs).await;
            let mut runs = Vec::new();
            for engine in ENGINES {
                runs.push(ranges_cold_start($docs, $scenario, engine).await);
            }
            record_comparison($file, &default_rows, &runs);
        }
    };
}

macro_rules! sweep_point {
    ($name:ident, $file:literal, $docs:literal, $diverged:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore = "benchmark: builds six iroh nodes and waits for real convergence"]
        async fn $name() {
            let scenario = format!("diff{}_docs{}", $diverged, $docs);
            let default_rows = baseline::tiny_diff($docs, $diverged).await;
            let mut runs = Vec::new();
            for engine in ENGINES {
                runs.push(ranges_tiny_diff($docs, $diverged, &scenario, engine).await);
            }
            record_comparison($file, &default_rows, &runs);
        }
    };
}

// M1 / M2: per-document and per-collection pull of a 10-document collection.
// The default path cannot tell the two apart — it is one request shape — but
// reconciliation is per-collection by construction, which is exactly the
// distinction the split exists to expose.
cold_start_point!(
    matrix_m1_per_doc,
    "m1_perdoc_docs10",
    "coldstart_docs10",
    10
);
cold_start_point!(
    matrix_m2_per_collection,
    "m2_collection_docs10",
    "coldstart_docs10",
    10
);

// M5: collection-size sweep with exactly one changed document. The whole point
// of set reconciliation is that this cost should not grow with `n`.
sweep_point!(matrix_m5_docs50, "m5_onedocchanged_docs50", 50, 1);
sweep_point!(matrix_m5_docs100, "m5_onedocchanged_docs100", 100, 1);
sweep_point!(matrix_m5_docs200, "m5_onedocchanged_docs200", 200, 1);
sweep_point!(matrix_m5_docs500, "m5_onedocchanged_docs500", 500, 1);
sweep_point!(matrix_m5_docs1000, "m5_onedocchanged_docs1000", 1000, 1);

// Difference sweep at a fixed collection size.
sweep_point!(matrix_diffsweep_diff1, "diffsweep_docs500_diff1", 500, 1);
sweep_point!(matrix_diffsweep_diff10, "diffsweep_docs500_diff10", 500, 10);
sweep_point!(matrix_diffsweep_diff50, "diffsweep_docs500_diff50", 500, 50);
sweep_point!(
    matrix_diffsweep_diff100,
    "diffsweep_docs500_diff100",
    500,
    100
);
sweep_point!(
    matrix_diffsweep_diff250,
    "diffsweep_docs500_diff250",
    500,
    250
);
sweep_point!(
    matrix_diffsweep_diff500,
    "diffsweep_docs500_diff500",
    500,
    500
);

/// The agreed-set session cost: nothing differs, so this is what a session
/// costs to prove there is nothing to do. Naming follows the v2 convention,
/// `diff{d}_docs{n}` with `d = 0`.
///
/// **This is the row the whole RIBLT comparison turns on**, and it is the one
/// regime a live node is in almost every time it reconciles. The range engine
/// answers agreement with one fingerprint and one all-skip; a rateless decoder
/// cannot know the sets agree until it has pulled a batch and decoded it, so it
/// always pays the opening batch. Chart 16 cannot be drawn without it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark: builds six iroh nodes and waits for real convergence"]
async fn matrix_zero_diff() {
    let default_rows = baseline::tiny_diff(500, 0).await;
    let mut runs = Vec::new();
    for engine in ENGINES {
        runs.push(ranges_tiny_diff(500, 0, "diff0_docs500", engine).await);
    }
    record_comparison("diffsweep_docs500_diff0", &default_rows, &runs);
}
