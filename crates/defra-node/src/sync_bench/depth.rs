//! Depth invariance: what a deeper divergence costs to *find*.
//!
//! Both paths discover a divergence head-first — the default path compares head
//! CIDs, a session compares head identities — so neither should care how many
//! commits stand behind a head. The payload must grow with depth, because those
//! commits have to be fetched and merged; the discovery cost must not. This
//! sweep holds the document count and the number of diverged documents fixed and
//! moves only the branch depth.
//!
//! `#[ignore]`d for the same reason the rest of the matrix is: each point builds
//! four iroh nodes and waits for real convergence.

use p2p::reconcile::EngineKind;

use super::baseline::SEED;
use super::csv::MeasurementRow;
use super::documents::{apply_deep_updates, quiesce, seed_docs};
use super::harness::NodePair;
use super::output::{
    assert_converged, assert_payload_identity, flag_payload_divergence, record, record_payload,
    record_sessions,
};
use super::ranges::{mode_for, RangesRun};
use super::scenario::DivergenceFixture;

/// Both engines, in the order their rows are recorded.
const ENGINES: [EngineKind; 2] = [EngineKind::Rbsr, EngineKind::Riblt];

/// Ten documents, all of them diverged, matching the Go bench's `manyhead_docs10`.
const DOCS: usize = 10;
const DIVERGED: usize = 10;

fn scenario(depth: u32) -> String {
    format!("manyhead_docs{DOCS}_depth{depth}")
}

async fn default_point(depth: u32) -> Vec<MeasurementRow> {
    let fixture = DivergenceFixture::new(SEED, DOCS, DIVERGED);
    let pair = NodePair::isolated().await;
    let doc_ids = seed_docs(&pair.writer, &fixture).await;
    quiesce(&pair.writer).await;

    pair.connect().await;
    let base = pair
        .measure_pull(&format!("{}_base", scenario(depth)), &doc_ids)
        .await;
    if base.iter().any(|row| !row.converged || !row.state_match) {
        pair.shutdown().await;
        return base;
    }

    apply_deep_updates(&pair.writer, &doc_ids, &fixture, depth).await;
    quiesce(&pair.writer).await;
    pair.reset_counters();
    let rows = pair.measure_pull(&scenario(depth), &doc_ids).await;
    pair.shutdown().await;
    rows
}

async fn ranges_point(depth: u32, engine: EngineKind) -> RangesRun {
    let fixture = DivergenceFixture::new(SEED, DOCS, DIVERGED);
    let pair = NodePair::isolated_with_reconcile(true).await;
    let doc_ids = seed_docs(&pair.writer, &fixture).await;
    quiesce(&pair.writer).await;

    pair.connect().await;
    let base = pair
        .measure_reconcile(&format!("{}_base", scenario(depth)), &doc_ids, engine)
        .await;
    if base
        .rows
        .iter()
        .any(|row| !row.converged || !row.state_match)
    {
        pair.shutdown().await;
        return base;
    }

    apply_deep_updates(&pair.writer, &doc_ids, &fixture, depth).await;
    quiesce(&pair.writer).await;
    pair.reset_counters();
    let run = pair
        .measure_reconcile(&scenario(depth), &doc_ids, engine)
        .await;
    pair.shutdown().await;
    run
}

macro_rules! depth_point {
    ($name:ident, $depth:literal) => {
        #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
        #[ignore = "benchmark: builds six iroh nodes and waits for real convergence"]
        async fn $name() {
            let default_rows = default_point($depth).await;
            let mut runs = Vec::new();
            for engine in ENGINES {
                runs.push(ranges_point($depth, engine).await);
            }

            let file = format!("depth_docs{DOCS}_k{}", $depth);
            let mut rows = default_rows.clone();
            for run in &runs {
                rows.extend(run.rows.iter().cloned());
            }
            record(&file, &rows);
            record_payload(&file, &rows);
            for run in &runs {
                record_sessions(
                    &format!("{file}_{}", mode_for(run.engine)),
                    &scenario($depth),
                    run,
                );
                flag_payload_divergence(&file, &default_rows, &run.rows);
            }
            for run in &runs {
                assert_converged(&run.rows);
                assert_payload_identity(&default_rows, &run.rows);
            }
        }
    };
}

depth_point!(depth_k3, 3);
depth_point!(depth_k10, 10);
depth_point!(depth_k25, 25);
depth_point!(depth_k50, 50);
