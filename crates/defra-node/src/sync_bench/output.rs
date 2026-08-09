//! Where a measured row goes and what must be true of it.
//!
//! Shared by both drivers so a `mode=ranges` row lands in the same directory,
//! under the same naming, and is judged by the same rule as the `mode=default`
//! row it will be compared against.

use std::path::PathBuf;

use super::csv::{self, MeasurementRow};
use super::ranges::RangesRun;
use super::session_csv;

pub(super) fn out_dir() -> PathBuf {
    std::env::var("DEFRA_SYNC_BENCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/sync-bench"))
}

pub(super) fn record(name: &str, rows: &[MeasurementRow]) {
    let path = out_dir().join(format!("{name}.csv"));
    csv::write(&path, rows).expect("write measurement csv");
    println!("{}\n{}", path.display(), csv::render(rows));
}

/// Record the discovery cost beside the measurement row it belongs to.
pub(super) fn record_sessions(name: &str, scenario: &str, run: &RangesRun) {
    let path = out_dir().join(format!("{name}_sessions.csv"));
    session_csv::write(&path, scenario, run).expect("write session csv");
    println!("{}\n{}", path.display(), session_csv::render(scenario, run));
}

/// Every row is recorded before it is judged, so a scenario that fails to
/// converge still leaves an honest `converged=False` row behind rather than
/// no row at all.
pub(super) fn assert_converged(rows: &[MeasurementRow]) {
    for row in rows {
        assert!(
            row.converged && row.state_match,
            "{} node {} did not reach an identical document set",
            row.scenario,
            row.node_id
        );
    }
}

/// Reconciliation may only change what it costs to *discover* a difference. The
/// blocks that end up on each node, and their total size, must be exactly what
/// the default path produced for the same scenario — a ranges row that moved
/// different payload is measuring a different thing, not a cheaper one.
pub(super) fn assert_payload_identity(
    default_rows: &[MeasurementRow],
    ranges_rows: &[MeasurementRow],
) {
    assert_eq!(
        default_rows.len(),
        ranges_rows.len(),
        "the two modes reported a different number of nodes"
    );
    for (default, ranges) in default_rows.iter().zip(ranges_rows) {
        assert_eq!(
            (default.node_id, default.blocks, default.block_bytes),
            (ranges.node_id, ranges.blocks, ranges.block_bytes),
            "node {}: ranges mode converged on different payload than the default path",
            default.node_id
        );
    }
}
