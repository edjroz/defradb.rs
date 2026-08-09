//! Where a measured row goes and what must be true of it.
//!
//! Shared by both drivers so a `mode=ranges` row lands in the same directory,
//! under the same naming, and is judged by the same rule as the `mode=default`
//! row it will be compared against.

use std::path::PathBuf;

use super::csv::{self, MeasurementRow};
use super::payload_csv;
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

/// Record what every mode put on the wire as payload, beside what it converged
/// on.
pub(super) fn record_payload(name: &str, rows: &[MeasurementRow]) {
    let path = out_dir().join(format!("{name}_payload.csv"));
    payload_csv::write(&path, rows).expect("write payload csv");
    println!("{}\n{}", path.display(), payload_csv::render(rows));
}

/// Compare the wire payload of two modes and say so loudly when they differ.
///
/// This is the check the final-state comparison below cannot make. Two modes
/// that converge on the same blockstore have proved they ended in the same
/// place; only the counters can say whether they moved the same bytes to get
/// there. A difference is a finding about a mode, not a broken run, so the row
/// is flagged and kept rather than thrown away — and the numbers are in the
/// payload sidecar either way.
pub(super) fn flag_payload_divergence(
    scenario: &str,
    left: &[MeasurementRow],
    right: &[MeasurementRow],
) {
    if payload_csv::payload_matches(left, right) {
        return;
    }
    let name = |rows: &[MeasurementRow]| {
        rows.first()
            .map(|row| row.mode.clone())
            .unwrap_or_else(|| "?".to_string())
    };
    println!(
        "  FLAG {scenario}: wire payload differs between mode={} and mode={}",
        name(left),
        name(right)
    );
    for rows in [left, right] {
        for row in rows {
            println!(
                "    mode={} node={} payload sent/recv {}/{} for {} blocks",
                row.mode, row.node_id, row.payload_bytes_sent, row.payload_bytes_recv, row.blocks
            );
        }
    }
}

/// Reconciliation may only change what it costs to *discover* a difference. The
/// blocks that end up on each node, and their total size, must be exactly what
/// the default path produced for the same scenario — a ranges row that moved
/// different payload is measuring a different thing, not a cheaper one.
///
/// This is a **final-state** comparison and is nearly tautological once both
/// modes converge; [`flag_payload_divergence`] is the wire-level counterpart.
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
