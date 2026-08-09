//! The discovery-cost sidecar a ranges run records next to its measurement row.
//!
//! The shared fourteen-column schema has no room for what a reconciliation
//! session actually costs: its `ctrlBytes` mix the session's frames with the
//! control traffic of the DAG fetch that follows, and no column holds the number
//! of sessions or what each end counted. Widening the schema would break the
//! overlay against the Go rows, so the discovery cost travels beside the row
//! instead, keyed by the same scenario name.

use std::fmt::Write as _;
use std::path::Path;

use super::ranges::RangesRun;

pub(super) const HEADER: &str = "scenario,mode,sessions,rounds,outcomeBytesSent,outcomeBytesRecv,wireSent,wireRecv,wireMsgs,sessionOpenBytes,headsNeeded,error";

/// Render one run's session accounting as a complete CSV document.
pub(super) fn render(scenario: &str, run: &RangesRun) -> String {
    let cost = &run.cost;
    let open_bytes = run
        .initiator_wire
        .bytes_sent
        .saturating_sub(cost.bytes_sent)
        .checked_div(u64::from(cost.sessions))
        .unwrap_or(0);

    let mut out = String::from(HEADER);
    out.push('\n');
    let _ = writeln!(
        out,
        "{},ranges,{},{},{},{},{},{},{},{},{},{}",
        scenario,
        cost.sessions,
        cost.rounds,
        cost.bytes_sent,
        cost.bytes_received,
        run.initiator_wire.bytes_sent,
        run.initiator_wire.bytes_recv,
        run.initiator_wire.msgs,
        open_bytes,
        cost.heads_needed,
        run.error.as_deref().unwrap_or("").replace(',', ";"),
    );
    out
}

/// Write `run`'s session accounting to `path`, creating parent directories.
pub(super) fn write(path: &Path, scenario: &str, run: &RangesRun) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, render(scenario, run))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync_bench::ranges::{AlpnCounts, SessionCost};

    fn run() -> RangesRun {
        RangesRun {
            rows: Vec::new(),
            cost: SessionCost {
                sessions: 2,
                rounds: 4,
                bytes_sent: 2983,
                bytes_received: 28147,
                heads_needed: 556,
            },
            initiator_wire: AlpnCounts {
                bytes_sent: 3069,
                bytes_recv: 28147,
                msgs: 10,
            },
            responder_wire: AlpnCounts {
                bytes_sent: 28147,
                bytes_recv: 3069,
                msgs: 10,
            },
            error: None,
        }
    }

    #[test]
    fn every_row_has_one_field_per_header_column() {
        let rendered = render("diff500_docs500", &run());
        let columns = HEADER.split(',').count();
        for line in rendered.lines() {
            assert_eq!(line.split(',').count(), columns, "bad row: {line}");
        }
    }

    /// The opening frame is charged per session, not per run, or a run that
    /// needed two sessions would report twice the handshake as one session's.
    #[test]
    fn the_session_open_frame_is_reported_per_session() {
        let rendered = render("diff500_docs500", &run());
        let fields: Vec<&str> = rendered.lines().nth(1).unwrap().split(',').collect();
        assert_eq!(fields[9], "43");
    }

    /// A run that ended badly must still produce a row, and its reason must not
    /// smuggle a comma into the schema.
    #[test]
    fn a_failed_run_records_its_reason_in_one_field() {
        let mut failed = run();
        failed.error = Some("round cap exceeded, max 32".to_string());
        let rendered = render("diff500_docs500", &failed);
        let fields: Vec<&str> = rendered.lines().nth(1).unwrap().split(',').collect();
        assert_eq!(fields.len(), HEADER.split(',').count());
        assert_eq!(fields[11], "round cap exceeded; max 32");
    }

    #[test]
    fn a_run_with_no_sessions_does_not_divide_by_zero() {
        let mut empty = run();
        empty.cost = SessionCost::default();
        empty.initiator_wire = AlpnCounts::default();
        assert!(render("nothing", &empty).contains(",0,0,"));
    }
}
