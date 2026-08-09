//! What each mode put on the wire as payload, beside what it discovered.
//!
//! Phase 3 asserted payload identity by comparing the two nodes' final
//! blockstore contents. That is a statement about where the modes ended up, and
//! it is very nearly a tautology once both converged: it cannot see a mode that
//! moved different bytes, or the same bytes more than once, on its way there.
//! This sidecar records the payload counters themselves, one row per node per
//! mode, so a three-way comparison can be checked rather than assumed.
//!
//! It is a sidecar and not new columns because the measurement schema is the Go
//! harness's and the overlay reads it positionally.

use std::fmt::Write as _;
use std::path::Path;

use super::csv::MeasurementRow;

pub(super) const HEADER: &str =
    "scenario,mode,nodeID,payloadBytesSent,payloadBytesRecv,blocks,blockBytes";

/// Render every row's payload accounting as a complete CSV document.
pub(super) fn render(rows: &[MeasurementRow]) -> String {
    let mut out = String::from(HEADER);
    out.push('\n');
    for row in rows {
        let _ = writeln!(
            out,
            "{},{},{},{},{},{},{}",
            row.scenario,
            row.mode,
            row.node_id,
            row.payload_bytes_sent,
            row.payload_bytes_recv,
            row.blocks,
            row.block_bytes,
        );
    }
    out
}

/// Write `rows`' payload accounting to `path`, creating parent directories.
pub(super) fn write(path: &Path, rows: &[MeasurementRow]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, render(rows))
}

/// Whether the two modes moved the same payload on every node.
///
/// Reported rather than asserted. A mode that moved more payload for the same
/// converged state is a finding about that mode — the shipped path's resend
/// behaviour is exactly such a finding — and losing the whole recorded point to
/// a panic would throw the evidence away with it.
pub(super) fn payload_matches(left: &[MeasurementRow], right: &[MeasurementRow]) -> bool {
    left.len() == right.len()
        && left.iter().zip(right).all(|(l, r)| {
            (l.node_id, l.payload_bytes_sent, l.payload_bytes_recv)
                == (r.node_id, r.payload_bytes_sent, r.payload_bytes_recv)
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(mode: &str, node_id: usize, sent: u64, recv: u64) -> MeasurementRow {
        MeasurementRow {
            topology: "pair".into(),
            n: 2,
            scenario: "diff1_docs500".into(),
            mode: mode.into(),
            node_id,
            ctrl_bytes_sent: 0,
            ctrl_bytes_recv: 0,
            ctrl_msgs: 0,
            blocks: 1506,
            block_bytes: 254_252,
            wall_ms: 0.0,
            rounds: 1,
            converged: true,
            state_match: true,
            payload_bytes_sent: sent,
            payload_bytes_recv: recv,
        }
    }

    #[test]
    fn every_row_has_one_field_per_header_column() {
        let rendered = render(&[row("default", 0, 1, 2), row("riblt", 1, 3, 4)]);
        let columns = HEADER.split(',').count();
        for line in rendered.lines() {
            assert_eq!(line.split(',').count(), columns, "bad row: {line}");
        }
    }

    #[test]
    fn the_mode_is_carried_so_three_modes_can_share_a_file() {
        let rendered = render(&[row("default", 0, 1, 2), row("riblt", 0, 1, 2)]);
        assert!(rendered.contains(",default,"));
        assert!(rendered.contains(",riblt,"));
    }

    #[test]
    fn identical_payload_on_every_node_matches() {
        let left = [row("default", 0, 10, 20), row("default", 1, 20, 10)];
        let right = [row("riblt", 0, 10, 20), row("riblt", 1, 20, 10)];
        assert!(payload_matches(&left, &right));
    }

    /// The case the blockstore comparison cannot see: same final state, more
    /// bytes moved to reach it.
    #[test]
    fn the_same_final_state_moved_twice_does_not_match() {
        let left = [row("default", 0, 20, 0)];
        let right = [row("riblt", 0, 10, 0)];
        assert!(!payload_matches(&left, &right));
    }

    #[test]
    fn a_missing_node_does_not_match() {
        assert!(!payload_matches(&[row("default", 0, 1, 1)], &[]));
    }
}
