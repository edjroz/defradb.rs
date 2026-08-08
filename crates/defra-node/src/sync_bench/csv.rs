//! CSV emission in the Go harness's per-node measurement schema.
//!
//! Column names, ordering, the Python-style `True`/`False` booleans and the
//! one-decimal `wallMs` all match `tests/bench/crossdevice/run.py` in the Go
//! tree, so Rust rows overlay Go rows in the existing plot pipeline without a
//! schema shim.

use std::fmt::Write as _;
use std::path::Path;

pub(crate) const HEADER: &str = "topology,N,scenario,mode,nodeID,ctrlBytesSent,ctrlBytesRecv,ctrlMsgs,blocks,blockBytes,wallMs,rounds,converged,stateMatch";

/// One node's view of one scenario run.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct MeasurementRow {
    pub topology: String,
    pub n: usize,
    pub scenario: String,
    /// `default` for the shipped DocSync + DAG-fill path; `ranges` and `riblt`
    /// are reserved for the reconciliation engines.
    pub mode: String,
    pub node_id: usize,
    pub ctrl_bytes_sent: u64,
    pub ctrl_bytes_recv: u64,
    pub ctrl_msgs: u64,
    pub blocks: u64,
    pub block_bytes: u64,
    pub wall_ms: f64,
    pub rounds: u32,
    pub converged: bool,
    /// Convergence asserted as identical document sets, not block counts.
    pub state_match: bool,
}

fn python_bool(value: bool) -> &'static str {
    if value {
        "True"
    } else {
        "False"
    }
}

/// Render `rows` as a complete CSV document, header included.
pub(crate) fn render(rows: &[MeasurementRow]) -> String {
    let mut out = String::from(HEADER);
    out.push('\n');
    for row in rows {
        let _ = writeln!(
            out,
            "{},{},{},{},{},{},{},{},{},{},{:.1},{},{},{}",
            row.topology,
            row.n,
            row.scenario,
            row.mode,
            row.node_id,
            row.ctrl_bytes_sent,
            row.ctrl_bytes_recv,
            row.ctrl_msgs,
            row.blocks,
            row.block_bytes,
            row.wall_ms,
            row.rounds,
            python_bool(row.converged),
            python_bool(row.state_match),
        );
    }
    out
}

/// Write `rows` to `path`, creating parent directories as needed.
pub(crate) fn write(path: &Path, rows: &[MeasurementRow]) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, render(rows))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row() -> MeasurementRow {
        MeasurementRow {
            topology: "pair".into(),
            n: 2,
            scenario: "tiny-diff".into(),
            mode: "default".into(),
            node_id: 0,
            ctrl_bytes_sent: 68084,
            ctrl_bytes_recv: 21011,
            ctrl_msgs: 3,
            blocks: 3007,
            block_bytes: 648_518,
            wall_ms: 2537.34,
            rounds: 1,
            converged: true,
            state_match: true,
        }
    }

    #[test]
    fn header_matches_the_go_schema() {
        assert_eq!(
            HEADER,
            "topology,N,scenario,mode,nodeID,ctrlBytesSent,ctrlBytesRecv,ctrlMsgs,\
blocks,blockBytes,wallMs,rounds,converged,stateMatch"
        );
    }

    #[test]
    fn a_row_renders_exactly_like_the_go_harness() {
        let rendered = render(&[row()]);
        let mut lines = rendered.lines();
        assert_eq!(lines.next(), Some(HEADER));
        assert_eq!(
            lines.next(),
            Some("pair,2,tiny-diff,default,0,68084,21011,3,3007,648518,2537.3,1,True,True")
        );
        assert_eq!(lines.next(), None);
    }

    #[test]
    fn every_row_has_one_field_per_header_column() {
        let rendered = render(&[row(), row()]);
        let columns = HEADER.split(',').count();
        for line in rendered.lines() {
            assert_eq!(line.split(',').count(), columns, "bad row: {line}");
        }
    }

    #[test]
    fn falsehood_renders_python_style() {
        let mut failed = row();
        failed.converged = false;
        failed.state_match = false;
        assert!(render(&[failed]).contains(",False,False"));
    }

    #[test]
    fn an_empty_run_still_writes_the_header() {
        assert_eq!(render(&[]), format!("{HEADER}\n"));
    }

    #[test]
    fn write_creates_missing_directories() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("nested/run.csv");
        write(&path, &[row()]).expect("write csv");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), render(&[row()]));
    }
}
