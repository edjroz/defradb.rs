//! What a driver learned about a run, before it is turned into per-node rows.
//!
//! The columns a measurement row carries come from two places: the node (its
//! counters and its blockstore) and the driver (how long it took, how many
//! rounds it needed, whether it converged). This is the driver's half, so the
//! default-mode and ranges-mode drivers can share one row builder and cannot
//! drift apart in how they fill the shared schema.

/// The driver's view of one finished run.
pub(crate) struct RunOutcome {
    pub scenario: String,
    /// `default` for the shipped DocSync + DAG-fill path, `ranges` for RBSR.
    pub mode: &'static str,
    pub wall_ms: f64,
    /// Protocol rounds, counted the same way in both modes: one request and its
    /// answer. In ranges mode this is the sum over every session the run drove.
    pub rounds: u32,
    pub converged: bool,
    pub state_match: bool,
}
