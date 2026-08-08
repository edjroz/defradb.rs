//! Phase-0 P2P sync benchmark harness: deterministic divergence fixtures and
//! CSV emission in the Go harness's measurement schema.
//!
//! Traffic counting itself lives in `p2p::metrics`; a benchmark hands a
//! [`p2p::metrics::TransportCounters`] to each node through
//! [`crate::P2PConfig::counters`] and reads the totals back into a
//! [`csv::MeasurementRow`].

pub(crate) mod csv;
pub(crate) mod scenario;
