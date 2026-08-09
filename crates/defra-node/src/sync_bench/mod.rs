//! P2P sync benchmark harness: deterministic divergence fixtures and CSV
//! emission in the Go harness's measurement schema, for both the shipped
//! DocSync path (`mode=default`) and set reconciliation (`mode=ranges`).
//!
//! Traffic counting itself lives in `p2p::metrics`; a benchmark hands a
//! [`p2p::metrics::TransportCounters`] to each node through
//! [`crate::P2PConfig::counters`] and reads the totals back into a
//! [`csv::MeasurementRow`].

mod baseline;
mod csv;
mod depth;
mod documents;
mod harness;
mod output;
mod ranges;
mod ranges_matrix;
mod run;
mod scenario;
mod session_csv;
