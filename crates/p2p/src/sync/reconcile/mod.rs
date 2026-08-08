//! Coordinator-side set reconciliation, sibling to [`dag_sync`](super::dag_sync).
//!
//! A session discovers a difference and stops there: the need set it produces is
//! handed to the DAG fetch path that already exists, exactly as a DocSync or
//! BranchableSync reply's heads are. Nothing here fetches, merges, or schedules.
//!
//! # One session, one direction
//!
//! A session makes the *initiator* whole and leaves the responder exactly as it
//! was. The initiator learns both halves of the difference — what it must pull,
//! and what it holds that the peer lacks — but a session never pushes, because
//! discovery and delivery are deliberately separate (the RFC's discovery-only
//! principle). Two peers converge on each other by running a session in each
//! direction, or by one of them acting on the `have` set through the ordinary
//! replication path.

mod session;
mod source_provider;

pub use session::{accept, initiate, serve};
pub use source_provider::ReconcileSourceProvider;
