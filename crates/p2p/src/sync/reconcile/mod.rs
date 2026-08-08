//! Coordinator-side set reconciliation, sibling to [`dag_sync`](super::dag_sync).
//!
//! A session discovers a difference and stops there: the need set it produces is
//! handed to the DAG fetch path that already exists, exactly as a DocSync or
//! BranchableSync reply's heads are. Nothing here fetches, merges, or schedules.

mod session;
mod source_provider;

pub use session::{accept, initiate, serve};
pub use source_provider::{EmptySourceProvider, ReconcileSourceProvider};
