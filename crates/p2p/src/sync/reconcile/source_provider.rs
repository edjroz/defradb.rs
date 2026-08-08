//! The seam between a reconciliation session and the local database.
//!
//! Mirrors [`DocumentHeadProvider`](crate::sync::DocumentHeadProvider): the
//! coordinator names a collection and gets back a set, without this crate
//! depending on the database crates that know how to read one.
//!
//! The return type is a concrete [`MemorySource`] rather than a boxed
//! [`ItemSource`](crate::reconcile::ItemSource) on purpose. `ItemSource` is
//! synchronous and index-addressed while every storage read here is
//! asynchronous, so any real implementation must materialize its snapshot
//! before the session starts. Naming that in the signature keeps the sealed-set
//! contract honest instead of implying a streaming source that cannot exist.

use async_trait::async_trait;

use crate::error::Result;
use crate::reconcile::MemorySource;

/// Supplies the sealed set a reconciliation session runs over.
#[async_trait]
pub trait ReconcileSourceProvider: Send + Sync {
    /// Snapshots the reconcilable items of one collection.
    ///
    /// The snapshot is taken once and used for the whole session; concurrent
    /// local writes are simply not in it. An unknown collection is an empty
    /// set, not an error, so a peer cannot probe which collections exist.
    async fn snapshot(&self, collection_id: &str) -> Result<MemorySource>;
}

/// A provider holding nothing, so a node with no database wiring answers every
/// session with an empty set rather than failing it.
pub struct EmptySourceProvider;

#[async_trait]
impl ReconcileSourceProvider for EmptySourceProvider {
    async fn snapshot(&self, _collection_id: &str) -> Result<MemorySource> {
        MemorySource::new(Vec::new())
            .map_err(|error| crate::error::Error::Transport(error.to_string()))
    }
}
