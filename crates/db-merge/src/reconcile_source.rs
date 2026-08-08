//! The headstore-backed set a reconciliation session runs over.
//!
//! One item per composite head: identity is the head CID's bytes and the sort
//! key's height is that head's commit priority. Both are read from what the
//! database already stores, so two peers holding the same commit derive the
//! same item without agreeing on anything beyond the commit itself.
//!
//! # What is read
//!
//! The collection's documents come from the document keyspace
//! (`/d/{collection_id}/{doc_short_id}`), each document's composite heads from
//! the headstore (`/d/{doc_short_id}/C/{cid}`), and each head's priority from
//! its block. Priority is taken from the block rather than from the headstore's
//! priority index because the index is a local artifact that a store may predate
//! or lack, while the block is the same bytes on every peer. An item whose sort
//! key differed between peers would be reported as a difference by both sides
//! and never reconcile, so this is a correctness choice, not a preference.
//!
//! # Snapshot semantics
//!
//! The whole set is materialized inside one read transaction before the session
//! starts, so a session sees a single consistent point in time no matter how
//! long it runs. Writes committed while a session is in flight are simply not in
//! it; they are found by the next session. This is what the reconciliation
//! engine's sealed-source contract requires, and it is why a "reconcile while
//! writing" workload converges on a later run rather than the current one.
//!
//! Nothing here writes, and no schema or index is added.

use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use datastore::NamespaceView;
use defra_core::{Block, CrdtDelta};
use storage::corekv::{IterOptions, Store};
use storage::keys::doc_id_index::decode_doc_short_id;
use storage::keys::document::DOC_KEY_PREFIX;
use storage::keys::headstore::HeadstoreDocKey;

use db::database::DB;
use p2p::error::{Error, Result};
use p2p::reconcile::{Item, ItemId, MemorySource};
use p2p::sync::ReconcileSourceProvider;

/// A read-only reconciliation source over the live database.
pub struct DbReconcileSource<S: Store> {
    db: Arc<DB<S>>,
}

impl<S: Store> DbReconcileSource<S> {
    /// Reads reconcilable items from the given database.
    pub fn new(db: Arc<DB<S>>) -> Self {
        Self { db }
    }
}

fn storage_error(context: &str, error: impl std::fmt::Display) -> Error {
    Error::HeadProvider(format!("reconcile source: {context}: {error}"))
}

#[async_trait]
impl<S: Store + 'static> ReconcileSourceProvider for DbReconcileSource<S> {
    async fn snapshot(&self, collection: &str) -> Result<MemorySource> {
        // Callers name a collection the way the rest of the P2P surface does,
        // which is sometimes its name and sometimes its ID; the document
        // keyspace is keyed by the ID alone.
        let collection_id = match self.db.get_collection(collection) {
            Ok(Some(found)) => found.collection_id().to_string(),
            _ => collection.to_string(),
        };

        let txn = self
            .db
            .new_txn(true)
            .await
            .map_err(|error| storage_error("failed to open transaction", error))?;

        let datastore = txn
            .datastore()
            .map_err(|error| storage_error("failed to open datastore", error))?;
        let headstore = txn
            .headstore()
            .map_err(|error| storage_error("failed to open headstore", error))?;
        let blockstore = txn
            .blockstore()
            .map_err(|error| storage_error("failed to open blockstore", error))?;

        let mut items = Vec::new();
        for doc_short_id in collection_doc_short_ids(&datastore, &collection_id).await? {
            for cid in composite_heads(&headstore, doc_short_id).await? {
                let Some(priority) = head_priority(&blockstore, &cid).await? else {
                    continue;
                };
                items.push(Item::new(priority, ItemId::new(cid.to_bytes())));
            }
        }

        let _ = txn.discard();
        MemorySource::new(items).map_err(|error| storage_error("duplicate head", error))
    }
}

/// Every document short ID in the collection, from the document keyspace.
async fn collection_doc_short_ids(
    datastore: &NamespaceView,
    collection_id: &str,
) -> Result<Vec<u64>> {
    let mut prefix = DOC_KEY_PREFIX.to_vec();
    prefix.extend_from_slice(collection_id.as_bytes());
    prefix.push(b'/');
    let prefix_len = prefix.len();

    let mut iter = datastore
        .iterator(IterOptions::new().with_prefix(prefix).with_keys_only(true))
        .await
        .map_err(|error| storage_error("failed to iterate documents", error))?;

    let mut ids = Vec::new();
    while let Some(pair) = iter
        .next()
        .await
        .map_err(|error| storage_error("document iteration failed", error))?
    {
        if let Ok(doc_short_id) = decode_doc_short_id(&pair.key[prefix_len..]) {
            ids.push(doc_short_id);
        }
    }
    iter.close()
        .await
        .map_err(|error| storage_error("document iterator close failed", error))?;
    Ok(ids)
}

/// The document's composite head CIDs.
async fn composite_heads(headstore: &NamespaceView, doc_short_id: u64) -> Result<Vec<Cid>> {
    let prefix = HeadstoreDocKey::field_prefix(doc_short_id, "C");
    let prefix_len = prefix.len();

    let mut iter = headstore
        .iterator(IterOptions::new().with_prefix(prefix).with_keys_only(true))
        .await
        .map_err(|error| storage_error("failed to iterate heads", error))?;

    let mut heads = Vec::new();
    while let Some(pair) = iter
        .next()
        .await
        .map_err(|error| storage_error("head iteration failed", error))?
    {
        let cid_str = String::from_utf8_lossy(&pair.key[prefix_len..]);
        if let Ok(cid) = Cid::from_str(&cid_str) {
            heads.push(cid);
        }
    }
    iter.close()
        .await
        .map_err(|error| storage_error("head iterator close failed", error))?;
    Ok(heads)
}

/// The head's commit priority, or `None` when its block cannot be read as a
/// composite. A head whose height is unknown is left out rather than guessed:
/// a guessed height would put the same commit at different sort keys on two
/// peers, which is the one thing the protocol cannot recover from.
async fn head_priority(blockstore: &NamespaceView, cid: &Cid) -> Result<Option<u64>> {
    let Some(bytes) = blockstore
        .get(&cid.to_bytes())
        .await
        .map_err(|error| storage_error("failed to read head block", error))?
    else {
        return Ok(None);
    };
    let Ok(block) = Block::from_dag_cbor(&bytes) else {
        return Ok(None);
    };
    if !matches!(block.delta, CrdtDelta::Composite(_)) {
        return Ok(None);
    }
    Ok(Some(block.delta.priority()))
}

#[cfg(test)]
#[path = "reconcile_source_tests.rs"]
mod reconcile_source_tests;
