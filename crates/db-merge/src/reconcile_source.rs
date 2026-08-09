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
//! # Why the headstore is read in one pass
//!
//! The headstore is keyed by document, not by collection, so the obvious read is
//! one prefix scan per document. That is what this did, and it made a snapshot
//! quadratic: opening a store iterator costs time proportional to the whole
//! store, so `n` of them cost `n` times that. Measured at 0.81, 1.60, 3.22 and
//! 6.63 ms per item at n = 250, 500, 1,000 and 2,000 — doubling with every
//! doubling of the collection.
//!
//! One scan over the whole document-head keyspace, filtered against the
//! collection's document set, pays for one iterator instead of `n`. The price is
//! reading head keys belonging to other collections; at ~0.5 µs per key against
//! ~3 ms per iterator open, that trade is not close at any collection size this
//! campaign measured.
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

use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use cid::Cid;
use datastore::NamespaceView;
use defra_core::{Block, CrdtDelta};
use storage::corekv::{IterOptions, Store};
use storage::keys::doc_id_index::{decode_doc_short_id, decode_doc_short_id_prefix};
use storage::keys::document::DOC_KEY_PREFIX;
use storage::keys::headstore::HEADSTORE_DOC_PREFIX;

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

        let documents = collection_doc_short_ids(&datastore, &collection_id).await?;
        let mut items = Vec::new();
        for cid in composite_heads(&headstore, &documents).await? {
            let Some(priority) = head_priority(&blockstore, &cid).await? else {
                continue;
            };
            items.push(Item::new(priority, ItemId::new(cid.to_bytes())));
        }

        let _ = txn.discard();
        MemorySource::new(items).map_err(|error| storage_error("duplicate head", error))
    }
}

/// Every document short ID in the collection, from the document keyspace.
async fn collection_doc_short_ids(
    datastore: &NamespaceView,
    collection_id: &str,
) -> Result<HashSet<u64>> {
    let mut prefix = DOC_KEY_PREFIX.to_vec();
    prefix.extend_from_slice(collection_id.as_bytes());
    prefix.push(b'/');
    let prefix_len = prefix.len();

    let mut iter = datastore
        .iterator(IterOptions::new().with_prefix(prefix).with_keys_only(true))
        .await
        .map_err(|error| storage_error("failed to iterate documents", error))?;

    let mut ids = HashSet::new();
    while let Some(pair) = iter
        .next()
        .await
        .map_err(|error| storage_error("document iteration failed", error))?
    {
        if let Ok(doc_short_id) = decode_doc_short_id(&pair.key[prefix_len..]) {
            ids.insert(doc_short_id);
        }
    }
    iter.close()
        .await
        .map_err(|error| storage_error("document iterator close failed", error))?;
    Ok(ids)
}

/// The composite head CIDs of the named documents, in one pass over the
/// document-head keyspace.
///
/// The field component is matched exactly against `C`, so a field whose name
/// merely starts with a `C` cannot contribute a head; only the composite head
/// carries the commit the whole document is identified by.
async fn composite_heads(headstore: &NamespaceView, documents: &HashSet<u64>) -> Result<Vec<Cid>> {
    if documents.is_empty() {
        return Ok(Vec::new());
    }

    let mut iter = headstore
        .iterator(
            IterOptions::new()
                .with_prefix(HEADSTORE_DOC_PREFIX.to_vec())
                .with_keys_only(true),
        )
        .await
        .map_err(|error| storage_error("failed to iterate heads", error))?;

    let mut heads = Vec::new();
    while let Some(pair) = iter
        .next()
        .await
        .map_err(|error| storage_error("head iteration failed", error))?
    {
        if let Some(cid) = composite_head_of(&pair.key, documents) {
            heads.push(cid);
        }
    }
    iter.close()
        .await
        .map_err(|error| storage_error("head iterator close failed", error))?;
    Ok(heads)
}

/// The head CID a `/d/{doc}/C/{cid}` key names, when its document is one of
/// `documents`.
fn composite_head_of(key: &[u8], documents: &HashSet<u64>) -> Option<Cid> {
    let rest = key.strip_prefix(HEADSTORE_DOC_PREFIX)?;
    let (rest, doc_short_id) = decode_doc_short_id_prefix(rest).ok()?;
    if !documents.contains(&doc_short_id) {
        return None;
    }
    let cid = rest.strip_prefix(b"/C/")?;
    Cid::from_str(&String::from_utf8_lossy(cid)).ok()
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
