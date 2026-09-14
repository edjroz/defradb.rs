use bytes::Bytes;
use cid::Cid;
use events::{Message, Update};
use storage::corekv::Store;

use super::BrowserSyncError;
use crate::write::autocommit::helpers::write_branchable_collection_block;

/// Append a branchable collection's commit for each composite root a `/sync`
/// push merged, returned in the order of `merged_roots`.
///
/// A local mutation writes this commit alongside the document; a fragment
/// merged from `/sync` is a document this node is the first to hold, so no
/// peer will ever send the commit for it. Without one the document is outside
/// the collection DAG, and a peer that discovers documents by asking for the
/// collection's heads can never reach it.
///
/// Every root goes in one transaction, so a push is either fully in the DAG or
/// not in it.
pub(super) async fn write_collection_commits<S: Store + 'static>(
    db: &crate::DB<S>,
    doc_id: &str,
    collection_id: &str,
    merged_roots: &[Cid],
) -> Result<Vec<(Cid, Bytes)>, BrowserSyncError> {
    let Some(collection) = db
        .find_collection_by_id(collection_id)
        .map_err(|error| BrowserSyncError::Storage(error.to_string()))?
    else {
        return Ok(Vec::new());
    };
    if !collection.schema().is_branchable || merged_roots.is_empty() {
        return Ok(Vec::new());
    }

    let signing_config = defra_core::signing::get_signing_config();
    let txn = db
        .new_txn(false)
        .await
        .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
    let mut commits = Vec::with_capacity(merged_roots.len());
    {
        let blockstore = txn
            .blockstore()
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        let headstore = txn
            .headstore()
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;
        for root in merged_roots {
            let commit = write_branchable_collection_block(
                db,
                collection.name(),
                &collection,
                &blockstore,
                &headstore,
                *root,
                signing_config.as_ref(),
            )
            .await
            .map_err(|error| BrowserSyncError::Storage(error.to_string()))?
            .expect("a branchable collection always yields a collection block");
            commits.push(commit);
        }
    }
    txn.commit()
        .await
        .map_err(|error| BrowserSyncError::Storage(error.to_string()))?;

    if let Some(bus) = db.event_bus() {
        for (cid, block) in &commits {
            bus.publish(Message::update(Update::new_with_subject_doc_id(
                String::new(),
                doc_id.to_string(),
                *cid,
                collection_id.to_string(),
                block.clone(),
                false,
                false,
            )));
        }
    }

    Ok(commits)
}
