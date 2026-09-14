use bytes::Bytes;
use cid::Cid;
use datastore::NamespaceView;
use storage::corekv::Store;

use super::composite::{CompositeMergeContext, CompositeMergeState};
use super::{DbMergeHandler, MergeError};
use crate::block::builder::write_collection_block;

impl<S: Store, B: blockstore::Blockstore> DbMergeHandler<S, B> {
    /// Write the branchable collection's commit for a merged composite, in the
    /// merge's own transaction, when the caller is an ingress that authors it.
    ///
    /// Returns the collection's short id alongside the commit: the append
    /// superseded the heads it was built on, and those keys are reclaimed once
    /// the transaction carrying the commit has landed.
    pub(crate) async fn author_collection_commit(
        &self,
        blockstore: &NamespaceView,
        headstore: &NamespaceView,
        context: &CompositeMergeContext<'_, '_>,
        state: &CompositeMergeState,
    ) -> Result<Option<(u32, (Cid, Bytes))>, MergeError> {
        if context.metadata.authored_collection_commit.is_none() || !state.is_branchable {
            return Ok(None);
        }
        let Some(collection) = context.collection.as_ref() else {
            return Ok(None);
        };

        let commit = write_collection_block(
            blockstore,
            headstore,
            collection.resolved_root_id(),
            collection.version_id(),
            *context.cid,
            defra_core::signing::get_signing_config().as_ref(),
        )
        .await
        .map_err(MergeError::Storage)?;

        Ok(Some((collection.resolved_root_id(), commit)))
    }
}
