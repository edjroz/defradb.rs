//! Index creation: the definition, the action that tracks it, and the
//! backfill of every document the collection already holds, under the
//! collection's write guard.

use datastore::NamespaceView;
use schema::{IndexDescription, IndexKind, IndexedFieldDescription};
use storage::corekv::{Key, Store};
use storage::keys::systemstore::{CollectionKey, CollectionNameKey};

use crate::collection::Collection;
use crate::error::{Error, Result};
use crate::index::IndexManager;
use crate::{BackfillSource, DB};

impl<S: Store> DB<S> {
    /// Create an index of `kind` over `fields` on `collection_name`, then
    /// index every document the collection already holds. A missing or empty
    /// `name` is generated.
    ///
    /// The collection's write guard, the one a truncate or a patch holds, is
    /// held from the definition through the backfill: a write landing between
    /// the backfill's scan and its commit would leave an entry for a value the
    /// document no longer has, and a document created during the backfill
    /// would be missed at any isolation level. The backfill's outcome is
    /// recorded on the collection's `BACKFILL_INDEX` action; a failed backfill
    /// leaves the definition in place and the failure on the action.
    pub async fn create_index(
        &self,
        collection_name: &str,
        name: Option<&str>,
        fields: Vec<IndexedFieldDescription>,
        kind: IndexKind,
    ) -> Result<IndexDescription> {
        let collection = self.require_collection(collection_name)?;
        let collection_id = collection.collection_id().to_string();
        let _guards = self
            .collection_write_guards(std::iter::once(collection_id.clone()))
            .await?;

        let txn = self.new_txn(false).await?;
        let (index, lease) = {
            let datastore = txn.datastore()?;
            let systemstore = txn.systemstore()?;
            let mut manager = IndexManager::from_collection(
                collection.schema().resolved_root_id(),
                collection.schema(),
            )?;
            let index = manager
                .create_index_of_kind(
                    &datastore,
                    collection_name,
                    name.unwrap_or("").to_string(),
                    fields,
                    kind,
                    &collection.schema().fields,
                )
                .await?;
            let mut schema = collection.schema().clone();
            schema.indexes.push(index.clone());
            let data = serde_json::to_vec(&schema).map_err(|error| {
                Error::collection_schema_json(
                    format!("failed to serialize schema for collection '{collection_name}'"),
                    error,
                )
            })?;
            systemstore
                .set(&CollectionKey::new(&schema.version_id).bytes(), &data)
                .await?;
            systemstore
                .set(
                    &CollectionNameKey::new(collection_name).bytes(),
                    schema.version_id.as_bytes(),
                )
                .await?;
            let lease = self
                .stage_action(
                    &systemstore,
                    &collection_id,
                    defra_core::Action::BACKFILL_INDEX,
                    &index.id.to_string(),
                )
                .await?;
            (index, lease)
        };
        txn.commit().await?;
        self.publish_started_action(&lease);
        self.reload_cache().await?;

        match self.backfill_index(collection_name, &index.name).await {
            Ok(()) => self.complete_action(lease).await?,
            Err(error) => self.fail_action(lease, &error.to_string()).await?,
        }
        self.reload_cache().await?;
        Ok(index)
    }

    async fn backfill_index(&self, collection_name: &str, index_name: &str) -> Result<()> {
        let collection = self.require_collection(collection_name)?;
        let txn = self.new_txn(false).await?;
        let datastore = txn.datastore()?;
        let systemstore = txn.systemstore()?;
        let result = backfill_in(&collection, &datastore, &systemstore, index_name).await;
        // A view holds a reference to the transaction, and a commit refuses
        // to run while one is alive.
        drop((datastore, systemstore));
        if let Err(error) = result {
            txn.discard()?;
            return Err(error);
        }
        txn.commit().await?;
        self.reindex_collection_with_migrations(collection_name)
            .await
    }
}

async fn backfill_in(
    collection: &Collection,
    datastore: &NamespaceView,
    systemstore: &NamespaceView,
    index_name: &str,
) -> Result<()> {
    let manager =
        IndexManager::from_collection(collection.schema().resolved_root_id(), collection.schema())?;
    let mut source =
        BackfillSource::open(collection.clone(), datastore.clone(), systemstore.clone()).await?;
    manager
        .bulk_index_from(datastore, index_name, &mut source, collection.schema())
        .await?;
    Ok(())
}
