//! A document pushed to `/sync` must join its branchable collection's DAG.
//!
//! `BranchableSync` is the only question a peer can ask without already
//! knowing a document id, and it answers with the collection's heads. A
//! document merged from `/sync` that no collection commit reaches is stored and
//! queryable, and invisible to that peer forever.

use cid::Cid;
use db::merge::browser_sync::BrowserSyncEngine;
use db::merge::head_provider::DbHeadProvider;
use db::AutoCommitMutator;
use db::DB;
use defra_core::browser_sync::BrowserSyncDocument;
use defra_core::{Block, CrdtDelta};
use document::{DocID, Document};
use p2p::sync::DocumentHeadProvider;
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use std::collections::HashSet;
use std::sync::Arc;
use storage::RegolithStore;

const COLLECTION: &str = "DagConfig";
const COLLECTION_ID: &str = "col-dag-config";

fn branchable_schema() -> CollectionVersion {
    CollectionVersion::new(
        COLLECTION,
        "dag-config-v1",
        COLLECTION_ID,
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "device", FieldKind::string()),
            FieldDescription::new("3", "seq", FieldKind::int()),
        ],
    )
    .as_branchable()
}

async fn branchable_node() -> Arc<DB<RegolithStore>> {
    let db = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    db.create_collection(branchable_schema()).await.unwrap();
    db
}

fn command(device: &str, seq: i64) -> Document {
    let mut document = Document::new();
    document.set("device", device);
    document.set("seq", seq);
    document
}

/// What a browser node hands central: a document it authored elsewhere, as
/// blocks.
async fn authored_elsewhere(author: &Arc<DB<RegolithStore>>, doc_id: &str) -> BrowserSyncDocument {
    let sync = BrowserSyncEngine::new(author.clone());
    let document_ref = sync.document_ref(doc_id).await.unwrap().unwrap();
    sync.load_document(&document_ref).await.unwrap().unwrap()
}

async fn collection_heads(db: &Arc<DB<RegolithStore>>) -> Vec<Cid> {
    DbHeadProvider::new(db.clone())
        .get_collection_heads(COLLECTION_ID)
        .await
        .unwrap()
}

async fn load_block(db: &Arc<DB<RegolithStore>>, cid: &Cid) -> Block {
    let txn = db.new_txn(true).await.unwrap();
    let data = txn
        .blockstore()
        .unwrap()
        .get(&cid.to_bytes())
        .await
        .unwrap()
        .expect("a collection head names a stored block");
    Block::from_dag_cbor(&data).unwrap()
}

/// The single head, which must be a collection commit linking `root` and
/// built on `parents`.
async fn assert_head_reaches(db: &Arc<DB<RegolithStore>>, root: &str, parents: &[Cid]) -> Cid {
    let heads = collection_heads(db).await;
    assert_eq!(heads.len(), 1, "one writer, one head: {heads:?}");
    let head = load_block(db, &heads[0]).await;
    assert!(matches!(head.delta, CrdtDelta::Collection(_)));
    let linked: Vec<String> = head
        .links
        .iter()
        .flatten()
        .map(|link| link.link.to_string())
        .collect();
    assert_eq!(
        linked,
        vec![root.to_string()],
        "the head must reach the pushed root"
    );
    assert_eq!(head.heads.clone().unwrap_or_default(), parents);
    heads[0]
}

#[tokio::test]
async fn a_sync_write_into_an_empty_branchable_collection_makes_a_head() {
    let browser = branchable_node().await;
    let central = branchable_node().await;
    let created = AutoCommitMutator::new(browser.clone())
        .create(COLLECTION, command("sensor-7", 1))
        .await
        .unwrap();
    let pushed = authored_elsewhere(&browser, &created.doc_id.to_string()).await;

    assert!(collection_heads(&central).await.is_empty());
    BrowserSyncEngine::new(central.clone())
        .apply_document(&pushed, "browser")
        .await
        .unwrap();

    assert_head_reaches(&central, &pushed.roots[0], &[]).await;
}

#[tokio::test]
async fn a_sync_write_extends_a_collection_that_already_has_heads() {
    let browser = branchable_node().await;
    let central = branchable_node().await;
    AutoCommitMutator::new(central.clone())
        .create(COLLECTION, command("probe", 1))
        .await
        .unwrap();
    let local_heads = collection_heads(&central).await;
    assert_eq!(local_heads.len(), 1);

    let created = AutoCommitMutator::new(browser.clone())
        .create(COLLECTION, command("sensor-7", 2))
        .await
        .unwrap();
    let doc_id = created.doc_id.to_string();
    let pushed = authored_elsewhere(&browser, &doc_id).await;
    let central_sync = BrowserSyncEngine::new(central.clone());
    central_sync
        .apply_document(&pushed, "browser")
        .await
        .unwrap();
    let after_create = assert_head_reaches(&central, &pushed.roots[0], &local_heads).await;

    // The newest command for a device is an update to its document, and it
    // has to move the heads just as the first one did.
    let mut update = command("sensor-7", 3);
    update.set_id(DocID::from_string(&doc_id).unwrap());
    AutoCommitMutator::new(browser.clone())
        .update(COLLECTION, update, HashSet::from(["seq".to_string()]))
        .await
        .unwrap();
    let updated = authored_elsewhere(&browser, &doc_id).await;
    central_sync
        .apply_document(&updated, "browser")
        .await
        .unwrap();
    let after_update = assert_head_reaches(&central, &updated.roots[0], &[after_create]).await;

    // The same push again merges nothing, so it commits nothing.
    central_sync
        .apply_document(&updated, "browser")
        .await
        .unwrap();
    assert_eq!(collection_heads(&central).await, vec![after_update]);
}

/// `TxnBroadcaster` test double: captures every event it is handed.
struct CapturingBroadcaster {
    events: Arc<std::sync::Mutex<Vec<db::event::emission::TxnBroadcastEvent>>>,
}

#[async_trait::async_trait]
impl db::event::emission::TxnBroadcaster for CapturingBroadcaster {
    async fn broadcast_update(&self, event: db::event::emission::TxnBroadcastEvent) {
        self.events.lock().unwrap().push(event);
    }
}

#[tokio::test]
async fn the_collection_commit_is_announced_with_the_document() {
    let browser = branchable_node().await;
    let central = branchable_node().await;
    let created = AutoCommitMutator::new(browser.clone())
        .create(COLLECTION, command("sensor-7", 1))
        .await
        .unwrap();
    let pushed = authored_elsewhere(&browser, &created.doc_id.to_string()).await;

    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    BrowserSyncEngine::with_broadcaster(
        central.clone(),
        Arc::new(CapturingBroadcaster {
            events: events.clone(),
        }),
    )
    .apply_document(&pushed, "browser")
    .await
    .unwrap();

    let events: Vec<_> = events.lock().unwrap().drain(..).collect();
    assert_eq!(events.len(), 1);
    let (announced, _) = events[0]
        .collection_block
        .as_ref()
        .expect("a peer replicating the collection needs its commit, not just the document");
    assert_eq!(vec![*announced], collection_heads(&central).await);
}

/// A block that arrived by replication must not author a commit: the
/// collection commit travels with the document from the node the write
/// entered, and authoring a second one here would fork the collection DAG on
/// every peer that merged it.
#[tokio::test]
async fn a_replicated_merge_authors_no_collection_commit() {
    use blockstore::{Blockstore as _, DefraBlockstore};
    use db::merge::merge_handler::DbMergeHandler;
    use defra_core::merge::{BlockMetadata, MergeHandler, MergeOutcome};

    let browser = branchable_node().await;
    let central = branchable_node().await;
    let created = AutoCommitMutator::new(browser.clone())
        .create(COLLECTION, command("sensor-7", 1))
        .await
        .unwrap();
    let pushed = authored_elsewhere(&browser, &created.doc_id.to_string()).await;

    // The same blocks the replication path merges, with the metadata that path
    // supplies: no ingress claim on it.
    let blockstore = Arc::new(DefraBlockstore::new(central.store().clone(), true));
    let handler = DbMergeHandler::new(central.clone(), blockstore.clone());
    for block in &pushed.blocks {
        blockstore
            .put(
                &Cid::try_from(block.cid.as_str()).unwrap(),
                &hex::decode(&block.data).unwrap(),
            )
            .await
            .unwrap();
    }
    let root = Cid::try_from(pushed.roots[0].as_str()).unwrap();
    let root_data = hex::decode(
        &pushed
            .blocks
            .iter()
            .find(|block| block.cid == pushed.roots[0])
            .unwrap()
            .data,
    )
    .unwrap();
    let outcome = handler
        .handle_block(
            &root,
            &root_data,
            BlockMetadata::normal(
                &pushed.doc_id,
                &pushed.collection_id,
                "a-peer",
                Some("a-peer"),
                false,
            ),
        )
        .await
        .unwrap();

    assert!(matches!(outcome, MergeOutcome::Merged));
    assert!(
        collection_heads(&central).await.is_empty(),
        "only the node a write entered authors the collection commit"
    );
}

/// A collection that keeps no DAG of its own gets no commit, and the
/// announcement says so rather than carrying an empty one.
#[tokio::test]
async fn a_push_into_a_non_branchable_collection_writes_no_commit() {
    let plain = CollectionVersion::new(
        "PlainConfig",
        "plain-v1",
        "col-plain-config",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "device", FieldKind::string()),
            FieldDescription::new("3", "seq", FieldKind::int()),
        ],
    );
    let browser = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    let central = Arc::new(DB::new(RegolithStore::in_memory().unwrap()).unwrap());
    for db in [&browser, &central] {
        db.create_collection(plain.clone()).await.unwrap();
    }

    let created = AutoCommitMutator::new(browser.clone())
        .create("PlainConfig", command("sensor-7", 1))
        .await
        .unwrap();
    let sync = BrowserSyncEngine::new(browser.clone());
    let document_ref = sync
        .document_ref(&created.doc_id.to_string())
        .await
        .unwrap()
        .unwrap();
    let pushed = sync.load_document(&document_ref).await.unwrap().unwrap();

    let events = Arc::new(std::sync::Mutex::new(Vec::new()));
    BrowserSyncEngine::with_broadcaster(
        central.clone(),
        Arc::new(CapturingBroadcaster {
            events: events.clone(),
        }),
    )
    .apply_document(&pushed, "browser")
    .await
    .unwrap();

    let events: Vec<_> = events.lock().unwrap().drain(..).collect();
    assert_eq!(events.len(), 1);
    assert!(events[0].collection_block.is_none());
}
