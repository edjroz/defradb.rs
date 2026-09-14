//! A P2P merge must hold the collection's read guard for the lifetime of the
//! transaction it writes, the same guard a truncate/delete/patch holds
//! exclusively (`DB::collection_write_guards`). Without it, a truncate can
//! delete a document's heads and keys out from under an in-flight merge.

use blockstore::DefraBlockstore;
use db::merge::merge_handler::DbMergeHandler;
use db::DB;
use defra_core::merge::{BlockMetadata, MergeHandler, MergeOutcome};
use document::Document;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use std::sync::Arc;
use storage::corekv::{IterOptions, Store};
use storage::RegolithStore;

fn branchable_schema() -> CollectionVersion {
    CollectionVersion::new(
        "Transcript",
        "v1",
        "col-transcript",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "body", FieldKind::string()),
        ],
    )
    .as_branchable()
}

async fn total_key_count(store: &Arc<RegolithStore>) -> usize {
    let txn = store.new_txn(true).await.unwrap();
    let mut iter = txn.iterator(IterOptions::new()).await.unwrap();
    let mut count = 0;
    while iter.next().await.unwrap().is_some() {
        count += 1;
    }
    count
}

#[tokio::test]
async fn merge_blocks_on_a_held_collection_write_guard() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let db = Arc::new(DB::from_arc(store.clone()).unwrap());
    db.create_collection(branchable_schema()).await.unwrap();

    let blockstore = Arc::new(DefraBlockstore::new(store.clone(), false));
    let handler = DbMergeHandler::new(db.clone(), blockstore.clone());

    let mut doc = Document::new();
    doc.set("body", "hello".to_string());
    let built = db::block::builder::build_blocks_from_document(&doc, "v1", &blockstore)
        .await
        .unwrap();

    let baseline = total_key_count(&store).await;

    // Stand in for a concurrent truncate/delete/patch, which holds this same
    // guard exclusively for the duration of its own write.
    let guards = db
        .collection_write_guards(std::iter::once("col-transcript".to_string()))
        .await
        .unwrap();

    let cid = built.cid;
    let block_data = built.block;
    let doc_id = built.doc_id;
    let task = tokio::spawn(async move {
        let metadata = BlockMetadata::normal(
            &doc_id,
            "col-transcript",
            "did:key:merge-guard-test",
            None,
            false,
        );
        handler.handle_block(&cid, &block_data, metadata).await
    });

    for _ in 0..64 {
        tokio::task::yield_now().await;
    }

    assert!(
        !task.is_finished(),
        "merge must block on the collection guard while a truncate-equivalent \
         write guard is held"
    );
    assert_eq!(
        total_key_count(&store).await,
        baseline,
        "merge must not write any head or document key while the collection \
         guard is held"
    );

    drop(guards);

    let outcome = task
        .await
        .expect("merge task panicked")
        .expect("merge should succeed once the guard is released");
    assert!(
        matches!(outcome, MergeOutcome::Merged),
        "expected the composite to merge, got {outcome:?}"
    );
    assert!(
        total_key_count(&store).await > baseline,
        "merge must land its head and document keys once unblocked"
    );
}
