use std::collections::BTreeSet;

use db::AutoCommitMutator;
use document::Document;
use p2p::reconcile::ItemSource;
use query::mutator::DocMutator;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use storage::backends::MemoryStore;

use super::*;

const COLLECTION: &str = "Note";

async fn database() -> Arc<DB<MemoryStore>> {
    let db = Arc::new(DB::from_arc(Arc::new(MemoryStore::new())).unwrap());
    db.create_collection(CollectionVersion::new(
        COLLECTION,
        "v1",
        "col-note",
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "body", FieldKind::string()),
        ],
    ))
    .await
    .unwrap();
    db
}

async fn add_notes(db: &Arc<DB<MemoryStore>>, bodies: &[&str]) {
    let mutator = AutoCommitMutator::new(db.clone());
    let docs = bodies
        .iter()
        .map(|body| {
            let mut doc = Document::new();
            doc.set("body", body.to_string());
            doc
        })
        .collect();
    mutator.create_many(COLLECTION, docs).await.unwrap();
}

fn item_ids(source: &MemorySource) -> BTreeSet<Vec<u8>> {
    (0..source.len())
        .map(|index| source.id(index).as_bytes().to_vec())
        .collect()
}

#[tokio::test]
async fn an_empty_collection_yields_an_empty_source() {
    let db = database().await;
    let source = DbReconcileSource::new(db)
        .snapshot(COLLECTION)
        .await
        .unwrap();
    assert!(source.is_empty());
}

#[tokio::test]
async fn an_unknown_collection_yields_an_empty_source() {
    let db = database().await;
    let source = DbReconcileSource::new(db)
        .snapshot("NoSuchCollection")
        .await
        .unwrap();
    assert!(
        source.is_empty(),
        "an unknown collection must read as empty, not fail, so a peer cannot probe for it"
    );
}

#[tokio::test]
async fn a_single_document_yields_its_one_head() {
    let db = database().await;
    add_notes(&db, &["only"]).await;

    let source = DbReconcileSource::new(db)
        .snapshot(COLLECTION)
        .await
        .unwrap();
    assert_eq!(source.len(), 1);
    assert!(
        Cid::try_from(source.id(0).as_bytes()).is_ok(),
        "an item identity must be a decodable head CID"
    );
}

#[tokio::test]
async fn every_document_contributes_a_head() {
    let db = database().await;
    add_notes(&db, &["a", "b", "c", "d"]).await;

    let source = DbReconcileSource::new(db)
        .snapshot(COLLECTION)
        .await
        .unwrap();
    assert_eq!(source.len(), 4);
    assert_eq!(item_ids(&source).len(), 4, "heads must be distinct");
}

#[tokio::test]
async fn items_are_in_ascending_sort_key_order() {
    let db = database().await;
    add_notes(&db, &["a", "b", "c", "d", "e", "f"]).await;

    let source = DbReconcileSource::new(db)
        .snapshot(COLLECTION)
        .await
        .unwrap();
    for index in 1..source.len() {
        assert!(
            source.key(index - 1) < source.key(index),
            "the engine requires a strictly ascending source"
        );
    }
}

#[tokio::test]
async fn the_collection_id_names_the_same_set_as_the_collection_name() {
    let db = database().await;
    add_notes(&db, &["a", "b"]).await;
    let provider = DbReconcileSource::new(db);

    let by_name = provider.snapshot(COLLECTION).await.unwrap();
    let by_id = provider.snapshot("col-note").await.unwrap();
    assert_eq!(item_ids(&by_name), item_ids(&by_id));
}

#[tokio::test]
async fn a_snapshot_does_not_see_writes_that_land_after_it() {
    let db = database().await;
    add_notes(&db, &["before"]).await;

    let snapshot = DbReconcileSource::new(db.clone())
        .snapshot(COLLECTION)
        .await
        .unwrap();
    add_notes(&db, &["after"]).await;

    assert_eq!(
        snapshot.len(),
        1,
        "a sealed snapshot must not grow under the session that holds it"
    );
    assert_eq!(
        DbReconcileSource::new(db)
            .snapshot(COLLECTION)
            .await
            .unwrap()
            .len(),
        2,
        "the next session must see the write"
    );
}

#[tokio::test]
async fn updating_a_document_moves_its_head() {
    let db = database().await;
    let mutator = AutoCommitMutator::new(db.clone());
    let mut doc = Document::new();
    doc.set("body", "first".to_string());
    let created = mutator.create(COLLECTION, doc).await.unwrap();

    let before = DbReconcileSource::new(db.clone())
        .snapshot(COLLECTION)
        .await
        .unwrap();

    let mut updated = created.document.clone();
    updated.set("body", "second".to_string());
    mutator
        .update(
            COLLECTION,
            updated,
            std::collections::HashSet::from(["body".to_string()]),
        )
        .await
        .unwrap();

    let after = DbReconcileSource::new(db)
        .snapshot(COLLECTION)
        .await
        .unwrap();
    assert_eq!(after.len(), 1, "a document still has exactly one head");
    assert_ne!(
        item_ids(&before),
        item_ids(&after),
        "an update must move the head, or reconciliation could not see it"
    );
}
