//! `DB::create_index`: the documents that already exist are indexed, and the
//! collection write guard is held while they are.

use std::sync::Arc;

use db::{AutoCommitMutator, DB};
use document::{Document, NormalValue};
use query::DocMutator;
use schema::{
    CollectionVersion, FieldDescription, FieldKind, IndexKind, IndexedFieldDescription,
    OrderedIndexDescription,
};
use storage::corekv::{IterOptions, Iterator};
use storage::keys::datastore::IndexDataStoreKey;
use storage::RegolithStore;

use crate::common::guard_events::{recorder, WRITE_HOLDING, WRITE_WAITING};

fn users_schema(name: &str, version_id: &str, collection_id: &str) -> CollectionVersion {
    CollectionVersion::new(
        name,
        version_id,
        collection_id,
        vec![
            FieldDescription::new("1", "_docID", FieldKind::doc_id()),
            FieldDescription::new("2", "name", FieldKind::string()),
        ],
    )
}

fn by_name() -> (Vec<IndexedFieldDescription>, IndexKind) {
    (
        vec![IndexedFieldDescription {
            name: "name".to_string(),
            descending: false,
        }],
        IndexKind::Ordered(OrderedIndexDescription { unique: false }),
    )
}

async fn create_user(mutator: &AutoCommitMutator<RegolithStore>, collection: &str, name: &str) {
    let mut doc = Document::new();
    doc.set("name", NormalValue::String(name.to_owned()));
    mutator.create(collection, doc).await.unwrap();
}

async fn index_entries(db: &DB<RegolithStore>, collection: &str, index_id: u32) -> usize {
    let short_id = db
        .require_collection(collection)
        .unwrap()
        .schema()
        .resolved_root_id();
    let txn = db.new_txn(true).await.unwrap();
    let datastore = txn.datastore().unwrap();
    let mut iter = datastore
        .iterator(
            IterOptions::new().with_prefix(IndexDataStoreKey::index_prefix(short_id, index_id)),
        )
        .await
        .unwrap();
    let mut count = 0;
    while iter.next().await.unwrap().is_some() {
        count += 1;
    }
    count
}

#[tokio::test]
async fn create_index_indexes_the_documents_that_already_exist() {
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let db = Arc::new(DB::from_arc(store).unwrap());
    db.create_collection(users_schema(
        "Backfilled",
        "backfilled-v1",
        "col-index-backfill",
    ))
    .await
    .unwrap();
    let mutator = AutoCommitMutator::new(db.clone());
    for name in ["ada", "grace", "linus"] {
        create_user(&mutator, "Backfilled", name).await;
    }

    let (fields, kind) = by_name();
    let index = db
        .create_index("Backfilled", Some("by_name"), fields, kind)
        .await
        .unwrap();

    // A completed backfill clears its action; only an errored or running one
    // is listed.
    let actions = db.list_index_actions("col-index-backfill").await.unwrap();
    assert!(
        !actions.contains_key(&index.id),
        "the backfill did not complete: {:?}",
        actions.get(&index.id)
    );
    assert_eq!(index_entries(&db, "Backfilled", index.id).await, 3);
    assert!(
        db.require_collection("Backfilled")
            .unwrap()
            .get_indexes()
            .iter()
            .any(|existing| existing.name == "by_name"),
        "the definition must be on the cached collection"
    );
}

/// Stand in for a truncate, delete or patch by holding the collection write
/// guard; index creation must wait on it, write nothing meanwhile, and land
/// once it is released. The guard events are counted per collection: the
/// test's own acquisition is the first waiting and holding pair.
#[tokio::test]
async fn create_index_waits_for_the_collection_write_guard() {
    const COLLECTION_ID: &str = "col-index-guard";
    let recorder = recorder();
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let db = Arc::new(DB::from_arc(store).unwrap());
    db.create_collection(users_schema("Guarded", "guarded-v1", COLLECTION_ID))
        .await
        .unwrap();
    create_user(&AutoCommitMutator::new(db.clone()), "Guarded", "ada").await;

    let guards = db
        .collection_write_guards(std::iter::once(COLLECTION_ID.to_string()))
        .await
        .unwrap();
    assert_eq!(recorder.count(COLLECTION_ID, WRITE_HOLDING), 1);

    let creator = db.clone();
    let mut task = tokio::spawn(async move {
        let (fields, kind) = by_name();
        creator
            .create_index("Guarded", Some("by_name"), fields, kind)
            .await
    });

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while recorder.count(COLLECTION_ID, WRITE_WAITING) < 2 {
        if task.is_finished() {
            let outcome = (&mut task).await;
            panic!("index creation ended without waiting on the collection guard: {outcome:?}");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "index creation never reached the collection guard"
        );
        tokio::task::yield_now().await;
    }
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }

    assert_eq!(
        recorder.count(COLLECTION_ID, WRITE_HOLDING),
        1,
        "index creation must not acquire the collection guard while it is held"
    );
    assert!(!task.is_finished());
    assert!(
        db.require_collection("Guarded")
            .unwrap()
            .get_indexes()
            .is_empty(),
        "no definition may land while the guard is held"
    );

    drop(guards);

    let index = task
        .await
        .expect("index creation panicked")
        .expect("index creation should succeed once the guard is released");
    assert_eq!(recorder.count(COLLECTION_ID, WRITE_HOLDING), 2);
    assert_eq!(index_entries(&db, "Guarded", index.id).await, 1);
}
