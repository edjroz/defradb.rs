//! A P2P merge must hold the collection's read guard for the lifetime of the
//! transaction it writes, the same guard a truncate/delete/patch holds
//! exclusively (`DB::collection_write_guards`). Without it, a truncate can
//! delete a document's heads and keys out from under an in-flight merge.
//!
//! The guard wait is observed through the trace events
//! `DB::collection_read_guard` emits, so the test proves the merge reached
//! the lock and was released by the drop, not merely that it was slow.
//!
//! The recorder is the process-wide subscriber, not a thread-local one:
//! tracing caches a callsite's interest once per process, computed on the
//! thread that hits it first, and with a single registered dispatcher that
//! computation uses that thread's default. A thread-local recorder therefore
//! misses any callsite another test in this binary reaches first. Events are
//! keyed by collection id so parallel tests cannot be mistaken for this one.

use blockstore::DefraBlockstore;
use db::merge::merge_handler::DbMergeHandler;
use db::DB;
use defra_core::merge::{BlockMetadata, MergeHandler, MergeOutcome};
use document::Document;
use schema::{CollectionVersion, FieldDescription, FieldKind};
use std::sync::{Arc, Mutex, OnceLock};
use storage::corekv::{IterOptions, Store};
use storage::RegolithStore;

const COLLECTION_ID: &str = "col-guard-race";
const GUARD_TARGET: &str = "db::collection::locks";
const WAITING: &str = "waiting for the collection guard";
const HOLDING: &str = "holding the collection guard";

/// Every guard event emitted anywhere in this process, as (collection id,
/// message).
struct Recorder(Mutex<Vec<(String, String)>>);

impl Recorder {
    fn count(&self, message: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(collection, seen)| collection == COLLECTION_ID && seen == message)
            .count()
    }
}

#[derive(Default)]
struct Fields {
    collection_id: String,
    message: String,
}

impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            "collection_id" => {
                self.collection_id = format!("{value:?}").trim_matches('"').to_string()
            }
            _ => {}
        }
    }
}

impl tracing::Subscriber for Recorder {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.target() == GUARD_TARGET
    }
    fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
        tracing::span::Id::from_u64(1)
    }
    fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
    fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
    fn event(&self, event: &tracing::Event<'_>) {
        let mut fields = Fields::default();
        event.record(&mut fields);
        self.0
            .lock()
            .unwrap()
            .push((fields.collection_id, fields.message));
    }
    fn enter(&self, _: &tracing::span::Id) {}
    fn exit(&self, _: &tracing::span::Id) {}
}

fn recorder() -> Arc<Recorder> {
    static RECORDER: OnceLock<Arc<Recorder>> = OnceLock::new();
    RECORDER
        .get_or_init(|| {
            let recorder = Arc::new(Recorder(Mutex::new(Vec::new())));
            tracing::subscriber::set_global_default(recorder.clone())
                .expect("this test binary installs no other global subscriber");
            recorder
        })
        .clone()
}

fn branchable_schema() -> CollectionVersion {
    CollectionVersion::new(
        "GuardRace",
        "guard-race-v1",
        COLLECTION_ID,
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
    let recorder = recorder();
    let store = Arc::new(RegolithStore::in_memory().unwrap());
    let db = Arc::new(DB::from_arc(store.clone()).unwrap());
    db.create_collection(branchable_schema()).await.unwrap();

    let blockstore = Arc::new(DefraBlockstore::new(store.clone(), false));
    let handler = DbMergeHandler::new(db.clone(), blockstore.clone());

    let mut doc = Document::new();
    doc.set("body", "hello".to_string());
    let built = db::block::builder::build_blocks_from_document(&doc, "guard-race-v1", &blockstore)
        .await
        .unwrap();

    let baseline = total_key_count(&store).await;

    // Stand in for a concurrent truncate/delete/patch, which holds this same
    // guard exclusively for the duration of its own write.
    let guards = db
        .collection_write_guards(std::iter::once(COLLECTION_ID.to_string()))
        .await
        .unwrap();

    let cid = built.cid;
    let block_data = built.block;
    let doc_id = built.doc_id;
    let mut task = tokio::spawn(async move {
        let metadata = BlockMetadata::normal(
            &doc_id,
            COLLECTION_ID,
            "did:key:merge-guard-test",
            None,
            false,
        );
        handler.handle_block(&cid, &block_data, metadata).await
    });

    // Yield until the merge is observably waiting on the guard, then keep
    // yielding for a bounded window in which it must not acquire it.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while recorder.count(WAITING) == 0 {
        if task.is_finished() {
            let outcome = (&mut task).await;
            panic!("the merge ended without waiting on the collection guard: {outcome:?}");
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the merge never reached the collection guard"
        );
        tokio::task::yield_now().await;
    }
    for _ in 0..64 {
        tokio::task::yield_now().await;
    }

    assert_eq!(
        recorder.count(WAITING),
        1,
        "the merge must wait on the collection guard exactly once"
    );
    assert_eq!(
        recorder.count(HOLDING),
        0,
        "the merge must not acquire the collection guard while a \
         truncate-equivalent write guard is held"
    );
    assert!(!task.is_finished());
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
    assert_eq!(
        recorder.count(HOLDING),
        1,
        "the merge must acquire the collection guard once the drop released it"
    );
    assert!(
        matches!(outcome, MergeOutcome::Merged),
        "expected the composite to merge, got {outcome:?}"
    );
    assert!(
        total_key_count(&store).await > baseline,
        "merge must land its head and document keys once unblocked"
    );
}
