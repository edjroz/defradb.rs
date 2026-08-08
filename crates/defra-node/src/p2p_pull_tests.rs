//! Pull-side (`sync_documents`) convergence.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use super::p2p_tests::{
    init_tracing, test_p2p_config, wait_for_connected_peer, wait_for_listen_addr,
};
use super::EmbeddedNode;

const SDL: &str = "type Bulk { name: String value: Int }";
const COLLECTION: &str = "Bulk";

async fn seed_docs(node: &EmbeddedNode, count: usize) -> Vec<String> {
    let mut doc_ids = Vec::with_capacity(count);
    for index in 0..count {
        let response = node
            .execute(&format!(
                r#"mutation {{ add_{COLLECTION}(input: {{name: "doc-{index:06}", value: {index}}}) {{ _docID }} }}"#
            ))
            .await;
        assert!(
            response.errors.is_empty(),
            "seed mutation returned errors: {:?}",
            response.errors
        );
        let doc_id = response
            .data
            .as_ref()
            .and_then(|data| data.get(format!("add_{COLLECTION}")))
            .and_then(|value| value.as_array())
            .and_then(|items| items.first())
            .and_then(|item| item.get("_docID"))
            .and_then(|value| value.as_str())
            .expect("seed mutation returned a _docID")
            .to_string();
        doc_ids.push(doc_id);
    }
    doc_ids
}

async fn doc_ids_present(node: &EmbeddedNode) -> BTreeSet<String> {
    let response = node
        .execute(&format!("query {{ {COLLECTION} {{ _docID }} }}"))
        .await;
    assert!(
        response.errors.is_empty(),
        "query returned errors: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get(COLLECTION))
        .and_then(|docs| docs.as_array())
        .map(|docs| {
            docs.iter()
                .filter_map(|doc| doc.get("_docID").and_then(|value| value.as_str()))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

/// Two connected nodes, writer seeded, reader empty. Returns the pair plus the
/// writer's document IDs.
async fn connected_pair(doc_count: usize) -> (EmbeddedNode, EmbeddedNode, Vec<String>) {
    init_tracing();

    let writer = EmbeddedNode::builder()
        .with_p2p(test_p2p_config())
        .build()
        .await
        .expect("build writer");
    let reader = EmbeddedNode::builder()
        .with_p2p(test_p2p_config())
        .build()
        .await
        .expect("build reader");

    writer.add_schema(SDL).await.expect("schema on writer");
    reader.add_schema(SDL).await.expect("schema on reader");

    let doc_ids = seed_docs(&writer, doc_count).await;

    let writer_addr = wait_for_listen_addr(&writer).await;
    reader
        .p2p()
        .expect("reader p2p")
        .connect_peer(&writer_addr)
        .await
        .expect("connect reader -> writer");
    wait_for_connected_peer(&writer).await;
    wait_for_connected_peer(&reader).await;

    (writer, reader, doc_ids)
}

/// Drive `sync_documents` for the documents the reader is still missing until
/// it holds `expected` or the deadline passes.
async fn pull_until_converged(
    reader: &EmbeddedNode,
    expected: &BTreeSet<String>,
    budget: Duration,
) -> BTreeSet<String> {
    let reader_p2p = reader.p2p().expect("reader p2p");
    let deadline = Instant::now() + budget;
    let mut present = BTreeSet::new();
    while Instant::now() < deadline {
        let pending: Vec<String> = expected.difference(&present).cloned().collect();
        reader_p2p
            .sync_documents(COLLECTION, pending)
            .await
            .expect("sync_documents");
        present = doc_ids_present(reader).await;
        if &present == expected {
            break;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    present
}

/// Neither node subscribes to the collection, so `sync_documents` is the only
/// path that can move a document: this fails outright when the serving node
/// answers DocSync with no heads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn doc_sync_pull_delivers_documents_without_a_subscription() {
    let (writer, reader, doc_ids) = connected_pair(2).await;
    let expected: BTreeSet<String> = doc_ids.into_iter().collect();

    let present = pull_until_converged(&reader, &expected, Duration::from_secs(30)).await;

    writer.shutdown().await;
    reader.shutdown().await;

    assert_eq!(present, expected, "DocSync pull delivered no documents");
}

/// A cold reader pulling a collection an order of magnitude larger must reach
/// the writer's document set, not a prefix of it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_pull_of_a_large_collection_converges() {
    let (writer, reader, doc_ids) = connected_pair(200).await;
    let expected: BTreeSet<String> = doc_ids.into_iter().collect();

    for node in [&writer, &reader] {
        node.p2p()
            .expect("p2p")
            .add_collections(vec![COLLECTION.to_string()])
            .await
            .expect("subscribe collection");
    }

    let present = pull_until_converged(&reader, &expected, Duration::from_secs(240)).await;
    let missing = expected.difference(&present).count();

    writer.shutdown().await;
    reader.shutdown().await;

    assert_eq!(
        missing,
        0,
        "reader is short {missing} of {} documents",
        expected.len()
    );
}
