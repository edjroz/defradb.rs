//! Document-level operations a scenario performs against a node: writing the
//! fixture, applying its updates, and reading back the state that `stateMatch`
//! and the block columns are computed from.

use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use super::scenario::DivergenceFixture;
use crate::EmbeddedNode;

pub(crate) const SDL: &str = "type BenchDoc { name: String value: Int }";
pub(crate) const COLLECTION: &str = "BenchDoc";

/// Write the fixture's seed documents and return their doc IDs in fixture
/// order.
pub(crate) async fn seed_docs(node: &EmbeddedNode, fixture: &DivergenceFixture) -> Vec<String> {
    let mut doc_ids = Vec::with_capacity(fixture.docs.len());
    for doc in &fixture.docs {
        let response = node
            .execute(&format!(
                r#"mutation {{ add_BenchDoc(input: {{name: "{}", value: {}}}) {{ _docID }} }}"#,
                doc.name, doc.value
            ))
            .await;
        doc_ids.push(created_doc_id(&response.data, &response.errors));
    }
    doc_ids
}

/// Apply the fixture's writer-only updates.
pub(crate) async fn apply_updates(
    node: &EmbeddedNode,
    doc_ids: &[String],
    fixture: &DivergenceFixture,
) {
    for update in &fixture.updates {
        let response = node
            .execute(&format!(
                r#"mutation {{ update_BenchDoc(docID: "{}", input: {{value: {}}}) {{ _docID }} }}"#,
                doc_ids[update.doc_index], update.value
            ))
            .await;
        assert!(
            response.errors.is_empty(),
            "update failed: {:?}",
            response.errors
        );
    }
}

/// Apply the fixture's updates `depth` times over, so every diverged document
/// carries a branch that many commits long.
///
/// Depth is built by repeated updates to the same documents, which is the shape
/// the Go depth-invariance bench uses. Each step writes a distinct value so no
/// update is a no-op that the CRDT could collapse.
pub(crate) async fn apply_deep_updates(
    node: &EmbeddedNode,
    doc_ids: &[String],
    fixture: &DivergenceFixture,
    depth: u32,
) {
    for step in 0..depth {
        for update in &fixture.updates {
            let response = node
                .execute(&format!(
                    r#"mutation {{ update_BenchDoc(docID: "{}", input: {{value: {}}}) {{ _docID }} }}"#,
                    doc_ids[update.doc_index],
                    update.value + i64::from(step)
                ))
                .await;
            assert!(
                response.errors.is_empty(),
                "deep update failed: {:?}",
                response.errors
            );
        }
    }
}

/// Wait until the node's own document state stops moving.
///
/// A reconciliation session snapshots the collection's head set once, at the
/// moment it starts. Measuring while the writer is still sealing commits would
/// make the recorded cost depend on how far the writes happened to have got,
/// which is a property of the run order and not of the protocol. Every measured
/// session is preceded by this.
pub(crate) async fn quiesce(node: &EmbeddedNode) {
    const POLL: std::time::Duration = std::time::Duration::from_millis(100);
    const STABLE_FOR: std::time::Duration = std::time::Duration::from_millis(750);
    const DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

    let started = std::time::Instant::now();
    let mut last = doc_values(node).await;
    let mut steady_since = std::time::Instant::now();
    while started.elapsed() < DEADLINE {
        tokio::time::sleep(POLL).await;
        let current = doc_values(node).await;
        if current != last {
            steady_since = std::time::Instant::now();
            last = current;
            continue;
        }
        if steady_since.elapsed() >= STABLE_FOR {
            return;
        }
    }
}

fn created_doc_id(data: &Option<JsonValue>, errors: &[impl std::fmt::Debug]) -> String {
    assert!(errors.is_empty(), "mutation failed: {errors:?}");
    data.as_ref()
        .and_then(|d| d.get("add_BenchDoc"))
        .and_then(|v| v.as_array())
        .and_then(|docs| docs.first())
        .and_then(|doc| doc.get("_docID"))
        .and_then(|id| id.as_str())
        .map(str::to_string)
        .unwrap_or_else(|| panic!("mutation returned no _docID: {data:?}"))
}

/// The node's document set as `docID -> value`, the basis for `stateMatch`.
pub(crate) async fn doc_values(node: &EmbeddedNode) -> BTreeMap<String, i64> {
    let response = node
        .execute(&format!("query {{ {COLLECTION} {{ _docID value }} }}"))
        .await;
    assert!(
        response.errors.is_empty(),
        "state query failed: {:?}",
        response.errors
    );
    response
        .data
        .as_ref()
        .and_then(|data| data.get(COLLECTION))
        .and_then(|docs| docs.as_array())
        .map(|docs| {
            docs.iter()
                .filter_map(|doc| {
                    let id = doc.get("_docID")?.as_str()?.to_string();
                    let value = doc.get("value")?.as_i64()?;
                    Some((id, value))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// Converged blockstore size, the same state measure the Go harness reported.
pub(crate) async fn block_stats(node: &EmbeddedNode) -> (u64, u64) {
    let Some(blockstore) = node.p2p_blockstore() else {
        return (0, 0);
    };
    let cids = blockstore.all_cids().await.expect("all_cids");
    let mut bytes = 0u64;
    for cid in &cids {
        bytes += blockstore
            .get_size(cid)
            .await
            .expect("get_size")
            .unwrap_or(0) as u64;
    }
    (cids.len() as u64, bytes)
}
