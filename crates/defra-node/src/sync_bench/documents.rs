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
