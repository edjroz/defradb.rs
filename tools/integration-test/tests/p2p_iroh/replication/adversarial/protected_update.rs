use std::collections::HashMap;

use cid::Cid;
use integration_test::{users_schema_with_policy, TestCluster, USER_ACP_POLICY};
use serial_test::serial;

use super::blocks::{
    aged, author, genesis_parent, named, update, with_creator, Author, Parent, Signing,
};
use super::peer::HostilePeer;
use super::receiver::{describe, p2p_addrs, user_by_id, wait_for_user, wait_listening};

/// The receiver's own record of a document's genesis, as `_commits` reports it.
fn genesis_of(cluster: &TestCluster, owner: &Author, doc_id: &str) -> Parent {
    let commits = cluster
        .client(0)
        .query_with_identity(
            &format!(r#"query {{ _commits(docID: "{doc_id}") {{ cid fieldName }} }}"#),
            &owner.private_key_hex,
        )
        .expect("commits query");
    let mut composite = None;
    let mut fields = HashMap::new();
    for commit in commits["_commits"].as_array().expect("commits") {
        let cid: Cid = commit["cid"].as_str().expect("cid").parse().expect("CID");
        match commit["fieldName"].as_str() {
            Some("_C") | None => composite = Some(cid),
            Some(field) => {
                fields.insert(field.to_string(), cid);
            }
        }
    }
    genesis_parent(doc_id, composite.expect("a composite commit"), fields)
}

#[tokio::test(flavor = "multi_thread")]
#[serial]
#[ignore = "local ACP merges do not check the signer's Update permission"]
async fn update_to_another_owners_protected_document_is_refused() {
    let owner = author(0x11);
    let attacker = author(0x22);

    let cluster = TestCluster::builder()
        .rust_nodes(1)
        .with_iroh_transport()
        .with_acp_local()
        .build()
        .await
        .expect("node starts");
    wait_listening(&cluster, 1).await;
    let node = cluster.client(0);

    let policy = node
        .acp_policy_add(USER_ACP_POLICY, &owner.private_key_hex)
        .expect("policy");
    let policy_id = policy["PolicyID"]
        .as_str()
        .or_else(|| policy["policyID"].as_str())
        .expect("PolicyID");
    node.schema_add_with_identity(&users_schema_with_policy(policy_id), &owner.private_key_hex)
        .expect("schema");
    let collection = describe(&node, "User");

    let created = node
        .query_with_identity(
            r#"mutation { add_User(input: {name: "Alice", age: 30}) { _docID } }"#,
            &owner.private_key_hex,
        )
        .expect("owner creates");
    let doc_id = created["add_User"][0]["_docID"]
        .as_str()
        .expect("docID")
        .to_string();
    let parent = genesis_of(&cluster, &owner, &doc_id);

    let peer = HostilePeer::dial(&p2p_addrs(&cluster, 0)).await;

    // Signed by the attacker, while the envelope names the owner as creator.
    let hijack = with_creator(
        &update(
            &parent,
            &aged(666),
            &collection.version_id,
            &attacker,
            Signing::EveryBlock,
        ),
        &owner.did,
    );
    peer.push(&hijack, &collection.collection_id).await;

    let sanctioned = update(
        &parent,
        &named("Alice (owner)"),
        &collection.version_id,
        &owner,
        Signing::EveryBlock,
    );
    peer.push(&sanctioned, &collection.collection_id).await;
    wait_for_user(&node, Some(&owner.private_key_hex), &doc_id, |row| {
        row["name"] == "Alice (owner)"
    })
    .await;

    let row = user_by_id(&node, Some(&owner.private_key_hex), &doc_id).expect("owner reads");
    assert_eq!(
        row["age"], 30,
        "a peer's update signed by a non-writer changed the owner's protected document: {row}"
    );
}
