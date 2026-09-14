//! A local-ACP iroh node with a policy-bound `User` collection, dialled by a
//! hostile peer.

use std::collections::HashMap;

use cid::Cid;
use integration_test::{users_schema_with_policy, DefraClient, TestCluster, USER_ACP_POLICY};

use super::blocks::{genesis_parent, named, update, Author, Parent, Signing};
use super::peer::HostilePeer;
use super::receiver::{describe, p2p_addrs, wait_for_user, wait_listening, Collection};

pub struct ProtectedNode {
    pub peer: HostilePeer,
    pub collection: Collection,
    pub cluster: TestCluster,
}

impl ProtectedNode {
    /// `owner` adds the policy and the collection bound to it.
    pub async fn start(owner: &Author) -> Self {
        let cluster = TestCluster::builder()
            .rust_nodes(1)
            .with_iroh_transport()
            .with_acp_local()
            .build()
            .await
            .expect("node starts");
        wait_listening(&cluster, 1).await;
        let client = cluster.client(0);

        let policy = client
            .acp_policy_add(USER_ACP_POLICY, &owner.private_key_hex)
            .expect("policy");
        let policy_id = policy["PolicyID"]
            .as_str()
            .or_else(|| policy["policyID"].as_str())
            .expect("PolicyID");
        client
            .schema_add_with_identity(&users_schema_with_policy(policy_id), &owner.private_key_hex)
            .expect("schema");
        let collection = describe(&client, "User");
        let peer = HostilePeer::dial(&p2p_addrs(&cluster, 0)).await;

        Self {
            peer,
            collection,
            cluster,
        }
    }

    pub fn client(&self) -> DefraClient {
        self.cluster.client(0)
    }

    /// A document `owner` creates on the node itself, so the node's ACP
    /// registers `owner` as its owner.
    pub fn create(&self, owner: &Author, name: &str, age: i64) -> (String, Parent) {
        let created = self
            .client()
            .query_with_identity(
                &format!(
                    r#"mutation {{ add_User(input: {{name: "{name}", age: {age}}}) {{ _docID }} }}"#
                ),
                &owner.private_key_hex,
            )
            .expect("owner creates");
        let doc_id = created["add_User"][0]["_docID"]
            .as_str()
            .expect("docID")
            .to_string();
        let parent = self.genesis_of(owner, &doc_id);
        (doc_id, parent)
    }

    /// The positive control: the same peer pushes an owner-signed rename,
    /// and it reads back.
    pub async fn rename_as_owner(&self, owner: &Author, doc_id: &str, parent: &Parent, name: &str) {
        self.rename_as_owner_via(&self.peer, owner, doc_id, parent, name)
            .await;
    }

    pub async fn rename_as_owner_via(
        &self,
        peer: &HostilePeer,
        owner: &Author,
        doc_id: &str,
        parent: &Parent,
        name: &str,
    ) {
        let sanctioned = update(
            parent,
            &named(name),
            &self.collection.version_id,
            owner,
            Signing::EveryBlock,
        );
        peer.push(&sanctioned, &self.collection.collection_id).await;
        wait_for_user(
            &self.client(),
            Some(&owner.private_key_hex),
            doc_id,
            |row| row["name"] == name,
        )
        .await;
    }

    /// The node's own record of a document's genesis, as `_commits` reports it.
    fn genesis_of(&self, owner: &Author, doc_id: &str) -> Parent {
        let commits = self
            .client()
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
}
