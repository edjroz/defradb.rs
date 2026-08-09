//! What the fetch fan-out is allowed to ask for.
//!
//! The numbers these pin came out of the phase 3 depth sweep: 38 fetches for 10
//! distinct heads at branch depth 50, and 559 for 500 documents at total
//! divergence. Both are this selection, unfiltered.

use blockstore::{Blockstore, DefraBlockstore};
use cid::Cid;
use multihash_codetable::{Code, MultihashDigest};
use storage::backends::MemoryStore;

use super::*;

type TestBlockstore = DefraBlockstore<MemoryStore>;

const DAG_CBOR: u64 = 0x71;

fn store() -> TestBlockstore {
    DefraBlockstore::new(std::sync::Arc::new(MemoryStore::new()), true)
}

/// A dag-cbor block with no links, so a DAG walk over it completes.
fn leaf(tag: u8) -> (Cid, Vec<u8>) {
    let data = serde_ipld_dagcbor::to_vec(&vec![tag]).expect("encode leaf");
    let cid = Cid::new_v1(DAG_CBOR, Code::Sha2_256.digest(&data));
    (cid, data)
}

/// A dag-cbor block linking to `child`, so a DAG walk over it needs `child`.
fn parent(child: &Cid) -> (Cid, Vec<u8>) {
    let data =
        serde_ipld_dagcbor::to_vec(&ipld_core::ipld!({ "l": child })).expect("encode parent");
    let cid = Cid::new_v1(DAG_CBOR, Code::Sha2_256.digest(&data));
    (cid, data)
}

fn need(cids: &[Cid]) -> Vec<ItemId> {
    cids.iter().map(|cid| ItemId::new(cid.to_bytes())).collect()
}

/// The defect as the depth sweep saw it: the same head named by five sessions
/// is one fetch, not five.
#[tokio::test]
async fn a_head_named_more_than_once_is_fetched_once() {
    let blockstore = store();
    let (cid, _) = leaf(1);

    let heads = heads_to_fetch(&blockstore, &need(&[cid, cid, cid, cid, cid])).await;

    assert_eq!(heads, vec![cid]);
}

/// A head that arrived while the session was running is not news. The session's
/// snapshot was taken before it landed, so it is reported as needed regardless.
#[tokio::test]
async fn a_head_whose_dag_is_local_and_merged_is_not_fetched() {
    let blockstore = store();
    let (cid, data) = leaf(2);
    blockstore.put(&cid, &data).await.expect("put");
    blockstore.mark_as_merged(&cid).await.expect("merge");

    assert!(heads_to_fetch(&blockstore, &need(&[cid])).await.is_empty());
}

/// Holding the head block says nothing about holding its ancestors, which is
/// exactly the mistake BranchableSync's own comment warns about.
#[tokio::test]
async fn a_head_present_without_its_ancestors_is_still_fetched() {
    let blockstore = store();
    let (child, _) = leaf(3);
    let (root, root_data) = parent(&child);
    blockstore.put(&root, &root_data).await.expect("put");
    blockstore.mark_as_merged(&root).await.expect("merge");

    assert_eq!(
        heads_to_fetch(&blockstore, &need(&[root])).await,
        vec![root]
    );
}

/// A complete but unmerged DAG has not been applied yet, so the head still has
/// work behind it.
#[tokio::test]
async fn a_complete_but_unmerged_head_is_still_fetched() {
    let blockstore = store();
    let (cid, data) = leaf(4);
    blockstore.put(&cid, &data).await.expect("put");

    assert_eq!(heads_to_fetch(&blockstore, &need(&[cid])).await, vec![cid]);
}

/// Distinct heads are all fetched, in the order the session named them, and a
/// locally satisfied one drops out of the middle without disturbing the rest.
#[tokio::test]
async fn distinct_missing_heads_survive_in_order() {
    let blockstore = store();
    let (first, _) = leaf(5);
    let (held, held_data) = leaf(6);
    let (last, _) = leaf(7);
    blockstore.put(&held, &held_data).await.expect("put");
    blockstore.mark_as_merged(&held).await.expect("merge");

    let heads = heads_to_fetch(&blockstore, &need(&[first, held, last, first])).await;

    assert_eq!(heads, vec![first, last]);
}

/// An identity that is not a CID is not a head. Dropping it here is what the
/// unfiltered path did too, and the filter must not start fetching garbage.
#[tokio::test]
async fn an_identity_that_is_not_a_cid_is_dropped() {
    let blockstore = store();
    let (cid, _) = leaf(8);
    let mut ids = need(&[cid]);
    ids.insert(0, ItemId::new(b"not-a-cid".to_vec()));

    assert_eq!(heads_to_fetch(&blockstore, &ids).await, vec![cid]);
}
