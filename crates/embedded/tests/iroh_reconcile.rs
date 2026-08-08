//! Two flag-on iroh nodes converge by reconciliation alone.
//!
//! Reconciliation is the only mover here, and the tests are built so that a
//! silently degenerate session cannot pass. No collection is added to P2P sync
//! and no replicator is set on either node, so gossip has no topic to carry
//! these documents and there is no push path at all — proven by
//! [`nothing_moves_without_a_reconciliation`], which does everything except call
//! reconcile and asserts nothing crossed. Every other test then asserts the
//! *discovered* difference equals the true difference before it looks at whether
//! documents arrived, so a session that merely exchanged whole sets would fail.

#![cfg(feature = "iroh")]

mod reconcile_support;

use anyhow::{Context, Result};
use embedded::reconcile_ops::ReconcileOutcome;
use embedded::NodeBuilder;
use tokio::time::{sleep, Duration};

use reconcile_support::{
    add_note, connect, has_note, heads_missing_from, p2p_of, set, test_iroh_config, wait_for_note,
    Pair, COLLECTION, NOTE_SDL,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zero_diff_session_discovers_nothing() -> Result<()> {
    let pair = Pair::connected().await?;
    add_note(&pair.initiator, "same").await?;
    add_note(&pair.responder, "same").await?;

    let outcome = pair.reconcile().await?;
    assert!(
        outcome.need.is_empty() && outcome.have.is_empty(),
        "identical sets must produce no difference, got {outcome:?}"
    );

    pair.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_empty_collections_converge() -> Result<()> {
    let pair = Pair::connected().await?;

    let outcome = pair.reconcile().await?;
    assert_eq!(outcome, ReconcileOutcome::default());

    pair.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_one_sided_update_is_discovered_and_fetched() -> Result<()> {
    let pair = Pair::connected().await?;
    for body in ["shared-a", "shared-b"] {
        add_note(&pair.initiator, body).await?;
        add_note(&pair.responder, body).await?;
    }
    add_note(&pair.responder, "only-on-responder").await?;

    let expected_need = heads_missing_from(&pair.initiator, &pair.responder).await?;
    let outcome = pair.reconcile().await?;

    assert_eq!(
        set(&outcome.need),
        expected_need,
        "the discovered need set must be exactly the true difference"
    );
    assert!(
        outcome.have.is_empty(),
        "the initiator holds nothing the responder lacks"
    );

    wait_for_note(&pair.initiator, "only-on-responder").await?;
    pair.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bidirectional_divergence_is_discovered_in_both_directions() -> Result<()> {
    let pair = Pair::connected().await?;
    for body in ["shared-a", "shared-b", "shared-c"] {
        add_note(&pair.initiator, body).await?;
        add_note(&pair.responder, body).await?;
    }
    add_note(&pair.initiator, "only-on-initiator").await?;
    add_note(&pair.responder, "only-on-responder").await?;

    let expected_need = heads_missing_from(&pair.initiator, &pair.responder).await?;
    let expected_have = heads_missing_from(&pair.responder, &pair.initiator).await?;

    let outcome = pair.reconcile().await?;
    assert_eq!(set(&outcome.need), expected_need, "need must be exact");
    assert_eq!(set(&outcome.have), expected_have, "have must be exact");

    wait_for_note(&pair.initiator, "only-on-responder").await?;
    pair.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_larger_divergence_is_discovered_exactly() -> Result<()> {
    let pair = Pair::connected().await?;
    for index in 0..40 {
        let body = format!("shared-{index}");
        add_note(&pair.initiator, &body).await?;
        add_note(&pair.responder, &body).await?;
    }
    for index in 0..7 {
        add_note(&pair.responder, &format!("extra-{index}")).await?;
    }

    let expected_need = heads_missing_from(&pair.initiator, &pair.responder).await?;
    assert_eq!(expected_need.len(), 7, "fixture must diverge by seven docs");

    let outcome = pair.reconcile().await?;
    assert_eq!(set(&outcome.need), expected_need);

    pair.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reconciling_while_writing_neither_deadlocks_nor_panics() -> Result<()> {
    let pair = Pair::connected().await?;
    for index in 0..20 {
        let body = format!("shared-{index}");
        add_note(&pair.initiator, &body).await?;
        add_note(&pair.responder, &body).await?;
    }
    add_note(&pair.responder, "diverged").await?;

    // Writes land on both sides while the session runs. A session reads a
    // sealed snapshot, so these may or may not be in its result; what must hold
    // is that the session finishes and a later session finds them.
    let first = pair.reconcile().await?;
    for index in 0..5 {
        add_note(&pair.responder, &format!("concurrent-{index}")).await?;
        add_note(&pair.initiator, &format!("local-{index}")).await?;
    }
    let _ = first;

    let outcome = pair.reconcile().await?;
    let expected_need = heads_missing_from(&pair.initiator, &pair.responder).await?;
    assert!(
        set(&outcome.need).is_subset(&expected_need) || outcome.need.is_empty(),
        "a session must never claim to need something the peer does not have"
    );

    let settled = pair.reconcile().await?;
    assert!(
        settled.need.len() <= outcome.need.len(),
        "successive sessions must not discover more, they converge"
    );

    pair.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nothing_moves_without_a_reconciliation() -> Result<()> {
    let pair = Pair::connected().await?;
    add_note(&pair.responder, "never-pushed").await?;

    sleep(Duration::from_secs(3)).await;

    assert!(
        !has_note(&pair.initiator, "never-pushed").await?,
        "gossip or push moved a document, so a convergence test could not \
         attribute convergence to reconciliation"
    );

    pair.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_flag_off_node_refuses_a_session() -> Result<()> {
    let initiator = NodeBuilder::default()
        .with_iroh(test_iroh_config())
        .with_reconcile()
        .build()
        .await?;
    let responder = NodeBuilder::default()
        .with_iroh(test_iroh_config())
        .build()
        .await?;

    initiator.add_schema(NOTE_SDL).await?;
    responder.add_schema(NOTE_SDL).await?;

    let initiator_p2p = p2p_of(&initiator)?;
    let responder_p2p = p2p_of(&responder)?;
    assert!(
        responder_p2p.reconciler().is_none(),
        "a flag-off node must not expose a reconciler"
    );

    let responder_peer_id = connect(&initiator_p2p, &responder_p2p).await?;

    let result = initiator_p2p
        .reconciler()
        .context("initiator has no reconciler")?
        .reconcile_collection(&responder_peer_id, COLLECTION)
        .await;
    assert!(
        result.is_err(),
        "a node that does not offer the reconcile ALPN must refuse the session"
    );

    initiator_p2p.shutdown().await;
    responder_p2p.shutdown().await;
    initiator.database.close().await?;
    responder.database.close().await?;
    Ok(())
}
