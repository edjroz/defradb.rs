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
use embedded::NodeBuilder;
use tokio::time::{sleep, Duration};

use reconcile_support::{
    add_note, connect, has_note, heads, heads_missing_from, p2p_of, set, test_iroh_config,
    wait_for_note, Pair, COLLECTION, NOTE_SDL,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_zero_diff_session_discovers_nothing_and_costs_almost_nothing() -> Result<()> {
    let pair = Pair::connected().await?;
    for index in 0..40 {
        let body = format!("same-{index}");
        add_note(&pair.initiator, &body).await?;
        add_note(&pair.responder, &body).await?;
    }

    let outcome = pair.reconcile().await?;
    assert!(
        outcome.need.is_empty() && outcome.have.is_empty(),
        "identical sets must produce no difference, got {outcome:?}"
    );

    // The point of reconciliation is that agreement is cheap regardless of set
    // size: one fingerprint out, one skip back. Forty documents' worth of head
    // CIDs would be well over a kilobyte, so this fails loudly if a session ever
    // degenerates into exchanging the set itself.
    assert_eq!(outcome.rounds, 1, "agreement should settle in one round");
    assert!(
        outcome.bytes_sent + outcome.bytes_received < 512,
        "a zero-diff session must be near-noop, cost {outcome:?}"
    );

    pair.shutdown().await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn both_empty_collections_converge() -> Result<()> {
    let pair = Pair::connected().await?;

    let outcome = pair.reconcile().await?;
    assert!(outcome.need.is_empty() && outcome.have.is_empty());
    assert!(
        outcome.bytes_sent + outcome.bytes_received < 512,
        "two empty sets must agree almost for free, cost {outcome:?}"
    );

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
async fn bidirectional_divergence_is_discovered_but_only_pulled_one_way() -> Result<()> {
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

    // One session makes the initiator whole and leaves the responder as it was.
    // The `have` set above says what the responder is missing; acting on it is
    // delivery, which reconciliation deliberately does not do. The responder
    // converges by running a session of its own.
    assert!(
        !has_note(&pair.responder, "only-on-initiator").await?,
        "a session must not push: the responder converges only on its own turn"
    );

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

    // Both stores take writes for the whole life of the session. A session runs
    // over a sealed snapshot, so whether any given write is in its result is
    // timing; what must hold is that the session finishes at all, that it never
    // claims to need something the peer does not have, and that re-running
    // eventually settles.
    let writes = async {
        for index in 0..10 {
            add_note(&pair.responder, &format!("concurrent-{index}")).await?;
            add_note(&pair.initiator, &format!("local-{index}")).await?;
        }
        Ok::<(), anyhow::Error>(())
    };
    let (outcome, written) = tokio::join!(pair.reconcile(), writes);
    written?;
    let outcome = outcome?;

    assert!(
        set(&outcome.need).is_subset(&heads(&pair.responder).await?),
        "a session must never claim to need a head the peer does not hold"
    );

    // Eventual convergence on re-run, which is the documented contract when the
    // store moves under a session.
    let mut remaining = outcome.need.len();
    for _ in 0..10 {
        if remaining == 0 {
            break;
        }
        sleep(Duration::from_millis(500)).await;
        remaining = pair.reconcile().await?.need.len();
    }
    assert_eq!(
        remaining, 0,
        "successive sessions over a quiet store must reach a zero need set"
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
async fn a_peer_that_goes_away_fails_the_session_instead_of_hanging() -> Result<()> {
    let pair = Pair::connected().await?;
    add_note(&pair.initiator, "shared").await?;
    add_note(&pair.responder, "shared").await?;
    add_note(&pair.responder, "diverged").await?;
    pair.reconcile().await?;

    p2p_of(&pair.responder)?.shutdown().await;

    // The bound is the assertion: a session against a peer that is no longer
    // there must return an error, not park on a read that will never complete.
    let result = tokio::time::timeout(Duration::from_secs(30), pair.reconcile())
        .await
        .context("a session against a dead peer did not terminate")?;
    assert!(result.is_err(), "a dead peer must fail the session");

    // And the local node is still a working node afterwards.
    assert!(has_note(&pair.initiator, "shared").await?);

    p2p_of(&pair.initiator)?.shutdown().await;
    pair.initiator.database.close().await?;
    pair.responder.database.close().await?;
    Ok(())
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
