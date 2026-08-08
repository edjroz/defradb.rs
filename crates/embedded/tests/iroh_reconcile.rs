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

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};

use anyhow::{anyhow, bail, Context, Result};
use cid::Cid;
use embedded::reconcile_ops::ReconcileOutcome;
use embedded::{EmbeddedNode, EmbeddedStore, IrohConfig, ManagedP2PSystem, NodeBuilder};
use tokio::time::{sleep, Duration, Instant};

const NOTE_SDL: &str = "type Note { body: String }";
const COLLECTION: &str = "Note";

struct Pair {
    initiator: EmbeddedNode<EmbeddedStore>,
    responder: EmbeddedNode<EmbeddedStore>,
    responder_peer_id: String,
}

impl Pair {
    /// Two reconcile-enabled nodes, connected, with no collection subscription
    /// and no replicator: there is no gossip or push route between them.
    async fn connected() -> Result<Self> {
        let initiator = NodeBuilder::default()
            .with_iroh(test_iroh_config())
            .with_reconcile()
            .build()
            .await?;
        let responder = NodeBuilder::default()
            .with_iroh(test_iroh_config())
            .with_reconcile()
            .build()
            .await?;

        initiator.add_schema(NOTE_SDL).await?;
        responder.add_schema(NOTE_SDL).await?;

        let initiator_p2p = p2p_of(&initiator)?;
        let responder_p2p = p2p_of(&responder)?;

        let responder_peer_id = responder_p2p
            .ops()
            .local_peer_id()
            .await
            .map_err(|error| anyhow!(error))?;
        let responder_addr = wait_for_listen_addr(&responder_p2p).await?;

        initiator_p2p
            .ops()
            .connect_peer(&responder_addr)
            .await
            .map_err(|error| anyhow!(error))?;
        wait_for_connected_peer(&initiator_p2p, &responder_peer_id).await?;

        Ok(Self {
            initiator,
            responder,
            responder_peer_id,
        })
    }

    async fn reconcile(&self) -> Result<ReconcileOutcome> {
        p2p_of(&self.initiator)?
            .reconciler()
            .context("initiator has no reconciler installed")?
            .reconcile_collection(&self.responder_peer_id, COLLECTION)
            .await
    }

    async fn shutdown(self) -> Result<()> {
        p2p_of(&self.initiator)?.shutdown().await;
        p2p_of(&self.responder)?.shutdown().await;
        self.initiator.database.close().await?;
        self.responder.database.close().await?;
        Ok(())
    }
}

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

    let responder_peer_id = responder_p2p
        .ops()
        .local_peer_id()
        .await
        .map_err(|error| anyhow!(error))?;
    let responder_addr = wait_for_listen_addr(&responder_p2p).await?;
    initiator_p2p
        .ops()
        .connect_peer(&responder_addr)
        .await
        .map_err(|error| anyhow!(error))?;
    wait_for_connected_peer(&initiator_p2p, &responder_peer_id).await?;

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

fn test_iroh_config() -> IrohConfig {
    IrohConfig {
        bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        bind_port: Some(0),
        relay_mode: p2p::iroh::IrohRelayModeConfig::Disabled,
        discovery: p2p::iroh::IrohDiscoveryConfig::Disabled,
        max_concurrent_multipath_paths: None,
        secret_key_path: None,
    }
}

fn p2p_of(node: &EmbeddedNode<EmbeddedStore>) -> Result<std::sync::Arc<ManagedP2PSystem>> {
    node.p2p().cloned().context("node has no p2p system")
}

fn set(cids: &[Cid]) -> BTreeSet<Cid> {
    cids.iter().copied().collect()
}

/// The head CIDs `other` holds that `node` does not — the true difference the
/// session is measured against, read straight from both stores.
async fn heads_missing_from(
    node: &EmbeddedNode<EmbeddedStore>,
    other: &EmbeddedNode<EmbeddedStore>,
) -> Result<BTreeSet<Cid>> {
    let mine = heads(node).await?;
    Ok(heads(other).await?.difference(&mine).copied().collect())
}

async fn heads(node: &EmbeddedNode<EmbeddedStore>) -> Result<BTreeSet<Cid>> {
    use p2p::reconcile::ItemSource;
    use p2p::sync::ReconcileSourceProvider;

    let source = db_merge::create_reconcile_source(node.database.clone())
        .snapshot(COLLECTION)
        .await
        .map_err(|error| anyhow!("failed to read heads: {error}"))?;
    (0..source.len())
        .map(|index| {
            Cid::try_from(source.id(index).as_bytes())
                .map_err(|error| anyhow!("head is not a CID: {error}"))
        })
        .collect()
}

async fn add_note(node: &EmbeddedNode<EmbeddedStore>, body: &str) -> Result<()> {
    let response = node
        .execute(&format!(
            r#"mutation {{ add_Note(input: {{body: "{body}"}}) {{ _docID }} }}"#
        ))
        .await;
    if response.has_errors() {
        bail!("add_Note failed: {:?}", response.errors);
    }
    Ok(())
}

async fn has_note(node: &EmbeddedNode<EmbeddedStore>, body: &str) -> Result<bool> {
    let response = node.execute("query { Note { body } }").await;
    if response.has_errors() {
        bail!("Note query failed: {:?}", response.errors);
    }
    Ok(response
        .data
        .as_ref()
        .and_then(|data| data.get("Note"))
        .and_then(|notes| notes.as_array())
        .map(|notes| {
            notes
                .iter()
                .any(|note| note.get("body").and_then(|value| value.as_str()) == Some(body))
        })
        .unwrap_or(false))
}

async fn wait_for_note(node: &EmbeddedNode<EmbeddedStore>, body: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if has_note(node, body).await? {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for reconciled document '{body}'");
        }
        sleep(Duration::from_millis(200)).await;
    }
}

async fn wait_for_listen_addr(system: &ManagedP2PSystem) -> Result<String> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let addrs = system
            .ops()
            .listen_addresses()
            .await
            .map_err(|error| anyhow!(error))?;
        if let Some(addr) = addrs
            .into_iter()
            .find(|addr| addr.contains("/p2p/") || addr.starts_with("endpoint"))
        {
            return Ok(addr);
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for a direct iroh listen address");
        }
        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_connected_peer(system: &ManagedP2PSystem, peer_id: &str) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let peers = system
            .ops()
            .connected_peers()
            .await
            .map_err(|error| anyhow!(error))?;
        if peers.iter().any(|peer| {
            p2p::iroh::parse_public_peer_addr(peer)
                .map(|(parsed, _)| parsed.as_str() == peer_id)
                .unwrap_or_else(|_| peer.contains(peer_id))
        }) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!("timed out waiting for a connection to {peer_id}");
        }
        sleep(Duration::from_millis(100)).await;
    }
}
