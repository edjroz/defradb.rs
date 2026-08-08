//! Harness for the reconciliation convergence tests: two connected iroh nodes
//! with no gossip topic and no replicator between them.

use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

use anyhow::{anyhow, bail, Context, Result};
use cid::Cid;
use embedded::reconcile_ops::ReconcileOutcome;
use embedded::{EmbeddedNode, EmbeddedStore, IrohConfig, ManagedP2PSystem, NodeBuilder};
use tokio::time::{sleep, Duration, Instant};

pub const NOTE_SDL: &str = "type Note { body: String }";
pub const COLLECTION: &str = "Note";

/// Two reconcile-enabled nodes, connected, with no collection subscription and
/// no replicator: there is no gossip or push route between them.
pub struct Pair {
    pub initiator: EmbeddedNode<EmbeddedStore>,
    pub responder: EmbeddedNode<EmbeddedStore>,
    pub responder_peer_id: String,
}

impl Pair {
    pub async fn connected() -> Result<Self> {
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

        let responder_peer_id = connect(&initiator_p2p, &responder_p2p).await?;

        Ok(Self {
            initiator,
            responder,
            responder_peer_id,
        })
    }

    pub async fn reconcile(&self) -> Result<ReconcileOutcome> {
        p2p_of(&self.initiator)?
            .reconciler()
            .context("initiator has no reconciler installed")?
            .reconcile_collection(&self.responder_peer_id, COLLECTION)
            .await
    }

    pub async fn shutdown(self) -> Result<()> {
        p2p_of(&self.initiator)?.shutdown().await;
        p2p_of(&self.responder)?.shutdown().await;
        self.initiator.database.close().await?;
        self.responder.database.close().await?;
        Ok(())
    }
}

/// Dials `responder` from `initiator` and returns the responder's peer ID.
pub async fn connect(initiator: &ManagedP2PSystem, responder: &ManagedP2PSystem) -> Result<String> {
    let peer_id = responder
        .ops()
        .local_peer_id()
        .await
        .map_err(|error| anyhow!(error))?;
    let addr = wait_for_listen_addr(responder).await?;
    initiator
        .ops()
        .connect_peer(&addr)
        .await
        .map_err(|error| anyhow!(error))?;
    wait_for_connected_peer(initiator, &peer_id).await?;
    Ok(peer_id)
}

pub fn test_iroh_config() -> IrohConfig {
    IrohConfig {
        bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        bind_port: Some(0),
        relay_mode: p2p::iroh::IrohRelayModeConfig::Disabled,
        discovery: p2p::iroh::IrohDiscoveryConfig::Disabled,
        max_concurrent_multipath_paths: None,
        secret_key_path: None,
    }
}

pub fn p2p_of(node: &EmbeddedNode<EmbeddedStore>) -> Result<Arc<ManagedP2PSystem>> {
    node.p2p().cloned().context("node has no p2p system")
}

pub fn set(cids: &[Cid]) -> BTreeSet<Cid> {
    cids.iter().copied().collect()
}

/// The head CIDs `other` holds that `node` does not — the true difference the
/// session is measured against, read straight from both stores.
pub async fn heads_missing_from(
    node: &EmbeddedNode<EmbeddedStore>,
    other: &EmbeddedNode<EmbeddedStore>,
) -> Result<BTreeSet<Cid>> {
    let mine = heads(node).await?;
    Ok(heads(other).await?.difference(&mine).copied().collect())
}

pub async fn heads(node: &EmbeddedNode<EmbeddedStore>) -> Result<BTreeSet<Cid>> {
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

pub async fn add_note(node: &EmbeddedNode<EmbeddedStore>, body: &str) -> Result<()> {
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

pub async fn has_note(node: &EmbeddedNode<EmbeddedStore>, body: &str) -> Result<bool> {
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

pub async fn wait_for_note(node: &EmbeddedNode<EmbeddedStore>, body: &str) -> Result<()> {
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

pub async fn wait_for_listen_addr(system: &ManagedP2PSystem) -> Result<String> {
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

pub async fn wait_for_connected_peer(system: &ManagedP2PSystem, peer_id: &str) -> Result<()> {
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
