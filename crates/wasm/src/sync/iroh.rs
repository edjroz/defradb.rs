//! Relay-only Iroh endpoint for the browser.
//!
//! A browser cannot open a UDP socket, so this endpoint never holds a direct
//! path to a peer: every packet is carried by a relay over WebSocket. It is
//! otherwise the same endpoint a native node runs, on the same mux ALPN, so a
//! peer cannot tell a browser dialer from any other.

use std::sync::Arc;

use futures::lock::Mutex;
use p2p::iroh::{IrohEndpointConfig, IrohRelayModeConfig, IrohTransport};
use p2p::transport::P2PTransport;
use serde::Deserialize;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::error::{Result, WasmError};

/// How the page wants its endpoint addressed.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct IrohSessionConfig {
    /// Relays to reach peers through. Empty uses iroh's default relays.
    #[serde(default)]
    relay_urls: Vec<String>,
    /// A 32-byte endpoint key as hex. Absent generates an ephemeral one, which
    /// gives the page a new endpoint id on every load.
    #[serde(default)]
    secret_key_hex: Option<String>,
    /// Publish and resolve addresses through the n0 pkarr relay. Off by
    /// default: a page that only ever dials out does not need to be findable,
    /// and publishing announces the endpoint id to a third party.
    #[serde(default)]
    discovery: bool,
}

/// A live browser endpoint.
#[wasm_bindgen]
pub struct IrohSession {
    transport: IrohTransport,
    endpoint_task: n0_future::task::JoinHandle<()>,
    drain_task: n0_future::task::AbortHandle,
    events_seen: Arc<Mutex<u64>>,
}

#[wasm_bindgen]
impl IrohSession {
    /// Bind a relay-only endpoint and start its event loop.
    #[wasm_bindgen(js_name = connect)]
    pub async fn connect(config: JsValue) -> std::result::Result<IrohSession, JsValue> {
        Self::connect_impl(config).await.map_err(Into::into)
    }

    /// This endpoint's iroh endpoint id, which is what a peer dials back.
    #[wasm_bindgen(js_name = endpointId)]
    pub fn endpoint_id(&self) -> String {
        self.transport.local_peer_id().to_string()
    }

    /// Dial a peer given its ticket or `<endpoint-id>@<relay-url>` address.
    #[wasm_bindgen]
    pub async fn dial(&self, addr: &str) -> std::result::Result<(), JsValue> {
        self.dial_impl(addr).await.map_err(Into::into)
    }

    /// Endpoint ids currently holding a live connection.
    #[wasm_bindgen(js_name = connectedPeers)]
    pub async fn connected_peers(&self) -> std::result::Result<JsValue, JsValue> {
        let peers =
            self.transport.connected_peers().await.map_err(|error| {
                WasmError::Sync(format!("failed to list connected peers: {error}"))
            })?;
        let peers: Vec<String> = peers.iter().map(|peer| peer.to_string()).collect();
        serde_wasm_bindgen::to_value(&peers).map_err(Into::into)
    }

    /// Transport events received since the endpoint started. Until the sync
    /// coordinator consumes them these are drained and counted, which is what
    /// keeps the endpoint's event channel from filling and stalling it.
    #[wasm_bindgen(js_name = eventsSeen)]
    pub async fn events_seen(&self) -> u64 {
        *self.events_seen.lock().await
    }

    /// Shut the endpoint down and release its relay connections.
    #[wasm_bindgen]
    pub async fn close(self) {
        self.drain_task.abort();
        self.endpoint_task.abort();
    }
}

impl IrohSession {
    async fn connect_impl(config: JsValue) -> Result<Self> {
        let config: IrohSessionConfig = if config.is_undefined() || config.is_null() {
            IrohSessionConfig::default()
        } else {
            serde_wasm_bindgen::from_value(config)
                .map_err(|error| WasmError::InvalidArgument(format!("invalid config: {error}")))?
        };

        let secret_key = match config.secret_key_hex.as_deref() {
            Some(hex) => p2p::iroh::secret_key_from_bytes(parse_secret_key(hex)?),
            None => p2p::iroh::generate_secret_key(),
        };

        let relay_mode = if config.relay_urls.is_empty() {
            IrohRelayModeConfig::Default
        } else {
            IrohRelayModeConfig::Custom(config.relay_urls.clone())
        };

        let endpoint_config = IrohEndpointConfig {
            secret_key: secret_key.clone(),
            relay_mode,
            discovery: if config.discovery {
                p2p::iroh::IrohDiscoveryConfig::N0
            } else {
                p2p::iroh::IrohDiscoveryConfig::Disabled
            },
            ..IrohEndpointConfig::default()
        };

        let (command_tx, mut events, _replicators, endpoint_task) =
            p2p::iroh::spawn_endpoint(endpoint_config)
                .await
                .map_err(|error| {
                    WasmError::Sync(format!("failed to bind iroh endpoint: {error}"))
                })?;

        let events_seen = Arc::new(Mutex::new(0u64));
        let counter = Arc::clone(&events_seen);
        let drain = n0_future::task::spawn(async move {
            while events.recv().await.is_some() {
                *counter.lock().await += 1;
            }
        });
        let drain_task = drain.abort_handle();
        spawn_local(async move {
            let _ = drain.await;
        });

        Ok(Self {
            transport: IrohTransport::new(command_tx, secret_key),
            endpoint_task,
            drain_task,
            events_seen,
        })
    }

    async fn dial_impl(&self, addr: &str) -> Result<()> {
        let (peer_id, addrs) = self.transport.parse_dial_addr(addr).map_err(|error| {
            WasmError::InvalidArgument(format!("invalid peer address: {error}"))
        })?;
        self.transport
            .dial(&peer_id, addrs)
            .await
            .map_err(|error| WasmError::Sync(format!("failed to dial {peer_id}: {error}")))
    }
}

fn parse_secret_key(hex_key: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(hex_key.trim())
        .map_err(|error| WasmError::InvalidArgument(format!("secret key is not hex: {error}")))?;
    bytes
        .try_into()
        .map_err(|_| WasmError::InvalidArgument("secret key must be exactly 32 bytes".to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use wasm_bindgen_test::{wasm_bindgen_test, wasm_bindgen_test_configure};

    wasm_bindgen_test_configure!(run_in_browser);

    /// A fixed key must yield a fixed endpoint id, which is what lets a page
    /// keep its peer identity across reloads.
    const KEY_HEX: &str = "0101010101010101010101010101010101010101010101010101010101010101";

    fn config(json: &str) -> JsValue {
        js_sys::JSON::parse(json).expect("test config parses")
    }

    #[wasm_bindgen_test]
    async fn endpoint_binds_in_the_browser() {
        let session = IrohSession::connect_impl(JsValue::UNDEFINED)
            .await
            .expect("a browser endpoint binds without a socket");

        // Reaching this point means iroh's relay-only bind, the endpoint task,
        // and its timers all ran under wasm-bindgen-futures rather than tokio.
        assert_eq!(session.endpoint_id().len(), 64);
        assert_eq!(session.events_seen().await, 0);
        session.close().await;
    }

    #[wasm_bindgen_test]
    async fn a_supplied_key_fixes_the_endpoint_id() {
        let json = format!("{{\"secret_key_hex\":\"{KEY_HEX}\"}}");
        let first = IrohSession::connect_impl(config(&json))
            .await
            .expect("first endpoint binds");
        let second = IrohSession::connect_impl(config(&json))
            .await
            .expect("second endpoint binds");

        assert_eq!(first.endpoint_id(), second.endpoint_id());
        first.close().await;
        second.close().await;
    }

    #[wasm_bindgen_test]
    async fn a_fresh_endpoint_has_no_peers() {
        let session = IrohSession::connect_impl(JsValue::UNDEFINED)
            .await
            .expect("endpoint binds");
        let peers = session.connected_peers().await.expect("peers are listable");
        let peers: Vec<String> = serde_wasm_bindgen::from_value(peers).expect("peers decode");

        assert!(peers.is_empty());
        session.close().await;
    }

    #[wasm_bindgen_test]
    async fn a_malformed_secret_key_is_rejected() {
        let short = IrohSession::connect_impl(config("{\"secret_key_hex\":\"0102\"}")).await;
        assert!(matches!(short, Err(WasmError::InvalidArgument(_))));

        let not_hex = IrohSession::connect_impl(config("{\"secret_key_hex\":\"zz\"}")).await;
        assert!(matches!(not_hex, Err(WasmError::InvalidArgument(_))));
    }

    /// Address parsing is deliberately permissive — anything unrecognized is
    /// read as a bare endpoint id, so only an empty address fails here. A
    /// junk address fails at dial time instead, against the network.
    #[wasm_bindgen_test]
    async fn an_empty_peer_address_is_rejected() {
        let session = IrohSession::connect_impl(JsValue::UNDEFINED)
            .await
            .expect("endpoint binds");

        let result = session.dial_impl("   ").await;
        assert!(matches!(result, Err(WasmError::InvalidArgument(_))));
        session.close().await;
    }
}
