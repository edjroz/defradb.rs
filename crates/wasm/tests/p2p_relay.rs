//! A browser peer and a native node replicating through a relay.
//!
//! `tools/browser-p2p-e2e.sh` starts a node that hosts an iroh relay, then
//! builds this test with the `relay-e2e` feature and `DEFRA_E2E_API` and
//! `DEFRA_E2E_RELAY` set. Missing either fails the build, so there is no way
//! for this test to run without a node behind it.
//!
//! Both directions are covered: a document the node already holds reaches the
//! browser through a replicator the node adds, and a document the browser
//! writes reaches the node through a replicator the browser adds. Neither side
//! has a direct path to the other, so every byte crosses the relay.

#![cfg(target_arch = "wasm32")]

use std::time::Duration;

use defra_wasm::DefraClient;
use serde_json::{json, Value};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use wasm_bindgen_test::*;
use web_sys::{Headers, Request, RequestInit, Response};

wasm_bindgen_test_configure!(run_in_browser);

const SCHEMA: &str = "type Note { text: String }";
const REPLICATION_TIMEOUT: Duration = Duration::from_secs(60);

#[wasm_bindgen_test]
async fn a_browser_and_a_node_replicate_through_the_hosted_relay() {
    let api = env!("DEFRA_E2E_API");
    let relay = env!("DEFRA_E2E_RELAY");

    let node_id = node_endpoint_id(api).await;
    http(api, "POST", "/api/v0/schema", "text/plain", SCHEMA).await;
    graphql(
        api,
        r#"mutation { create_Note(input: {text: "from-node"}) { _docID } }"#,
    )
    .await;

    let mut browser = DefraClient::create(
        serde_wasm_bindgen::to_value(&json!({ "db_name": "p2p_relay_e2e" })).unwrap(),
    )
    .await
    .unwrap();
    browser.add_schema(SCHEMA).await.unwrap();
    let browser_id = browser
        .start_p2p(serde_wasm_bindgen::to_value(&json!({ "relay_urls": [relay] })).unwrap())
        .await
        .unwrap();

    let node_address = format!("{node_id}@{relay}");
    let browser_address = format!("{browser_id}@{relay}");
    browser.connect_peer(&node_address).await.unwrap();

    http(
        api,
        "POST",
        "/api/v0/p2p/replicators",
        "application/json",
        &json!({ "Collections": ["Note"], "Addresses": [browser_address] }).to_string(),
    )
    .await;
    wait_for("the node's document to reach the browser", || async {
        let result: Value =
            serde_wasm_bindgen::from_value(browser.query("{ Note { text } }").await.unwrap())
                .unwrap();
        has_note(&result["data"], "from-node")
    })
    .await;

    browser
        .add_replicator(
            &node_address,
            serde_wasm_bindgen::to_value(&["Note"]).unwrap(),
        )
        .await
        .unwrap();
    browser
        .mutate(r#"mutation { create_Note(input: {text: "from-browser"}) { _docID } }"#)
        .await
        .unwrap();
    wait_for("the browser's document to reach the node", || async {
        has_note(
            &graphql(api, "{ Note { text } }").await["data"],
            "from-browser",
        )
    })
    .await;

    browser.close().await.unwrap();
}

/// The bare endpoint id among the node's advertised addresses, which is what a
/// relay address is built from.
async fn node_endpoint_id(api: &str) -> String {
    let addresses = http(api, "GET", "/api/v0/p2p/info", "application/json", "").await;
    addresses
        .as_array()
        .expect("p2p info is an address list")
        .iter()
        .filter_map(Value::as_str)
        .find(|address| address.len() == 64 && address.chars().all(|c| c.is_ascii_hexdigit()))
        .expect("the node advertises its endpoint id")
        .to_string()
}

fn has_note(data: &Value, text: &str) -> bool {
    data["Note"]
        .as_array()
        .is_some_and(|notes| notes.iter().any(|note| note["text"] == text))
}

async fn wait_for<F, Fut>(what: &str, mut check: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let started = js_sys::Date::now();
    loop {
        if check().await {
            return;
        }
        let elapsed = Duration::from_millis((js_sys::Date::now() - started) as u64);
        assert!(
            elapsed < REPLICATION_TIMEOUT,
            "timed out waiting for {what}"
        );
        gloo_timers::future::TimeoutFuture::new(250).await;
    }
}

async fn graphql(api: &str, query: &str) -> Value {
    let response = http(
        api,
        "POST",
        "/api/v0/graphql",
        "application/json",
        &json!({ "query": query }).to_string(),
    )
    .await;
    assert!(
        response["errors"]
            .as_array()
            .is_none_or(|errors| errors.is_empty()),
        "graphql failed: {response}"
    );
    response
}

async fn http(api: &str, method: &str, path: &str, content_type: &str, body: &str) -> Value {
    let headers = Headers::new().unwrap();
    headers.set("Content-Type", content_type).unwrap();
    let init = RequestInit::new();
    init.set_method(method);
    init.set_headers(&headers);
    if !body.is_empty() {
        init.set_body(&JsValue::from_str(body));
    }
    let request = Request::new_with_str_and_init(&format!("{api}{path}"), &init).unwrap();
    let response: Response =
        JsFuture::from(web_sys::window().unwrap().fetch_with_request(&request))
            .await
            .unwrap()
            .dyn_into()
            .unwrap();
    let text = JsFuture::from(response.text().unwrap())
        .await
        .unwrap()
        .as_string()
        .unwrap_or_default();
    assert!(
        response.ok(),
        "{method} {path} returned {}: {text}",
        response.status()
    );
    serde_json::from_str(&text).unwrap_or(Value::Null)
}
