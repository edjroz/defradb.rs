//! Spike H2: does the persisted retry ladder drain after an overload burst?
//!
//! One sender fans out to two receivers with every P2P default unchanged.
//! `WRITERS` unthrottled writers drive the sender to its local write ceiling
//! for `BURST`, then stop; the test samples each receiver's document count, the
//! sender's push-backlog counters, and the sender's durable retry markers once
//! every `SAMPLE` until `SETTLE` elapses.
//!
//! Ignored by default: it runs for ~8 minutes and saturates several cores.
//!
//! A shared `CARGO_TARGET_DIR` across worktrees makes the harness's
//! workspace-root build resolve to whichever worktree compiled last, so pin the
//! binary:
//!
//! ```text
//! cargo build --offline -p cli && cp "$CARGO_TARGET_DIR/debug/defra" /tmp/defra-h2
//! DEFRA_RUST_BINARY=/tmp/defra-h2 \
//!   cargo test --offline -p integration-test --test spike_h2_drain -- --ignored --nocapture
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use integration_test::TestCluster;
use serde_json::{json, Value};

const SCHEMA: &str = "type DrainDoc { name: String  seq: Int }";
const WRITERS: usize = 8;
const BURST: Duration = Duration::from_secs(120);
/// Aggregate writes/s across `WRITERS`. Unthrottled (`0`) buries the node in
/// its own local writes long before replication matters, so the default paces
/// just above what the replication path sustains.
const DEFAULT_TARGET_RATE: f64 = 100.0;
const SETTLE: Duration = Duration::from_secs(360);
const SAMPLE: Duration = Duration::from_secs(10);

async fn gql(http: &reqwest::Client, url: &str, query: &str) -> Result<Value, String> {
    let body: Value = http
        .post(format!("{url}/api/v0/graphql"))
        .json(&json!({ "query": query }))
        .send()
        .await
        .map_err(|error| format!("http error: {error}"))?
        .json()
        .await
        .map_err(|error| format!("bad json: {error}"))?;
    if let Some(errors) = body.get("errors").and_then(Value::as_array) {
        if !errors.is_empty() {
            return Err(errors
                .iter()
                .map(|error| error["message"].as_str().unwrap_or("?").to_string())
                .collect::<Vec<_>>()
                .join("; "));
        }
    }
    Ok(body["data"].clone())
}

async fn doc_count(http: &reqwest::Client, url: &str) -> u64 {
    match gql(http, url, "query { DrainDoc { _docID } }").await {
        Ok(data) => data["DrainDoc"].as_array().map(|a| a.len() as u64).unwrap_or(0),
        Err(_) => u64::MAX,
    }
}

async fn sync_status(http: &reqwest::Client, url: &str) -> Value {
    match http.get(format!("{url}/api/v0/p2p/sync/status")).send().await {
        Ok(response) => response.json().await.unwrap_or(Value::Null),
        Err(_) => Value::Null,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "runs for ~8 minutes"]
async fn overload_burst_drains_to_zero_missing() {
    // The harness defaults to `--store memory`, which has no fsync and lets the
    // local write path run an order of magnitude ahead of the field. Pin the
    // on-disk engine so the write and replicate sides are comparable.
    let cluster = TestCluster::builder()
        .rust_nodes(3)
        .with_store("regolith")
        .with_p2p()
        .build()
        .await
        .expect("cluster start");

    for node in 0..3 {
        cluster
            .wait_for_log(node, "p2p_listening", Duration::from_secs(30))
            .await
            .unwrap_or_else(|error| panic!("node{node} P2P listener: {error}"));
        cluster.client(node).schema_add(SCHEMA).expect("schema");
    }

    let sender = cluster.client(0);
    for receiver in 1..3 {
        let info = cluster.client(receiver).p2p_info().expect("receiver p2p info");
        let addr = info[0].as_str().expect("receiver address").to_string();
        sender.p2p_connect(&[&addr]).expect("connect");
        sender
            .p2p_replicator_set(&["DrainDoc"], &addr)
            .expect("replicator");
    }

    let sender_url = cluster.api_url(0).to_string();
    let receiver_urls = [cluster.api_url(1).to_string(), cluster.api_url(2).to_string()];
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("http client");

    let written = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let burst_start = Instant::now();
    let target_rate: f64 = std::env::var("DEFRA_H2_RATE")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_TARGET_RATE);
    let period = (target_rate > 0.0).then(|| Duration::from_secs_f64(WRITERS as f64 / target_rate));
    let mut tasks = Vec::new();
    for writer in 0..WRITERS {
        let http = http.clone();
        let url = sender_url.clone();
        let written = Arc::clone(&written);
        let failed = Arc::clone(&failed);
        tasks.push(tokio::spawn(async move {
            let mut ticker = period.map(tokio::time::interval);
            let mut seq = 0u64;
            while burst_start.elapsed() < BURST {
                if let Some(ticker) = ticker.as_mut() {
                    ticker.tick().await;
                }
                let mutation = format!(
                    r#"mutation {{ create_DrainDoc(input: {{name: "w{writer}-{seq}", seq: {seq}}}) {{ _docID }} }}"#
                );
                match gql(&http, &url, &mutation).await {
                    Ok(_) => {
                        written.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                seq += 1;
            }
        }));
    }
    for task in tasks {
        let _ = task.await;
    }

    let sent = written.load(Ordering::Relaxed);
    let write_errors = failed.load(Ordering::Relaxed);
    let burst_secs = burst_start.elapsed().as_secs_f64();
    println!(
        "burst: {sent} docs in {burst_secs:.1}s ({:.1}/s, target {target_rate}/s), {write_errors} write errors"
        , sent as f64 / burst_secs
    );

    println!("elapsed_s,sender_docs,rx1_docs,rx2_docs,missing,queued_items,rejected_items,rejected_bytes,doc_markers,col_markers,next_retry_in_s");
    let settle_start = Instant::now();
    let mut last_missing;
    loop {
        let sender_docs = doc_count(&http, &sender_url).await;
        let rx1_docs = doc_count(&http, &receiver_urls[0]).await;
        let rx2_docs = doc_count(&http, &receiver_urls[1]).await;
        let status = sync_status(&http, &sender_url).await;
        let backlog = &status["push_backlog"];
        let markers = &status["push_retry_markers"];
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let next_retry_in = markers["oldest_scheduled_retry_unix"]
            .as_u64()
            .map(|deadline| deadline as i64 - now as i64)
            .unwrap_or(-1);
        last_missing = (sender_docs as i64 - rx1_docs as i64) + (sender_docs as i64 - rx2_docs as i64);
        println!(
            "{:.0},{sender_docs},{rx1_docs},{rx2_docs},{last_missing},{},{},{},{},{},{next_retry_in}",
            settle_start.elapsed().as_secs_f64(),
            backlog["queued_items"].as_u64().unwrap_or(0),
            backlog["rejected_items_total"].as_u64().unwrap_or(0),
            backlog["rejected_bytes_total"].as_u64().unwrap_or(0),
            markers["document_markers"].as_u64().unwrap_or(0),
            markers["collection_markers"].as_u64().unwrap_or(0),
        );
        if last_missing == 0 {
            break;
        }
        if settle_start.elapsed() >= SETTLE {
            break;
        }
        tokio::time::sleep(SAMPLE).await;
    }

    assert_eq!(
        last_missing,
        0,
        "receivers still missing {last_missing} documents {}s after the burst stopped",
        settle_start.elapsed().as_secs()
    );
}
