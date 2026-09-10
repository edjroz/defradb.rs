//! Spike H2: does the persisted retry ladder drain after an overload burst?
//!
//! Writes above the replicated-apply ceiling on one Rust node for a fixed
//! burst, stops writing, then samples the receiver's document count, the
//! sender's push-backlog counters, and the sender's durable retry markers
//! once every `SAMPLE` until `SETTLE` elapses.
//!
//! Ignored by default: it runs for ~8 minutes and saturates one core.
//!
//! ```text
//! cargo test -p integration-test --test spike_h2_drain -- --ignored --nocapture
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use integration_test::TestCluster;
use serde_json::{json, Value};

const SCHEMA: &str = "type DrainDoc { name: String  seq: Int }";
const WRITERS: usize = 4;
const TARGET_RATE: f64 = 25.0;
const BURST: Duration = Duration::from_secs(120);
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
    let cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .expect("cluster start");

    for node in 0..2 {
        cluster
            .wait_for_log(node, "p2p_listening", Duration::from_secs(30))
            .await
            .unwrap_or_else(|error| panic!("node{node} P2P listener: {error}"));
        cluster.client(node).schema_add(SCHEMA).expect("schema");
    }

    let receiver_addr = {
        let info = cluster.client(1).p2p_info().expect("receiver p2p info");
        info[0].as_str().expect("receiver address").to_string()
    };
    let sender = cluster.client(0);
    sender.p2p_connect(&[&receiver_addr]).expect("connect");
    sender
        .p2p_replicator_set(&["DrainDoc"], &receiver_addr)
        .expect("replicator");

    let sender_url = cluster.api_url(0).to_string();
    let receiver_url = cluster.api_url(1).to_string();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("http client");

    let written = Arc::new(AtomicU64::new(0));
    let failed = Arc::new(AtomicU64::new(0));
    let burst_start = Instant::now();
    let period = Duration::from_secs_f64(WRITERS as f64 / TARGET_RATE);
    let mut tasks = Vec::new();
    for writer in 0..WRITERS {
        let http = http.clone();
        let url = sender_url.clone();
        let written = Arc::clone(&written);
        let failed = Arc::clone(&failed);
        tasks.push(tokio::spawn(async move {
            let mut ticker = tokio::time::interval(period);
            let mut seq = 0u64;
            while burst_start.elapsed() < BURST {
                ticker.tick().await;
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
        "burst: {sent} docs in {burst_secs:.1}s ({:.1}/s), {write_errors} write errors",
        sent as f64 / burst_secs
    );

    println!("elapsed_s,sender_docs,receiver_docs,missing,rejected_items,rejected_bytes,doc_markers,col_markers,next_retry_in_s");
    let settle_start = Instant::now();
    let mut last_missing;
    loop {
        let sender_docs = doc_count(&http, &sender_url).await;
        let receiver_docs = doc_count(&http, &receiver_url).await;
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
        last_missing = sender_docs as i64 - receiver_docs as i64;
        println!(
            "{:.0},{sender_docs},{receiver_docs},{last_missing},{},{},{},{},{next_retry_in}",
            settle_start.elapsed().as_secs_f64(),
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
        "receiver still missing {last_missing} documents {}s after the burst stopped",
        settle_start.elapsed().as_secs()
    );
}
