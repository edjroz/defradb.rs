//! Spike H2: does a Go pusher read a Rust receiver's nack as success?
//!
//! `message.Send` at Go compat `53f0e76a3` tests the *request's* `ErrMessage`
//! (`internal/db/p2p/message/message.go:175`), which is always empty, instead of
//! the reply's. The claim under test is that a Rust backpressure nack therefore
//! reaches Go, is read as success, and the document is never retried.
//!
//! Four phases, one Go writer and one Rust receiver each:
//!
//! * `Clamped`  - the receiver's request rate limiter holds a single token, so
//!   a burst of creates is nacked with `RATE_LIMITED_MESSAGE`.
//! * `Default`  - the same burst with the limiter left alone (control).
//! * `Down`     - the receiver is stopped before the burst, so Go's pushes fail
//!   at the transport. This is the positive control for the Go-side reads: it
//!   shows what `handleReplicatorFailure` looks like when it does run.
//! * `Shed`     - the receiver's pending-DAG map holds a single slot
//!   (`DEFRA_P2P_MAX_PENDING_DAGS=1`), so the burst is nacked by the early
//!   capacity shed (`AT_CAPACITY_MESSAGE`) instead of the rate limiter.
//!
//! Go's `/rep/retry/doc/{peer}/{doc}` keyspace is read behaviourally. Its only
//! writer is `handleReplicatorFailure` (`internal/db/p2p/replicator.go:457-482`),
//! which in the same three statements flips the replicator to Inactive (logging
//! `Replicator status changed`, `replicator.go:555-562`) and writes the marker;
//! `retryReplicators` (`:601`) then re-pushes the doc's heads on the 30s rung
//! (`internal/db/config.go:33-41`), which lands as a second PushLog arrival for
//! that doc on the receiver. Go's `/debug/dump` cannot be used instead: it
//! aborts with HTTP 400 on the first blockstore key (`internal/db/db.go:498-527`
//! calls `HumanReadableKey`, whose `blockStoreKey` arm `cid.Cast`s a chunked
//! key) long before it reaches the `peers/` namespace.
//!
//! ```text
//! cargo build --offline -p cli && cp "$CARGO_TARGET_DIR/debug/defra" /tmp/defra-h2-nack
//! touch tools/integration-test/src/lib.rs
//! DEFRA_RUST_BINARY=/tmp/defra-h2-nack DEFRA_SKIP_VERSION_CHECK=1 \
//!   DEFRA_GO_BINARY=$HOME/.cache/defra-harness/53f0e76a3/defradb \
//!   RUST_LOG='info,p2p::sync::coordinator::event_handler=debug' \
//!   cargo test --offline -p integration-test --test spike_h2_go_nack -- --ignored --nocapture
//! ```

use std::collections::HashSet;
use std::path::PathBuf;
use std::time::Duration;

use integration_test::{BinarySource, TestCluster};
use serde_json::{json, Value};

const SCHEMA: &str = "type NackDoc { name: String  seq: Int }";
const DOCS: usize = 20;
/// Go's first replicator retry rung is 30s (`internal/db/config.go:33-41`) and
/// its retry loop ticks every 2s (`replicator.go:44`), so this spans it twice.
const SETTLE: Duration = Duration::from_secs(75);

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Clamped,
    Default,
    Down,
    Shed,
}

fn go_binary() -> PathBuf {
    std::env::var_os("DEFRA_GO_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").expect("HOME"))
                .join(".cache/defra-harness/53f0e76a3/defradb")
        })
}

async fn gql(http: &reqwest::Client, url: &str, query: &str) -> Value {
    http.post(format!("{url}/api/v0/graphql"))
        .json(&json!({ "query": query }))
        .send()
        .await
        .expect("graphql request")
        .json()
        .await
        .expect("graphql json")
}

async fn doc_count(http: &reqwest::Client, url: &str) -> usize {
    gql(http, url, "query { NackDoc { _docID } }").await["data"]["NackDoc"]
        .as_array()
        .map(|a| a.len())
        .unwrap_or(0)
}

fn log_path(cluster: &TestCluster, index: usize) -> PathBuf {
    cluster.nodes[index]
        .rootdir
        .parent()
        .expect("node rootdir has a parent")
        .join("logs/stdout.log")
}

fn read_log(path: &PathBuf) -> String {
    std::fs::read_to_string(path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

fn lines_with<'a>(log: &'a str, needle: &str) -> Vec<&'a str> {
    log.lines().filter(|l| l.contains(needle)).collect()
}

fn doc_id_of(line: &str) -> Option<&str> {
    line.split("doc_id=")
        .nth(1)
        .map(|rest| rest.split_whitespace().next().unwrap_or(rest))
}

struct Outcome {
    go_docs: usize,
    rust_docs: usize,
    arrivals: usize,
    distinct_arrivals: usize,
    nacks: usize,
    late_rejects: usize,
    reply_send_failures: usize,
    go_push_failures: usize,
    go_status_changes: usize,
    replicator_status: Value,
    samples: Vec<String>,
}

async fn run(mode: Mode) -> Outcome {
    let mut builder = TestCluster::builder()
        .rust_nodes(1)
        .go_nodes(1)
        .with_p2p()
        .with_go_binary(BinarySource::Path(go_binary()));
    if mode == Mode::Clamped {
        // Burst 1. The request limiter floors its refill at 1 token/s
        // (`crates/p2p/src/sync/rate_limiter.rs:100,166-172`), so a sub-second
        // burst of creates outruns it while the gossip limiter stays clamped
        // to the configured 0.02/s.
        builder = builder.with_extra_rust_args([
            "--p2p-rate-limit-burst",
            "1",
            "--p2p-rate-limit-rate",
            "0.02",
        ]);
    }
    if mode == Mode::Shed {
        std::env::set_var("DEFRA_P2P_MAX_PENDING_DAGS", "1");
    }
    let mut cluster = builder.build().await.expect("cluster start");
    std::env::remove_var("DEFRA_P2P_MAX_PENDING_DAGS");

    for node in 0..2 {
        cluster
            .wait_for_log(node, "p2p_listening", Duration::from_secs(30))
            .await
            .unwrap_or_else(|e| panic!("node{node} P2P listener: {e}"));
        cluster.client(node).schema_add(SCHEMA).expect("schema");
    }

    let rust = cluster.client(0);
    let go = cluster.client(1);
    let rust_addr = rust.p2p_info().expect("rust p2p info")[0]
        .as_str()
        .expect("rust address")
        .to_string();
    go.p2p_connect(&[&rust_addr]).expect("go connect");
    go.p2p_replicator_set(&["NackDoc"], &rust_addr)
        .expect("go replicator");

    let go_url = cluster.api_url(1).to_string();
    let rust_url = cluster.api_url(0).to_string();
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()
        .expect("http client");

    // Resolved before the receiver may be stopped: the file survives the stop,
    // but `stop_node` clears the slot it is derived from.
    let rust_log_path = log_path(&cluster, 0);
    let go_log_path = log_path(&cluster, 1);
    if mode == Mode::Down {
        cluster.stop_node(0).await.expect("stop receiver");
    }

    for seq in 0..DOCS {
        let body = gql(
            &http,
            &go_url,
            &format!(
                "mutation {{ add_NackDoc(input: [{{name: \"d{seq}\", seq: {seq}}}]) {{ _docID }} }}"
            ),
        )
        .await;
        assert!(body.get("errors").is_none(), "go create {seq}: {body}");
    }

    tokio::time::sleep(SETTLE).await;

    let rust_log = read_log(&rust_log_path);
    let go_log = read_log(&go_log_path);
    let arrivals = lines_with(
        &rust_log,
        "Host received PushLog request via two-stream protocol",
    );
    let distinct: HashSet<&str> = arrivals.iter().filter_map(|l| doc_id_of(l)).collect();
    let nacks: Vec<&str> = if mode == Mode::Shed {
        lines_with(
            &rust_log,
            "Pending DAGs at capacity, shedding PushLog before block verification",
        )
    } else {
        lines_with(&rust_log, "Rate limit exceeded, rejecting event")
            .into_iter()
            .filter(|l| l.contains("TwoStreamRequest"))
            .collect()
    };
    let go_failures = lines_with(&go_log, "Failed pushing log");
    let go_status = lines_with(&go_log, "Replicator status changed");

    let mut samples = Vec::new();
    for (label, line) in [
        ("rust arrival", arrivals.first()),
        ("rust nack", nacks.first()),
        ("go failure", go_failures.first()),
        ("go status", go_status.first()),
    ] {
        if let Some(line) = line {
            samples.push(format!("{label}: {line}"));
        }
    }

    Outcome {
        go_docs: doc_count(&http, &go_url).await,
        rust_docs: if mode == Mode::Down {
            0
        } else {
            doc_count(&http, &rust_url).await
        },
        arrivals: arrivals.len(),
        distinct_arrivals: distinct.len(),
        nacks: nacks.len(),
        late_rejects: lines_with(&rust_log, "rejecting PushLog DAG registration").len(),
        reply_send_failures: lines_with(&rust_log, "Failed to send two-stream response").len(),
        go_push_failures: go_failures.len(),
        go_status_changes: go_status.len(),
        replicator_status: go.p2p_replicator_list().expect("replicator list"),
        samples,
    }
}

fn report(label: &str, o: &Outcome) {
    println!("--- {label} ---");
    println!("go_docs={} rust_docs={}", o.go_docs, o.rust_docs);
    println!(
        "rust: pushlog arrivals={} (distinct docs {}) nacks={} late rejects={} reply-send failures={}",
        o.arrivals, o.distinct_arrivals, o.nacks, o.late_rejects, o.reply_send_failures
    );
    println!(
        "go: 'Failed pushing log'={} 'Replicator status changed'={}",
        o.go_push_failures, o.go_status_changes
    );
    println!("go replicator list: {}", o.replicator_status);
    for sample in &o.samples {
        println!("  {sample}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns a Go node and settles for 75s per phase"]
async fn go_pusher_reads_rust_nack_as_success() {
    let down = run(Mode::Down).await;
    report("receiver stopped (positive control)", &down);
    assert!(
        down.go_push_failures > 0 && down.go_status_changes > 0,
        "go's failure path never ran, so its silence under a nack proves nothing"
    );

    let nacked = run(Mode::Clamped).await;
    report("clamped receiver", &nacked);
    assert!(
        nacked.nacks > 0,
        "receiver never nacked; nothing was exercised"
    );
    assert_eq!(nacked.go_docs, DOCS, "go did not commit every document");
    assert!(
        nacked.rust_docs < DOCS,
        "every document arrived despite {} nacks",
        nacked.nacks
    );
    assert_eq!(
        nacked.go_push_failures, 0,
        "go logged push failures; it did read the nack"
    );
    assert_eq!(
        nacked.go_status_changes, 0,
        "go flipped the replicator to inactive, so handleReplicatorFailure ran"
    );
    assert_eq!(
        nacked.arrivals, nacked.distinct_arrivals,
        "a doc was pushed twice, so a /rep/retry/doc marker existed and fired"
    );

    let control = run(Mode::Default).await;
    report("default receiver (control)", &control);
    assert_eq!(control.nacks, 0, "control receiver nacked");
    assert_eq!(control.rust_docs, DOCS, "control did not replicate");
}

/// The same Go defect, reached through the pending-DAG capacity shed rather
/// than the rate limiter: every document Go commits must end up on the
/// receiver, because Go never retries a push it read as success.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns a Go node and settles for 75s"]
async fn go_pusher_loses_documents_to_the_capacity_shed() {
    let shed = run(Mode::Shed).await;
    report("single-slot receiver (capacity shed)", &shed);
    assert!(shed.nacks > 0, "receiver never shed; nothing was exercised");
    assert_eq!(shed.go_docs, DOCS, "go did not commit every document");
    assert_eq!(
        shed.go_push_failures, 0,
        "go logged push failures; it did read the nack"
    );
    assert_eq!(
        shed.go_status_changes, 0,
        "go flipped the replicator to inactive, so handleReplicatorFailure ran"
    );
    assert_eq!(
        shed.rust_docs, DOCS,
        "the receiver is missing documents go acked as delivered ({} shed)",
        shed.nacks
    );
}
