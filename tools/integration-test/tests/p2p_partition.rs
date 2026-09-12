//! H1 characterisation: how long does a Rust node take to converge after a
//! network partition that severs the TCP path without closing it?
//!
//! Soak run 1789012939-622 cut two Rust containers out of a six-node mesh with
//! `docker network disconnect`/`connect` (11.9 s and 21.2 s down). Convergence
//! lag INTO those nodes was 53 s p50 / 178 s p95 (rust-1) and 175 s / 261 s
//! (rust-2) for the next 300 s, with a multi-minute tail for the rest of the
//! 30-minute run. A second run with the same binary and different seed caught
//! up in 6-14 s p50. This test reproduces the cut locally so the slow path can
//! be told apart from seed variance.
//!
//! The cut is a black hole, not a close: a userspace TCP relay sits between the
//! two nodes and simply stops reading during the partition. No FIN, no RST --
//! both nodes keep believing the connection is alive, the sender's writes stall
//! against a zero window, and on heal the byte stream resumes intact. That is
//! the same shape as `docker network disconnect` followed by `connect` onto the
//! same IP, and it is the shape a clean process restart does NOT produce.
//!
//! Own test binary: it holds a listener and relay threads for its whole run.

use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use integration_test::TestCluster;

const SCHEMA: &str = "type Doc { label: String  seq: Int }";
/// Documents written on node0 while node1 is cut off.
const DURING_CUT: usize = 10;
/// Ceiling this test asserts. It is a characterisation bound, not a target:
/// the measured convergence is printed and the report carries the number.
/// A fix for H1 would bring convergence under `FIXED_THRESHOLD`.
const CONVERGE_CEILING: Duration = Duration::from_secs(240);
/// What "fixed" would mean: catch-up no slower than the outage plus one
/// replicator retry step.
const FIXED_THRESHOLD: Duration = Duration::from_secs(30);
/// Outages `rust_rust_node_down_three_cuts` inflicts on the same peer inside
/// one node0 process lifetime.
const CUTS: usize = 3;
/// Documents written on node0 during each of those outages. Short on purpose:
/// what is being measured is the schedule, not throughput.
const THREE_CUT_BURST: usize = 3;

/// Schema for `rust_rust_fetch_stall_21s`. The `body` field is the point: a
/// commit carrying it is not one small block, so a missed update chain leaves
/// a frontier that has to be fetched rather than one a PushLog carries inline.
const FETCH_SCHEMA: &str = "type Blob { label: String  seq: Int  body: String }";
/// Payload written into `body` on every create and every update.
const BODY_BYTES: usize = 8 * 1024;
/// Few documents, many updates: depth in the commit DAG, not width in the
/// document set, is what makes catch-up walk and fetch.
const STREAM_DOCS: usize = 4;
/// The outage, matching soak run 622 event 7 (21.4 s).
const FETCH_CUT: Duration = Duration::from_secs(21);
/// Ceiling for the fetch-stall measurement. Wider than `CONVERGE_CEILING`
/// because the shape it hunts is the soak's 175-192 s p50, and it is
/// *measured* rather than asserted: a run that does not converge records
/// `None` and still reports its rotation counts.
const FETCH_CEILING: Duration = Duration::from_secs(300);
/// Gap between writes in the continuous stream. The stream is load-bearing
/// twice over: it keeps the marker set non-empty, so `clear_retry_peer_once`
/// stays a no-op (`crates/storage/src/stores/peerstore.rs:795-801`), and it
/// keeps producing new heads for the rejoined node to chase.
const STREAM_INTERVAL: Duration = Duration::from_millis(350);
/// How long the stream keeps running past the heal.
const STREAM_AFTER_HEAL: Duration = Duration::from_secs(60);
/// More rotations than this on one root is a rotation *loop* rather than the
/// single pass through the alternates a healthy fetch makes. Four providers
/// (origin plus `MAX_PENDING_DAG_ALTERNATE_PROVIDERS = 3`,
/// `crates/p2p/src/sync/pending_store.rs:36`) is one clean rotation.
const ROTATION_LOOP_THRESHOLD: usize = 4;
/// What the node logs have to carry for the measurement to mean anything.
/// `Attempt stall budget exhausted` is DEBUG (`dag_fetcher.rs:772-777`) and
/// was absent from soak runs 622 and C1b, which is why they could not answer
/// this question.
const FETCH_RUST_LOG: &str = "info,p2p::sync::coordinator=debug,\
p2p::host::command_handler::bitswap=debug,defra_p2p_adapter=debug,iroh_bitswap=debug";

/// Which recovery machinery actually ran. `TestCluster::wait_for_log` matches
/// *registered pattern names*, not free text -- backbone
/// `defra-harness/src/observe/patterns.rs` defines exactly four -- so these are
/// the only signatures observable without a harness change. `peer_disconnected`
/// on node0 is the one that matters: it says whether the sender ever noticed
/// its socket had died.
const SIGNATURES: &[(usize, &str)] = &[
    (0, "p2p_listening"),
    (0, "peer_connected"),
    (0, "peer_disconnected"),
    (0, "replication_started"),
    (1, "peer_connected"),
    (1, "peer_disconnected"),
];

#[derive(Clone, Copy, PartialEq)]
enum CutMode {
    /// Freeze the relay: no FIN, no RST. Both ends keep believing the
    /// connection is up, exactly as a peer still on the network sees a
    /// container that was `docker network disconnect`ed.
    BlackHole,
    /// Close both sockets. libp2p sees `ConnectionClosed` at once. This is the
    /// control: it is what a process restart or a graceful leave looks like.
    Reset,
    /// Freeze the relay *and* stop node1's process, restarting it on the same
    /// ports and peer id before the heal. This is the full shape of a docker
    /// partition: the peer that stays on the network sees silence rather than
    /// a reset, while the partitioned node loses its own socket state and is
    /// unreachable at every address it advertises.
    NodeDown,
    /// Stop and restart node1 with the relay left open, so the sender sees an
    /// immediate reset instead of silence. Isolates "the peer went away" from
    /// "the sender was never told".
    RestartOnly,
}

/// A TCP relay that can be frozen mid-stream without closing either socket.
struct BlackHole {
    port: u16,
    open: Arc<AtomicBool>,
    accepted: Arc<AtomicUsize>,
    /// When each inbound connection was accepted. While node1's process is
    /// down every accept is a dial that reached the relay and found nothing
    /// behind it, so the gaps between them are the retry rungs node0 is
    /// actually walking -- the one thing the node logs never print.
    dials: Arc<Mutex<Vec<Instant>>>,
}

impl BlackHole {
    /// Listen on an ephemeral loopback port and relay to `target_port`.
    fn start(target_port: u16, mode: CutMode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind black hole");
        let port = listener.local_addr().expect("local_addr").port();
        let open = Arc::new(AtomicBool::new(true));
        let accepted = Arc::new(AtomicUsize::new(0));
        let dials = Arc::new(Mutex::new(Vec::new()));

        let gate = open.clone();
        let count = accepted.clone();
        let dialed = dials.clone();
        thread::spawn(move || {
            for inbound in listener.incoming() {
                let Ok(inbound) = inbound else { break };
                dialed.lock().expect("dial log").push(Instant::now());
                if mode == CutMode::Reset && !gate.load(Ordering::SeqCst) {
                    // A re-dial during a hard cut must fail, not hang.
                    continue;
                }
                let Ok(outbound) = TcpStream::connect(("127.0.0.1", target_port)) else {
                    continue;
                };
                count.fetch_add(1, Ordering::Relaxed);
                let (a, b) = (inbound, outbound);
                let (a2, b2) = (
                    a.try_clone().expect("clone inbound"),
                    b.try_clone().expect("clone outbound"),
                );
                let g1 = gate.clone();
                let g2 = gate.clone();
                thread::spawn(move || pump(a, b, g1, mode));
                thread::spawn(move || pump(b2, a2, g2, mode));
            }
        });

        BlackHole {
            port,
            open,
            accepted,
            dials,
        }
    }

    /// Seconds from `since` to each dial the relay accepted after it.
    fn dials_since(&self, since: Instant) -> Vec<f64> {
        self.dials
            .lock()
            .expect("dial log")
            .iter()
            .filter(|at| **at >= since)
            .map(|at| at.duration_since(since).as_secs_f64())
            .collect()
    }

    fn cut(&self) {
        self.open.store(false, Ordering::SeqCst);
    }

    fn heal(&self) {
        self.open.store(true, Ordering::SeqCst);
    }
}

/// Copy `from` into `to` while the gate is open. While it is shut we stop
/// reading entirely: bytes pile up in the kernel receive buffer, the window
/// closes, and the far side stalls without ever seeing an error.
fn pump(mut from: TcpStream, mut to: TcpStream, gate: Arc<AtomicBool>, mode: CutMode) {
    from.set_read_timeout(Some(Duration::from_millis(100)))
        .expect("read timeout");
    let mut buf = vec![0u8; 32 * 1024];
    loop {
        if !gate.load(Ordering::SeqCst) {
            if mode == CutMode::Reset {
                // Dropping both halves closes the connection.
                break;
            }
            thread::sleep(Duration::from_millis(50));
            continue;
        }
        match from.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => {
                if to.write_all(&buf[..n]).is_err() {
                    break;
                }
            }
            Err(e) if matches!(e.kind(), ErrorKind::WouldBlock | ErrorKind::TimedOut) => continue,
            Err(_) => break,
        }
    }
}

/// Split `/ip4/<ip>/tcp/<port>/p2p/<peer>` into its port and peer id.
fn split_multiaddr(addr: &str) -> (u16, String) {
    let parts: Vec<&str> = addr.split('/').collect();
    let port = parts
        .iter()
        .position(|p| *p == "tcp")
        .and_then(|i| parts.get(i + 1))
        .and_then(|p| p.parse().ok())
        .unwrap_or_else(|| panic!("no tcp port in {addr}"));
    let peer = parts
        .iter()
        .position(|p| *p == "p2p")
        .and_then(|i| parts.get(i + 1))
        .unwrap_or_else(|| panic!("no peer id in {addr}"))
        .to_string();
    (port, peer)
}

/// A document count that keeps the failure. The in-outage check needs to tell
/// "node1 is up and has not caught up" from "node1 is not answering at all";
/// collapsing both to `0` is what made the old `during <= 1` assertion vacuous
/// for the two modes that stop the process.
fn try_count_docs(cluster: &TestCluster, idx: usize) -> Result<usize, String> {
    cluster
        .client(idx)
        .query("query { Doc { _docID } }")
        .map_err(|e| e.to_string())
        .map(|v| v["Doc"].as_array().map(|a| a.len()).unwrap_or(0))
}

fn count_docs(cluster: &TestCluster, idx: usize) -> usize {
    try_count_docs(cluster, idx).unwrap_or(0)
}

/// node1's peer id as its own p2p info reports it.
fn peer_id_of(cluster: &TestCluster, idx: usize) -> String {
    let addr = cluster.client(idx).p2p_info().expect("p2p_info")[0]
        .as_str()
        .expect("address")
        .to_string();
    split_multiaddr(&addr).1
}

/// Wall clock, so a printed row can be lined up against the node log's own
/// timestamps. `Instant` cannot be.
fn epoch_secs() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// node0's replicator record for node1. `"status":0` is Active, which
/// `finish_peer` writes only after `clear_retry_peer` drained the marker set
/// (`crates/p2p-adapter/src/retry.rs:264-285`) -- the one path that resets the
/// rung. `1` is Inactive: markers survived the pass and the rung did not reset.
fn replicator_status(cluster: &TestCluster) -> String {
    cluster
        .client(0)
        .p2p_replicator_list()
        .map(|v| v.to_string())
        .unwrap_or_else(|e| format!("error: {e}"))
}

fn write_doc(cluster: &TestCluster, idx: usize, label: &str, seq: usize) {
    cluster
        .client(idx)
        .query(&format!(
            r#"mutation {{ add_Doc(input: {{label: "{label}", seq: {seq}}}) {{ _docID }} }}"#
        ))
        .unwrap_or_else(|e| panic!("create {label}: {e}"));
}

/// Poll until `want` docs land on node1, returning how long it took.
fn wait_for(cluster: &TestCluster, want: usize, limit: Duration) -> Option<Duration> {
    let start = Instant::now();
    while start.elapsed() < limit {
        if count_docs(cluster, 1) >= want {
            return Some(start.elapsed());
        }
        thread::sleep(Duration::from_millis(250));
    }
    None
}

/// Which of the recovery signatures appear in the node logs.
async fn probe_signatures(cluster: &TestCluster) -> Vec<String> {
    let mut seen = Vec::new();
    for (idx, pattern) in SIGNATURES {
        if cluster
            .wait_for_log(*idx, pattern, Duration::from_millis(500))
            .await
            .is_ok()
        {
            seen.push(format!("node{idx}:{pattern}"));
        }
    }
    seen
}

async fn partition_catchup(mode: CutMode, cut: Duration, name: &str) {
    partition_catchup_with(mode, cut, name, &[]).await
}

/// Build the two-node pair, route node0's replicator to node1 through the
/// relay, and replicate one baseline document over it. Returns the cluster,
/// the relay, node1's peer id and how long the baseline took.
///
/// `extra_args` are appended to both Rust nodes' command lines.
async fn proxied_pair(
    mode: CutMode,
    name: &str,
    extra_args: &[&str],
) -> (TestCluster, BlackHole, String, Duration) {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", "info,p2p=debug,defra_p2p_adapter=debug");
    }
    // A persistent store is mandatory here, not a preference: the harness
    // defaults to `--store memory` (backbone `defra-harness/src/node/rust_node.rs:120`),
    // so a node stopped and restarted by `CutMode::NodeDown` would come back
    // with no schema and no documents, and "never converged" would measure the
    // wipe rather than the partition.
    // A file keyring is as mandatory as the store. The harness default is
    // `KeyringBackend::None` (backbone `cluster/builder.rs:105`), which becomes
    // `--no-keyring` (`node/rust_node.rs:103`); with the keyring disabled the
    // node passes no peer keypair (`crates/cli/src/commands/start/node.rs:224`)
    // and libp2p generates a fresh ed25519 identity
    // (`crates/p2p/src/host/p2p_host/mod.rs:295`). A restarted node1 would come
    // back on a NEW peer id while node0's replicator record and retry markers
    // still name the old one, so every redial fails at the handshake and
    // "never converged" would measure that, not the partition. The harness
    // documents the same trap for Go at `cluster/builder.rs:309-312`.
    let mut builder = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .with_store("regolith")
        .with_file_keyring();
    if !extra_args.is_empty() {
        builder = builder.with_extra_rust_args(extra_args.iter().map(|a| a.to_string()));
    }
    let cluster = builder.build().await.expect("cluster");

    for idx in 0..2 {
        cluster
            .wait_for_log(idx, "p2p_listening", Duration::from_secs(30))
            .await
            .unwrap_or_else(|_| panic!("node{idx} P2P listener did not start"));
        cluster.client(idx).schema_add(SCHEMA).expect("schema");
    }

    let real = cluster.client(1).p2p_info().expect("p2p_info")[0]
        .as_str()
        .expect("node1 address")
        .to_string();
    let (real_port, peer_id) = split_multiaddr(&real);

    let hole = BlackHole::start(real_port, mode);
    let proxied = format!("/ip4/127.0.0.1/tcp/{}/p2p/{}", hole.port, peer_id);
    eprintln!("[h1:{name}] node1 real={real} proxied={proxied}");

    let node0 = cluster.client(0);
    node0.p2p_connect(&[&proxied]).expect("connect via proxy");
    node0.p2p_collection_add(&["Doc"]).expect("subscribe node0");
    cluster
        .client(1)
        .p2p_collection_add(&["Doc"])
        .expect("subscribe node1");
    node0
        .p2p_replicator_set(&["Doc"], &proxied)
        .expect("replicator via proxy");

    // Baseline: the proxied path replicates at all, and how fast.
    write_doc(&cluster, 0, "pre", 0);
    let baseline = wait_for(&cluster, 1, Duration::from_secs(60))
        .expect("baseline doc never replicated through the proxy");
    (cluster, hole, peer_id, baseline)
}

/// `extra_args` are appended to both Rust nodes' command lines.
async fn partition_catchup_with(mode: CutMode, cut: Duration, name: &str, extra_args: &[&str]) {
    let (mut cluster, hole, peer_id, baseline) = proxied_pair(mode, name, extra_args).await;

    if mode != CutMode::RestartOnly {
        hole.cut();
    }
    let stopped = if matches!(mode, CutMode::NodeDown | CutMode::RestartOnly) {
        Some(cluster.stop_node(1).await.expect("stop node1"))
    } else {
        None
    };
    let conns_before = hole.accepted.load(Ordering::Relaxed);
    let cut_at = Instant::now();
    for i in 0..DURING_CUT {
        write_doc(&cluster, 0, &format!("cut-{i}"), i + 1);
        thread::sleep(Duration::from_millis(200));
    }
    while cut_at.elapsed() < cut {
        thread::sleep(Duration::from_millis(200));
    }
    let during = try_count_docs(&cluster, 1);
    match mode {
        // A clean close is repaired inside the cut window: libp2p redials the
        // peer at the real address it learned over identify, so the relay is
        // bypassed entirely and replication never actually stops.
        CutMode::Reset => assert_eq!(
            during,
            Ok(DURING_CUT + 1),
            "[{name}] expected the clean-close cut to be routed around"
        ),
        // node1 is alive and answering; the cut held iff it is still at the
        // baseline document. Both halves matter: an answering node proves the
        // check is not vacuous, and the count proves the partition worked.
        CutMode::BlackHole => assert_eq!(
            during,
            Ok(1),
            "[{name}] black hole: node1 must answer and still hold only the baseline doc"
        ),
        // node1's process is stopped, so the only non-vacuous statement is that
        // it does not answer at all.
        CutMode::NodeDown | CutMode::RestartOnly => assert!(
            during.is_err(),
            "[{name}] node1 answered a query ({during:?}) while its process was stopped"
        ),
    }

    // Heal onto the same address, ports and peer id.
    let mut peer_id_after = peer_id.clone();
    if let Some(stopped) = stopped {
        cluster
            .start_stopped_node(stopped, Duration::from_secs(60))
            .await
            .expect("restart node1");
        peer_id_after = peer_id_of(&cluster, 1);
    }
    if mode != CutMode::RestartOnly {
        hole.heal();
    }
    let want = DURING_CUT + 1;
    let converged = wait_for(&cluster, want, CONVERGE_CEILING);
    let conns_after = hole.accepted.load(Ordering::Relaxed);
    let seen = probe_signatures(&cluster).await;
    // Replicator status is the observable proxy for durable retry markers:
    // `finish_peer` leaves a peer Active only when its marker set is empty
    // (`crates/p2p-adapter/src/retry.rs:264-285`), and `record_push_failure`
    // flips it Inactive (`retry.rs:99-108`). Active here means the dropped
    // documents were never handed to the persisted ladder at all.
    let replicators = replicator_status(&cluster);

    eprintln!(
        "[h1:{name}] RESULT baseline={baseline:?} cut={cut:?} docs={want} converged={converged:?} \
         ceiling={CONVERGE_CEILING:?} fixed_threshold={FIXED_THRESHOLD:?} \
         proxy_connections={conns_before}->{conns_after} peer_id_before={peer_id} \
         peer_id_after={peer_id_after} peer_id_stable={} signatures={seen:?} replicators={replicators}",
        peer_id_after == peer_id
    );

    // Without this the four restart modes measure a harness artifact rather
    // than the partition: node0's replicator, its address book and its retry
    // markers all name the pre-restart peer id.
    assert_eq!(
        peer_id_after, peer_id,
        "[{name}] node1 came back on a different peer id -- the keyring did not persist it"
    );

    let converged = converged.unwrap_or_else(|| {
        panic!(
            "[{name}] node1 still at {} of {want} docs {CONVERGE_CEILING:?} after the heal",
            count_docs(&cluster, 1),
        )
    });
    assert!(
        converged <= CONVERGE_CEILING,
        "[{name}] catch-up {converged:?} exceeded the characterised ceiling {CONVERGE_CEILING:?}"
    );
    assert!(
        converged <= FIXED_THRESHOLD,
        "[{name}] catch-up {converged:?} exceeded the fixed threshold {FIXED_THRESHOLD:?}"
    );
}

/// Cut the *same* peer `CUTS` times inside one node0 process lifetime.
///
/// The durable rung is monotonic: every failed pass bumps `num_retries`
/// (`crates/p2p-adapter/src/retry.rs:250`, `:258`), a successful replay does
/// nothing to it (`:224-232`), and the only reset is `clear_retry_peer` from
/// `finish_peer` once the peer's marker set drains (`:264-269`, `:272-285`).
/// One outage therefore only ever walks the first two or three rungs, which is
/// tens of seconds. Whether a *repeated* outage keeps climbing -- turning tens
/// of seconds into the minutes run 622 reported -- is what this measures, and
/// every other test in this file partitions a fresh peer exactly once.
///
/// Each cut prints its own row before the assertions run, so a cut that never
/// converges is still reported rather than lost to the panic.
async fn three_cut_catchup(name: &str, cut: Duration, extra_args: &[&str]) {
    let (mut cluster, hole, peer_id, baseline) =
        proxied_pair(CutMode::NodeDown, name, extra_args).await;

    let mut total = 1usize;
    let mut converged = Vec::new();
    for n in 1..=CUTS {
        hole.cut();
        let stopped = cluster.stop_node(1).await.expect("stop node1");
        let cut_at = Instant::now();
        for i in 0..THREE_CUT_BURST {
            write_doc(&cluster, 0, &format!("c{n}-{i}"), total + i);
            thread::sleep(Duration::from_millis(200));
        }
        total += THREE_CUT_BURST;
        while cut_at.elapsed() < cut {
            thread::sleep(Duration::from_millis(200));
        }
        let during = try_count_docs(&cluster, 1);
        assert!(
            during.is_err(),
            "[{name}] cut {n}: node1 answered a query ({during:?}) while its process was stopped"
        );
        // Every dial the relay took while node1 was gone found nothing behind
        // it, so these are the sweep's redials and their gaps are the rungs.
        let redials = hole.dials_since(cut_at);

        cluster
            .start_stopped_node(stopped, Duration::from_secs(60))
            .await
            .expect("restart node1");
        let peer_id_after = peer_id_of(&cluster, 1);
        hole.heal();
        let healed_at = epoch_secs();
        let caught_up = wait_for(&cluster, total, CONVERGE_CEILING);
        converged.push(caught_up);

        eprintln!(
            "[h1:{name}] CUT {n} docs={total} converged={caught_up:?} \
             outage_redials_s={redials:?} heal_epoch={healed_at:.3} \
             peer_id_stable={} replicators={}",
            peer_id_after == peer_id,
            replicator_status(&cluster),
        );
        assert_eq!(
            peer_id_after, peer_id,
            "[{name}] cut {n}: node1 came back on a different peer id"
        );
    }

    eprintln!(
        "[h1:{name}] RESULT baseline={baseline:?} cut={cut:?} cuts={CUTS} \
         burst={THREE_CUT_BURST} docs={total} converged={converged:?} \
         ceiling={CONVERGE_CEILING:?} fixed_threshold={FIXED_THRESHOLD:?}"
    );

    for (i, caught_up) in converged.iter().enumerate() {
        let caught_up = caught_up.unwrap_or_else(|| {
            panic!(
                "[{name}] cut {} never converged inside {CONVERGE_CEILING:?}",
                i + 1
            )
        });
        assert!(
            caught_up <= FIXED_THRESHOLD,
            "[{name}] cut {} catch-up {caught_up:?} exceeded the fixed threshold \
             {FIXED_THRESHOLD:?}",
            i + 1
        );
    }
}

/// Three 60 s outages on one peer, default ladder. If the rung survives a
/// successful catch-up, cut 3 waits on a rung several steps up and the third
/// number is minutes rather than the ~38 s p50 a single 60 s cut costs.
#[tokio::test]
async fn rust_rust_node_down_three_cuts() {
    three_cut_catchup("threecut", Duration::from_secs(60), &[]).await;
}

/// The same three cuts with the ladder flattened, so any climb across cuts is
/// separated from the ladder's own shape.
#[tokio::test]
async fn rust_rust_node_down_three_cuts_flat_ladder() {
    three_cut_catchup(
        "threecut-flat",
        Duration::from_secs(60),
        &["--replicator-retry-intervals", "2,2,2,2"],
    )
    .await;
}

/// Every `Blob`'s `seq` on node `idx`, keyed by document id. `None` when the
/// node does not answer at all, which is how the in-outage check stays
/// non-vacuous.
fn blob_seqs(cluster: &TestCluster, idx: usize) -> Option<HashMap<String, i64>> {
    let value = cluster
        .client(idx)
        .query("query { Blob { _docID seq } }")
        .ok()?;
    let rows = value["Blob"].as_array()?;
    Some(
        rows.iter()
            .filter_map(|row| Some((row["_docID"].as_str()?.to_string(), row["seq"].as_i64()?)))
            .collect(),
    )
}

/// True once every document has reached at least the `seq` it held on node0
/// when the partition healed. Counting documents would not do: the stream is
/// mostly updates, so the document count never moves and the whole missed
/// commit chain would be invisible.
fn caught_up(cluster: &TestCluster, idx: usize, want: &HashMap<String, i64>) -> bool {
    let Some(have) = blob_seqs(cluster, idx) else {
        return false;
    };
    want.iter()
        .all(|(id, seq)| have.get(id).is_some_and(|got| got >= seq))
}

fn create_blob(cluster: &TestCluster, label: &str, seq: usize, body: &str) -> String {
    let value = cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ add_Blob(input: {{label: "{label}", seq: {seq}, body: "{body}"}}) {{ _docID }} }}"#
        ))
        .unwrap_or_else(|e| panic!("create {label}: {e}"));
    value["add_Blob"][0]["_docID"]
        .as_str()
        .unwrap_or_else(|| panic!("no _docID for {label}"))
        .to_string()
}

fn update_blob(cluster: &TestCluster, doc_id: &str, seq: usize, body: &str) {
    cluster
        .client(0)
        .query(&format!(
            r#"mutation {{ update_Blob(docID: "{doc_id}", input: {{seq: {seq}, body: "{body}"}}) {{ _docID }} }}"#
        ))
        .unwrap_or_else(|e| panic!("update {doc_id} to seq {seq}: {e}"));
}

/// node `idx`'s stdout log. The harness keeps the run directory only under
/// `DEFRA_E2E_KEEP=1` (backbone `cluster/builder.rs:420-423`), which
/// `runs-h1/run.sh` sets; without it there is no log and no measurement, so
/// this panics rather than reporting a silent zero.
fn node_log(idx: usize) -> String {
    let root = std::env::var("DEFRA_WORKSPACE_ROOT")
        .expect("DEFRA_WORKSPACE_ROOT must point at this worktree");
    let e2e = std::path::Path::new(&root).join("target/e2e");
    let mut newest: Option<(std::time::SystemTime, std::path::PathBuf)> = None;
    for entry in std::fs::read_dir(&e2e)
        .unwrap_or_else(|e| panic!("{}: {e} -- run with DEFRA_E2E_KEEP=1", e2e.display()))
        .flatten()
    {
        let log = entry
            .path()
            .join(format!("rust-{idx}"))
            .join("logs/stdout.log");
        if !log.exists() {
            continue;
        }
        let at = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(std::time::UNIX_EPOCH);
        if newest.as_ref().is_none_or(|(best, _)| at >= *best) {
            newest = Some((at, log));
        }
    }
    let path = newest
        .unwrap_or_else(|| panic!("no rust-{idx} stdout.log under {}", e2e.display()))
        .1;
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Provider rotations in one node's log, bucketed by root CID. The WARN at
/// `crates/p2p/src/sync/coordinator/dag_fetcher.rs:786-793` is emitted at the
/// same site as `record_provider_rotation`, so this counts rotations exactly.
/// Returns (total, distinct roots, worst root's count).
fn rotations_by_root(log: &str) -> (usize, usize, usize) {
    let mut per_root: HashMap<&str, usize> = HashMap::new();
    let mut total = 0usize;
    for line in log.lines() {
        if !line.contains("No blocks from provider within fetch window") {
            continue;
        }
        total += 1;
        let root = line
            .split("root_cid=")
            .nth(1)
            .and_then(|rest| rest.split_whitespace().next())
            .unwrap_or("?");
        *per_root.entry(root).or_default() += 1;
    }
    let worst = per_root.values().copied().max().unwrap_or(0);
    (total, per_root.len(), worst)
}

/// The corrected H1 model: a symmetric black hole, a continuous write stream,
/// commit DAGs wide enough to need a real block fetch, and a third node so the
/// provider rotation has somewhere to rotate to.
///
/// Why this shape, and not the `HalfClose` asymmetry an earlier draft proposed:
/// the log check on soak runs 622 and C1b found **no** `Peer connected` or
/// `Peer disconnected` naming the partitioned node at event 7, on any node,
/// in either run -- the only such transitions in the whole 30 minutes are its
/// two graceful leaves. So a `docker network disconnect` is invisible to
/// libp2p on *both* ends, nobody re-dials, `redundant_connections`
/// (`crates/p2p/src/host/p2p_host/connection_manager.rs:103-134`) is never
/// called, and there is no stale-connection dedup to build a test around.
/// The partition's real shape is the symmetric black hole this file already
/// has. What 622 shows instead is receiver-side: the partitioned node rotates
/// providers on one root ten to fifteen times over 95-300 s, on a 10 s
/// cadence with a 2 s gap every fourth -- four providers
/// (`crates/p2p/src/sync/pending_store.rs:36`), three attempts
/// (`.../coordinator/dag_retry.rs:16`), the bitswap per-block window
/// (`crates/p2p/src/host/command_handler/bitswap.rs:74`) -- while the two
/// unpartitioned nodes never rotate a root more than once. This test tries to
/// make that loop happen locally, which no run has yet done.
///
/// Topology: node1 sits behind the relay and node0 *and* node2 reach it only
/// through that relay, so one cut isolates node1 from both, as the container
/// partition does. node0 replicates to node2 directly, so node2 holds every
/// block written during the outage and is a genuine alternate provider for
/// node1's catch-up. node2 is also the control: it is connected throughout and
/// takes the same stream, so its rotation count is the same measurement on an
/// unpartitioned node in the same process -- the local mirror of the soak's
/// rust-2 against rust-0/rust-1.
///
/// Measured, not asserted: convergence is recorded as `Option<Duration>` under
/// a 300 s ceiling and a run that never converges reports `None` rather than
/// panicking, because a non-convergence with its rotation counts is exactly the
/// result worth having. The only hard assertions are that the cut actually held
/// and that the logs were readable.
///
/// Needs `DEFRA_E2E_KEEP=1` and `RUST_LOG` at least as verbose as
/// `FETCH_RUST_LOG`; it sets that itself if `RUST_LOG` is unset.
#[tokio::test]
#[ignore = "pending a quiet host: three nodes and ~6 minutes of wall time; the soak queue owns this machine"]
async fn rust_rust_fetch_stall_21s() {
    if std::env::var_os("RUST_LOG").is_none() {
        std::env::set_var("RUST_LOG", FETCH_RUST_LOG);
    }
    let cluster = TestCluster::builder()
        .rust_nodes(3)
        .with_p2p()
        .with_store("regolith")
        .with_file_keyring()
        .build()
        .await
        .expect("cluster");

    for idx in 0..3 {
        cluster
            .wait_for_log(idx, "p2p_listening", Duration::from_secs(30))
            .await
            .unwrap_or_else(|_| panic!("node{idx} P2P listener did not start"));
        cluster
            .client(idx)
            .schema_add(FETCH_SCHEMA)
            .expect("schema");
    }

    let real = cluster.client(1).p2p_info().expect("p2p_info")[0]
        .as_str()
        .expect("node1 address")
        .to_string();
    let (real_port, peer_id) = split_multiaddr(&real);
    let node2_addr = cluster.client(2).p2p_info().expect("p2p_info")[0]
        .as_str()
        .expect("node2 address")
        .to_string();

    let hole = BlackHole::start(real_port, CutMode::BlackHole);
    let proxied = format!("/ip4/127.0.0.1/tcp/{}/p2p/{}", hole.port, peer_id);
    eprintln!("[h1:fetch-stall] node1 real={real} proxied={proxied} node2={node2_addr}");

    // Everything that reaches node1 goes through the relay, so one cut
    // isolates it from both peers. node2 is reached directly and stays up.
    cluster
        .client(0)
        .p2p_connect(&[&proxied, &node2_addr])
        .expect("node0 connect");
    cluster
        .client(2)
        .p2p_connect(&[&proxied])
        .expect("node2 connect to node1 via proxy");
    for idx in 0..3 {
        cluster
            .client(idx)
            .p2p_collection_add(&["Blob"])
            .expect("subscribe");
    }
    cluster
        .client(0)
        .p2p_replicator_set(&["Blob"], &proxied)
        .expect("replicator to node1");
    cluster
        .client(0)
        .p2p_replicator_set(&["Blob"], &node2_addr)
        .expect("replicator to node2");

    let body = "x".repeat(BODY_BYTES);
    let mut docs: Vec<(String, usize)> = Vec::new();
    for i in 0..STREAM_DOCS {
        docs.push((create_blob(&cluster, &format!("blob-{i}"), 0, &body), 0));
    }
    let baseline = Instant::now();
    while baseline.elapsed() < Duration::from_secs(60) {
        if blob_seqs(&cluster, 1).is_some_and(|s| s.len() >= STREAM_DOCS)
            && blob_seqs(&cluster, 2).is_some_and(|s| s.len() >= STREAM_DOCS)
        {
            break;
        }
        thread::sleep(Duration::from_millis(250));
    }
    assert!(
        blob_seqs(&cluster, 1).is_some_and(|s| s.len() >= STREAM_DOCS),
        "[fetch-stall] baseline never replicated to node1 through the proxy"
    );
    let baseline = baseline.elapsed();

    // The stream: one update every STREAM_INTERVAL, round-robin over the
    // documents, from the cut until 60 s past the heal.
    hole.cut();
    let cut_at = Instant::now();
    let mut next = 0usize;
    while cut_at.elapsed() < FETCH_CUT {
        let (id, seq) = &mut docs[next % STREAM_DOCS];
        *seq += 1;
        update_blob(&cluster, id.as_str(), *seq, &body);
        next += 1;
        thread::sleep(STREAM_INTERVAL);
    }

    // node1 is alive and must answer, and must still be behind: both halves
    // matter, or "it caught up" is measuring nothing.
    let during = blob_seqs(&cluster, 1);
    assert!(
        during.is_some(),
        "[fetch-stall] node1 stopped answering during a black hole; its process should be up"
    );
    let behind = during
        .expect("checked")
        .values()
        .copied()
        .max()
        .unwrap_or(0);
    assert!(
        behind < docs.iter().map(|(_, s)| *s).max().unwrap_or(0) as i64,
        "[fetch-stall] the cut did not hold: node1 is level with node0 at seq {behind}"
    );

    hole.heal();
    let healed_at = Instant::now();
    let want: HashMap<String, i64> = docs
        .iter()
        .map(|(id, seq)| (id.clone(), *seq as i64))
        .collect();

    let mut converged = None;
    let mut control = None;
    loop {
        let elapsed = healed_at.elapsed();
        if elapsed >= FETCH_CEILING {
            break;
        }
        if elapsed < STREAM_AFTER_HEAL {
            let (id, seq) = &mut docs[next % STREAM_DOCS];
            *seq += 1;
            update_blob(&cluster, id.as_str(), *seq, &body);
            next += 1;
        }
        if converged.is_none() && caught_up(&cluster, 1, &want) {
            converged = Some(elapsed);
        }
        if control.is_none() && caught_up(&cluster, 2, &want) {
            control = Some(elapsed);
        }
        if converged.is_some() && elapsed >= STREAM_AFTER_HEAL {
            break;
        }
        thread::sleep(STREAM_INTERVAL);
    }

    let partitioned = node_log(1);
    let unpartitioned = node_log(2);
    let (rot1, roots1, worst1) = rotations_by_root(&partitioned);
    let (rot2, roots2, worst2) = rotations_by_root(&unpartitioned);
    let stall_budget = partitioned
        .matches("Attempt stall budget exhausted")
        .count();
    let batch_timeouts = partitioned
        .matches("Timeout fetching selective block batch")
        .count();
    let loop_seen = worst1 > ROTATION_LOOP_THRESHOLD;

    eprintln!(
        "[h1:fetch-stall] RESULT baseline={baseline:?} cut={FETCH_CUT:?} updates={next} \
         converged_node1={converged:?} converged_node2={control:?} ceiling={FETCH_CEILING:?} \
         rotations_node1={rot1} roots_node1={roots1} worst_root_node1={worst1} \
         rotations_node2={rot2} roots_node2={roots2} worst_root_node2={worst2} \
         stall_budget_exhausted={stall_budget} selective_batch_timeouts={batch_timeouts} \
         rotation_loop={loop_seen} peer_id={peer_id}"
    );
    if loop_seen {
        eprintln!(
            "[h1:fetch-stall] REPRODUCED: one root rotated {worst1} times on the partitioned \
             node against {worst2} on the control -- this is the loop soak run 622 shows and \
             no local run has had before"
        );
    }

    assert!(
        rot1 + rot2 > 0 || converged.is_some(),
        "[fetch-stall] neither node converged and neither log recorded a single provider \
         rotation -- the logs are probably not verbose enough; RUST_LOG must include \
         p2p::sync::coordinator=debug"
    );
}

/// The briefed shape: a 15 s cut, shorter than the 30 s push send timeout.
#[tokio::test]
async fn rust_rust_black_hole_cut_15s() {
    partition_catchup(CutMode::BlackHole, Duration::from_secs(15), "blackhole-15s").await;
}

/// Long enough that `DEFAULT_PUSH_SEND_TIMEOUT` (30 s) fires and the durable
/// replicator ladder is definitely engaged before the heal.
#[tokio::test]
async fn rust_rust_black_hole_cut_45s() {
    partition_catchup(CutMode::BlackHole, Duration::from_secs(45), "blackhole-45s").await;
}

/// Control: the same outage delivered as a clean close.
#[tokio::test]
async fn rust_rust_reset_cut_45s() {
    partition_catchup(CutMode::Reset, Duration::from_secs(45), "reset-45s").await;
}

/// The full docker-partition shape: silence toward the sender, lost socket
/// state on the receiver. If this converges far slower than `black_hole` and
/// `reset`, the slow path is real and the discriminator is the reset the
/// sender never receives.
#[tokio::test]
async fn rust_rust_node_down_cut_45s() {
    partition_catchup(CutMode::NodeDown, Duration::from_secs(45), "nodedown-45s").await;
}

/// The briefed 15 s outage in that same full shape.
#[tokio::test]
async fn rust_rust_node_down_cut_15s() {
    partition_catchup(CutMode::NodeDown, Duration::from_secs(15), "nodedown-15s").await;
}

/// Discriminator for the durable replicator ladder
/// (`RETRY_INTERVALS_SECS = [30, 60, 120, 240, 480, 960, 1920]`,
/// `crates/storage/src/stores/retry_info.rs:14`). Same partition, same shape,
/// but the ladder is flattened to a flat 2 s via `--replicator-retry-intervals`.
/// If this converges quickly while `rust_rust_node_down_cut_15s` does not, the
/// ladder is what holds the documents back; if it also fails, the cause is on
/// the receiver and the ladder is a bystander.
#[tokio::test]
async fn rust_rust_node_down_cut_15s_flat_ladder() {
    partition_catchup_with(
        CutMode::NodeDown,
        Duration::from_secs(15),
        "nodedown-15s-flat",
        &["--replicator-retry-intervals", "2,2,2,2"],
    )
    .await;
}

/// Same downtime, but the sender is told: the relay stays open, so node1's
/// death arrives as a reset. If this converges and `rust_rust_node_down_cut_15s`
/// does not, silence toward the sender is the trigger; if both fail, any
/// downtime loses the writes and the partition is incidental.
#[tokio::test]
async fn rust_rust_restart_only_15s() {
    partition_catchup(CutMode::RestartOnly, Duration::from_secs(15), "restart-15s").await;
}

/// A 60 s outage: longer than the first default ladder rung (30 s), so the
/// persisted replicator retry is certain to have fired and failed at least
/// once before the heal. Paired with the flat-ladder variant below, this is
/// the second point of the p50 measurement the rerun brief asks for.
#[tokio::test]
async fn rust_rust_node_down_cut_60s() {
    partition_catchup(CutMode::NodeDown, Duration::from_secs(60), "nodedown-60s").await;
}

/// The 60 s outage with the ladder flattened to 2 s, isolating pacing from
/// everything else at the longer outage.
#[tokio::test]
async fn rust_rust_node_down_cut_60s_flat_ladder() {
    partition_catchup_with(
        CutMode::NodeDown,
        Duration::from_secs(60),
        "nodedown-60s-flat",
        &["--replicator-retry-intervals", "2,2,2,2"],
    )
    .await;
}
