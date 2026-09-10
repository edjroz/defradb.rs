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

use std::io::{ErrorKind, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
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

/// Log lines that say which recovery machinery actually ran. Probed on both
/// nodes after the heal so the two candidate mechanisms can be told apart.
const SIGNATURES: &[(usize, &str)] = &[
    (0, "p2p_listening"),
    (0, "Peer disconnected"),
    (0, "retry push failed"),
    (0, "retry push timed out"),
    (0, "Peer connected"),
    (1, "Timeout fetching selective block batch"),
    (1, "Bitswap timeout waiting for block"),
    (1, "DAG fetch failed after exhausting retries"),
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
}

/// A TCP relay that can be frozen mid-stream without closing either socket.
struct BlackHole {
    port: u16,
    open: Arc<AtomicBool>,
    accepted: Arc<AtomicUsize>,
}

impl BlackHole {
    /// Listen on an ephemeral loopback port and relay to `target_port`.
    fn start(target_port: u16, mode: CutMode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind black hole");
        let port = listener.local_addr().expect("local_addr").port();
        let open = Arc::new(AtomicBool::new(true));
        let accepted = Arc::new(AtomicUsize::new(0));

        let gate = open.clone();
        let count = accepted.clone();
        thread::spawn(move || {
            for inbound in listener.incoming() {
                let Ok(inbound) = inbound else { break };
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
        }
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

fn count_docs(cluster: &TestCluster, idx: usize) -> usize {
    cluster
        .client(idx)
        .query("query { Doc { _docID } }")
        .ok()
        .and_then(|v| v["Doc"].as_array().map(|a| a.len()))
        .unwrap_or(0)
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
    std::env::set_var("RUST_LOG", "info,p2p=debug,defra_p2p_adapter=debug");
    let mut cluster = TestCluster::builder()
        .rust_nodes(2)
        .with_p2p()
        .build()
        .await
        .expect("cluster");

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

    hole.cut();
    let stopped = if mode == CutMode::NodeDown {
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
    let during = count_docs(&cluster, 1);
    match mode {
        // A clean close is repaired inside the cut window: libp2p redials the
        // peer at the real address it learned over identify, so the relay is
        // bypassed entirely and replication never actually stops.
        CutMode::Reset => assert_eq!(
            during,
            DURING_CUT + 1,
            "[{name}] expected the clean-close cut to be routed around"
        ),
        _ => assert!(
            during <= 1,
            "[{name}] the cut did not partition the nodes -- node1 saw {during} docs"
        ),
    }

    // Heal onto the same address, ports and peer id.
    if let Some(stopped) = stopped {
        cluster
            .start_stopped_node(stopped, Duration::from_secs(60))
            .await
            .expect("restart node1");
    }
    hole.heal();
    let want = DURING_CUT + 1;
    let converged = wait_for(&cluster, want, CONVERGE_CEILING);
    let conns_after = hole.accepted.load(Ordering::Relaxed);
    let seen = probe_signatures(&cluster).await;

    eprintln!(
        "[h1:{name}] RESULT baseline={baseline:?} cut={cut:?} docs={want} converged={converged:?} \
         ceiling={CONVERGE_CEILING:?} fixed_threshold={FIXED_THRESHOLD:?} \
         proxy_connections={conns_before}->{conns_after} signatures={seen:?}"
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
