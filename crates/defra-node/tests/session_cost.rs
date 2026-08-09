//! What a reconciliation session costs to set up on a real node.
//!
//! The engine-level measurement in `p2p/tests/reconcile_session_memory.rs`
//! builds its head set from synthetic identities in memory. This is the node
//! scale spot-check the acceptance criteria ask for: the same two structures,
//! but built from a real store through the real provider, so the storage read
//! is inside the number.
//!
//! Its own binary because it installs a counting allocator, which a
//! `#[global_allocator]` applies to everything in the binary it lives in. Put in
//! the library's test module it would wrap every other `defra-node` test's
//! allocations too, for one benchmark's benefit.
//!
//! ```text
//! DEFRA_SYNC_BENCH_OUT=/tmp/ranges \
//!   cargo test -p defra-node --features p2p --test session_cost \
//!   -- --ignored --nocapture
//! ```

#![cfg(feature = "p2p")]

use std::alloc::{GlobalAlloc, Layout, System};
use std::net::{IpAddr, Ipv4Addr};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use defra_node::{EmbeddedNode, P2PConfig};
use p2p::reconcile::engine::rbsr::SegmentTree;

/// Live heap bytes, tracked by wrapping the system allocator.
///
/// This counts *requested* bytes: allocator metadata, size-class rounding and
/// fragmentation are outside it, so the totals are a floor on resident memory,
/// not a measurement of it.
struct Counting;

static LIVE: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let pointer = unsafe { System.alloc(layout) };
        if !pointer.is_null() {
            LIVE.fetch_add(layout.size(), Ordering::Relaxed);
        }
        pointer
    }

    unsafe fn dealloc(&self, pointer: *mut u8, layout: Layout) {
        LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(pointer, layout) }
    }

    unsafe fn realloc(&self, pointer: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let moved = unsafe { System.realloc(pointer, layout, new_size) };
        if !moved.is_null() {
            LIVE.fetch_add(new_size, Ordering::Relaxed);
            LIVE.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        moved
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

const SDL: &str = "type BenchDoc { name: String value: Int }";
const COLLECTION: &str = "BenchDoc";

fn bench_p2p_config() -> P2PConfig {
    P2PConfig {
        port: 0,
        bind_addr: Some(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        relay_mode: p2p::iroh::IrohRelayModeConfig::Disabled,
        discovery: p2p::iroh::IrohDiscoveryConfig::Disabled,
        max_concurrent_multipath_paths: None,
        secret_key_path: None,
        load_persisted_collections: false,
        max_concurrent_dag_fetches: p2p::sync::DEFAULT_MAX_CONCURRENT_DAG_FETCHES,
        max_concurrent_push_tasks: p2p::sync::DEFAULT_MAX_CONCURRENT_PUSH_TASKS,
        max_doc_sync_request_doc_ids: p2p::sync::DEFAULT_MAX_DOC_SYNC_REQUEST_DOC_IDS,
        rate_limit_burst: p2p::sync::DEFAULT_RATE_LIMIT_BURST,
        rate_limit_rate: p2p::sync::DEFAULT_RATE_LIMIT_RATE,
        max_pending_dags: p2p::sync::DEFAULT_MAX_PENDING_DAGS,
        counters: None,
        reconcile_enabled: true,
    }
}

async fn node_with_documents(docs: usize) -> EmbeddedNode {
    let node = EmbeddedNode::builder()
        .with_p2p(bench_p2p_config())
        .build()
        .await
        .expect("build bench node");
    node.add_schema(SDL).await.expect("add bench schema");
    for index in 0..docs {
        let response = node
            .execute(&format!(
                r#"mutation {{ add_BenchDoc(input: {{name: "doc-{index:06}", value: {index}}}) {{ _docID }} }}"#
            ))
            .await;
        assert!(
            response.errors.is_empty(),
            "seed failed: {:?}",
            response.errors
        );
    }
    node
}

/// One measurement: how long the provider takes to seal the collection's head
/// set and fold its tree, and what the pair holds once it has.
///
/// The retained figure is read across the *drop*, not across the build. A live
/// node is allocating and freeing on other threads throughout a build that takes
/// hundreds of milliseconds, so a before-and-after delta over that window
/// measures the node as much as the session — at n=250 it came out negative.
/// Dropping the tree releases the source with it, synchronously and in
/// microseconds, so the bytes released across that instant are the structures'
/// own with far less of the node mixed in.
async fn measure(node: &EmbeddedNode) -> (usize, f64, usize) {
    let provider = node
        .reconcile_source()
        .expect("node was not built with reconciliation enabled")
        .clone();

    let started = Instant::now();
    let source = provider.snapshot(COLLECTION).await.expect("snapshot");
    let items = p2p::reconcile::ItemSource::len(&source);
    let tree = SegmentTree::build(source);
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;

    assert!(!tree.is_empty());
    let held = LIVE.load(Ordering::Relaxed);
    drop(tree);
    let retained = held.saturating_sub(LIVE.load(Ordering::Relaxed));
    (items, elapsed_ms, retained)
}

fn out_path() -> PathBuf {
    std::env::var("DEFRA_SYNC_BENCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/sync-bench"))
        .join("session_cost_node.csv")
}

/// Three runs per size, reported as min and median.
///
/// Unlike the engine-level measurement this reads a real store, so neither the
/// time nor the allocation total is guaranteed steady and a single draw would
/// not say which.
///
/// The four sizes are what makes this a shape measurement rather than three
/// numbers. Per-item build cost used to double with every doubling of `n` —
/// 0.81, 1.60, 3.22 ms at 250, 500 and 1,000 — because the snapshot opened one
/// store iterator per document and opening one costs time proportional to the
/// whole store. The assertion at the end is the shape, with enough headroom
/// that machine noise cannot trip it: quadratic multiplies the per-item cost by
/// eight across this range, linear leaves it flat.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark: builds a real node and seeds up to two thousand documents"]
async fn node_level_session_cost() {
    const RUNS: usize = 3;
    const SIZES: [usize; 4] = [250, 500, 1000, 2000];
    let mut csv = String::from(
        "side,n,runs,items,buildMsMin,buildMsMedian,retainedBytesMin,retainedBytesMedian,bytesPerItemMedian,msPerItemMedian\n",
    );
    let mut per_item_ms = Vec::with_capacity(SIZES.len());

    for docs in SIZES {
        let node = node_with_documents(docs).await;

        let mut times = Vec::with_capacity(RUNS);
        let mut retained = Vec::with_capacity(RUNS);
        let mut items = 0;
        for _ in 0..RUNS {
            let (n, ms, bytes) = measure(&node).await;
            items = n;
            times.push(ms);
            retained.push(bytes);
        }
        node.shutdown().await;

        times.sort_by(|a, b| a.partial_cmp(b).expect("finite"));
        retained.sort_unstable();
        let median_bytes = retained[RUNS / 2];
        let per_item = median_bytes as f64 / items.max(1) as f64;
        let ms_per_item = times[RUNS / 2] / items.max(1) as f64;
        per_item_ms.push((docs, ms_per_item));
        println!(
            "n={docs}: {items} items, build {:.2} ms (median of {RUNS}), {ms_per_item:.4} ms/item, retained {median_bytes} B, {per_item:.1} B/item",
            times[RUNS / 2]
        );
        csv.push_str(&format!(
            "node_session_storage,{docs},{RUNS},{items},{:.2},{:.2},{},{median_bytes},{per_item:.1},{ms_per_item:.4}\n",
            times[0], times[RUNS / 2], retained[0]
        ));
    }

    let path = out_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create out dir");
    }
    std::fs::write(&path, &csv).expect("write node session cost csv");
    println!("{}\n{csv}", path.display());

    let (small_n, small) = per_item_ms[0];
    let (large_n, large) = per_item_ms[per_item_ms.len() - 1];
    assert!(
        large <= small * 2.0,
        "per-item build cost went from {small:.4} ms at n={small_n} to {large:.4} ms at n={large_n}; \
         a per-item cost that grows with n is a superlinear read, not a constant factor"
    );
}
