//! What one reconciliation session retains, measured rather than estimated.
//!
//! Rust pays no per-commit write tax for reconciliation — the priority index a
//! session reads is product code that already exists — but it does pay a
//! per-session read and build: the collection's whole head set is materialised
//! into a [`MemorySource`] and folded into a [`SegmentTree`], on both sides, for
//! the life of the session. That is the maintenance cost this design trades the
//! write tax for, so it has to be a number.
//!
//! Retained, not peak. The question is how much memory a session holds while it
//! runs, which is what bounds how many sessions a node can serve at once; the
//! transient cost of building the structures is a separate concern and the build
//! *time* is already benched in `benches/reconcile.rs`.
//!
//! ```text
//! DEFRA_SYNC_BENCH_OUT=/tmp/ranges \
//!   cargo test --release -p p2p --features iroh-transport \
//!   --test reconcile_session_memory -- --ignored --nocapture
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};

use p2p::reconcile::engine::rbsr::SegmentTree;
use p2p::reconcile::source::{Item, ItemId, MemorySource};
use sha2::{Digest, Sha256};

/// Live heap bytes, tracked by wrapping the system allocator.
///
/// Relaxed ordering throughout: the measurements below are single-threaded, and
/// a total order across allocations would cost more than it tells us.
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

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

fn id(seed: usize) -> ItemId {
    ItemId::new(Sha256::digest((seed as u64).to_be_bytes()).to_vec())
}

/// Heap held by the source alone, and by the source plus its tree.
fn retained(n: usize) -> (usize, usize) {
    let before = live();
    let source =
        MemorySource::new((0..n).map(|seed| Item::new(0, id(seed)))).expect("distinct seeds");
    let source_only = live() - before;

    let tree = SegmentTree::build(source);
    let with_tree = live() - before;

    // Keep both alive across the reads above; dropping earlier would measure a
    // structure that no longer exists.
    assert!(!tree.is_empty());
    drop(tree);
    (source_only, with_tree)
}

fn out_path() -> PathBuf {
    std::env::var("DEFRA_SYNC_BENCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("target/sync-bench"))
        .join("session_memory.csv")
}

/// One run per size, three times over, reported as min and median.
///
/// Allocation totals are far steadier than timings, but they are not guaranteed
/// steady, and a single draw would not say which.
#[test]
#[ignore = "benchmark: allocates a hundred thousand items several times over"]
fn session_memory_vs_set_size() {
    const RUNS: usize = 3;
    let mut csv = String::from(
        "side,n,runs,sourceBytesMin,treeBytesMin,treeBytesMedian,bytesPerItemMedian\n",
    );

    for n in [1_000usize, 10_000, 100_000] {
        let mut sources = Vec::with_capacity(RUNS);
        let mut trees = Vec::with_capacity(RUNS);
        for _ in 0..RUNS {
            let (source, tree) = retained(n);
            sources.push(source);
            trees.push(tree);
        }
        sources.sort_unstable();
        trees.sort_unstable();

        let median = trees[RUNS / 2];
        let per_item = median as f64 / n as f64;
        println!(
            "n={n}: source {} B, session {} B (median of {RUNS}), {per_item:.1} B/item",
            sources[0], median
        );
        csv.push_str(&format!(
            "session_storage,{n},{RUNS},{},{},{median},{per_item:.1}\n",
            sources[0], trees[0]
        ));
    }

    let path = out_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).expect("create out dir");
    }
    std::fs::write(&path, &csv).expect("write session memory csv");
    println!("{}\n{csv}", path.display());
}
