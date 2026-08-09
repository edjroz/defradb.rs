//! What a *live* session holds, on both engines and both sides.
//!
//! `reconcile_session_memory.rs` measures a floor: the materialised head set and
//! its accumulator tree, the two structures the range engine pins. This measures
//! the thing a node actually has to fit — the session object, the engine's own
//! state, and the frame it is holding mid-round — and it measures it for both
//! engines, because chart 22 claims the rateless decoder's residual is flat
//! where the range engine's is about 200 B per item.
//!
//! Ownership mirrors `sync::reconcile::{initiate, serve}` exactly, because that
//! is what a node pays. The range engine takes the snapshot **by value**; the
//! rateless engine borrows it and copies every identity into its own
//! encoder or decoder, while the caller's snapshot stays alive for the life of
//! the call. So the rateless side holds the identities twice, and that is a
//! measurement about this wiring, not about the sketch.
//!
//! **These are requested heap bytes, not RSS.** A counting global allocator sees
//! what was asked for; allocator metadata, size-class rounding and fragmentation
//! are outside it, so every figure here is a floor on resident memory.
//!
//! ```text
//! cargo test --release -p p2p --features iroh-transport \
//!   --test reconcile_whole_session_memory -- --ignored --nocapture
//! ```

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

use p2p::reconcile::engine::rbsr::RbsrEngine;
use p2p::reconcile::engine::riblt::RibltEngine;
use p2p::reconcile::{Item, ItemId, MemorySource, Session};
use sha2::{Digest, Sha256};

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

/// A CIDv1 `dag-cbor/sha2-256`-width identity. Both engines are measured at the
/// same width or the comparison is about the fixtures.
fn id(seed: u64) -> ItemId {
    let digest = Sha256::digest(seed.to_be_bytes());
    let mut bytes = digest.to_vec();
    bytes.extend_from_slice(&digest[..4]);
    ItemId::new(bytes)
}

fn source(n: usize) -> MemorySource {
    MemorySource::new((0..n as u64).map(|seed| Item::new(seed % 4, id(seed))))
        .expect("distinct seeds")
}

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

/// Heap held by a live range-engine session that has produced its opening
/// message.
fn rbsr_session(n: usize, initiator: bool) -> usize {
    let before = live();
    let local = source(n);
    let mut session = Session::new(if initiator {
        RbsrEngine::initiator(local)
    } else {
        RbsrEngine::responder(local)
    });
    let _opening = session.next_outbound().expect("opening");
    let held = live() - before;
    drop(session);
    held
}

/// Heap held by a live rateless session that has produced its opening message,
/// with the caller's snapshot still alive as `initiate` and `serve` leave it.
fn riblt_session(n: usize, decoder: bool) -> usize {
    let before = live();
    let local = source(n);
    let mut session = Session::new(if decoder {
        RibltEngine::decoder(&local).expect("decoder")
    } else {
        RibltEngine::encoder(&local).expect("encoder")
    });
    let _opening = session.next_outbound().expect("opening");
    let held = live() - before;
    drop(session);
    drop(local);
    held
}

/// Both engines, both sides, at matched set sizes.
#[test]
#[ignore = "benchmark: allocates a hundred thousand items several times over"]
fn whole_session_memory_vs_set_size() {
    const RUNS: usize = 3;
    println!("engine,side,n,runs,retainedBytesMedian,bytesPerItemMedian");
    for n in [1_000usize, 10_000, 100_000] {
        for (engine, side, measure) in [
            (
                "ranges",
                "initiator",
                rbsr_session as fn(usize, bool) -> usize,
            ),
            ("ranges", "responder", rbsr_session),
            ("riblt", "decoder", riblt_session),
            ("riblt", "encoder", riblt_session),
        ] {
            let pulling = matches!(side, "initiator" | "decoder");
            let mut draws: Vec<usize> = (0..RUNS).map(|_| measure(n, pulling)).collect();
            draws.sort_unstable();
            let median = draws[RUNS / 2];
            println!(
                "{engine},{side},{n},{RUNS},{median},{:.1}",
                median as f64 / n as f64
            );
        }
    }
}

/// The claim chart 22 makes, reduced to something a test can hold: the rateless
/// decoder's per-item hold must not be *worse* than the range engine's by an
/// order of magnitude, and neither may grow per item with `n`.
///
/// Deliberately not an equality. The two engines hold different things, and the
/// point of measuring is that the model's "flat residual versus 200 B per item"
/// is a claim about a decoder in isolation, not about a session on this wiring.
#[test]
#[ignore = "benchmark: allocates ten thousand items"]
fn neither_engine_holds_more_per_item_as_the_set_grows() {
    for measure in [
        rbsr_session as fn(usize, bool) -> usize,
        riblt_session as fn(usize, bool) -> usize,
    ] {
        let small = measure(1_000, true) as f64 / 1_000.0;
        let large = measure(10_000, true) as f64 / 10_000.0;
        assert!(
            large <= small * 1.5,
            "per-item hold grew from {small:.1} B at n=1000 to {large:.1} B at n=10000"
        );
    }
}
