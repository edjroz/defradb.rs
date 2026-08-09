//! Fuzzes the RIBLT decode and peel path a remote peer can reach.
//!
//! The decoder is the side that holds state, allocates, and solves, so it is
//! the side a hostile encoder attacks: cells whose checksums lie, counts that
//! contradict what has been peeled, widths that do not match, batches that
//! never resolve. The property under test is that every such sequence ends in
//! convergence or a `ReconcileError` — never a panic, never an unbounded
//! allocation, never a peel that does not terminate.
//!
//! The input is read as a leading shape byte selecting the local set, then a
//! run of `u16` big-endian length-prefixed frames, so one input drives a whole
//! multi-batch session rather than a single message. Frames that fail to decode
//! are skipped rather than ending the run, so a single malformed batch does not
//! cut off the rest of the input.

#![no_main]

use libfuzzer_sys::fuzz_target;
use p2p::reconcile::engine::riblt::{RibltEngine, RibltMessage};
use p2p::reconcile::{codec, Item, ItemId, MemorySource, Session};

/// Item counts the local set is built at, spanning the empty, single-item, and
/// multi-batch regimes.
const SHAPES: [usize; 6] = [0, 1, 8, 64, 200, 2000];

/// A 36-byte identity, the width the session reconciles at.
fn id(index: usize) -> ItemId {
    let mut bytes = (index as u64).to_be_bytes().to_vec();
    bytes.resize(36, index as u8);
    ItemId::new(bytes)
}

fn build_source(shape: u8) -> MemorySource {
    let count = SHAPES[usize::from(shape) % SHAPES.len()];
    let items = (0..count).map(|index| Item::new(index as u64 % 7, id(index)));
    MemorySource::new(items).expect("distinct indices yield distinct sort keys")
}

fn frames(mut rest: &[u8]) -> Vec<&[u8]> {
    let mut out = Vec::new();
    while rest.len() >= 2 {
        let len = usize::from(u16::from_be_bytes([rest[0], rest[1]]));
        rest = &rest[2..];
        let take = len.min(rest.len());
        out.push(&rest[..take]);
        rest = &rest[take..];
    }
    out
}

fuzz_target!(|data: &[u8]| {
    let Some((&shape, rest)) = data.split_first() else {
        return;
    };

    let source = build_source(shape);
    let engine = if shape & 0x80 == 0 {
        RibltEngine::decoder(&source)
    } else {
        RibltEngine::encoder(&source)
    };
    let Ok(engine) = engine else {
        return;
    };
    let mut session = Session::new(engine);

    while session.next_outbound().is_ok_and(|out| out.is_some()) {}

    for frame in frames(rest) {
        let Ok(message) = codec::decode::<RibltMessage>(frame) else {
            continue;
        };
        if session.ingest(message).is_err() {
            break;
        }
        while let Ok(Some(outbound)) = session.next_outbound() {
            codec::encode(&outbound).expect("an engine's own message must encode");
        }
    }
});
