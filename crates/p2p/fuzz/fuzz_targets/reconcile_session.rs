//! Fuzzes the session ingest path a remote peer can reach.
//!
//! A node's reconciliation session consumes whatever the peer puts on the wire:
//! frames in any order, ranges in any tiling, bounds that need not ascend, ID
//! lists of any size. The property under test is that every such sequence ends
//! in convergence or in a `ReconcileError` — never a panic, an unbounded
//! allocation, or a loop that does not terminate.
//!
//! The input is read as a leading shape byte selecting the local set and the
//! role, followed by a run of `u16` big-endian length-prefixed frames, so one
//! input drives a whole multi-round session rather than a single message.

#![no_main]

use libfuzzer_sys::fuzz_target;
use p2p::reconcile::engine::rbsr::{RbsrEngine, RbsrMessage};
use p2p::reconcile::{codec, Item, ItemId, MemorySource, Session};

/// Item counts the local set is built at, spanning the empty, single-item,
/// below-threshold, above-threshold, and multi-split regimes.
const SHAPES: [usize; 6] = [0, 1, 8, 64, 200, 2000];

fn build_source(shape: u8) -> MemorySource {
    let count = SHAPES[usize::from(shape) % SHAPES.len()];
    let items = (0..count).map(|index| {
        let seed = (index as u64).to_be_bytes();
        Item::new(index as u64 % 7, ItemId::new(seed.to_vec()))
    });
    MemorySource::new(items).expect("distinct seeds yield distinct sort keys")
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
        RbsrEngine::initiator(source)
    } else {
        RbsrEngine::responder(source)
    };
    let mut session = Session::new(engine);

    while session.next_outbound().is_ok_and(|out| out.is_some()) {}

    for frame in frames(rest) {
        let Ok(message) = codec::decode::<RbsrMessage>(frame) else {
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
