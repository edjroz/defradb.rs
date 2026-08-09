//! What the RIBLT engine promises a session, and what it refuses.

use super::caps;
use super::engine::RibltEngine;
use super::message::RibltMessage;
use super::simulate::{ids, run, source};
use super::symbol::CodedSymbol;
use crate::reconcile::engine::{Engine, Progress};
use crate::reconcile::error::ReconcileError;
use crate::reconcile::source::{Item, ItemId, MemorySource};

/// The headline claim: what crosses the wire tracks the difference, not the
/// sets. Same difference of two, a set fifty times larger, and neither the
/// symbol count nor the round count moves.
///
/// The byte count moves by two, and only by two, and not for the reason it
/// first looks like. It is not cell zero's count: `1002` and `50002` are both
/// three CBOR bytes. It is cells six and seven, whose member counts are just
/// under 256 at the smaller set and just over it at the larger, costing one
/// byte each. The count fields are the only term in a session that grows with
/// `n` at all, and they grow as the log of it, one CBOR width step at a time.
#[test]
fn the_cost_tracks_the_difference_not_the_set_size() {
    let small = run(&source(0..1_000), &source(0..1_002)).expect("converges");
    let large = run(&source(0..50_000), &source(0..50_002)).expect("converges");

    assert_eq!(small.need.len(), 2, "the difference under test");
    assert_eq!(small.symbols, large.symbols, "coded symbols");
    assert_eq!(small.rounds, large.rounds, "rounds");
    assert_eq!(large.bytes - small.bytes, 2, "bytes on the wire");
}

/// Agreement is one batch out and one batch back however large the agreement
/// is, because the shared set annihilates itself in the very first cell.
#[test]
fn agreement_costs_one_round_at_any_set_size() {
    for n in [0u64, 1, 200, 20_000] {
        let outcome = run(&source(0..n), &source(0..n)).expect("converges");
        assert!(outcome.need.is_empty() && outcome.have.is_empty(), "n={n}");
        assert_eq!(outcome.rounds, 1, "n={n}");
    }
}

#[test]
fn the_difference_is_exact_in_both_directions() {
    let outcome = run(&source(0..100), &source(20..120)).expect("converges");
    assert_eq!(outcome.need, ids(100..120), "need is the encoder-only set");
    assert_eq!(outcome.have, ids(0..20), "have is the decoder-only set");
}

/// A stall is answered by reading more, so a difference well past the first
/// batch still converges — it just takes more batches.
#[test]
fn a_difference_larger_than_one_batch_keeps_streaming() {
    let outcome = run(&source(0..500), &source(0..700)).expect("converges");
    assert_eq!(outcome.need, ids(500..700));
    assert!(
        outcome.rounds > 1,
        "a 200-item difference cannot fit the first batch of {}",
        caps::INITIAL_SYMBOL_BATCH
    );
}

/// A decoder is not an encoder. Feeding a role the message it produces itself
/// is a protocol violation, not something to reinterpret.
#[test]
fn each_role_refuses_the_other_roles_message() {
    let set = source(0..4);

    let mut decoder = RibltEngine::decoder(&set).expect("uniform width");
    let request = decoder.next_outbound().expect("no error").expect("opening");
    assert_eq!(
        decoder
            .ingest(request.clone())
            .expect_err("a decoder cannot answer a request"),
        ReconcileError::UnexpectedMessage
    );

    let mut encoder = RibltEngine::encoder(&set).expect("uniform width");
    encoder
        .ingest(request)
        .expect("a request is the encoder's input");
    let batch = encoder.next_outbound().expect("no error").expect("batch");
    assert_eq!(
        encoder.ingest(batch).expect_err("an encoder cannot decode"),
        ReconcileError::UnexpectedMessage
    );
}

/// An empty batch would leave the decoder with nothing received and every cell
/// trivially accounted for, which reads as convergence. It is refused instead.
#[test]
fn an_empty_symbol_batch_is_refused() {
    let set = source(0..4);
    let mut decoder = RibltEngine::decoder(&set).expect("uniform width");
    let _ = decoder.next_outbound();

    assert_eq!(
        decoder
            .ingest(RibltMessage::symbols(36, Vec::new()))
            .expect_err("an empty batch says nothing"),
        ReconcileError::UnexpectedMessage
    );
}

/// The symbol is the identity, so a set of mixed identity widths has no XOR
/// group to be reconciled over and is refused before a session starts.
#[test]
fn a_source_of_mixed_widths_is_refused() {
    let mixed = MemorySource::new([
        Item::new(0, ItemId::new(vec![1u8; 8])),
        Item::new(0, ItemId::new(vec![2u8; 36])),
    ])
    .expect("distinct keys");

    assert_eq!(
        RibltEngine::decoder(&mixed).err().expect("mixed widths"),
        ReconcileError::SymbolWidthMismatch {
            found: 36,
            expected: 8
        }
    );
}

/// Identities wider than the cap would let a peer's declared width size the
/// encoder's allocation.
#[test]
fn a_source_wider_than_the_cap_is_refused() {
    let wide = MemorySource::new([Item::new(
        0,
        ItemId::new(vec![7u8; caps::MAX_SYMBOL_BYTES + 1]),
    )])
    .expect("one key");

    assert_eq!(
        RibltEngine::encoder(&wide).err().expect("too wide"),
        ReconcileError::SymbolTooWide {
            size: caps::MAX_SYMBOL_BYTES + 1,
            max: caps::MAX_SYMBOL_BYTES,
        }
    );
}

/// Two peers whose identities are different widths cannot reconcile at all; the
/// alternative is a session that decodes a fabricated difference.
#[test]
fn peers_of_different_symbol_widths_refuse_each_other() {
    let narrow =
        MemorySource::new((0..4u64).map(|n| Item::new(0, ItemId::new(n.to_be_bytes().to_vec()))))
            .expect("distinct keys");
    let wide = source(0..4);

    let mut decoder = RibltEngine::decoder(&narrow).expect("uniform");
    let mut encoder = RibltEngine::encoder(&wide).expect("uniform");
    let request = decoder.next_outbound().expect("no error").expect("opening");

    assert_eq!(
        encoder.ingest(request).expect_err("widths disagree"),
        ReconcileError::SymbolWidthMismatch {
            found: 8,
            expected: 36
        }
    );
}

/// A decoder that never converges must stop pulling. The cap is the encoder's
/// bandwidth guard as much as the decoder's memory guard.
#[test]
fn a_stream_of_useless_cells_ends_on_the_symbol_cap() {
    let set = source(0..8);
    let mut decoder = RibltEngine::decoder(&set).expect("uniform width");
    let _ = decoder.next_outbound();

    let mut sent = 0;
    let error = loop {
        let useless = CodedSymbol::new(vec![0xab; 36], 0x1234_5678, 5);
        let batch = RibltMessage::symbols(36, vec![useless; 1024]);
        sent += 1024;
        match decoder.ingest(batch) {
            Ok(Progress::Continue) => {}
            Ok(Progress::Converged) => panic!("garbage cells must not converge"),
            Err(error) => break error,
        }
        assert!(sent <= caps::MAX_CODED_SYMBOLS + 1024, "the cap must bite");
        let _ = decoder.next_outbound();
    };

    assert_eq!(
        error,
        ReconcileError::SymbolCapExceeded {
            max: caps::MAX_CODED_SYMBOLS
        }
    );
}
