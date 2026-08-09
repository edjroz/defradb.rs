//! Streams a hostile peer can send, and what the decoder may conclude from them.
//!
//! Every other test in this module drives the decoder from the honest encoder,
//! which can only produce cells that are consistent by construction. That is
//! exactly the wrong place to look for the failure that matters here: the
//! decoder's inputs come off a network, and a peer is free to send cells no
//! encoder would ever produce.
//!
//! The invariant these tests hold the decoder to:
//!
//! > If the decoder reports convergence, every cell it received is fully
//! > explained by the difference it recovered — that is, every residual is the
//! > zero cell.
//!
//! Reporting convergence with an unexplained residual is the dangerous failure,
//! because it is *silent*: the session logs as a normal agreement, the caller
//! sees an empty or fabricated difference, and the node stays stale with no
//! error anywhere. A decoder that instead keeps asking for symbols and ends on
//! its cap has failed loudly, which is the correct outcome against a peer that
//! is lying.

use super::decoder::Decoder;
use super::engine::RibltEngine;
use super::message::RibltMessage;
use super::simulate::{id, source};
use super::symbol::{symbol_hash, CodedSymbol};
use crate::reconcile::engine::{Engine, Progress};
use crate::reconcile::source::ItemSource;

const WIDTH: usize = 36;

/// A decoder engine over the given local seeds, already past its opening request.
fn decoder_engine(seeds: std::ops::Range<u64>) -> RibltEngine {
    let local = source(seeds);
    let mut engine = RibltEngine::decoder(&local).expect("uniform width");
    engine.next_outbound().expect("no error").expect("opening");
    engine
}

fn feed(engine: &mut RibltEngine, cells: Vec<CodedSymbol>) -> crate::reconcile::Result<Progress> {
    engine.ingest(RibltMessage::symbols(WIDTH, cells))
}

/// The cell the decoder itself derives for stream position zero: every local
/// symbol folded together. An attacker that has reconciled with this peer
/// before knows the set, so it can compute this too — which is what makes the
/// crafted-residual test below a realistic attack and not a curiosity.
fn local_cell_zero(seeds: std::ops::Range<u64>) -> CodedSymbol {
    let set = source(seeds);
    let mut sum = vec![0u8; WIDTH];
    let mut checksum = 0u64;
    for index in 0..set.len() {
        let bytes = set.id(index).as_bytes();
        for (slot, byte) in sum.iter_mut().zip(bytes) {
            *slot ^= byte;
        }
        checksum ^= symbol_hash(bytes);
    }
    CodedSymbol::new(sum, checksum, set.len() as i64)
}

/// C-1, against an empty local set: one frame, and the peer is told it is in
/// sync with a set it has never seen.
#[test]
fn a_cell_that_only_claims_to_be_empty_does_not_converge() {
    let mut engine = decoder_engine(0..0);

    let progress = feed(&mut engine, vec![CodedSymbol::new(vec![0xde; WIDTH], 0, 0)])
        .expect("a malformed cell is not an error, only unconvincing");

    assert_eq!(
        progress,
        Progress::Continue,
        "a cell with a non-zero sum is not an explained residual, whatever its \
         count and checksum say"
    );
    assert!(engine.diff().is_empty());
}

/// C-1, against a real set: the attacker knows the peer's items, so it can
/// cancel the peer's own cell zero exactly and leave a residual whose count and
/// checksum are both zero while its sum is anything it likes.
#[test]
fn a_crafted_zero_residual_does_not_convince_a_decoder_with_a_real_set() {
    let mut engine = decoder_engine(0..25);

    let mirror = local_cell_zero(0..25);
    let hostile = CodedSymbol::new(
        mirror.sum().iter().map(|byte| byte ^ 0xa5).collect(),
        mirror.checksum(),
        mirror.count(),
    );

    let progress = feed(&mut engine, vec![hostile]).expect("not an error");
    assert_eq!(
        progress,
        Progress::Continue,
        "the residual's sum is 36 bytes of the attacker's choosing"
    );
    assert!(
        engine.diff().is_empty(),
        "and nothing may be reported from it"
    );
}

/// C-2: index zero is in every symbol's mapping, so peeling anything touches
/// cell zero. If cell zero was already resolved, that touch makes it look pure
/// again — and a decoder that trusts the queue rather than re-checking will
/// resolve it a second time, paying for a cell it never explained.
#[test]
fn a_cell_resolved_once_is_not_resolved_again() {
    let mut decoder = Decoder::new(WIDTH);
    let symbol = vec![7u8; WIDTH];

    decoder
        .add_coded_symbol(CodedSymbol::new(vec![0u8; WIDTH], 0, 0))
        .expect("width matches");
    decoder
        .add_coded_symbol(CodedSymbol::new(symbol.clone(), symbol_hash(&symbol), 1))
        .expect("width matches");
    decoder
        .add_coded_symbol(CodedSymbol::new(vec![9u8; WIDTH], 12_345, 5))
        .expect("width matches");
    decoder.try_decode();

    assert!(
        !decoder.is_decoded(),
        "the third cell holds five members and was never explained"
    );

    let remote: Vec<&[u8]> = decoder.remote().collect();
    let local: Vec<&[u8]> = decoder.local().collect();
    assert!(
        !remote.iter().any(|r| local.contains(r)),
        "no symbol may be recovered as both needed and held: {remote:?} / {local:?}"
    );
}

/// The same shape one level up, where it is a product failure rather than a
/// decoder curiosity: three frames and the session reports agreement.
#[test]
fn a_re_queued_cell_cannot_converge_a_session() {
    let mut engine = decoder_engine(0..0);
    let symbol = vec![7u8; WIDTH];

    let progress = feed(
        &mut engine,
        vec![
            CodedSymbol::new(vec![0u8; WIDTH], 0, 0),
            CodedSymbol::new(symbol.clone(), symbol_hash(&symbol), 1),
            CodedSymbol::new(vec![9u8; WIDTH], 12_345, 5),
        ],
    )
    .expect("not an error");

    assert_eq!(progress, Progress::Continue);
    assert!(
        engine.diff().is_empty(),
        "nothing is reported from a stream that never resolved"
    );
}

/// A count no peel can produce is not a reason to stop, and not a reason to
/// panic either. The reference panics here; skipping is only safe if the cell
/// stays unresolved, which is what this pins.
#[test]
fn counts_no_peel_could_produce_leave_the_stream_unresolved() {
    for count in [2i64, -2, 7, i64::MIN, i64::MAX] {
        let mut engine = decoder_engine(0..0);
        let symbol = vec![3u8; WIDTH];
        let progress = feed(
            &mut engine,
            vec![CodedSymbol::new(
                symbol.clone(),
                symbol_hash(&symbol),
                count,
            )],
        )
        .expect("not an error");
        assert_eq!(progress, Progress::Continue, "count {count}");
        assert!(engine.diff().is_empty(), "count {count}");
    }
}

/// The same cell twice is a stream no encoder produces: position `i` and
/// position `i+1` are different cells of the sequence.
#[test]
fn a_repeated_cell_does_not_resolve_itself() {
    let mut engine = decoder_engine(0..8);
    let symbol = vec![5u8; WIDTH];
    let cell = CodedSymbol::new(symbol.clone(), symbol_hash(&symbol), 1);

    let progress = feed(&mut engine, vec![cell.clone(), cell.clone(), cell]).expect("not an error");
    assert_eq!(progress, Progress::Continue);
}

/// A width-zero session has no symbols to reconcile, so the only cell it can
/// honestly see is the zero cell. Without a guard, the empty byte string hashes
/// like any other and peels out as an item the caller is told to fetch.
#[test]
fn a_width_zero_session_cannot_decode_an_empty_identity() {
    let mut decoder = Decoder::new(0);
    decoder
        .add_coded_symbol(CodedSymbol::new(Vec::new(), symbol_hash(&[]), 1))
        .expect("width matches");
    decoder.try_decode();

    assert_eq!(
        decoder.remote().count(),
        0,
        "an empty identity is not an item"
    );
    assert_eq!(decoder.local().count(), 0);
    assert!(!decoder.is_decoded());
}

/// Hostile cells mixed into an otherwise honest prefix must not let the session
/// stop early: the honest part resolves, the hostile part does not, and the
/// decoder keeps asking.
#[test]
fn hostile_cells_among_honest_ones_hold_the_session_open() {
    let local = source(0..50);
    let remote = source(0..53);
    let mut engine = RibltEngine::decoder(&local).expect("uniform width");
    engine.next_outbound().expect("no error").expect("opening");

    let mut encoder = super::encoder::Encoder::new(WIDTH);
    for index in 0..remote.len() {
        encoder.add(remote.id(index).as_bytes().to_vec());
    }

    let mut cells: Vec<CodedSymbol> = (0..16).map(|_| encoder.produce_next()).collect();
    cells.push(CodedSymbol::new(vec![0x11; WIDTH], 0, 0));

    let progress = feed(&mut engine, cells).expect("not an error");
    assert_eq!(
        progress,
        Progress::Continue,
        "one unexplained cell keeps the whole batch unresolved"
    );
    assert!(engine.diff().is_empty(), "and nothing is reported early");
}

/// The honest path still converges, and still reports the true difference —
/// the guard above must not be bought by refusing real streams.
#[test]
fn an_honest_stream_still_converges_after_the_guard() {
    let local = source(0..50);
    let remote = source(0..53);
    let mut engine = RibltEngine::decoder(&local).expect("uniform width");
    engine.next_outbound().expect("no error").expect("opening");

    let mut encoder = super::encoder::Encoder::new(WIDTH);
    for index in 0..remote.len() {
        encoder.add(remote.id(index).as_bytes().to_vec());
    }
    let cells: Vec<CodedSymbol> = (0..16).map(|_| encoder.produce_next()).collect();

    assert_eq!(
        feed(&mut engine, cells).expect("not an error"),
        Progress::Converged
    );
    let mut need = engine.diff().need().to_vec();
    need.sort();
    assert_eq!(
        need,
        vec![id(50), id(51), id(52)]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    );
    assert!(engine.diff().have().is_empty());
}
