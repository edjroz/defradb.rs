//! Pins whole decode runs against the reference.
//!
//! Same driver, same symbols, same construction as `mapping_tests.rs`: for each
//! case the reference's `Encoder` and `Decoder` were run one coded symbol at a
//! time until `Decoded()`, and the symbol count and the two recovered sets
//! printed. Matching the *count* is the strong part — it says the peel
//! cascades at exactly the same points, not merely that both implementations
//! eventually find the same answer.

use super::decoder::Decoder;
use super::encoder::Encoder;
use super::mapping_tests::vector_item;

/// `(common, encoder-only, decoder-only, coded symbols consumed)`.
const DECODE_RUNS: [(usize, usize, usize, usize); 9] = [
    (0, 0, 0, 1),
    (10, 1, 0, 1),
    (10, 0, 1, 1),
    (10, 1, 1, 2),
    (100, 3, 4, 10),
    (1000, 0, 0, 1),
    (1000, 20, 20, 58),
    (0, 50, 0, 72),
    (500, 100, 100, 257),
];

/// Runs a case to convergence, returning the symbols consumed and the recovered
/// item numbers by side.
fn run(common: usize, encoder_only: usize, decoder_only: usize) -> (usize, Vec<usize>, Vec<usize>) {
    let mut encoder = Encoder::new(36);
    let mut decoder = Decoder::new(36);

    let mut next = 0;
    for _ in 0..common {
        encoder.add(vector_item(next));
        decoder.add(vector_item(next));
        next += 1;
    }
    for _ in 0..encoder_only {
        encoder.add(vector_item(next));
        next += 1;
    }
    for _ in 0..decoder_only {
        decoder.add(vector_item(next));
        next += 1;
    }

    let mut consumed = 0;
    while !decoder.is_decoded() {
        decoder
            .add_coded_symbol(encoder.produce_next())
            .expect("the widths agree");
        consumed += 1;
        decoder.try_decode();
        assert!(consumed < 100_000, "decode did not converge");
    }

    (
        consumed,
        item_numbers(decoder.remote(), next),
        item_numbers(decoder.local(), next),
    )
}

fn item_numbers<'a>(symbols: impl Iterator<Item = &'a [u8]>, limit: usize) -> Vec<usize> {
    let mut numbers: Vec<usize> = symbols
        .map(|symbol| {
            (0..limit)
                .find(|n| vector_item(*n) == symbol)
                .expect("decoded a symbol that was never encoded")
        })
        .collect();
    numbers.sort_unstable();
    numbers
}

#[test]
fn decode_runs_match_the_reference() {
    for (common, encoder_only, decoder_only, symbols) in DECODE_RUNS {
        let (consumed, remote, local) = run(common, encoder_only, decoder_only);
        let case = format!("common={common} enc={encoder_only} dec={decoder_only}");

        assert_eq!(consumed, symbols, "{case}: coded symbols consumed");
        assert_eq!(
            remote,
            (common..common + encoder_only).collect::<Vec<_>>(),
            "{case}: encoder-only set"
        );
        assert_eq!(
            local,
            (common + encoder_only..common + encoder_only + decoder_only).collect::<Vec<_>>(),
            "{case}: decoder-only set"
        );
    }
}

/// The sign is the whole reason one stream answers both questions: `+1` is
/// "the peer has it and I do not", `-1` the reverse.
#[test]
fn the_sign_of_a_pure_cell_separates_need_from_have() {
    let (_, remote, local) = run(0, 1, 1);
    assert_eq!(remote, [0], "the encoder-only symbol is the need side");
    assert_eq!(local, [1], "the decoder-only symbol is the have side");
}

/// A cell whose width does not match the session's is refused rather than
/// silently XORed against a prefix of itself.
#[test]
fn a_mismatched_cell_width_is_refused() {
    let mut decoder = Decoder::new(36);
    let mut narrow = Encoder::new(8);
    narrow.add(vec![1u8; 8]);

    let error = decoder
        .add_coded_symbol(narrow.produce_next())
        .expect_err("a 8-byte cell is not a 36-byte cell");
    assert!(matches!(
        error,
        crate::reconcile::ReconcileError::SymbolWidthMismatch {
            found: 8,
            expected: 36
        }
    ));
}
