//! Pins the coded-symbol stream itself against the reference.
//!
//! Same driver and same symbols as `mapping_tests.rs`: the five symbols
//! `item(0..5)` were loaded into the reference's `Encoder` and its first eight
//! `ProduceNextCodedSymbol` results printed. Matching them means the sum, the
//! checksum fold and the signed count all agree cell for cell, which is the
//! strongest statement AC3 can make about the encoder.

use super::encoder::Encoder;
use super::mapping_tests::vector_item;
use super::symbol::CodedSymbol;

/// `(sum, checksum, count)` of coded symbols 0..8 for the five-symbol set.
const CODED_PREFIX: [(&str, u64, i64); 8] = [
    (
        "ce94f61c52d03905c61a91b3ba352480f47c99a190d559b88eccb68edfefc7dece94f61c",
        0xf043_74a9_14f7_3960,
        5,
    ),
    (
        "573996ae38b871bfb0471a417ef2afaf1e64fa3c08726d55212fa7b0ada1c4ff573996ae",
        0xbb9b_f8f5_0d06_2090,
        3,
    ),
    (
        "809e67db0abea071e7c379515147908ae129447ecfed4350e369c987a2bdfb71809e67db",
        0x1c70_5508_f116_7a47,
        2,
    ),
    (
        "31467d72288d00fcbd82a82503e4e8670797d248e2354a0e37cde4958d7d037f31467d72",
        0xc8af_0b7c_dd32_0d21,
        1,
    ),
    (
        "1815469276077ab2e9aa856574916c4cfcbdadebb46030aa915c5e4bb334a1cd18154692",
        0x1a29_918b_d931_9f1c,
        2,
    ),
    (
        "31467d72288d00fcbd82a82503e4e8670797d248e2354a0e37cde4958d7d037f31467d72",
        0xc8af_0b7c_dd32_0d21,
        1,
    ),
    (
        "fef4ca956c8cab8003ac4e5058c0bb0e0467c1e191ca54a164d7d4e931559d3cfef4ca95",
        0x756d_370a_f813_c8ea,
        2,
    ),
    (
        "988b21497cb9dac30e69fc3425d6fcc61d94e9957b8d73fa723597cc11895abc988b2149",
        0x0659_c483_2827_e55b,
        2,
    ),
];

fn encoder_over(n: usize) -> Encoder {
    let mut encoder = Encoder::new(36);
    for index in 0..n {
        encoder.add(vector_item(index));
    }
    encoder
}

fn expected(vector: (&str, u64, i64)) -> CodedSymbol {
    CodedSymbol::new(
        hex::decode(vector.0).expect("vector hex"),
        vector.1,
        vector.2,
    )
}

#[test]
fn the_coded_symbol_stream_matches_the_reference() {
    let mut encoder = encoder_over(5);
    for (index, vector) in CODED_PREFIX.iter().enumerate() {
        assert_eq!(encoder.produce_next(), expected(*vector), "index {index}");
    }
}

/// Coded symbol 0 is the whole set folded into one cell — the property that
/// makes two peers' shared items cancel before a single peel happens.
#[test]
fn the_first_coded_symbol_holds_every_symbol() {
    for n in [0usize, 1, 5, 200] {
        assert_eq!(encoder_over(n).produce_next().count(), n as i64, "n={n}");
    }
}

/// An empty set streams identity cells forever, so a peer with nothing still
/// answers and the difference it reveals is the whole remote set.
#[test]
fn an_empty_set_streams_identity_cells() {
    let mut encoder = encoder_over(0);
    for _ in 0..16 {
        let cell = encoder.produce_next();
        assert!(cell.is_empty());
        assert_eq!(cell.sum(), [0u8; 36]);
    }
}

/// The stream is a function of the set, not of the order it was loaded in.
#[test]
fn the_stream_is_independent_of_insertion_order() {
    let mut forward = Encoder::new(36);
    for index in 0..24 {
        forward.add(vector_item(index));
    }
    let mut backward = Encoder::new(36);
    for index in (0..24).rev() {
        backward.add(vector_item(index));
    }

    for index in 0..40 {
        assert_eq!(
            forward.produce_next(),
            backward.produce_next(),
            "index {index}"
        );
    }
}
