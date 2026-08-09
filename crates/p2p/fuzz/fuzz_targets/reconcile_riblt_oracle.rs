//! Fuzzes the RIBLT decoder where mutation alone cannot reach, with an oracle
//! that can see a wrong answer.
//!
//! The byte-oriented target next door cannot find the failures that matter, for
//! two structural reasons. A cell is only peeled if its checksum equals
//! `SHA-256(sum)[..8]`, which a mutator will not stumble on, so the peel cascade
//! is unreachable and the decoder never gets past "subtract and store". And its
//! only oracle is "did not panic", so a session that converges on a difference
//! that is simply *wrong* is invisible by construction — which is exactly the
//! shape of the two defects a review found by hand.
//!
//! So this target builds cells *from symbols*: checksums are correct because
//! they were computed, not guessed, and the honest prefix of the stream comes
//! from a real encoder. On top of that it interleaves the things a peer can do
//! that an encoder cannot — restate a count, claim a residual is explained,
//! forge a correctly-checksummed cell for a symbol nobody holds, repeat a cell,
//! send the wrong width.
//!
//! # The oracle
//!
//! > If the decoder reports convergence, the difference it recovered must
//! > explain every cell it accepted.
//!
//! Checked by recomputing each accepted cell's residual from scratch: subtract
//! the local set and the recovered `need`, add back the recovered `have`, and
//! require the identity. The mapping and hash used to do that are reimplemented
//! here rather than borrowed from the engine, so the oracle is independent of
//! the thing it is judging; both are pinned to the Go reference by unit tests,
//! so the duplicate cannot drift silently.
//!
//! When nothing tampered with the stream, the stronger check applies: the
//! recovered difference must equal the true one.

#![no_main]

use std::collections::BTreeSet;

use libfuzzer_sys::arbitrary::{self, Arbitrary};
use libfuzzer_sys::fuzz_target;
use p2p::reconcile::engine::riblt::{CodedSymbol, Decoder, Encoder};
use sha2::{Digest, Sha256};

const WIDTH: usize = 36;

/// Most cells one input may drive, so a single execution stays short enough to
/// keep the corpus turning over.
const MAX_CELLS: usize = 512;

/// Most items per set, for the same reason.
const MAX_ITEMS: usize = 48;

/// How a peer may corrupt one cell of the stream it was asked for.
#[derive(Arbitrary, Debug)]
enum Tamper {
    /// Pass the encoder's cell through untouched.
    Honest,
    /// Keep the cell but claim a different membership count.
    Recount(i64),
    /// Claim the cell is a fully explained residual.
    ClaimEmpty,
    /// Replace it with a correctly-checksummed cell for a symbol nobody holds —
    /// the forgery a byte mutator can never synthesize.
    ForgePure { seed: u8, negative: bool },
    /// Send the previous cell again in this position.
    Repeat,
    /// Send a cell of the wrong width.
    Widen,
}

#[derive(Arbitrary, Debug)]
struct Session {
    shared: Vec<u16>,
    local_only: Vec<u16>,
    remote_only: Vec<u16>,
    stream: Vec<Tamper>,
}

fn symbol(seed: u32) -> Vec<u8> {
    let digest = Sha256::digest(seed.to_be_bytes());
    let mut bytes = digest.to_vec();
    bytes.resize(WIDTH, 0);
    bytes
}

/// The engine's symbol hash, transcribed. Pinned to the Go reference by
/// `mapping_tests::the_symbol_hash_matches_the_reference_drivers_hash`.
fn symbol_hash(bytes: &[u8]) -> u64 {
    let digest = Sha256::digest(bytes);
    u64::from_le_bytes(digest[..8].try_into().expect("sha-256 is 32 bytes"))
}

/// The engine's index mapping, transcribed. Pinned to the Go reference by
/// `mapping_tests::the_index_mapping_matches_the_reference`.
fn mapped_indices(hash: u64, limit: usize) -> Vec<usize> {
    let mut prng = hash;
    let mut index: u64 = 0;
    let mut out = Vec::new();
    while index < limit as u64 {
        out.push(index as usize);
        prng = prng.wrapping_mul(0xda94_2042_e4dd_58b5);
        let jump =
            (index as f64 + 1.5) * ((1u64 << 32) as f64 / (prng as f64 + 1.0).sqrt() - 1.0);
        index = index.saturating_add(jump.ceil() as u64);
    }
    out
}

/// Three disjoint sets of seeds, deduplicated globally so the true difference
/// is exactly known.
fn partition(session: &Session) -> (Vec<u32>, Vec<u32>, Vec<u32>) {
    let mut seen = BTreeSet::new();
    let mut group = |seeds: &[u16]| -> Vec<u32> {
        let mut out = Vec::new();
        for seed in seeds {
            if out.len() == MAX_ITEMS {
                break;
            }
            if seen.insert(*seed) {
                out.push(u32::from(*seed));
            }
        }
        out
    };
    (
        group(&session.shared),
        group(&session.local_only),
        group(&session.remote_only),
    )
}

/// One residual under reconstruction.
struct Residual {
    sum: Vec<u8>,
    checksum: u64,
    count: i64,
}

impl Residual {
    fn apply(&mut self, symbol: &[u8], direction: i64) {
        for (slot, byte) in self.sum.iter_mut().zip(symbol) {
            *slot ^= byte;
        }
        self.checksum ^= symbol_hash(symbol);
        self.count = self.count.saturating_add(direction);
    }

    fn is_identity(&self) -> bool {
        self.count == 0 && self.checksum == 0 && self.sum.iter().all(|byte| *byte == 0)
    }
}

/// Recomputes every accepted cell's residual and requires the identity.
fn every_cell_is_explained(
    accepted: &[CodedSymbol],
    local: &[Vec<u8>],
    need: &[Vec<u8>],
    have: &[Vec<u8>],
) -> bool {
    let mut residuals: Vec<Residual> = accepted
        .iter()
        .map(|cell| Residual {
            sum: cell.sum().to_vec(),
            checksum: cell.checksum(),
            count: cell.count(),
        })
        .collect();

    // The local set and everything the session says it needs were folded in by
    // the peer or by the decoder's own subtraction; everything it says it holds
    // was subtracted and must be added back.
    let mut fold = |symbols: &[Vec<u8>], direction: i64| {
        for item in symbols {
            for index in mapped_indices(symbol_hash(item), residuals.len()) {
                residuals[index].apply(item, direction);
            }
        }
    };
    fold(local, -1);
    fold(need, -1);
    fold(have, 1);

    residuals.iter().all(Residual::is_identity)
}

fuzz_target!(|session: Session| {
    if session.stream.is_empty() {
        return;
    }
    let (shared, local_only, remote_only) = partition(&session);

    let mut encoder = Encoder::new(WIDTH);
    let mut decoder = Decoder::new(WIDTH);
    for seed in shared.iter().chain(remote_only.iter()) {
        encoder.add(symbol(*seed));
    }
    for seed in shared.iter().chain(local_only.iter()) {
        decoder.add(symbol(*seed));
    }

    let mut honest = true;
    let mut accepted: Vec<CodedSymbol> = Vec::new();
    let mut previous: Option<CodedSymbol> = None;

    for tamper in session.stream.iter().take(MAX_CELLS) {
        let produced = encoder.produce_next();
        let cell = match tamper {
            Tamper::Honest => produced,
            Tamper::Recount(count) => {
                honest = false;
                CodedSymbol::new(produced.sum().to_vec(), produced.checksum(), *count)
            }
            Tamper::ClaimEmpty => {
                honest = false;
                CodedSymbol::new(produced.sum().to_vec(), 0, 0)
            }
            Tamper::ForgePure { seed, negative } => {
                honest = false;
                let forged = symbol(u32::from(*seed) | 0x00ff_0000);
                let sign = if *negative { -1 } else { 1 };
                CodedSymbol::new(forged.clone(), symbol_hash(&forged), sign)
            }
            Tamper::Repeat => match previous.clone() {
                Some(cell) => {
                    honest = false;
                    cell
                }
                None => produced,
            },
            Tamper::Widen => {
                honest = false;
                CodedSymbol::new(vec![0u8; WIDTH + 1], produced.checksum(), produced.count())
            }
        };

        previous = Some(cell.clone());
        if decoder.add_coded_symbol(cell.clone()).is_err() {
            continue;
        }
        accepted.push(cell);
        decoder.try_decode();
        if decoder.is_decoded() {
            break;
        }
    }

    if !decoder.is_decoded() {
        return;
    }

    let need: Vec<Vec<u8>> = decoder.remote().map(<[u8]>::to_vec).collect();
    let have: Vec<Vec<u8>> = decoder.local().map(<[u8]>::to_vec).collect();

    for item in need.iter().chain(have.iter()) {
        assert_eq!(
            item.len(),
            WIDTH,
            "a recovered symbol must be a whole identity"
        );
    }

    let local: Vec<Vec<u8>> = shared
        .iter()
        .chain(local_only.iter())
        .map(|seed| symbol(*seed))
        .collect();
    assert!(
        every_cell_is_explained(&accepted, &local, &need, &have),
        "converged on a difference that does not explain the cells it accepted"
    );

    if honest {
        let mut expected_need: Vec<Vec<u8>> = remote_only.iter().map(|s| symbol(*s)).collect();
        let mut expected_have: Vec<Vec<u8>> = local_only.iter().map(|s| symbol(*s)).collect();
        let (mut got_need, mut got_have) = (need, have);
        expected_need.sort();
        expected_have.sort();
        got_need.sort();
        got_have.sort();
        assert_eq!(got_need, expected_need, "honest stream, wrong need set");
        assert_eq!(got_have, expected_have, "honest stream, wrong have set");
    }
});
