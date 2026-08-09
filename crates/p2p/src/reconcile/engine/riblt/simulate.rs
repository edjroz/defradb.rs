//! A two-engine, in-memory RIBLT driver for tests.
//!
//! The drive shape is the one [`crate::reconcile::drive`] runs over a real
//! stream — flush, hand over, ingest — and every message crosses the real
//! [`codec`], so a convergence proved here is a statement about the engines and
//! the wire format together, and the byte counts are measured rather than
//! estimated.

use sha2::Digest;

use super::engine::RibltEngine;
use super::message::RibltMessage;
use crate::reconcile::codec;
use crate::reconcile::engine::Progress;
use crate::reconcile::error::Result;
use crate::reconcile::session::Session;
use crate::reconcile::source::{Item, ItemId, MemorySource};

/// What one simulated session produced.
pub(super) struct Outcome {
    /// Identities the decoder must pull, ascending.
    pub need: Vec<ItemId>,
    /// Identities the decoder must push, ascending.
    pub have: Vec<ItemId>,
    /// Peer messages the decoder consumed.
    pub rounds: usize,
    /// Coded symbols the encoder produced.
    pub symbols: usize,
    /// Total encoded bytes crossing the wire in both directions.
    pub bytes: usize,
}

/// Reconciles `local` (as decoder) against `remote` (as encoder).
pub(super) fn run(local: &MemorySource, remote: &MemorySource) -> Result<Outcome> {
    let mut decoder = Session::new(RibltEngine::decoder(local)?);
    let mut encoder = Session::new(RibltEngine::encoder(remote)?);
    let mut bytes = 0usize;
    let mut symbols = 0usize;

    let mut request = decoder.next_outbound()?.expect("the decoder opens");
    loop {
        let (message, sent) = hop(&request)?;
        bytes += sent;
        encoder.ingest(message)?;

        let batch = encoder
            .next_outbound()?
            .expect("the encoder always answers a request");
        let (batch, sent) = hop(&batch)?;
        bytes += sent;
        symbols += batch.symbol_count();

        if decoder.ingest(batch)? == Progress::Converged {
            break;
        }
        request = decoder
            .next_outbound()?
            .expect("a decoder short of the difference asks again");
    }

    let mut need = decoder.diff().need().to_vec();
    let mut have = decoder.diff().have().to_vec();
    need.sort();
    have.sort();

    Ok(Outcome {
        need,
        have,
        rounds: decoder.rounds(),
        symbols,
        bytes,
    })
}

/// A deterministic 36-byte identity for a seed, the width of a CIDv1
/// `dag-cbor/sha2-256` item.
pub(super) fn id(seed: u64) -> ItemId {
    let digest = sha2::Sha256::digest(seed.to_be_bytes());
    let mut bytes = digest.to_vec();
    bytes.extend_from_slice(&digest[..4]);
    ItemId::new(bytes)
}

/// Seals a set of seeds into a source.
pub(super) fn source(seeds: impl IntoIterator<Item = u64>) -> MemorySource {
    MemorySource::new(seeds.into_iter().map(|seed| Item::new(seed % 4, id(seed))))
        .expect("seeds are distinct")
}

/// The ascending identities of the given seeds.
pub(super) fn ids(seeds: impl IntoIterator<Item = u64>) -> Vec<ItemId> {
    let mut ids: Vec<ItemId> = seeds.into_iter().map(id).collect();
    ids.sort();
    ids
}

/// The difference sizes the acceptance criteria call for at a given set size.
pub(super) fn difference_sizes(n: usize) -> [usize; 5] {
    [0, 1, 8, n / 2, n]
}

/// Builds a set pair of about `n` items whose symmetric difference is exactly
/// `d`, with the divergent items scattered through the set.
pub(super) fn diverged(n: usize, d: usize, seed: u64) -> (Vec<u64>, Vec<u64>) {
    let mut universe: Vec<u64> = (0..(n + d) as u64).collect();
    shuffle(&mut universe, seed);

    let (divergent, shared) = universe.split_at(d);
    let local_only = d.div_ceil(2);

    let mut local = shared.to_vec();
    local.extend_from_slice(&divergent[..local_only]);
    let mut remote = shared.to_vec();
    remote.extend_from_slice(&divergent[local_only..]);

    (local, remote)
}

fn shuffle(values: &mut [u64], seed: u64) {
    let mut state = seed | 1;
    for index in (1..values.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        values.swap(index, (state % (index as u64 + 1)) as usize);
    }
}

/// Sends one message across the codec, returning what the peer decodes and how
/// many bytes it cost.
fn hop(message: &RibltMessage) -> Result<(RibltMessage, usize)> {
    let bytes = codec::encode(message)?;
    let decoded: RibltMessage = codec::decode(&bytes)?;
    assert_eq!(&decoded, message, "the codec must round-trip every message");
    Ok((decoded, bytes.len()))
}
