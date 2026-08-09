//! The two engines driven on identical terms.
//!
//! Each engine already has an in-memory driver for its own tests, and those two
//! drivers are *not* comparable: the range engine's builds 32-byte identities
//! and the rateless engine's builds 36-byte ones, which understates every listed
//! identity the range engine ships by an eighth. A comparison run through them
//! would be measuring the fixtures.
//!
//! So this driver owns the identities, builds both engines' sources from the
//! same items, and encodes every message through the same [`codec`]. Both
//! engines therefore pay the same CBOR envelope tax — the single biggest way to
//! get this comparison wrong is to charge it to one of them and not the other.
//!
//! What is *not* included here, and belongs to the node-level rows instead: the
//! `SessionOpen` frame, which costs 43 bytes for the range engine and 52 for the
//! rateless one, because the engine tag is omitted when it names the default.

use std::result::Result as StdResult;

use crate::reconcile::codec;
use crate::reconcile::engine::rbsr::caps::{BRANCHING_FACTOR, ID_LIST_THRESHOLD};
use crate::reconcile::engine::rbsr::RbsrEngine;
use crate::reconcile::engine::riblt::{RibltEngine, RibltMessage};
use crate::reconcile::engine::Progress;
use crate::reconcile::error::Result;
use crate::reconcile::session::Session;
use crate::reconcile::source::{Item, ItemId, MemorySource};

/// Identity width both engines are measured at: a CIDv1 `dag-cbor/sha2-256`.
pub(crate) const IDENTITY_WIDTH: usize = 36;

/// A deterministic identity of exactly [`IDENTITY_WIDTH`] bytes.
pub(crate) fn id(seed: u64) -> ItemId {
    use sha2::Digest;
    let digest = sha2::Sha256::digest(seed.to_be_bytes());
    let mut bytes = digest.to_vec();
    bytes.extend_from_slice(&digest[..IDENTITY_WIDTH - digest.len()]);
    ItemId::new(bytes)
}

/// What one session cost, in the terms both engines can be asked for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Measurement {
    /// Encoded bytes crossing the wire, both directions, envelope included.
    pub bytes: usize,
    /// Peer messages the initiating side consumed.
    pub rounds: usize,
    /// Coded cells the initiating side consumed. Zero for the range engine,
    /// which codes nothing — the tail this study reports is a property of the
    /// rateless stream and the range engine has no analogue of it.
    pub symbols: usize,
    /// Identities the initiating side must pull.
    pub need: usize,
    /// Identities the initiating side holds that the peer does not.
    pub have: usize,
}

/// A pair of sources of about `n` items whose symmetric difference is `d`, with
/// the divergent items scattered rather than clustered — the harder case for a
/// range protocol and a neutral one for a sketch.
///
/// The seed moves the identity space as well as which items diverge, because a
/// coded stream is a function of the set and not of the order it was built in.
pub(crate) fn diverged(n: usize, d: usize, seed: u64) -> (MemorySource, MemorySource) {
    let mut universe: Vec<u64> = (0..(n + d) as u64).collect();
    shuffle(&mut universe, seed);

    let (divergent, shared) = universe.split_at(d);
    let local_only = d.div_ceil(2);

    let mut local = shared.to_vec();
    local.extend_from_slice(&divergent[..local_only]);
    let mut remote = shared.to_vec();
    remote.extend_from_slice(&divergent[local_only..]);

    let offset = seed.wrapping_mul(0x0010_0000_0000_0001);
    (source(&local, offset), source(&remote, offset))
}

fn source(seeds: &[u64], offset: u64) -> MemorySource {
    MemorySource::new(seeds.iter().map(|seed| {
        let seed = seed.wrapping_add(offset);
        Item::new(seed % 4, id(seed))
    }))
    .expect("seeds are distinct")
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

/// Reconciles `local` against `remote` over the range engine.
pub(crate) fn rbsr(local: &MemorySource, remote: &MemorySource) -> Result<Measurement> {
    let mut initiator = Session::new(RbsrEngine::initiator(local));
    let mut responder = Session::new(RbsrEngine::responder(remote));
    let mut bytes = 0usize;

    let opening = initiator.next_outbound()?.expect("the initiator opens");
    bytes += codec::encode(&opening)?.len();
    let mut message = opening;

    loop {
        responder.ingest(message)?;
        let reply = responder
            .next_outbound()?
            .expect("the responder always answers");
        bytes += codec::encode(&reply)?.len();
        initiator.ingest(reply)?;
        match initiator.next_outbound()? {
            None => break,
            Some(next) => {
                bytes += codec::encode(&next)?.len();
                message = next;
            }
        }
    }

    Ok(Measurement {
        bytes,
        rounds: initiator.rounds(),
        symbols: 0,
        need: initiator.diff().need().len(),
        have: initiator.diff().have().len(),
    })
}

/// Reconciles `local` against `remote` over the rateless engine.
pub(crate) fn riblt(local: &MemorySource, remote: &MemorySource) -> Result<Measurement> {
    let mut decoder = Session::new(RibltEngine::decoder(local)?);
    let mut encoder = Session::new(RibltEngine::encoder(remote)?);
    let mut bytes = 0usize;
    let mut symbols = 0usize;

    let mut request = decoder.next_outbound()?.expect("the decoder opens");
    loop {
        bytes += codec::encode(&request)?.len();
        encoder.ingest(request)?;
        let batch = encoder
            .next_outbound()?
            .expect("the encoder always answers a request");
        bytes += codec::encode(&batch)?.len();
        symbols += cells(&batch);
        if decoder.ingest(batch)? == Progress::Converged {
            break;
        }
        request = decoder
            .next_outbound()?
            .expect("a decoder short of the difference asks again");
    }

    Ok(Measurement {
        bytes,
        rounds: decoder.rounds(),
        symbols,
        need: decoder.diff().need().len(),
        have: decoder.diff().have().len(),
    })
}

/// Coded cells in one encoder answer. A request carries none.
fn cells(message: &RibltMessage) -> usize {
    match message {
        RibltMessage::Symbols { symbols, .. } => symbols.len(),
        RibltMessage::Request { .. } => 0,
    }
}

/// Where the range engine's listing regime ends and `O(d log n)` begins.
///
/// Both studies that use this driver run above it, because below it the range
/// engine lists whole mismatching ranges and a comparison there is about the
/// listing rule rather than about the protocols.
pub(crate) const LISTING_BOUNDARY: usize = BRANCHING_FACTOR * ID_LIST_THRESHOLD;

/// One point's result for both engines, or the reason an engine gave up.
pub(crate) struct Point {
    pub rbsr: StdResult<Measurement, String>,
    pub riblt: StdResult<Measurement, String>,
}

/// Both engines against the same pair of sets.
pub(crate) fn measure(n: usize, d: usize, seed: u64) -> Point {
    let (local, remote) = diverged(n, d, seed);
    Point {
        rbsr: rbsr(&local, &remote).map_err(|error| error.to_string()),
        riblt: riblt(&local, &remote).map_err(|error| error.to_string()),
    }
}

/// One point as two CSV rows, one per engine. A cap hit prints as an outcome
/// rather than as a missing line, so a study never loses a point in silence.
pub(crate) fn row(n: usize, d: usize, seed: u64, point: &Point) {
    for (engine, result) in [("ranges", &point.rbsr), ("riblt", &point.riblt)] {
        match result {
            Ok(m) => println!(
                "{n},{d},{seed:#x},{engine},{},{},{},{},{},converged",
                m.bytes, m.rounds, m.symbols, m.need, m.have
            ),
            Err(error) => println!("{n},{d},{seed:#x},{engine},,,,,,{error}"),
        }
    }
}

/// The header both studies' raw-draw tables carry.
pub(crate) const ROW_HEADER: &str = "n,d,seed,engine,bytes,rounds,symbols,need,have,outcome";
