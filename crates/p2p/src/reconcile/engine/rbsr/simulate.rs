//! A two-engine, in-memory reconciliation driver for tests.
//!
//! Every message crosses the real [`codec`](crate::reconcile::codec), so the
//! byte counts below are measured rather than estimated and the codec is
//! exercised by every protocol test. Each message is also checked against the
//! tiling invariant, so a bug that breaks the keyspace cover fails here rather
//! than silently changing what converges.

use super::engine::RbsrEngine;
use sha2::Digest;

use super::message::RbsrMessage;
use crate::reconcile::codec;
use crate::reconcile::error::Result;
use crate::reconcile::session::Session;
use crate::reconcile::source::{Bound, Item, ItemId, MemorySource};

/// What one simulated session produced.
pub(super) struct Outcome {
    /// Identities the initiator must pull, ascending.
    pub need: Vec<ItemId>,
    /// Identities the initiator must push, ascending.
    pub have: Vec<ItemId>,
    /// Peer messages the initiator consumed.
    pub rounds: usize,
    /// Total encoded bytes crossing the wire in both directions.
    pub bytes: usize,
}

/// Reconciles `local` (as initiator) against `remote` (as responder).
pub(super) fn run(local: &MemorySource, remote: &MemorySource) -> Result<Outcome> {
    let mut initiator = Session::new(RbsrEngine::initiator(local));
    let mut responder = Session::new(RbsrEngine::responder(remote));
    let mut bytes = 0usize;

    let opening = initiator.next_outbound()?.expect("the initiator opens");
    let (mut message, sent) = hop(&opening)?;
    bytes += sent;

    loop {
        assert_tiles(&message);
        responder.ingest(message)?;
        let reply = responder
            .next_outbound()?
            .expect("the responder always answers");
        assert_tiles(&reply);
        let (reply, sent) = hop(&reply)?;
        bytes += sent;

        initiator.ingest(reply)?;
        match initiator.next_outbound()? {
            None => break,
            Some(next) => {
                let (next, sent) = hop(&next)?;
                bytes += sent;
                message = next;
            }
        }
    }

    let mut need = initiator.diff().need().to_vec();
    let mut have = initiator.diff().have().to_vec();
    need.sort();
    have.sort();

    Ok(Outcome {
        need,
        have,
        rounds: initiator.rounds(),
        bytes,
    })
}

/// A deterministic 32-byte identity for a seed, standing in for a CID.
pub(super) fn id(seed: u64) -> ItemId {
    ItemId::new(sha2::Sha256::digest(seed.to_be_bytes()).to_vec())
}

/// Seals a set of seeds into a source.
///
/// Heights are derived from the seed so the layered ordering is exercised while
/// each identity still maps to exactly one sort key, which is what lets the
/// tests compare diffs by identity alone.
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
/// `d`, with the divergent items scattered through the keyspace rather than
/// clustered — the harder case for a range protocol.
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
fn hop(message: &RbsrMessage) -> Result<(RbsrMessage, usize)> {
    let bytes = codec::encode(message)?;
    let decoded: RbsrMessage = codec::decode(&bytes)?;
    assert_eq!(&decoded, message, "the codec must round-trip every message");
    Ok((decoded, bytes.len()))
}

/// Asserts the ranges tile `[Min, Max)` completely, in strictly ascending order,
/// with the final range ending at [`Bound::Max`].
fn assert_tiles(message: &RbsrMessage) {
    let ranges = message.ranges();
    if ranges.is_empty() {
        return;
    }

    let mut previous = Bound::Min;
    for (position, range) in ranges.iter().enumerate() {
        let last = position + 1 == ranges.len();
        assert_eq!(
            last,
            range.upper_bound == Bound::Max,
            "only the final range may end at Max"
        );
        assert!(
            previous < range.upper_bound,
            "range bounds must strictly ascend"
        );
        previous = range.upper_bound.clone();
    }
}
