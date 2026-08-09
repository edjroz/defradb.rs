//! Rateless set reconciliation (RIBLT).
//!
//! Two peers discover the difference between their sets while exchanging bytes
//! proportional to `|A △ B|` and *independent of `|A ∪ B|`*. The paper's
//! protocol is half a round trip — one side streams until told to stop — but
//! this one is not: deviation 3 below turns the stream into a pull with
//! doubling batches, so a session takes `O(log d)` round trips, measured at one
//! up to `d = 8`, six at `d = 100` and nine at `d = 1000`. That is still far
//! fewer bytes than the range engine at small `d`, and it is *not* the
//! round-trip win the paper describes; anything reasoning about high-latency
//! links has to use the measured rounds.
//!
//! Where the [`rbsr`](super::rbsr) engine spends interactive rounds
//! locating the difference inside an ordered keyspace, this engine never locates
//! anything: the shared majority annihilates itself when the decoder subtracts
//! its own sketch, and what is left on the wire is only ever about the
//! difference. There is no order, no range, and no tiling here — the whole
//! sort-key axis of the RBSR engine simply does not exist, replaced by XOR
//! arithmetic over a hash-driven index mapping.
//!
//! The trade is determinism for obliviousness. RBSR converges in a fixed round
//! bound; peeling is probabilistic, and the number of coded symbols a given
//! difference needs is a distribution rather than a number. An unlucky prefix
//! costs a few more symbols, never a failure — see [`engine`] for how that is
//! signalled and [`caps`] for where it stops.
//!
//! # Provenance
//!
//! An implementation of *Practical Rateless Set Reconciliation* (Yang, Gilad,
//! Alizadeh, ACM SIGCOMM 2024), following the authors' reference implementation
//! `github.com/yangl1996/riblt`. Keeping the two observably alike is
//! deliberate: the model constants a comparison campaign is built on come from
//! that work, and a silent divergence in the mapping or the peel would poison
//! the comparison. The reference was cloned read-only and driven by a small Go
//! program over its exported API to produce fixed vectors; nothing of it is
//! vendored here.
//!
//! ## Adopted from the reference unchanged
//!
//! - The index mapping: PRNG state seeded with the symbol's hash, multiplied by
//!   `0xda942042e4dd58b5` per step, with the approximated inverse-CDF jump that
//!   gives participation probability `1/(1+i/2)`. Pinned bit for bit against
//!   reference vectors in `mapping_tests.rs`.
//! - The coded symbol: XOR of member symbols, XOR of member hashes, signed
//!   member count. Pinned cell for cell in `encoder_tests.rs`.
//! - The decoder's three windows and its peel, including the invariant that a
//!   cell queued as decodable stays decodable. Nine whole decode runs are
//!   pinned in `decoder_tests.rs`, including the exact symbol count each took.
//!
//! ## Deliberate deviations
//!
//! 1. **The checksum is SHA-256 truncated to eight bytes**, read little-endian,
//!    not SipHash. The reference leaves the hash to the caller and its own tests
//!    pick arbitrary SipHash keys, so there is no wire to be compatible with;
//!    the RBSR engine already hashes with SHA-256, and one primitive is enough.
//!    Consequence for comparability: none for symbol counts, which depend on the
//!    hash only through its uniformity. Anything comparing raw cells across
//!    implementations must use this hash, which is what the vectors do.
//! 2. **The reference's `panic` on an impossible cell is replaced by two
//!    invariants, not by a skip.** The reference is explicit that it is built
//!    for trusted input and enforces that with a panic; a node cannot panic, and
//!    simply skipping the cell removes the check without replacing what made it
//!    unreachable. So convergence is counted as "every residual is the identity"
//!    rather than "every queued cell was visited", and a cell that has ever been
//!    fully explained is never peeled again. See [`decoder`] for why both are
//!    load-bearing and what is deliberately not claimed.
//! 3. **The session is a pull, not a push.** The paper's encoder streams until
//!    told to stop. The shared session loop drains what a side has to say and
//!    then waits, so an encoder that always had another cell would never yield.
//!    The decoder therefore asks for batches that double in size; see
//!    [`message`]. Same exchange, explicit flow control, one small frame per
//!    batch rather than one per cell.
//! 4. **The set is enumerated once per session.** The reference also offers a
//!    `Sketch` maintained across insertions and deletions, which would move the
//!    cost onto every local commit. The accepted design excludes that until its
//!    write tax has been measured the way the ordered index's was.
//! 5. **Symbols have a declared width and a cap.** The reference's symbol type
//!    is fixed at compile time; here the width comes from the item identity, is
//!    stated in every frame, and is bounded by
//!    [`caps::MAX_SYMBOL_BYTES`] — because a peer can declare it.
//!
//! One thing that is *not* protocol, and matters when comparing byte counts:
//! `ciborium` encodes each cell as a map with string keys, so a cell costs 68
//! bytes on this wire against the model's 45 of payload. That is the same
//! representation choice every engine here pays and belongs to the deferred
//! wire-format phase.

pub mod caps;
mod decoder;
mod encoder;
mod engine;
mod mapping;
mod message;
mod symbol;
mod window;

pub use decoder::Decoder;
pub use encoder::Encoder;
pub use engine::RibltEngine;
pub use message::RibltMessage;
pub use symbol::CodedSymbol;

#[cfg(test)]
mod simulate;

#[cfg(test)]
mod convergence_proptests;

#[cfg(test)]
#[path = "adversarial_tests.rs"]
mod adversarial_tests;

#[cfg(test)]
#[path = "decoder_tests.rs"]
mod decoder_tests;

#[cfg(test)]
#[path = "encoder_tests.rs"]
mod encoder_tests;

#[cfg(test)]
#[path = "engine_tests.rs"]
mod engine_tests;

#[cfg(test)]
#[path = "mapping_tests.rs"]
mod mapping_tests;

#[cfg(test)]
#[path = "overhead_tests.rs"]
mod overhead_tests;
