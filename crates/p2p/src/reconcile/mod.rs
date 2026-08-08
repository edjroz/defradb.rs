//! Transport-agnostic set reconciliation.
//!
//! Two peers holding overlapping sets discover their difference by exchanging
//! bytes proportional to that difference rather than to the size of the sets.
//! This module is pure protocol logic: no sockets, no storage, no timers. The
//! engines read a local set through [`source::ItemSource`], speak through
//! [`engine::Engine`], are driven by [`session::Session`], and are framed by
//! [`codec`]. Nothing outside this module's own tests calls it yet; the
//! transport binding and the headstore-backed `ItemSource` land with the next
//! phase.
//!
//! # Provenance
//!
//! The RBSR engine is a port of the Go reference implementation in
//! `sourcenetwork/defradb`'s `internal/db/p2p/negentropy` package, which is
//! itself an implementation of the Negentropy V1 range-based set reconciliation
//! protocol. Keeping the two observably alike is deliberate: a later phase
//! compares measured Rust and Go behavior on the same scenarios, and a silent
//! divergence in constants or heuristics would poison that comparison.
//!
//! ## Adopted from the Go engine unchanged
//!
//! - Sort key `heightBE8 || id`, so the keyspace is ordered by causal layer and
//!   then hash-ordered within a layer.
//! - Fingerprint `SHA256( (Σ SHA256(id)) mod 2^256 || uvarint(count) )[..16]`,
//!   including the count fold that defeats additive cancellation.
//! - Caps: branching factor 16, ID-list threshold 64, max IDs per range 64, max
//!   ranges per message 16384, max rounds 32. See [`engine::rbsr::caps`].
//! - Split heuristic: even division of the *index* window into up to 16 buckets
//!   whose bounds land on real item sort keys, so both peers derive identical
//!   windows from the same bounds.
//! - Topology: a stateless responder, an initiator that learns both its need and
//!   its have set, and convergence declared when the initiator's outgoing
//!   message is all-skip.
//!
//! ## Deliberate deviations
//!
//! Each of these is a representation or structure choice; none changes what
//! ranges are split, what is listed, or what bytes are hashed.
//!
//! 1. **Range bounds are an enum.** Go encodes the low and high sentinels as an
//!    empty and a nil byte slice and notes that the distinction may not survive
//!    a CBOR round trip. [`source::Bound`] makes them variants, whose derived
//!    ordering is exactly Go's `CompareBound` and which round-trip unambiguously.
//! 2. **The accumulator holds four 64-bit limbs**, not 32 bytes. The arithmetic
//!    modulo 2^256 is identical; a test pins the limb implementation against a
//!    byte-wise transcription of Go's `acc256.addInto`.
//! 3. **`ItemSource` is the ordered-item seam only.** Go's `Storage` interface
//!    also owned range fingerprints; here those live in
//!    `engine::rbsr::SegmentTree` on top of the source, because a RIBLT engine
//!    needs to enumerate items but has no use for range fingerprints.
//! 4. **`Engine` and `Session` are abstractions Go does not have.** Go exposes a
//!    concrete `Initiator` plus a free `Respond` function. The trait exists so
//!    the RIBLT engine can be dropped in without reworking the session loop; see
//!    [`engine::Engine`] for the mapping.
//! 5. **The round cap lives on the session, not the initiator.** Same value, same
//!    fail-with-error behavior; it is session policy that applies to any engine.
//! 6. **Messages have a real wire encoding.** Go left its encoding undefined and
//!    approximated message size with a per-element heuristic. [`codec`] defines a
//!    versioned CBOR envelope, so sizes here are measured rather than estimated.
//!    No wire parity with Go is claimed — that is the deferred interop phase.
//! 7. **The initiator does not transmit its terminal all-skip message.** Go's
//!    driver does the same, but leaves it implicit; here [`session::Session`]
//!    makes it explicit by returning no outbound message once converged.
//!
//! ## Deferred, not deviated
//!
//! - `ItemSource` is infallible over a sealed snapshot, matching Go. A
//!   storage-backed source that can fail mid-session is a phase-2 concern.
//! - The fingerprint index is built once per session in `O(n)`, matching Go. A
//!   standing index maintained across writes is a measured option for a later
//!   phase, not a phase-1 guess.

pub mod codec;
pub mod engine;
pub mod error;
pub mod session;
pub mod source;

pub use codec::{decode, encode, MessageKind, WireMessage, PROTOCOL_VERSION};
pub use engine::{Diff, Engine, Progress};
pub use error::{ReconcileError, Result};
pub use session::Session;
pub use source::{Bound, Item, ItemId, ItemSource, MemorySource, SortKey};
