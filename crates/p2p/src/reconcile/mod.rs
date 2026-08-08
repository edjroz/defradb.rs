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
//!   including the count fold that defeats additive cancellation. Checked, not
//!   assumed: `fingerprint_tests.rs` pins six vectors produced by running the Go
//!   reference's own `FingerprintOf`.
//! - Caps: branching factor 16, ID-list threshold 64, max IDs per range 64, max
//!   ranges per message 16384 (see [`engine::rbsr::caps`]) and max rounds 32
//!   (see [`session::MAX_ROUNDS`]). Phase 1's measurements found no reason to
//!   retune any of them: sessions converge in at most four rounds at n=100 000.
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
//!    [`engine::rbsr::SegmentTree`] on top of the source, because a RIBLT engine
//!    needs to enumerate items but has no use for range fingerprints.
//! 4. **`Engine` and `Session` are abstractions Go does not have.** Go exposes a
//!    concrete `Initiator` plus a free `Respond` function. The trait exists so
//!    the RIBLT engine can be dropped in without reworking the session loop; see
//!    [`engine::Engine`] for the mapping.
//! 5. **The round cap lives on the session, not the initiator.** Same value and
//!    same off-by-one as Go, but because it is session policy it also bounds a
//!    *responding* session, which Go's stateless `Respond` function does not.
//!    A driver that ingests on the responder first therefore sees a stalled
//!    session attributed to the responder where Go would attribute it to the
//!    initiator. See [`session::MAX_ROUNDS`].
//! 6. **Messages have a real wire encoding.** Go left its encoding undefined and
//!    approximated message size with a per-element heuristic. [`codec`] defines a
//!    versioned CBOR envelope, so sizes here are measured rather than estimated.
//!    No wire parity with Go is claimed — that is the deferred interop phase.
//! 7. **The initiator does not transmit its terminal all-skip message.** Go's
//!    driver does the same, but leaves it implicit; here [`session::Session`]
//!    makes it explicit by returning no outbound message once converged.
//! 8. **The ID-list cap is enforced on receipt, not only on emission.** Go
//!    guarantees it when building a list; both sides here also reject an
//!    incoming list that exceeds it, so a peer cannot make the local node
//!    allocate past the cap.
//! 9. **A response is capped by payload bytes as well as by range count.** Go's
//!    cap set is internally inconsistent — 16384 ranges of 64 identities is 32
//!    MiB against its declared 16 MiB frame — which is latent there only because
//!    Go never enforces a frame cap. Having a real codec makes it reachable, so
//!    [`engine::rbsr::caps::MAX_LISTED_ID_BYTES`] bounds the payload directly.
//!
//! One thing that is *not* protocol, and matters when comparing byte counts:
//! `ciborium` encodes structs as maps with string keys, so every range pays for
//! the literal field names and every mode and bound variant pays for its own
//! name. That inflates a message by roughly 25 bytes per range against a compact
//! representation. It is a serde representation choice, identical for whatever
//! engine sits behind it, and it belongs to the deferred wire-format phase.
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
