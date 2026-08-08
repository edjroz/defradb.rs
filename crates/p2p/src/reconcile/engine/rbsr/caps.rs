//! Protocol caps, baked in so a single misbehaving peer cannot cause unbounded
//! work, allocation, or message size.
//!
//! Every value here is adopted verbatim from the Go reference engine. Phase 1
//! measured no reason to diverge, and holding them equal is what makes the
//! phase 3 cross-node comparison meaningful; any future change belongs in a
//! measurement, not a guess.

/// Number of sub-ranges a responder splits a mismatching fingerprint range into.
/// Higher fan-out means fewer rounds but larger messages.
pub const BRANCHING_FACTOR: usize = 16;

/// Largest item count in a range for which a responder emits an explicit ID list
/// instead of splitting further.
pub const ID_LIST_THRESHOLD: usize = 64;

/// Hard cap on identities in a single ID-list range. Held equal to
/// [`ID_LIST_THRESHOLD`] so a listed range never exceeds the cap; named
/// separately so the list-vs-split threshold and the frame guard can be tuned
/// independently.
pub const MAX_IDS_PER_RANGE: usize = 64;

/// Bound on the number of ranges a single response may carry. Once reached, the
/// responder defers refinement of the remaining keyspace to a later round rather
/// than emitting an oversized message.
pub const MAX_RANGES_PER_MESSAGE: usize = 1 << 14;

/// Bound on the identity bytes a single response may list.
///
/// [`MAX_RANGES_PER_MESSAGE`] alone does not bound a message. The Go engine caps
/// ranges and IDs-per-range but never enforces its frame cap, and the two cap
/// sets are mutually inconsistent: 16384 ranges each listing 64 identities of 32
/// bytes is 32 MiB of payload against a 16 MiB frame. Bounding the payload
/// directly closes that, leaving the rest of the frame budget for range bounds
/// and CBOR framing, which cost well under a megabyte even at the range cap.
///
/// A response that would exceed this defers the remaining refinement to a later
/// round exactly as the range cap does, so the session still converges.
pub const MAX_LISTED_ID_BYTES: usize = crate::reconcile::codec::MAX_FRAME_BYTES / 2;
