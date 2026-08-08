//! Range-based set reconciliation (RBSR, a.k.a. Negentropy).
//!
//! Two peers discover the difference between their sets while exchanging data
//! proportional to `|A △ B|` rather than `|A ∪ B|`, converging in `O(log n)`
//! rounds.
//!
//! The keyspace is tiled by [`Range`]s. The initiator opens with one full-range
//! [`Fingerprint`] and thereafter maps every incoming range to exactly one
//! outgoing range, never changing the tiling; only the responder refines, either
//! splitting a mismatching range into [`caps::BRANCHING_FACTOR`] sub-ranges or,
//! once the range holds at most [`caps::ID_LIST_THRESHOLD`] items, listing it
//! out. A single initiator-driven session teaches the initiator both what it
//! needs and what it has that the peer lacks; the responder stays stateless.

pub mod caps;
mod fingerprint;
mod message;

pub use fingerprint::{Accumulator, Fingerprint, FINGERPRINT_LEN};
pub use message::{Mode, Range, RbsrMessage};
