//! The protocol-agnostic engine seam.
//!
//! An engine owns one side of one reconciliation session. It is driven by two
//! calls, and the drive loop never assumes strict alternation:
//!
//! ```text
//! loop {
//!     while let Some(msg) = engine.next_outbound()? { send(msg) }
//!     if converged { break }
//!     match engine.ingest(recv()?)? { … }
//! }
//! ```
//!
//! # Why this shape fits RIBLT as well as RBSR
//!
//! RBSR is a strictly alternating multi-round protocol: the initiator emits one
//! range message, the responder answers with one refined message, and each side
//! emits exactly one message per turn. That maps onto `next_outbound` returning
//! `Some` once per round and `None` afterwards.
//!
//! A rateless-IBLT engine has the opposite shape — a one-way stream of coded
//! symbols with no per-symbol reply:
//!
//! | RIBLT concept | Mapping onto this trait |
//! |---|---|
//! | Encoder emitting symbol *i* of an unbounded stream | `next_outbound` returns `Some(batch)` on every call, with no `ingest` in between; the drive loop's inner `while` drains as many as flow control allows |
//! | Decoder peeling symbols, still short of the difference | `ingest` returns `Progress::Continue` and `next_outbound` returns `None` — a silent side is expressible |
//! | Decoder finishing the peel | `ingest` returns `Progress::Converged` and populates [`Diff`] |
//! | Decoder telling the encoder to stop | the decoder's next `next_outbound` returns the terminal acknowledgement |
//! | Encoder learning the session is over | its `ingest` of that acknowledgement returns `Progress::Converged` |
//!
//! Nothing above needs a trait change: the two requirements are that
//! `next_outbound` may fire repeatedly without an intervening `ingest`, and that
//! either side may be silent for arbitrarily many turns. `engine_tests.rs`
//! carries a compiled shape stub exercising exactly that, so the claim is
//! checked rather than asserted.

use super::error::{ReconcileError, Result};
use super::source::ItemId;

/// Which reconciliation protocol a session runs.
///
/// A session's two peers must agree, and the initiator chooses: it names the
/// engine in its opening frame and a responder that does not implement it
/// refuses the session outright. Both engines produce the same [`Diff`] and hand
/// it to the same fetch path, so the choice is about how the difference is
/// found, never about what is done with it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[repr(u8)]
pub enum EngineKind {
    /// Range-based reconciliation: deterministic, `O(log n)` interactive
    /// rounds, and the only one that can scope a session to a sub-range.
    #[default]
    Rbsr = 0,
    /// Rateless sketch reconciliation: `O(d)` bytes independent of set size, in
    /// about half a round trip, at the cost of a probabilistic symbol count.
    Riblt = 1,
}

impl EngineKind {
    /// Reads an engine tag off the wire.
    pub fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            tag if tag == Self::Rbsr as u8 => Ok(Self::Rbsr),
            tag if tag == Self::Riblt as u8 => Ok(Self::Riblt),
            other => Err(ReconcileError::UnsupportedEngine { tag: other }),
        }
    }
}

/// What a peer message told the engine about the session's progress.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Progress {
    /// More rounds (or more symbols) are needed.
    Continue,
    /// The local side has learned its full difference; the session is over.
    Converged,
}

/// The set difference a session discovered, from the local peer's point of view.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Diff {
    need: Vec<ItemId>,
    have: Vec<ItemId>,
}

impl Diff {
    /// Items the peer holds and the local side lacks — the pull list.
    pub fn need(&self) -> &[ItemId] {
        &self.need
    }

    /// Items the local side holds and the peer lacks — the push list.
    pub fn have(&self) -> &[ItemId] {
        &self.have
    }

    /// Whether the two sets were already identical.
    pub fn is_empty(&self) -> bool {
        self.need.is_empty() && self.have.is_empty()
    }

    /// Records an item the local side must pull.
    pub fn record_need(&mut self, id: ItemId) {
        self.need.push(id);
    }

    /// Records an item the local side must push.
    pub fn record_have(&mut self, id: ItemId) {
        self.have.push(id);
    }
}

/// One side of one reconciliation session.
///
/// Engines are single-use: build one per session, drive it to convergence, take
/// its [`Diff`], drop it.
pub trait Engine {
    /// The protocol message this engine exchanges.
    type Message;

    /// Returns the next message to send, or `None` when this side has nothing
    /// to say right now. May legitimately return `Some` many times in a row.
    fn next_outbound(&mut self) -> Result<Option<Self::Message>>;

    /// Consumes a message from the peer.
    fn ingest(&mut self, message: Self::Message) -> Result<Progress>;

    /// The difference learned so far; complete once the session has converged.
    fn diff(&self) -> &Diff;
}

pub mod rbsr;
pub mod riblt;

#[cfg(test)]
#[path = "engine_tests.rs"]
mod engine_tests;
