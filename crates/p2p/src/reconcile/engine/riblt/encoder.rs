//! The oblivious side of a session: an unbounded stream of coded symbols.
//!
//! The encoder learns nothing. It never sees the peer's set, never branches on
//! anything the peer said, and produces the same stream for every peer — which
//! is what makes one stream broadcastable to many behind peers, and what makes
//! the whole protocol half a round trip rather than a narrowing conversation.
//!
//! Its cost is the honest one: the set is enumerated once per session, at
//! `O(n)` to load plus `O(log m)` amortized per emitted cell. The accepted
//! design deliberately excludes a standing sketch maintained across writes,
//! which would move that cost onto every local commit; that option, and the
//! write-tax measurement it would need, belong to a later phase.

use super::symbol::{CodedSymbol, HashedSymbol, ADD};
use super::window::CodingWindow;

/// Produces the coded-symbol stream of one sealed set.
pub struct Encoder {
    window: CodingWindow,
    width: usize,
}

impl Encoder {
    /// An encoder over an empty set of `width`-byte symbols.
    pub fn new(width: usize) -> Self {
        Self {
            window: CodingWindow::new(),
            width,
        }
    }

    /// Adds one source symbol. All symbols must be added before the first
    /// [`Self::produce_next`]: the stream is defined over a sealed set.
    pub fn add(&mut self, symbol: Vec<u8>) {
        self.window.add(HashedSymbol::new(symbol));
    }

    /// Produces the next coded symbol of the stream.
    pub fn produce_next(&mut self) -> CodedSymbol {
        let mut cell = CodedSymbol::zero(self.width);
        self.window.apply(&mut cell, ADD);
        cell
    }
}
