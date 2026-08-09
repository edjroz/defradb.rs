//! The two frames a RIBLT session exchanges.
//!
//! The protocol the paper describes is one-way: the encoder streams and the
//! decoder says stop. This is that protocol turned around into a pull, because
//! the session loop both engines run under drains what a side has to say and
//! then waits — an encoder that answered "another cell" forever would never
//! yield the loop. A decoder that asks for a batch and then asks for a larger
//! one is the same exchange with the flow control made explicit, and it costs
//! one small frame per batch rather than one per cell.
//!
//! Both frames carry the symbol width. The decoder cannot infer it when its own
//! set is empty, which is exactly the case where a session matters most, so the
//! width is stated rather than assumed and a disagreement is an error.

use serde::{Deserialize, Serialize};

use super::symbol::CodedSymbol;
use crate::reconcile::codec::{MessageKind, WireMessage};

/// One message of a RIBLT session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RibltMessage {
    /// The decoder asking for the next `symbols` cells of the stream.
    Request {
        /// Cells wanted.
        symbols: u32,
        /// Symbol width the decoder reconciles at; `0` if its set is empty.
        width: u16,
    },
    /// The encoder answering with a prefix of its stream.
    Symbols {
        /// Symbol width every cell in the batch carries.
        width: u16,
        /// The cells, in stream order.
        symbols: Vec<CodedSymbol>,
    },
}

impl RibltMessage {
    /// A decoder's request for `symbols` more cells.
    pub fn request(width: usize, symbols: usize) -> Self {
        Self::Request {
            symbols: symbols as u32,
            width: width as u16,
        }
    }

    /// An encoder's batch of cells.
    pub fn symbols(width: usize, symbols: Vec<CodedSymbol>) -> Self {
        Self::Symbols {
            width: width as u16,
            symbols,
        }
    }

    /// How many coded symbols the message carries, which is what a session
    /// counts against its symbol budget.
    pub fn symbol_count(&self) -> usize {
        match self {
            Self::Request { .. } => 0,
            Self::Symbols { symbols, .. } => symbols.len(),
        }
    }
}

impl WireMessage for RibltMessage {
    const KIND: MessageKind = MessageKind::RibltSymbols;
}
