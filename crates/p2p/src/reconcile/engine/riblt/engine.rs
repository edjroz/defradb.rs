//! The RIBLT engine: one side of one rateless-sketch session.
//!
//! The asymmetry is sharper than RBSR's. There the two roles run the same
//! keyspace machinery from opposite ends; here the encoder holds a stream
//! generator and no session state at all, while the decoder holds everything
//! that is being solved. So the two roles are two variants rather than a flag,
//! and each refuses the other's message outright — a decoder handed a request
//! has no answer that is not a fabrication.
//!
//! # Stopping
//!
//! The decoder decides, and it has exactly three answers. Every cell received
//! accounted for is [`Progress::Converged`], and the session ends by the
//! initiator closing its stream, which the phase 2 responder already reads as
//! "we are done". A stall, or a residual that has not resolved, is
//! [`Progress::Continue`] with a larger request — never a failure, because a
//! stalled peel means the prefix is short, not wrong. Reaching
//! [`caps::MAX_CODED_SYMBOLS`] is
//! [`SymbolCapExceeded`](crate::reconcile::ReconcileError::SymbolCapExceeded).

use super::caps;
use super::decoder::Decoder;
use super::encoder::Encoder;
use super::message::RibltMessage;
use crate::reconcile::engine::{Diff, Engine, Progress};
use crate::reconcile::error::{ReconcileError, Result};
use crate::reconcile::source::{ItemId, ItemSource};

/// Which side of the session this engine plays.
enum Role {
    /// Streams cells and learns nothing.
    Encoder { encoder: Encoder, produced: usize },
    /// Subtracts, peels, and learns the difference. Boxed because a decoder
    /// carries every cell of the session and an encoder carries none.
    Decoder {
        decoder: Box<Decoder>,
        received: usize,
    },
}

/// A rateless set-reconciliation engine over a sealed local set.
pub struct RibltEngine {
    role: Role,
    width: usize,
    pending: Option<RibltMessage>,
    diff: Diff,
}

impl RibltEngine {
    /// Builds the decoding side, which opens the session by asking for cells.
    pub fn decoder<S: ItemSource>(source: &S) -> Result<Self> {
        let width = session_width(source)?;
        let mut decoder = Decoder::new(width);
        for index in 0..source.len() {
            decoder.add(source.id(index).as_bytes().to_vec());
        }
        Ok(Self {
            role: Role::Decoder {
                decoder: Box::new(decoder),
                received: 0,
            },
            width,
            pending: Some(RibltMessage::request(width, caps::INITIAL_SYMBOL_BATCH)),
            diff: Diff::default(),
        })
    }

    /// Builds the encoding side, which stays silent until asked.
    pub fn encoder<S: ItemSource>(source: &S) -> Result<Self> {
        let width = session_width(source)?;
        let mut encoder = Encoder::new(width);
        for index in 0..source.len() {
            encoder.add(source.id(index).as_bytes().to_vec());
        }
        Ok(Self {
            role: Role::Encoder {
                encoder,
                produced: 0,
            },
            width,
            pending: None,
            diff: Diff::default(),
        })
    }

    /// Answers a decoder's request with the next cells of the stream.
    fn serve(&mut self, wanted: u32, peer_width: u16) -> Result<Progress> {
        self.adopt_width(peer_width)?;
        let Role::Encoder { encoder, produced } = &mut self.role else {
            return Err(ReconcileError::UnexpectedMessage);
        };

        let remaining = caps::MAX_CODED_SYMBOLS - *produced;
        if remaining == 0 {
            return Err(ReconcileError::SymbolCapExceeded {
                max: caps::MAX_CODED_SYMBOLS,
            });
        }
        let count = (wanted as usize)
            .clamp(1, caps::MAX_SYMBOL_BATCH)
            .min(remaining);

        let batch = (0..count).map(|_| encoder.produce_next()).collect();
        *produced += count;
        self.pending = Some(RibltMessage::symbols(self.width, batch));
        Ok(Progress::Continue)
    }

    /// Takes a batch of cells, peels, and decides whether to ask for more.
    fn peel(
        &mut self,
        peer_width: u16,
        batch: Vec<super::symbol::CodedSymbol>,
    ) -> Result<Progress> {
        if batch.is_empty() {
            return Err(ReconcileError::UnexpectedMessage);
        }
        self.adopt_width(peer_width)?;
        let width = self.width;
        let Role::Decoder { decoder, received } = &mut self.role else {
            return Err(ReconcileError::UnexpectedMessage);
        };

        if *received + batch.len() > caps::MAX_CODED_SYMBOLS {
            return Err(ReconcileError::SymbolCapExceeded {
                max: caps::MAX_CODED_SYMBOLS,
            });
        }
        *received += batch.len();
        for cell in batch {
            decoder.add_coded_symbol(cell)?;
        }
        decoder.try_decode();

        if !decoder.is_decoded() {
            self.pending = Some(RibltMessage::request(width, next_batch(*received)));
            return Ok(Progress::Continue);
        }

        for symbol in decoder.remote() {
            self.diff.record_need(ItemId::new(symbol.to_vec()));
        }
        for symbol in decoder.local() {
            self.diff.record_have(ItemId::new(symbol.to_vec()));
        }
        self.pending = None;
        Ok(Progress::Converged)
    }

    /// Reconciles the peer's declared width with the local one.
    ///
    /// A side whose set is empty has no width of its own and takes the peer's;
    /// its own cells are the identity at any width, so nothing is lost. Two
    /// sides that both hold items must agree, because a symbol of one width is
    /// never equal to a symbol of another and reconciling them would decode a
    /// difference that is an artifact of the encoding.
    fn adopt_width(&mut self, peer_width: u16) -> Result<()> {
        let peer_width = usize::from(peer_width);
        if peer_width > caps::MAX_SYMBOL_BYTES {
            return Err(ReconcileError::SymbolTooWide {
                size: peer_width,
                max: caps::MAX_SYMBOL_BYTES,
            });
        }
        if self.width == peer_width || peer_width == 0 {
            return Ok(());
        }
        if self.width != 0 {
            return Err(ReconcileError::SymbolWidthMismatch {
                found: peer_width,
                expected: self.width,
            });
        }

        self.width = peer_width;
        self.role = match &self.role {
            Role::Encoder { produced, .. } => Role::Encoder {
                encoder: Encoder::new(peer_width),
                produced: *produced,
            },
            Role::Decoder { received, .. } => Role::Decoder {
                decoder: Box::new(Decoder::new(peer_width)),
                received: *received,
            },
        };
        Ok(())
    }
}

impl Engine for RibltEngine {
    type Message = RibltMessage;

    fn next_outbound(&mut self) -> Result<Option<RibltMessage>> {
        Ok(self.pending.take())
    }

    fn ingest(&mut self, message: RibltMessage) -> Result<Progress> {
        match (&self.role, message) {
            (Role::Encoder { .. }, RibltMessage::Request { symbols, width }) => {
                self.serve(symbols, width)
            }
            (Role::Decoder { .. }, RibltMessage::Symbols { width, symbols }) => {
                self.peel(width, symbols)
            }
            _ => Err(ReconcileError::UnexpectedMessage),
        }
    }

    fn diff(&self) -> &Diff {
        &self.diff
    }
}

/// The batch to ask for next: double what has arrived, within the frame cap.
fn next_batch(received: usize) -> usize {
    received.clamp(caps::INITIAL_SYMBOL_BATCH, caps::MAX_SYMBOL_BATCH)
}

/// The one symbol width a source reconciles at.
///
/// # Precondition this cannot check cheaply
///
/// Identities must be distinct. The symbol *is* the identity, and the group
/// operation is its own inverse, so a set holding the same identity twice folds
/// it to nothing: the item is invisible to reconciliation and both peers agree
/// on a difference neither of them has.
///
/// [`MemorySource`](crate::reconcile::MemorySource) rejects duplicate *sort
/// keys*, which are `height || id`, so it would admit one identity at two
/// heights. The headstore-backed source cannot produce that — a head CID has
/// exactly one commit priority — so this is a constraint on future sources
/// rather than a live gap, and it is stated rather than enforced because
/// enforcing it means a set the size of the snapshot on every session.
fn session_width<S: ItemSource>(source: &S) -> Result<usize> {
    let mut width = 0;
    for index in 0..source.len() {
        let found = source.id(index).as_bytes().len();
        if index == 0 {
            width = found;
        } else if found != width {
            return Err(ReconcileError::SymbolWidthMismatch {
                found,
                expected: width,
            });
        }
    }
    if width > caps::MAX_SYMBOL_BYTES {
        return Err(ReconcileError::SymbolTooWide {
            size: width,
            max: caps::MAX_SYMBOL_BYTES,
        });
    }
    Ok(width)
}
