//! The side that solves for the difference: subtract, then peel.
//!
//! All the working state of a session lives here — the encoder is oblivious, so
//! the decoder alone decides when enough has arrived. Each arriving cell is
//! first *subtracted*: the decoder folds its own set into it, which annihilates
//! every symbol both peers hold before any decoding happens. That is where the
//! `O(d)` comes from — the shared majority cancels algebraically, at zero
//! communication cost, however large it is.
//!
//! What remains is a linear system over the difference alone, solved by
//! peeling: a cell holding exactly one symbol (count `±1`, checksum
//! confirming) yields that symbol, which is then XORed out of every other cell
//! it maps into, typically making more cells pure and cascading. When the
//! cascade stalls with cells still unaccounted for, that is not failure — it
//! means the prefix received so far is too short, and more symbols resume it.
//!
//! # Deliberate deviation from the reference
//!
//! The reference panics if a cell it queued as decodable is later found with a
//! count outside `-1..=1`, on the argument that peeling only ever removes
//! symbols. The argument holds for a well-formed stream, but the cells here
//! come off a network, so such a cell is skipped instead. The session then
//! never reaches a decoded state and ends on its symbol cap — an error, which
//! is what a hostile peer should get, rather than a panicking node.

use super::mapping::RandomMapping;
use super::symbol::{CodedSymbol, HashedSymbol, ADD, REMOVE};
use super::window::CodingWindow;
use crate::reconcile::error::{ReconcileError, Result};

/// Recovers the symmetric difference between the local set and a peer's.
pub struct Decoder {
    width: usize,
    cells: Vec<CodedSymbol>,
    window: CodingWindow,
    remote: CodingWindow,
    local: CodingWindow,
    decodable: Vec<usize>,
    decoded: usize,
}

impl Decoder {
    /// A decoder over an empty local set of `width`-byte symbols.
    pub fn new(width: usize) -> Self {
        Self {
            width,
            cells: Vec::new(),
            window: CodingWindow::new(),
            remote: CodingWindow::new(),
            local: CodingWindow::new(),
            decodable: Vec::new(),
            decoded: 0,
        }
    }

    /// Adds one local source symbol. All must be added before the first
    /// [`Self::add_coded_symbol`].
    pub fn add(&mut self, symbol: Vec<u8>) {
        self.window.add(HashedSymbol::new(symbol));
    }

    /// Takes the next cell of the peer's stream, subtracting the local set and
    /// everything already peeled out of it.
    pub fn add_coded_symbol(&mut self, cell: CodedSymbol) -> Result<()> {
        if cell.sum().len() != self.width {
            return Err(ReconcileError::SymbolWidthMismatch {
                found: cell.sum().len(),
                expected: self.width,
            });
        }

        let mut residual = cell;
        self.window.apply(&mut residual, REMOVE);
        self.remote.apply(&mut residual, REMOVE);
        self.local.apply(&mut residual, ADD);

        if residual.is_pure() || residual.is_empty() {
            self.decodable.push(self.cells.len());
        }
        self.cells.push(residual);
        Ok(())
    }

    /// Peels every cell that has become pure, cascading until it stalls.
    pub fn try_decode(&mut self) {
        let mut next = 0;
        while next < self.decodable.len() {
            let index = self.decodable[next];
            next += 1;

            let count = self.cells[index].count();
            if count == 0 {
                self.decoded += 1;
                continue;
            }
            if count != 1 && count != -1 {
                continue;
            }

            let symbol = HashedSymbol::new(self.cells[index].sum().to_vec());
            let direction = if count == 1 { REMOVE } else { ADD };
            let mapping = self.peel(&symbol, direction);
            if count == 1 {
                self.remote.add_with_mapping(symbol, mapping);
            } else {
                self.local.add_with_mapping(symbol, mapping);
            }
            self.decoded += 1;
        }
        self.decodable.clear();
    }

    /// Whether every cell received so far is accounted for, which is the
    /// session's signal to stop reading.
    pub fn is_decoded(&self) -> bool {
        !self.cells.is_empty() && self.decoded == self.cells.len()
    }

    /// Symbols the peer holds and the local set lacks.
    pub fn remote(&self) -> impl Iterator<Item = &[u8]> {
        self.remote.raw_symbols()
    }

    /// Symbols the local set holds and the peer lacks.
    pub fn local(&self) -> impl Iterator<Item = &[u8]> {
        self.local.raw_symbols()
    }

    /// XORs a recovered symbol out of every cell it maps into, queueing any
    /// that became pure, and returns its mapping positioned past those cells so
    /// later arrivals are corrected too.
    fn peel(&mut self, symbol: &HashedSymbol, direction: i64) -> RandomMapping {
        let mut mapping = RandomMapping::new(symbol.hash());
        while mapping.index() < self.cells.len() as u64 {
            let index = mapping.index() as usize;
            self.cells[index].apply(symbol, direction);
            if self.cells[index].is_pure() {
                self.decodable.push(index);
            }
            mapping.next_index();
        }
        mapping
    }
}
