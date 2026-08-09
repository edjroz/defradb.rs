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
//! # Two invariants, because the input is a network and not a library caller
//!
//! The reference implementation is explicit that it is built for trusted input,
//! and it enforces its assumptions with a `panic`. Panicking is not available
//! to a node, so the assumptions have to be carried by invariants instead. Two
//! of them, and both are load-bearing:
//!
//! 1. **Convergence is "every residual is the zero cell", counted directly.**
//!    [`Self::unresolved`] tracks how many cells are not the identity, and it is
//!    the only thing [`Self::is_decoded`] consults. A cell is therefore never
//!    "resolved" by having been visited, only by actually being zero — so a
//!    peer cannot spend the decoder's convergence budget on cells it never
//!    explained.
//! 2. **A cell that has ever been fully explained is never peeled.** Index 0 is
//!    in every symbol's mapping, so *any* peel touches cell 0 and can leave an
//!    already-zero cell looking pure again. Without [`Self::settled`] the
//!    decoder would peel that cell, recover the same symbol into the opposite
//!    half of the difference, and — because the second peel undoes the first —
//!    do it again. That is a fabricated difference at best and a loop at worst.
//!    A cell reaches zero only when every member has been peeled out of it, so
//!    on a well-formed stream nothing can legitimately re-enter it; a cell that
//!    changes after reaching zero has been changed by the peer.
//!
//! Both hold for a well-formed stream trivially; they exist for the streams
//! that are not. A hostile peer's stream simply never converges, and the
//! session ends on its symbol cap, which is an error rather than a silent lie.
//!
//! What is deliberately *not* claimed: nothing here can tell a real remote
//! symbol from one the peer invented, because a peer that computes a checksum
//! correctly has produced a valid cell by definition. That is why reconciliation
//! is discovery-only — the recovered identities are content-addressed, so an
//! invented one simply fails to fetch.

use super::mapping::RandomMapping;
use super::symbol::{CodedSymbol, HashedSymbol, ADD, REMOVE};
use super::window::CodingWindow;
use crate::reconcile::error::{ReconcileError, Result};

/// Recovers the symmetric difference between the local set and a peer's.
pub struct Decoder {
    width: usize,
    cells: Vec<CodedSymbol>,
    /// Cells whose residual is not yet the identity. Zero means converged.
    unresolved: usize,
    /// Whether a cell has ever been fully explained — reached the identity, or
    /// been used as a peel source. Settled cells are never peeled again.
    settled: Vec<bool>,
    window: CodingWindow,
    remote: CodingWindow,
    local: CodingWindow,
    decodable: Vec<usize>,
}

impl Decoder {
    /// A decoder over an empty local set of `width`-byte symbols.
    pub fn new(width: usize) -> Self {
        Self {
            width,
            cells: Vec::new(),
            unresolved: 0,
            settled: Vec::new(),
            window: CodingWindow::new(),
            remote: CodingWindow::new(),
            local: CodingWindow::new(),
            decodable: Vec::new(),
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

        let empty = residual.is_empty();
        if !empty {
            self.unresolved += 1;
        }
        if residual.is_pure() {
            self.decodable.push(self.cells.len());
        }
        self.cells.push(residual);
        self.settled.push(empty);
        Ok(())
    }

    /// Peels every cell that has become pure, cascading until it stalls.
    ///
    /// Purity is re-checked when a cell is taken off the queue rather than
    /// trusted from when it was put on, because a cell can be changed by another
    /// cell's peel in between.
    pub fn try_decode(&mut self) {
        let mut next = 0;
        while next < self.decodable.len() {
            let index = self.decodable[next];
            next += 1;

            if self.settled[index] || !self.cells[index].is_pure() {
                continue;
            }
            self.settled[index] = true;

            let count = self.cells[index].count();
            let symbol = HashedSymbol::new(self.cells[index].sum().to_vec());
            let direction = if count == 1 { REMOVE } else { ADD };
            let mapping = self.peel(&symbol, direction);
            if count == 1 {
                self.remote.add_with_mapping(symbol, mapping);
            } else {
                self.local.add_with_mapping(symbol, mapping);
            }
        }
        self.decodable.clear();
    }

    /// Whether every cell received so far is fully explained by the difference
    /// recovered, which is the session's signal to stop reading.
    pub fn is_decoded(&self) -> bool {
        !self.cells.is_empty() && self.unresolved == 0
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
            let was_empty = self.cells[index].is_empty();
            self.cells[index].apply(symbol, direction);

            match (was_empty, self.cells[index].is_empty()) {
                (false, true) => {
                    self.unresolved -= 1;
                    self.settled[index] = true;
                }
                (true, false) => self.unresolved += 1,
                _ => {}
            }
            if self.cells[index].is_pure() {
                self.decodable.push(index);
            }
            mapping.next_index();
        }
        mapping
    }
}
