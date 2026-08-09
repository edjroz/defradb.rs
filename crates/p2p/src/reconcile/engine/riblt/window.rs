//! The set of source symbols a stream position is being folded from.
//!
//! Both roles need the same thing: a collection of symbols, each with its own
//! index mapping, and a way to fold every symbol due at the current stream
//! position into one cell in one pass. A min-heap keyed by each symbol's next
//! index gives that in `O(log n)` per fold rather than a scan of the whole set
//! per coded symbol — the same structure the reference uses, expressed with
//! [`BinaryHeap`]'s `peek_mut`, whose re-sift on drop is exactly the reference's
//! `fixHead`.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::mapping::RandomMapping;
use super::symbol::{CodedSymbol, HashedSymbol};

/// Source symbols and their positions in the coded-symbol stream.
pub(super) struct CodingWindow {
    symbols: Vec<HashedSymbol>,
    mappings: Vec<RandomMapping>,
    queue: BinaryHeap<Reverse<(u64, usize)>>,
    next_index: u64,
}

impl CodingWindow {
    /// An empty window positioned at the start of the stream.
    pub(super) fn new() -> Self {
        Self {
            symbols: Vec::new(),
            mappings: Vec::new(),
            queue: BinaryHeap::new(),
            next_index: 0,
        }
    }

    /// Adds a symbol at the start of the stream.
    pub(super) fn add(&mut self, symbol: HashedSymbol) {
        let mapping = RandomMapping::new(symbol.hash());
        self.add_with_mapping(symbol, mapping);
    }

    /// Adds a symbol whose mapping has already been advanced past the cells it
    /// was peeled out of, so it is only folded into cells that arrive later.
    pub(super) fn add_with_mapping(&mut self, symbol: HashedSymbol, mapping: RandomMapping) {
        self.queue
            .push(Reverse((mapping.index(), self.symbols.len())));
        self.symbols.push(symbol);
        self.mappings.push(mapping);
    }

    /// Folds every symbol due at the current stream position into `cell` and
    /// advances one position.
    pub(super) fn apply(&mut self, cell: &mut CodedSymbol, direction: i64) {
        while let Some(mut top) = self.queue.peek_mut() {
            let Reverse((index, source)) = *top;
            if index != self.next_index {
                break;
            }
            cell.apply(&self.symbols[source], direction);
            *top = Reverse((self.mappings[source].next_index(), source));
        }
        self.next_index += 1;
    }

    /// The raw symbols held, in the order they were added.
    pub(super) fn raw_symbols(&self) -> impl Iterator<Item = &[u8]> {
        self.symbols.iter().map(HashedSymbol::symbol)
    }
}
