//! The deterministic, infinite index sequence one source symbol participates in.
//!
//! This is what makes the code rateless. Every symbol is in coded symbol 0, and
//! thereafter participates in index `i` with probability `1/(1+i/2)`, so any
//! prefix of the coded-symbol stream is a well-proportioned sketch for the
//! difference size that prefix can carry — no size estimate, no resizing, no
//! estimator protocol in front.
//!
//! The sequence is a transcription of the reference implementation's
//! `randomMapping`, including its approximated inverse-CDF step
//! `diff = (1.5 + i) · ((1-u)^(-1/2) - 1)` with `(1-u)^(-1/2)` replaced by
//! `2^32 / sqrt(r)` for a uniform `u64` draw `r`. The arithmetic is IEEE-754
//! double precision on both sides, and square root, multiplication and division
//! are all correctly rounded, so the two implementations agree bit for bit —
//! `mapping_tests.rs` pins that against vectors the reference produced.

/// The reference's multiplier. Odd, so the update is a permutation of `u64`.
const MULTIPLIER: u64 = 0xda94_2042_e4dd_58b5;

/// Which coded symbols one source symbol is mapped into.
pub(super) struct RandomMapping {
    prng: u64,
    last_index: u64,
}

impl RandomMapping {
    /// Seeds the sequence from a source symbol's hash.
    pub(super) fn new(seed: u64) -> Self {
        Self {
            prng: seed,
            last_index: 0,
        }
    }

    /// The index most recently produced; `0` before the first advance, which is
    /// why coded symbol 0 contains every symbol.
    pub(super) fn index(&self) -> u64 {
        self.last_index
    }

    /// Advances to the next index of the sequence.
    ///
    /// Two departures from the reference, neither of which changes an index it
    /// can produce, and both of which remove a way to be silently wrong:
    ///
    /// - The addition saturates where the reference lets its `int` wrap. The
    ///   index is compared against a coded-symbol count capped far below
    ///   `u64::MAX`, so this is unreachable in a session; it keeps the sequence
    ///   monotonic for anything a fuzzer invents.
    /// - The step is at least one. A `u64` draw within about `2^11` of the top
    ///   rounds to `2^64` in double precision, making the computed step exactly
    ///   zero — roughly a `2^-53` event per draw. The reference would then map a
    ///   symbol to the same coded symbol twice in a row, and because the group
    ///   operation is its own inverse, the second application would silently
    ///   cancel the first. Enforcing a strictly increasing sequence costs
    ///   nothing and cannot alter any step the reference computes as non-zero.
    pub(super) fn next_index(&mut self) -> u64 {
        self.prng = self.prng.wrapping_mul(MULTIPLIER);
        let jump = (self.last_index as f64 + 1.5)
            * ((1u64 << 32) as f64 / (self.prng as f64 + 1.0).sqrt() - 1.0);
        self.last_index = self.last_index.saturating_add((jump.ceil() as u64).max(1));
        self.last_index
    }
}
