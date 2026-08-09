//! Guardrails on an open-ended coded-symbol stream.
//!
//! RBSR's caps bound an interactive protocol; these bound a stream, and the
//! trust model inverts with it. Against RBSR a hostile initiator can only burn
//! its own capped round budget at a stateless responder. Here a hostile decoder
//! attacks the encoder's *bandwidth* by never saying stop, and a hostile encoder
//! attacks the decoder's *memory and CPU* with cells that never peel — so both
//! sides are bounded, by the same [`MAX_CODED_SYMBOLS`] reached from either end.

/// Coded symbols the decoder asks for in its opening request.
///
/// Small on purpose: the difference a live node reconciles is usually a handful
/// of heads, and a first batch that already covers `d ≈ 5` makes the common
/// session a single round trip. Batches then double, so the total a session
/// pulls stays within a factor of two of what the difference needed.
pub const INITIAL_SYMBOL_BATCH: usize = 8;

/// Most coded symbols one frame may carry, in either direction.
///
/// At the CID symbol width this is a few hundred kilobytes, well inside the
/// codec's frame cap, and it bounds what one message can make the peer allocate.
pub const MAX_SYMBOL_BATCH: usize = 16_384;

/// Most coded symbols one session may exchange.
///
/// Sized to carry a difference past a hundred thousand items at the reference's
/// measured overhead, which is far beyond any difference a live node should
/// reconcile — past that, shipping the set outright is cheaper anyway. Reaching
/// it ends the session with [`SymbolCapExceeded`], which is what a peer whose
/// cells never peel deserves.
///
/// [`SymbolCapExceeded`]: crate::reconcile::ReconcileError::SymbolCapExceeded
pub const MAX_CODED_SYMBOLS: usize = 262_144;

/// Widest item identity a session will reconcile.
///
/// A CIDv1 `dag-cbor/sha2-256` identity is 36 bytes. The cap exists because the
/// symbol width is declared by the peer when the local set is empty, and an
/// undeclared bound there would let one frame size every allocation that follows.
pub const MAX_SYMBOL_BYTES: usize = 64;
