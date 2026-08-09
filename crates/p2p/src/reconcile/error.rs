//! Errors raised by the reconciliation protocol layer.

use thiserror::Error;

/// Result alias for reconciliation operations.
pub type Result<T> = std::result::Result<T, ReconcileError>;

/// Every way a reconciliation session or its codec can fail.
///
/// All variants are recoverable: a peer that sends malformed, truncated, or
/// unsupported input ends its own session with an error and never panics the
/// local node.
#[derive(Debug, Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum ReconcileError {
    /// Two items shared a sort key, violating the duplicate-free set semantics
    /// the fingerprint relies on.
    #[error("duplicate item sort key in reconciliation source")]
    DuplicateItem,

    /// An incoming range's upper bound preceded its lower bound, so it cannot
    /// map to a valid index window.
    #[error("reconcile range upper bound precedes its lower bound")]
    MalformedBound,

    /// The session hit its round cap without converging, typically because the
    /// peer returned fingerprints that never agree.
    #[error("reconciliation exceeded the maximum of {max} rounds")]
    RoundCapExceeded {
        /// The cap that was exceeded.
        max: usize,
    },

    /// A message arrived after the session had already converged.
    #[error("reconciliation session already converged")]
    SessionClosed,

    /// The frame declared a protocol version this build does not implement.
    #[error("unsupported reconciliation protocol version {found}, expected {expected}")]
    UnsupportedVersion {
        /// The version carried by the frame.
        found: u16,
        /// The version this build speaks.
        expected: u16,
    },

    /// The frame declared a message kind this build does not know.
    #[error("unknown reconciliation message kind {0}")]
    UnknownMessageKind(u8),

    /// The frame carried a known kind, but not the one the caller asked to decode.
    #[error("reconciliation message kind mismatch: frame is {found}, expected {expected}")]
    MessageKindMismatch {
        /// The kind the frame declared.
        found: u8,
        /// The kind the caller tried to decode.
        expected: u8,
    },

    /// A peer's ID-list range carried more identities than the cap allows.
    #[error("reconcile ID list of {size} entries exceeds the cap of {max}")]
    IdListTooLarge {
        /// Number of identities the peer sent.
        size: usize,
        /// The cap it exceeded.
        max: usize,
    },

    /// A frame exceeded the transport frame cap.
    #[error("reconciliation frame of {size} bytes exceeds the {max}-byte cap")]
    FrameTooLarge {
        /// Size of the offending frame.
        size: usize,
        /// The cap it exceeded.
        max: usize,
    },

    /// CBOR encoding or decoding failed (truncated, garbage, or type-mismatched
    /// input).
    #[error("reconciliation codec error: {0}")]
    Codec(String),

    /// A peer's coded symbol was not the width this session reconciles at.
    /// XOR is only a group operation over one fixed width, so a mismatch is
    /// refused rather than reconciled against a prefix of itself.
    #[error("reconcile symbol width {found} does not match this session's {expected}")]
    SymbolWidthMismatch {
        /// Width the peer's symbol carried.
        found: usize,
        /// Width this session reconciles at.
        expected: usize,
    },

    /// A peer's item identity was wider than the reconcilable cap.
    #[error("reconcile symbol of {size} bytes exceeds the {max}-byte cap")]
    SymbolTooWide {
        /// Width the peer declared.
        size: usize,
        /// The cap it exceeded.
        max: usize,
    },

    /// A session exchanged its whole coded-symbol budget without the decoder
    /// converging, which is how a stream that never peels is stopped.
    #[error("reconciliation exceeded the maximum of {max} coded symbols")]
    SymbolCapExceeded {
        /// The cap that was exceeded.
        max: usize,
    },

    /// A peer sent a message the local role has no answer to: a request to a
    /// decoder, a symbol batch to an encoder, or a batch with no cells in it.
    #[error("unexpected reconciliation message for this session's role")]
    UnexpectedMessage,

    /// The stream carrying the session failed, or the peer closed it before the
    /// session finished. A session that loses its transport ends here rather
    /// than waiting on a peer that will never answer.
    #[error("reconciliation transport error: {0}")]
    Transport(String),
}
