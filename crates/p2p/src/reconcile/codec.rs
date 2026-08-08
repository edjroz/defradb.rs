//! The versioned CBOR envelope every reconciliation message travels in.
//!
//! A frame is a CBOR map of `{version, kind, body}` where `body` is the opaque
//! CBOR encoding of the message itself. Decoding is two-stage on purpose: the
//! version and kind are checked *before* the body is interpreted, so a peer
//! speaking a future protocol version gets a clean
//! [`ReconcileError::UnsupportedVersion`] rather than a confusing type error —
//! and no input, however malformed, can do worse than return an error.
//!
//! No wire parity with the Go reference is claimed or attempted; the Go engine
//! deliberately left its wire encoding undefined. Cross-implementation framing
//! belongs to the deferred interop phase.

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use super::engine::rbsr::RbsrMessage;
use super::error::{ReconcileError, Result};

/// The reconciliation protocol version this build speaks.
pub const PROTOCOL_VERSION: u16 = 1;

/// Largest frame this codec will encode or decode, mirroring the transport's
/// frame cap.
pub const MAX_FRAME_BYTES: usize = 16 << 20;

/// Which protocol engine a frame's body belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum MessageKind {
    /// A range tiling from the RBSR engine.
    RbsrRanges = 1,
    /// A coded-symbol batch from the RIBLT engine. Reserved: nothing encodes
    /// this yet, but the kind is allocated so an RBSR-only peer rejects it as a
    /// mismatch rather than as garbage.
    RibltSymbols = 2,
}

impl MessageKind {
    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            tag if tag == Self::RbsrRanges as u8 => Ok(Self::RbsrRanges),
            tag if tag == Self::RibltSymbols as u8 => Ok(Self::RibltSymbols),
            other => Err(ReconcileError::UnknownMessageKind(other)),
        }
    }
}

/// A message type that can travel in a reconciliation frame.
pub trait WireMessage: Serialize + DeserializeOwned {
    /// The kind tag frames of this type carry.
    const KIND: MessageKind;
}

impl WireMessage for RbsrMessage {
    const KIND: MessageKind = MessageKind::RbsrRanges;
}

/// The version and kind a frame declares, read without interpreting its body.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FrameHeader {
    /// Protocol version the sender claims to speak.
    pub version: u16,
    /// Kind tag of the body.
    pub kind: u8,
}

#[derive(Serialize, Deserialize)]
struct Frame {
    version: u16,
    kind: u8,
    body: serde_bytes::ByteBuf,
}

/// Encodes a message into a versioned frame.
pub fn encode<M: WireMessage>(message: &M) -> Result<Vec<u8>> {
    encode_frame(PROTOCOL_VERSION, M::KIND as u8, encode_body(message)?)
}

/// Reads a frame's version and kind without interpreting its body.
pub fn peek_header(bytes: &[u8]) -> Result<FrameHeader> {
    let frame = decode_frame(bytes)?;
    Ok(FrameHeader {
        version: frame.version,
        kind: frame.kind,
    })
}

/// Decodes a frame into the requested message type.
///
/// Fails with a distinct error for an oversized frame, malformed CBOR, an
/// unsupported protocol version, an unknown kind tag, and a known-but-wrong kind
/// tag — in that order, so the caller can tell a future peer from a broken one.
pub fn decode<M: WireMessage>(bytes: &[u8]) -> Result<M> {
    let frame = decode_frame(bytes)?;

    if frame.version != PROTOCOL_VERSION {
        return Err(ReconcileError::UnsupportedVersion {
            found: frame.version,
            expected: PROTOCOL_VERSION,
        });
    }

    let kind = MessageKind::from_tag(frame.kind)?;
    if kind != M::KIND {
        return Err(ReconcileError::MessageKindMismatch {
            found: frame.kind,
            expected: M::KIND as u8,
        });
    }

    ciborium::from_reader(frame.body.as_ref()).map_err(codec_error)
}

fn encode_body<M: WireMessage>(message: &M) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    ciborium::into_writer(message, &mut body).map_err(codec_error)?;
    Ok(body)
}

fn encode_frame(version: u16, kind: u8, body: Vec<u8>) -> Result<Vec<u8>> {
    let frame = Frame {
        version,
        kind,
        body: serde_bytes::ByteBuf::from(body),
    };
    let mut bytes = Vec::new();
    ciborium::into_writer(&frame, &mut bytes).map_err(codec_error)?;
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(ReconcileError::FrameTooLarge {
            size: bytes.len(),
            max: MAX_FRAME_BYTES,
        });
    }
    Ok(bytes)
}

fn decode_frame(bytes: &[u8]) -> Result<Frame> {
    if bytes.len() > MAX_FRAME_BYTES {
        return Err(ReconcileError::FrameTooLarge {
            size: bytes.len(),
            max: MAX_FRAME_BYTES,
        });
    }
    ciborium::from_reader(bytes).map_err(codec_error)
}

fn codec_error<E: std::fmt::Display>(error: E) -> ReconcileError {
    ReconcileError::Codec(error.to_string())
}

#[cfg(test)]
fn encode_with_version<M: WireMessage>(message: &M, version: u16) -> Result<Vec<u8>> {
    encode_frame(version, M::KIND as u8, encode_body(message)?)
}

#[cfg(test)]
fn encode_raw(version: u16, kind: u8, body: Vec<u8>) -> Result<Vec<u8>> {
    encode_frame(version, kind, body)
}

#[cfg(test)]
#[path = "codec_tests.rs"]
mod codec_tests;
