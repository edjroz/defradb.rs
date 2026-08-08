//! A reconciliation session's frame duplex over one QUIC bi-stream.
//!
//! Framing is the module's existing convention — a `u32` big-endian length
//! followed by that many bytes — but the frames here are already-encoded
//! reconciliation envelopes rather than CBOR values, because the reconciliation
//! codec owns its own encoding and versioning.
//!
//! The one thing this type adds over [`protocols::read_message_bytes`] is a
//! clean end of stream: a session's normal termination is the peer finishing its
//! sending half, which must be reported as "no more frames" rather than as a
//! read error, or every successful session would end in a failure.

use async_trait::async_trait;
use iroh::endpoint::{ReadExactError, RecvStream, SendStream};

use crate::reconcile::codec::MAX_FRAME_BYTES;
use crate::reconcile::error::{ReconcileError, Result};
use crate::reconcile::stream::ReconcileStream;

/// Both halves of the bi-stream carrying one reconciliation session.
#[derive(Debug)]
pub struct IrohReconcileStream {
    send: SendStream,
    recv: RecvStream,
}

impl IrohReconcileStream {
    /// Wraps an accepted or opened bi-stream.
    pub fn new(send: SendStream, recv: RecvStream) -> Self {
        Self { send, recv }
    }
}

fn transport_error(context: &str, error: impl std::fmt::Display) -> ReconcileError {
    ReconcileError::Transport(format!("{context}: {error}"))
}

#[async_trait]
impl ReconcileStream for IrohReconcileStream {
    async fn send_frame(&mut self, frame: &[u8]) -> Result<()> {
        let len = u32::try_from(frame.len()).map_err(|_| ReconcileError::FrameTooLarge {
            size: frame.len(),
            max: MAX_FRAME_BYTES,
        })?;
        self.send
            .write_all(&len.to_be_bytes())
            .await
            .map_err(|error| transport_error("failed to write frame length", error))?;
        self.send
            .write_all(frame)
            .await
            .map_err(|error| transport_error("failed to write frame", error))
    }

    async fn recv_frame(&mut self) -> Result<Option<Vec<u8>>> {
        let mut header = [0u8; 4];
        match self.recv.read_exact(&mut header).await {
            Ok(()) => {}
            Err(ReadExactError::FinishedEarly(0)) => return Ok(None),
            Err(error) => return Err(transport_error("failed to read frame length", error)),
        }

        let len = u32::from_be_bytes(header) as usize;
        if len > MAX_FRAME_BYTES {
            return Err(ReconcileError::FrameTooLarge {
                size: len,
                max: MAX_FRAME_BYTES,
            });
        }

        let mut frame = vec![0u8; len];
        self.recv
            .read_exact(&mut frame)
            .await
            .map_err(|error| transport_error("failed to read frame", error))?;
        Ok(Some(frame))
    }

    async fn finish(&mut self) -> Result<()> {
        self.send
            .finish()
            .map_err(|error| transport_error("failed to finish stream", error))
    }
}
