//! The frame duplex a reconciliation session runs over.
//!
//! A session is multi-round, so it needs both directions of one stream for its
//! whole life rather than a request and a correlated reply. [`ReconcileStream`]
//! is that seam: it carries whole [`codec`](super::codec) frames in both
//! directions and reports the peer's clean shutdown as an event rather than an
//! error, because a clean close is how a converged initiator tells a stateless
//! responder that the session is over.
//!
//! Keeping the seam this narrow is what lets the drive loop be tested over an
//! in-memory pipe and shipped over QUIC without a second implementation of the
//! protocol.

use async_trait::async_trait;

use super::error::Result;

/// One session's bidirectional frame stream.
#[async_trait]
pub trait ReconcileStream: std::fmt::Debug + Send {
    /// Sends one whole frame.
    async fn send_frame(&mut self, frame: &[u8]) -> Result<()>;

    /// Receives one whole frame, or `None` once the peer has closed its sending
    /// half with no partial frame outstanding.
    async fn recv_frame(&mut self) -> Result<Option<Vec<u8>>>;

    /// Closes the local sending half, signalling the peer that nothing more is
    /// coming.
    async fn finish(&mut self) -> Result<()>;
}

/// An in-memory [`ReconcileStream`] pair, so the drive loop is proven without a
/// socket.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct MemoryStream {
    outbound: Option<tokio::sync::mpsc::UnboundedSender<Vec<u8>>>,
    inbound: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
}

#[cfg(test)]
impl MemoryStream {
    /// Two ends of one pipe.
    pub(crate) fn pair() -> (Self, Self) {
        let (left_tx, left_rx) = tokio::sync::mpsc::unbounded_channel();
        let (right_tx, right_rx) = tokio::sync::mpsc::unbounded_channel();
        (
            Self {
                outbound: Some(left_tx),
                inbound: right_rx,
            },
            Self {
                outbound: Some(right_tx),
                inbound: left_rx,
            },
        )
    }
}

#[cfg(test)]
#[async_trait]
impl ReconcileStream for MemoryStream {
    async fn send_frame(&mut self, frame: &[u8]) -> Result<()> {
        let sender = self
            .outbound
            .as_ref()
            .ok_or_else(|| super::error::ReconcileError::Transport("stream finished".into()))?;
        sender
            .send(frame.to_vec())
            .map_err(|_| super::error::ReconcileError::Transport("peer hung up".into()))
    }

    async fn recv_frame(&mut self) -> Result<Option<Vec<u8>>> {
        Ok(self.inbound.recv().await)
    }

    async fn finish(&mut self) -> Result<()> {
        self.outbound = None;
        Ok(())
    }
}

#[async_trait]
impl<S: ReconcileStream + ?Sized> ReconcileStream for Box<S> {
    async fn send_frame(&mut self, frame: &[u8]) -> Result<()> {
        (**self).send_frame(frame).await
    }

    async fn recv_frame(&mut self) -> Result<Option<Vec<u8>>> {
        (**self).recv_frame().await
    }

    async fn finish(&mut self) -> Result<()> {
        (**self).finish().await
    }
}
