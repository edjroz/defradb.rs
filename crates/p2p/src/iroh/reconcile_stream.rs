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
//!
//! Frames are metered against [`ALPN_RECON`](protocols::ALPN_RECON) like every
//! other protocol on this transport, so a benchmark can attribute what a
//! session cost on the wire independently of what the session itself reports.
//! The counted quantity is the frame body, excluding the four-byte length
//! prefix — the same convention [`protocols::write_message`] uses — so the two
//! accountings are directly comparable.

use std::sync::Arc;

use async_trait::async_trait;
use iroh::endpoint::{ReadExactError, RecvStream, SendStream};

use super::protocols::{self, Meter};
use crate::metrics::TransportCounters;
use crate::reconcile::codec::MAX_FRAME_BYTES;
use crate::reconcile::error::{ReconcileError, Result};
use crate::reconcile::stream::ReconcileStream;

/// Both halves of the bi-stream carrying one reconciliation session.
#[derive(Debug)]
pub struct IrohReconcileStream {
    send: SendStream,
    recv: RecvStream,
    counters: Option<Arc<TransportCounters>>,
}

impl IrohReconcileStream {
    /// Wraps an accepted or opened bi-stream. A `None` counter handle disables
    /// counting, which is what every production caller passes.
    pub fn new(
        send: SendStream,
        recv: RecvStream,
        counters: Option<Arc<TransportCounters>>,
    ) -> Self {
        Self {
            send,
            recv,
            counters,
        }
    }

    fn meter(&self) -> Meter<'_> {
        Meter::for_alpn(self.counters.as_ref(), protocols::ALPN_RECON)
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
            .map_err(|error| transport_error("failed to write frame", error))?;
        self.meter().record_raw_sent(frame.len());
        Ok(())
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
        self.meter().record_raw_recv(frame.len());
        Ok(Some(frame))
    }

    async fn finish(&mut self) -> Result<()> {
        self.send
            .finish()
            .map_err(|error| transport_error("failed to finish stream", error))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    /// Both ends of a session must count the same frames, on the reconciliation
    /// ALPN, in the control class.
    ///
    /// Written as a two-end agreement test rather than a single-end assertion
    /// because that is what a metering bug actually looks like: one direction
    /// silently uncounted leaves every total self-consistent and the two nodes
    /// incomparable. It is also the test that fails if either `record_raw_*`
    /// call below is removed, which is the property a benchmark depends on and
    /// which nothing else pins — the whole ranges series is attributed through
    /// these two lines.
    #[tokio::test]
    async fn session_frames_are_counted_as_control_on_both_ends() {
        let opening = vec![1u8; 84];
        let reply = vec![2u8; 444];

        let responder_endpoint = localhost_endpoint(vec![protocols::ALPN_RECON.to_vec()]).await;
        let initiator_endpoint = localhost_endpoint(Vec::new()).await;
        let addr = responder_endpoint.addr();

        let responder_counters = TransportCounters::new();
        let responder = tokio::spawn({
            let counters = Arc::clone(&responder_counters);
            let opening = opening.clone();
            let reply = reply.clone();
            async move {
                let connection = responder_endpoint
                    .accept()
                    .await
                    .expect("incoming")
                    .await
                    .expect("connection");
                let (send, recv) = connection.accept_bi().await.expect("accept_bi");
                let mut stream = IrohReconcileStream::new(send, recv, Some(counters));

                assert_eq!(stream.recv_frame().await.expect("read"), Some(opening));
                stream.send_frame(&reply).await.expect("write");
                stream.finish().await.expect("finish");
                assert_eq!(stream.recv_frame().await.expect("read end"), None);
                connection
            }
        });

        let initiator_counters = TransportCounters::new();
        let connection = initiator_endpoint
            .connect(addr, protocols::ALPN_RECON)
            .await
            .expect("connect");
        let (send, recv) = connection.open_bi().await.expect("open_bi");
        let mut stream =
            IrohReconcileStream::new(send, recv, Some(Arc::clone(&initiator_counters)));

        stream.send_frame(&opening).await.expect("write");
        assert_eq!(stream.recv_frame().await.expect("read"), Some(reply));
        stream.finish().await.expect("finish");

        responder.await.expect("responder task");

        let initiated = initiator_counters.snapshot();
        let served = responder_counters.snapshot();
        assert_eq!(initiated.control_bytes_sent(), 84);
        assert_eq!(initiated.control_bytes_recv(), 444);
        assert_eq!(served.control_bytes_recv(), initiated.control_bytes_sent());
        assert_eq!(served.control_bytes_sent(), initiated.control_bytes_recv());

        // Frame bodies only: the four-byte length prefix is excluded, on the
        // same terms as every other protocol on this transport.
        let alpn = String::from_utf8_lossy(protocols::ALPN_RECON).to_string();
        assert_eq!(initiated.protocols[&alpn].msgs_sent, 1);
        assert_eq!(initiated.payload_bytes_sent(), 0);
        assert_eq!(initiated.payload_bytes_recv(), 0);
    }

    /// A stream built without counters must not allocate a protocol entry, or
    /// production nodes would carry a benchmark's bookkeeping.
    #[tokio::test]
    async fn an_unmetered_stream_counts_nothing() {
        let responder_endpoint = localhost_endpoint(vec![protocols::ALPN_RECON.to_vec()]).await;
        let initiator_endpoint = localhost_endpoint(Vec::new()).await;
        let addr = responder_endpoint.addr();

        let responder = tokio::spawn(async move {
            let connection = responder_endpoint
                .accept()
                .await
                .expect("incoming")
                .await
                .expect("connection");
            let (send, recv) = connection.accept_bi().await.expect("accept_bi");
            let mut stream = IrohReconcileStream::new(send, recv, None);
            assert!(stream.recv_frame().await.expect("read").is_some());
            connection
        });

        let connection = initiator_endpoint
            .connect(addr, protocols::ALPN_RECON)
            .await
            .expect("connect");
        let (send, recv) = connection.open_bi().await.expect("open_bi");
        let mut stream = IrohReconcileStream::new(send, recv, None);
        stream.send_frame(&[9u8; 16]).await.expect("write");
        stream.finish().await.expect("finish");

        responder.await.expect("responder task");
    }

    async fn localhost_endpoint(alpns: Vec<Vec<u8>>) -> iroh::Endpoint {
        iroh::Endpoint::builder(iroh::endpoint::presets::Minimal)
            .relay_mode(iroh::RelayMode::Disabled)
            .alpns(alpns)
            .bind_addr("127.0.0.1:0".parse::<std::net::SocketAddr>().unwrap())
            .expect("bind addr")
            .bind()
            .await
            .expect("bind endpoint")
    }
}
