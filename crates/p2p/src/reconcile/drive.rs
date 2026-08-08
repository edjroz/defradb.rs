//! Runs a [`Session`] to completion over a [`ReconcileStream`].
//!
//! The two roles terminate on different signals, which is why they are two
//! functions rather than one with a flag. An initiator knows it is finished when
//! its own engine converges, and a peer that vanishes before that has failed it.
//! A responder is stateless and never converges on its own; the initiator
//! closing its sending half *is* the responder's terminal signal, so the same
//! event is success for one role and failure for the other.
//!
//! One session reconciles in one direction only. The initiator learns what it
//! needs and what it holds that the peer lacks, and the responder learns
//! nothing and changes nothing — that is the RFC's discovery-only principle, not
//! an omission. Two peers converge on each other by running a session each way.
//!
//! Every exit is an exit: a broken stream, an undecodable frame, a peer that
//! never converges, a peer that closes early, and a peer that simply stops
//! talking all return an error rather than leaving a task parked on a read that
//! will never complete.

use std::time::Duration;

use tokio::time::{timeout_at, Instant};

use super::codec::{self, WireMessage};
use super::engine::{Diff, Engine};
use super::error::{ReconcileError, Result};
use super::session::Session;
use super::stream::ReconcileStream;

/// Longest a session will wait for any single message from its peer.
///
/// Matches the iroh transport's own request/response timeout, so a
/// reconciliation round is not more patient than any other exchange with the
/// same peer.
pub const ROUND_TIMEOUT: Duration = Duration::from_secs(30);

/// Longest a whole session may run, however chatty the peer.
///
/// [`ROUND_TIMEOUT`] alone bounds silence, not duration: a peer that answers
/// just inside the round timeout every round would hold a session open for
/// [`MAX_ROUNDS`](super::session::MAX_ROUNDS) times as long. This bounds the
/// session itself, so the memory its snapshot pins is released on a schedule a
/// peer cannot extend.
pub const SESSION_TIMEOUT: Duration = Duration::from_secs(120);

/// What one session cost, in the units a measurement campaign records.
///
/// Byte counts are of whole encoded frames, which is what actually crosses the
/// transport seam, and exclude the transport's own framing and QUIC overhead.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SessionCost {
    /// Peer messages consumed.
    pub rounds: usize,
    /// Encoded frame bytes written to the peer.
    pub bytes_sent: u64,
    /// Encoded frame bytes read from the peer.
    pub bytes_received: u64,
}

/// Drives the initiating side and returns the difference it learned.
pub async fn drive_initiator<E, S>(
    mut session: Session<E>,
    stream: &mut S,
) -> Result<(Diff, SessionCost)>
where
    E: Engine,
    E::Message: WireMessage,
    S: ReconcileStream + ?Sized,
{
    let deadline = Instant::now() + SESSION_TIMEOUT;
    let mut cost = SessionCost::default();
    flush(&mut session, stream, &mut cost).await?;

    while !session.is_converged() {
        let Some(frame) = recv_within(stream, deadline).await? else {
            return Err(ReconcileError::Transport(
                "peer closed the session before it converged".into(),
            ));
        };
        cost.bytes_received += frame.len() as u64;
        session.ingest(codec::decode::<E::Message>(&frame)?)?;
        flush(&mut session, stream, &mut cost).await?;
    }

    stream.finish().await?;
    cost.rounds = session.rounds();
    Ok((session.diff().clone(), cost))
}

/// Drives the responding side until the initiator closes the stream.
pub async fn drive_responder<E, S>(mut session: Session<E>, stream: &mut S) -> Result<SessionCost>
where
    E: Engine,
    E::Message: WireMessage,
    S: ReconcileStream + ?Sized,
{
    let deadline = Instant::now() + SESSION_TIMEOUT;
    let mut cost = SessionCost::default();

    while let Some(frame) = recv_within(stream, deadline).await? {
        cost.bytes_received += frame.len() as u64;
        session.ingest(codec::decode::<E::Message>(&frame)?)?;
        flush(&mut session, stream, &mut cost).await?;
    }

    stream.finish().await?;
    cost.rounds = session.rounds();
    Ok(cost)
}

/// Waits for one frame, bounded by both the per-round and the whole-session
/// budget, whichever expires first.
async fn recv_within<S>(stream: &mut S, deadline: Instant) -> Result<Option<Vec<u8>>>
where
    S: ReconcileStream + ?Sized,
{
    let round_deadline = (Instant::now() + ROUND_TIMEOUT).min(deadline);
    match timeout_at(round_deadline, stream.recv_frame()).await {
        Ok(frame) => frame,
        Err(_) => Err(ReconcileError::Transport(
            "peer stopped responding before the session finished".into(),
        )),
    }
}

/// Sends everything the session currently has to say.
async fn flush<E, S>(session: &mut Session<E>, stream: &mut S, cost: &mut SessionCost) -> Result<()>
where
    E: Engine,
    E::Message: WireMessage,
    S: ReconcileStream + ?Sized,
{
    while let Some(message) = session.next_outbound()? {
        let frame = codec::encode(&message)?;
        cost.bytes_sent += frame.len() as u64;
        stream.send_frame(&frame).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "drive_tests.rs"]
mod drive_tests;
