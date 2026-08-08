//! Runs a [`Session`] to completion over a [`ReconcileStream`].
//!
//! The two roles terminate on different signals, which is why they are two
//! functions rather than one with a flag. An initiator knows it is finished when
//! its own engine converges, and a peer that vanishes before that has failed it.
//! A responder is stateless and never converges on its own; the initiator
//! closing its sending half *is* the responder's terminal signal, so the same
//! event is success for one role and failure for the other.
//!
//! Every exit is an exit: a broken stream, an undecodable frame, a peer that
//! never converges, and a peer that closes early all return an error rather than
//! leaving a task parked on a read that will never complete.

use super::codec::{self, WireMessage};
use super::engine::{Diff, Engine};
use super::error::{ReconcileError, Result};
use super::session::Session;
use super::stream::ReconcileStream;

/// Drives the initiating side and returns the difference it learned.
pub async fn drive_initiator<E, S>(mut session: Session<E>, stream: &mut S) -> Result<Diff>
where
    E: Engine,
    E::Message: WireMessage,
    S: ReconcileStream + ?Sized,
{
    flush(&mut session, stream).await?;

    while !session.is_converged() {
        let Some(frame) = stream.recv_frame().await? else {
            return Err(ReconcileError::Transport(
                "peer closed the session before it converged".into(),
            ));
        };
        session.ingest(codec::decode::<E::Message>(&frame)?)?;
        flush(&mut session, stream).await?;
    }

    stream.finish().await?;
    Ok(session.diff().clone())
}

/// Drives the responding side until the initiator closes the stream.
///
/// Returns the number of rounds served, which is what a caller measures; the
/// responder learns no difference.
pub async fn drive_responder<E, S>(mut session: Session<E>, stream: &mut S) -> Result<usize>
where
    E: Engine,
    E::Message: WireMessage,
    S: ReconcileStream + ?Sized,
{
    while let Some(frame) = stream.recv_frame().await? {
        session.ingest(codec::decode::<E::Message>(&frame)?)?;
        flush(&mut session, stream).await?;
    }

    stream.finish().await?;
    Ok(session.rounds())
}

/// Sends everything the session currently has to say.
async fn flush<E, S>(session: &mut Session<E>, stream: &mut S) -> Result<()>
where
    E: Engine,
    E::Message: WireMessage,
    S: ReconcileStream + ?Sized,
{
    while let Some(message) = session.next_outbound()? {
        stream.send_frame(&codec::encode(&message)?).await?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "drive_tests.rs"]
mod drive_tests;
