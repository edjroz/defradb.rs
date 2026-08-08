//! Drives one [`Engine`] through one reconciliation session.
//!
//! The session owns the policy that is the same whichever engine is underneath:
//! bound the number of rounds, stop emitting once the local side has converged,
//! and refuse input afterwards. Which *role* a session plays — RBSR initiator or
//! responder, RIBLT encoder or decoder — is a property of the engine it was
//! built around, not of the session, so there is nothing role-shaped here.
//!
//! The drive loop a transport runs over it is:
//!
//! ```text
//! while let Some(msg) = session.next_outbound()? { send(msg) }
//! while !session.is_converged() {
//!     session.ingest(recv()?)?;
//!     while let Some(msg) = session.next_outbound()? { send(msg) }
//! }
//! ```

use super::engine::{Diff, Engine, Progress};
use super::error::{ReconcileError, Result};

/// Bound on the rounds a session will run before giving up.
///
/// A peer whose answers never converge — through malice or a bug — terminates
/// the session with [`ReconcileError::RoundCapExceeded`] instead of looping
/// forever. The value is the Go reference's `MaxRounds`; it lives here rather
/// than beside the RBSR caps because it is session policy that applies to any
/// engine.
pub const MAX_ROUNDS: usize = 32;

/// One reconciliation session over one engine.
///
/// Single-use: build it, drive it to convergence, take its [`Diff`].
pub struct Session<E: Engine> {
    engine: E,
    rounds: usize,
    converged: bool,
}

impl<E: Engine> Session<E> {
    /// Wraps an engine in a fresh session.
    pub fn new(engine: E) -> Self {
        Self {
            engine,
            rounds: 0,
            converged: false,
        }
    }

    /// The next message to send, or `None` when this side has nothing to say.
    /// Always `None` once the session has converged: the terminal message is a
    /// local conclusion, not something the peer needs.
    pub fn next_outbound(&mut self) -> Result<Option<E::Message>> {
        if self.converged {
            return Ok(None);
        }
        self.engine.next_outbound()
    }

    /// Consumes one message from the peer.
    pub fn ingest(&mut self, message: E::Message) -> Result<Progress> {
        if self.converged {
            return Err(ReconcileError::SessionClosed);
        }
        if self.rounds >= MAX_ROUNDS {
            return Err(ReconcileError::RoundCapExceeded { max: MAX_ROUNDS });
        }
        self.rounds += 1;

        let progress = self.engine.ingest(message)?;
        if progress == Progress::Converged {
            self.converged = true;
        }
        Ok(progress)
    }

    /// Number of peer messages consumed so far.
    pub fn rounds(&self) -> usize {
        self.rounds
    }

    /// Whether the local side has learned its full difference.
    pub fn is_converged(&self) -> bool {
        self.converged
    }

    /// The difference learned so far; complete once converged.
    pub fn diff(&self) -> &Diff {
        self.engine.diff()
    }

    /// Consumes the session, yielding the difference it learned.
    pub fn into_diff(self) -> Diff {
        self.engine.diff().clone()
    }
}

#[cfg(test)]
#[path = "session_tests.rs"]
mod session_tests;
