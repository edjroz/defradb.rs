//! The frame that names the set two peers are about to reconcile.
//!
//! An engine reconciles whatever its [`ItemSource`](super::source::ItemSource)
//! contains and has no opinion about what that is. A responder, though, must
//! build its own source before it can answer anything, and only the initiator
//! knows which set it wants. This frame carries that one fact, once, ahead of
//! the engine's first message.
//!
//! It travels in the same versioned envelope as every other reconciliation
//! frame, so a peer speaking a future protocol version is rejected at the
//! envelope rather than after a partial handshake.

use serde::{Deserialize, Serialize};

use super::codec::{MessageKind, WireMessage};

/// The initiator's opening frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOpen {
    collection: String,
}

impl SessionOpen {
    /// Names the collection whose document heads are to be reconciled.
    pub fn new(collection: impl Into<String>) -> Self {
        Self {
            collection: collection.into(),
        }
    }

    /// The collection being reconciled.
    pub fn collection(&self) -> &str {
        &self.collection
    }
}

impl WireMessage for SessionOpen {
    const KIND: MessageKind = MessageKind::SessionOpen;
}
