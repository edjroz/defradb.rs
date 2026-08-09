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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::codec;

    /// The opening frame is the difference between what a session's own
    /// accounting reports and what the transport counters see, so a benchmark
    /// that reconciles the two derives its size by division and has no way to
    /// check the answer. This is that check: the phase 3 campaign derived 43
    /// bytes per session for the collection `BenchDoc`, and here is the same
    /// number produced by encoding the frame directly.
    #[test]
    fn a_named_collection_costs_what_the_campaign_derived() {
        let encoded = codec::encode(&SessionOpen::new("BenchDoc")).expect("encode");
        assert_eq!(encoded.len(), 43);
    }

    /// The frame is a fixed envelope plus the collection name, so the derived
    /// per-session constant is only constant for a given collection.
    #[test]
    fn the_frame_grows_only_with_the_collection_name() {
        let short = codec::encode(&SessionOpen::new("A")).expect("encode");
        let longer = codec::encode(&SessionOpen::new("AAAAAAAA")).expect("encode");
        assert_eq!(longer.len() - short.len(), 7);
    }
}
