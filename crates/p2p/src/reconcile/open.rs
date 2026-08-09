//! The frame that names the set two peers are about to reconcile.
//!
//! An engine reconciles whatever its [`ItemSource`](super::source::ItemSource)
//! contains and has no opinion about what that is. A responder, though, must
//! build its own source before it can answer anything, and only the initiator
//! knows which set it wants. This frame carries that one fact, once, ahead of
//! the engine's first message.
//!
//! It carries the one other fact a responder cannot infer: which engine the
//! session runs. Protocol selection lives here rather than on a second ALPN
//! because this frame already exists, already rides the versioned envelope, and
//! already precedes every session — so an unsupported engine is a clean error
//! from a connected peer rather than a dial failure the initiator has to
//! interpret, and phase 2's never-register inertness stays a property of one
//! protocol rather than two.
//!
//! It travels in the same versioned envelope as every other reconciliation
//! frame, so a peer speaking a future protocol version is rejected at the
//! envelope rather than after a partial handshake.
//!
//! The engine is carried as a raw tag and read in a second step, exactly as the
//! envelope reads its message kind: an engine this build does not implement is
//! then [`ReconcileError::UnsupportedEngine`] rather than a decode failure that
//! looks like corruption. The default engine is omitted from the encoding, so a
//! range-based session's opening frame is byte for byte what it was before
//! there was anything to choose.

use serde::{Deserialize, Serialize};

use super::codec::{MessageKind, WireMessage};
use super::engine::EngineKind;
use super::error::Result;

/// The initiator's opening frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SessionOpen {
    collection: String,
    #[serde(default, skip_serializing_if = "is_default_engine")]
    engine: u8,
}

impl SessionOpen {
    /// Names the collection whose document heads are to be reconciled and the
    /// engine that will reconcile them.
    pub fn new(collection: impl Into<String>, engine: EngineKind) -> Self {
        Self {
            collection: collection.into(),
            engine: engine as u8,
        }
    }

    /// The collection being reconciled.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// The engine the initiator asked for, or
    /// [`ReconcileError::UnsupportedEngine`] if this build does not run it.
    pub fn engine(&self) -> Result<EngineKind> {
        EngineKind::from_tag(self.engine)
    }
}

fn is_default_engine(engine: &u8) -> bool {
    *engine == EngineKind::Rbsr as u8
}

impl WireMessage for SessionOpen {
    const KIND: MessageKind = MessageKind::SessionOpen;
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reconcile::codec;
    use crate::reconcile::error::ReconcileError;

    /// The opening frame is the difference between what a session's own
    /// accounting reports and what the transport counters see, so a benchmark
    /// that reconciles the two derives its size by division and has no way to
    /// check the answer. This is that check: the phase 3 campaign derived 43
    /// bytes per session for the collection `BenchDoc`, and here is the same
    /// number produced by encoding the frame directly.
    #[test]
    fn a_named_collection_costs_what_the_campaign_derived() {
        let encoded =
            codec::encode(&SessionOpen::new("BenchDoc", EngineKind::Rbsr)).expect("encode");
        assert_eq!(encoded.len(), 43);
    }

    /// Choosing an engine must not silently reprice the sessions the campaign
    /// already measured, which is why the default is omitted from the encoding.
    #[test]
    fn only_a_non_default_engine_costs_anything_to_name() {
        let default = codec::encode(&SessionOpen::new("Note", EngineKind::Rbsr)).expect("encode");
        let chosen = codec::encode(&SessionOpen::new("Note", EngineKind::Riblt)).expect("encode");
        assert_eq!(chosen.len() - default.len(), 9);
    }

    #[test]
    fn an_engine_this_build_does_not_run_is_refused_by_name() {
        let frame = codec::encode(&SessionOpen::new("Note", EngineKind::Riblt)).expect("encode");
        let mut open: SessionOpen = codec::decode(&frame).expect("decode");
        assert_eq!(open.engine().expect("known engine"), EngineKind::Riblt);

        open.engine = 200;
        assert_eq!(
            open.engine(),
            Err(ReconcileError::UnsupportedEngine { tag: 200 })
        );
    }

    /// The frame is a fixed envelope plus the collection name, so the derived
    /// per-session constant is only constant for a given collection.
    #[test]
    fn the_frame_grows_only_with_the_collection_name() {
        let short = codec::encode(&SessionOpen::new("A", EngineKind::Rbsr)).expect("encode");
        let longer =
            codec::encode(&SessionOpen::new("AAAAAAAA", EngineKind::Rbsr)).expect("encode");
        assert_eq!(longer.len() - short.len(), 7);
    }
}
