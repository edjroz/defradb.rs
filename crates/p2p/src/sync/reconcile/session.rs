//! The two halves of one reconciliation session over a live stream.
//!
//! Everything protocol-shaped lives in [`crate::reconcile`]; what is added here
//! is the two facts a stream needs that an engine does not have — which
//! collection is being reconciled, and which engine is going to reconcile it —
//! and the mapping from that engine to the role each side plays.
//!
//! The roles do not line up between the engines, which is why the mapping lives
//! here rather than in the drive loop. RBSR's initiator drives an interactive
//! narrowing and its responder answers; RIBLT's initiator *decodes* and its
//! responder streams cells obliviously. Both leave the initiator knowing its
//! difference and the responder unchanged, which is the only thing the
//! coordinator above depends on.

use crate::error::{Error, Result};
use crate::reconcile::engine::rbsr::RbsrEngine;
use crate::reconcile::engine::riblt::RibltEngine;
use crate::reconcile::{
    codec, drive_initiator, drive_responder, Diff, EngineKind, MemorySource, ReconcileStream,
    Session, SessionCost, SessionOpen,
};

/// Opens a session against a peer and runs it to convergence.
///
/// Convergence here is the local side's: this node ends knowing its full
/// difference from the peer. The peer learns nothing and changes nothing.
pub async fn initiate(
    stream: &mut (impl ReconcileStream + ?Sized),
    collection_id: &str,
    local: MemorySource,
    engine: EngineKind,
) -> Result<(Diff, SessionCost)> {
    let open = codec::encode(&SessionOpen::new(collection_id, engine)).map_err(reconcile_error)?;
    stream.send_frame(&open).await.map_err(reconcile_error)?;

    match engine {
        EngineKind::Rbsr => {
            drive_initiator(Session::new(RbsrEngine::initiator(local)), stream).await
        }
        EngineKind::Riblt => {
            let engine = RibltEngine::decoder(&local).map_err(reconcile_error)?;
            drive_initiator(Session::new(engine), stream).await
        }
    }
    .map_err(reconcile_error)
}

/// Reads a peer's opening frame and reports what it asked for.
///
/// Split from [`serve`] because the caller must build the local snapshot in
/// between, and only this frame says which one to build. An engine this build
/// does not implement is refused here, before any snapshot is taken.
pub async fn accept(stream: &mut (impl ReconcileStream + ?Sized)) -> Result<(String, EngineKind)> {
    let Some(frame) = stream.recv_frame().await.map_err(reconcile_error)? else {
        return Err(Error::Transport(
            "reconciliation peer closed before naming a collection".to_string(),
        ));
    };
    let open = codec::decode::<SessionOpen>(&frame).map_err(reconcile_error)?;
    let engine = open.engine().map_err(reconcile_error)?;
    Ok((open.collection().to_string(), engine))
}

/// Answers a peer's session until it closes the stream, returning what it cost.
pub async fn serve(
    stream: &mut (impl ReconcileStream + ?Sized),
    local: MemorySource,
    engine: EngineKind,
) -> Result<SessionCost> {
    match engine {
        EngineKind::Rbsr => {
            drive_responder(Session::new(RbsrEngine::responder(local)), stream).await
        }
        EngineKind::Riblt => {
            let engine = RibltEngine::encoder(&local).map_err(reconcile_error)?;
            drive_responder(Session::new(engine), stream).await
        }
    }
    .map_err(reconcile_error)
}

fn reconcile_error(error: crate::reconcile::ReconcileError) -> Error {
    Error::Transport(format!("reconciliation session failed: {error}"))
}
