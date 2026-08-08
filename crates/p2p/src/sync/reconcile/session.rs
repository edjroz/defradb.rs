//! The two halves of one reconciliation session over a live stream.
//!
//! Everything protocol-shaped lives in [`crate::reconcile`]; what is added here
//! is the one fact a stream needs that an engine does not have — which
//! collection is being reconciled — and the choice of engine behind each role.

use crate::error::{Error, Result};
use crate::reconcile::engine::rbsr::RbsrEngine;
use crate::reconcile::{
    codec, drive_initiator, drive_responder, Diff, MemorySource, ReconcileStream, Session,
    SessionCost, SessionOpen,
};

/// Opens a session against a peer and runs it to convergence.
pub async fn initiate(
    stream: &mut (impl ReconcileStream + ?Sized),
    collection_id: &str,
    local: MemorySource,
) -> Result<(Diff, SessionCost)> {
    let open = codec::encode(&SessionOpen::new(collection_id)).map_err(reconcile_error)?;
    stream.send_frame(&open).await.map_err(reconcile_error)?;

    drive_initiator(Session::new(RbsrEngine::initiator(local)), stream)
        .await
        .map_err(reconcile_error)
}

/// Reads a peer's opening frame and reports which collection it wants.
///
/// Split from [`serve`] because the caller must build the local snapshot in
/// between, and only this frame says which one to build.
pub async fn accept(stream: &mut (impl ReconcileStream + ?Sized)) -> Result<String> {
    let Some(frame) = stream.recv_frame().await.map_err(reconcile_error)? else {
        return Err(Error::Transport(
            "reconciliation peer closed before naming a collection".to_string(),
        ));
    };
    let open = codec::decode::<SessionOpen>(&frame).map_err(reconcile_error)?;
    Ok(open.collection().to_string())
}

/// Answers a peer's session until it closes the stream, returning what it cost.
pub async fn serve(
    stream: &mut (impl ReconcileStream + ?Sized),
    local: MemorySource,
) -> Result<SessionCost> {
    drive_responder(Session::new(RbsrEngine::responder(local)), stream)
        .await
        .map_err(reconcile_error)
}

fn reconcile_error(error: crate::reconcile::ReconcileError) -> Error {
    Error::Transport(format!("reconciliation session failed: {error}"))
}
