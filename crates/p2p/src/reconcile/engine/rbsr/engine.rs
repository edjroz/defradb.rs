//! The RBSR engine: one side of one range-reconciliation session.

use super::message::RbsrMessage;
use super::segment_tree::SegmentTree;
use super::{initiator, responder};
use crate::reconcile::engine::{Diff, Engine, Progress};
use crate::reconcile::error::Result;
use crate::reconcile::source::ItemSource;

/// Which side of the session this engine plays.
///
/// The asymmetry is the protocol's, not an implementation detail: only the
/// initiator learns a difference, and only the responder refines ranges.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    /// Opens the session, keeps the tiling fixed, and learns the difference.
    Initiator,
    /// Refines mismatching ranges and learns nothing.
    Responder,
}

/// A range-based set reconciliation engine over a sealed local set.
pub struct RbsrEngine<S: ItemSource> {
    index: SegmentTree<S>,
    role: Role,
    pending: Option<RbsrMessage>,
    diff: Diff,
}

impl<S: ItemSource> RbsrEngine<S> {
    /// Builds the initiating side, which opens with a full-range fingerprint.
    pub fn initiator(source: S) -> Self {
        let index = SegmentTree::build(source);
        let opening = initiator::initiate(&index);
        Self {
            index,
            role: Role::Initiator,
            pending: Some(opening),
            diff: Diff::default(),
        }
    }

    /// Builds the responding side, which stays silent until spoken to.
    pub fn responder(source: S) -> Self {
        Self {
            index: SegmentTree::build(source),
            role: Role::Responder,
            pending: None,
            diff: Diff::default(),
        }
    }

    /// Which side this engine plays.
    pub fn role(&self) -> Role {
        self.role
    }
}

impl<S: ItemSource> Engine for RbsrEngine<S> {
    type Message = RbsrMessage;

    fn next_outbound(&mut self) -> Result<Option<RbsrMessage>> {
        Ok(self.pending.take())
    }

    fn ingest(&mut self, message: RbsrMessage) -> Result<Progress> {
        match self.role {
            Role::Initiator => {
                let Self { index, diff, .. } = self;
                let outgoing = initiator::reconcile(index, diff, &message)?;
                if outgoing.is_all_skip() {
                    self.pending = None;
                    return Ok(Progress::Converged);
                }
                self.pending = Some(outgoing);
                Ok(Progress::Continue)
            }
            Role::Responder => {
                self.pending = Some(responder::respond(&self.index, &message)?);
                Ok(Progress::Continue)
            }
        }
    }

    fn diff(&self) -> &Diff {
        &self.diff
    }
}
