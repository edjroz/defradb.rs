//! What one reconciliation session found and what it cost.
//!
//! This lives here, beside the coordinator method that produces it, rather than
//! in each embedding crate. Two node builders wrap that method and both must
//! hand callers the same answer; when the type was defined twice, "the same
//! answer" was a convention rather than a fact, and a field added to one copy
//! would have been silently missing from the other.
//!
//! The cost fields are not diagnostics: round count and control bytes are the
//! quantities a reconciliation protocol is judged on, so a caller that cannot
//! read them cannot tell a working session from one that degenerated into
//! exchanging whole sets.

use cid::Cid;

use crate::reconcile::{Diff, ItemId, SessionCost};

/// The difference a session discovered, and what discovering it cost.
///
/// One session reconciles one direction. `need` is what this node will pull;
/// `have` is what the peer is missing and will not receive from this session.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconcileOutcome {
    /// Head CIDs the peer holds and this node lacked; a fetch is already under
    /// way for each.
    pub need: Vec<Cid>,
    /// Head CIDs this node holds and the peer lacked.
    pub have: Vec<Cid>,
    /// Peer messages this session consumed.
    pub rounds: usize,
    /// Encoded frame bytes sent to the peer.
    pub bytes_sent: u64,
    /// Encoded frame bytes received from the peer.
    pub bytes_received: u64,
}

impl ReconcileOutcome {
    /// Reads a coordinator session's result into the shape callers see.
    ///
    /// Identities that are not CIDs are dropped rather than failing the
    /// session: a reconcilable set is CIDs today, and a caller asking what
    /// changed is better served by the heads it can act on than by an error
    /// about one it cannot.
    pub fn new(diff: &Diff, cost: SessionCost) -> Self {
        Self {
            need: cids(diff.need()),
            have: cids(diff.have()),
            rounds: cost.rounds,
            bytes_sent: cost.bytes_sent,
            bytes_received: cost.bytes_received,
        }
    }
}

fn cids(ids: &[ItemId]) -> Vec<Cid> {
    ids.iter()
        .filter_map(|id| Cid::try_from(id.as_bytes()).ok())
        .collect()
}
