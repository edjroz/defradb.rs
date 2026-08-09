//! The `mode=ranges` driver: convergence through real RBSR sessions.
//!
//! A session discovers head CIDs and hands them to the same DAG fetch path the
//! default mode's pull ends in, so the two modes differ in *discovery* and in
//! nothing else. That is the whole claim the campaign tests, and it is why the
//! payload columns of a ranges row must equal those of the default row for the
//! same scenario.
//!
//! Convergence here is one-directional by construction: one session teaches the
//! initiator what it lacks and teaches the responder nothing. The reader is the
//! initiator, matching the default mode where the reader is the one that calls
//! `sync_documents`, so both modes measure the same direction.

use std::time::{Duration, Instant};

use p2p::metrics::CountersSnapshot;

use super::csv::MeasurementRow;
use super::documents::COLLECTION;
use super::harness::NodePair;
use super::run::RunOutcome;

/// Upper bound on sessions driven for one measurement.
///
/// A session is one-shot: it reports what was missing when it ran. Documents
/// that arrive afterwards need another session to be noticed, so a run is a
/// small loop, not a single call. The bound is generous because exceeding it is
/// a finding, not a timeout to be tuned away.
const MAX_SESSIONS: u32 = 24;

/// Overall budget for one scenario's convergence.
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(900);

/// What the sessions themselves reported, as distinct from what the transport
/// counters saw.
///
/// Recording both is the point: [`ReconcileOutcome`](crate::reconcile::ReconcileOutcome)
/// is the session's own accounting of the frames it wrote and read, and the
/// per-ALPN counters are what the transport actually moved. They should agree
/// exactly — both count whole encoded frame bodies — and a disagreement means
/// one of the two is not measuring what it claims.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct SessionCost {
    /// Sessions driven before the run stopped.
    pub sessions: u32,
    /// Peer messages consumed, summed over every session.
    pub rounds: u32,
    pub bytes_sent: u64,
    pub bytes_received: u64,
    /// Head CIDs the sessions asked the fetch path for, summed.
    pub heads_needed: usize,
}

/// One protocol's traffic as the transport counted it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct AlpnCounts {
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub msgs: u64,
}

impl AlpnCounts {
    /// The reconciliation ALPN's slice of a counter snapshot.
    pub(crate) fn reconcile(snapshot: &CountersSnapshot) -> Self {
        let key = String::from_utf8_lossy(p2p::iroh::ALPN_RECON).to_string();
        snapshot
            .protocols
            .get(&key)
            .map(|counts| Self {
                bytes_sent: counts.bytes_sent,
                bytes_recv: counts.bytes_recv,
                msgs: counts.msgs_sent + counts.msgs_recv,
            })
            .unwrap_or_default()
    }
}

/// A ranges run: the rows it recorded, what the sessions cost, and why it
/// stopped if it stopped badly.
pub(crate) struct RangesRun {
    pub rows: Vec<MeasurementRow>,
    pub cost: SessionCost,
    /// The initiator's reconciliation ALPN traffic as the transport counted it.
    pub initiator_wire: AlpnCounts,
    /// The responder's, which must mirror the initiator's.
    pub responder_wire: AlpnCounts,
    /// The error that ended the run, if one did. A session that fails is a
    /// result — the round cap is a cliff, not a degradation — so it is recorded
    /// rather than retried away.
    pub error: Option<String>,
}

impl NodePair {
    /// Drive reconciliation sessions from the reader until the pair agrees.
    pub(crate) async fn measure_reconcile(&self, scenario: &str, doc_ids: &[String]) -> RangesRun {
        let peer_id = self.writer_peer_id().await;
        let reconciler = self
            .reader
            .reconciler()
            .expect("reader was not built with reconciliation enabled")
            .clone();

        let started = Instant::now();
        let mut cost = SessionCost::default();
        let mut error = None;
        let mut converged = false;

        while cost.sessions < MAX_SESSIONS && started.elapsed() < CONVERGENCE_TIMEOUT {
            cost.sessions += 1;
            let outcome = match reconciler.reconcile_collection(&peer_id, COLLECTION).await {
                Ok(outcome) => outcome,
                Err(reason) => {
                    error = Some(reason.to_string());
                    break;
                }
            };
            cost.rounds += outcome.rounds as u32;
            cost.bytes_sent += outcome.bytes_sent;
            cost.bytes_received += outcome.bytes_received;
            cost.heads_needed += outcome.need.len();

            let pending = self.settle(doc_ids).await;
            println!(
                "  {scenario} session {}: {} heads needed, {} of {} documents still divergent",
                cost.sessions,
                outcome.need.len(),
                pending.len(),
                doc_ids.len()
            );
            if pending.is_empty() {
                converged = true;
                break;
            }
            if outcome.need.is_empty() {
                // The session says the initiator is missing nothing, yet the
                // documents differ. Another session would say the same, so
                // looping would only inflate the cost of a run that is already
                // not going to converge.
                error = Some(format!(
                    "session found nothing to fetch with {} documents still divergent",
                    pending.len()
                ));
                break;
            }
        }

        let wall_ms = started.elapsed().as_secs_f64() * 1000.0;
        let state_match = converged && self.divergent(doc_ids).await.is_empty();
        let rows = self
            .rows(&RunOutcome {
                scenario: scenario.to_string(),
                mode: "ranges",
                wall_ms,
                rounds: cost.rounds,
                converged,
                state_match,
            })
            .await;

        RangesRun {
            rows,
            cost,
            initiator_wire: AlpnCounts::reconcile(&self.reader_snapshot()),
            responder_wire: AlpnCounts::reconcile(&self.writer_snapshot()),
            error,
        }
    }
}
