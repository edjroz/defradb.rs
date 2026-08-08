use super::*;
use crate::reconcile::error::ReconcileError;
use crate::reconcile::source::ItemId;

/// An engine that echoes a scripted progress sequence, so the session's own
/// policy can be tested without any protocol logic underneath it.
struct ScriptedEngine {
    outbound: Vec<u8>,
    script: Vec<Progress>,
    diff: Diff,
}

impl ScriptedEngine {
    fn never_converging() -> Self {
        Self {
            outbound: vec![0],
            script: Vec::new(),
            diff: Diff::default(),
        }
    }
}

impl Engine for ScriptedEngine {
    type Message = u8;

    fn next_outbound(&mut self) -> Result<Option<u8>> {
        Ok(self.outbound.pop())
    }

    fn ingest(&mut self, message: u8) -> Result<Progress> {
        self.outbound.push(message);
        if self.script.is_empty() {
            return Ok(Progress::Continue);
        }
        Ok(self.script.remove(0))
    }

    fn diff(&self) -> &Diff {
        &self.diff
    }
}

#[test]
fn outbound_messages_pass_through_until_convergence() {
    let mut session = Session::new(ScriptedEngine {
        outbound: vec![7],
        script: vec![Progress::Converged],
        diff: Diff::default(),
    });

    assert_eq!(session.next_outbound().expect("no error"), Some(7));
    assert_eq!(session.next_outbound().expect("no error"), None);

    assert_eq!(session.ingest(1).expect("no error"), Progress::Converged);
    assert!(session.is_converged());
    assert_eq!(session.rounds(), 1);
}

#[test]
fn a_converged_session_emits_nothing_further() {
    let mut session = Session::new(ScriptedEngine {
        outbound: vec![1, 2],
        script: vec![Progress::Converged],
        diff: Diff::default(),
    });

    session.ingest(9).expect("no error");
    assert!(session.is_converged());
    assert_eq!(
        session.next_outbound().expect("no error"),
        None,
        "the terminal message is never transmitted"
    );
}

#[test]
fn a_converged_session_refuses_further_input() {
    let mut session = Session::new(ScriptedEngine {
        outbound: Vec::new(),
        script: vec![Progress::Converged],
        diff: Diff::default(),
    });

    session.ingest(1).expect("no error");
    assert_eq!(session.ingest(2), Err(ReconcileError::SessionClosed));
}

#[test]
fn a_peer_that_never_agrees_hits_the_round_cap() {
    let mut session = Session::new(ScriptedEngine::never_converging());

    for round in 1..=MAX_ROUNDS {
        assert_eq!(
            session.ingest(0).expect("within the cap"),
            Progress::Continue
        );
        assert_eq!(session.rounds(), round);
    }

    assert_eq!(
        session.ingest(0),
        Err(ReconcileError::RoundCapExceeded { max: MAX_ROUNDS })
    );
}

#[test]
fn the_diff_survives_the_session() {
    let mut engine = ScriptedEngine::never_converging();
    engine.diff.record_need(ItemId::new(vec![1]));
    engine.script = vec![Progress::Converged];

    let mut session = Session::new(engine);
    session.ingest(0).expect("no error");

    assert_eq!(session.diff().need().len(), 1);
    assert_eq!(session.into_diff().need().len(), 1);
}
