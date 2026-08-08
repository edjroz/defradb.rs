//! Contract tests for the [`Engine`] trait.
//!
//! The RIBLT shape stub below is deliberately *not* an implementation of
//! rateless IBLT. It exists to prove, at compile time and at runtime, that the
//! trait accommodates a one-way coded-symbol stream — an encoder that emits many
//! messages with no intervening `ingest`, and a decoder that stays silent until
//! it has peeled enough symbols. See the module doc on [`Engine`] for the
//! mapping this stub stands in for.

use super::*;
use crate::reconcile::error::Result;
use crate::reconcile::source::ItemId;

fn id(n: u64) -> ItemId {
    ItemId::new(n.to_be_bytes().to_vec())
}

/// One "coded symbol" batch in the stub stream.
#[derive(Clone, Debug, PartialEq, Eq)]
struct SymbolBatch {
    symbols: Vec<ItemId>,
    /// Set by the decoder once it has peeled everything it needs.
    decoded: bool,
}

/// Stands in for a RIBLT encoder: emits symbols indefinitely, one batch per
/// `next_outbound`, until the peer's decoder tells it to stop.
struct SymbolStreamEncoder {
    remaining: Vec<ItemId>,
    stopped: bool,
    diff: Diff,
}

impl Engine for SymbolStreamEncoder {
    type Message = SymbolBatch;

    fn next_outbound(&mut self) -> Result<Option<SymbolBatch>> {
        if self.stopped || self.remaining.is_empty() {
            return Ok(None);
        }
        Ok(Some(SymbolBatch {
            symbols: vec![self.remaining.remove(0)],
            decoded: false,
        }))
    }

    fn ingest(&mut self, message: SymbolBatch) -> Result<Progress> {
        if message.decoded {
            self.stopped = true;
            return Ok(Progress::Converged);
        }
        Ok(Progress::Continue)
    }

    fn diff(&self) -> &Diff {
        &self.diff
    }
}

/// Stands in for a RIBLT decoder: silent while peeling, then one terminal
/// acknowledgement.
struct SymbolStreamDecoder {
    want: usize,
    peeled: Vec<ItemId>,
    ack: Option<SymbolBatch>,
    diff: Diff,
}

impl Engine for SymbolStreamDecoder {
    type Message = SymbolBatch;

    fn next_outbound(&mut self) -> Result<Option<SymbolBatch>> {
        Ok(self.ack.take())
    }

    fn ingest(&mut self, message: SymbolBatch) -> Result<Progress> {
        self.peeled.extend(message.symbols);
        if self.peeled.len() < self.want {
            return Ok(Progress::Continue);
        }
        for item in self.peeled.drain(..) {
            self.diff.record_need(item);
        }
        self.ack = Some(SymbolBatch {
            symbols: Vec::new(),
            decoded: true,
        });
        Ok(Progress::Converged)
    }

    fn diff(&self) -> &Diff {
        &self.diff
    }
}

#[test]
fn an_encoder_may_emit_many_messages_before_any_ingest() {
    let mut encoder = SymbolStreamEncoder {
        remaining: vec![id(1), id(2), id(3)],
        stopped: false,
        diff: Diff::default(),
    };

    let mut emitted = Vec::new();
    while let Some(batch) = encoder.next_outbound().expect("stream is infallible") {
        emitted.push(batch);
    }

    assert_eq!(emitted.len(), 3, "the stream shape needs no alternation");
}

#[test]
fn a_decoder_stays_silent_until_it_converges() {
    let mut decoder = SymbolStreamDecoder {
        want: 2,
        peeled: Vec::new(),
        ack: None,
        diff: Diff::default(),
    };

    assert!(decoder.next_outbound().expect("no error").is_none());

    let first = decoder
        .ingest(SymbolBatch {
            symbols: vec![id(1)],
            decoded: false,
        })
        .expect("no error");
    assert_eq!(first, Progress::Continue);
    assert!(decoder.next_outbound().expect("no error").is_none());

    let second = decoder
        .ingest(SymbolBatch {
            symbols: vec![id(2)],
            decoded: false,
        })
        .expect("no error");
    assert_eq!(second, Progress::Converged);

    let ack = decoder.next_outbound().expect("no error").expect("ack");
    assert!(ack.decoded);
    assert_eq!(decoder.diff().need(), &[id(1), id(2)]);
}

#[test]
fn the_stream_shape_converges_under_the_same_session_drive_loop() {
    let mut encoder = SymbolStreamEncoder {
        remaining: vec![id(1), id(2)],
        stopped: false,
        diff: Diff::default(),
    };
    let mut decoder = SymbolStreamDecoder {
        want: 2,
        peeled: Vec::new(),
        ack: None,
        diff: Diff::default(),
    };

    let mut encoder_done = false;
    for _ in 0..8 {
        let mut inbound = Vec::new();
        while let Some(msg) = encoder.next_outbound().expect("no error") {
            inbound.push(msg);
        }
        for msg in inbound {
            let _ = decoder.ingest(msg).expect("no error");
        }
        while let Some(msg) = decoder.next_outbound().expect("no error") {
            if decoder_ack_stops(&mut encoder, msg) {
                encoder_done = true;
            }
        }
        if encoder_done {
            break;
        }
    }

    assert!(encoder_done, "the ack must reach the encoder");
    assert_eq!(decoder.diff().need().len(), 2);
}

fn decoder_ack_stops(encoder: &mut SymbolStreamEncoder, msg: SymbolBatch) -> bool {
    encoder.ingest(msg).expect("no error") == Progress::Converged
}

#[test]
fn diff_records_both_directions_without_duplication() {
    let mut diff = Diff::default();
    diff.record_need(id(1));
    diff.record_have(id(2));

    assert_eq!(diff.need(), &[id(1)]);
    assert_eq!(diff.have(), &[id(2)]);
    assert!(!diff.is_empty());

    let empty = Diff::default();
    assert!(empty.is_empty());
}
