//! Per-protocol application traffic counters for the P2P transports.
//!
//! A transport binding records every framed message it writes or reads against
//! the protocol that carried it. Control traffic (coordination messages) is
//! kept apart from payload traffic (block transfer) so a sync benchmark can
//! report discovery cost independently of the data it moved.
//!
//! What is counted is the message body — the serialised CBOR, or a raw CAR
//! body — and not the bytes the network moved: the 4-byte length prefix, QUIC
//! framing, ALPN negotiation, acknowledgements and retransmissions are all
//! outside these totals. Two nodes' counts are therefore comparable with each
//! other, but they are a floor on link utilisation, not a measurement of it.
//!
//! Counting is opt-in: transports hold an `Option<Arc<TransportCounters>>` and
//! skip the work entirely when it is `None`.

use std::collections::BTreeMap;
use std::sync::Arc;

use parking_lot::Mutex;

/// Whether a protocol carries coordination messages or block payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TrafficClass {
    /// Coordination messages: requests, replies, gossip.
    Control,
    /// Block transfer.
    Payload,
}

/// Byte and message counts for a single protocol, split by direction.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProtocolCounts {
    pub class: TrafficClass,
    pub bytes_sent: u64,
    pub bytes_recv: u64,
    pub msgs_sent: u64,
    pub msgs_recv: u64,
}

impl ProtocolCounts {
    fn new(class: TrafficClass) -> Self {
        Self {
            class,
            bytes_sent: 0,
            bytes_recv: 0,
            msgs_sent: 0,
            msgs_recv: 0,
        }
    }
}

/// Accumulates per-protocol application traffic for one node.
///
/// Shared across the transport's tasks behind an [`Arc`]; every method takes
/// `&self`.
#[derive(Debug, Default)]
pub struct TransportCounters {
    protocols: Mutex<BTreeMap<String, ProtocolCounts>>,
}

impl TransportCounters {
    /// A new, empty counter set ready to hand to a transport.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record one outbound message of `bytes` framed payload.
    pub fn record_sent(&self, protocol: &str, class: TrafficClass, bytes: usize) {
        let mut protocols = self.protocols.lock();
        let counts = protocols
            .entry(protocol.to_string())
            .or_insert_with(|| ProtocolCounts::new(class));
        counts.bytes_sent += bytes as u64;
        counts.msgs_sent += 1;
    }

    /// Record one inbound message of `bytes` framed payload.
    pub fn record_recv(&self, protocol: &str, class: TrafficClass, bytes: usize) {
        let mut protocols = self.protocols.lock();
        let counts = protocols
            .entry(protocol.to_string())
            .or_insert_with(|| ProtocolCounts::new(class));
        counts.bytes_recv += bytes as u64;
        counts.msgs_recv += 1;
    }

    /// Drop all accumulated counts, so a benchmark can exclude setup traffic.
    pub fn reset(&self) {
        self.protocols.lock().clear();
    }

    /// An immutable view of the counts at this instant.
    pub fn snapshot(&self) -> CountersSnapshot {
        CountersSnapshot {
            protocols: self.protocols.lock().clone(),
        }
    }
}

/// An immutable view of a [`TransportCounters`] at a point in time.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CountersSnapshot {
    /// Counts keyed by protocol identifier, in deterministic order.
    pub protocols: BTreeMap<String, ProtocolCounts>,
}

impl CountersSnapshot {
    fn sum<F>(&self, class: TrafficClass, field: F) -> u64
    where
        F: Fn(&ProtocolCounts) -> u64,
    {
        self.protocols
            .values()
            .filter(|counts| counts.class == class)
            .map(field)
            .sum()
    }

    pub fn control_bytes_sent(&self) -> u64 {
        self.sum(TrafficClass::Control, |c| c.bytes_sent)
    }

    pub fn control_bytes_recv(&self) -> u64 {
        self.sum(TrafficClass::Control, |c| c.bytes_recv)
    }

    /// Control messages in both directions, matching the Go harness's
    /// `ctrlMsgs` column (`msgsSent + msgsRecv`).
    pub fn control_msgs(&self) -> u64 {
        self.sum(TrafficClass::Control, |c| c.msgs_sent + c.msgs_recv)
    }

    pub fn payload_bytes_sent(&self) -> u64 {
        self.sum(TrafficClass::Payload, |c| c.bytes_sent)
    }

    pub fn payload_bytes_recv(&self) -> u64 {
        self.sum(TrafficClass::Payload, |c| c.bytes_recv)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CTRL: TrafficClass = TrafficClass::Control;
    const DATA: TrafficClass = TrafficClass::Payload;

    #[test]
    fn accumulates_per_protocol_and_direction() {
        let counters = TransportCounters::new();
        counters.record_sent("docsync", CTRL, 100);
        counters.record_sent("docsync", CTRL, 50);
        counters.record_recv("docsync", CTRL, 7);
        counters.record_sent("car", DATA, 4096);

        let snapshot = counters.snapshot();
        let docsync = snapshot.protocols["docsync"];
        assert_eq!(docsync.bytes_sent, 150);
        assert_eq!(docsync.msgs_sent, 2);
        assert_eq!(docsync.bytes_recv, 7);
        assert_eq!(docsync.msgs_recv, 1);
        assert_eq!(snapshot.protocols["car"].bytes_sent, 4096);
    }

    #[test]
    fn separates_control_from_payload() {
        let counters = TransportCounters::new();
        counters.record_sent("docsync", CTRL, 100);
        counters.record_recv("docsync-resp", CTRL, 200);
        counters.record_sent("car", DATA, 4096);
        counters.record_recv("car-resp", DATA, 8192);

        let snapshot = counters.snapshot();
        assert_eq!(snapshot.control_bytes_sent(), 100);
        assert_eq!(snapshot.control_bytes_recv(), 200);
        assert_eq!(snapshot.control_msgs(), 2);
        assert_eq!(snapshot.payload_bytes_sent(), 4096);
        assert_eq!(snapshot.payload_bytes_recv(), 8192);
    }

    #[test]
    fn reset_clears_counts() {
        let counters = TransportCounters::new();
        counters.record_sent("docsync", CTRL, 100);
        counters.reset();

        let snapshot = counters.snapshot();
        assert!(snapshot.protocols.is_empty());
        assert_eq!(snapshot.control_bytes_sent(), 0);
    }

    #[test]
    fn snapshot_is_detached_from_later_writes() {
        let counters = TransportCounters::new();
        counters.record_sent("docsync", CTRL, 100);
        let snapshot = counters.snapshot();
        counters.record_sent("docsync", CTRL, 900);

        assert_eq!(snapshot.control_bytes_sent(), 100);
        assert_eq!(counters.snapshot().control_bytes_sent(), 1000);
    }

    #[test]
    fn counts_are_shared_across_clones_of_the_handle() {
        let counters = TransportCounters::new();
        let other = Arc::clone(&counters);
        counters.record_sent("docsync", CTRL, 10);
        other.record_sent("docsync", CTRL, 10);

        assert_eq!(counters.snapshot().control_bytes_sent(), 20);
    }
}
