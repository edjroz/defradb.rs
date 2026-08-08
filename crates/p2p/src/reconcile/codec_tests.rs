use super::*;
use crate::reconcile::engine::rbsr::{Fingerprint, Range, RbsrMessage};
use crate::reconcile::error::ReconcileError;
use crate::reconcile::source::{Bound, ItemId, SortKey};

fn id(n: u64) -> ItemId {
    ItemId::new(n.to_be_bytes().to_vec())
}

fn sample_message() -> RbsrMessage {
    RbsrMessage::new(vec![
        Range::skip(Bound::Key(SortKey::new(1, &id(1)))),
        Range::fingerprint(
            Bound::Key(SortKey::new(2, &id(2))),
            Fingerprint::of([id(3), id(4)].iter()),
        ),
        Range::id_list(Bound::Max, vec![id(5), id(6)]),
    ])
}

#[test]
fn every_range_mode_roundtrips() {
    let msg = sample_message();
    let bytes = encode(&msg).expect("encodes");
    let decoded: RbsrMessage = decode(&bytes).expect("decodes");
    assert_eq!(decoded, msg);
}

#[test]
fn empty_message_roundtrips() {
    let msg = RbsrMessage::new(Vec::new());
    let bytes = encode(&msg).expect("encodes");
    assert_eq!(decode::<RbsrMessage>(&bytes).expect("decodes"), msg);
}

#[test]
fn every_bound_variant_roundtrips() {
    for bound in [Bound::Min, Bound::Key(SortKey::new(7, &id(7))), Bound::Max] {
        let msg = RbsrMessage::new(vec![Range::skip(bound.clone())]);
        let bytes = encode(&msg).expect("encodes");
        assert_eq!(decode::<RbsrMessage>(&bytes).expect("decodes"), msg);
    }
}

#[test]
fn frames_carry_the_protocol_version_and_kind() {
    let bytes = encode(&sample_message()).expect("encodes");
    let header = peek_header(&bytes).expect("header decodes");
    assert_eq!(header.version, PROTOCOL_VERSION);
    assert_eq!(header.kind, MessageKind::RbsrRanges as u8);
}

#[test]
fn unknown_version_fails_safe() {
    let bytes = encode_with_version(&sample_message(), PROTOCOL_VERSION + 1).expect("encodes");
    assert_eq!(
        decode::<RbsrMessage>(&bytes),
        Err(ReconcileError::UnsupportedVersion {
            found: PROTOCOL_VERSION + 1,
            expected: PROTOCOL_VERSION,
        })
    );
}

#[test]
fn unknown_message_kind_fails_safe() {
    let bytes = encode_raw(PROTOCOL_VERSION, 99, Vec::new()).expect("encodes");
    assert_eq!(
        decode::<RbsrMessage>(&bytes),
        Err(ReconcileError::UnknownMessageKind(99))
    );
}

#[test]
fn mismatched_known_kind_fails_safe() {
    let bytes = encode_raw(
        PROTOCOL_VERSION,
        MessageKind::RibltSymbols as u8,
        Vec::new(),
    )
    .expect("encodes");
    assert_eq!(
        decode::<RbsrMessage>(&bytes),
        Err(ReconcileError::MessageKindMismatch {
            found: MessageKind::RibltSymbols as u8,
            expected: MessageKind::RbsrRanges as u8,
        })
    );
}

#[test]
fn truncated_frames_error_and_never_panic() {
    let bytes = encode(&sample_message()).expect("encodes");
    for cut in 0..bytes.len() {
        let err = decode::<RbsrMessage>(&bytes[..cut]);
        assert!(err.is_err(), "truncation at {cut} must not decode");
    }
}

#[test]
fn garbage_input_errors_and_never_panics() {
    let mut state = 0x2545_f491_4f6c_dd1du64;
    for len in 0..64usize {
        let garbage: Vec<u8> = (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state & 0xff) as u8
            })
            .collect();
        let _ = decode::<RbsrMessage>(&garbage);
    }
}

#[test]
fn bit_flips_in_a_valid_frame_error_or_decode_but_never_panic() {
    let bytes = encode(&sample_message()).expect("encodes");
    for byte in 0..bytes.len() {
        for bit in 0..8 {
            let mut corrupt = bytes.clone();
            corrupt[byte] ^= 1 << bit;
            let _ = decode::<RbsrMessage>(&corrupt);
        }
    }
}

#[test]
fn oversized_frames_are_rejected_before_decoding() {
    let huge = vec![0u8; MAX_FRAME_BYTES + 1];
    assert_eq!(
        decode::<RbsrMessage>(&huge),
        Err(ReconcileError::FrameTooLarge {
            size: MAX_FRAME_BYTES + 1,
            max: MAX_FRAME_BYTES,
        })
    );
}
