//! Fuzzes the reconciliation wire codec against arbitrary bytes.
//!
//! The codec is the node's first contact with a remote peer's bytes, so the
//! property under test is total: for any input at all, decoding either returns a
//! message or returns an error, and never panics, aborts, or allocates without
//! bound. The round trip re-encodes whatever decoded, which catches an encoder
//! that cannot represent a value its own decoder accepts.

#![no_main]

use libfuzzer_sys::fuzz_target;
use p2p::reconcile::codec;
use p2p::reconcile::engine::rbsr::RbsrMessage;
use p2p::reconcile::engine::riblt::RibltMessage;
use p2p::reconcile::SessionOpen;

fuzz_target!(|data: &[u8]| {
    let _ = codec::peek_header(data);

    if let Ok(message) = codec::decode::<RbsrMessage>(data) {
        let reencoded = codec::encode(&message).expect("a decoded message must re-encode");
        let round_tripped =
            codec::decode::<RbsrMessage>(&reencoded).expect("a re-encoded message must decode");
        assert_eq!(message, round_tripped, "codec round trip must be lossless");
    }

    if let Ok(message) = codec::decode::<RibltMessage>(data) {
        let reencoded = codec::encode(&message).expect("a decoded message must re-encode");
        let round_tripped =
            codec::decode::<RibltMessage>(&reencoded).expect("a re-encoded message must decode");
        assert_eq!(message, round_tripped, "codec round trip must be lossless");
    }

    // The opening frame is the newest protocol surface and the only one a peer
    // reaches before any engine exists, so reading its engine tag must be total:
    // an engine this build does not run is an error, never a panic, and the
    // frame must still round-trip whichever tag it carried.
    if let Ok(open) = codec::decode::<SessionOpen>(data) {
        let _ = open.engine();
        let reencoded = codec::encode(&open).expect("a decoded frame must re-encode");
        let round_tripped =
            codec::decode::<SessionOpen>(&reencoded).expect("a re-encoded frame must decode");
        assert_eq!(open, round_tripped, "codec round trip must be lossless");
    }
});
