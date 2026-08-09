//! What a session costs on a link that is not loopback.
//!
//! Every byte measurement in this campaign runs at RTT ≈ 0, and that is
//! structurally biased: it charges the range engine nothing for the rounds it
//! spends narrowing, and gives the rateless engine no credit for spending
//! fewer. The recovered comparison plan called this the single most
//! decision-relevant chart for exactly that reason.
//!
//! The delay is injected **in the harness**, not with `tc` or `dummynet`: a
//! deterministic sleep on every frame arrival, applied to both ends and
//! therefore to both engines identically. One frame in flight costs one one-way
//! delay, whichever engine sent it, so the injected term is
//! `messages x delay` and the rest of the measured time is compute.
//!
//! **The shipped DocSync path is not in this study**, and that is the same
//! ruling the byte tables already carry: its wall time is bounded below by the
//! adapter's timeout ladder, so what would be measured is the ladder, not the
//! protocol.
//!
//! Rounds are `O(log d)`, not "about half a round trip" — 1 at `d = 0` and
//! `d = 1`, 3 at `d = 8`, 6 at `d = 100`, 9 at `d = 1000` — and the totals below
//! are driven by the measured message count, never by that retracted headline.
//!
//! ```text
//! cargo test --release -p p2p --features iroh-transport --lib \
//!   sync::reconcile::latency_tests -- --ignored --nocapture
//! ```

use std::time::{Duration, Instant};

use async_trait::async_trait;

use super::{accept, initiate, serve};
use crate::reconcile::engine::comparison::diverged;
use crate::reconcile::error::Result as ReconcileResult;
use crate::reconcile::stream::MemoryStream;
use crate::reconcile::{EngineKind, ReconcileStream};

const COLLECTION: &str = "BenchDoc";

/// One-way delays the study sweeps: loopback, a metro hop, a continental hop,
/// and a satellite-shaped one.
const DELAYS_MS: [u64; 4] = [0, 25, 100, 300];

/// Above `BRANCHING_FACTOR * ID_LIST_THRESHOLD`, so the range engine is in its
/// `O(d log n)` regime rather than listing whole ranges.
const SET_SIZE: usize = 10_240;

const SEED: u64 = 0x5EED_C0FFEE;

/// A [`ReconcileStream`] that makes every arriving frame late.
///
/// The delay is charged on receive rather than on send so that a frame costs
/// exactly one one-way delay end to end, and so that a side which sends two
/// frames back to back pays for two hops — which is what a real link does and
/// what a send-side sleep would hide.
#[derive(Debug)]
struct DelayedStream<S: ReconcileStream> {
    inner: S,
    one_way: Duration,
    frames: usize,
}

impl<S: ReconcileStream> DelayedStream<S> {
    fn new(inner: S, one_way: Duration) -> Self {
        Self {
            inner,
            one_way,
            frames: 0,
        }
    }
}

#[async_trait]
impl<S: ReconcileStream> ReconcileStream for DelayedStream<S> {
    async fn send_frame(&mut self, frame: &[u8]) -> ReconcileResult<()> {
        self.inner.send_frame(frame).await
    }

    async fn recv_frame(&mut self) -> ReconcileResult<Option<Vec<u8>>> {
        let frame = self.inner.recv_frame().await?;
        if frame.is_some() {
            self.frames += 1;
            if !self.one_way.is_zero() {
                tokio::time::sleep(self.one_way).await;
            }
        }
        Ok(frame)
    }

    async fn finish(&mut self) -> ReconcileResult<()> {
        self.inner.finish().await
    }
}

/// What one delayed session cost.
struct Timing {
    wall: Duration,
    rounds: usize,
    /// Frames that crossed the link in both directions, the opening frame
    /// included — the quantity the injected delay multiplies.
    frames: usize,
}

async fn timed(engine: EngineKind, n: usize, d: usize, one_way: Duration) -> Timing {
    let (local, remote) = diverged(n, d, SEED);
    let (left, right) = MemoryStream::pair();
    let mut left = DelayedStream::new(left, one_way);
    let mut right = DelayedStream::new(right, one_way);

    let started = Instant::now();
    let responder = async {
        let (collection, engine) = accept(&mut right).await?;
        assert_eq!(collection, COLLECTION);
        serve(&mut right, remote, engine).await
    };
    let (initiated, served) =
        tokio::join!(initiate(&mut left, COLLECTION, local, engine), responder);
    let wall = started.elapsed();

    let (_, cost) = initiated.expect("the session converges");
    served.expect("the responder finishes");
    Timing {
        wall,
        rounds: cost.rounds,
        frames: left.frames + right.frames,
    }
}

/// Total sync time per engine across the delay sweep.
///
/// Compute and injected latency are printed separately, because they answer
/// different questions and only one of them is a property of the link. The
/// `frames` column is what the delay multiplies and is measured, not modelled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "benchmark: sleeps its way through a simulated three-hundred-millisecond link"]
async fn total_sync_time_against_link_latency() {
    println!("n,d,engine,delayMs,frames,rounds,wallMs");
    for d in [0usize, 1, 8, 100, 1_000] {
        for engine in [EngineKind::Rbsr, EngineKind::Riblt] {
            let name = match engine {
                EngineKind::Rbsr => "ranges",
                EngineKind::Riblt => "riblt",
            };
            for delay_ms in DELAYS_MS {
                let timing = timed(engine, SET_SIZE, d, Duration::from_millis(delay_ms)).await;
                println!(
                    "{SET_SIZE},{d},{name},{delay_ms},{},{},{:.1}",
                    timing.frames,
                    timing.rounds,
                    timing.wall.as_secs_f64() * 1000.0
                );
            }
        }
    }
}

/// The injected delay must be charged to both engines the same way, or the
/// study is about the harness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_frame_costs_exactly_one_one_way_delay() {
    const ONE_WAY: Duration = Duration::from_millis(20);
    for engine in [EngineKind::Rbsr, EngineKind::Riblt] {
        let free = timed(engine, 256, 4, Duration::ZERO).await;
        let delayed = timed(engine, 256, 4, ONE_WAY).await;

        assert_eq!(
            free.frames, delayed.frames,
            "delaying a link must not change how many frames cross it"
        );
        let injected = ONE_WAY * delayed.frames as u32;
        assert!(
            delayed.wall >= injected,
            "{engine:?}: {:?} is less than the {injected:?} injected over {} frames",
            delayed.wall,
            delayed.frames
        );
    }
}
