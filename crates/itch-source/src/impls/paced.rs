//! Pacing combinator for `MessageSource`.
//!
//! Per `docs/ITCH-SOURCE.md` §13:
//!
//! > "My source produces messages with absolute timestamps; the
//! > transport wants relative pacing. Where do I put the sleep? —
//! > Inside the source."
//!
//! [`paced()`] wraps any inner [`crate::MessageSource`] and emits its
//! messages on a clock chosen via [`Pacing`]:
//!
//! - [`Pacing::MaxSpeed`] — yield as fast as the inner stream
//!   allows. Equivalent to no wrapping (provided here so the call
//!   site is `match`-uniform).
//! - [`Pacing::Fixed`] — one message per `period`.
//! - [`Pacing::Realtime`] — between message *N* (timestamp `t_n`)
//!   and message *N+1* (timestamp `t_{n+1}`), sleep
//!   `t_{n+1} - t_n` before yielding *N+1*. The first message
//!   yields without delay. Backed by `tokio::time::sleep`.
//!
//! `FileReplaySource` (full `.itch` capture-file replay) lands with
//! `itch-replay` in v0.5 (issue #32). The pacing primitive ships
//! here so any inner stream — `IteratorSource`, `ChannelSource`,
//! the future `FileReplaySource` — can opt into pacing without
//! reinventing the sleep logic.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;
use itch_protocol::Message;
use tokio::time::{sleep, Instant, Sleep};

use crate::SourceError;

/// Emission cadence for a paced source.
#[derive(Clone, Copy, Debug)]
pub enum Pacing {
    /// Yield messages as fast as the inner stream allows.
    MaxSpeed,
    /// Emit one message per `period`. The first message is emitted
    /// immediately; subsequent messages sleep the remainder of the
    /// period after the previous emission.
    Fixed {
        /// Inter-message period.
        period: Duration,
    },
    /// Honor the gaps between consecutive `Message::header().timestamp`
    /// values. The first message yields without delay; for each
    /// subsequent message *N+1*, sleep `t_{n+1} - t_n` before
    /// yielding.
    ///
    /// Out-of-order timestamps (`t_{n+1} <= t_n`) yield without
    /// delay — the source forwards them in order received without
    /// retroactive smoothing.
    Realtime,
}

/// Stream adapter that emits an inner [`crate::MessageSource`] under
/// a chosen [`Pacing`]. Construct via [`paced()`].
#[must_use = "streams do nothing unless polled"]
pub struct PacedSource<S> {
    inner: S,
    pacing: Pacing,
    pending: Option<Message>,
    sleep: Option<Pin<Box<Sleep>>>,
    last_ts_ns: Option<u64>,
    last_tick: Option<Instant>,
}

/// Wrap `inner` in a [`PacedSource`] under the chosen [`Pacing`].
///
/// `Pacing::MaxSpeed` returns a wrapper that delegates poll directly
/// to the inner stream (no-op cost beyond the wrapper).
pub fn paced<S>(inner: S, pacing: Pacing) -> PacedSource<S>
where
    S: Stream<Item = Result<Message, SourceError>> + Send + Unpin,
{
    PacedSource {
        inner,
        pacing,
        pending: None,
        sleep: None,
        last_ts_ns: None,
        last_tick: None,
    }
}

impl<S> Stream for PacedSource<S>
where
    S: Stream<Item = Result<Message, SourceError>> + Send + Unpin,
{
    type Item = Result<Message, SourceError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Self is Unpin (no #[pin] fields); take &mut Self.
        let this = Pin::into_inner(self);

        // 1) Drive any active sleep to completion before yielding.
        if let Some(sleep_fut) = this.sleep.as_mut() {
            match sleep_fut.as_mut().poll(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(()) => {
                    this.sleep = None;
                    if let Some(msg) = this.pending.take() {
                        this.last_ts_ns = Some(msg.header().timestamp.as_u64());
                        return Poll::Ready(Some(Ok(msg)));
                    }
                }
            }
        }

        // 2) Pull the next item from the inner stream.
        let item = match Pin::new(&mut this.inner).poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
            Poll::Ready(Some(Ok(msg))) => msg,
        };

        // 3) Decide how to emit, given the configured pacing.
        match this.pacing {
            Pacing::MaxSpeed => {
                this.last_ts_ns = Some(item.header().timestamp.as_u64());
                Poll::Ready(Some(Ok(item)))
            }
            Pacing::Fixed { period } => {
                let now = Instant::now();
                match this.last_tick {
                    None => {
                        // First message: emit immediately, anchor the
                        // tick.
                        this.last_tick = Some(now);
                        this.last_ts_ns = Some(item.header().timestamp.as_u64());
                        Poll::Ready(Some(Ok(item)))
                    }
                    Some(prev) => {
                        let next_tick = prev + period;
                        if now >= next_tick {
                            this.last_tick = Some(now);
                            this.last_ts_ns = Some(item.header().timestamp.as_u64());
                            Poll::Ready(Some(Ok(item)))
                        } else {
                            this.pending = Some(item);
                            this.last_tick = Some(next_tick);
                            this.sleep = Some(Box::pin(tokio::time::sleep_until(next_tick)));
                            cx.waker().wake_by_ref();
                            Poll::Pending
                        }
                    }
                }
            }
            Pacing::Realtime => {
                let cur_ts = item.header().timestamp.as_u64();
                let last = this.last_ts_ns;
                match last {
                    None => {
                        this.last_ts_ns = Some(cur_ts);
                        Poll::Ready(Some(Ok(item)))
                    }
                    Some(prev_ts) if cur_ts <= prev_ts => {
                        this.last_ts_ns = Some(cur_ts);
                        Poll::Ready(Some(Ok(item)))
                    }
                    Some(prev_ts) => {
                        let delay = Duration::from_nanos(cur_ts - prev_ts);
                        this.pending = Some(item);
                        this.sleep = Some(Box::pin(sleep(delay)));
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{IteratorSource, MessageSource};
    use futures::StreamExt;
    use itch_protocol::{
        enums::EventCode, messages::SystemEvent, Header, Message, StockLocate, Timestamp,
        TrackingNumber,
    };
    use std::time::Instant as StdInstant;

    fn ts(ns: u64) -> Header {
        Header {
            stock_locate: StockLocate::from_u16(0),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(ns),
        }
    }

    fn msg(ns: u64) -> Message {
        Message::SystemEvent(SystemEvent {
            header: ts(ns),
            event_code: EventCode::StartOfMessages,
        })
    }

    #[tokio::test]
    async fn max_speed_yields_immediately() {
        let inner = IteratorSource::new(vec![msg(1), msg(2), msg(3)]);
        let mut s = paced(inner, Pacing::MaxSpeed);
        let collected: Vec<_> = s.by_ref().collect().await;
        assert_eq!(collected.len(), 3);
        for item in &collected {
            assert!(item.is_ok());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn fixed_pacing_emits_one_per_period() {
        let inner = IteratorSource::new(vec![msg(0), msg(0), msg(0), msg(0)]);
        let mut s = paced(
            inner,
            Pacing::Fixed {
                period: Duration::from_millis(100),
            },
        );
        let start = tokio::time::Instant::now();
        let mut got = 0;
        while let Some(item) = s.next().await {
            assert!(item.is_ok());
            got += 1;
        }
        let elapsed = tokio::time::Instant::now() - start;
        assert_eq!(got, 4);
        // 4 messages on a 100 ms period: first at t0, then t100,
        // t200, t300 → total elapsed ≥ 300 ms.
        assert!(
            elapsed >= Duration::from_millis(300),
            "elapsed {elapsed:?} too short for 4×100ms",
        );
    }

    #[tokio::test(start_paused = true)]
    async fn realtime_honors_inter_message_gap() {
        let inner = IteratorSource::new(vec![
            msg(0),
            msg(50_000_000),  // 50 ms after first
            msg(150_000_000), // 100 ms after second
        ]);
        let mut s = paced(inner, Pacing::Realtime);
        let start = tokio::time::Instant::now();
        let _first = s.next().await.unwrap().unwrap();
        let after_first = tokio::time::Instant::now() - start;
        assert!(
            after_first < Duration::from_millis(5),
            "first must be ~ immediate"
        );

        let _second = s.next().await.unwrap().unwrap();
        let after_second = tokio::time::Instant::now() - start;
        assert!(
            after_second >= Duration::from_millis(50),
            "second after at least 50 ms, got {after_second:?}",
        );

        let _third = s.next().await.unwrap().unwrap();
        let after_third = tokio::time::Instant::now() - start;
        assert!(
            after_third >= Duration::from_millis(150),
            "third after at least 150 ms, got {after_third:?}",
        );

        assert!(s.next().await.is_none());
    }

    #[tokio::test]
    async fn realtime_out_of_order_emits_without_delay() {
        let inner = IteratorSource::new(vec![msg(1_000_000), msg(500_000)]);
        let mut s = paced(inner, Pacing::Realtime);
        let start = StdInstant::now();
        let _ = s.next().await.unwrap();
        let _ = s.next().await.unwrap();
        let elapsed = start.elapsed();
        assert!(
            elapsed < Duration::from_millis(50),
            "out-of-order must not sleep, got {elapsed:?}",
        );
    }

    #[test]
    fn paced_source_is_message_source() {
        let inner = IteratorSource::new(Vec::<Message>::new());
        let s = paced(inner, Pacing::MaxSpeed);
        let _: &dyn MessageSource = &s;
    }
}
