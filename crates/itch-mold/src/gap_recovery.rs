//! Gap-recovery client.
//!
//! Wraps a [`MoldStream`] with a [`RequestClient`] pool that issues
//! retransmission requests automatically when the receiver
//! observes a gap. Per ADR-0010, the user gets a clean
//! `Stream<Item = MoldEvent>` with the gap holes filled in
//! transparently — `MoldEvent::Gap` events still surface (for
//! observability) but consecutive `MoldEvent::Message`s flow as if
//! no loss had occurred.
//!
//! # Failover
//!
//! `GapRecoveryConfig::request_servers` is an ordered list. The
//! client tries them in order; on a TCP error or a `STATUS_HOLE`
//! that has been retried `max_retries_per_server` times, it
//! advances to the next server. After every server has been
//! exhausted, the gap is given up on and the receiver state
//! advances to the next-known sequence (a tracing warn is emitted
//! and the missed messages are reported via
//! `MoldError::RequestTimeout`).
//!
//! # Timeouts
//!
//! - `request_timeout` (default 5 s): caps each individual TCP
//!   request round trip.
//! - `gap_timeout` (default 60 s): total wall-clock budget for
//!   recovering one gap. After this the gap is given up and a
//!   resync happens (next-expected jumps past the gap).
//!
//! # Backpressure
//!
//! The recovery client never blocks the multicast read loop. If
//! the request server is slow, the receiver continues to ingest
//! multicast packets normally; the recovered frames are fed back
//! into the `MoldStream` pending buffer and surface in order with
//! whatever subsequent multicast traffic also lands in the buffer.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::Stream;

use crate::error::MoldError;
use crate::event::MoldEvent;
use crate::receiver::MoldStream;
use crate::request_server::{
    CachedFrame, RequestClient, RequestResponse, STATUS_END_OF_SESSION, STATUS_HOLE, STATUS_OK,
};

/// Default per-request timeout.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

/// Default total budget per gap.
pub const DEFAULT_GAP_TIMEOUT: Duration = Duration::from_secs(60);

/// Default initial backoff between retries.
pub const DEFAULT_BACKOFF_MIN: Duration = Duration::from_millis(50);

/// Default maximum backoff between retries.
pub const DEFAULT_BACKOFF_MAX: Duration = Duration::from_secs(2);

/// Default maximum retries against a single server before falling
/// over to the next.
pub const DEFAULT_MAX_RETRIES_PER_SERVER: u32 = 3;

/// Configuration for [`GapRecoveryClient`].
#[derive(Debug, Clone)]
#[must_use]
pub struct GapRecoveryConfig {
    /// Ordered list of request-server addresses to try. Empty
    /// disables recovery (gaps surface as observability events
    /// only and the receiver eventually times them out via the
    /// pending-buffer overflow path).
    pub request_servers: Vec<std::net::SocketAddr>,
    /// Per-request timeout.
    pub request_timeout: Duration,
    /// Total wall-clock budget per gap.
    pub gap_timeout: Duration,
    /// Initial backoff between retries.
    pub backoff_min: Duration,
    /// Maximum backoff between retries.
    pub backoff_max: Duration,
    /// Retries on the same server before failover.
    pub max_retries_per_server: u32,
}

impl Default for GapRecoveryConfig {
    fn default() -> Self {
        Self {
            request_servers: Vec::new(),
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            gap_timeout: DEFAULT_GAP_TIMEOUT,
            backoff_min: DEFAULT_BACKOFF_MIN,
            backoff_max: DEFAULT_BACKOFF_MAX,
            max_retries_per_server: DEFAULT_MAX_RETRIES_PER_SERVER,
        }
    }
}

impl GapRecoveryConfig {
    /// Construct a config bound to one or more request servers.
    pub fn new(request_servers: Vec<std::net::SocketAddr>) -> Self {
        Self {
            request_servers,
            ..Self::default()
        }
    }

    /// Override the per-request timeout.
    #[inline]
    pub fn with_request_timeout(mut self, t: Duration) -> Self {
        self.request_timeout = t;
        self
    }

    /// Override the per-gap timeout.
    #[inline]
    pub fn with_gap_timeout(mut self, t: Duration) -> Self {
        self.gap_timeout = t;
        self
    }
}

/// Outcome of one gap-recovery attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GapRecoveryOutcome {
    /// All requested frames were retransmitted.
    Filled {
        /// First sequence covered.
        from: u64,
        /// Last sequence covered (inclusive).
        to: u64,
    },
    /// Server signalled end-of-session for this range.
    EndOfSession {
        /// First sequence requested.
        from: u64,
    },
    /// Every server has been tried and failed; the recovery client
    /// gave up. The receiver advances past the gap.
    GivenUp {
        /// First sequence requested.
        from: u64,
        /// Last sequence requested (inclusive).
        to: u64,
        /// The last error we saw.
        last_error: String,
    },
}

/// Client that wraps a [`MoldStream`] with automatic gap recovery.
///
/// Wire it up with the same [`GapRecoveryConfig::request_servers`]
/// values as the publisher's [`crate::MoldRequestServer`].
pub struct GapRecoveryClient {
    stream: MoldStream,
    config: GapRecoveryConfig,
    /// Active recovery task (one at a time per gap). Result is
    /// flushed back into `stream` when complete.
    active: Option<RecoveryTask>,
}

struct RecoveryTask {
    handle: tokio::task::JoinHandle<RecoveryResult>,
    #[allow(dead_code)]
    from: u64,
    #[allow(dead_code)]
    to: u64,
}

#[derive(Debug)]
struct RecoveryResult {
    from: u64,
    to: u64,
    outcome: Result<RecoveryFrames, MoldError>,
}

#[derive(Debug)]
struct RecoveryFrames {
    frames: Vec<CachedFrame>,
    eos: bool,
}

impl GapRecoveryClient {
    /// Wrap a [`MoldStream`] with a recovery client.
    pub fn new(stream: MoldStream, config: GapRecoveryConfig) -> Self {
        Self {
            stream,
            config,
            active: None,
        }
    }

    /// Borrow the underlying stream (read-only).
    #[must_use]
    pub fn stream(&self) -> &MoldStream {
        &self.stream
    }

    /// Mutable access to the underlying stream. Use sparingly —
    /// most users should drive the recovery client via its own
    /// `Stream` impl.
    pub fn stream_mut(&mut self) -> &mut MoldStream {
        &mut self.stream
    }

    /// Spawn the recovery task for a gap.
    fn spawn_recovery(&mut self, from: u64, to: u64) {
        let cfg = self.config.clone();
        let handle = tokio::spawn(async move {
            let outcome = recover_gap(&cfg, from, to).await;
            RecoveryResult { from, to, outcome }
        });
        self.active = Some(RecoveryTask { handle, from, to });
    }
}

async fn recover_gap(
    cfg: &GapRecoveryConfig,
    from: u64,
    to: u64,
) -> Result<RecoveryFrames, MoldError> {
    if cfg.request_servers.is_empty() {
        return Err(MoldError::RequestTimeout {
            server: "0.0.0.0:0".parse().expect("trivial"),
            seq: from,
        });
    }
    let count = (to - from + 1).min(u32::MAX as u64) as u32;

    let deadline = tokio::time::Instant::now() + cfg.gap_timeout;
    let mut last_err: Option<MoldError> = None;

    for &server in &cfg.request_servers {
        let mut backoff = cfg.backoff_min;
        for attempt in 0..cfg.max_retries_per_server.max(1) {
            if tokio::time::Instant::now() >= deadline {
                return Err(MoldError::RequestTimeout { server, seq: from });
            }
            let attempt_budget = deadline.saturating_duration_since(tokio::time::Instant::now());
            let per_request_budget = attempt_budget.min(cfg.request_timeout);

            match try_one_server(server, from, count, per_request_budget).await {
                Ok(resp) => match resp.status {
                    STATUS_OK | STATUS_HOLE if !resp.frames.is_empty() => {
                        return Ok(RecoveryFrames {
                            frames: resp.frames,
                            eos: false,
                        });
                    }
                    STATUS_END_OF_SESSION => {
                        return Ok(RecoveryFrames {
                            frames: resp.frames,
                            eos: true,
                        });
                    }
                    _ => {
                        // Empty hole — server doesn't have it (yet); retry.
                        last_err = Some(MoldError::RequestTimeout { server, seq: from });
                    }
                },
                Err(err) => {
                    tracing::warn!(?server, attempt, ?err, "gap recovery attempt failed");
                    last_err = Some(err);
                }
            }

            // Exponential backoff with cap.
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(cfg.backoff_max);
        }
        // Exhausted retries on this server; failover.
    }
    Err(last_err.unwrap_or(MoldError::RequestTimeout {
        server: cfg.request_servers[0],
        seq: from,
    }))
}

async fn try_one_server(
    addr: std::net::SocketAddr,
    from: u64,
    count: u32,
    budget: Duration,
) -> Result<RequestResponse, MoldError> {
    let fut = async {
        let mut client = RequestClient::connect(addr).await?;
        let resp = client.request(from, count).await?;
        let _ = client.close().await;
        Ok::<_, MoldError>(resp)
    };
    match tokio::time::timeout(budget, fut).await {
        Ok(res) => res,
        Err(_) => Err(MoldError::RequestTimeout {
            server: addr,
            seq: from,
        }),
    }
}

impl Stream for GapRecoveryClient {
    type Item = Result<MoldEvent, MoldError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // 1. Drain any completed recovery task into the stream.
        if let Some(task) = self.active.as_mut() {
            let pinned = Pin::new(&mut task.handle);
            match pinned.poll(cx) {
                Poll::Ready(Ok(result)) => {
                    self.active = None;
                    match result.outcome {
                        Ok(rf) => {
                            tracing::info!(
                                from = result.from,
                                to = result.to,
                                count = rf.frames.len(),
                                eos = rf.eos,
                                "gap recovery succeeded"
                            );
                            self.stream.ingest_retransmit(&rf.frames);
                            // Note: if eos, the publisher has stopped;
                            // multicast EOS will surface separately.
                        }
                        Err(err) => {
                            tracing::warn!(
                                from = result.from,
                                to = result.to,
                                ?err,
                                "gap recovery gave up; advancing past gap"
                            );
                            return Poll::Ready(Some(Err(err)));
                        }
                    }
                }
                Poll::Ready(Err(join_err)) => {
                    self.active = None;
                    tracing::error!(?join_err, "gap recovery task panicked");
                }
                Poll::Pending => {
                    // Continue to poll the underlying stream so we
                    // don't stall on the recovery task — multicast
                    // packets keep flowing.
                }
            }
        }

        // 2. Poll the underlying stream.
        let pinned = Pin::new(&mut self.stream);
        match pinned.poll_next(cx) {
            Poll::Ready(Some(Ok(MoldEvent::Gap { from, to }))) => {
                if !self.config.request_servers.is_empty() && self.active.is_none() {
                    tracing::info!(from, to, "gap detected; spawning recovery");
                    self.spawn_recovery(from, to);
                }
                // Surface the gap event for observability.
                Poll::Ready(Some(Ok(MoldEvent::Gap { from, to })))
            }
            other => other,
        }
    }
}

// `JoinHandle` is a `Future` — we need it explicitly for `Pin::new`.
use std::future::Future;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{session_from_str, MessageBlock};
    use crate::receiver::MoldConfig;
    use crate::request_server::MoldRequestServer;
    use crate::{MoldEvent, MoldPacket};
    use futures::StreamExt;
    use itch_protocol::Message;
    use itch_protocol::{
        AddOrder, Header, OrderReference, Price4, Shares, Side, Stock, StockLocate, Timestamp,
        TrackingNumber,
    };

    fn fixture_session() -> [u8; 10] {
        session_from_str("ITCH50").unwrap()
    }

    fn add_order(seq_marker: u64) -> Message {
        Message::AddOrder(AddOrder {
            header: Header {
                stock_locate: StockLocate::from_u16((seq_marker & 0xFFFF) as u16),
                tracking_number: TrackingNumber::from_u16(0),
                timestamp: Timestamp::from_u64(seq_marker),
            },
            order_ref: OrderReference::from_u64(seq_marker + 1000),
            side: Side::Buy,
            shares: Shares::from_u32(100),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_000_000),
        })
    }

    fn encode(msg: &Message) -> Vec<u8> {
        let total = msg.encoded_len();
        let mut buf = vec![0u8; total];
        msg.encode(&mut buf).expect("encode");
        buf
    }

    fn data_packet(session: [u8; 10], first_seq: u64, msgs: &[Message]) -> MoldPacket {
        let blocks: Vec<_> = msgs
            .iter()
            .map(|m| MessageBlock::new(encode(m)).unwrap())
            .collect();
        MoldPacket::new_data(session, first_seq, blocks).unwrap()
    }

    /// Happy path: gap detected, request server holds the missing
    /// frames, recovery client refills the receiver, in-order
    /// delivery resumes.
    #[tokio::test]
    async fn happy_path_gap_filled_via_request_server() {
        // Pre-populate the request server with frames 1..=5.
        let server = MoldRequestServer::new(64);
        for s in 1u64..=5 {
            server
                .cache_frame(CachedFrame {
                    sequence: s,
                    body: encode(&add_order(s)),
                })
                .await;
        }
        let (addr, server_handle) = server.bind("127.0.0.1:0").await.expect("bind server");

        // Receiver locked to the same session, starting at seq 1.
        let cfg = MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(1);
        let stream = MoldStream::test_only(cfg);
        let recovery_cfg = GapRecoveryConfig::new(vec![addr])
            .with_request_timeout(Duration::from_millis(500))
            .with_gap_timeout(Duration::from_secs(5));
        let mut client = GapRecoveryClient::new(stream, recovery_cfg);

        // Multicast jumps from the start to seq=4 (a gap of 1..=3).
        client
            .stream_mut()
            .test_ingest(data_packet(fixture_session(), 4, &[add_order(4)]));

        // We expect: Gap event, then the recovered messages 1, 2, 3, 4.
        let evt = client.next().await.unwrap().unwrap();
        assert!(matches!(evt, MoldEvent::Gap { from: 1, to: 3 }));

        let mut delivered = Vec::new();
        // Drain up to 4 messages.
        for _ in 0..4 {
            match tokio::time::timeout(Duration::from_secs(2), client.next()).await {
                Ok(Some(Ok(MoldEvent::Message { sequence, .. }))) => {
                    delivered.push(sequence);
                }
                Ok(other) => panic!("unexpected: {other:?}"),
                Err(_) => panic!("timeout waiting for recovery"),
            }
        }
        assert_eq!(delivered, vec![1, 2, 3, 4]);
        assert_eq!(client.stream().next_expected_sequence(), 5);

        server_handle.abort();
    }

    /// All servers fail: the recovery client gives up and surfaces
    /// the error. The stream is **not** poisoned.
    #[tokio::test]
    async fn all_servers_fail_gives_up_with_error() {
        let cfg = MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(1);
        let stream = MoldStream::test_only(cfg);

        // Bind a TCP listener and immediately drop it so the port is
        // closed; subsequent connects either ECONNREFUSED quickly or
        // time out. Either way the recovery client must surface an
        // error rather than block.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let bad = listener.local_addr().expect("local_addr");
        drop(listener);

        let recovery_cfg = GapRecoveryConfig::new(vec![bad])
            .with_request_timeout(Duration::from_millis(50))
            .with_gap_timeout(Duration::from_millis(200));
        let mut client = GapRecoveryClient::new(stream, recovery_cfg);

        // Trigger a gap.
        client
            .stream_mut()
            .test_ingest(data_packet(fixture_session(), 4, &[add_order(4)]));

        // First event is the Gap.
        let evt = client.next().await.unwrap().unwrap();
        assert!(matches!(evt, MoldEvent::Gap { from: 1, to: 3 }));

        // Eventually we should see *some* error from the recovery
        // exhaustion path (Io / RequestTimeout). The exact variant
        // depends on platform timing.
        let mut got_err = false;
        for _ in 0..20 {
            match tokio::time::timeout(Duration::from_millis(500), client.next()).await {
                Ok(Some(Err(_))) => {
                    got_err = true;
                    break;
                }
                Ok(Some(Ok(_))) => continue,
                Ok(None) => break,
                Err(_) => break,
            }
        }
        assert!(got_err, "expected an error from exhausted recovery");
    }

    /// Failover: first server is unreachable, second server has
    /// the data — recovery succeeds via the second.
    #[tokio::test]
    async fn failover_to_second_server() {
        let server2 = MoldRequestServer::new(64);
        for s in 1u64..=3 {
            server2
                .cache_frame(CachedFrame {
                    sequence: s,
                    body: encode(&add_order(s)),
                })
                .await;
        }
        let (addr2, h2) = server2.bind("127.0.0.1:0").await.expect("bind server2");
        let bad: std::net::SocketAddr = "127.0.0.1:1".parse().unwrap();

        let cfg = MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(1);
        let stream = MoldStream::test_only(cfg);
        let recovery_cfg = GapRecoveryConfig::new(vec![bad, addr2])
            .with_request_timeout(Duration::from_millis(100))
            .with_gap_timeout(Duration::from_secs(5));
        let mut client = GapRecoveryClient::new(stream, recovery_cfg);

        client
            .stream_mut()
            .test_ingest(data_packet(fixture_session(), 4, &[add_order(4)]));

        // Drain Gap + 4 messages.
        let _gap = client.next().await.unwrap().unwrap();
        let mut delivered = Vec::new();
        for _ in 0..4 {
            match tokio::time::timeout(Duration::from_secs(3), client.next()).await {
                Ok(Some(Ok(MoldEvent::Message { sequence, .. }))) => delivered.push(sequence),
                Ok(other) => panic!("unexpected: {other:?}"),
                Err(_) => panic!("timeout"),
            }
        }
        assert_eq!(delivered, vec![1, 2, 3, 4]);

        h2.abort();
    }
}
