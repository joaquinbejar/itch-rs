//! `MoldStream` — MoldUDP64 V1.00 multicast receiver.
//!
//! A receiver opens a UDP socket bound to the multicast group, joins
//! the group on the given local interface, and yields events
//! decoded from incoming datagrams. Per ADR-0010, the receiver
//! itself is a `Stream<Item = MoldResult<MoldEvent>>`; the
//! gap-recovery client (issues #22, #23, #24) wraps it.
//!
//! # Heartbeat / silent-link detection
//!
//! - Soft warning at `silence_warning` (default 1 s): emits a
//!   `tracing::warn!` and (for tests) makes
//!   [`MoldStream::silent_warned`] return `true`.
//! - Hard timeout at `silence_dead_link` (default 15 s): the
//!   stream yields `Err(MoldError::PeerSilent)` once and resumes
//!   normal operation as soon as the next packet arrives. The
//!   error does **not** poison the stream; callers can decide
//!   whether to propagate or shut down.
//!
//! # Bounded buffering
//!
//! Out-of-order blocks (seq > expected) are buffered in a bounded
//! `BTreeMap<u64, Bytes>` capped at `max_pending_messages`. When
//! the cap is exceeded, the oldest pending entry is dropped and a
//! tracing warning is emitted. Issue #22 turns the bound into a
//! hard error and #24 wires up retransmission; in #21 we land the
//! foundation only.

use std::collections::BTreeMap;
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::Bytes;
use futures::Stream;
use itch_protocol::Message;
use tokio::net::UdpSocket;
use tokio::time::{Instant, Sleep};

use crate::codec::{MoldPacket, MAX_BLOCK_LEN};
use crate::error::MoldError;
use crate::event::MoldEvent;

/// Default soft silence warning threshold.
pub const DEFAULT_SILENCE_WARNING: Duration = Duration::from_secs(1);

/// Default hard dead-link silence threshold.
pub const DEFAULT_SILENCE_DEAD_LINK: Duration = Duration::from_secs(15);

/// Default maximum pending out-of-order blocks held while waiting
/// for gap recovery. Issue #22 turns overflow into a hard error;
/// for now we drop oldest with a warning.
pub const DEFAULT_MAX_PENDING: usize = 10_000;

/// Maximum UDP datagram the receiver will accept. 2 KiB is enough
/// for any well-formed MoldUDP64 packet (header 20 B + many small
/// blocks fitting under the 1500 B Ethernet MTU); anything larger
/// is treated as a framing error.
pub const RECV_BUFFER_LEN: usize = 2048;

/// Configuration for [`MoldStream`].
#[derive(Debug, Clone)]
#[must_use]
pub struct MoldConfig {
    /// Multicast group + UDP port to join.
    pub multicast_group: SocketAddr,
    /// Local IPv4 interface to join the multicast group on. `None`
    /// means "default" (`0.0.0.0`). For loopback testing use
    /// `127.0.0.1`.
    pub interface: Option<std::net::Ipv4Addr>,
    /// Re-request servers (unicast UDP). Populated by issue #24.
    pub request_servers: Vec<SocketAddr>,
    /// Pinned session id. `None` means "lock onto whatever the
    /// first packet announces".
    pub session: Option<[u8; 10]>,
    /// Initial value of the next-expected sequence number. `0`
    /// means "lock onto the first packet's sequence".
    pub start_sequence: u64,
    /// Soft silence warning threshold.
    pub silence_warning: Duration,
    /// Hard dead-link silence threshold.
    pub silence_dead_link: Duration,
    /// Cap on the pending out-of-order buffer.
    pub max_pending_messages: usize,
}

impl Default for MoldConfig {
    fn default() -> Self {
        Self {
            multicast_group: SocketAddr::from(([233, 252, 0, 1], 5000)),
            interface: None,
            request_servers: Vec::new(),
            session: None,
            start_sequence: 0,
            silence_warning: DEFAULT_SILENCE_WARNING,
            silence_dead_link: DEFAULT_SILENCE_DEAD_LINK,
            max_pending_messages: DEFAULT_MAX_PENDING,
        }
    }
}

impl MoldConfig {
    /// Construct a fresh config bound to the given multicast group.
    pub fn new(multicast_group: SocketAddr) -> Self {
        Self {
            multicast_group,
            ..Self::default()
        }
    }

    /// Lock onto a specific session id.
    #[inline]
    pub fn with_session(mut self, session: [u8; 10]) -> Self {
        self.session = Some(session);
        self
    }

    /// Override the silence thresholds.
    #[inline]
    pub fn with_silence(mut self, warning: Duration, dead_link: Duration) -> Self {
        self.silence_warning = warning;
        self.silence_dead_link = dead_link;
        self
    }

    /// Override the start sequence.
    #[inline]
    pub fn with_start_sequence(mut self, seq: u64) -> Self {
        self.start_sequence = seq;
        self
    }

    /// Override the pending-buffer cap.
    #[inline]
    pub fn with_max_pending(mut self, n: usize) -> Self {
        self.max_pending_messages = n;
        self
    }
}

/// Internal receiver state shared between the async UDP path and
/// the test-driven packet path. Pure logic — no socket access.
#[derive(Debug)]
pub(crate) struct ReceiverState {
    /// Pinned (or learned) session id.
    pub(crate) session: Option<[u8; 10]>,
    /// Next sequence number we expect to deliver to the caller. If
    /// the stream has not seen any packet yet and `start_sequence`
    /// was `0`, this is `0`; the first packet "locks in" the
    /// expected sequence to its own first block.
    pub(crate) next_expected: u64,
    /// Whether we have observed at least one packet.
    pub(crate) bootstrapped: bool,
    /// Out-of-order blocks waiting to be delivered, keyed by
    /// sequence.
    pub(crate) pending: BTreeMap<u64, Bytes>,
    /// Pending events queued for the next `poll_next`.
    pub(crate) outbox: std::collections::VecDeque<Result<MoldEvent, MoldError>>,
    /// Whether the EOS packet has been observed.
    pub(crate) eos: bool,
    /// Cap on the pending buffer.
    pub(crate) max_pending: usize,
    /// Configured `start_sequence`.
    pub(crate) start_sequence: u64,
}

impl ReceiverState {
    pub(crate) fn new(cfg: &MoldConfig) -> Self {
        Self {
            session: cfg.session,
            next_expected: cfg.start_sequence,
            bootstrapped: false,
            pending: BTreeMap::new(),
            outbox: std::collections::VecDeque::new(),
            eos: false,
            max_pending: cfg.max_pending_messages,
            start_sequence: cfg.start_sequence,
        }
    }

    /// Ingest one decoded packet, queuing zero or more events on
    /// the outbox.
    pub(crate) fn ingest(&mut self, pkt: MoldPacket) {
        // Session check / lock.
        match self.session {
            Some(expected) if expected != pkt.header.session => {
                self.outbox.push_back(Err(MoldError::SessionMismatch {
                    expected,
                    got: pkt.header.session,
                }));
                return;
            }
            None => {
                tracing::info!(session = ?pkt.header.session, "mold session locked");
                self.session = Some(pkt.header.session);
            }
            _ => {}
        }

        // First-packet bootstrap of the expected-sequence cursor.
        if !self.bootstrapped {
            if self.start_sequence == 0 {
                // Lock on to whatever the first packet announces.
                self.next_expected = pkt.header.sequence;
            }
            self.bootstrapped = true;
        }

        // Heartbeat / EOS: the header sequence is the publisher's
        // next-expected. Emit the corresponding event and return.
        if pkt.header.is_heartbeat() {
            self.outbox.push_back(Ok(MoldEvent::Heartbeat {
                next_seq: pkt.header.sequence,
            }));
            return;
        }
        if pkt.header.is_end_of_session() {
            self.eos = true;
            self.outbox.push_back(Ok(MoldEvent::EndOfSession {
                next_seq: pkt.header.sequence,
            }));
            return;
        }

        // Sequenced data.
        let first_seq = pkt.header.sequence;
        let count = pkt.blocks.len() as u64;

        // Late retransmission (entire packet older than expected): drop.
        if first_seq + count <= self.next_expected {
            tracing::debug!(
                first_seq,
                count,
                next_expected = self.next_expected,
                "dropping fully-retransmitted packet"
            );
            return;
        }

        // Gap: emit observability event + stash future blocks.
        if first_seq > self.next_expected {
            let from = self.next_expected;
            let to = first_seq - 1;
            tracing::warn!(from, to, "gap detected");
            self.outbox.push_back(Ok(MoldEvent::Gap { from, to }));
            for (i, block) in pkt.blocks.into_iter().enumerate() {
                let seq = first_seq + i as u64;
                self.stash_pending(seq, Bytes::from(block.data));
            }
            return;
        }

        // first_seq <= next_expected: deliver consecutive blocks
        // starting from next_expected, then drain pending.
        for (i, block) in pkt.blocks.into_iter().enumerate() {
            let seq = first_seq + i as u64;
            if seq < self.next_expected {
                // Already delivered (partial retransmission).
                continue;
            }
            self.deliver(seq, &block.data);
        }
        self.flush_pending();
    }

    /// Decode + emit an in-order block.
    fn deliver(&mut self, seq: u64, body: &[u8]) {
        debug_assert_eq!(seq, self.next_expected);
        if body.len() > MAX_BLOCK_LEN {
            self.outbox.push_back(Err(MoldError::BlockTooLarge {
                got: body.len(),
                max: MAX_BLOCK_LEN,
            }));
            return;
        }
        match Message::decode(body) {
            Ok(message) => {
                self.outbox.push_back(Ok(MoldEvent::Message {
                    sequence: seq,
                    message,
                }));
                self.next_expected = seq + 1;
            }
            Err(err) => {
                tracing::warn!(seq, ?err, "bad inner ITCH frame; dropping block");
                self.outbox.push_back(Err(err.into()));
                // Advance regardless so we don't deadlock on a poison block.
                self.next_expected = seq + 1;
            }
        }
    }

    fn stash_pending(&mut self, seq: u64, data: Bytes) {
        if self.pending.contains_key(&seq) {
            return;
        }
        if self.pending.len() >= self.max_pending {
            // Drop oldest, emit warning. Issue #22 turns this into
            // a hard PendingBufferFull error.
            if let Some((old_seq, _)) = self.pending.pop_first() {
                tracing::warn!(
                    dropped_seq = old_seq,
                    "pending buffer full; dropping oldest entry"
                );
            }
        }
        self.pending.insert(seq, data);
    }

    fn flush_pending(&mut self) {
        while let Some((&seq, _)) = self.pending.iter().next() {
            if seq != self.next_expected {
                break;
            }
            // Safe: just peeked.
            let body = self.pending.remove(&seq).expect("just peeked");
            self.deliver(seq, &body);
        }
    }
}

/// Driver-agnostic packet ingestion. The async UDP receiver and the
/// in-process tests share the state machine via [`ReceiverState`];
/// this struct adds the silence timer and the `Stream` impl.
pub struct MoldStream {
    state: ReceiverState,
    socket: Option<UdpSocket>,
    cfg: MoldConfig,
    /// Last instant we received a packet.
    last_seen: Instant,
    /// Soft warning emitted? Reset on each packet.
    soft_warned: bool,
    /// Sleep that drives the silence-timer wheel. Reset on every
    /// packet, on every soft-warning emission, and on every
    /// dead-link emission to the next deadline.
    silence_timer: Pin<Box<Sleep>>,
    /// Set once silence > dead-link. Reset on next packet so the
    /// stream does not loop on the same error.
    dead_link_pending: bool,
    /// Final stream-end flag — set when EOS event has been
    /// delivered.
    finished: bool,
    /// Receive buffer reused across reads.
    buf: Vec<u8>,
}

impl std::fmt::Debug for MoldStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MoldStream")
            .field("session", &self.state.session)
            .field("next_expected", &self.state.next_expected)
            .field("pending", &self.state.pending.len())
            .field("eos", &self.state.eos)
            .field("finished", &self.finished)
            .finish()
    }
}

impl MoldStream {
    /// Open a UDP socket on the multicast group address and join
    /// the group. Returns a ready-to-poll [`MoldStream`].
    ///
    /// Per ADR-0010, the request-server client is configured here
    /// but inactive in #21; gap recovery comes online in #24.
    ///
    /// # Errors
    ///
    /// - I/O errors from `UdpSocket::bind`, `set_reuse_address`,
    ///   `join_multicast_v4`.
    pub async fn join(cfg: MoldConfig) -> Result<Self, MoldError> {
        let bind_addr: SocketAddr = match cfg.multicast_group {
            SocketAddr::V4(v4) => SocketAddr::from(([0u8, 0, 0, 0], v4.port())),
            SocketAddr::V6(_) => {
                return Err(MoldError::Io(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "ipv6 multicast not yet supported",
                )))
            }
        };
        let socket = UdpSocket::bind(bind_addr).await?;
        // Best-effort; many platforms require this for shared-port multicast.
        let _ = socket.set_broadcast(true);

        if let SocketAddr::V4(v4) = cfg.multicast_group {
            let iface = cfg.interface.unwrap_or(std::net::Ipv4Addr::UNSPECIFIED);
            socket.join_multicast_v4(*v4.ip(), iface)?;
            tracing::info!(group = %v4.ip(), iface = %iface, port = v4.port(), "mold multicast group joined");
        }

        Ok(Self::from_socket(socket, cfg))
    }

    /// Build a [`MoldStream`] from an already-bound `UdpSocket`.
    /// Used by the integration tests and by callers that want to
    /// configure the socket themselves (e.g. SO_REUSEPORT on
    /// Linux).
    #[must_use]
    pub fn from_socket(socket: UdpSocket, cfg: MoldConfig) -> Self {
        let now = Instant::now();
        let warn = cfg.silence_warning;
        Self {
            state: ReceiverState::new(&cfg),
            socket: Some(socket),
            cfg,
            last_seen: now,
            soft_warned: false,
            silence_timer: Box::pin(tokio::time::sleep(warn)),
            dead_link_pending: false,
            finished: false,
            buf: vec![0u8; RECV_BUFFER_LEN],
        }
    }

    /// Build a packet-driven stream for tests. The state machine is
    /// identical to the live socket path; the test harness pushes
    /// pre-decoded `MoldPacket`s via [`Self::test_ingest`].
    #[doc(hidden)]
    #[must_use]
    pub fn test_only(cfg: MoldConfig) -> Self {
        let warn = cfg.silence_warning;
        Self {
            state: ReceiverState::new(&cfg),
            socket: None,
            cfg,
            last_seen: Instant::now(),
            soft_warned: false,
            silence_timer: Box::pin(tokio::time::sleep(warn)),
            dead_link_pending: false,
            finished: false,
            buf: vec![0u8; RECV_BUFFER_LEN],
        }
    }

    /// Push one decoded packet through the state machine. Tests
    /// use this to avoid a real UDP socket.
    #[doc(hidden)]
    pub fn test_ingest(&mut self, pkt: MoldPacket) {
        self.state.ingest(pkt);
        self.touch_packet();
    }

    /// Returns the current next-expected sequence number.
    #[must_use]
    pub fn next_expected_sequence(&self) -> u64 {
        self.state.next_expected
    }

    /// Returns the locked session id, or `None` if no packet has
    /// been observed yet.
    #[must_use]
    pub fn current_session(&self) -> Option<&[u8; 10]> {
        self.state.session.as_ref()
    }

    /// Number of out-of-order blocks pending.
    #[must_use]
    pub fn pending_count(&self) -> usize {
        self.state.pending.len()
    }

    /// `true` iff the soft silence-warning threshold has been
    /// reached at least once since the last packet.
    #[must_use]
    pub fn silent_warned(&self) -> bool {
        self.soft_warned
    }

    /// Reset the silence timer to "just received a packet".
    fn touch_packet(&mut self) {
        self.last_seen = Instant::now();
        self.soft_warned = false;
        self.dead_link_pending = false;
        let warn = self.cfg.silence_warning;
        self.silence_timer.as_mut().reset(self.last_seen + warn);
    }

    /// Re-arm the silence timer to fire at the next interesting
    /// deadline given current state.
    fn rearm_silence_timer(&mut self) {
        let target = if !self.soft_warned {
            self.last_seen + self.cfg.silence_warning
        } else {
            self.last_seen + self.cfg.silence_dead_link
        };
        self.silence_timer.as_mut().reset(target);
    }

    fn check_silence(&mut self) -> Option<MoldError> {
        let elapsed = self.last_seen.elapsed();
        if !self.soft_warned && elapsed >= self.cfg.silence_warning {
            tracing::warn!(?elapsed, "mold receiver silence warning");
            self.soft_warned = true;
        }
        if elapsed >= self.cfg.silence_dead_link && !self.dead_link_pending {
            self.dead_link_pending = true;
            return Some(MoldError::PeerSilent(elapsed));
        }
        None
    }
}

impl Stream for MoldStream {
    type Item = Result<MoldEvent, MoldError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        // Drain outbox first.
        if let Some(event) = self.state.outbox.pop_front() {
            // EOS terminates the stream after delivery.
            if matches!(event, Ok(MoldEvent::EndOfSession { .. })) {
                self.finished = true;
            }
            return Poll::Ready(Some(event));
        }

        // No buffered events. Read more datagrams or check silence.
        loop {
            // Try to receive a datagram (only if we have a socket).
            if let Some(sock) = self.socket.as_ref() {
                let mut tmp = [0u8; RECV_BUFFER_LEN];
                let mut buf = tokio::io::ReadBuf::new(&mut tmp);
                match sock.poll_recv_from(cx, &mut buf) {
                    Poll::Ready(Ok(_peer)) => {
                        let n = buf.filled().len();
                        // Capture into `self.buf` so we can decode without
                        // holding the borrow on `tmp`.
                        let bytes = &tmp[..n];
                        self.buf.clear();
                        self.buf.extend_from_slice(bytes);
                        self.touch_packet();
                        match MoldPacket::decode(&self.buf) {
                            Ok(pkt) => self.state.ingest(pkt),
                            Err(err) => {
                                tracing::warn!(?err, n, "bad mold packet; dropping datagram");
                                // Don't poison the stream — surface the error and continue.
                                return Poll::Ready(Some(Err(err)));
                            }
                        }
                        if let Some(event) = self.state.outbox.pop_front() {
                            if matches!(event, Ok(MoldEvent::EndOfSession { .. })) {
                                self.finished = true;
                            }
                            return Poll::Ready(Some(event));
                        }
                        // No event surfaced (e.g. fully-retransmitted late packet);
                        // try another recv.
                        continue;
                    }
                    Poll::Ready(Err(err)) => {
                        return Poll::Ready(Some(Err(err.into())));
                    }
                    Poll::Pending => {
                        // Fall through to silence-timer check.
                    }
                }
            }

            // Silence-timer wheel.
            match self.silence_timer.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    if let Some(err) = self.check_silence() {
                        // Re-arm to the next dead-link deadline so
                        // we don't loop hot on the same error.
                        let next = Instant::now() + self.cfg.silence_dead_link;
                        self.silence_timer.as_mut().reset(next);
                        return Poll::Ready(Some(Err(err)));
                    }
                    // Soft-warning case: re-arm on the dead-link
                    // deadline (next.check_silence will fire it).
                    self.rearm_silence_timer();
                    // Continue polling so we either see a new
                    // datagram or correctly register the new sleep
                    // wakeup with the runtime.
                }
                Poll::Pending => {
                    return Poll::Pending;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::session_from_str;
    use futures::StreamExt;
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
            .map(|m| crate::codec::MessageBlock::new(encode(m)).unwrap())
            .collect();
        MoldPacket::new_data(session, first_seq, blocks).unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn in_order_messages_are_delivered() {
        let cfg = MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(1);
        let mut stream = MoldStream::test_only(cfg);

        // Publisher sends packet [seq=1, count=3]
        let m1 = add_order(1);
        let m2 = add_order(2);
        let m3 = add_order(3);
        stream.test_ingest(data_packet(fixture_session(), 1, &[m1, m2, m3]));

        match stream.next().await.unwrap().unwrap() {
            MoldEvent::Message { sequence, message } => {
                assert_eq!(sequence, 1);
                assert_eq!(message, m1);
            }
            other => panic!("unexpected event: {other:?}"),
        }
        match stream.next().await.unwrap().unwrap() {
            MoldEvent::Message { sequence, .. } => assert_eq!(sequence, 2),
            other => panic!("unexpected: {other:?}"),
        }
        match stream.next().await.unwrap().unwrap() {
            MoldEvent::Message { sequence, .. } => assert_eq!(sequence, 3),
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(stream.next_expected_sequence(), 4);
    }

    #[tokio::test(start_paused = true)]
    async fn first_packet_locks_session_and_sequence_when_unset() {
        let cfg = MoldConfig::default(); // session=None, start_sequence=0
        let mut stream = MoldStream::test_only(cfg);

        let m = add_order(42);
        stream.test_ingest(data_packet(
            fixture_session(),
            100,
            std::slice::from_ref(&m),
        ));

        let evt = stream.next().await.unwrap().unwrap();
        match evt {
            MoldEvent::Message { sequence, message } => {
                assert_eq!(sequence, 100);
                assert_eq!(message, m);
            }
            other => panic!("unexpected: {other:?}"),
        }
        assert_eq!(stream.current_session(), Some(&fixture_session()));
        assert_eq!(stream.next_expected_sequence(), 101);
    }

    #[tokio::test(start_paused = true)]
    async fn heartbeat_event_emitted() {
        let cfg = MoldConfig::default().with_session(fixture_session());
        let mut stream = MoldStream::test_only(cfg);

        stream.test_ingest(MoldPacket::heartbeat(fixture_session(), 1));

        let evt = stream.next().await.unwrap().unwrap();
        assert!(matches!(evt, MoldEvent::Heartbeat { next_seq: 1 }));
    }

    #[tokio::test(start_paused = true)]
    async fn end_of_session_terminates_stream() {
        let cfg = MoldConfig::default().with_session(fixture_session());
        let mut stream = MoldStream::test_only(cfg);

        stream.test_ingest(MoldPacket::end_of_session(fixture_session(), 99));

        let evt = stream.next().await.unwrap().unwrap();
        assert!(matches!(evt, MoldEvent::EndOfSession { next_seq: 99 }));

        // Stream is finished.
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn gap_event_emitted_then_pending_flushes_on_resync() {
        let cfg = MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(1);
        let mut stream = MoldStream::test_only(cfg);

        // Packet at seq=1 (good).
        stream.test_ingest(data_packet(fixture_session(), 1, &[add_order(1)]));
        let _ = stream.next().await.unwrap().unwrap();

        // Packet at seq=4 (gap of 2..=3).
        let m4 = add_order(4);
        let m5 = add_order(5);
        stream.test_ingest(data_packet(fixture_session(), 4, &[m4, m5]));

        let gap = stream.next().await.unwrap().unwrap();
        assert!(matches!(gap, MoldEvent::Gap { from: 2, to: 3 }));

        // No further events yet (the gap-filler hasn't arrived).
        // Advance time slightly and try a non-blocking poll via packet ingestion.
        // The pending buffer holds seq 4 and 5.
        assert_eq!(stream.pending_count(), 2);

        // Filler packet at seq=2 with messages 2, 3.
        let m2 = add_order(2);
        let m3 = add_order(3);
        stream.test_ingest(data_packet(fixture_session(), 2, &[m2, m3]));

        // Now we should receive 2, 3, 4, 5 in order.
        for expected in 2..=5 {
            match stream.next().await.unwrap().unwrap() {
                MoldEvent::Message { sequence, .. } => assert_eq!(sequence, expected),
                other => panic!("unexpected: {other:?}"),
            }
        }
        assert_eq!(stream.next_expected_sequence(), 6);
        assert_eq!(stream.pending_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn session_mismatch_emits_error_does_not_poison_stream() {
        let cfg = MoldConfig::default().with_session(fixture_session());
        let mut stream = MoldStream::test_only(cfg);

        // Wrong session id.
        let other = session_from_str("OTHER").unwrap();
        stream.test_ingest(data_packet(other, 1, &[add_order(1)]));
        let evt = stream.next().await.unwrap();
        assert!(matches!(evt, Err(MoldError::SessionMismatch { .. })));

        // Now a valid packet — must still be processed.
        stream.test_ingest(data_packet(fixture_session(), 1, &[add_order(1)]));
        let evt = stream.next().await.unwrap().unwrap();
        assert!(matches!(evt, MoldEvent::Message { sequence: 1, .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn fully_retransmitted_old_packet_is_dropped() {
        let cfg = MoldConfig::default()
            .with_session(fixture_session())
            .with_start_sequence(10);
        let mut stream = MoldStream::test_only(cfg);

        // Bootstrap with a packet at seq 10..=12.
        stream.test_ingest(data_packet(
            fixture_session(),
            10,
            &[add_order(10), add_order(11), add_order(12)],
        ));
        for _ in 0..3 {
            let _ = stream.next().await.unwrap().unwrap();
        }
        assert_eq!(stream.next_expected_sequence(), 13);

        // Old retransmission at seq 5..=8 — fully behind next_expected.
        stream.test_ingest(data_packet(
            fixture_session(),
            5,
            &[add_order(5), add_order(6), add_order(7), add_order(8)],
        ));
        // No outbox entries.
        assert!(stream.state.outbox.is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn silent_dead_link_emits_peer_silent_then_resumes() {
        let cfg = MoldConfig::default()
            .with_session(fixture_session())
            .with_silence(Duration::from_millis(50), Duration::from_millis(200))
            .with_start_sequence(1);
        let mut stream = MoldStream::test_only(cfg);

        // Drive past the dead-link threshold.
        tokio::time::advance(Duration::from_millis(250)).await;
        let evt = stream.next().await.unwrap();
        assert!(matches!(evt, Err(MoldError::PeerSilent(_))));

        // Subsequent packet resumes operation.
        stream.test_ingest(data_packet(fixture_session(), 1, &[add_order(1)]));
        let evt = stream.next().await.unwrap().unwrap();
        assert!(matches!(evt, MoldEvent::Message { sequence: 1, .. }));
    }

    #[tokio::test(start_paused = true)]
    async fn silence_warning_flag_set_then_cleared_on_packet() {
        let cfg = MoldConfig::default()
            .with_session(fixture_session())
            .with_silence(Duration::from_millis(50), Duration::from_secs(15))
            .with_start_sequence(1);
        let mut stream = MoldStream::test_only(cfg);

        // Just past the soft warning threshold; need to drive the
        // poller once to evaluate. Use a short timeout-poll trick.
        tokio::time::advance(Duration::from_millis(60)).await;
        // Run a single poll cycle by selecting on a tiny sleep —
        // the timer wakes us so the silence path fires.
        let next_fut = stream.next();
        let result = tokio::time::timeout(Duration::from_millis(10), next_fut).await;
        // We expect timeout (no event) because we are below the
        // dead-link threshold.
        assert!(result.is_err(), "no event before dead-link");
        assert!(stream.silent_warned());

        // Now a packet arrives.
        stream.test_ingest(data_packet(fixture_session(), 1, &[add_order(1)]));
        let evt = stream.next().await.unwrap().unwrap();
        assert!(matches!(evt, MoldEvent::Message { sequence: 1, .. }));
        assert!(!stream.silent_warned());
    }
}
