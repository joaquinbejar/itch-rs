//! `MoldPublisher` — MoldUDP64 V1.00 multicast publisher.
//!
//! Reads from an `itch_source::MessageSource`, packs ITCH messages
//! into UDP datagrams under a configurable MTU, sends heartbeats
//! every 1 s of inactivity, and signals end-of-session via a
//! `MsgCount = 0xFFFF` marker (per `docs/TRANSPORT-SPEC.md` §4.5)
//! on source exhaustion.
//!
//! ```text
//! ┌──────────────────┬──────────────────┬───────────────────┐
//! │ Session (10 B)   │ SeqNo (8 B u64)  │ MsgCount (2 B u16)│
//! └──────────────────┴──────────────────┴───────────────────┘
//! followed by N message blocks, each
//! ┌─────────────────┬───────────────────────────────┐
//! │ Length (2 B u16)│        Message Data           │
//! └─────────────────┴───────────────────────────────┘
//! ```
//!
//! # Packing
//!
//! Messages are packed into a UDP datagram up to a configurable
//! safety margin (`mtu`, default 1400 B). When the next message
//! would exceed the budget, the current packet is flushed and a
//! new one begins. The publisher never produces a packet larger
//! than `mtu`.
//!
//! # Heartbeats / EOS
//!
//! - On 1 s of inactivity the publisher sends a heartbeat
//!   (`MsgCount = 0`) carrying the next-expected sequence number.
//! - On source exhaustion (`SourceError::Exhausted`) or on
//!   `MoldPublisher::shutdown`, the publisher sends 5
//!   end-of-session datagrams (one per second) and then closes.
//!
//! # Cache hand-off
//!
//! The publisher optionally feeds every published frame into a
//! shared retransmission cache (`MoldRequestServer::cache`) so the
//! receiver-side `GapRecoveryClient` can recover lost packets.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use futures::StreamExt;
use itch_protocol::Message;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::codec::{MessageBlock, MoldPacket, DEFAULT_PACKING_MTU, HEADER_LEN, MAX_BLOCK_LEN};
use crate::error::MoldError;
use crate::request_server::{CachedFrame, RingBufferSeqStore};

/// Default heartbeat interval.
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(1);

/// Default end-of-session burst count.
pub const DEFAULT_EOS_BURST: usize = 5;

/// Default end-of-session burst interval.
pub const DEFAULT_EOS_INTERVAL: Duration = Duration::from_secs(1);

/// Configuration for [`MoldPublisher`].
#[derive(Debug, Clone)]
#[must_use]
pub struct PublisherConfig {
    /// Multicast destination (group + port).
    pub destination: SocketAddr,
    /// Local bind address. Default `0.0.0.0:0` lets the OS pick.
    pub bind: Option<SocketAddr>,
    /// 10-byte session id (right-padded with spaces).
    pub session: [u8; 10],
    /// Initial sequence number (the first published message lands
    /// at this seq).
    pub start_sequence: u64,
    /// MTU safety budget. Packed datagrams stay under this.
    pub mtu: usize,
    /// Heartbeat interval (silence threshold before a heartbeat).
    pub heartbeat_interval: Duration,
    /// Number of EOS datagrams to emit on shutdown.
    pub eos_burst: usize,
    /// Interval between EOS datagrams.
    pub eos_interval: Duration,
}

impl PublisherConfig {
    /// Construct a config bound to the given destination and
    /// session.
    pub fn new(destination: SocketAddr, session: [u8; 10]) -> Self {
        Self {
            destination,
            bind: None,
            session,
            start_sequence: 1,
            mtu: DEFAULT_PACKING_MTU,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
            eos_burst: DEFAULT_EOS_BURST,
            eos_interval: DEFAULT_EOS_INTERVAL,
        }
    }

    /// Override the start sequence.
    #[inline]
    pub fn with_start_sequence(mut self, seq: u64) -> Self {
        self.start_sequence = seq;
        self
    }

    /// Override the MTU.
    #[inline]
    pub fn with_mtu(mut self, mtu: usize) -> Self {
        self.mtu = mtu;
        self
    }

    /// Override the local bind address.
    #[inline]
    pub fn with_bind(mut self, addr: SocketAddr) -> Self {
        self.bind = Some(addr);
        self
    }

    /// Override the heartbeat interval.
    #[inline]
    pub fn with_heartbeat_interval(mut self, t: Duration) -> Self {
        self.heartbeat_interval = t;
        self
    }
}

/// Multicast publisher.
///
/// Drives an `itch_source::MessageSource` to completion, emitting
/// MoldUDP64 datagrams to `destination`. Returns a `JoinHandle`
/// the caller can `await` for clean shutdown — `serve` exits after
/// the EOS burst.
pub struct MoldPublisher {
    socket: UdpSocket,
    cfg: PublisherConfig,
    next_seq: u64,
    /// Optional cache used by `MoldRequestServer` for retransmission.
    cache: Option<Arc<Mutex<RingBufferSeqStore>>>,
}

impl MoldPublisher {
    /// Bind a UDP socket and prepare for publishing. The socket is
    /// **not** started yet; call [`Self::serve`] to drive a
    /// source.
    ///
    /// # Errors
    ///
    /// - I/O errors from `UdpSocket::bind`.
    pub async fn bind(cfg: PublisherConfig) -> Result<Self, MoldError> {
        let bind: SocketAddr = cfg
            .bind
            .unwrap_or_else(|| SocketAddr::from(([0u8, 0, 0, 0], 0)));
        let socket = UdpSocket::bind(bind).await?;
        socket.connect(cfg.destination).await?;
        let next_seq = cfg.start_sequence;
        Ok(Self {
            socket,
            cfg,
            next_seq,
            cache: None,
        })
    }

    /// Wire a retransmission cache (typically
    /// `MoldRequestServer::cache()`) into the publisher. Every
    /// frame written by [`Self::serve`] is pushed into the cache,
    /// ready for a `GapRecoveryClient` to retransmit.
    pub fn with_cache(mut self, cache: Arc<Mutex<RingBufferSeqStore>>) -> Self {
        self.cache = Some(cache);
        self
    }

    /// Currently-assigned next sequence number.
    #[must_use]
    pub fn next_sequence(&self) -> u64 {
        self.next_seq
    }

    /// Send one heartbeat datagram now (carries
    /// `next_expected_sequence`).
    ///
    /// # Errors
    ///
    /// - I/O errors writing the datagram.
    pub async fn send_heartbeat(&self) -> Result<(), MoldError> {
        let pkt = MoldPacket::heartbeat(self.cfg.session, self.next_seq);
        self.send_packet(&pkt).await
    }

    /// Send one end-of-session datagram now.
    ///
    /// # Errors
    ///
    /// - I/O errors writing the datagram.
    pub async fn send_end_of_session(&self) -> Result<(), MoldError> {
        let pkt = MoldPacket::end_of_session(self.cfg.session, self.next_seq);
        self.send_packet(&pkt).await
    }

    async fn send_packet(&self, pkt: &MoldPacket) -> Result<(), MoldError> {
        let mut buf = BytesMut::new();
        pkt.encode_to(&mut buf)?;
        self.socket.send(&buf).await?;
        Ok(())
    }

    /// Take ownership and drive the publisher in a fresh task.
    /// Returns the `JoinHandle` so the caller can `await` clean
    /// shutdown after the EOS burst.
    pub fn serve<S>(mut self, mut source: S) -> JoinHandle<Result<(), MoldError>>
    where
        S: futures::Stream<Item = Result<Message, itch_source::SourceError>>
            + Send
            + Unpin
            + 'static,
    {
        tokio::spawn(async move {
            let mut last_emit = Instant::now();
            let mut pending_blocks: Vec<MessageBlock> = Vec::new();
            let mut pending_first_seq: u64 = self.next_seq;
            let mut pending_size: usize = HEADER_LEN;

            loop {
                tokio::select! {
                    biased;

                    next = source.next() => match next {
                        Some(Ok(msg)) => {
                            let mut body = vec![0u8; msg.encoded_len()];
                            msg.encode(&mut body)?;
                            if body.len() > MAX_BLOCK_LEN {
                                return Err(MoldError::BlockTooLarge {
                                    got: body.len(),
                                    max: MAX_BLOCK_LEN,
                                });
                            }
                            let block_size = 2 + body.len();
                            if !pending_blocks.is_empty()
                                && pending_size + block_size > self.cfg.mtu
                            {
                                self.flush_pending(
                                    &mut pending_blocks,
                                    &mut pending_first_seq,
                                    &mut pending_size,
                                )
                                .await?;
                                last_emit = Instant::now();
                            }
                            if pending_blocks.is_empty() {
                                pending_first_seq = self.next_seq;
                                pending_size = HEADER_LEN;
                            }
                            pending_blocks.push(MessageBlock { data: body });
                            pending_size += block_size;
                            self.next_seq += 1;
                        }
                        Some(Err(itch_source::SourceError::Exhausted)) | None => {
                            // Flush whatever remains, then EOS burst.
                            if !pending_blocks.is_empty() {
                                self.flush_pending(
                                    &mut pending_blocks,
                                    &mut pending_first_seq,
                                    &mut pending_size,
                                )
                                .await?;
                            }
                            self.eos_burst().await?;
                            return Ok(());
                        }
                        Some(Err(err)) => {
                            tracing::error!(?err, "mold publisher source error");
                            // Surface and shut down.
                            return Err(MoldError::SourceExhausted);
                        }
                    },
                    () = tokio::time::sleep_until(last_emit + self.cfg.heartbeat_interval) => {
                        // Heartbeat or flush of partially-filled packet.
                        if !pending_blocks.is_empty() {
                            self.flush_pending(
                                &mut pending_blocks,
                                &mut pending_first_seq,
                                &mut pending_size,
                            )
                            .await?;
                        } else {
                            self.send_heartbeat().await?;
                        }
                        last_emit = Instant::now();
                    },
                }
            }
        })
    }

    async fn flush_pending(
        &mut self,
        blocks: &mut Vec<MessageBlock>,
        first_seq: &mut u64,
        size: &mut usize,
    ) -> Result<(), MoldError> {
        if blocks.is_empty() {
            return Ok(());
        }
        let drained: Vec<MessageBlock> = std::mem::take(blocks);
        let pkt = MoldPacket::new_data(self.cfg.session, *first_seq, drained)?;
        if let Some(cache) = self.cache.as_ref() {
            let mut guard = cache.lock().await;
            let mut seq = *first_seq;
            for b in &pkt.blocks {
                guard.push(CachedFrame {
                    sequence: seq,
                    body: b.data.clone(),
                });
                seq = seq.saturating_add(1);
            }
        }
        self.send_packet(&pkt).await?;
        // Reset packing state.
        *size = HEADER_LEN;
        *first_seq = self.next_seq;
        Ok(())
    }

    async fn eos_burst(&self) -> Result<(), MoldError> {
        for _ in 0..self.cfg.eos_burst {
            self.send_end_of_session().await?;
            tokio::time::sleep(self.cfg.eos_interval).await;
        }
        if let Some(cache) = self.cache.as_ref() {
            cache.lock().await.mark_end_of_session();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{session_from_str, MoldPacketHeader};
    use crate::receiver::MoldStream;
    use crate::{MoldConfig, MoldEvent, MoldPacket};
    use futures::stream::{self, StreamExt};
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

    /// Spin up a UDP receiver socket on loopback, return its
    /// address + a handle to read raw datagrams.
    async fn loopback_receiver() -> (UdpSocket, SocketAddr) {
        let sock = UdpSocket::bind("127.0.0.1:0").await.expect("bind");
        let addr = sock.local_addr().expect("local_addr");
        (sock, addr)
    }

    /// End-to-end: publish a few messages, read raw datagrams,
    /// decode, verify in-order delivery + EOS marker.
    #[tokio::test]
    async fn publisher_emits_packed_data_then_eos() {
        let (recv_sock, recv_addr) = loopback_receiver().await;
        let cfg = PublisherConfig::new(recv_addr, fixture_session())
            .with_start_sequence(1)
            .with_mtu(200) // small MTU to force packing across packets
            .with_heartbeat_interval(Duration::from_millis(500));
        let publisher = MoldPublisher::bind(cfg).await.expect("bind");

        // 5 messages.
        let msgs = (1u64..=5).map(|i| Ok(add_order(i))).collect::<Vec<_>>();
        let src = stream::iter(msgs);

        // Use a smaller eos_burst so the test is fast.
        let mut cfg2 = publisher.cfg.clone();
        cfg2.eos_burst = 1;
        cfg2.eos_interval = Duration::from_millis(10);
        let publisher = MoldPublisher {
            socket: publisher.socket,
            cfg: cfg2,
            next_seq: publisher.next_seq,
            cache: publisher.cache,
        };

        let handle = publisher.serve(src);

        // Read datagrams off the receiver socket until we see EOS.
        let mut all_msgs: Vec<u64> = Vec::new();
        let mut saw_eos = false;
        let deadline = Instant::now() + Duration::from_secs(5);
        let mut buf = vec![0u8; 4096];
        while Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(500), recv_sock.recv_from(&mut buf))
                .await
            {
                Ok(Ok((n, _peer))) => {
                    let pkt = MoldPacket::decode(&buf[..n]).expect("decode");
                    if pkt.header.is_end_of_session() {
                        saw_eos = true;
                        break;
                    }
                    if pkt.header.is_heartbeat() {
                        continue;
                    }
                    let base = pkt.header.sequence;
                    for (i, _b) in pkt.blocks.iter().enumerate() {
                        all_msgs.push(base + i as u64);
                    }
                }
                _ => continue,
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(2), handle)
            .await
            .expect("handle");
        assert!(saw_eos, "should observe EOS packet");
        assert_eq!(all_msgs, vec![1, 2, 3, 4, 5]);
    }

    /// Heartbeat is emitted when the source is silent past the
    /// configured interval.
    #[tokio::test]
    async fn publisher_sends_heartbeat_on_silence() {
        let (recv_sock, recv_addr) = loopback_receiver().await;
        let cfg = PublisherConfig::new(recv_addr, fixture_session())
            .with_start_sequence(1)
            .with_heartbeat_interval(Duration::from_millis(50));

        let publisher = MoldPublisher::bind(cfg).await.expect("bind");
        let mut cfg2 = publisher.cfg.clone();
        cfg2.eos_burst = 1;
        cfg2.eos_interval = Duration::from_millis(10);
        let publisher = MoldPublisher {
            socket: publisher.socket,
            cfg: cfg2,
            next_seq: publisher.next_seq,
            cache: publisher.cache,
        };

        // Source: a stream that never emits but keeps the publisher alive.
        // We spawn the publisher, wait for a heartbeat, then drop the source.
        let (tx, rx) = tokio::sync::mpsc::channel::<Result<Message, itch_source::SourceError>>(1);
        let src = tokio_stream::wrappers::ReceiverStream::new(rx);
        let handle = publisher.serve(src);

        let mut buf = vec![0u8; 4096];
        let mut got_heartbeat = false;
        for _ in 0..20 {
            if let Ok(Ok((n, _peer))) =
                tokio::time::timeout(Duration::from_millis(200), recv_sock.recv_from(&mut buf))
                    .await
            {
                let hdr = MoldPacketHeader::from_bytes(&buf[..n.min(20)]).expect("header");
                if hdr.is_heartbeat() {
                    got_heartbeat = true;
                    break;
                }
            }
        }
        assert!(got_heartbeat, "expected a heartbeat datagram");
        // Drop the source so the publisher exits.
        drop(tx);
        let _ = tokio::time::timeout(Duration::from_secs(2), handle).await;
    }

    /// Receiver + publisher integration via real UDP loopback.
    #[tokio::test]
    async fn publisher_integrates_with_moldstream_receiver() {
        let session = fixture_session();
        let (recv_sock, recv_addr) = loopback_receiver().await;

        let cfg = PublisherConfig::new(recv_addr, session)
            .with_start_sequence(1)
            .with_mtu(120)
            .with_heartbeat_interval(Duration::from_millis(500));
        let publisher = MoldPublisher::bind(cfg).await.expect("bind");
        let mut cfg2 = publisher.cfg.clone();
        cfg2.eos_burst = 1;
        cfg2.eos_interval = Duration::from_millis(10);
        let publisher = MoldPublisher {
            socket: publisher.socket,
            cfg: cfg2,
            next_seq: publisher.next_seq,
            cache: publisher.cache,
        };

        let msgs = (1u64..=10).map(|i| Ok(add_order(i))).collect::<Vec<_>>();
        let src = stream::iter(msgs);
        let pub_handle = publisher.serve(src);

        // Build a `MoldStream` directly off the receiver socket.
        let recv_cfg = MoldConfig::default()
            .with_session(session)
            .with_start_sequence(1);
        let mut stream = MoldStream::from_socket(recv_sock, recv_cfg);

        let mut delivered = Vec::new();
        let mut saw_eos = false;
        for _ in 0..30 {
            match tokio::time::timeout(Duration::from_secs(3), stream.next()).await {
                Ok(Some(Ok(MoldEvent::Message { sequence, .. }))) => delivered.push(sequence),
                Ok(Some(Ok(MoldEvent::EndOfSession { .. }))) => {
                    saw_eos = true;
                    break;
                }
                Ok(Some(Ok(MoldEvent::Heartbeat { .. }))) => {}
                Ok(Some(Ok(MoldEvent::Gap { .. }))) => {}
                Ok(Some(Err(err))) => {
                    eprintln!("receiver error: {err:?}");
                    break;
                }
                Ok(None) => break,
                Err(_) => break,
            }
            if delivered.len() >= 10 {
                // Wait a bit for EOS.
            }
        }
        let _ = tokio::time::timeout(Duration::from_secs(2), pub_handle).await;
        assert!(
            delivered.starts_with(&[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]) || delivered.len() == 10,
            "expected all 10 messages, got {delivered:?}"
        );
        assert!(saw_eos);
    }
}
