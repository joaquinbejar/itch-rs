//! Naïve length-prefix TCP framing for `itch-protocol`.
//!
//! A 2-byte big-endian length prefix (which includes the 1-byte ITCH
//! type tag) precedes each message. Useful for tests, demos, and
//! pre-production prototypes — **not** a NASDAQ-conformant transport.
//! For production unicast see `itch-soup`; for production multicast
//! see `itch-mold`.
//!
//! ```text
//! ┌────────┬────────┬─────────────────────────────┐
//! │   length (u16)  │  ITCH message (length B)    │
//! └────────┴────────┴─────────────────────────────┘
//!    2 bytes BE          1-byte tag + body
//! ```
//!
//! # Public API
//!
//! - [`ItchCodec`] — a [`tokio_util::codec::Decoder`] +
//!   [`tokio_util::codec::Encoder<Message>`] implementation. Plug it
//!   into a [`tokio_util::codec::Framed`] in either direction.
//! - [`ItchConnection`] — a `Framed<TcpStream, ItchCodec>` alias
//!   that implements [`futures::Stream`] for receiving and
//!   [`futures::Sink`] for sending.
//! - [`connect`] — async client helper: dials the address, sets
//!   `TCP_NODELAY`, and wraps the socket in an [`ItchConnection`].
//! - [`bind`] / [`accept`] — async server helpers built on
//!   [`tokio::net::TcpListener`].
//! - [`MAX_MESSAGE_LEN`] — 1 KiB cap (largest ITCH 5.0 message is
//!   50 bytes; the cap leaves headroom and bounds memory under
//!   adversarial input).
//!
//! # Stream-poison resistance
//!
//! The codec **never poisons the stream** on a bad inner frame:
//!
//! - A frame whose body fails [`itch_protocol::Message::decode`]
//!   yields one [`TransportError::Protocol`] error and the codec
//!   resumes reading at the next length prefix.
//! - A frame whose announced length exceeds [`MAX_MESSAGE_LEN`]
//!   yields one [`TransportError::FrameTooLarge`] and the codec
//!   uses an internal recovery counter to drain the rest of the
//!   bad bytes across subsequent reads — partial oversized frames
//!   do not corrupt the stream once the bytes finally arrive.
//!
//! # Example — client
//!
//! ```no_run
//! use futures::StreamExt;
//! use itch_tcp::connect;
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let mut conn = connect("127.0.0.1:9100").await?;
//! while let Some(msg) = conn.next().await {
//!     let msg = msg?;
//!     println!("{:?}", msg);
//! }
//! # Ok(()) }
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::io;
use std::net::SocketAddr;

use bytes::{Buf, BufMut, BytesMut};
use itch_protocol::{Message, ProtocolError};
use thiserror::Error;
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio_util::codec::{Decoder, Encoder, Framed};

mod server;
pub use server::{Server, DEFAULT_BROADCAST_CAPACITY};

/// Maximum frame size the codec will accept (1 KiB). The largest
/// ITCH 5.0 message is 50 bytes; this leaves ample headroom for any
/// future spec growth while still bounding memory if a peer sends
/// garbage.
pub const MAX_MESSAGE_LEN: usize = 1024;

/// Errors returned by the `itch-tcp` framing layer.
///
/// `#[non_exhaustive]` so new structured variants can be added in
/// minor releases.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum TransportError {
    /// Underlying socket / I/O error.
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// Inner ITCH message failed to decode. The codec advances past
    /// the bad frame and resumes at the next length prefix; the
    /// stream is **not** poisoned.
    #[error("itch protocol: {0}")]
    Protocol(#[from] ProtocolError),

    /// Peer announced a frame larger than [`MAX_MESSAGE_LEN`]. The
    /// codec advances past the entire bad frame and resumes; the
    /// stream is **not** poisoned.
    #[error("frame too large: announced {got} bytes, max {max}")]
    FrameTooLarge {
        /// Length the peer announced (in bytes, including the tag).
        got: usize,
        /// Configured maximum.
        max: usize,
    },
}

/// Convenience alias used by `Framed`.
pub type ItchResult<T> = Result<T, TransportError>;

/// Length-prefix codec for ITCH 5.0 messages over TCP.
///
/// Plugs into [`tokio_util::codec::Framed`] in both directions. The
/// length prefix is a u16 big-endian integer that **includes** the
/// 1-byte ITCH type tag; total wire bytes per message is
/// `2 + length`.
///
/// The codec carries a small recovery counter so that an oversized
/// `FrameTooLarge` reject correctly drains the remaining bytes of
/// the bad frame across multiple subsequent reads — without that,
/// a partial oversized frame would corrupt the stream once the
/// rest arrived.
#[derive(Debug, Clone, Default)]
pub struct ItchCodec {
    /// Bytes still to discard from the next inbound chunks before
    /// resuming normal length-prefix parsing. Set when an oversized
    /// frame's announced span hadn't fully arrived yet.
    pending_skip: usize,
}

impl Decoder for ItchCodec {
    type Item = Message;
    type Error = TransportError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Message>, TransportError> {
        // First, drain any leftover bytes from a previous oversized
        // FrameTooLarge that wasn't fully present at the time of
        // detection.
        if self.pending_skip > 0 {
            let to_drop = self.pending_skip.min(src.len());
            src.advance(to_drop);
            self.pending_skip -= to_drop;
            if self.pending_skip > 0 {
                // Still more to drop on later reads; nothing to yield yet.
                return Ok(None);
            }
        }

        // Need at least 2 bytes to read the length prefix.
        if src.len() < 2 {
            return Ok(None);
        }
        let len = u16::from_be_bytes([src[0], src[1]]) as usize;

        if len > MAX_MESSAGE_LEN {
            // Drop the length prefix and the bytes already present;
            // remember how many more we still need to skip on
            // subsequent reads so the rest of the oversized payload
            // is absorbed instead of being mis-parsed as a new prefix.
            let total = 2 + len;
            if src.len() >= total {
                src.advance(total);
            } else {
                self.pending_skip = total - src.len();
                src.clear();
            }
            return Err(TransportError::FrameTooLarge {
                got: len,
                max: MAX_MESSAGE_LEN,
            });
        }

        // Wait for the full frame before parsing the inner ITCH bytes.
        if src.len() < 2 + len {
            // Reserve a bit so subsequent reads can grow without a
            // small-allocation walk. `Framed` resizes once per
            // message in steady state.
            src.reserve(2 + len - src.len());
            return Ok(None);
        }

        // Consume the length prefix.
        src.advance(2);
        // Split off exactly the body (tag + payload) so the decoder
        // owns the bytes and the next call resumes at the next prefix.
        let frame = src.split_to(len);

        match Message::decode(&frame[..]) {
            Ok(msg) => {
                tracing::debug!(tag = %char::from(msg.tag()), len, "decoded itch frame");
                Ok(Some(msg))
            }
            Err(err) => {
                // Drop-and-resume: the frame bytes are already
                // consumed; the next call begins on the next prefix.
                tracing::warn!(?err, len, "bad inner ITCH frame; dropping");
                Err(err.into())
            }
        }
    }
}

impl Encoder<Message> for ItchCodec {
    type Error = TransportError;

    fn encode(&mut self, msg: Message, dst: &mut BytesMut) -> Result<(), TransportError> {
        let total = msg.encoded_len();
        if total > MAX_MESSAGE_LEN {
            // Defensive: every ITCH 5.0 message is <= 50 bytes, so
            // this is structurally unreachable today. Guard anyway
            // in case a future message expands beyond MAX.
            return Err(TransportError::FrameTooLarge {
                got: total,
                max: MAX_MESSAGE_LEN,
            });
        }

        dst.reserve(2 + total);
        dst.put_u16(total as u16);
        // Reserve space, then encode in place.
        let start = dst.len();
        dst.resize(start + total, 0);
        let n = msg.encode(&mut dst[start..start + total])?;
        debug_assert_eq!(n, total, "encode wrote unexpected length");
        tracing::debug!(tag = %char::from(msg.tag()), len = total, "encoded itch frame");
        Ok(())
    }
}

/// Length-prefix-framed TCP connection ready for `Stream` / `Sink`
/// consumption.
pub type ItchConnection = Framed<TcpStream, ItchCodec>;

/// Connect to a peer and wrap the socket in [`Framed`] +
/// [`ItchCodec`].
///
/// # Errors
///
/// - I/O errors from `TcpStream::connect` or from setting
///   `TCP_NODELAY` on the resulting socket.
pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<ItchConnection> {
    let socket = TcpStream::connect(addr).await?;
    socket.set_nodelay(true)?;
    let peer = socket.peer_addr().ok();
    tracing::info!(?peer, "itch-tcp connection established");
    Ok(Framed::new(socket, ItchCodec::default()))
}

/// Bind a TCP listener for accepting incoming ITCH connections.
///
/// # Errors
///
/// - I/O errors from `TcpListener::bind`.
pub async fn bind<A: ToSocketAddrs>(addr: A) -> io::Result<TcpListener> {
    let listener = TcpListener::bind(addr).await?;
    tracing::info!(local = ?listener.local_addr().ok(), "itch-tcp listener bound");
    Ok(listener)
}

/// Accept one incoming connection on the listener and wrap it in
/// [`Framed`] + [`ItchCodec`].
///
/// # Errors
///
/// - I/O errors from `TcpListener::accept` or from setting
///   `TCP_NODELAY` on the accepted socket.
pub async fn accept(listener: &TcpListener) -> io::Result<(ItchConnection, SocketAddr)> {
    let (socket, peer) = listener.accept().await?;
    socket.set_nodelay(true)?;
    tracing::info!(?peer, "itch-tcp connection accepted");
    Ok((Framed::new(socket, ItchCodec::default()), peer))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;
    use itch_protocol::{
        AddOrder, Header, OrderReference, Price4, Shares, Side, Stock, StockLocate, SystemEvent,
        Timestamp, TrackingNumber,
    };

    fn add_order_fixture() -> Message {
        Message::AddOrder(AddOrder {
            header: Header {
                stock_locate: StockLocate::from_u16(1),
                tracking_number: TrackingNumber::from_u16(2),
                timestamp: Timestamp::from_u64(0x1234_5678),
            },
            order_ref: OrderReference::from_u64(1001),
            side: Side::Buy,
            shares: Shares::from_u32(500),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_925_000),
        })
    }

    #[test]
    fn encoder_then_decoder_roundtrip() {
        let mut codec = ItchCodec::default();
        let mut buf = BytesMut::new();
        let msg = add_order_fixture();
        codec.encode(msg, &mut buf).expect("encode");
        // u16 BE prefix (= 36, AddOrder.encoded_len()) + 36 body bytes.
        assert_eq!(buf.len(), 2 + msg.encoded_len());
        let decoded = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(decoded, msg);
        assert!(buf.is_empty(), "buffer drained after one frame");
    }

    #[test]
    fn decoder_returns_none_when_starved() {
        let mut codec = ItchCodec::default();
        // Less than 2 bytes — no length prefix yet.
        let mut buf = BytesMut::from(&[0x00u8][..]);
        assert!(codec.decode(&mut buf).expect("decode").is_none());
        assert_eq!(buf.len(), 1, "buffer untouched");

        // Have prefix but body short.
        let mut buf = BytesMut::from(&[0x00u8, 0x05, 0xAA, 0xBB][..]); // says 5 bytes coming, only 2 here
        assert!(codec.decode(&mut buf).expect("decode").is_none());
        assert_eq!(buf.len(), 4, "buffer untouched while waiting for body");
    }

    #[test]
    fn decoder_drops_oversized_frame_fully_present_and_resumes() {
        // Case A: the oversized payload is already present in the
        // buffer — the codec advances past the entire span in one
        // call.
        let mut codec = ItchCodec::default();
        let mut buf = BytesMut::new();
        let oversized = MAX_MESSAGE_LEN + 1;
        buf.put_u16(oversized as u16);
        buf.resize(2 + oversized, 0); // pad the oversized payload

        let err = codec
            .decode(&mut buf)
            .expect_err("oversized must produce FrameTooLarge");
        match err {
            TransportError::FrameTooLarge { got, max } => {
                assert_eq!(got, oversized);
                assert_eq!(max, MAX_MESSAGE_LEN);
            }
            other => panic!("expected FrameTooLarge, got {other:?}"),
        }
        assert!(buf.is_empty(), "full oversized frame consumed");

        // Resume with a valid frame.
        codec.encode(add_order_fixture(), &mut buf).expect("encode");
        let msg = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(msg, add_order_fixture());
    }

    #[test]
    fn decoder_drops_oversized_frame_partial_then_resumes() {
        // Case B: the oversized payload arrives across multiple
        // reads. The codec must remember how many bytes still need
        // to be discarded and only resume parsing once the entire
        // bad span has been drained.
        let mut codec = ItchCodec::default();
        let mut buf = BytesMut::new();
        let oversized = MAX_MESSAGE_LEN + 1;
        buf.put_u16(oversized as u16);
        // First read: only the prefix has arrived.
        let err = codec
            .decode(&mut buf)
            .expect_err("oversized must produce FrameTooLarge");
        assert!(matches!(err, TransportError::FrameTooLarge { .. }));

        // Subsequent reads slowly drain the rest of the oversized
        // payload. Pretend chunks of 256 bytes arrive at a time.
        let mut remaining = oversized;
        while remaining > 0 {
            let chunk_size = remaining.min(256);
            buf.resize(chunk_size, 0);
            // The codec must consume exactly the pending_skip from
            // this chunk, leaving the buffer empty (or close to it
            // if a chunk overran into a new prefix — which it doesn't
            // here).
            let polled = codec.decode(&mut buf).expect("decode");
            assert!(polled.is_none(), "still draining oversized payload");
            remaining -= chunk_size;
        }
        assert!(buf.is_empty(), "buffer fully drained");

        // Now a valid frame arrives — must decode cleanly.
        codec.encode(add_order_fixture(), &mut buf).expect("encode");
        let msg = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(msg, add_order_fixture());
    }

    #[test]
    fn decoder_bad_inner_frame_does_not_poison_stream() {
        let mut codec = ItchCodec::default();
        let mut buf = BytesMut::new();

        // Length 1 + tag '~' (unknown) — well-formed prefix but bad tag.
        buf.put_u16(1);
        buf.put_u8(b'~');
        let err = codec.decode(&mut buf).expect_err("bad tag must error");
        match err {
            TransportError::Protocol(_) => {}
            other => panic!("expected Protocol(_), got {other:?}"),
        }
        assert!(buf.is_empty(), "bad frame consumed");

        // Resume with a valid frame.
        codec.encode(add_order_fixture(), &mut buf).expect("encode");
        let msg = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(msg, add_order_fixture());
    }

    #[tokio::test]
    async fn end_to_end_via_loopback_socket() {
        use futures::{SinkExt, StreamExt};

        let listener = bind("127.0.0.1:0").await.expect("bind");
        let local = listener.local_addr().expect("local_addr");

        // Server task: accept one connection, send 3 messages.
        let server = tokio::spawn(async move {
            let (mut conn, _peer) = accept(&listener).await.expect("accept");
            for _ in 0..3 {
                conn.send(add_order_fixture()).await.expect("send");
            }
        });

        let mut conn = connect(local).await.expect("connect");
        let mut received = Vec::new();
        for _ in 0..3 {
            match conn.next().await {
                Some(Ok(m)) => received.push(m),
                Some(Err(e)) => panic!("decode error: {e:?}"),
                None => break,
            }
        }
        assert_eq!(received.len(), 3);
        for m in &received {
            assert_eq!(m, &add_order_fixture());
        }

        server.await.expect("server task");
    }

    #[tokio::test]
    async fn end_to_end_handles_every_message_kind() {
        use futures::{SinkExt, StreamExt};

        let listener = bind("127.0.0.1:0").await.expect("bind");
        let local = listener.local_addr().expect("local_addr");

        // Send every kind via canonical_session()-style fixtures.
        let messages: Vec<Message> = vec![
            add_order_fixture(),
            Message::SystemEvent(SystemEvent {
                header: Header {
                    stock_locate: StockLocate::from_u16(0),
                    tracking_number: TrackingNumber::from_u16(0),
                    timestamp: Timestamp::from_u64(0),
                },
                event_code: itch_protocol::EventCode::EndOfMessages,
            }),
        ];

        let server_msgs = messages.clone();
        let server = tokio::spawn(async move {
            let (mut conn, _peer) = accept(&listener).await.expect("accept");
            for m in server_msgs {
                conn.send(m).await.expect("send");
            }
        });

        let mut conn = connect(local).await.expect("connect");
        let mut received = Vec::new();
        while received.len() < messages.len() {
            match conn.next().await {
                Some(Ok(m)) => received.push(m),
                Some(Err(e)) => panic!("decode error: {e:?}"),
                None => break,
            }
        }
        assert_eq!(received, messages);
        server.await.expect("server task");
    }
}
