//! Errors returned by the MoldUDP64 transport.
//!
//! `MoldError` aggregates `std::io::Error` and
//! `itch_protocol::ProtocolError` via `#[from]` so the `?` operator
//! works at every transport boundary. Variants are structured —
//! never opaque strings — and the type is `#[non_exhaustive]` so
//! new variants land in minor releases without breaking matchers.

use std::io;
use std::net::SocketAddr;

use itch_protocol::ProtocolError;
use thiserror::Error;

/// Errors returned by the `itch-mold` transport.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum MoldError {
    /// Underlying socket / I/O error.
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// Inner ITCH message failed to decode. Aggregated via `#[from]`
    /// for ergonomic propagation from the message-block decoder.
    #[error("itch protocol: {0}")]
    Protocol(#[from] ProtocolError),

    /// Buffer was shorter than the announced packet (header or
    /// block).
    #[error("truncated: need {need} bytes, got {got}")]
    Truncated {
        /// Number of bytes the codec needed.
        need: usize,
        /// Number of bytes actually present.
        got: usize,
    },

    /// Caller-provided buffer is too small for the encode target.
    #[error("buffer too small: need {need} bytes, got {got}")]
    BufferTooSmall {
        /// Number of bytes the encoder needs.
        need: usize,
        /// Number of bytes actually available.
        got: usize,
    },

    /// A message block announced (or carried) more bytes than the
    /// configured maximum.
    #[error("block too large: got {got} bytes, max {max}")]
    BlockTooLarge {
        /// Bytes announced or carried.
        got: usize,
        /// Configured maximum (bytes).
        max: usize,
    },

    /// A message block had a zero-length payload (reserved value).
    #[error("zero-length message block (reserved)")]
    EmptyBlock,

    /// A heartbeat or end-of-session packet carried unexpected
    /// payload bytes after the header.
    #[error("trailing bytes after header-only packet: {extra} extra")]
    TrailingBytes {
        /// Number of unexpected bytes after the header.
        extra: usize,
    },

    /// Caller tried to construct a data packet with more blocks
    /// than the `MsgCount` field can encode (`u16`, with `0xFFFF`
    /// reserved for end-of-session).
    #[error("too many blocks for one packet: {got}")]
    TooManyBlocks {
        /// Number of blocks the caller passed.
        got: usize,
    },

    /// The receiver's session id did not match the one the
    /// publisher sent.
    #[error("session mismatch: expected {expected:?}, got {got:?}")]
    SessionMismatch {
        /// Session bytes the receiver expected.
        expected: [u8; 10],
        /// Session bytes actually carried by the offending packet.
        got: [u8; 10],
    },

    /// A `SessionId` builder received an ASCII string longer than
    /// 10 bytes.
    #[error("session too long: got {got} bytes (max 10)")]
    SessionTooLong {
        /// Number of bytes provided.
        got: usize,
    },

    /// Receiver's pending out-of-order buffer is full. The receiver
    /// emits this before dropping the connection so the application
    /// can decide whether to reset state or restart the stream.
    #[error("pending buffer full: holding {size} messages")]
    PendingBufferFull {
        /// Current size of the pending buffer.
        size: usize,
    },

    /// Gap larger than the receiver is willing to attempt to recover.
    #[error("gap too large: {gap} messages, max {max}")]
    GapTooLarge {
        /// Width of the missing range.
        gap: u64,
        /// Configured maximum.
        max: usize,
    },

    /// A request to a re-request server timed out.
    #[error("request timeout: server {server} did not retransmit seq {seq} in time")]
    RequestTimeout {
        /// Address of the request server that failed to respond.
        server: SocketAddr,
        /// First missing sequence number requested.
        seq: u64,
    },

    /// The publisher's source signalled exhaustion (or an internal
    /// invariant violation) during packing.
    #[error("source exhausted")]
    SourceExhausted,

    /// Peer (publisher / multicast group) has been silent past the
    /// configured threshold. The receiver emits this as an error
    /// after the threshold is exceeded.
    #[error("peer silent for {0:?}")]
    PeerSilent(std::time::Duration),
}
