//! SoupBinTCP 3.00 packet envelope codec for `itch-protocol`.
//!
//! SoupBinTCP is NASDAQ's lightweight point-to-point session
//! protocol layered on top of TCP. It frames an opaque payload
//! (typically one ITCH 5.0 message) inside a typed envelope and
//! provides login, sequenced-data delivery, heartbeats, and
//! end-of-session signaling. See `docs/specs/soupbintcp-3.0.md`
//! and `docs/TRANSPORT-SPEC.md` §3.
//!
//! This crate provides the **packet envelope codec only** —
//! [`SoupCodec`] (a [`tokio_util::codec::Decoder`] +
//! [`tokio_util::codec::Encoder<SoupPacket>`]) over the standard
//! length-type-payload framing. Session state (login handshake,
//! heartbeats, sequence tracking, reconnect) lands in subsequent
//! issues per the project roadmap.
//!
//! ```text
//! ┌────────┬────────┬────────┬────────────────────────────┐
//! │   length (u16)  │ type 1B│        payload             │
//! └────────┴────────┴────────┴────────────────────────────┘
//!    2 bytes BE                  variable, can be empty
//! ```
//!
//! `length` excludes its own 2 bytes and includes the 1-byte type
//! tag. The type tag identifies the packet variant; the payload
//! shape depends on the tag.
//!
//! # Stream-poison resistance
//!
//! On a malformed inbound packet (oversized announced length,
//! unknown type tag, truncated body for a fixed-size packet) the
//! codec consumes the bad bytes, emits exactly one error, and
//! resumes parsing at the next length prefix. A peer cannot poison
//! the stream with one bad frame.
//!
//! # Bounded buffering
//!
//! [`MAX_MESSAGE_LEN`] caps the total wire bytes per packet at
//! 1 KiB. A peer that announces a longer packet receives a single
//! [`SoupError::FrameTooLarge`] reject and the bytes are drained
//! across as many subsequent reads as needed.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::fmt;
use std::io;

use bytes::{Buf, BufMut, BytesMut};
use itch_protocol::ProtocolError;
use thiserror::Error;
use tokio_util::codec::{Decoder, Encoder};

mod connection;
pub use connection::{
    login, login_with_timeout, SoupConnection, SoupCredentials, DEFAULT_LOGIN_TIMEOUT,
    DEFAULT_SEND_TIMEOUT,
};

mod heartbeat;
pub use heartbeat::{HeartbeatConfig, InboundHeartbeat, OutboundHeartbeat};

mod resilient;
pub use resilient::{
    ResilientSoupClient, ResilientSoupConfig, DEFAULT_BACKOFF_MAX, DEFAULT_BACKOFF_MIN,
    DEFAULT_RESILIENT_LOGIN_TIMEOUT,
};

mod server;
pub use server::{
    AllowAllAuthenticator, Authenticator, SoupServer, SoupSession, StaticAuthenticator,
    DEFAULT_BROADCAST_CAPACITY, DEFAULT_SHUTDOWN_GRACE, DEFAULT_UNSEQUENCED_INBOX_CAPACITY,
};

/// Maximum total wire bytes the codec will accept for a single
/// SoupBinTCP packet (length prefix + type tag + payload). Mirrors
/// `itch-tcp::MAX_MESSAGE_LEN` so a malicious peer cannot force
/// unbounded buffering at any framing layer.
pub const MAX_MESSAGE_LEN: usize = 1024;

/// Wire length of the [`LoginRequest`] payload (after the 1-byte
/// type tag). 6 + 10 + 10 + 20 = 46 bytes.
pub const LOGIN_REQUEST_PAYLOAD_LEN: usize = 6 + 10 + 10 + 20;

/// Wire length of the [`LoginAccepted`] payload (after the 1-byte
/// type tag). 10 + 20 = 30 bytes.
pub const LOGIN_ACCEPTED_PAYLOAD_LEN: usize = 10 + 20;

/// Wire length of the Login Rejected payload (after the 1-byte
/// type tag). One ASCII reject-reason byte.
pub const LOGIN_REJECTED_PAYLOAD_LEN: usize = 1;

// ---------- Type tags ----------

/// SoupBinTCP packet type tags. See §3.1 of `TRANSPORT-SPEC.md`.
mod tag {
    pub const DEBUG: u8 = b'+';
    pub const LOGIN_ACCEPTED: u8 = b'A';
    pub const LOGIN_REJECTED: u8 = b'J';
    pub const SEQUENCED_DATA: u8 = b'S';
    pub const SERVER_HEARTBEAT: u8 = b'H';
    pub const END_OF_SESSION: u8 = b'Z';
    pub const LOGIN_REQUEST: u8 = b'L';
    pub const UNSEQUENCED_DATA: u8 = b'U';
    pub const CLIENT_HEARTBEAT: u8 = b'R';
    pub const LOGOUT_REQUEST: u8 = b'O';
}

// ---------- Errors ----------

/// Errors returned by the SoupBinTCP packet codec.
///
/// Marked `#[non_exhaustive]` so additional structured variants
/// (login rejection, session ended, peer-silent timeout) can be
/// added in minor releases without breaking matchers.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum SoupError {
    /// Underlying socket / I/O error.
    #[error("io: {0}")]
    Io(#[from] io::Error),

    /// Inner ITCH message failed to decode. Aggregated via
    /// `#[from]` so the `?` operator works at call sites that
    /// extract an `itch_protocol::Message` from a payload.
    #[error("itch protocol: {0}")]
    Protocol(#[from] ProtocolError),

    /// Peer announced a packet larger than [`MAX_MESSAGE_LEN`].
    /// The codec drains the bad span (across multiple reads if
    /// necessary) before resuming.
    #[error("frame too large: announced {got} bytes, max {max}")]
    FrameTooLarge {
        /// Total wire length the peer announced (length prefix + tag + payload).
        got: usize,
        /// Configured maximum.
        max: usize,
    },

    /// Peer sent an unrecognised packet type tag.
    #[error("unknown soup packet type: 0x{tag:02X} ({})", display_byte(*tag))]
    UnknownPacketType {
        /// The byte value that did not match any known tag.
        tag: u8,
    },

    /// A fixed-size packet (`A`, `J`, `L`) had a payload of the
    /// wrong length.
    #[error(
        "bad payload length for tag {} (0x{tag:02X}): expected {expected}, got {got}",
        display_byte(*tag)
    )]
    BadPayloadLength {
        /// The packet type tag.
        tag: u8,
        /// The number of payload bytes the spec requires.
        expected: usize,
        /// The number of payload bytes actually present.
        got: usize,
    },

    /// Server rejected a Login Request. Carried separately from
    /// [`SoupError::Protocol`] because the rejection is a
    /// well-formed Soup packet, not a malformed one. The session
    /// layer (next issue) raises this on a `J` packet during the
    /// handshake.
    #[error("login rejected: {0}")]
    LoginRejected(LoginRejectReason),

    /// Server signaled end-of-session via a `Z` packet. The
    /// session layer raises this; the bare codec just returns the
    /// [`SoupPacket::EndOfSession`] variant.
    #[error("session ended")]
    SessionEnded,

    /// Heartbeat-deadline exceeded. The session layer raises this;
    /// the bare codec does not track time.
    #[error("peer silent for {0:?}")]
    PeerSilent(std::time::Duration),

    /// Server's `Login Accepted` packet returned a session id that
    /// did not match the one the client requested. Per spec this is
    /// only valid when the client requested the empty / "current"
    /// session — any explicit mismatch is fatal.
    #[error("session mismatch: requested {requested:?}, got {got:?}")]
    SessionMismatch {
        /// The session id the client asked for in `Login Request`.
        requested: String,
        /// The session id the server returned in `Login Accepted`.
        got: String,
    },

    /// Server closed the socket before completing the login
    /// handshake (peer sent EOF before `A` or `J`). Distinct from
    /// [`SoupError::SessionEnded`] — that one represents a graceful
    /// `Z` end-of-session after a successful login.
    #[error("connection closed before login completed")]
    PrematureClose,

    /// Server sent an unexpected packet during the login handshake
    /// (something other than `A`, `J`, or `+`). Indicates a buggy
    /// or hostile peer.
    #[error("unexpected packet during login handshake: tag 0x{tag:02X}")]
    UnexpectedHandshakePacket {
        /// The unexpected packet's type tag.
        tag: u8,
    },

    /// Login handshake did not complete within the configured
    /// timeout. The session layer raises this around the
    /// handshake exchange.
    #[error("login timed out after {0:?}")]
    LoginTimeout(std::time::Duration),

    /// SoupBinTCP framing-layer violation that does not warrant a
    /// more specific variant — for example a client-direction
    /// packet (`L` / `U` / `R` / `O`) seen on a server-bound stream
    /// or a subscriber that lagged past the broadcast capacity.
    /// Carries a static `reason` string so callers can pattern-match
    /// without parsing.
    #[error("soup framing violation: {reason}")]
    SoupFraming {
        /// Static description of the violation.
        reason: &'static str,
    },
}

/// Codes carried by a Login Rejected (`J`) packet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LoginRejectReason {
    /// `A` — Not Authorized: the username / password combination
    /// in the Login Request did not match.
    NotAuthorized,
    /// `S` — Session not available: the requested session was
    /// invalid or unavailable.
    SessionUnavailable,
}

impl LoginRejectReason {
    /// ASCII byte the spec assigns to this reason.
    #[must_use]
    #[inline]
    pub const fn to_byte(self) -> u8 {
        match self {
            LoginRejectReason::NotAuthorized => b'A',
            LoginRejectReason::SessionUnavailable => b'S',
        }
    }

    /// Decode an ASCII reject-reason byte.
    ///
    /// # Errors
    ///
    /// Returns [`SoupError::Protocol`] wrapping
    /// [`ProtocolError::InvalidEnumCode`] if `b` is not `A` or
    /// `S`.
    #[inline]
    pub fn from_byte(b: u8) -> Result<Self, SoupError> {
        match b {
            b'A' => Ok(LoginRejectReason::NotAuthorized),
            b'S' => Ok(LoginRejectReason::SessionUnavailable),
            code => Err(SoupError::Protocol(ProtocolError::InvalidEnumCode {
                field: "LoginRejectReason",
                code,
            })),
        }
    }
}

impl fmt::Display for LoginRejectReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoginRejectReason::NotAuthorized => f.write_str("not authorized"),
            LoginRejectReason::SessionUnavailable => f.write_str("session unavailable"),
        }
    }
}

#[cold]
#[inline(never)]
fn display_byte(b: u8) -> String {
    if b.is_ascii_graphic() {
        format!("'{}'", b as char)
    } else {
        format!("0x{b:02X}")
    }
}

// ---------- Login DTOs ----------

/// Client → server `L` Login Request payload. Username and
/// password are right-padded with spaces to their fixed widths
/// (6 and 10 bytes); session is left-blank-padded ANUM (`""`
/// means "currently active session"); sequence is the 20-byte
/// ASCII representation of the next-expected sequence number, or
/// `0` for "most recent".
///
/// Field widths:
/// - `username`: 6 bytes, ANUM, right-padded with spaces.
/// - `password`: 10 bytes, ANUM, right-padded with spaces.
/// - `requested_session`: 10 bytes, ANUM, left-padded with spaces
///   (empty / all-spaces means "currently active session").
/// - `requested_sequence`: 20 bytes, ASCII decimal, left-padded
///   with spaces. `0` means "start from most recent".
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginRequest {
    /// Username (case-insensitive per spec; up to 6 bytes).
    pub username: String,
    /// Password (case-insensitive per spec; up to 10 bytes).
    pub password: String,
    /// Requested session id (empty / blank = "currently active").
    pub requested_session: String,
    /// Next-expected sequence number; `0` means "most recent".
    pub requested_sequence: u64,
}

/// Server → client `A` Login Accepted payload.
///
/// - `session`: 10 bytes ANUM, left-padded with spaces.
/// - `sequence`: 20-byte ASCII decimal, left-padded with spaces.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LoginAccepted {
    /// Session id the client is now logged into.
    pub session: String,
    /// Sequence number of the next sequenced message.
    pub sequence: u64,
}

// ---------- Packet enum ----------

/// One SoupBinTCP 3.00 logical packet.
///
/// Variants without payload (`ServerHeartbeat`, `ClientHeartbeat`,
/// `EndOfSession`, `LogoutRequest`) carry no fields. Data-bearing
/// variants (`SequencedData`, `UnsequencedData`, `Debug`) carry
/// the raw payload bytes verbatim — SoupBinTCP is opaque to the
/// higher-level protocol, so this codec does not parse the inner
/// ITCH message. The session layer (next issue) wraps payloads as
/// `itch_protocol::Message` after envelope framing succeeds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SoupPacket {
    /// `+` Free-form troubleshooting text. Either side may send.
    Debug(Vec<u8>),
    /// `L` Client → server login.
    LoginRequest(LoginRequest),
    /// `A` Server → client login accepted.
    LoginAccepted(LoginAccepted),
    /// `J` Server → client login rejected.
    LoginRejected(LoginRejectReason),
    /// `S` Server → client sequenced data; payload is opaque to
    /// SoupBinTCP.
    SequencedData(Vec<u8>),
    /// `U` Client → server unsequenced data; payload is opaque to
    /// SoupBinTCP.
    UnsequencedData(Vec<u8>),
    /// `H` Server heartbeat (zero payload).
    ServerHeartbeat,
    /// `R` Client heartbeat (zero payload).
    ClientHeartbeat,
    /// `Z` End of session (zero payload).
    EndOfSession,
    /// `O` Client → server logout request (zero payload).
    LogoutRequest,
}

impl SoupPacket {
    /// Type tag this packet uses on the wire.
    #[must_use]
    #[inline]
    pub const fn tag(&self) -> u8 {
        match self {
            SoupPacket::Debug(_) => tag::DEBUG,
            SoupPacket::LoginRequest(_) => tag::LOGIN_REQUEST,
            SoupPacket::LoginAccepted(_) => tag::LOGIN_ACCEPTED,
            SoupPacket::LoginRejected(_) => tag::LOGIN_REJECTED,
            SoupPacket::SequencedData(_) => tag::SEQUENCED_DATA,
            SoupPacket::UnsequencedData(_) => tag::UNSEQUENCED_DATA,
            SoupPacket::ServerHeartbeat => tag::SERVER_HEARTBEAT,
            SoupPacket::ClientHeartbeat => tag::CLIENT_HEARTBEAT,
            SoupPacket::EndOfSession => tag::END_OF_SESSION,
            SoupPacket::LogoutRequest => tag::LOGOUT_REQUEST,
        }
    }

    /// Payload byte length (excluding the 1-byte tag and the
    /// 2-byte length prefix). Used to size the encoder output.
    #[must_use]
    #[inline]
    pub fn payload_len(&self) -> usize {
        match self {
            SoupPacket::Debug(p)
            | SoupPacket::SequencedData(p)
            | SoupPacket::UnsequencedData(p) => p.len(),
            SoupPacket::LoginRequest(_) => LOGIN_REQUEST_PAYLOAD_LEN,
            SoupPacket::LoginAccepted(_) => LOGIN_ACCEPTED_PAYLOAD_LEN,
            SoupPacket::LoginRejected(_) => LOGIN_REJECTED_PAYLOAD_LEN,
            SoupPacket::ServerHeartbeat
            | SoupPacket::ClientHeartbeat
            | SoupPacket::EndOfSession
            | SoupPacket::LogoutRequest => 0,
        }
    }

    /// Wire length of the encoded packet — `2 (length prefix) + 1
    /// (type tag) + payload`.
    #[must_use]
    #[inline]
    pub fn encoded_len(&self) -> usize {
        2 + 1 + self.payload_len()
    }
}

// ---------- ASCII field helpers ----------

/// Right-pad `src` to `width` bytes with ASCII spaces. Truncates
/// if `src` is too long. Used for username / password.
fn pad_right(src: &str, width: usize, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), width);
    let bytes = src.as_bytes();
    let n = bytes.len().min(width);
    dst[..n].copy_from_slice(&bytes[..n]);
    for slot in dst.iter_mut().skip(n) {
        *slot = b' ';
    }
}

/// Left-pad `src` to `width` bytes with ASCII spaces. Truncates
/// (keeps the trailing bytes) if `src` is too long. Used for
/// session id.
fn pad_left(src: &str, width: usize, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), width);
    let bytes = src.as_bytes();
    if bytes.len() >= width {
        dst.copy_from_slice(&bytes[bytes.len() - width..]);
    } else {
        let pad = width - bytes.len();
        for slot in dst.iter_mut().take(pad) {
            *slot = b' ';
        }
        dst[pad..].copy_from_slice(bytes);
    }
}

/// Read an ANUM field (raw bytes) and return it as an owned
/// `String` with leading and trailing spaces trimmed. Falls back
/// to a lossy decode on the (vanishingly rare) case of non-UTF-8
/// bytes — SoupBinTCP fields are documented as ASCII.
fn read_anum(src: &[u8]) -> String {
    let trimmed = trim_spaces(src);
    String::from_utf8_lossy(trimmed).into_owned()
}

/// Trim ASCII spaces from both ends of a byte slice.
#[inline]
fn trim_spaces(src: &[u8]) -> &[u8] {
    let start = src.iter().position(|&b| b != b' ').unwrap_or(src.len());
    let end = src
        .iter()
        .rposition(|&b| b != b' ')
        .map(|i| i + 1)
        .unwrap_or(start);
    src.get(start..end).unwrap_or(&[])
}

/// Encode an unsigned integer as a 20-byte left-space-padded
/// ASCII decimal field.
fn encode_ascii_u64(value: u64, dst: &mut [u8]) {
    debug_assert_eq!(dst.len(), 20);
    // u64::MAX is 20 digits, so the buffer always fits.
    let mut buf = [0u8; 20];
    let mut n = value;
    let mut i = buf.len();
    if n == 0 {
        i -= 1;
        buf[i] = b'0';
    } else {
        while n > 0 {
            i -= 1;
            buf[i] = b'0' + (n % 10) as u8;
            n /= 10;
        }
    }
    let digits = &buf[i..];
    let pad = dst.len() - digits.len();
    for slot in dst.iter_mut().take(pad) {
        *slot = b' ';
    }
    dst[pad..].copy_from_slice(digits);
}

/// Decode a 20-byte left-space-padded ASCII decimal field. An
/// empty / all-spaces field decodes as `0`.
///
/// # Errors
///
/// Returns [`SoupError::Protocol`] wrapping
/// [`ProtocolError::InvalidEnumCode`] if a non-digit, non-space
/// byte is present, or if the decimal value overflows `u64`.
fn decode_ascii_u64(src: &[u8]) -> Result<u64, SoupError> {
    let trimmed = trim_spaces(src);
    if trimmed.is_empty() {
        return Ok(0);
    }
    let mut value: u64 = 0;
    for &b in trimmed {
        if !b.is_ascii_digit() {
            return Err(SoupError::Protocol(ProtocolError::InvalidEnumCode {
                field: "SoupAsciiNumber",
                code: b,
            }));
        }
        let digit = u64::from(b - b'0');
        value = value
            .checked_mul(10)
            .and_then(|v| v.checked_add(digit))
            .ok_or(SoupError::Protocol(ProtocolError::InvalidEnumCode {
                field: "SoupAsciiNumber",
                code: b,
            }))?;
    }
    Ok(value)
}

// ---------- Codec ----------

/// Length-type-payload framing codec for SoupBinTCP 3.00.
///
/// Plugs into [`tokio_util::codec::Framed`] in either direction.
///
/// On a malformed inbound packet the codec drops the bad bytes
/// and resumes parsing at the next length prefix. The internal
/// `pending_skip` counter handles oversized frames whose announced
/// length had not yet fully arrived — across multiple reads, the
/// remaining bad bytes are absorbed before any new prefix is
/// considered.
#[derive(Debug, Clone, Default)]
pub struct SoupCodec {
    /// Bytes still to discard from upcoming reads before resuming
    /// normal length-prefix parsing.
    pending_skip: usize,
}

impl SoupCodec {
    /// Construct a fresh codec with no pending recovery state.
    #[must_use]
    #[inline]
    pub fn new() -> Self {
        Self::default()
    }
}

impl Decoder for SoupCodec {
    type Item = SoupPacket;
    type Error = SoupError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<SoupPacket>, SoupError> {
        // Drain any leftover oversized payload from a previous
        // read that yielded `FrameTooLarge`.
        if self.pending_skip > 0 {
            let to_drop = self.pending_skip.min(src.len());
            src.advance(to_drop);
            self.pending_skip -= to_drop;
            if self.pending_skip > 0 {
                return Ok(None);
            }
        }

        if src.len() < 2 {
            return Ok(None);
        }
        // `length` excludes its own 2 bytes and includes the type
        // tag.
        let length = u16::from_be_bytes([src[0], src[1]]) as usize;
        if length == 0 {
            // A zero-length packet has no type tag and is not
            // allowed by the spec. Drop the prefix and surface a
            // typed error — the stream is not poisoned.
            src.advance(2);
            return Err(SoupError::FrameTooLarge {
                got: 2,
                max: MAX_MESSAGE_LEN,
            });
        }

        let total = 2 + length;
        if total > MAX_MESSAGE_LEN {
            // Drop the prefix and as much of the bad span as is
            // already buffered; remember how many more bytes need
            // to be discarded on subsequent reads.
            if src.len() >= total {
                src.advance(total);
            } else {
                self.pending_skip = total - src.len();
                src.clear();
            }
            return Err(SoupError::FrameTooLarge {
                got: total,
                max: MAX_MESSAGE_LEN,
            });
        }

        // Wait for the full packet before parsing its body.
        if src.len() < total {
            src.reserve(total - src.len());
            return Ok(None);
        }

        // Consume the length prefix and split off the body
        // (type tag + payload).
        src.advance(2);
        let frame = src.split_to(length);
        // length >= 1 (zero-length rejected above); index 0 is the tag.
        let tag = frame[0];
        let payload = &frame[1..];

        match decode_packet(tag, payload) {
            Ok(pkt) => {
                tracing::debug!(
                    tag = %char::from(tag),
                    len = payload.len(),
                    "decoded soup packet"
                );
                Ok(Some(pkt))
            }
            Err(err) => {
                // Drop-and-resume: the bad bytes are already
                // consumed; the next call begins on the next
                // prefix.
                tracing::warn!(
                    ?err,
                    tag = %char::from(tag),
                    "bad soup packet; dropping"
                );
                Err(err)
            }
        }
    }
}

impl Encoder<SoupPacket> for SoupCodec {
    type Error = SoupError;

    fn encode(&mut self, packet: SoupPacket, dst: &mut BytesMut) -> Result<(), SoupError> {
        let total = packet.encoded_len();
        if total > MAX_MESSAGE_LEN {
            return Err(SoupError::FrameTooLarge {
                got: total,
                max: MAX_MESSAGE_LEN,
            });
        }
        // length prefix excludes itself; it covers tag + payload.
        let length = (1 + packet.payload_len()) as u16;
        dst.reserve(total);
        dst.put_u16(length);
        dst.put_u8(packet.tag());
        write_payload(&packet, dst);
        tracing::debug!(
            tag = %char::from(packet.tag()),
            len = packet.payload_len(),
            "encoded soup packet"
        );
        Ok(())
    }
}

fn write_payload(packet: &SoupPacket, dst: &mut BytesMut) {
    match packet {
        SoupPacket::Debug(p) | SoupPacket::SequencedData(p) | SoupPacket::UnsequencedData(p) => {
            dst.extend_from_slice(p);
        }
        SoupPacket::LoginRequest(req) => {
            let start = dst.len();
            dst.resize(start + LOGIN_REQUEST_PAYLOAD_LEN, 0);
            let buf = &mut dst[start..start + LOGIN_REQUEST_PAYLOAD_LEN];
            pad_right(&req.username, 6, &mut buf[0..6]);
            pad_right(&req.password, 10, &mut buf[6..16]);
            pad_left(&req.requested_session, 10, &mut buf[16..26]);
            encode_ascii_u64(req.requested_sequence, &mut buf[26..46]);
        }
        SoupPacket::LoginAccepted(acc) => {
            let start = dst.len();
            dst.resize(start + LOGIN_ACCEPTED_PAYLOAD_LEN, 0);
            let buf = &mut dst[start..start + LOGIN_ACCEPTED_PAYLOAD_LEN];
            pad_left(&acc.session, 10, &mut buf[0..10]);
            encode_ascii_u64(acc.sequence, &mut buf[10..30]);
        }
        SoupPacket::LoginRejected(reason) => {
            dst.put_u8(reason.to_byte());
        }
        SoupPacket::ServerHeartbeat
        | SoupPacket::ClientHeartbeat
        | SoupPacket::EndOfSession
        | SoupPacket::LogoutRequest => {
            // No payload.
        }
    }
}

fn decode_packet(tag: u8, payload: &[u8]) -> Result<SoupPacket, SoupError> {
    match tag {
        tag::DEBUG => Ok(SoupPacket::Debug(payload.to_vec())),
        tag::LOGIN_REQUEST => {
            ensure_len(tag, payload, LOGIN_REQUEST_PAYLOAD_LEN)?;
            let username = read_anum(&payload[0..6]);
            let password = read_anum(&payload[6..16]);
            let requested_session = read_anum(&payload[16..26]);
            let requested_sequence = decode_ascii_u64(&payload[26..46])?;
            Ok(SoupPacket::LoginRequest(LoginRequest {
                username,
                password,
                requested_session,
                requested_sequence,
            }))
        }
        tag::LOGIN_ACCEPTED => {
            ensure_len(tag, payload, LOGIN_ACCEPTED_PAYLOAD_LEN)?;
            let session = read_anum(&payload[0..10]);
            let sequence = decode_ascii_u64(&payload[10..30])?;
            Ok(SoupPacket::LoginAccepted(LoginAccepted {
                session,
                sequence,
            }))
        }
        tag::LOGIN_REJECTED => {
            ensure_len(tag, payload, LOGIN_REJECTED_PAYLOAD_LEN)?;
            Ok(SoupPacket::LoginRejected(LoginRejectReason::from_byte(
                payload[0],
            )?))
        }
        tag::SEQUENCED_DATA => Ok(SoupPacket::SequencedData(payload.to_vec())),
        tag::UNSEQUENCED_DATA => Ok(SoupPacket::UnsequencedData(payload.to_vec())),
        tag::SERVER_HEARTBEAT => {
            ensure_len(tag, payload, 0)?;
            Ok(SoupPacket::ServerHeartbeat)
        }
        tag::CLIENT_HEARTBEAT => {
            ensure_len(tag, payload, 0)?;
            Ok(SoupPacket::ClientHeartbeat)
        }
        tag::END_OF_SESSION => {
            ensure_len(tag, payload, 0)?;
            Ok(SoupPacket::EndOfSession)
        }
        tag::LOGOUT_REQUEST => {
            ensure_len(tag, payload, 0)?;
            Ok(SoupPacket::LogoutRequest)
        }
        other => Err(SoupError::UnknownPacketType { tag: other }),
    }
}

#[cold]
#[inline(never)]
fn bad_payload_length(tag: u8, expected: usize, got: usize) -> SoupError {
    SoupError::BadPayloadLength { tag, expected, got }
}

#[inline]
fn ensure_len(tag: u8, payload: &[u8], expected: usize) -> Result<(), SoupError> {
    if payload.len() == expected {
        Ok(())
    } else {
        Err(bad_payload_length(tag, expected, payload.len()))
    }
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(packet: SoupPacket) {
        let mut codec = SoupCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(packet.clone(), &mut buf).expect("encode");
        assert_eq!(buf.len(), packet.encoded_len(), "encoded length matches");
        let decoded = codec
            .decode(&mut buf)
            .expect("decode result")
            .expect("frame present");
        assert_eq!(decoded, packet);
        assert!(buf.is_empty(), "buffer drained after one frame");
    }

    #[test]
    fn test_debug_packet_roundtrip() {
        roundtrip(SoupPacket::Debug(b"hello world".to_vec()));
        roundtrip(SoupPacket::Debug(Vec::new()));
    }

    #[test]
    fn test_login_request_roundtrip() {
        roundtrip(SoupPacket::LoginRequest(LoginRequest {
            username: "alice".into(),
            password: "secret".into(),
            requested_session: "sess001".into(),
            requested_sequence: 42,
        }));
    }

    #[test]
    fn test_login_request_blank_session_zero_sequence_wire_shape() {
        // Spec idiom: blank session + sequence=0 means
        // "current session, start from most recent".
        let pkt = SoupPacket::LoginRequest(LoginRequest {
            username: "u".into(),
            password: "p".into(),
            requested_session: String::new(),
            requested_sequence: 0,
        });
        let mut codec = SoupCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(pkt.clone(), &mut buf).expect("encode");
        // length prefix = 47 = 1 tag + 46 payload
        assert_eq!(buf[0..2], [0x00, 0x2F]);
        assert_eq!(buf[2], b'L');
        // username right-padded with spaces.
        assert_eq!(&buf[3..9], b"u     ");
        // password right-padded with spaces.
        assert_eq!(&buf[9..19], b"p         ");
        // requested_session: all spaces.
        assert_eq!(&buf[19..29], b"          ");
        // requested_sequence: 19 spaces + "0".
        assert_eq!(&buf[29..49], b"                   0");
        let decoded = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(decoded, pkt);
    }

    #[test]
    fn test_login_accepted_roundtrip() {
        roundtrip(SoupPacket::LoginAccepted(LoginAccepted {
            session: "S001".into(),
            sequence: 1,
        }));
        roundtrip(SoupPacket::LoginAccepted(LoginAccepted {
            session: String::new(),
            sequence: u64::MAX,
        }));
    }

    #[test]
    fn test_login_rejected_roundtrip_both_reasons() {
        roundtrip(SoupPacket::LoginRejected(LoginRejectReason::NotAuthorized));
        roundtrip(SoupPacket::LoginRejected(
            LoginRejectReason::SessionUnavailable,
        ));
    }

    #[test]
    fn test_login_rejected_unknown_reason_returns_protocol_error() {
        let mut codec = SoupCodec::new();
        let mut buf = BytesMut::new();
        // length=2 (1 tag + 1 reason byte), tag='J', reason='X' (bad).
        buf.put_u16(2);
        buf.put_u8(b'J');
        buf.put_u8(b'X');
        match codec.decode(&mut buf) {
            Err(SoupError::Protocol(ProtocolError::InvalidEnumCode { field, code })) => {
                assert_eq!(field, "LoginRejectReason");
                assert_eq!(code, b'X');
            }
            other => panic!("expected InvalidEnumCode, got {other:?}"),
        }
        assert!(buf.is_empty(), "bad frame consumed; stream not poisoned");
    }

    #[test]
    fn test_sequenced_and_unsequenced_data_roundtrip() {
        roundtrip(SoupPacket::SequencedData(vec![0x00, 0x01, 0xFF, 0xAA]));
        roundtrip(SoupPacket::UnsequencedData(vec![0x55; 200]));
        // Empty payload is legal.
        roundtrip(SoupPacket::SequencedData(Vec::new()));
    }

    #[test]
    fn test_zero_payload_packets_roundtrip() {
        roundtrip(SoupPacket::ServerHeartbeat);
        roundtrip(SoupPacket::ClientHeartbeat);
        roundtrip(SoupPacket::EndOfSession);
        roundtrip(SoupPacket::LogoutRequest);
    }

    #[test]
    fn test_decoder_returns_none_when_starved() {
        let mut codec = SoupCodec::new();
        // No bytes yet.
        let mut buf = BytesMut::new();
        assert!(codec.decode(&mut buf).expect("decode").is_none());

        // Only one byte of the length prefix.
        let mut buf = BytesMut::from(&[0x00u8][..]);
        assert!(codec.decode(&mut buf).expect("decode").is_none());
        assert_eq!(buf.len(), 1, "buffer untouched");

        // Prefix says 5 bytes but only 2 are present.
        let mut buf = BytesMut::from(&[0x00u8, 0x05, b'S', 0xAA][..]);
        assert!(codec.decode(&mut buf).expect("decode").is_none());
        assert_eq!(buf.len(), 4, "buffer untouched while waiting");
    }

    #[test]
    fn test_truncated_login_request_returns_bad_payload_length() {
        // Truncated `L` payload — only 10 bytes instead of 46.
        let mut codec = SoupCodec::new();
        let mut buf = BytesMut::new();
        buf.put_u16(11); // 1 tag + 10 payload
        buf.put_u8(b'L');
        buf.extend_from_slice(&[0u8; 10]);
        match codec.decode(&mut buf) {
            Err(SoupError::BadPayloadLength { tag, expected, got }) => {
                assert_eq!(tag, b'L');
                assert_eq!(expected, LOGIN_REQUEST_PAYLOAD_LEN);
                assert_eq!(got, 10);
            }
            other => panic!("expected BadPayloadLength, got {other:?}"),
        }
        assert!(buf.is_empty(), "bad frame consumed; stream not poisoned");

        // A valid packet must decode after the bad one.
        codec
            .encode(SoupPacket::ServerHeartbeat, &mut buf)
            .expect("encode");
        let pkt = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(pkt, SoupPacket::ServerHeartbeat);
    }

    #[test]
    fn test_oversized_frame_fully_present_drains_and_resumes() {
        let mut codec = SoupCodec::new();
        let mut buf = BytesMut::new();
        // total = 2 + (MAX-1) = 1025 > 1024
        let length = (MAX_MESSAGE_LEN - 1) as u16;
        buf.put_u16(length);
        buf.resize(2 + length as usize, 0);

        let err = codec.decode(&mut buf).expect_err("oversized must error");
        match err {
            SoupError::FrameTooLarge { got, max } => {
                assert_eq!(got, 2 + length as usize);
                assert_eq!(max, MAX_MESSAGE_LEN);
            }
            other => panic!("expected FrameTooLarge, got {other:?}"),
        }
        assert!(buf.is_empty(), "oversized payload drained");

        // Resume with a valid packet.
        codec
            .encode(SoupPacket::EndOfSession, &mut buf)
            .expect("encode");
        let pkt = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(pkt, SoupPacket::EndOfSession);
    }

    #[test]
    fn test_oversized_frame_partial_drains_across_reads() {
        let mut codec = SoupCodec::new();
        let mut buf = BytesMut::new();
        let length = (MAX_MESSAGE_LEN - 1) as u16; // total > MAX
        buf.put_u16(length);
        // First read: only the prefix has arrived.
        let err = codec.decode(&mut buf).expect_err("oversized must error");
        assert!(matches!(err, SoupError::FrameTooLarge { .. }));

        // Drain the rest of the bad payload across multiple reads.
        let mut remaining = length as usize;
        while remaining > 0 {
            let chunk = remaining.min(256);
            buf.resize(chunk, 0);
            let polled = codec.decode(&mut buf).expect("decode");
            assert!(polled.is_none(), "still draining oversized payload");
            remaining -= chunk;
        }
        assert!(buf.is_empty(), "buffer drained");

        // Now a valid packet decodes cleanly.
        codec
            .encode(SoupPacket::ClientHeartbeat, &mut buf)
            .expect("encode");
        let pkt = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(pkt, SoupPacket::ClientHeartbeat);
    }

    #[test]
    fn test_unknown_packet_type_returns_typed_error_and_resumes() {
        let mut codec = SoupCodec::new();
        let mut buf = BytesMut::new();
        // length=1 (tag only), tag='~' (unknown).
        buf.put_u16(1);
        buf.put_u8(b'~');
        match codec.decode(&mut buf) {
            Err(SoupError::UnknownPacketType { tag }) => assert_eq!(tag, b'~'),
            other => panic!("expected UnknownPacketType, got {other:?}"),
        }
        assert!(buf.is_empty(), "bad frame consumed");

        // Valid packet decodes after.
        codec
            .encode(SoupPacket::ServerHeartbeat, &mut buf)
            .expect("encode");
        let pkt = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(pkt, SoupPacket::ServerHeartbeat);
    }

    #[test]
    fn test_zero_length_packet_returns_typed_error_and_resumes() {
        // length=0 has no type tag — bogus packet.
        let mut codec = SoupCodec::new();
        let mut buf = BytesMut::new();
        buf.put_u16(0);
        // Append a valid follow-up packet so we can verify resume.
        codec
            .encode(SoupPacket::LogoutRequest, &mut buf)
            .expect("encode");
        match codec.decode(&mut buf) {
            Err(SoupError::FrameTooLarge { .. }) => {}
            other => panic!("expected FrameTooLarge for zero-length, got {other:?}"),
        }
        let pkt = codec.decode(&mut buf).expect("decode").expect("frame");
        assert_eq!(pkt, SoupPacket::LogoutRequest);
    }

    #[test]
    fn test_two_packets_in_one_buffer_decode_sequentially() {
        let mut codec = SoupCodec::new();
        let mut buf = BytesMut::new();
        codec
            .encode(SoupPacket::ServerHeartbeat, &mut buf)
            .expect("encode");
        codec
            .encode(SoupPacket::LogoutRequest, &mut buf)
            .expect("encode");
        let p1 = codec.decode(&mut buf).expect("decode").expect("frame 1");
        let p2 = codec.decode(&mut buf).expect("decode").expect("frame 2");
        assert_eq!(p1, SoupPacket::ServerHeartbeat);
        assert_eq!(p2, SoupPacket::LogoutRequest);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_login_request_long_username_truncates_to_six_bytes() {
        // A username longer than 6 bytes is truncated on encode by spec.
        let pkt = SoupPacket::LoginRequest(LoginRequest {
            username: "averylongname".into(),
            password: "pw".into(),
            requested_session: String::new(),
            requested_sequence: 1,
        });
        let mut codec = SoupCodec::new();
        let mut buf = BytesMut::new();
        codec.encode(pkt, &mut buf).expect("encode");
        // Bytes [3..9] are the username field — first 6 chars only.
        assert_eq!(&buf[3..9], b"averyl");
        // Decoded username comes back trimmed.
        let decoded = codec.decode(&mut buf).expect("decode").expect("frame");
        match decoded {
            SoupPacket::LoginRequest(req) => assert_eq!(req.username, "averyl"),
            other => panic!("expected LoginRequest, got {other:?}"),
        }
    }

    #[test]
    fn test_decode_ascii_u64_rejects_non_digit() {
        // Inject an `L` packet with a bad ASCII digit in the
        // sequence field.
        let mut codec = SoupCodec::new();
        let mut buf = BytesMut::new();
        buf.put_u16(1 + LOGIN_REQUEST_PAYLOAD_LEN as u16);
        buf.put_u8(b'L');
        buf.extend_from_slice(&[b' '; 6]); // username
        buf.extend_from_slice(&[b' '; 10]); // password
        buf.extend_from_slice(&[b' '; 10]); // session
        buf.extend_from_slice(b"                  9X"); // sequence: trailing 'X'
        match codec.decode(&mut buf) {
            Err(SoupError::Protocol(ProtocolError::InvalidEnumCode { field, code })) => {
                assert_eq!(field, "SoupAsciiNumber");
                assert_eq!(code, b'X');
            }
            other => panic!("expected InvalidEnumCode, got {other:?}"),
        }
    }

    #[test]
    fn test_login_reject_reason_byte_roundtrip() {
        for r in [
            LoginRejectReason::NotAuthorized,
            LoginRejectReason::SessionUnavailable,
        ] {
            assert_eq!(LoginRejectReason::from_byte(r.to_byte()).expect("ok"), r);
        }
        assert!(matches!(
            LoginRejectReason::from_byte(b'Q'),
            Err(SoupError::Protocol(ProtocolError::InvalidEnumCode {
                field: "LoginRejectReason",
                code: b'Q',
            }))
        ));
    }
}
