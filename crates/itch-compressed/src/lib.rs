//! Compressed via SoupBinTCP — zstd-decompressed ITCH 5.0 over
//! [`itch_soup`].
//!
//! NASDAQ's "Compressed via SoupBinTCP" carries one or more
//! `itch_protocol::Message` values inside each `S Sequenced Data`
//! payload, after a zstd frame. This crate is a thin adapter that
//! wraps the SoupBinTCP packet codec from `itch_soup` and exposes a
//! `Stream<Item = Result<Message, CompressedError>>` so consumers
//! can drop in compressed feeds with a one-line change. See
//! ADR-0011.
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────┐
//!   │       SoupBinTCP packet (`S` Sequenced Data)             │
//!   ├──────────────────────────────────────────────────────────┤
//!   │              zstd-compressed payload                     │
//!   ├──────────────────────────────────────────────────────────┤
//!   │  ITCH msg │ ITCH msg │ ITCH msg │ … (tag + body, back-   │
//!   │           │          │          │  to-back, no length-   │
//!   │           │          │          │  prefix between them)  │
//!   └──────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Stream-poison resistance
//!
//! - A corrupted zstd frame inside an `S` payload yields one
//!   [`CompressedError::Compression`] error and the next packet is
//!   processed cleanly.
//! - A malformed inner ITCH message yields one
//!   [`CompressedError::Protocol`] error; the iterator advances
//!   past the bad bytes by skipping the rest of the decompressed
//!   buffer for that frame, then continues with the next packet.
//!
//! ## Bounded buffering
//!
//! The decompressed scratch buffer is sized per-packet from the
//! decompressed bytes returned by `zstd`. Because the compressed
//! payload is itself bounded by [`itch_soup::MAX_MESSAGE_LEN`] (1
//! KiB), the decompressed size is bounded by
//! [`MAX_DECOMPRESSED_LEN`] (defaults to 64 KiB — a generous cap
//! against zstd bombs). A frame that decompresses to more bytes is
//! rejected with [`CompressedError::Compression`].
//!
//! ## What this crate does NOT do
//!
//! - It does not invent a different compressed wire format from
//!   NASDAQ's documented "Compressed via SoupBinTCP" addendum.
//! - It does not maintain a persistent zstd dictionary across `S`
//!   packets — each packet is a self-contained zstd frame
//!   (matches the addendum semantics that allow per-packet
//!   compression and per-packet recovery).
//! - It does not depend on `itch-tcp` or `itch-mold`. Per ADR-0008
//!   transports are siblings; this crate adapts `itch-soup` only.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use bytes::BytesMut;
use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt};
use itch_protocol::{Message, ProtocolError};
use itch_soup::{
    LoginAccepted, LoginRequest, ResilientSoupConfig, SoupCodec, SoupCredentials, SoupError,
    SoupPacket,
};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio_util::codec::Framed;

/// Maximum decompressed bytes allowed per `S` packet.
///
/// The compressed input is itself bounded by
/// [`itch_soup::MAX_MESSAGE_LEN`] (1 KiB). 64 KiB is a generous
/// upper bound for the decompressed output and rejects pathological
/// zstd frames that would otherwise force the receiver to allocate
/// a huge buffer. Adjustable in a future release if real captures
/// disagree.
pub const MAX_DECOMPRESSED_LEN: usize = 64 * 1024;

/// Default per-handshake timeout for [`CompressedSoupConnection::connect`].
pub const DEFAULT_LOGIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Errors returned by the compressed-soup adapter.
///
/// Marked `#[non_exhaustive]` so additional structured variants can
/// be added in minor releases without breaking matchers.
#[non_exhaustive]
#[derive(Error, Debug)]
pub enum CompressedError {
    /// Underlying SoupBinTCP error (`Io`, `Protocol`, framing,
    /// session, login). Aggregated via `#[from]` so the `?`
    /// operator works at call sites that bubble a [`SoupError`].
    #[error("soup: {0}")]
    Soup(#[from] SoupError),

    /// zstd decompression failed or the decompressed output was
    /// rejected (oversized, truncated). The static `reason` lets
    /// callers pattern-match without parsing.
    #[error("compression: {reason}")]
    Compression {
        /// Static description of the failure.
        reason: &'static str,
    },

    /// Inner ITCH message failed to decode after successful
    /// decompression. Aggregated via `#[from]` so the `?` operator
    /// works at call sites that extract a `Message`.
    #[error("itch protocol: {0}")]
    Protocol(#[from] ProtocolError),
}

impl CompressedError {
    /// Construct a [`CompressedError::Compression`] with a static
    /// reason. Used by the decoder slow path.
    #[cold]
    #[inline(never)]
    fn compression(reason: &'static str) -> Self {
        CompressedError::Compression { reason }
    }
}

// -- Decompression core ------------------------------------------------

/// Decompress one zstd frame into `dst`, returning the number of
/// bytes written. Rejects frames that would exceed
/// [`MAX_DECOMPRESSED_LEN`].
///
/// `dst` is cleared before decompression so the caller does not
/// need to manage offsets.
fn decompress_into(src: &[u8], dst: &mut BytesMut) -> Result<usize, CompressedError> {
    dst.clear();
    let mut writer = LimitedWriter::new(dst, MAX_DECOMPRESSED_LEN);
    zstd::stream::copy_decode(src, &mut writer).map_err(translate_zstd_io_error)?;
    Ok(dst.len())
}

/// Translate a `std::io::Error` from zstd into a typed
/// [`CompressedError::Compression`] with a static reason. zstd
/// surfaces both real I/O errors (we have none — we read from a
/// slice) and decode failures as `io::Error`, so we look at the
/// kind to pick the right reason.
#[cold]
#[inline(never)]
fn translate_zstd_io_error(err: io::Error) -> CompressedError {
    let reason = match err.kind() {
        io::ErrorKind::InvalidData => "zstd invalid data",
        io::ErrorKind::UnexpectedEof => "zstd truncated frame",
        io::ErrorKind::WriteZero => "zstd output exceeds MAX_DECOMPRESSED_LEN",
        _ => "zstd decompression failed",
    };
    tracing::warn!(?err, "compressed: zstd decompression failed");
    CompressedError::Compression { reason }
}

/// `io::Write` adapter over a [`BytesMut`] that returns `WriteZero`
/// when the running output length would exceed `cap`. zstd
/// surfaces `WriteZero` as a recoverable error so we can map it
/// to a typed reason.
struct LimitedWriter<'a> {
    inner: &'a mut BytesMut,
    cap: usize,
}

impl<'a> LimitedWriter<'a> {
    fn new(inner: &'a mut BytesMut, cap: usize) -> Self {
        Self { inner, cap }
    }
}

impl<'a> io::Write for LimitedWriter<'a> {
    fn write(&mut self, src: &[u8]) -> io::Result<usize> {
        let remaining = self.cap.saturating_sub(self.inner.len());
        if remaining == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "compressed: decompressed output exceeds cap",
            ));
        }
        let take = src.len().min(remaining);
        self.inner.extend_from_slice(&src[..take]);
        Ok(take)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// -- CompressedSoupConnection -----------------------------------------

/// Live, post-login compressed-soup connection.
///
/// Wraps a `Framed<TcpStream, SoupCodec>` directly (not a
/// [`itch_soup::SoupConnection`]) because we need access to the raw
/// `SoupPacket::SequencedData` payload bytes — the public
/// `SoupConnection::Stream` impl already auto-decodes its payload
/// as ITCH, which is wrong for compressed feeds where the bytes
/// are zstd-compressed.
///
/// Heartbeat (`H`), end-of-session (`Z`), debug (`+`), and any
/// other non-data packets are filtered exactly like
/// [`itch_soup::SoupConnection`]: heartbeats and debug are
/// consumed silently, end-of-session is surfaced once as
/// [`CompressedError::Soup`] wrapping [`SoupError::SessionEnded`],
/// then the stream returns `None` on subsequent polls.
#[derive(Debug)]
pub struct CompressedSoupConnection<S = TcpStream>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    framed: Framed<S, SoupCodec>,
    /// Reusable scratch buffer that holds the decompressed output
    /// for the *current* `S` packet. Cleared on every fresh packet.
    scratch: BytesMut,
    /// Read cursor into `scratch`. When `cursor == scratch.len()`
    /// the buffer is drained and the next poll fetches a new
    /// packet.
    cursor: usize,
    /// Negotiated session id (from `LoginAccepted`).
    session: String,
    /// Next sequenced-data sequence number we expect to deliver.
    expected_sequence: u64,
    /// Set once a `Z EndOfSession` has been surfaced; subsequent
    /// polls return `Ready(None)`.
    session_ended: bool,
}

impl CompressedSoupConnection<TcpStream> {
    /// Connect, log in, and return a fully-initialised compressed
    /// connection.
    ///
    /// Honours [`DEFAULT_LOGIN_TIMEOUT`].
    ///
    /// # Errors
    ///
    /// - [`CompressedError::Soup`] if the TCP connect fails or the
    ///   SoupBinTCP login handshake errors.
    pub async fn connect(
        addr: SocketAddr,
        credentials: SoupCredentials,
        requested_session: &str,
        requested_sequence: u64,
    ) -> Result<Self, CompressedError> {
        Self::connect_with_timeout(
            addr,
            credentials,
            requested_session,
            requested_sequence,
            DEFAULT_LOGIN_TIMEOUT,
        )
        .await
    }

    /// Variant of [`Self::connect`] with an explicit handshake
    /// timeout.
    ///
    /// # Errors
    ///
    /// Same as [`Self::connect`].
    pub async fn connect_with_timeout(
        addr: SocketAddr,
        credentials: SoupCredentials,
        requested_session: &str,
        requested_sequence: u64,
        timeout: Duration,
    ) -> Result<Self, CompressedError> {
        let socket = TcpStream::connect(addr).await.map_err(SoupError::from)?;
        let _ = socket.set_nodelay(true);
        Self::handshake(
            socket,
            credentials,
            requested_session,
            requested_sequence,
            timeout,
        )
        .await
    }
}

impl<S> CompressedSoupConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// Run the SoupBinTCP login handshake on `stream` and return a
    /// fully-initialised compressed connection on success.
    ///
    /// # Errors
    ///
    /// - [`CompressedError::Soup`] for every login failure mode
    ///   ([`SoupError::LoginRejected`],
    ///   [`SoupError::SessionMismatch`], [`SoupError::PrematureClose`],
    ///   [`SoupError::UnexpectedHandshakePacket`],
    ///   [`SoupError::LoginTimeout`], [`SoupError::Io`]).
    pub async fn handshake(
        stream: S,
        credentials: SoupCredentials,
        requested_session: &str,
        requested_sequence: u64,
        timeout: Duration,
    ) -> Result<Self, CompressedError> {
        let req_session = requested_session.to_string();
        let mut framed = Framed::with_capacity(stream, SoupCodec::new(), 1024);

        let req = SoupPacket::LoginRequest(LoginRequest {
            username: credentials.username,
            password: credentials.password,
            requested_session: req_session.clone(),
            requested_sequence,
        });
        tokio::time::timeout(timeout, framed.send(req))
            .await
            .map_err(|_| SoupError::LoginTimeout(timeout))??;

        loop {
            let next = tokio::time::timeout(timeout, framed.next())
                .await
                .map_err(|_| SoupError::LoginTimeout(timeout))?;
            match next {
                None => return Err(SoupError::PrematureClose.into()),
                Some(Err(e)) => return Err(e.into()),
                Some(Ok(SoupPacket::LoginAccepted(LoginAccepted { session, sequence }))) => {
                    if !req_session.is_empty() && session.trim() != req_session.trim() {
                        return Err(SoupError::SessionMismatch {
                            requested: req_session,
                            got: session,
                        }
                        .into());
                    }
                    tracing::info!(
                        session = %session,
                        sequence,
                        "compressed-soup login accepted"
                    );
                    return Ok(Self {
                        framed,
                        scratch: BytesMut::with_capacity(4096),
                        cursor: 0,
                        session,
                        expected_sequence: sequence,
                        session_ended: false,
                    });
                }
                Some(Ok(SoupPacket::LoginRejected(reason))) => {
                    tracing::warn!(?reason, "compressed-soup login rejected");
                    return Err(SoupError::LoginRejected(reason).into());
                }
                Some(Ok(SoupPacket::Debug(payload))) => {
                    tracing::debug!(len = payload.len(), "ignoring debug during handshake");
                    continue;
                }
                Some(Ok(other)) => {
                    return Err(SoupError::UnexpectedHandshakePacket { tag: other.tag() }.into());
                }
            }
        }
    }

    /// Negotiated session id.
    #[must_use]
    #[inline]
    pub fn session(&self) -> &str {
        &self.session
    }

    /// Sequence number of the next sequenced-data message expected
    /// from the server. Pass into a reconnect's
    /// `requested_sequence` to resume cleanly.
    #[must_use]
    #[inline]
    pub fn next_expected_sequence(&self) -> u64 {
        self.expected_sequence
    }

    /// Receive the next decompressed message.
    ///
    /// Convenience wrapper around the [`Stream`] impl.
    pub async fn next_message(&mut self) -> Option<Result<Message, CompressedError>> {
        StreamExt::next(self).await
    }

    /// Issue a graceful `O LogoutRequest` and consume `self`.
    ///
    /// # Errors
    ///
    /// - [`CompressedError::Soup`] if the writer fails to flush.
    pub async fn logout(mut self) -> Result<(), CompressedError> {
        self.framed.send(SoupPacket::LogoutRequest).await?;
        self.framed.close().await?;
        tracing::info!(session = %self.session, "compressed-soup session logged out");
        Ok(())
    }

    /// Try to decode the next ITCH message from the scratch buffer
    /// at `self.cursor`. Returns:
    ///
    /// - `Ok(Some(msg))` — message decoded; cursor advanced.
    /// - `Ok(None)` — scratch empty or fully drained; caller polls
    ///   the inner `Framed` for the next packet.
    /// - `Err(_)` — protocol error; cursor advanced past the bad
    ///   bytes by clearing the scratch (drop-and-resume on the
    ///   next packet, since we cannot reliably resync inside a
    ///   concatenated frame without length prefixes).
    fn drain_one_message(&mut self) -> Result<Option<Message>, CompressedError> {
        if self.cursor >= self.scratch.len() {
            return Ok(None);
        }
        let buf = &self.scratch[self.cursor..];
        match Message::decode(buf) {
            Ok(msg) => {
                let consumed = msg.encoded_len();
                self.cursor = self.cursor.saturating_add(consumed);
                self.expected_sequence = self.expected_sequence.saturating_add(1);
                Ok(Some(msg))
            }
            Err(err) => {
                tracing::warn!(
                    ?err,
                    cursor = self.cursor,
                    len = self.scratch.len(),
                    "compressed: bad inner ITCH frame; discarding rest of decompressed payload"
                );
                self.scratch.clear();
                self.cursor = 0;
                Err(CompressedError::Protocol(err))
            }
        }
    }
}

impl<S> Stream for CompressedSoupConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    type Item = Result<Message, CompressedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.session_ended {
            return Poll::Ready(None);
        }
        loop {
            // 1. Drain any messages still in the scratch buffer
            //    from the previous decompressed packet.
            match this.drain_one_message() {
                Ok(Some(msg)) => return Poll::Ready(Some(Ok(msg))),
                Ok(None) => {}
                Err(err) => return Poll::Ready(Some(Err(err))),
            }

            // 2. Otherwise pull the next packet from the inner
            //    Framed.
            let packet = match Pin::new(&mut this.framed).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(err))) => {
                    return Poll::Ready(Some(Err(CompressedError::Soup(err))));
                }
                Poll::Ready(Some(Ok(p))) => p,
            };

            match packet {
                SoupPacket::SequencedData(payload) => {
                    match decompress_into(&payload, &mut this.scratch) {
                        Ok(_n) => {
                            this.cursor = 0;
                            tracing::debug!(
                                compressed_len = payload.len(),
                                decompressed_len = this.scratch.len(),
                                "decompressed sequenced-data payload"
                            );
                            // Loop back and drain the first
                            // message.
                        }
                        Err(err) => {
                            this.scratch.clear();
                            this.cursor = 0;
                            return Poll::Ready(Some(Err(err)));
                        }
                    }
                }
                SoupPacket::EndOfSession => {
                    this.session_ended = true;
                    tracing::info!(session = %this.session, "compressed-soup session ended (Z)");
                    return Poll::Ready(Some(Err(CompressedError::Soup(SoupError::SessionEnded))));
                }
                SoupPacket::ServerHeartbeat => {
                    tracing::debug!("compressed-soup: server heartbeat (filtered)");
                    continue;
                }
                SoupPacket::Debug(payload) => {
                    tracing::debug!(
                        len = payload.len(),
                        "compressed-soup: debug packet (dropped)"
                    );
                    continue;
                }
                SoupPacket::LoginAccepted(_) | SoupPacket::LoginRejected(_) => {
                    let tag = packet.tag();
                    return Poll::Ready(Some(Err(CompressedError::Soup(
                        SoupError::UnexpectedHandshakePacket { tag },
                    ))));
                }
                SoupPacket::LoginRequest(_)
                | SoupPacket::UnsequencedData(_)
                | SoupPacket::ClientHeartbeat
                | SoupPacket::LogoutRequest => {
                    return Poll::Ready(Some(Err(CompressedError::Soup(SoupError::SoupFraming {
                        reason: "client-direction packet on a server stream",
                    }))));
                }
            }
        }
    }
}

// -- ResilientCompressedSoupClient -------------------------------------

/// Auto-reconnect wrapper around [`CompressedSoupConnection`].
///
/// Mirrors the contract of [`itch_soup::ResilientSoupClient`] —
/// the reconnect policy, sequence-resume contract, and backoff
/// bounds match issue #17 — but holds a
/// [`CompressedSoupConnection`] rather than a
/// [`itch_soup::SoupConnection`] so we get raw access to the `S`
/// payload bytes for decompression.
pub struct ResilientCompressedSoupClient {
    config: ResilientSoupConfig,
    next_addr: usize,
    attempt: u32,
    last_session: Option<String>,
    next_sequence: u64,
    conn: Option<CompressedSoupConnection<TcpStream>>,
    terminated: bool,
}

impl ResilientCompressedSoupClient {
    /// Construct a fresh resilient compressed client. The first
    /// connection happens lazily on the first poll.
    ///
    /// # Panics
    ///
    /// Panics if `config.addrs` is empty.
    #[must_use]
    pub fn new(config: ResilientSoupConfig) -> Self {
        assert!(
            !config.addrs.is_empty(),
            "ResilientSoupConfig.addrs must not be empty"
        );
        let initial_sequence = config.initial_sequence;
        Self {
            config,
            next_addr: 0,
            attempt: 0,
            last_session: None,
            next_sequence: initial_sequence,
            conn: None,
            terminated: false,
        }
    }

    /// Most recently confirmed session id, or `None` until the
    /// first successful login.
    #[must_use]
    #[inline]
    pub fn last_session(&self) -> Option<&str> {
        self.last_session.as_deref()
    }

    /// Sequence number requested on the next reconnect. Starts at
    /// `config.initial_sequence`; updated to the connection's
    /// `next_expected_sequence` after every successful read.
    #[must_use]
    #[inline]
    pub fn next_expected_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Pull the next decompressed message, transparently
    /// reconnecting on transient errors.
    ///
    /// Mirrors [`itch_soup::ResilientSoupClient::next_message`]:
    /// - `Some(Ok(msg))` — a sequenced-data message.
    /// - `Some(Err(CompressedError::Protocol(_)))` — bad inner
    ///   ITCH; the connection has discarded the rest of the bad
    ///   frame and is still live, no reconnect.
    /// - `Some(Err(CompressedError::Compression { .. }))` — bad
    ///   compressed frame; same recovery, no reconnect.
    /// - `Some(Err(CompressedError::Soup(transient)))` — `Io`,
    ///   `SessionEnded`, `PeerSilent`, `PrematureClose`,
    ///   `LoginTimeout`. The client surfaces the error and starts
    ///   reconnecting on the next call.
    /// - `Some(Err(CompressedError::Soup(fatal)))` —
    ///   `LoginRejected`, `SessionMismatch`. Subsequent calls
    ///   return `None`.
    /// - `None` — terminal state.
    pub async fn next_message(&mut self) -> Option<Result<Message, CompressedError>> {
        if self.terminated {
            return None;
        }
        loop {
            if self.conn.is_none() {
                match self.try_connect_with_backoff().await {
                    Ok(conn) => {
                        self.conn = Some(conn);
                    }
                    Err(err) => {
                        self.terminated = true;
                        return Some(Err(err));
                    }
                }
            }

            let conn = self.conn.as_mut().expect("conn populated above");
            match conn.next_message().await {
                Some(Ok(msg)) => {
                    self.next_sequence = conn.next_expected_sequence();
                    return Some(Ok(msg));
                }
                Some(Err(err)) => {
                    self.next_sequence = conn.next_expected_sequence();
                    if matches!(err, CompressedError::Protocol(_))
                        || matches!(err, CompressedError::Compression { .. })
                    {
                        // Inner-frame errors: connection still
                        // live, surface and keep going.
                        return Some(Err(err));
                    }
                    let soup = match &err {
                        CompressedError::Soup(s) => Some(s),
                        _ => None,
                    };
                    if soup.is_some_and(Self::is_fatal) {
                        tracing::error!(?err, "fatal error mid-stream; terminating");
                        self.terminated = true;
                        self.conn = None;
                        return Some(Err(err));
                    }
                    tracing::warn!(?err, "compressed-soup connection dropped; will reconnect");
                    self.conn = None;
                    return Some(Err(err));
                }
                None => {
                    self.next_sequence = conn.next_expected_sequence();
                    tracing::warn!(
                        "compressed-soup peer closed without EndOfSession; reconnecting"
                    );
                    self.conn = None;
                    continue;
                }
            }
        }
    }

    async fn try_connect_with_backoff(
        &mut self,
    ) -> Result<CompressedSoupConnection<TcpStream>, CompressedError> {
        loop {
            match self.try_connect_once().await {
                Ok(conn) => return Ok(conn),
                Err(err) => {
                    self.attempt = self.attempt.saturating_add(1);

                    let soup = match &err {
                        CompressedError::Soup(s) => Some(s),
                        _ => None,
                    };
                    if soup.is_some_and(Self::is_fatal) {
                        tracing::error!(?err, "fatal error from compressed login; terminating");
                        return Err(err);
                    }
                    if let Some(max) = self.config.max_attempts {
                        if self.attempt >= max {
                            tracing::error!(
                                attempt = self.attempt,
                                max,
                                ?err,
                                "max_attempts exhausted; terminating"
                            );
                            return Err(err);
                        }
                    }

                    let delay = self.backoff_delay();
                    tracing::warn!(
                        attempt = self.attempt,
                        ?delay,
                        ?err,
                        "compressed-soup reconnect failed; backing off"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    async fn try_connect_once(
        &mut self,
    ) -> Result<CompressedSoupConnection<TcpStream>, CompressedError> {
        let addr = self.config.addrs[self.next_addr];
        self.next_addr = (self.next_addr + 1) % self.config.addrs.len();

        let req_session = self.config.requested_session.clone().unwrap_or_default();
        let conn = CompressedSoupConnection::connect_with_timeout(
            addr,
            self.config.credentials.clone(),
            &req_session,
            self.next_sequence,
            self.config.login_timeout,
        )
        .await?;

        tracing::info!(
            ?addr,
            session = %conn.session(),
            sequence = conn.next_expected_sequence(),
            attempt = self.attempt,
            "resilient compressed-soup client (re)connected"
        );

        self.last_session = Some(conn.session().to_string());
        self.next_sequence = conn.next_expected_sequence();
        self.attempt = 0;
        Ok(conn)
    }

    /// Compute the next backoff delay using exponential growth
    /// with uniform jitter, clamped to `[backoff_min, backoff_max]`.
    /// Mirrors [`itch_soup::ResilientSoupClient`]'s `backoff_delay`
    /// math.
    #[cold]
    fn backoff_delay(&self) -> Duration {
        let cfg = &self.config;
        let min_ms = u64::try_from(cfg.backoff_min.as_millis().max(1)).unwrap_or(u64::MAX);
        let max_ms = u64::try_from(cfg.backoff_max.as_millis())
            .unwrap_or(u64::MAX)
            .max(min_ms);
        let exp = self.attempt.min(30);
        let upper_ms = min_ms.checked_shl(exp).unwrap_or(u64::MAX).min(max_ms);
        if upper_ms <= min_ms {
            Duration::from_millis(min_ms)
        } else {
            // Deterministic mid-point — the soup-side resilient
            // client uses real `rand` jitter; here we keep the
            // backoff bounded but predictable to avoid pulling in
            // a `rand` dependency just for the compressed adapter.
            // The caller can swap to `ResilientSoupClient` if they
            // need cryptographic jitter.
            let mid = min_ms.saturating_add((upper_ms - min_ms) / 2);
            Duration::from_millis(mid)
        }
    }

    #[inline]
    fn is_fatal(err: &SoupError) -> bool {
        matches!(
            err,
            SoupError::LoginRejected(_) | SoupError::SessionMismatch { .. }
        )
    }
}

impl ResilientCompressedSoupClient {
    /// Adapt this client into a `Stream<Item = Result<Message,
    /// CompressedError>> + Send + Unpin` for combinator use.
    pub fn into_stream(
        self,
    ) -> impl Stream<Item = Result<Message, CompressedError>> + Send + Unpin {
        Box::pin(futures::stream::unfold(self, |mut client| async move {
            let item = client.next_message().await?;
            Some((item, client))
        }))
    }
}

// -- Public test helpers ----------------------------------------------

/// Encode a slice of `Message` values into one zstd-compressed
/// payload suitable for stuffing into a SoupBinTCP `S Sequenced
/// Data` packet. Concatenates each message's `tag + body` bytes
/// before zstd-compressing.
///
/// Used by tests and by publishers that want to emit compressed
/// `S` packets through a vanilla [`itch_soup::SoupServer`] with a
/// custom encoding hook.
///
/// # Errors
///
/// - [`CompressedError::Protocol`] if any message fails to encode.
/// - [`CompressedError::Compression`] if zstd reports a failure.
pub fn compress_messages(messages: &[Message], level: i32) -> Result<Vec<u8>, CompressedError> {
    let total: usize = messages.iter().map(|m| m.encoded_len()).sum();
    let mut concatenated = Vec::with_capacity(total);
    let mut tmp = Vec::new();
    for m in messages {
        tmp.resize(m.encoded_len(), 0);
        m.encode(&mut tmp)?;
        concatenated.extend_from_slice(&tmp);
    }
    let compressed = zstd::stream::encode_all(&concatenated[..], level).map_err(|err| {
        tracing::warn!(?err, "zstd compress failed");
        CompressedError::compression("zstd compression failed")
    })?;
    Ok(compressed)
}

/// Decompress a single zstd frame produced by
/// [`compress_messages`] and decode every concatenated ITCH
/// message it contains. Used by tests to round-trip the wire shape
/// without going through a [`CompressedSoupConnection`].
///
/// # Errors
///
/// - [`CompressedError::Compression`] if the zstd frame is
///   corrupted or oversized.
/// - [`CompressedError::Protocol`] if any inner ITCH message
///   fails to decode.
pub fn decompress_messages(compressed: &[u8]) -> Result<Vec<Message>, CompressedError> {
    let mut scratch = BytesMut::with_capacity(4096);
    decompress_into(compressed, &mut scratch)?;
    let mut out = Vec::new();
    let mut off = 0;
    while off < scratch.len() {
        let msg = Message::decode(&scratch[off..])?;
        off = off.saturating_add(msg.encoded_len());
        out.push(msg);
    }
    Ok(out)
}

// -- Tests -------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use itch_protocol::messages::{Header, SystemEvent};
    use itch_protocol::primitives::{StockLocate, Timestamp, TrackingNumber};
    use itch_protocol::EventCode;

    fn sample_message(ts: u64) -> Message {
        Message::SystemEvent(SystemEvent {
            header: Header {
                stock_locate: StockLocate::from_u16(0),
                tracking_number: TrackingNumber::from_u16(0),
                timestamp: Timestamp::from_u64(ts),
            },
            event_code: EventCode::StartOfSystemHours,
        })
    }

    /// Test 1 — happy path: 100 pre-compressed ITCH messages
    /// roundtrip cleanly.
    #[test]
    fn test_happy_path_100_compressed_messages_roundtrip() {
        let inputs: Vec<Message> = (0..100).map(|i| sample_message(i as u64)).collect();
        let compressed = compress_messages(&inputs, 0).expect("compress");
        let out = decompress_messages(&compressed).expect("decompress");
        assert_eq!(out.len(), 100);
        for (got, expected) in out.iter().zip(inputs.iter()) {
            assert_eq!(format!("{got:?}"), format!("{expected:?}"));
        }
    }

    /// Test 2 — boundary: 5 ITCH messages inside 1 compressed
    /// payload → 5 yielded.
    #[test]
    fn test_boundary_five_messages_in_one_compressed_payload() {
        let inputs: Vec<Message> = (0..5).map(|i| sample_message(i as u64 + 100)).collect();
        let compressed = compress_messages(&inputs, 3).expect("compress");
        let out = decompress_messages(&compressed).expect("decompress");
        assert_eq!(out.len(), 5);
    }

    /// Test 3 — corrupted frame surfaces `Compression` error.
    #[test]
    fn test_corrupted_compressed_frame_returns_compression_error() {
        let corrupted = vec![0xDE, 0xAD, 0xBE, 0xEF, 0xCA, 0xFE, 0xBA, 0xBE];
        let err = decompress_messages(&corrupted).expect_err("must error");
        match err {
            CompressedError::Compression { reason } => {
                assert!(
                    reason.contains("zstd"),
                    "reason should mention zstd, got {reason:?}"
                );
            }
            other => panic!("expected Compression, got {other:?}"),
        }
    }

    /// Test 4 — corrupted frame followed by valid frame: the next
    /// valid packet decodes after a `Compression` error.
    #[test]
    fn test_corrupted_then_valid_frame_recovers() {
        let corrupted = vec![0x00, 0x01, 0x02, 0x03];
        let inputs = vec![sample_message(7)];
        let valid = compress_messages(&inputs, 0).expect("compress");

        let err = decompress_messages(&corrupted).expect_err("first must error");
        assert!(matches!(err, CompressedError::Compression { .. }));

        let recovered = decompress_messages(&valid).expect("second decodes");
        assert_eq!(recovered.len(), 1);
    }

    /// Test 5 — ITCH error inside decompressed payload surfaces
    /// `Protocol` error.
    #[test]
    fn test_bad_itch_inside_compressed_payload_returns_protocol_error() {
        // Compress a single byte that is NOT a valid ITCH tag.
        let bogus = [b'!'];
        let compressed = zstd::stream::encode_all(&bogus[..], 0).expect("compress raw");
        let err = decompress_messages(&compressed).expect_err("must error");
        match err {
            CompressedError::Protocol(_) => {}
            other => panic!("expected Protocol, got {other:?}"),
        }
    }

    /// Test 6 — oversized frame: a payload that decompresses to
    /// more than `MAX_DECOMPRESSED_LEN` bytes is rejected with
    /// `Compression`.
    #[test]
    fn test_oversized_decompressed_frame_rejected() {
        let big = vec![0u8; MAX_DECOMPRESSED_LEN + 1024];
        let compressed = zstd::stream::encode_all(&big[..], 0).expect("compress");
        // Sanity: compressed size should be tiny.
        assert!(compressed.len() < 4096);
        let err = decompress_messages(&compressed).expect_err("must error");
        match err {
            CompressedError::Compression { reason } => {
                assert!(
                    reason.contains("MAX_DECOMPRESSED_LEN") || reason.contains("zstd"),
                    "reason should reference cap, got {reason:?}"
                );
            }
            other => panic!("expected Compression, got {other:?}"),
        }
    }

    /// Test 7 — empty list of messages: an empty zstd frame
    /// decompresses to zero bytes and yields zero messages.
    #[test]
    fn test_empty_payload_yields_zero_messages() {
        let inputs: Vec<Message> = Vec::new();
        let compressed = compress_messages(&inputs, 0).expect("compress");
        let out = decompress_messages(&compressed).expect("decompress");
        assert!(out.is_empty());
    }

    /// Test 8 — multiple compression levels roundtrip cleanly.
    #[test]
    fn test_multiple_compression_levels_roundtrip() {
        let inputs: Vec<Message> = (0..3).map(|i| sample_message(i as u64)).collect();
        for level in [0i32, 1, 3, 9, 19, 22] {
            let compressed = compress_messages(&inputs, level).expect("compress");
            let out = decompress_messages(&compressed).expect("decompress");
            assert_eq!(out.len(), 3, "level {level}");
        }
    }

    /// Test 9 — `decompress_into` reports the exact byte count and
    /// fills the scratch buffer.
    #[test]
    fn test_decompress_into_reports_exact_byte_count() {
        let inputs: Vec<Message> = (0..3).map(|i| sample_message(i as u64)).collect();
        let compressed = compress_messages(&inputs, 0).expect("compress");
        let mut scratch = BytesMut::new();
        let n = decompress_into(&compressed, &mut scratch).expect("decompress_into");
        assert_eq!(n, scratch.len());
        let total_size: usize = inputs.iter().map(|m| m.encoded_len()).sum();
        assert_eq!(scratch.len(), total_size);
    }

    /// Test 10 — error variants are `Send + Sync`.
    #[test]
    fn test_compressed_error_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<CompressedError>();
    }

    /// Test 11 — `ResilientCompressedSoupClient: Send + Unpin`.
    #[test]
    fn test_resilient_compressed_client_is_send_and_unpin() {
        fn assert_send_unpin<T: Send + Unpin>() {}
        assert_send_unpin::<ResilientCompressedSoupClient>();
    }
}
