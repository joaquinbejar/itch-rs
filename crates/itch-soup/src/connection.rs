//! SoupBinTCP 3.00 session layer: login state machine, sequenced-data
//! delivery, logout. Built on top of the [`SoupCodec`](crate::SoupCodec)
//! envelope codec from the same crate.
//!
//! Per ADR-0009 the base [`SoupConnection`] does **not** auto-reconnect:
//! it surfaces every fatal session event ([`SoupError::SessionEnded`],
//! [`SoupError::LoginRejected`], [`SoupError::PrematureClose`],
//! underlying I/O errors) as a typed error so the caller can decide
//! whether to re-issue [`login`] with `requested_sequence =
//! next_expected_sequence()`. A higher-level `ResilientSoupClient` will
//! wrap that retry loop in a later issue.
//!
//! ```text
//!   client                                server
//!     │                                     │
//!     │ ─────── L LoginRequest ─────────▶  │
//!     │                                     │
//!     │  ◀───── A LoginAccepted ────────   │   (success)
//!     │  ◀───── J LoginRejected ────────   │   (Err: LoginRejected)
//!     │  ◀───── EOF / RST ──────────────   │   (Err: PrematureClose)
//!     │                                     │
//!     │ ─── (sequenced-data flow) ──────▶  │
//!     │                                     │
//!     │ ─────── O LogoutRequest ─────────▶ │
//!     │                                     │
//! ```
//!
//! The handshake is wrapped in a configurable timeout (default 10 s)
//! to bound the cost of a peer that opens a TCP connection but never
//! sends `A` or `J`. The data-phase has no per-message timeout — that
//! belongs to the heartbeat scheduler in a later issue.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::sink::SinkExt;
use futures::stream::{Stream, StreamExt};
use itch_protocol::Message;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

use crate::{LoginAccepted, LoginRequest, SoupCodec, SoupError, SoupPacket};

/// Default upper bound on how long [`login`] waits for the server's
/// `LoginAccepted` / `LoginRejected` reply before giving up with
/// [`SoupError::LoginTimeout`].
pub const DEFAULT_LOGIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Default upper bound on how long [`SoupConnection::send_unsequenced`]
/// waits for the underlying writer to drain before giving up with an
/// `Io::WouldBlock`-style error. Honours the project-wide "never
/// block forever on send" rule from `docs/TRANSPORT-SPEC.md` §7.1.
pub const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_millis(250);

/// Bounded capacity of the lazy `+` Debug packet channel. Once a
/// caller subscribes via [`SoupConnection::debug_packets`], up to
/// this many packets may queue before further `+` packets are
/// dropped silently (the connection itself never blocks waiting for
/// the user's debug receiver).
const DEBUG_CHANNEL_CAPACITY: usize = 16;

/// Username / password pair sent in the `Login Request` packet.
///
/// The session layer just forwards the strings verbatim; authentication
/// is the server's responsibility and a failed match is reported as a
/// [`SoupError::LoginRejected`] from [`login`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoupCredentials {
    /// Username, ANUM, up to 6 bytes.
    pub username: String,
    /// Password, ANUM, up to 10 bytes.
    pub password: String,
}

impl SoupCredentials {
    /// Construct a new credentials pair.
    #[must_use]
    #[inline]
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

/// A live SoupBinTCP 3.00 client session, post-login.
///
/// Construct one via [`login`] (which performs the handshake and
/// returns `Self` only on `LoginAccepted`). The connection then offers
/// three operations:
///
/// - [`SoupConnection::next_message`] — receive the next sequenced-data
///   message, or surface a typed error when the session ends.
/// - [`SoupConnection::send`] — send an unsequenced ITCH message back
///   to the server.
/// - [`SoupConnection::logout`] — issue a graceful `O LogoutRequest`
///   and consume `self`.
///
/// State that survives across reconnects (the negotiated session id and
/// the next-expected sequence number) is exposed via
/// [`SoupConnection::session`] and
/// [`SoupConnection::next_expected_sequence`] for the
/// `ResilientSoupClient` to feed back into a fresh [`login`] call.
///
/// Heartbeat scheduling and end-of-session pacing are NOT implemented
/// here — they land in subsequent issues per the project roadmap.
#[derive(Debug)]
pub struct SoupConnection<S> {
    framed: Framed<S, SoupCodec>,
    session: String,
    /// Next sequenced-data sequence number we expect to deliver. The
    /// server's `Login Accepted` payload populates this, and each
    /// successful sequenced-data delivery increments it. A sequenced
    /// data packet whose body fails to decode does NOT advance this
    /// counter so a reconnect with `requested_sequence =
    /// next_expected_sequence()` reliably re-fetches the bad
    /// message.
    expected_sequence: u64,
    /// Set once a `Z EndOfSession` has been surfaced as
    /// [`SoupError::SessionEnded`]. Subsequent stream polls return
    /// `None` so consumers see a single typed signal followed by a
    /// clean stream end.
    session_ended: bool,
    /// Bounded sender for `+` Debug packets. Lazily allocated by
    /// [`SoupConnection::debug_packets`]; until the user subscribes,
    /// `Debug` packets are consumed and dropped.
    debug_tx: Option<mpsc::Sender<Vec<u8>>>,
    /// Per-call timeout applied to [`SoupConnection::send_unsequenced`].
    send_timeout: Duration,
}

impl<S> SoupConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    /// The session id the server confirmed in its `Login Accepted`
    /// packet. Pass this back into [`login`] on a reconnect to resume
    /// the same logical session.
    #[must_use]
    #[inline]
    pub fn session(&self) -> &str {
        &self.session
    }

    /// The sequence number of the next sequenced-data message this
    /// connection expects to deliver. Pass this back into [`login`]'s
    /// `requested_sequence` argument on reconnect to resume from the
    /// gap.
    #[must_use]
    #[inline]
    pub fn next_expected_sequence(&self) -> u64 {
        self.expected_sequence
    }

    /// Receive the next ITCH message from the server's sequenced-data
    /// flow.
    ///
    /// Thin async wrapper around the [`Stream`] impl — see the
    /// `impl Stream` block on `SoupConnection` for the complete
    /// filtering rules. Kept for backwards compatibility with the
    /// pre-`Stream` shape; new code should prefer the `Stream` API.
    pub async fn next_message(&mut self) -> Option<Result<Message, SoupError>> {
        // Delegate to the `Stream` impl so the two surfaces stay in
        // lock-step — every filter rule (heartbeat suppression,
        // Debug-packet routing, EOS bookkeeping) lives in one place.
        StreamExt::next(self).await
    }

    /// Override the default per-call send timeout
    /// ([`DEFAULT_SEND_TIMEOUT`]). The connection retains the new
    /// timeout for the rest of its lifetime.
    #[must_use]
    #[inline]
    pub fn with_send_timeout(mut self, timeout: Duration) -> Self {
        self.send_timeout = timeout;
        self
    }

    /// Subscribe to incoming `+` Debug packets.
    ///
    /// Lazy: the channel is only allocated on the first call. A
    /// second call returns a fresh receiver (and disconnects the
    /// previous one — only one subscriber at a time). Packets that
    /// arrive while the channel is full are dropped silently and
    /// counted via `tracing::debug!`.
    #[must_use]
    pub fn debug_packets(&mut self) -> mpsc::Receiver<Vec<u8>> {
        let (tx, rx) = mpsc::channel(DEBUG_CHANNEL_CAPACITY);
        self.debug_tx = Some(tx);
        rx
    }

    /// Send an unsequenced ITCH message to the server (`U` packet).
    ///
    /// Honours the configured send timeout (default
    /// [`DEFAULT_SEND_TIMEOUT`]); a writer that cannot drain the
    /// packet within the deadline returns
    /// `SoupError::Io(io::ErrorKind::WouldBlock)` rather than blocking
    /// forever.
    ///
    /// # Errors
    ///
    /// - [`SoupError::Protocol`] if the message fails to encode.
    /// - [`SoupError::Io`] if the underlying socket write fails or
    ///   the send timeout elapses (kind: `WouldBlock`).
    #[inline]
    pub async fn send(&mut self, msg: Message) -> Result<(), SoupError> {
        self.send_unsequenced(msg).await
    }

    /// Canonical-name variant of [`SoupConnection::send`]. See
    /// `docs/TRANSPORT-SPEC.md` §3.4.
    ///
    /// # Errors
    ///
    /// Same as [`SoupConnection::send`].
    pub async fn send_unsequenced(&mut self, msg: Message) -> Result<(), SoupError> {
        let mut buf = vec![0u8; msg.encoded_len()];
        let n = msg.encode(&mut buf)?;
        debug_assert_eq!(n, buf.len());
        match tokio::time::timeout(
            self.send_timeout,
            self.framed.send(SoupPacket::UnsequencedData(buf)),
        )
        .await
        {
            Ok(res) => res,
            Err(_) => Err(SoupError::Io(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                format!(
                    "send_unsequenced did not complete within {:?}",
                    self.send_timeout
                ),
            ))),
        }
    }

    /// Issue a graceful `O LogoutRequest` and consume `self`.
    ///
    /// Per spec the server may close the socket immediately on receipt
    /// of `O`; this method does **not** wait for an acknowledgement,
    /// it only flushes the request through the writer.
    ///
    /// # Errors
    ///
    /// - [`SoupError::Io`] if the writer fails to flush.
    pub async fn logout(mut self) -> Result<(), SoupError> {
        self.framed.send(SoupPacket::LogoutRequest).await?;
        self.framed.close().await?;
        tracing::info!(session = %self.session, "soup session logged out");
        Ok(())
    }

    fn deliver_sequenced(&mut self, payload: &[u8]) -> Result<Message, SoupError> {
        let msg = Message::decode(payload)?;
        // Sequence numbering: per spec each sequenced-data packet
        // carries one logical message and increments the counter by
        // exactly one. Detected mismatches are tolerated here (the
        // session layer cannot read the wire SeqNo because the spec
        // does not put one on the wire); tracking is done locally
        // for the reconnect loop.
        self.expected_sequence = self.expected_sequence.saturating_add(1);
        Ok(msg)
    }
}

/// `Stream<Item = Result<Message, SoupError>>` — poll-based access
/// to the same data that [`SoupConnection::next_message`] exposes.
///
/// Filter rules (mirror the `next_message` async method):
///
/// - `S` (Sequenced Data): decoded into a `Message`, sequence
///   counter advances **on success only**.
/// - `H` (server heartbeat): consumed silently; stream re-polls.
/// - `+` (Debug): routed to the lazy
///   [`SoupConnection::debug_packets`] receiver if subscribed,
///   otherwise dropped silently; stream re-polls.
/// - `Z` (EndOfSession): emitted **once** as
///   `Some(Err(SoupError::SessionEnded))`; subsequent polls return
///   `Ready(None)`.
/// - Stray `A`/`J` (server-side handshake packets outside the
///   handshake): emitted as
///   `Some(Err(SoupError::UnexpectedHandshakePacket { tag }))`.
/// - Client-direction packet (`L`, `U`, `R`, `O`) on a server
///   stream: emitted as
///   `Some(Err(SoupError::SoupFraming { reason }))`.
///
/// Bad inner-frame `Protocol(_)` errors **do not** advance the
/// sequence counter — this is what lets a reconnect cleanly resume
/// from `next_expected_sequence()`.
impl<S> Stream for SoupConnection<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    type Item = Result<Message, SoupError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // SAFETY-NOTE: only the inner `framed` is moved out via
        // `Pin::new`; the rest of the struct is moved through `&mut`
        // shared via `Pin::get_mut`. `S: Unpin` so this is sound
        // without a `pin_project!`.
        let this = self.get_mut();
        if this.session_ended {
            return Poll::Ready(None);
        }
        loop {
            let packet = match Pin::new(&mut this.framed).poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(None),
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(Some(Ok(p))) => p,
            };
            match packet {
                SoupPacket::SequencedData(payload) => {
                    match this.deliver_sequenced(&payload) {
                        Ok(msg) => {
                            tracing::debug!(seq = this.expected_sequence, "decoded sequenced-data");
                            return Poll::Ready(Some(Ok(msg)));
                        }
                        Err(err) => {
                            tracing::warn!(?err, "bad inner ITCH frame in sequenced data");
                            return Poll::Ready(Some(Err(err)));
                        }
                    }
                }
                SoupPacket::EndOfSession => {
                    this.session_ended = true;
                    tracing::info!(session = %this.session, "soup session ended (Z)");
                    return Poll::Ready(Some(Err(SoupError::SessionEnded)));
                }
                SoupPacket::ServerHeartbeat => {
                    tracing::debug!("server heartbeat (filtered)");
                    continue;
                }
                SoupPacket::Debug(payload) => {
                    if let Some(tx) = this.debug_tx.as_ref() {
                        match tx.try_send(payload) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                tracing::debug!("debug channel full; dropping packet");
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                this.debug_tx = None;
                            }
                        }
                    }
                    continue;
                }
                SoupPacket::LoginAccepted(_) | SoupPacket::LoginRejected(_) => {
                    // Server-only packet outside the handshake — buggy
                    // peer.
                    let tag = packet.tag();
                    return Poll::Ready(Some(Err(SoupError::UnexpectedHandshakePacket { tag })));
                }
                SoupPacket::LoginRequest(_)
                | SoupPacket::UnsequencedData(_)
                | SoupPacket::ClientHeartbeat
                | SoupPacket::LogoutRequest => {
                    // Client-direction packet on a server stream — typed
                    // framing violation per `docs/TRANSPORT-SPEC.md`
                    // §3.4.
                    return Poll::Ready(Some(Err(SoupError::SoupFraming {
                        reason: "client-direction packet on a server stream",
                    })));
                }
            }
        }
    }
}

/// Perform the SoupBinTCP login handshake on `stream`, returning a
/// fully-initialised [`SoupConnection`] on success.
///
/// Wraps `stream` in a [`Framed<_, SoupCodec>`] and:
///
/// 1. Sends `L Login Request` carrying the supplied credentials,
///    `requested_session` (`""` = "current session"), and
///    `requested_sequence` (`0` = "most recent").
/// 2. Awaits the server's reply, ignoring incidental `Debug` packets.
/// 3. Returns the connection on `A LoginAccepted` (after validating
///    the returned session id), or one of:
///    - [`SoupError::LoginRejected`] on `J`,
///    - [`SoupError::SessionMismatch`] on `A` with a session id that
///      doesn't match an explicit non-empty request,
///    - [`SoupError::PrematureClose`] if the peer hangs up before
///      replying,
///    - [`SoupError::UnexpectedHandshakePacket { tag }`] if the
///      server sends something other than `A`/`J`/`+`,
///    - [`SoupError::LoginTimeout`] if the handshake doesn't
///      complete within `timeout` (default
///      [`DEFAULT_LOGIN_TIMEOUT`] when calling
///      [`login`] directly).
///
/// On `LoginAccepted` the connection's `next_expected_sequence` is
/// initialised from the server's reply, **not** from the
/// `requested_sequence` argument: a server is allowed to start at a
/// later sequence (e.g. when the client requested `0` = "most
/// recent").
///
/// # Errors
///
/// See above; all failure modes return a [`SoupError`].
pub async fn login<S>(
    stream: S,
    credentials: SoupCredentials,
    requested_session: &str,
    requested_sequence: u64,
) -> Result<SoupConnection<S>, SoupError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    login_with_timeout(
        stream,
        credentials,
        requested_session,
        requested_sequence,
        DEFAULT_LOGIN_TIMEOUT,
    )
    .await
}

/// Variant of [`login`] that takes an explicit handshake timeout.
///
/// # Errors
///
/// Same as [`login`].
pub async fn login_with_timeout<S>(
    stream: S,
    credentials: SoupCredentials,
    requested_session: &str,
    requested_sequence: u64,
    timeout: Duration,
) -> Result<SoupConnection<S>, SoupError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let req_session = requested_session.to_string();
    let mut framed = Framed::with_capacity(stream, SoupCodec::new(), 1024);

    // 1. Send the Login Request.
    let req = SoupPacket::LoginRequest(LoginRequest {
        username: credentials.username,
        password: credentials.password,
        requested_session: req_session.clone(),
        requested_sequence,
    });
    tokio::time::timeout(timeout, framed.send(req))
        .await
        .map_err(|_| SoupError::LoginTimeout(timeout))??;

    // 2. Await the reply.
    loop {
        let next = tokio::time::timeout(timeout, framed.next())
            .await
            .map_err(|_| SoupError::LoginTimeout(timeout))?;
        match next {
            None => return Err(SoupError::PrematureClose),
            Some(Err(e)) => return Err(e),
            Some(Ok(SoupPacket::LoginAccepted(LoginAccepted { session, sequence }))) => {
                if !req_session.is_empty() && session.trim() != req_session.trim() {
                    return Err(SoupError::SessionMismatch {
                        requested: req_session,
                        got: session,
                    });
                }
                tracing::info!(
                    session = %session,
                    sequence,
                    "soup login accepted"
                );
                return Ok(SoupConnection {
                    framed,
                    session,
                    expected_sequence: sequence,
                    session_ended: false,
                    debug_tx: None,
                    send_timeout: DEFAULT_SEND_TIMEOUT,
                });
            }
            Some(Ok(SoupPacket::LoginRejected(reason))) => {
                tracing::warn!(?reason, "soup login rejected");
                return Err(SoupError::LoginRejected(reason));
            }
            Some(Ok(SoupPacket::Debug(payload))) => {
                tracing::debug!(len = payload.len(), "ignoring debug packet during login");
                continue;
            }
            Some(Ok(other)) => {
                return Err(SoupError::UnexpectedHandshakePacket { tag: other.tag() });
            }
        }
    }
}

// ---------- Tests ----------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LoginRejectReason, SoupCodec};
    use bytes::{BufMut, BytesMut};
    use futures::SinkExt;
    use itch_protocol::messages::{Header, SystemEvent};
    use itch_protocol::primitives::{StockLocate, Timestamp, TrackingNumber};
    use itch_protocol::EventCode;
    use tokio::io::{duplex, AsyncWriteExt, DuplexStream};
    use tokio_util::codec::Framed;

    fn sample_message() -> Message {
        Message::SystemEvent(SystemEvent {
            header: Header {
                stock_locate: StockLocate::from_u16(0),
                tracking_number: TrackingNumber::from_u16(0),
                timestamp: Timestamp::from_u64(123_456_789),
            },
            event_code: EventCode::StartOfSystemHours,
        })
    }

    /// Spin up an in-memory full-duplex socket pair. The first half is
    /// driven by the client under test; the second half plays the
    /// "server" role inside the test.
    fn pair() -> (DuplexStream, DuplexStream) {
        duplex(8192)
    }

    /// Convenience: wrap the server side of the duplex in a
    /// [`Framed<_, SoupCodec>`].
    fn server_framed(s: DuplexStream) -> Framed<DuplexStream, SoupCodec> {
        Framed::new(s, SoupCodec::new())
    }

    fn creds() -> SoupCredentials {
        SoupCredentials::new("alice", "secret")
    }

    #[tokio::test]
    async fn test_successful_login_returns_connection() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        // Run the handshake and the server response concurrently.
        let server_task = tokio::spawn(async move {
            let req = server.next().await.expect("login req").expect("ok");
            match req {
                SoupPacket::LoginRequest(r) => {
                    assert_eq!(r.username, "alice");
                    assert_eq!(r.password, "secret");
                    assert_eq!(r.requested_session, "");
                    assert_eq!(r.requested_sequence, 0);
                }
                other => panic!("expected LoginRequest, got {other:?}"),
            }
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "S0001".into(),
                    sequence: 7,
                }))
                .await
                .expect("send");
            server
        });

        let conn = login(client, creds(), "", 0).await.expect("login ok");
        assert_eq!(conn.session(), "S0001");
        assert_eq!(conn.next_expected_sequence(), 7);

        // Make sure the server task ran cleanly.
        let _server = server_task.await.expect("server join");
    }

    #[tokio::test]
    async fn test_login_rejected_not_authorized_surfaces_typed_error() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _req = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginRejected(LoginRejectReason::NotAuthorized))
                .await
                .expect("send");
        });

        let err = login(client, creds(), "", 0).await.expect_err("rejected");
        match err {
            SoupError::LoginRejected(LoginRejectReason::NotAuthorized) => {}
            other => panic!("expected LoginRejected(NotAuthorized), got {other:?}"),
        }
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_login_rejected_session_unavailable_surfaces_typed_error() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _req = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginRejected(
                    LoginRejectReason::SessionUnavailable,
                ))
                .await
                .expect("send");
        });

        let err = login(client, creds(), "S0001", 1)
            .await
            .expect_err("rejected");
        match err {
            SoupError::LoginRejected(LoginRejectReason::SessionUnavailable) => {}
            other => panic!("expected LoginRejected(SessionUnavailable), got {other:?}"),
        }
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_premature_close_before_reply_returns_premature_close() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _req = server.next().await.expect("req").expect("ok");
            // Drop the server half — the client reads EOF before any
            // reply.
            drop(server);
        });

        let err = login(client, creds(), "", 0)
            .await
            .expect_err("premature close");
        match err {
            SoupError::PrematureClose => {}
            other => panic!("expected PrematureClose, got {other:?}"),
        }
        server_task.await.expect("join");
    }

    #[tokio::test(start_paused = true)]
    async fn test_login_timeout_when_server_silent() {
        let (client, server) = pair();

        // The "server" reads the request but never replies.
        let server_task = tokio::spawn(async move {
            let mut server = server_framed(server);
            let _req = server.next().await.expect("req").expect("ok");
            // Hold the socket open without sending anything.
            tokio::time::sleep(Duration::from_secs(60)).await;
            drop(server);
        });

        let err = login_with_timeout(client, creds(), "", 0, Duration::from_millis(250))
            .await
            .expect_err("timeout");
        match err {
            SoupError::LoginTimeout(d) => assert_eq!(d, Duration::from_millis(250)),
            other => panic!("expected LoginTimeout, got {other:?}"),
        }
        server_task.abort();
    }

    #[tokio::test]
    async fn test_session_mismatch_when_server_returns_different_session() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _req = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "OTHER".into(),
                    sequence: 1,
                }))
                .await
                .expect("send");
        });

        let err = login(client, creds(), "S0001", 1)
            .await
            .expect_err("mismatch");
        match err {
            SoupError::SessionMismatch { requested, got } => {
                assert_eq!(requested, "S0001");
                assert_eq!(got, "OTHER");
            }
            other => panic!("expected SessionMismatch, got {other:?}"),
        }
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_blank_requested_session_accepts_any_returned_session() {
        // Per spec a blank `requested_session` means "current session" —
        // whatever the server returns is accepted, no SessionMismatch.
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _req = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "ANY".into(),
                    sequence: 42,
                }))
                .await
                .expect("send");
        });

        let conn = login(client, creds(), "", 0).await.expect("login ok");
        assert_eq!(conn.session(), "ANY");
        assert_eq!(conn.next_expected_sequence(), 42);
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_unexpected_handshake_packet_returns_typed_error() {
        // Server sends `H` instead of `A` or `J` — protocol violation.
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _req = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::ServerHeartbeat)
                .await
                .expect("send");
        });

        let err = login(client, creds(), "", 0)
            .await
            .expect_err("unexpected packet");
        match err {
            SoupError::UnexpectedHandshakePacket { tag } => assert_eq!(tag, b'H'),
            other => panic!("expected UnexpectedHandshakePacket, got {other:?}"),
        }
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_next_message_delivers_sequenced_data_and_increments_counter() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        // Spawn the "server" side: accept login, then push a single
        // sequenced-data ITCH message followed by `Z`.
        let server_task = tokio::spawn(async move {
            let _req = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "S".into(),
                    sequence: 1,
                }))
                .await
                .expect("send accepted");
            // Encode an ITCH message into the sequenced-data payload.
            let m = sample_message();
            let mut buf = vec![0u8; m.encoded_len()];
            m.encode(&mut buf).expect("encode");
            server
                .send(SoupPacket::SequencedData(buf))
                .await
                .expect("send data");
            server
                .send(SoupPacket::EndOfSession)
                .await
                .expect("send eos");
        });

        let mut conn = login(client, creds(), "", 0).await.expect("login");
        assert_eq!(conn.next_expected_sequence(), 1);

        let m = conn
            .next_message()
            .await
            .expect("some")
            .expect("ok message");
        match m {
            Message::SystemEvent(_) => {}
            other => panic!("expected SystemEvent, got {other:?}"),
        }
        assert_eq!(conn.next_expected_sequence(), 2, "counter incremented");

        match conn.next_message().await.expect("some") {
            Err(SoupError::SessionEnded) => {}
            other => panic!("expected SessionEnded, got {other:?}"),
        }
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_logout_sends_logout_request_packet() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _req = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "S".into(),
                    sequence: 1,
                }))
                .await
                .expect("send accepted");
            // Now wait for the logout packet.
            let pkt = server.next().await.expect("pkt").expect("ok");
            assert_eq!(pkt, SoupPacket::LogoutRequest);
            // Confirm no further packets arrive.
            assert!(server.next().await.is_none());
        });

        let conn = login(client, creds(), "", 0).await.expect("login");
        conn.logout().await.expect("logout ok");
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_send_emits_unsequenced_data_packet() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _req = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "S".into(),
                    sequence: 1,
                }))
                .await
                .expect("send accepted");
            // Read the unsequenced-data packet the client sent and
            // verify it round-trips back to a Message.
            let pkt = server.next().await.expect("pkt").expect("ok");
            match pkt {
                SoupPacket::UnsequencedData(payload) => {
                    let decoded = Message::decode(&payload).expect("decode inner");
                    assert!(matches!(decoded, Message::SystemEvent(_)));
                }
                other => panic!("expected UnsequencedData, got {other:?}"),
            }
        });

        let mut conn = login(client, creds(), "", 0).await.expect("login");
        conn.send(sample_message()).await.expect("send ok");
        // Drop the writer so the server task's `next().await` returns None.
        drop(conn);
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_debug_packet_during_handshake_is_ignored() {
        // Server sends `+` Debug before `A` — the login should still
        // succeed.
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _req = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::Debug(b"informational".to_vec()))
                .await
                .expect("send debug");
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "S".into(),
                    sequence: 5,
                }))
                .await
                .expect("send accepted");
        });

        let conn = login(client, creds(), "", 0).await.expect("login");
        assert_eq!(conn.next_expected_sequence(), 5);
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_premature_close_after_login_request_yields_io_or_premature() {
        // Server reads the login request and immediately closes; client
        // sees EOF on the read half.
        let (client, server) = pair();
        let server_task = tokio::spawn(async move {
            // Read whatever bytes the client wrote, then drop.
            let mut s = server;
            let mut buf = [0u8; 64];
            // Block until at least the login request bytes arrive,
            // then drop without responding.
            tokio::io::AsyncReadExt::read(&mut s, &mut buf)
                .await
                .expect("read");
            drop(s);
        });

        let err = login(client, creds(), "", 0).await.expect_err("must error");
        match err {
            SoupError::PrematureClose | SoupError::Io(_) => {}
            other => panic!("expected PrematureClose or Io, got {other:?}"),
        }
        server_task.await.expect("join");
    }

    /// Sanity: a `Framed<_, SoupCodec>` wrapping the client side of a
    /// duplex pair really does roundtrip a `LoginRequest` whose wire
    /// shape matches the spec (uses `BytesMut` to peek under the
    /// hood).
    #[tokio::test]
    async fn test_login_request_wire_shape_through_framed() {
        let (mut client, server) = pair();
        let mut framed_client = Framed::new(&mut client, SoupCodec::new());
        framed_client
            .send(SoupPacket::LoginRequest(LoginRequest {
                username: "u".into(),
                password: "p".into(),
                requested_session: String::new(),
                requested_sequence: 0,
            }))
            .await
            .expect("send");
        framed_client.flush().await.expect("flush");
        drop(framed_client);
        client.shutdown().await.expect("shutdown");

        // Now read raw bytes from the server side.
        let mut server = server;
        let mut buf = BytesMut::with_capacity(64);
        let mut tmp = [0u8; 64];
        loop {
            let n = tokio::io::AsyncReadExt::read(&mut server, &mut tmp)
                .await
                .expect("read");
            if n == 0 {
                break;
            }
            buf.put_slice(&tmp[..n]);
        }
        // length=47, tag='L', then 6+10+10+20 ASCII bytes.
        assert_eq!(buf[0..2], [0x00, 0x2F]);
        assert_eq!(buf[2], b'L');
    }

    // -----------------------------------------------------------
    // #16 — Stream<Message> + send_unsequenced specific tests.
    // -----------------------------------------------------------

    /// Encode a `Message` exactly as the wire `S` packet would carry
    /// it (1 tag byte + body), so the server-side test can stuff it
    /// into a `SequencedData` payload directly.
    fn message_payload(msg: &Message) -> Vec<u8> {
        let mut buf = vec![0u8; msg.encoded_len()];
        let n = msg.encode(&mut buf).expect("encode");
        debug_assert_eq!(n, buf.len());
        buf
    }

    #[tokio::test]
    async fn test_stream_yields_sequenced_messages_in_order_and_advances_counter() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _ = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "STREAM".into(),
                    sequence: 100,
                }))
                .await
                .expect("send accepted");
            for _ in 0..3 {
                let payload = message_payload(&sample_message());
                server
                    .send(SoupPacket::SequencedData(payload))
                    .await
                    .expect("send S");
            }
            // Send Z so the stream terminates cleanly.
            server.send(SoupPacket::EndOfSession).await.expect("send Z");
            // Yield until the client closes.
            let _ = server.next().await;
        });

        let mut conn = login(client, creds(), "", 0).await.expect("login");
        assert_eq!(conn.next_expected_sequence(), 100);

        let mut got = 0u32;
        while let Some(item) = conn.next().await {
            match item {
                Ok(_msg) => got += 1,
                Err(SoupError::SessionEnded) => break,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
        }
        assert_eq!(got, 3);
        assert_eq!(conn.next_expected_sequence(), 103);

        // Stream terminates after SessionEnded.
        assert!(conn.next().await.is_none());

        // Drop conn so the server's lingering read returns None.
        drop(conn);
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_stream_bad_inner_frame_does_not_advance_counter_or_poison() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _ = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "BAD".into(),
                    sequence: 50,
                }))
                .await
                .expect("send accepted");
            // 1) Garbage payload (single byte that decodes as an
            //    unknown ITCH tag).
            server
                .send(SoupPacket::SequencedData(vec![b'?']))
                .await
                .expect("send bad");
            // 2) Good payload immediately after.
            server
                .send(SoupPacket::SequencedData(message_payload(&sample_message())))
                .await
                .expect("send good");
            server.send(SoupPacket::EndOfSession).await.expect("send Z");
            let _ = server.next().await;
        });

        let mut conn = login(client, creds(), "", 0).await.expect("login");
        let initial_seq = conn.next_expected_sequence();
        assert_eq!(initial_seq, 50);

        // First poll: protocol error from the bad payload, counter
        // does NOT move.
        match conn.next().await {
            Some(Err(SoupError::Protocol(_))) => {}
            other => panic!("expected Protocol error, got {other:?}"),
        }
        assert_eq!(conn.next_expected_sequence(), initial_seq);

        // Second poll: good message decodes, counter advances by 1.
        match conn.next().await {
            Some(Ok(_)) => {}
            other => panic!("expected Ok(msg), got {other:?}"),
        }
        assert_eq!(conn.next_expected_sequence(), initial_seq + 1);

        // Third poll: SessionEnded.
        match conn.next().await {
            Some(Err(SoupError::SessionEnded)) => {}
            other => panic!("expected SessionEnded, got {other:?}"),
        }
        assert!(conn.next().await.is_none());

        // Drop the client side so the server's lingering read returns
        // None and the spawned task can join.
        drop(conn);
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_stream_filters_heartbeats_and_debug_silently() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _ = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "FILTER".into(),
                    sequence: 1,
                }))
                .await
                .expect("send accepted");
            // Pepper the stream with H/+/H around two real S packets.
            server
                .send(SoupPacket::ServerHeartbeat)
                .await
                .expect("h1");
            server
                .send(SoupPacket::SequencedData(message_payload(&sample_message())))
                .await
                .expect("s1");
            server
                .send(SoupPacket::Debug(b"info".to_vec()))
                .await
                .expect("d");
            server
                .send(SoupPacket::ServerHeartbeat)
                .await
                .expect("h2");
            server
                .send(SoupPacket::SequencedData(message_payload(&sample_message())))
                .await
                .expect("s2");
            server.send(SoupPacket::EndOfSession).await.expect("z");
            let _ = server.next().await;
        });

        let mut conn = login(client, creds(), "", 0).await.expect("login");
        let m1 = conn.next().await.expect("m1").expect("ok");
        let m2 = conn.next().await.expect("m2").expect("ok");
        assert_eq!(format!("{m1:?}"), format!("{:?}", sample_message()));
        assert_eq!(format!("{m2:?}"), format!("{:?}", sample_message()));
        match conn.next().await {
            Some(Err(SoupError::SessionEnded)) => {}
            other => panic!("expected SessionEnded, got {other:?}"),
        }
        assert!(conn.next().await.is_none());

        drop(conn);
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_stream_client_direction_packet_yields_soup_framing_error() {
        // Server (buggy) sends a `R ClientHeartbeat` — that's a
        // client → server packet — on the read half. Per
        // `docs/TRANSPORT-SPEC.md` §3.4 we surface `SoupFraming`.
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            let _ = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "BUGGY".into(),
                    sequence: 1,
                }))
                .await
                .expect("send accepted");
            server
                .send(SoupPacket::ClientHeartbeat)
                .await
                .expect("send R");
        });

        let mut conn = login(client, creds(), "", 0).await.expect("login");
        match conn.next().await {
            Some(Err(SoupError::SoupFraming { reason })) => {
                assert!(reason.contains("client-direction"));
            }
            other => panic!("expected SoupFraming, got {other:?}"),
        }
        server_task.await.expect("join");
    }

    #[tokio::test]
    async fn test_send_unsequenced_writes_a_u_packet() {
        let (client, server) = pair();
        let mut server = server_framed(server);

        let server_task = tokio::spawn(async move {
            // 1) Login.
            let _ = server.next().await.expect("req").expect("ok");
            server
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "U".into(),
                    sequence: 0,
                }))
                .await
                .expect("send accepted");
            // 2) Expect one U packet from client.
            let pkt = server.next().await.expect("u").expect("ok");
            match pkt {
                SoupPacket::UnsequencedData(payload) => {
                    let m = Message::decode(&payload).expect("decode");
                    assert_eq!(format!("{m:?}"), format!("{:?}", sample_message()));
                }
                other => panic!("expected UnsequencedData, got {other:?}"),
            }
        });

        let mut conn = login(client, creds(), "", 0).await.expect("login");
        conn.send_unsequenced(sample_message())
            .await
            .expect("send_unsequenced");
        drop(conn);
        server_task.await.expect("join");
    }
}
