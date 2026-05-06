//! Auto-reconnect wrapper around [`SoupConnection`].
//!
//! Per ADR-0009 the base [`SoupConnection`] does **not** auto-reconnect:
//! it surfaces `SessionEnded` / `Io` / `PeerSilent` errors so the user
//! can decide. [`ResilientSoupClient`] wraps that pattern up:
//!
//! - keeps the negotiated session id and `next_expected_sequence`
//!   across socket failures,
//! - reconnects round-robin across a list of `addrs`,
//! - applies exponential backoff with jitter
//!   (`docs/TRANSPORT-SPEC.md` §3.7, `rules/global_rules.md`),
//! - bubbles login rejection / max-attempt-exhaustion as terminal
//!   stream errors.
//!
//! ```text
//!   ┌────────────────┐  Ok(msg)        ┌────────────┐
//!   │ Resilient      │ ──────────────▶ │ user code  │
//!   │ Soup           │                 └────────────┘
//!   │ Client         │  Io / EOS /
//!   │                │  PeerSilent     ┌─────────────┐
//!   │                │ ──────────────▶ │  reconnect  │
//!   │                │                 │  backoff    │
//!   │                │                 │  + jitter   │
//!   │                │ ◀───── new SoupConnection ──── │
//!   └────────────────┘                 └─────────────┘
//! ```
//!
//! Inner ITCH `Protocol` errors are forwarded once and the underlying
//! `SoupConnection` recovers in place — no reconnect is triggered.

use std::net::SocketAddr;
use std::time::Duration;

use futures::stream::Stream;
use itch_protocol::Message;
use rand::Rng;
use tokio::net::TcpStream;

use crate::{login_with_timeout, LoginRejectReason, SoupConnection, SoupCredentials, SoupError};

/// Default minimum backoff between reconnect attempts.
pub const DEFAULT_BACKOFF_MIN: Duration = Duration::from_millis(100);
/// Default maximum backoff between reconnect attempts.
pub const DEFAULT_BACKOFF_MAX: Duration = Duration::from_secs(30);
/// Default per-handshake timeout for reconnect attempts.
pub const DEFAULT_RESILIENT_LOGIN_TIMEOUT: Duration = Duration::from_secs(5);

/// Configuration for [`ResilientSoupClient`].
///
/// Mirrors the spec from `docs/TRANSPORT-SPEC.md` §3.7. All fields
/// are public so a caller can build the struct literal in a const-
/// like way, but the [`Self::new`] constructor is the recommended
/// entry point because it sets correct defaults for the optional
/// fields.
#[derive(Debug, Clone)]
pub struct ResilientSoupConfig {
    /// Addresses to try on each (re)connect, round-robin. At least
    /// one address is required.
    pub addrs: Vec<SocketAddr>,
    /// Username / password for the `Login Request` packet.
    pub credentials: SoupCredentials,
    /// Pinned session id; carried across reconnects unchanged.
    /// `None` means "current session" (`""` on the wire).
    pub requested_session: Option<String>,
    /// First-connect `requested_sequence` (`0` = "most recent" per
    /// spec). Subsequent reconnects use the inner connection's
    /// `next_expected_sequence`.
    pub initial_sequence: u64,
    /// Maximum number of consecutive failed attempts before the
    /// stream ends. `None` means "retry forever".
    pub max_attempts: Option<u32>,
    /// Lower bound on backoff between consecutive reconnect
    /// attempts. Default [`DEFAULT_BACKOFF_MIN`].
    pub backoff_min: Duration,
    /// Upper bound on backoff between consecutive reconnect
    /// attempts. Default [`DEFAULT_BACKOFF_MAX`].
    pub backoff_max: Duration,
    /// Per-handshake timeout. Default
    /// [`DEFAULT_RESILIENT_LOGIN_TIMEOUT`] (shorter than the
    /// `SoupConnection` default 10 s — a stuck handshake during
    /// reconnect is just retry pressure).
    pub login_timeout: Duration,
}

impl ResilientSoupConfig {
    /// Construct a config with sensible defaults.
    ///
    /// `addrs` is the first-and-only failover list.
    /// `requested_session` is `None` (server-pinned).
    /// `initial_sequence` is `0` (most recent).
    /// `max_attempts` is `None` (retry forever).
    #[must_use]
    pub fn new(addrs: Vec<SocketAddr>, credentials: SoupCredentials) -> Self {
        Self {
            addrs,
            credentials,
            requested_session: None,
            initial_sequence: 0,
            max_attempts: None,
            backoff_min: DEFAULT_BACKOFF_MIN,
            backoff_max: DEFAULT_BACKOFF_MAX,
            login_timeout: DEFAULT_RESILIENT_LOGIN_TIMEOUT,
        }
    }

    /// Pin a specific session id across reconnects.
    #[must_use]
    pub fn with_requested_session(mut self, session: impl Into<String>) -> Self {
        self.requested_session = Some(session.into());
        self
    }

    /// Override the first-connect `requested_sequence`. Reconnects
    /// resume from `next_expected_sequence` regardless.
    #[must_use]
    pub fn with_initial_sequence(mut self, seq: u64) -> Self {
        self.initial_sequence = seq;
        self
    }

    /// Cap the number of consecutive failed attempts.
    #[must_use]
    pub fn with_max_attempts(mut self, max: u32) -> Self {
        self.max_attempts = Some(max);
        self
    }

    /// Override backoff bounds.
    #[must_use]
    pub fn with_backoff(mut self, min: Duration, max: Duration) -> Self {
        self.backoff_min = min;
        self.backoff_max = max;
        self
    }

    /// Override the per-handshake login timeout.
    #[must_use]
    pub fn with_login_timeout(mut self, timeout: Duration) -> Self {
        self.login_timeout = timeout;
        self
    }
}

/// Auto-reconnect wrapper around [`SoupConnection`].
///
/// Implements `Stream<Item = Result<Message, SoupError>>` and
/// transparently reconnects on `Io` / `SessionEnded` / `PeerSilent`
/// while preserving the session id and the next-expected sequence
/// number. Login rejection and exhausted retry budget terminate the
/// stream.
///
/// Backoff is exponential with jitter — the actual delay before the
/// `n`-th attempt is sampled uniformly from
/// `[backoff_min, min(backoff_max, backoff_min * 2^n)]`. This avoids
/// thundering-herd behaviour when many clients reconnect in lockstep
/// (`docs/TRANSPORT-SPEC.md` §7.1, `rules/global_rules.md`).
///
/// The implementation drives the inner state machine inside an
/// `async fn next_message`; the [`Stream`] impl polls a single
/// boxed future of that method to stay `Send + Unpin`.
pub struct ResilientSoupClient {
    config: ResilientSoupConfig,
    /// Round-robin cursor into `config.addrs`.
    next_addr: usize,
    /// Number of consecutive failed attempts since the last
    /// successful login. Reset to `0` on a successful login.
    attempt: u32,
    /// Last session id observed via a successful login.
    last_session: Option<String>,
    /// Sequence number to feed into the next `Login Request`. Starts
    /// at `initial_sequence`; updated to `next_expected_sequence`
    /// after every successful read.
    next_sequence: u64,
    /// Live connection (when in the connected state).
    conn: Option<SoupConnection<TcpStream>>,
    /// Set to `true` once a fatal error has been surfaced; subsequent
    /// reads return `None`.
    terminated: bool,
}

impl ResilientSoupClient {
    /// Construct a fresh resilient client. The first attempted
    /// connection happens lazily, on the first `next_message` call.
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

    /// Session id confirmed by the most recent successful login, or
    /// `None` if no login has succeeded yet.
    #[must_use]
    #[inline]
    pub fn last_session(&self) -> Option<&str> {
        self.last_session.as_deref()
    }

    /// The sequence number that will be requested on the next
    /// reconnect. Equals `config.initial_sequence` until the first
    /// successful read; after that, the inner connection's
    /// `next_expected_sequence`.
    #[must_use]
    #[inline]
    pub fn next_expected_sequence(&self) -> u64 {
        self.next_sequence
    }

    /// Receive the next message, transparently reconnecting on
    /// transient errors.
    ///
    /// Returns:
    /// - `Some(Ok(msg))` — a sequenced-data message.
    /// - `Some(Err(SoupError::Protocol(_)))` — bad inner ITCH frame;
    ///   the inner connection has already advanced past it, no
    ///   reconnect.
    /// - `Some(Err(transient))` — `Io` / `SessionEnded` /
    ///   `PeerSilent` / `PrematureClose` / `LoginTimeout`. The
    ///   client surfaces the error and starts the reconnect dance
    ///   on the next call.
    /// - `Some(Err(fatal))` — `LoginRejected` / `SessionMismatch`.
    ///   The next call returns `None`.
    /// - `None` — terminal state reached (fatal error or
    ///   `max_attempts` exhausted).
    pub async fn next_message(&mut self) -> Option<Result<Message, SoupError>> {
        if self.terminated {
            return None;
        }
        loop {
            // 1. Establish a connection if we don't have one.
            if self.conn.is_none() {
                match self.try_connect_with_backoff().await {
                    Ok(conn) => {
                        self.conn = Some(conn);
                    }
                    Err(err) => {
                        // Bubble the terminal error and remember.
                        self.terminated = true;
                        return Some(Err(err));
                    }
                }
            }

            // 2. Read from the live connection.
            let conn = self.conn.as_mut().expect("conn populated above");
            match conn.next_message().await {
                Some(Ok(msg)) => {
                    self.next_sequence = conn.next_expected_sequence();
                    return Some(Ok(msg));
                }
                Some(Err(err)) => {
                    self.next_sequence = conn.next_expected_sequence();
                    if matches!(err, SoupError::Protocol(_)) {
                        // Inner-frame protocol errors do not poison
                        // the underlying socket — surface and keep
                        // the connection live.
                        return Some(Err(err));
                    }
                    if Self::is_fatal(&err) {
                        tracing::error!(?err, "fatal error mid-stream; terminating");
                        self.terminated = true;
                        self.conn = None;
                        return Some(Err(err));
                    }
                    // Transient: surface the error and drop conn so
                    // the next loop iteration triggers reconnect.
                    tracing::warn!(?err, "soup connection dropped; will reconnect");
                    self.conn = None;
                    return Some(Err(err));
                }
                None => {
                    // Peer cleanly closed the socket without `Z`.
                    // Treat as a transient disconnect, no error
                    // surfaced (silent fall-through).
                    self.next_sequence = conn.next_expected_sequence();
                    tracing::warn!("soup peer closed without EndOfSession; reconnecting");
                    self.conn = None;
                    // Loop again to schedule the reconnect.
                    continue;
                }
            }
        }
    }

    /// Drive the connect+login retry loop until success or terminal
    /// failure. On failure returns the structured error.
    async fn try_connect_with_backoff(&mut self) -> Result<SoupConnection<TcpStream>, SoupError> {
        loop {
            match self.try_connect_once().await {
                Ok(conn) => return Ok(conn),
                Err(err) => {
                    self.attempt = self.attempt.saturating_add(1);

                    if Self::is_fatal(&err) {
                        tracing::error!(?err, "fatal error from soup login; terminating");
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
                        "soup reconnect failed; backing off"
                    );
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Attempt one connect → login round-trip. Returns the live
    /// connection on success or the structured error on failure.
    /// On success advances the round-robin cursor and resets the
    /// attempt counter.
    async fn try_connect_once(&mut self) -> Result<SoupConnection<TcpStream>, SoupError> {
        let addr = self.config.addrs[self.next_addr];
        self.next_addr = (self.next_addr + 1) % self.config.addrs.len();

        let socket = TcpStream::connect(addr).await?;
        let _ = socket.set_nodelay(true);

        let req_session = self.config.requested_session.clone().unwrap_or_default();
        let conn = login_with_timeout(
            socket,
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
            "resilient soup client (re)connected"
        );

        self.last_session = Some(conn.session().to_string());
        self.next_sequence = conn.next_expected_sequence();
        self.attempt = 0;
        Ok(conn)
    }

    /// Compute the next backoff delay using exponential growth with
    /// uniform jitter, clamped to `[backoff_min, backoff_max]`.
    #[cold]
    fn backoff_delay(&self) -> Duration {
        let cfg = &self.config;
        let min_ms = u64::try_from(cfg.backoff_min.as_millis().max(1)).unwrap_or(u64::MAX);
        let max_ms = u64::try_from(cfg.backoff_max.as_millis()).unwrap_or(u64::MAX).max(min_ms);
        // Cap exponent so `min_ms << exp` cannot overflow.
        let exp = self.attempt.min(30);
        let upper_ms = min_ms.checked_shl(exp).unwrap_or(u64::MAX).min(max_ms);
        let chosen = if upper_ms <= min_ms {
            min_ms
        } else {
            let mut rng = rand::thread_rng();
            rng.gen_range(min_ms..=upper_ms)
        };
        Duration::from_millis(chosen)
    }

    /// Returns `true` if `err` should terminate the stream
    /// permanently — typically authentication / mismatch failures.
    #[inline]
    fn is_fatal(err: &SoupError) -> bool {
        matches!(
            err,
            SoupError::LoginRejected(LoginRejectReason::NotAuthorized)
                | SoupError::LoginRejected(LoginRejectReason::SessionUnavailable)
                | SoupError::SessionMismatch { .. }
        )
    }
}

impl ResilientSoupClient {
    /// Adapt this client into a `Stream<Item = Result<Message,
    /// SoupError>>` via `futures::stream::unfold`. The returned
    /// stream owns the client; once it ends, the client is
    /// dropped.
    ///
    /// Use this when you need to compose with `StreamExt`
    /// combinators (`for_each`, `take_while`, etc.). For ad-hoc
    /// loops, [`Self::next_message`] is simpler.
    pub fn into_stream(
        self,
    ) -> impl Stream<Item = Result<Message, SoupError>> + Send + Unpin {
        Box::pin(futures::stream::unfold(self, |mut client| async move {
            let item = client.next_message().await?;
            Some((item, client))
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        LoginAccepted, LoginRequest, LoginRejectReason, SoupCodec, SoupCredentials, SoupPacket,
    };
    use futures::{SinkExt, StreamExt};
    use itch_protocol::messages::{Header, SystemEvent};
    use itch_protocol::primitives::{StockLocate, Timestamp, TrackingNumber};
    use itch_protocol::EventCode;
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::Mutex;
    use tokio_util::codec::Framed;

    fn sample_message() -> Message {
        Message::SystemEvent(SystemEvent {
            header: Header {
                stock_locate: StockLocate::from_u16(0),
                tracking_number: TrackingNumber::from_u16(0),
                timestamp: Timestamp::from_u64(0),
            },
            event_code: EventCode::StartOfSystemHours,
        })
    }

    fn sample_payload() -> Vec<u8> {
        let m = sample_message();
        let mut buf = vec![0u8; m.encoded_len()];
        let n = m.encode(&mut buf).expect("encode");
        debug_assert_eq!(n, buf.len());
        buf
    }

    /// Spin up an in-process server that accepts up to N connections
    /// and replays a per-connection script. After the script ends the
    /// server simply drops the socket (so the client sees EOF / Io).
    async fn spawn_scripted_server(
        scripts: Vec<Vec<SoupPacket>>,
        session: String,
    ) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");

        let handle = tokio::spawn(async move {
            for script in scripts {
                let (sock, _peer) = listener.accept().await.expect("accept");
                let mut framed = Framed::new(sock, SoupCodec::new());
                let req = framed.next().await.expect("login req").expect("ok");
                let requested_seq = match req {
                    SoupPacket::LoginRequest(LoginRequest {
                        requested_sequence, ..
                    }) => requested_sequence,
                    other => panic!("expected LoginRequest, got {other:?}"),
                };
                let assigned_seq = if requested_seq == 0 { 1 } else { requested_seq };
                framed
                    .send(SoupPacket::LoginAccepted(LoginAccepted {
                        session: session.clone(),
                        sequence: assigned_seq,
                    }))
                    .await
                    .expect("accepted");
                for pkt in script {
                    if framed.send(pkt).await.is_err() {
                        break;
                    }
                }
                drop(framed);
            }
        });
        (addr, handle)
    }

    #[tokio::test]
    async fn test_resilient_client_resumes_after_socket_drop() {
        // First connection delivers 2 messages and drops; second
        // delivers 2 more then sends Z. Client must observe 4 in
        // total.
        let scripts = vec![
            vec![
                SoupPacket::SequencedData(sample_payload()),
                SoupPacket::SequencedData(sample_payload()),
            ],
            vec![
                SoupPacket::SequencedData(sample_payload()),
                SoupPacket::SequencedData(sample_payload()),
                SoupPacket::EndOfSession,
            ],
        ];
        let (addr, server) = spawn_scripted_server(scripts, "S0001".into()).await;

        let cfg = ResilientSoupConfig::new(
            vec![addr],
            SoupCredentials::new("alice", "secret"),
        )
        .with_backoff(Duration::from_millis(5), Duration::from_millis(20))
        .with_login_timeout(Duration::from_secs(2));
        let mut client = ResilientSoupClient::new(cfg);

        let mut got_ok = 0u32;
        let mut steps = 0;
        loop {
            steps += 1;
            assert!(steps < 50, "infinite loop guard");
            match client.next_message().await {
                Some(Ok(_msg)) => got_ok += 1,
                Some(Err(SoupError::SessionEnded)) => break,
                Some(Err(_)) => continue,
                None => break,
            }
        }
        assert_eq!(got_ok, 4, "delivered every message across the gap");
        assert_eq!(client.last_session(), Some("S0001"));
        // The last sequence we saw must be at least 5 (after 4 ok
        // messages from sequence 1).
        assert!(client.next_expected_sequence() >= 5);
        let _ = server.await;
    }

    #[tokio::test]
    async fn test_resilient_client_login_rejected_is_terminal() {
        // Server accepts one connection and immediately rejects.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");

        let server = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.expect("accept");
            let mut framed = Framed::new(sock, SoupCodec::new());
            let _req = framed.next().await.expect("req").expect("ok");
            framed
                .send(SoupPacket::LoginRejected(LoginRejectReason::NotAuthorized))
                .await
                .expect("rej");
        });

        let cfg = ResilientSoupConfig::new(
            vec![addr],
            SoupCredentials::new("bob", "wrongpw"),
        )
        .with_max_attempts(5);
        let mut client = ResilientSoupClient::new(cfg);

        match client.next_message().await {
            Some(Err(SoupError::LoginRejected(LoginRejectReason::NotAuthorized))) => {}
            other => panic!("expected LoginRejected(NotAuthorized), got {other:?}"),
        }
        // Stream must terminate after a fatal login rejection.
        assert!(
            client.next_message().await.is_none(),
            "stream ends after fatal login rejection"
        );
        let _ = server.await;
    }

    #[tokio::test]
    async fn test_resilient_client_max_attempts_exhausted() {
        // No server listening at this address — every connect fails.
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        // Drop the listener — the address is now closed.
        drop(listener);

        let cfg = ResilientSoupConfig::new(
            vec![addr],
            SoupCredentials::new("alice", "secret"),
        )
        .with_max_attempts(3)
        .with_backoff(Duration::from_millis(1), Duration::from_millis(5))
        .with_login_timeout(Duration::from_millis(50));
        let mut client = ResilientSoupClient::new(cfg);

        // First poll surfaces the third (final) error.
        let err = client
            .next_message()
            .await
            .expect("some error before terminal");
        assert!(err.is_err());
        // Stream then terminates.
        assert!(client.next_message().await.is_none());
    }

    #[tokio::test]
    async fn test_resilient_client_round_robins_addrs() {
        // Two listeners: first immediately drops every connection,
        // second accepts and sends one Sequenced + EndOfSession. The
        // client must round-robin to the second server.
        let lis1 = TcpListener::bind("127.0.0.1:0").await.expect("bind1");
        let addr1 = lis1.local_addr().expect("addr1");

        let lis2 = TcpListener::bind("127.0.0.1:0").await.expect("bind2");
        let addr2 = lis2.local_addr().expect("addr2");

        let bad_server = tokio::spawn(async move {
            for _ in 0..3 {
                let (sock, _) = lis1.accept().await.expect("accept");
                drop(sock);
            }
        });
        let good_server = tokio::spawn(async move {
            let (sock, _) = lis2.accept().await.expect("accept");
            let mut framed = Framed::new(sock, SoupCodec::new());
            let _req = framed.next().await.expect("req").expect("ok");
            framed
                .send(SoupPacket::LoginAccepted(LoginAccepted {
                    session: "GOOD".into(),
                    sequence: 1,
                }))
                .await
                .expect("accepted");
            framed
                .send(SoupPacket::SequencedData(sample_payload()))
                .await
                .expect("data");
            framed
                .send(SoupPacket::EndOfSession)
                .await
                .expect("eos");
        });

        let cfg = ResilientSoupConfig::new(
            vec![addr1, addr2],
            SoupCredentials::new("alice", "secret"),
        )
        .with_backoff(Duration::from_millis(1), Duration::from_millis(5))
        .with_login_timeout(Duration::from_secs(1));
        let mut client = ResilientSoupClient::new(cfg);

        let mut seen_ok = false;
        for _ in 0..30 {
            match client.next_message().await {
                Some(Ok(_)) => {
                    seen_ok = true;
                    break;
                }
                Some(Err(SoupError::SessionEnded)) => break,
                Some(Err(_)) => continue,
                None => break,
            }
        }
        assert!(seen_ok, "client did receive a message via the good server");
        bad_server.abort();
        good_server.abort();
    }

    /// Backoff math invariants: `backoff_delay` returns a value in
    /// `[backoff_min, backoff_max]` regardless of `attempt`.
    #[test]
    fn test_backoff_delay_clamped_to_bounds() {
        let cfg = ResilientSoupConfig::new(
            vec!["127.0.0.1:1".parse().unwrap()],
            SoupCredentials::new("u", "p"),
        )
        .with_backoff(Duration::from_millis(10), Duration::from_millis(500));
        let mut client = ResilientSoupClient::new(cfg);
        for attempt in 0..50 {
            client.attempt = attempt;
            let d = client.backoff_delay();
            assert!(
                d >= Duration::from_millis(10) && d <= Duration::from_millis(500),
                "attempt {attempt} produced delay {d:?} outside bounds"
            );
        }
    }

    /// `last_session` is `None` until first successful login;
    /// `next_expected_sequence` returns `initial_sequence` until then.
    #[tokio::test]
    async fn test_initial_state_observability() {
        let cfg = ResilientSoupConfig::new(
            vec!["127.0.0.1:1".parse().unwrap()],
            SoupCredentials::new("u", "p"),
        )
        .with_initial_sequence(42);
        let client = ResilientSoupClient::new(cfg);
        assert_eq!(client.last_session(), None);
        assert_eq!(client.next_expected_sequence(), 42);
    }

    /// Ensure `ResilientSoupClient: Send + Unpin` so it can drive a
    /// generic `Stream<Item = Result<Message, SoupError>>` consumer.
    #[test]
    fn test_resilient_client_is_send_and_unpin() {
        fn assert_send_unpin<T: Send + Unpin>() {}
        assert_send_unpin::<ResilientSoupClient>();
    }

    /// Smoke test: `into_stream()` produces a usable `Stream`.
    /// Caps `max_attempts(2)` so the client gives up reconnecting
    /// after the script ends (otherwise SessionEnded → reconnect →
    /// connection refused → infinite retry).
    #[tokio::test]
    async fn test_resilient_client_works_via_into_stream() {
        let scripts = vec![vec![
            SoupPacket::SequencedData(sample_payload()),
            SoupPacket::EndOfSession,
        ]];
        let (addr, server) = spawn_scripted_server(scripts, "X".into()).await;
        let cfg = ResilientSoupConfig::new(
            vec![addr],
            SoupCredentials::new("alice", "secret"),
        )
        .with_login_timeout(Duration::from_millis(100))
        .with_backoff(Duration::from_millis(1), Duration::from_millis(5))
        .with_max_attempts(2);
        let client = ResilientSoupClient::new(cfg);

        let mut stream = client.into_stream();
        // First yield: the one Sequenced Data message.
        let m = StreamExt::next(&mut stream).await;
        assert!(matches!(m, Some(Ok(_))));
        // Drain everything else; the stream should end after the
        // `max_attempts(2)` budget is exhausted.
        let mut steps = 0;
        while let Some(_item) = StreamExt::next(&mut stream).await {
            steps += 1;
            assert!(steps < 30, "infinite loop guard");
        }
        let _ = server.await;
    }

    /// Type-system: `Arc<Mutex<ResilientSoupClient>>` compiles.
    #[test]
    fn test_resilient_client_arc_mutex_compiles() {
        let cfg = ResilientSoupConfig::new(
            vec!["127.0.0.1:1".parse().unwrap()],
            SoupCredentials::new("u", "p"),
        );
        let _: Arc<Mutex<ResilientSoupClient>> = Arc::new(Mutex::new(ResilientSoupClient::new(cfg)));
    }
}
