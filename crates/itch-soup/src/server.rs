//! `SoupServer` — server-side SoupBinTCP 3.00 publisher.
//!
//! Generic over the three [`itch_source`] traits (`MessageSource`,
//! `SeqStore`, `SubscriptionPolicy`), per ADR-0012. One ingest task
//! drains the source into the [`SeqStore`] and a broadcast fan-out;
//! per-connection tasks authenticate the client, replay
//! `policy.warmup()` + the requested sequence range from the store,
//! then attach to the live broadcast.
//!
//! Mirrors the shape of [`itch_tcp::Server`] but speaks the
//! SoupBinTCP packet envelope on every connection:
//!
//! ```text
//!  source ──► ingest task ──► SeqStore ─┬──► broadcast
//!                                       │
//!                                       └──► (replay on reconnect)
//!
//!  client ──► accept ──► login (auth) ──► warmup ──► resume ──► live
//!                                                        ▲
//!                                                        │
//!                                                    broadcast
//! ```
//!
//! Per `docs/TRANSPORT-SPEC.md` §3.4:
//!
//! - The server is responsible for the session id and the server-
//!   side sequence counter (assigned monotonically as messages are
//!   ingested).
//! - On reconnect with `requested_sequence > 0`, the server fast-
//!   forwards by replaying every persisted message from the store
//!   from `requested_sequence` up to `latest()` before attaching to
//!   the live broadcast.
//! - On graceful shutdown the server sends `Z EndOfSession` to every
//!   connected subscriber, then drops the listener.
//! - Subscribers that lag past the broadcast capacity are dropped
//!   (real exchanges drop slow consumers; per spec §3.4).

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use futures::sink::SinkExt;
use futures::stream::StreamExt;
use itch_protocol::Message;
use itch_source::{MessageSource, SeqStore, SourceError, SubscriptionPolicy};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::{LoginAccepted, LoginRejectReason, SoupCodec, SoupPacket};

/// Default broadcast-channel capacity. Slow subscribers that lag
/// past this many messages are dropped per
/// `docs/TRANSPORT-SPEC.md` §3.4.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 4_096;

/// Default unsequenced-inbox channel capacity (`U` packets sent by
/// clients). Beyond this, further `U` packets from a chatty client
/// are dropped with a `tracing::warn!`.
pub const DEFAULT_UNSEQUENCED_INBOX_CAPACITY: usize = 1_024;

/// Default upper bound on shutdown grace — how long [`SoupServer::shutdown`]
/// waits for in-flight per-connection tasks to flush `EndOfSession`
/// before being aborted.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

/// Trait the server consults for username / password validation.
///
/// Auth lives in the transport, not in `itch_source` per ADR-0012 §G.
/// Implementors return `Ok(())` on success and a typed
/// [`LoginRejectReason`] on failure.
pub trait Authenticator: Send + Sync + 'static {
    /// Validate the credentials carried by an `L LoginRequest`.
    ///
    /// # Errors
    ///
    /// Returns the [`LoginRejectReason`] the server will encode
    /// into the `J LoginRejected` reply.
    fn check(&self, username: &str, password: &str) -> Result<(), LoginRejectReason>;
}

/// Authenticator that accepts every login. Useful for tests and
/// demos; production users should plug in their own.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllowAllAuthenticator;

impl Authenticator for AllowAllAuthenticator {
    fn check(&self, _username: &str, _password: &str) -> Result<(), LoginRejectReason> {
        Ok(())
    }
}

/// Authenticator that accepts a single fixed `(username, password)`
/// pair. Convenient for integration tests.
#[derive(Debug, Clone)]
pub struct StaticAuthenticator {
    username: String,
    password: String,
}

impl StaticAuthenticator {
    /// Construct a static authenticator.
    #[must_use]
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: password.into(),
        }
    }
}

impl Authenticator for StaticAuthenticator {
    fn check(&self, username: &str, password: &str) -> Result<(), LoginRejectReason> {
        if username == self.username && password == self.password {
            Ok(())
        } else {
            Err(LoginRejectReason::NotAuthorized)
        }
    }
}

/// Configurable session id type — 10-byte ANUM left-padded with
/// spaces on the wire.
#[derive(Debug, Clone)]
pub struct SoupSession {
    /// Session id, 10 bytes max.
    pub id: String,
}

impl SoupSession {
    /// Construct a session id, truncating to 10 bytes if necessary.
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        let mut s: String = id.into();
        if s.len() > 10 {
            s.truncate(10);
        }
        Self { id: s }
    }
}

/// Generic SoupBinTCP publisher driven by the three `itch-source`
/// traits.
///
/// Wires a single [`MessageSource`] (one ingest task) to many
/// connected subscribers (one task per accept) via a
/// [`tokio::sync::broadcast`] fan-out. New subscribers receive
/// `policy.warmup()` first, then the persisted gap range from the
/// `SeqStore` (if `requested_sequence > 0`), then attach to the live
/// broadcast.
///
/// Each ingest tick:
///
/// 1. Pull the next `Message` from the source.
/// 2. Assign sequence `next_seq` (starting from `start_sequence`).
/// 3. Persist `(next_seq, msg)` to the `SeqStore` **before** any
///    subscriber sees it.
/// 4. Broadcast.
/// 5. `next_seq += 1`.
///
/// Shutdown: dropping the [`SoupServer`] aborts the listener and
/// the ingest task. The cooperative [`SoupServer::shutdown`] sends
/// `Z EndOfSession` to every connected subscriber and waits up to
/// the shutdown grace before tearing down.
pub struct SoupServer<S, St, P>
where
    S: MessageSource + 'static,
    St: SeqStore + 'static,
    P: SubscriptionPolicy + 'static,
{
    listener: TcpListener,
    source: S,
    store: Arc<St>,
    policy: Arc<P>,
    authenticator: Arc<dyn Authenticator>,
    session: SoupSession,
    start_sequence: u64,
    broadcast_capacity: usize,
    shutdown_grace: Duration,
    /// Channel for `U UnsequencedData` packets from connected
    /// clients. Allocated lazily by `unsequenced_inbox()`.
    unsequenced_tx: Option<mpsc::Sender<(SocketAddr, Message)>>,
    unsequenced_rx: Option<mpsc::Receiver<(SocketAddr, Message)>>,
}

impl<S, St, P> SoupServer<S, St, P>
where
    S: MessageSource + 'static,
    St: SeqStore + 'static,
    P: SubscriptionPolicy + 'static,
{
    /// Bind a `SoupServer` to `addr` with the supplied source / store
    /// / policy and accept-anyone authentication. The listener
    /// accepts new connections asynchronously once
    /// [`Self::serve`] is awaited.
    ///
    /// For a typed authenticator use [`Self::bind_with_auth`].
    ///
    /// # Errors
    ///
    /// Returns the underlying `io::Error` if the listener fails to
    /// bind.
    pub async fn bind<A: ToSocketAddrs>(
        addr: A,
        source: S,
        store: St,
        policy: P,
    ) -> io::Result<Self> {
        Self::bind_with_auth(addr, source, store, policy, AllowAllAuthenticator).await
    }

    /// Variant of [`Self::bind`] that takes a typed authenticator.
    ///
    /// # Errors
    ///
    /// Returns the underlying `io::Error` if the listener fails to
    /// bind.
    pub async fn bind_with_auth<A: ToSocketAddrs, Auth: Authenticator>(
        addr: A,
        source: S,
        store: St,
        policy: P,
        authenticator: Auth,
    ) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let (utx, urx) = mpsc::channel::<(SocketAddr, Message)>(DEFAULT_UNSEQUENCED_INBOX_CAPACITY);
        Ok(Self {
            listener,
            source,
            store: Arc::new(store),
            policy: Arc::new(policy),
            authenticator: Arc::new(authenticator),
            session: SoupSession::new("S0001"),
            start_sequence: 1,
            broadcast_capacity: DEFAULT_BROADCAST_CAPACITY,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
            unsequenced_tx: Some(utx),
            unsequenced_rx: Some(urx),
        })
    }

    /// Override the broadcast-channel capacity (default
    /// [`DEFAULT_BROADCAST_CAPACITY`]).
    #[must_use]
    pub fn with_broadcast_capacity(mut self, capacity: usize) -> Self {
        self.broadcast_capacity = capacity.max(1);
        self
    }

    /// Override the session id (default `"S0001"`).
    #[must_use]
    pub fn with_session(mut self, session: SoupSession) -> Self {
        self.session = session;
        self
    }

    /// Override the first sequence number assigned by the ingest
    /// task (default `1`).
    #[must_use]
    pub fn with_start_sequence(mut self, seq: u64) -> Self {
        self.start_sequence = seq.max(1);
        self
    }

    /// Override the shutdown grace (default
    /// [`DEFAULT_SHUTDOWN_GRACE`]).
    #[must_use]
    pub fn with_shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }

    /// Take the receiver end of the unsequenced-data channel. The
    /// next call returns `None`.
    pub fn unsequenced_inbox(&mut self) -> Option<mpsc::Receiver<(SocketAddr, Message)>> {
        self.unsequenced_rx.take()
    }

    /// Local address the listener is bound to.
    ///
    /// # Errors
    ///
    /// Forwards `TcpListener::local_addr` errors.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Run the server until the source ends OR an external shutdown
    /// signal fires. Returns when the source is exhausted and every
    /// subscriber has disconnected.
    ///
    /// # Errors
    ///
    /// Returns the first fatal `io::Error` from the listener's
    /// accept loop (typically only on bind / file-descriptor
    /// exhaustion).
    pub async fn serve(self) -> io::Result<()> {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        self.serve_with_shutdown(shutdown_rx).await
    }

    /// Variant of [`Self::serve`] that listens for an external
    /// shutdown signal. Sending `true` on the watch channel triggers
    /// graceful shutdown: every connected subscriber receives
    /// `Z EndOfSession` before the listener is dropped.
    ///
    /// # Errors
    ///
    /// Same as [`Self::serve`].
    pub async fn serve_with_shutdown(
        self,
        mut shutdown_rx: watch::Receiver<bool>,
    ) -> io::Result<()> {
        let Self {
            listener,
            mut source,
            store,
            policy,
            authenticator,
            session,
            start_sequence,
            broadcast_capacity,
            shutdown_grace,
            unsequenced_tx,
            unsequenced_rx: _,
        } = self;

        let session_id: String = session.id.clone();

        let (tx, _) = broadcast::channel::<(u64, Message)>(broadcast_capacity);

        // Ingest task: drain the source, persist into the store,
        // broadcast.
        let ingest_tx = tx.clone();
        let ingest_store = Arc::clone(&store);
        let ingest = tokio::spawn(async move {
            let mut next_seq = match ingest_store.latest().await {
                Ok(latest) => latest.saturating_add(1).max(start_sequence),
                Err(err) => {
                    warn!(
                        ?err,
                        "store.latest() failed; using start_sequence as fallback"
                    );
                    start_sequence
                }
            };
            while let Some(item) = source.next().await {
                match item {
                    Ok(msg) => {
                        if let Err(err) = ingest_store.store(next_seq, &msg).await {
                            warn!(?err, seq = next_seq, "store.store() failed; dropping frame");
                            continue;
                        }
                        if ingest_tx.send((next_seq, msg)).is_err() {
                            debug!(seq = next_seq, "no live subscribers; broadcast skipped");
                        }
                        next_seq = next_seq.saturating_add(1);
                    }
                    Err(SourceError::Exhausted) => {
                        info!("source exhausted; closing publisher");
                        break;
                    }
                    Err(SourceError::Backend(err)) => {
                        warn!(?err, "source backend error; closing publisher");
                        break;
                    }
                    Err(SourceError::Invariant(reason)) => {
                        warn!(reason, "source invariant violated; closing publisher");
                        break;
                    }
                    Err(other) => {
                        warn!(?other, "unrecognised SourceError; closing publisher");
                        break;
                    }
                }
            }
            // Drop ingest_tx → subscribers' broadcast receiver sees
            // `RecvError::Closed`. Per-connection tasks then send
            // `Z EndOfSession` on their way out.
            drop(ingest_tx);
        });

        // Accept loop. New connections subscribe to the broadcast.
        let accept_tx = tx;
        let mut subscriber_handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    match accept {
                        Ok((sock, peer)) => {
                            let _ = sock.set_nodelay(true);
                            let rx = accept_tx.subscribe();
                            let policy = Arc::clone(&policy);
                            let store = Arc::clone(&store);
                            let auth = Arc::clone(&authenticator);
                            let session_id = session_id.clone();
                            let utx = unsequenced_tx.clone();
                            let handle = tokio::spawn(handle_subscriber(
                                sock, peer, policy, store, auth, session_id, rx, utx,
                            ));
                            subscriber_handles.push(handle);
                        }
                        Err(err) => {
                            warn!(?err, "accept failed");
                        }
                    }
                }
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        info!("external shutdown signal received");
                        break;
                    }
                }
                _ = wait_ingest(&ingest) => {
                    info!("ingest task ended; closing listener");
                    break;
                }
            }
        }
        // Wait for the ingest task to fully unwind.
        let _ = ingest.await;
        // Cooperative grace for in-flight subscriber tasks.
        let _ = tokio::time::timeout(shutdown_grace, async {
            for h in subscriber_handles {
                let _ = h.await;
            }
        })
        .await;
        Ok(())
    }
}

/// Helper future that resolves when the ingest task completes.
async fn wait_ingest(handle: &tokio::task::JoinHandle<()>) {
    while !handle.is_finished() {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_subscriber<P, St>(
    sock: TcpStream,
    peer: SocketAddr,
    policy: Arc<P>,
    store: Arc<St>,
    authenticator: Arc<dyn Authenticator>,
    session_id: String,
    mut rx: broadcast::Receiver<(u64, Message)>,
    unsequenced_tx: Option<mpsc::Sender<(SocketAddr, Message)>>,
) where
    P: SubscriptionPolicy + 'static,
    St: SeqStore + 'static,
{
    let mut framed = Framed::new(sock, SoupCodec::new());

    // 1) Login handshake.
    let req = match framed.next().await {
        Some(Ok(SoupPacket::LoginRequest(r))) => r,
        Some(Ok(other)) => {
            warn!(?peer, tag = %char::from(other.tag()), "non-login packet at start; closing");
            return;
        }
        Some(Err(err)) => {
            warn!(?peer, ?err, "login read failed; closing");
            return;
        }
        None => {
            debug!(?peer, "client disconnected before login");
            return;
        }
    };
    if let Err(reason) = authenticator.check(&req.username, &req.password) {
        let _ = framed.send(SoupPacket::LoginRejected(reason)).await;
        let _ = framed.close().await;
        info!(?peer, ?reason, "login rejected");
        return;
    }
    let trimmed_requested = req.requested_session.trim();
    if !trimmed_requested.is_empty() && trimmed_requested != session_id.trim() {
        let _ = framed
            .send(SoupPacket::LoginRejected(
                LoginRejectReason::SessionUnavailable,
            ))
            .await;
        let _ = framed.close().await;
        info!(
            ?peer,
            requested = %req.requested_session,
            actual = %session_id,
            "login rejected: session mismatch"
        );
        return;
    }

    // Determine the subscriber's resume point.
    let latest = match store.latest().await {
        Ok(l) => l,
        Err(err) => {
            warn!(?peer, ?err, "store.latest() failed; assuming empty");
            0
        }
    };
    let resume_from = if req.requested_sequence == 0 {
        // "Most recent" — start at next live message; tell client
        // about the next sequence we'll emit (latest + 1).
        latest.saturating_add(1)
    } else {
        req.requested_sequence
    };

    if let Err(err) = framed
        .send(SoupPacket::LoginAccepted(LoginAccepted {
            session: session_id.clone(),
            sequence: resume_from,
        }))
        .await
    {
        warn!(?peer, ?err, "send LoginAccepted failed");
        return;
    }
    info!(?peer, session = %session_id, sequence = resume_from, "soup client logged in");

    // 2) Warmup messages from the policy. Replayed *before* the
    // resume range so the subscriber's state machine sees admin
    // messages first.
    match policy.warmup().await {
        Ok(warmup) => {
            for msg in warmup {
                let mut buf = vec![0u8; msg.encoded_len()];
                if msg.encode(&mut buf).is_err() {
                    warn!(?peer, "warmup encode failed; closing connection");
                    return;
                }
                if let Err(err) = framed.send(SoupPacket::SequencedData(buf)).await {
                    warn!(?peer, ?err, "warmup send failed; closing connection");
                    return;
                }
            }
        }
        Err(err) => {
            warn!(?peer, ?err, "policy.warmup() failed; closing connection");
            return;
        }
    }

    // 3) Replay the persisted gap range, in chunks of 256 to keep
    // memory bounded.
    let mut next_expected = resume_from;
    if next_expected <= latest {
        loop {
            match store.range(next_expected, 256).await {
                Ok(range) if range.is_empty() => break,
                Ok(range) => {
                    for (seq, msg) in &range {
                        let mut buf = vec![0u8; msg.encoded_len()];
                        if msg.encode(&mut buf).is_err() {
                            warn!(?peer, "replay encode failed; closing connection");
                            return;
                        }
                        if let Err(err) = framed.send(SoupPacket::SequencedData(buf)).await {
                            warn!(?peer, ?err, "replay send failed; closing connection");
                            return;
                        }
                        next_expected = seq.saturating_add(1);
                    }
                }
                Err(err) => {
                    warn!(?peer, ?err, "store.range() failed; closing connection");
                    return;
                }
            }
        }
    }

    // 4) Live: forward broadcast messages until end-of-stream or
    // the subscriber lags past the broadcast capacity.
    loop {
        tokio::select! {
            // Live broadcast receive.
            broadcast = rx.recv() => match broadcast {
                Ok((seq, msg)) => {
                    // Skip messages already covered by the replay step.
                    if seq < next_expected {
                        continue;
                    }
                    let mut buf = vec![0u8; msg.encoded_len()];
                    if msg.encode(&mut buf).is_err() {
                        warn!(?peer, "live encode failed; closing connection");
                        break;
                    }
                    if let Err(err) = framed.send(SoupPacket::SequencedData(buf)).await {
                        debug!(?peer, ?err, "live send failed; closing connection");
                        break;
                    }
                    next_expected = seq.saturating_add(1);
                }
                Err(broadcast::error::RecvError::Closed) => {
                    debug!(?peer, "broadcast closed; sending EndOfSession");
                    let _ = framed.send(SoupPacket::EndOfSession).await;
                    let _ = framed.close().await;
                    return;
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    warn!(
                        subscriber = ?peer,
                        skipped,
                        "dropping lagging subscriber"
                    );
                    return;
                }
            },
            // Read-half: we may receive `R ClientHeartbeat`,
            // `U UnsequencedData`, `O LogoutRequest`, or `+ Debug`.
            packet = framed.next() => match packet {
                Some(Ok(SoupPacket::LogoutRequest)) => {
                    info!(?peer, "client logout");
                    let _ = framed.close().await;
                    return;
                }
                Some(Ok(SoupPacket::ClientHeartbeat)) | Some(Ok(SoupPacket::Debug(_))) => {
                    continue;
                }
                Some(Ok(SoupPacket::UnsequencedData(payload))) => {
                    if let Some(tx) = unsequenced_tx.as_ref() {
                        match Message::decode(&payload) {
                            Ok(msg) => {
                                if tx.try_send((peer, msg)).is_err() {
                                    debug!(?peer, "unsequenced inbox full; dropping packet");
                                }
                            }
                            Err(err) => {
                                warn!(?peer, ?err, "bad unsequenced ITCH frame; dropping");
                            }
                        }
                    }
                    continue;
                }
                Some(Ok(other)) => {
                    warn!(
                        ?peer,
                        tag = %char::from(other.tag()),
                        "unexpected packet on read half; closing connection"
                    );
                    return;
                }
                Some(Err(err)) => {
                    warn!(?peer, ?err, "read failed; closing connection");
                    return;
                }
                None => {
                    debug!(?peer, "client closed socket");
                    return;
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{login, login_with_timeout, SoupConnection, SoupCredentials, SoupError};
    use itch_protocol::messages::{Header, SystemEvent};
    use itch_protocol::primitives::{StockLocate, Timestamp, TrackingNumber};
    use itch_protocol::EventCode;
    use itch_source::{ChannelSource, NullSeqStore, RingBufferSeqStore, StaticPolicy};
    use std::time::Duration;
    use tokio::net::TcpStream;

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

    /// Build a controlled source whose producer side stays open
    /// until the test drops the sender. Avoids the "ingest task
    /// finishes before any client connects" race.
    fn controlled_source() -> (tokio::sync::mpsc::Sender<Message>, impl MessageSource) {
        ChannelSource::bounded(64)
    }

    async fn login_via_tcp(
        addr: SocketAddr,
        credentials: SoupCredentials,
        requested_session: &str,
        requested_sequence: u64,
    ) -> Result<SoupConnection<TcpStream>, SoupError> {
        let socket = TcpStream::connect(addr).await?;
        login_with_timeout(
            socket,
            credentials,
            requested_session,
            requested_sequence,
            Duration::from_secs(2),
        )
        .await
    }

    #[tokio::test]
    async fn test_server_delivers_messages_in_order() {
        let (src_tx, source) = controlled_source();
        let store = RingBufferSeqStore::new();
        let policy = StaticPolicy::empty();
        let server = SoupServer::bind_with_auth(
            "127.0.0.1:0",
            source,
            store,
            policy,
            StaticAuthenticator::new("alice", "secret"),
        )
        .await
        .expect("bind");
        let addr = server.local_addr().expect("addr");

        let server_task = tokio::spawn(async move { server.serve().await.expect("serve") });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut conn = login_via_tcp(addr, SoupCredentials::new("alice", "secret"), "", 0)
            .await
            .expect("login");

        // Now push 3 messages after the subscriber is connected.
        for _ in 0..3 {
            src_tx.send(sample_message()).await.expect("send");
        }
        // Drop sender → source ends → server sends EOS.
        drop(src_tx);

        let mut got = 0u32;
        loop {
            match conn.next_message().await {
                Some(Ok(_)) => got += 1,
                Some(Err(SoupError::SessionEnded)) => break,
                Some(Err(other)) => panic!("unexpected error: {other:?}"),
                None => break,
            }
        }
        assert_eq!(got, 3);
        let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
    }

    #[tokio::test]
    async fn test_server_rejects_bad_credentials() {
        // Use a never-ending source so the server stays up while we
        // probe authentication.
        let (_src_tx, source) = ChannelSource::bounded(8);
        let server = SoupServer::bind_with_auth(
            "127.0.0.1:0",
            source,
            NullSeqStore::new(),
            StaticPolicy::empty(),
            StaticAuthenticator::new("alice", "secret"),
        )
        .await
        .expect("bind");
        let addr = server.local_addr().expect("addr");
        let server_task = tokio::spawn(async move { server.serve().await.expect("serve") });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Bad password.
        let err = login_via_tcp(addr, SoupCredentials::new("alice", "wrong"), "", 0)
            .await
            .expect_err("must reject");
        match err {
            SoupError::LoginRejected(LoginRejectReason::NotAuthorized) => {}
            other => panic!("expected NotAuthorized, got {other:?}"),
        }
        // Drop _src_tx implicitly at end of scope so server shuts down.
        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn test_server_rejects_session_mismatch() {
        let (_src_tx, source) = ChannelSource::bounded(8);
        let server = SoupServer::bind(
            "127.0.0.1:0",
            source,
            NullSeqStore::new(),
            StaticPolicy::empty(),
        )
        .await
        .expect("bind")
        .with_session(SoupSession::new("S0001"));
        let addr = server.local_addr().expect("addr");
        let server_task = tokio::spawn(async move { server.serve().await.expect("serve") });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let err = login_via_tcp(addr, SoupCredentials::new("alice", "any"), "OTHER", 0)
            .await
            .expect_err("session mismatch");
        match err {
            SoupError::LoginRejected(LoginRejectReason::SessionUnavailable) => {}
            other => panic!("expected SessionUnavailable, got {other:?}"),
        }
        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn test_server_replays_warmup_before_live() {
        // Policy returns one warmup message; source produces 2 live ones.
        let warmup = vec![sample_message()];
        let (src_tx, source) = controlled_source();
        let server = SoupServer::bind(
            "127.0.0.1:0",
            source,
            RingBufferSeqStore::new(),
            StaticPolicy::new(warmup.clone()),
        )
        .await
        .expect("bind");
        let addr = server.local_addr().expect("addr");
        let server_task = tokio::spawn(async move { server.serve().await.expect("serve") });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut conn = login(
            TcpStream::connect(addr).await.expect("connect"),
            SoupCredentials::new("u", "p"),
            "",
            0,
        )
        .await
        .expect("login");

        // Push 2 live messages, then close source to trigger EOS.
        for _ in 0..2 {
            src_tx.send(sample_message()).await.expect("send");
        }
        drop(src_tx);

        let mut got = 0u32;
        while let Some(item) = conn.next_message().await {
            match item {
                Ok(_) => got += 1,
                Err(SoupError::SessionEnded) => break,
                Err(other) => panic!("unexpected error: {other:?}"),
            }
        }
        // Total = warmup (1) + live (2) = 3
        assert_eq!(got, 3);
        let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
    }

    #[tokio::test]
    async fn test_server_two_clients_receive_same_messages() {
        let (src_tx, source) = controlled_source();
        let server = SoupServer::bind(
            "127.0.0.1:0",
            source,
            RingBufferSeqStore::new(),
            StaticPolicy::empty(),
        )
        .await
        .expect("bind");
        let addr = server.local_addr().expect("addr");
        let server_task = tokio::spawn(async move { server.serve().await.expect("serve") });
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Connect both clients before pushing anything, so both see
        // every live message.
        let s_a = TcpStream::connect(addr).await.expect("ca");
        let a = login(s_a, SoupCredentials::new("u", "p"), "", 0)
            .await
            .expect("login a");
        let s_b = TcpStream::connect(addr).await.expect("cb");
        let b = login(s_b, SoupCredentials::new("u", "p"), "", 0)
            .await
            .expect("login b");

        tokio::time::sleep(Duration::from_millis(50)).await;
        for _ in 0..3 {
            src_tx.send(sample_message()).await.expect("send");
        }
        drop(src_tx);

        let drain = |mut conn: SoupConnection<TcpStream>| async move {
            let mut got = 0u32;
            while let Some(item) = conn.next_message().await {
                match item {
                    Ok(_) => got += 1,
                    Err(SoupError::SessionEnded) => break,
                    Err(other) => panic!("unexpected: {other:?}"),
                }
            }
            got
        };
        let task_a = tokio::spawn(async move { drain(a).await });
        let task_b = tokio::spawn(async move { drain(b).await });
        let count_a = task_a.await.expect("a join");
        let count_b = task_b.await.expect("b join");
        assert_eq!(count_a, 3, "client A saw all 3 live messages");
        assert_eq!(count_b, 3, "client B saw all 3 live messages");
        let _ = tokio::time::timeout(Duration::from_secs(2), server_task).await;
    }

    #[tokio::test]
    async fn test_unsequenced_inbox_receives_client_message() {
        let (_src_tx, source) = ChannelSource::bounded(8);
        let mut server = SoupServer::bind(
            "127.0.0.1:0",
            source,
            NullSeqStore::new(),
            StaticPolicy::empty(),
        )
        .await
        .expect("bind");
        let addr = server.local_addr().expect("addr");
        let mut inbox = server.unsequenced_inbox().expect("inbox");
        let server_task = tokio::spawn(async move { server.serve().await.expect("serve") });
        tokio::time::sleep(Duration::from_millis(50)).await;

        let mut conn = login_via_tcp(addr, SoupCredentials::new("u", "p"), "", 0)
            .await
            .expect("login");
        conn.send(sample_message()).await.expect("send U");

        // The unsequenced message should arrive on the inbox channel.
        let received = tokio::time::timeout(Duration::from_secs(1), inbox.recv())
            .await
            .expect("not timed out");
        let (peer, msg) = received.expect("inbox closed unexpectedly");
        assert_eq!(peer.ip().to_string(), "127.0.0.1");
        assert!(matches!(msg, Message::SystemEvent(_)));
        drop(conn);
        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn test_authenticator_default_allows_any() {
        let auth = AllowAllAuthenticator;
        assert!(auth.check("anyone", "any").is_ok());
    }

    #[tokio::test]
    async fn test_static_authenticator_matches() {
        let auth = StaticAuthenticator::new("alice", "secret");
        assert!(auth.check("alice", "secret").is_ok());
        assert_eq!(
            auth.check("alice", "wrong"),
            Err(LoginRejectReason::NotAuthorized)
        );
    }

    #[tokio::test]
    async fn test_session_truncates_long_id() {
        let s = SoupSession::new("ABCDEFGHIJKLMNOP");
        assert_eq!(s.id.len(), 10);
        assert_eq!(s.id, "ABCDEFGHIJ");
    }
}
