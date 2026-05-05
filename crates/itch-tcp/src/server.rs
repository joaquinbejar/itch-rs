//! `Server` — generic over the three `itch-source` traits.
//!
//! The canonical 0.2 publisher entry point:
//!
//! ```no_run
//! use itch_source::{IteratorSource, RingBufferSeqStore, StaticPolicy, canonical_session};
//! use itch_tcp::Server;
//!
//! # async fn run() -> std::io::Result<()> {
//! let source = IteratorSource::new(canonical_session());
//! let store  = RingBufferSeqStore::new();
//! let policy = StaticPolicy::empty();
//!
//! Server::bind("127.0.0.1:0", source, store, policy)
//!     .await?
//!     .serve()
//!     .await?;
//! # Ok(()) }
//! ```
//!
//! See `docs/ITCH-SOURCE.md` §5 (transport consumption pattern)
//! and §8 (backpressure).

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
use itch_protocol::Message;
use itch_source::{MessageSource, SeqStore, SourceError, SubscriptionPolicy};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::broadcast;
use tokio_util::codec::Framed;
use tracing::{debug, info, warn};

use crate::ItchCodec;

/// Default broadcast-channel capacity. Slow subscribers that lag
/// past this many messages are dropped per `docs/ITCH-SOURCE.md`
/// §8.
pub const DEFAULT_BROADCAST_CAPACITY: usize = 4_096;

/// Generic TCP publisher driven by the three `itch-source` traits.
///
/// Wires a single `MessageSource` (one ingest task) to many
/// connected subscribers (one task per accept) via a
/// [`tokio::sync::broadcast`] fan-out. Each new subscriber receives
/// `policy.warmup()` messages first, then attaches to the live
/// broadcast.
///
/// Per-message flow:
///
/// 1. Ingest task pulls the next `Message` from the source.
/// 2. Sequence number is assigned (`store.latest() + 1`).
/// 3. `(seq, msg)` is **persisted** to the `SeqStore` — *before*
///    any subscriber sees the bytes.
/// 4. The message is broadcast to every connected writer.
///
/// On source exhaustion (stream end, `SourceError::Exhausted`,
/// backend error) the broadcast sender is dropped, every
/// subscriber sees `RecvError::Closed` and disconnects cleanly.
pub struct Server<S, St, P>
where
    S: MessageSource + 'static,
    St: SeqStore + 'static,
    P: SubscriptionPolicy + 'static,
{
    listener: TcpListener,
    source: S,
    store: Arc<St>,
    policy: Arc<P>,
    broadcast_capacity: usize,
}

impl<S, St, P> Server<S, St, P>
where
    S: MessageSource + 'static,
    St: SeqStore + 'static,
    P: SubscriptionPolicy + 'static,
{
    /// Bind a `Server` to `addr`. The listener accepts new
    /// connections asynchronously once [`Self::serve`] is awaited.
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
        let listener = TcpListener::bind(addr).await?;
        Ok(Self {
            listener,
            source,
            store: Arc::new(store),
            policy: Arc::new(policy),
            broadcast_capacity: DEFAULT_BROADCAST_CAPACITY,
        })
    }

    /// Override the broadcast-channel capacity (default
    /// [`DEFAULT_BROADCAST_CAPACITY`]).
    #[must_use]
    pub fn with_broadcast_capacity(mut self, capacity: usize) -> Self {
        self.broadcast_capacity = capacity.max(1);
        self
    }

    /// Local address the listener is bound to.
    ///
    /// # Errors
    ///
    /// Forwards `TcpListener::local_addr` errors.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    /// Run the server until the source ends. Returns when the
    /// source is exhausted and every subscriber has disconnected.
    ///
    /// # Errors
    ///
    /// Returns the first fatal `io::Error` from the listener's
    /// accept loop.
    pub async fn serve(self) -> io::Result<()> {
        let Self {
            listener,
            mut source,
            store,
            policy,
            broadcast_capacity,
        } = self;

        let (tx, _) = broadcast::channel::<Message>(broadcast_capacity);

        // Ingest task: drain the source, persist into the store,
        // broadcast. When the source closes (any way), we drop the
        // broadcast sender so subscribers see end-of-stream.
        let ingest_tx = tx.clone();
        let ingest_store = Arc::clone(&store);
        let ingest = tokio::spawn(async move {
            let mut next_seq = match ingest_store.latest().await {
                Ok(latest) => latest.saturating_add(1).max(1),
                Err(err) => {
                    warn!(?err, "store.latest() failed; using seq=1 as fallback");
                    1
                }
            };
            while let Some(item) = source.next().await {
                match item {
                    Ok(msg) => {
                        if let Err(err) = ingest_store.store(next_seq, &msg).await {
                            warn!(?err, seq = next_seq, "store.store() failed; dropping frame");
                            continue;
                        }
                        if ingest_tx.send(msg).is_err() {
                            // No live subscribers: keep persisting,
                            // they may connect later and receive a
                            // retransmit window.
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
                        // SourceError is `#[non_exhaustive]` — newer
                        // variants get the same close-on-error path
                        // until we add explicit handling.
                        warn!(?other, "unrecognised SourceError; closing publisher");
                        break;
                    }
                }
            }
            // Drop ingest_tx → subscribers' broadcast::Receiver sees
            // RecvError::Closed and exits cleanly.
            drop(ingest_tx);
        });

        // Accept loop. New connections subscribe to the broadcast.
        // The loop exits when the listener errors *or* the ingest
        // task has dropped the last clone of the broadcast sender.
        let accept_tx = tx;
        loop {
            tokio::select! {
                accept = listener.accept() => {
                    match accept {
                        Ok((sock, peer)) => {
                            // Best-effort: not fatal on failure.
                            let _ = sock.set_nodelay(true);
                            let rx = accept_tx.subscribe();
                            let policy = Arc::clone(&policy);
                            tokio::spawn(handle_subscriber::<P>(sock, peer, policy, rx));
                        }
                        Err(err) => {
                            warn!(?err, "accept failed");
                        }
                    }
                }
                _ = wait_ingest(&ingest) => {
                    info!("ingest task ended; closing listener");
                    break;
                }
            }
        }
        // Wait for the ingest task to fully unwind (it may already
        // be done if the select arm fired on it).
        let _ = ingest.await;
        Ok(())
    }
}

/// Helper future that resolves when the ingest task completes.
/// Awaited by reference so the surrounding `tokio::select!` does
/// not consume the `JoinHandle`.
async fn wait_ingest(handle: &tokio::task::JoinHandle<()>) {
    // We can't move the handle, but `is_finished` lets us poll it.
    while !handle.is_finished() {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn handle_subscriber<P>(
    sock: TcpStream,
    peer: SocketAddr,
    policy: Arc<P>,
    mut rx: broadcast::Receiver<Message>,
) where
    P: SubscriptionPolicy + 'static,
{
    let mut framed = Framed::new(sock, ItchCodec::default());

    // 1) Warmup first — these are *not* taken from the broadcast.
    match policy.warmup().await {
        Ok(warmup) => {
            for msg in warmup {
                if let Err(err) = framed.send(msg).await {
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

    // 2) Live: forward broadcast messages until end-of-stream or
    // the subscriber lags past the broadcast capacity.
    loop {
        match rx.recv().await {
            Ok(msg) => {
                if let Err(err) = framed.send(msg).await {
                    debug!(?peer, ?err, "subscriber send failed; closing connection");
                    return;
                }
            }
            Err(broadcast::error::RecvError::Closed) => {
                debug!(?peer, "broadcast closed; subscriber exiting");
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
        }
    }
}
