//! Server-side retransmission cache for MoldUDP64 gap recovery.
//!
//! On the wire, MoldUDP64 V1.00 uses a unicast UDP "Re-request"
//! mechanism. We deliberately diverge from the spec **here** and
//! use TCP for the project's request channel: TCP gives ordered,
//! retried delivery of the request itself (so the client doesn't
//! have to handle yet another lost-packet path) and lets us
//! frame both the request and the response cleanly.
//!
//! The codec for the request protocol is internal to this crate —
//! receiver and request server are released together. Public API
//! is just `MoldRequestServer::bind`, `serve`, and the in-memory
//! cache plus a `RequestClient` (used by issue #24's
//! `GapRecoveryClient`).
//!
//! # Wire format (TCP)
//!
//! Request frame (12 B):
//!
//! ```text
//! ┌──────────────────┬──────────────────┐
//! │ Sequence (u64 BE)│ Count    (u32 BE)│
//! └──────────────────┴──────────────────┘
//! ```
//!
//! Response (variable):
//!
//! ```text
//! ┌──────────────────┬──────────────────┐
//! │ Status (u32 BE)  │ FrameCount (u32 BE)│
//! └──────────────────┴──────────────────┘
//! followed by FrameCount blocks
//! ┌──────────────────┬─────────────────────────┐
//! │ Length (u16 BE)  │  Encoded ITCH (length B)│
//! └──────────────────┴─────────────────────────┘
//! ```
//!
//! Status values:
//! - `0x00000000` — OK, FrameCount frames follow, all in order from
//!   the requested sequence.
//! - `0xFFFFFFFE` — partial / hole: the cache only has the first
//!   FrameCount frames; further sequences are not (yet) available.
//! - `0xFFFFFFFF` — end-of-session: the publisher has stopped; the
//!   requested range is past the last cached sequence.
//!
//! # Bounded cache
//!
//! The cache is a [`RingBufferSeqStore`]: a fixed-capacity vector
//! of `(seq, Vec<u8>)` slots. Every cached frame consumes one slot.
//! When capacity is reached, the oldest slot is overwritten. A
//! peer cannot exhaust memory by replaying retransmission requests.
//!
//! # Shutdown
//!
//! `MoldRequestServer::serve` returns a [`tokio::task::JoinHandle`]
//! that drives accepts + reads. Drop the handle (or `abort()` it) to
//! shut down. Per-connection tasks are tracked via a `JoinSet`
//! internal to `serve`; when `serve` exits, every per-conn task is
//! aborted.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::{Buf, BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};

use crate::codec::MAX_BLOCK_LEN;
use crate::error::MoldError;

/// Wire-level "OK" status.
pub const STATUS_OK: u32 = 0x0000_0000;

/// Wire-level "hole" status: the cache only had the first
/// `frame_count` frames; further sequences are not yet available.
pub const STATUS_HOLE: u32 = 0xFFFF_FFFE;

/// Wire-level end-of-session status: requested range is past the
/// last cached sequence and the publisher has signalled EOS.
pub const STATUS_END_OF_SESSION: u32 = 0xFFFF_FFFF;

/// Default request-server cache capacity (frames). 16 384 frames
/// at ~50 B per ITCH 5.0 message ≈ 800 KiB; fits comfortably in
/// memory and absorbs ~16 s of a 1 KHz feed.
pub const DEFAULT_CACHE_CAPACITY: usize = 16_384;

/// Wire size of one request frame.
pub const REQUEST_FRAME_LEN: usize = 12;

/// Wire size of one response header.
pub const RESPONSE_HEADER_LEN: usize = 8;

/// Hard cap on the number of frames a single response will carry.
/// Prevents a malicious requester asking for 4 billion frames.
pub const MAX_RESPONSE_FRAMES: u32 = 65_536;

/// One cached frame.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedFrame {
    /// Sequence number assigned by the publisher.
    pub sequence: u64,
    /// Encoded inner ITCH message (tag + body).
    pub body: Vec<u8>,
}

/// Bounded ring-buffer cache of recently-published frames.
///
/// Entries are indexed by `sequence`. A binary search across the
/// underlying `VecDeque` would suffice for our access pattern but
/// `VecDeque::binary_search_by_key` requires sorted input — which
/// the publisher guarantees, so we exploit it directly.
#[derive(Debug)]
pub struct RingBufferSeqStore {
    inner: VecDeque<CachedFrame>,
    capacity: usize,
    /// Sticky once the publisher signalled EOS — the request server
    /// should answer ranges past the last cached seq with `STATUS_END_OF_SESSION`.
    eos: bool,
}

impl RingBufferSeqStore {
    /// Create a fresh cache with the given capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity.max(1)),
            capacity: capacity.max(1),
            eos: false,
        }
    }

    /// Push one frame into the cache. If the cache is full, the
    /// oldest frame is dropped.
    pub fn push(&mut self, frame: CachedFrame) {
        if self.inner.len() == self.capacity {
            self.inner.pop_front();
        }
        self.inner.push_back(frame);
    }

    /// Mark the session ended.
    pub fn mark_end_of_session(&mut self) {
        self.eos = true;
    }

    /// `true` iff EOS has been marked.
    #[must_use]
    pub fn is_end_of_session(&self) -> bool {
        self.eos
    }

    /// Number of currently cached frames.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// `true` iff the cache holds zero frames.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Lowest sequence currently cached, or `None`.
    #[must_use]
    pub fn first_sequence(&self) -> Option<u64> {
        self.inner.front().map(|f| f.sequence)
    }

    /// Highest sequence currently cached, or `None`.
    #[must_use]
    pub fn last_sequence(&self) -> Option<u64> {
        self.inner.back().map(|f| f.sequence)
    }

    /// Look up consecutive frames starting at `start_seq`. Returns
    /// up to `count` frames (or fewer if the cache holds fewer
    /// consecutive entries from that point, or the cap
    /// [`MAX_RESPONSE_FRAMES`] is reached).
    #[must_use]
    pub fn lookup(&self, start_seq: u64, count: u32) -> Vec<CachedFrame> {
        let cap = count.min(MAX_RESPONSE_FRAMES) as usize;
        let mut out = Vec::with_capacity(cap.min(self.inner.len()));
        // Find the index of start_seq via binary search (sorted by seq).
        let idx = match self.inner.binary_search_by_key(&start_seq, |f| f.sequence) {
            Ok(i) => i,
            Err(_) => return out,
        };
        let mut next_expected = start_seq;
        for f in self.inner.iter().skip(idx) {
            if out.len() == cap {
                break;
            }
            if f.sequence != next_expected {
                break;
            }
            out.push(f.clone());
            next_expected = next_expected.saturating_add(1);
        }
        out
    }

    /// Decide what status to return for a request that asked for
    /// `start_seq` and got `frames_returned` frames back.
    #[must_use]
    pub fn status_for(&self, start_seq: u64, count: u32, frames_returned: u32) -> u32 {
        if frames_returned == count.min(MAX_RESPONSE_FRAMES) {
            return STATUS_OK;
        }
        // Less than asked. Three cases:
        // 1. Asked past the end. If EOS, signal that; else hole.
        // 2. Hit a discontinuity in the cache.
        let last = self.last_sequence();
        let asked_end = start_seq.saturating_add(count.min(MAX_RESPONSE_FRAMES) as u64);
        if let Some(l) = last {
            if start_seq > l && self.eos {
                return STATUS_END_OF_SESSION;
            }
            if asked_end > l + 1 && self.eos {
                return STATUS_END_OF_SESSION;
            }
        } else if self.eos {
            return STATUS_END_OF_SESSION;
        }
        STATUS_HOLE
    }
}

/// MoldUDP64 request server: TCP-based retransmission cache.
///
/// Owns a [`RingBufferSeqStore`] behind a `tokio::sync::Mutex`. The
/// publisher (issue #25) feeds frames in via [`Self::cache_frame`];
/// receivers (issue #24) reach in via TCP requests on the bound
/// address.
#[derive(Debug)]
pub struct MoldRequestServer {
    cache: Arc<Mutex<RingBufferSeqStore>>,
}

impl MoldRequestServer {
    /// Build a fresh server with the given cache capacity.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(RingBufferSeqStore::with_capacity(capacity))),
        }
    }

    /// Borrow the underlying cache handle. Useful for the
    /// publisher to push frames concurrently with `serve`.
    #[must_use]
    pub fn cache(&self) -> Arc<Mutex<RingBufferSeqStore>> {
        Arc::clone(&self.cache)
    }

    /// Push one frame into the cache. Convenience wrapper that
    /// avoids the caller having to await the inner mutex.
    pub async fn cache_frame(&self, frame: CachedFrame) {
        self.cache.lock().await.push(frame);
    }

    /// Mark the session ended in the cache. Subsequent requests
    /// past the last cached sequence will receive
    /// [`STATUS_END_OF_SESSION`].
    pub async fn mark_end_of_session(&self) {
        self.cache.lock().await.mark_end_of_session();
    }

    /// Bind a TCP listener and start serving requests in a fresh
    /// task. Returns the bound address (so the caller can plumb it
    /// into a [`crate::MoldConfig::request_servers`]) and a
    /// [`JoinHandle`] for the accept loop. Drop / abort the handle
    /// to shut down.
    ///
    /// # Errors
    ///
    /// - I/O errors from `TcpListener::bind`.
    pub async fn bind<A: ToSocketAddrs>(
        &self,
        addr: A,
    ) -> Result<(SocketAddr, JoinHandle<()>), MoldError> {
        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        tracing::info!(?local, "mold request server bound");
        let cache = Arc::clone(&self.cache);
        let handle = tokio::spawn(serve_loop(listener, cache));
        Ok((local, handle))
    }
}

async fn serve_loop(listener: TcpListener, cache: Arc<Mutex<RingBufferSeqStore>>) {
    let mut conns: JoinSet<()> = JoinSet::new();
    loop {
        tokio::select! {
            res = listener.accept() => match res {
                Ok((stream, peer)) => {
                    tracing::debug!(?peer, "mold request conn accepted");
                    let cache = Arc::clone(&cache);
                    conns.spawn(handle_conn(stream, peer, cache));
                }
                Err(err) => {
                    tracing::error!(?err, "mold request server accept failed");
                    break;
                }
            },
            // Reap finished connection tasks so the JoinSet doesn't grow.
            Some(_) = conns.join_next(), if !conns.is_empty() => {}
        }
    }
    // Cleanly shut all connection tasks.
    conns.abort_all();
}

async fn handle_conn(
    mut stream: TcpStream,
    peer: SocketAddr,
    cache: Arc<Mutex<RingBufferSeqStore>>,
) {
    if let Err(err) = serve_conn(&mut stream, &cache).await {
        tracing::warn!(?peer, ?err, "mold request conn ended with error");
    }
    let _ = stream.shutdown().await;
}

async fn serve_conn(
    stream: &mut TcpStream,
    cache: &Arc<Mutex<RingBufferSeqStore>>,
) -> Result<(), MoldError> {
    let mut buf = [0u8; REQUEST_FRAME_LEN];
    loop {
        // Read one request frame, or exit cleanly on EOF.
        match stream.read_exact(&mut buf).await {
            Ok(_) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        }
        let mut cursor = &buf[..];
        let sequence = cursor.get_u64();
        let count = cursor.get_u32();
        if count == 0 {
            // Defensive: zero-frame request — answer with empty OK.
            write_response(stream, STATUS_OK, &[]).await?;
            continue;
        }

        let (frames, status) = {
            let guard = cache.lock().await;
            let frames = guard.lookup(sequence, count);
            let status = guard.status_for(sequence, count, frames.len() as u32);
            (frames, status)
        };

        write_response(stream, status, &frames).await?;
    }
}

async fn write_response(
    stream: &mut TcpStream,
    status: u32,
    frames: &[CachedFrame],
) -> Result<(), MoldError> {
    let mut out = BytesMut::with_capacity(
        RESPONSE_HEADER_LEN + frames.iter().map(|f| 2 + f.body.len()).sum::<usize>(),
    );
    out.put_u32(status);
    out.put_u32(frames.len() as u32);
    for f in frames {
        if f.body.len() > MAX_BLOCK_LEN {
            return Err(MoldError::BlockTooLarge {
                got: f.body.len(),
                max: MAX_BLOCK_LEN,
            });
        }
        out.put_u16(f.body.len() as u16);
        out.extend_from_slice(&f.body);
    }
    stream.write_all(&out).await?;
    stream.flush().await?;
    Ok(())
}

// ---------- Lightweight request client (used by tests + #24) ----------

/// Decoded server response.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestResponse {
    /// Wire status.
    pub status: u32,
    /// Returned frames (may be fewer than requested for HOLE / EOS).
    pub frames: Vec<CachedFrame>,
}

/// Minimal TCP client that sends one or more `(seq, count)`
/// requests on a single connection. Used by the integration tests
/// in this PR and by the gap-recovery client in #24.
#[derive(Debug)]
pub struct RequestClient {
    stream: TcpStream,
}

impl RequestClient {
    /// Connect to a request server.
    ///
    /// # Errors
    ///
    /// - I/O errors from `TcpStream::connect` / `set_nodelay`.
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<Self, MoldError> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        Ok(Self { stream })
    }

    /// Send one request and read the response.
    ///
    /// # Errors
    ///
    /// - I/O errors at any point in the round trip.
    /// - [`MoldError::BlockTooLarge`] if the server announces a
    ///   per-frame body size greater than [`MAX_BLOCK_LEN`].
    pub async fn request(
        &mut self,
        sequence: u64,
        count: u32,
    ) -> Result<RequestResponse, MoldError> {
        let mut req = [0u8; REQUEST_FRAME_LEN];
        let mut cursor = &mut req[..];
        cursor.put_u64(sequence);
        cursor.put_u32(count);
        self.stream.write_all(&req).await?;
        self.stream.flush().await?;

        let mut hdr = [0u8; RESPONSE_HEADER_LEN];
        self.stream.read_exact(&mut hdr).await?;
        let status = u32::from_be_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
        let frame_count = u32::from_be_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
        if frame_count > MAX_RESPONSE_FRAMES {
            return Err(MoldError::BlockTooLarge {
                got: frame_count as usize,
                max: MAX_RESPONSE_FRAMES as usize,
            });
        }
        let mut frames = Vec::with_capacity(frame_count as usize);
        let mut next_seq = sequence;
        for _ in 0..frame_count {
            let mut len_buf = [0u8; 2];
            self.stream.read_exact(&mut len_buf).await?;
            let len = u16::from_be_bytes(len_buf) as usize;
            if len == 0 {
                return Err(MoldError::EmptyBlock);
            }
            if len > MAX_BLOCK_LEN {
                return Err(MoldError::BlockTooLarge {
                    got: len,
                    max: MAX_BLOCK_LEN,
                });
            }
            let mut body = vec![0u8; len];
            self.stream.read_exact(&mut body).await?;
            frames.push(CachedFrame {
                sequence: next_seq,
                body,
            });
            next_seq = next_seq.saturating_add(1);
        }
        Ok(RequestResponse { status, frames })
    }

    /// Close the connection cleanly.
    pub async fn close(mut self) -> Result<(), MoldError> {
        self.stream.shutdown().await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(seq: u64) -> CachedFrame {
        CachedFrame {
            sequence: seq,
            body: vec![0xAB; 16 + (seq as usize % 16)],
        }
    }

    #[test]
    fn ringbuffer_push_evicts_oldest_at_capacity() {
        let mut store = RingBufferSeqStore::with_capacity(3);
        store.push(frame(1));
        store.push(frame(2));
        store.push(frame(3));
        store.push(frame(4));
        assert_eq!(store.len(), 3);
        assert_eq!(store.first_sequence(), Some(2));
        assert_eq!(store.last_sequence(), Some(4));
    }

    #[test]
    fn ringbuffer_lookup_consecutive_returns_cached() {
        let mut store = RingBufferSeqStore::with_capacity(8);
        for s in 1u64..=5 {
            store.push(frame(s));
        }
        let frames = store.lookup(2, 3);
        assert_eq!(frames.len(), 3);
        assert_eq!(frames[0].sequence, 2);
        assert_eq!(frames[1].sequence, 3);
        assert_eq!(frames[2].sequence, 4);
    }

    #[test]
    fn ringbuffer_lookup_unknown_seq_returns_empty() {
        let mut store = RingBufferSeqStore::with_capacity(8);
        for s in 1u64..=5 {
            store.push(frame(s));
        }
        let frames = store.lookup(100, 5);
        assert!(frames.is_empty());
    }

    #[test]
    fn ringbuffer_lookup_stops_at_hole() {
        let mut store = RingBufferSeqStore::with_capacity(8);
        // Put 1, 2, 4, 5 (gap at 3).
        store.push(frame(1));
        store.push(frame(2));
        store.push(frame(4));
        store.push(frame(5));
        let frames = store.lookup(1, 5);
        // Only 1, 2 are returned consecutively from 1.
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].sequence, 1);
        assert_eq!(frames[1].sequence, 2);
    }

    #[test]
    fn status_for_full_match_returns_ok() {
        let mut store = RingBufferSeqStore::with_capacity(8);
        for s in 1u64..=5 {
            store.push(frame(s));
        }
        assert_eq!(store.status_for(1, 3, 3), STATUS_OK);
    }

    #[test]
    fn status_for_partial_match_returns_hole() {
        let mut store = RingBufferSeqStore::with_capacity(8);
        for s in 1u64..=5 {
            store.push(frame(s));
        }
        assert_eq!(store.status_for(1, 10, 5), STATUS_HOLE);
    }

    #[test]
    fn status_for_past_end_with_eos_returns_eos() {
        let mut store = RingBufferSeqStore::with_capacity(8);
        for s in 1u64..=5 {
            store.push(frame(s));
        }
        store.mark_end_of_session();
        assert_eq!(store.status_for(100, 5, 0), STATUS_END_OF_SESSION);
    }

    #[tokio::test]
    async fn end_to_end_request_returns_cached_frames() {
        let server = MoldRequestServer::new(64);
        for s in 1u64..=10 {
            server.cache_frame(frame(s)).await;
        }
        let (addr, handle) = server.bind("127.0.0.1:0").await.expect("bind");

        let mut client = RequestClient::connect(addr).await.expect("connect");
        let resp = client.request(3, 4).await.expect("request");
        assert_eq!(resp.status, STATUS_OK);
        assert_eq!(resp.frames.len(), 4);
        for (i, f) in resp.frames.iter().enumerate() {
            assert_eq!(f.sequence, 3 + i as u64);
        }
        client.close().await.expect("close");
        handle.abort();
    }

    #[tokio::test]
    async fn request_for_hole_returns_status_hole() {
        let server = MoldRequestServer::new(64);
        for s in 1u64..=5 {
            server.cache_frame(frame(s)).await;
        }
        let (addr, handle) = server.bind("127.0.0.1:0").await.expect("bind");
        let mut client = RequestClient::connect(addr).await.expect("connect");
        let resp = client.request(1, 100).await.expect("request");
        assert_eq!(resp.status, STATUS_HOLE);
        assert_eq!(resp.frames.len(), 5);
        client.close().await.expect("close");
        handle.abort();
    }

    #[tokio::test]
    async fn request_past_end_after_eos_returns_status_end_of_session() {
        let server = MoldRequestServer::new(64);
        for s in 1u64..=5 {
            server.cache_frame(frame(s)).await;
        }
        server.mark_end_of_session().await;
        let (addr, handle) = server.bind("127.0.0.1:0").await.expect("bind");
        let mut client = RequestClient::connect(addr).await.expect("connect");
        let resp = client.request(100, 1).await.expect("request");
        assert_eq!(resp.status, STATUS_END_OF_SESSION);
        assert!(resp.frames.is_empty());
        client.close().await.expect("close");
        handle.abort();
    }

    #[tokio::test]
    async fn multiple_sequential_requests_on_one_conn() {
        let server = MoldRequestServer::new(64);
        for s in 1u64..=10 {
            server.cache_frame(frame(s)).await;
        }
        let (addr, handle) = server.bind("127.0.0.1:0").await.expect("bind");
        let mut client = RequestClient::connect(addr).await.expect("connect");

        let r1 = client.request(1, 3).await.expect("request 1");
        assert_eq!(r1.frames.len(), 3);
        let r2 = client.request(5, 2).await.expect("request 2");
        assert_eq!(r2.frames.len(), 2);
        client.close().await.expect("close");
        handle.abort();
    }

    #[tokio::test]
    async fn empty_request_returns_ok_with_no_frames() {
        let server = MoldRequestServer::new(64);
        let (addr, handle) = server.bind("127.0.0.1:0").await.expect("bind");
        let mut client = RequestClient::connect(addr).await.expect("connect");
        let resp = client.request(1, 0).await.expect("request");
        assert_eq!(resp.status, STATUS_OK);
        assert!(resp.frames.is_empty());
        client.close().await.expect("close");
        handle.abort();
    }
}
