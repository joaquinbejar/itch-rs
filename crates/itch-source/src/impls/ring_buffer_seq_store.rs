//! `RingBufferSeqStore` — bounded in-memory `SeqStore`.
//!
//! Sequences are persisted into a `BTreeMap<u64, Message>` so that
//! lookups, range queries, and `latest()`/`earliest()` are
//! `O(log n)` instead of `O(n)`, regardless of insertion order. When
//! the map reaches `capacity`, the lowest-keyed entry is evicted
//! (advancing `earliest`).
//!
//! Concurrency: safe under one writer + many readers via
//! `tokio::sync::RwLock`. (No `parking_lot` — banned by
//! `rules/global_rules.md`.)

use std::collections::BTreeMap;

use itch_protocol::Message;
use thiserror::Error;
use tokio::sync::RwLock;

use crate::SeqStore;

/// Default ring capacity (65 536 frames).
pub const DEFAULT_RING_CAPACITY: usize = 65_536;

/// Error type returned by `RingBufferSeqStore`.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum RingBufferSeqStoreError {
    /// A different message was already stored at this sequence
    /// number. The store rejects re-binding a sequence to a new
    /// message — the contract requires monotonic, idempotent stores.
    #[error("seq {seq} already bound to a different message")]
    SequenceConflict {
        /// Sequence number whose binding was rejected.
        seq: u64,
    },

    /// The internal invariant of the ring map was violated.
    #[error("ring buffer invariant: {reason}")]
    BackingStore {
        /// Description of the violated invariant.
        reason: &'static str,
    },
}

/// Bounded in-memory `SeqStore`.
///
/// Holds at most `capacity` frames; on overflow the lowest-keyed
/// entry is dropped. Stores are idempotent on `(seq, msg)`;
/// storing a different `msg` at an already-occupied `seq` returns
/// [`RingBufferSeqStoreError::SequenceConflict`].
///
/// Backed by `BTreeMap`. Per-operation costs:
///
/// - `store` — `O(log n)` insert.
/// - `range(from, count)` — `O(log n + k)` where `k` is the
///   returned length (every returned entry is walked once).
/// - `latest` / `earliest` — `O(log n)` key lookup.
///
/// **Out-of-order inserts.** The contract requires
/// `latest() = "highest sequence ever stored"`. The store keeps a
/// monotonic high-water mark internally so that `latest()` never
/// moves backwards even if a smaller sequence is stored later
/// (e.g. `store(5)` then `store(2)` with capacity 1).
/// `earliest()` returns the lowest currently retained key — note
/// that on out-of-order inserts that hit the capacity cap, the
/// store evicts the current minimum and inserts the new key, so
/// `earliest()` may move *down* (not just up) compared to a
/// previous read.
#[derive(Debug)]
pub struct RingBufferSeqStore {
    capacity: usize,
    inner: RwLock<Inner>,
}

/// Combined map + monotonic high-water mark held under one lock.
#[derive(Debug, Default)]
struct Inner {
    map: BTreeMap<u64, Message>,
    max_seen: u64,
}

impl RingBufferSeqStore {
    /// Construct a new `RingBufferSeqStore` with the default
    /// capacity ([`DEFAULT_RING_CAPACITY`]).
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacity(DEFAULT_RING_CAPACITY)
    }

    /// Construct a new `RingBufferSeqStore` with the given capacity.
    ///
    /// `capacity` is clamped to a minimum of `1` — a store of
    /// capacity `0` would drop every frame on store and trivially
    /// fail the contract.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            inner: RwLock::new(Inner::default()),
        }
    }

    /// Configured capacity (post-clamp).
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Default for RingBufferSeqStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl SeqStore for RingBufferSeqStore {
    type Error = RingBufferSeqStoreError;

    async fn store(&self, seq: u64, msg: &Message) -> Result<(), Self::Error> {
        let mut guard = self.inner.write().await;

        // Idempotent on (seq, msg); reject re-bind to different msg.
        if let Some(existing) = guard.map.get(&seq) {
            if existing == msg {
                // Still update the high-water mark in case the
                // re-store is the highest seen so far.
                if seq > guard.max_seen {
                    guard.max_seen = seq;
                }
                return Ok(());
            }
            return Err(RingBufferSeqStoreError::SequenceConflict { seq });
        }

        if guard.map.len() == self.capacity {
            // Evict the lowest-keyed entry. With out-of-order
            // inserts, this may make the new minimum lower than the
            // previously-evicted one — that is documented on
            // `RingBufferSeqStore`.
            if let Some((&first, _)) = guard.map.iter().next() {
                guard.map.remove(&first);
            }
        }
        guard.map.insert(seq, *msg);
        if seq > guard.max_seen {
            guard.max_seen = seq;
        }
        Ok(())
    }

    async fn range(&self, from: u64, count: usize) -> Result<Vec<(u64, Message)>, Self::Error> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let guard = self.inner.read().await;
        if guard.map.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(count.min(guard.map.len()));
        let mut expected = from;
        for (&s, &m) in guard.map.range(from..) {
            if s != expected {
                // Gap — stop. Contract: `range` MUST NOT skip
                // missing sequences.
                break;
            }
            out.push((s, m));
            expected = expected.saturating_add(1);
            if out.len() == count {
                break;
            }
        }
        Ok(out)
    }

    async fn latest(&self) -> Result<u64, Self::Error> {
        // High-water mark — never moves backwards, per the contract
        // ("highest sequence ever stored").
        let guard = self.inner.read().await;
        Ok(guard.max_seen)
    }

    async fn earliest(&self) -> Result<u64, Self::Error> {
        // Lowest *currently retained* key. May decrease over time
        // if out-of-order inserts hit the capacity cap.
        let guard = self.inner.read().await;
        Ok(guard.map.keys().next().copied().unwrap_or(0))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::assert_seq_store_contract;
    use itch_protocol::{enums::EventCode, messages::SystemEvent, Header, Message};

    fn msg(code: EventCode) -> Message {
        Message::SystemEvent(SystemEvent {
            header: Header::default(),
            event_code: code,
        })
    }

    #[tokio::test]
    async fn ring_buffer_satisfies_contract() {
        let store = RingBufferSeqStore::with_capacity(64);
        assert_seq_store_contract(&store).await;
    }

    #[tokio::test]
    async fn capacity_zero_clamped_to_one() {
        let store = RingBufferSeqStore::with_capacity(0);
        assert_eq!(store.capacity(), 1);
    }

    #[tokio::test]
    async fn overflow_drops_oldest_and_advances_earliest() {
        let store = RingBufferSeqStore::with_capacity(4);
        for s in 1..=6u64 {
            store
                .store(s, &msg(EventCode::StartOfMessages))
                .await
                .unwrap();
        }
        // After storing 1..=6 with capacity 4: retains 3..=6.
        assert_eq!(store.earliest().await.unwrap(), 3);
        assert_eq!(store.latest().await.unwrap(), 6);
        let r = store.range(1, 1).await.unwrap();
        assert!(r.is_empty(), "evicted seq must yield empty range");
        let r = store.range(3, 4).await.unwrap();
        assert_eq!(r.len(), 4);
    }

    #[tokio::test]
    async fn out_of_order_inserts_track_min_max_correctly() {
        let store = RingBufferSeqStore::with_capacity(64);
        store
            .store(5, &msg(EventCode::StartOfMessages))
            .await
            .unwrap();
        store
            .store(2, &msg(EventCode::StartOfSystemHours))
            .await
            .unwrap();
        store
            .store(8, &msg(EventCode::EndOfMessages))
            .await
            .unwrap();
        // BTreeMap-backed: latest is 8, earliest is 2.
        assert_eq!(store.earliest().await.unwrap(), 2);
        assert_eq!(store.latest().await.unwrap(), 8);
    }

    #[tokio::test]
    async fn idempotent_restore_same_message() {
        let store = RingBufferSeqStore::new();
        let m = msg(EventCode::StartOfMessages);
        store.store(1, &m).await.unwrap();
        store.store(1, &m).await.unwrap();
        assert_eq!(store.range(1, 1).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn restore_different_message_errors() {
        let store = RingBufferSeqStore::new();
        store
            .store(1, &msg(EventCode::StartOfMessages))
            .await
            .unwrap();
        let err = store
            .store(1, &msg(EventCode::EndOfMessages))
            .await
            .unwrap_err();
        assert!(matches!(
            err,
            RingBufferSeqStoreError::SequenceConflict { seq: 1 }
        ));
    }

    #[tokio::test]
    async fn range_stops_at_gap() {
        let store = RingBufferSeqStore::new();
        store
            .store(1, &msg(EventCode::StartOfMessages))
            .await
            .unwrap();
        store
            .store(3, &msg(EventCode::EndOfMessages))
            .await
            .unwrap();
        // Asking for 1..=2 finds seq 1 then hits a gap at 2 → stop.
        let r = store.range(1, 5).await.unwrap();
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, 1);
    }

    #[tokio::test]
    async fn concurrent_store_and_range_no_panic() {
        use std::sync::Arc;
        let store = Arc::new(RingBufferSeqStore::with_capacity(1024));
        let writer = {
            let s = Arc::clone(&store);
            tokio::spawn(async move {
                for seq in 1..=512u64 {
                    s.store(seq, &msg(EventCode::StartOfMessages))
                        .await
                        .unwrap();
                }
            })
        };
        let reader = {
            let s = Arc::clone(&store);
            tokio::spawn(async move {
                for _ in 0..256 {
                    let _ = s.range(1, 64).await.unwrap();
                    tokio::task::yield_now().await;
                }
            })
        };
        writer.await.unwrap();
        reader.await.unwrap();
    }

    /// 100 k-store stress per the #50 acceptance criteria. Runs in
    /// the default `cargo test` (and under tarpaulin) — the
    /// `BTreeMap`-backed implementation is `O(n log n)` for the
    /// loop, well under the per-test timeout even with coverage
    /// instrumentation.
    #[tokio::test]
    async fn stress_100k_stores() {
        let store = RingBufferSeqStore::with_capacity(100_000);
        for seq in 1..=100_000u64 {
            store
                .store(seq, &msg(EventCode::StartOfMessages))
                .await
                .unwrap();
        }
        assert_eq!(store.earliest().await.unwrap(), 1);
        assert_eq!(store.latest().await.unwrap(), 100_000);
        let r = store.range(50_000, 100).await.unwrap();
        assert_eq!(r.len(), 100);
        assert_eq!(r[0].0, 50_000);
        assert_eq!(r[99].0, 50_099);
    }

    #[tokio::test]
    async fn latest_never_moves_backwards_under_out_of_order_inserts() {
        // Capacity 1: store(5) then store(2) makes the map only
        // hold {2}, but the contract requires latest() to remain
        // the highest seq ever stored — i.e. 5.
        let store = RingBufferSeqStore::with_capacity(1);
        store
            .store(5, &msg(EventCode::StartOfMessages))
            .await
            .unwrap();
        assert_eq!(store.latest().await.unwrap(), 5);
        store
            .store(2, &msg(EventCode::EndOfMessages))
            .await
            .unwrap();
        assert_eq!(
            store.latest().await.unwrap(),
            5,
            "latest() must be the high-water mark, not the highest retained key",
        );
        // earliest() reflects what's actually retained.
        assert_eq!(store.earliest().await.unwrap(), 2);
    }
}
