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
/// entry is dropped and `earliest()` advances. Stores are
/// idempotent on `(seq, msg)`; storing a different `msg` at an
/// already-occupied `seq` returns
/// [`RingBufferSeqStoreError::SequenceConflict`].
///
/// Backed by `BTreeMap` so `store` / `range` / `latest` /
/// `earliest` are all `O(log n)` — out-of-order inserts behave
/// correctly and 100 k+ stress is tractable under coverage
/// instrumentation.
#[derive(Debug)]
pub struct RingBufferSeqStore {
    capacity: usize,
    inner: RwLock<BTreeMap<u64, Message>>,
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
            inner: RwLock::new(BTreeMap::new()),
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
        if let Some(existing) = guard.get(&seq) {
            if existing == msg {
                return Ok(());
            }
            return Err(RingBufferSeqStoreError::SequenceConflict { seq });
        }

        if guard.len() == self.capacity {
            // Evict the lowest-keyed entry (BTreeMap has no
            // pop_first stable until Rust 1.66 — itch-rs MSRV is
            // 1.75 so this is fine).
            if let Some((&first, _)) = guard.iter().next() {
                guard.remove(&first);
            }
        }
        guard.insert(seq, *msg);
        Ok(())
    }

    async fn range(
        &self,
        from: u64,
        count: usize,
    ) -> Result<Vec<(u64, Message)>, Self::Error> {
        if count == 0 {
            return Ok(Vec::new());
        }
        let guard = self.inner.read().await;
        if guard.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::with_capacity(count.min(guard.len()));
        let mut expected = from;
        for (&s, &m) in guard.range(from..) {
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
        let guard = self.inner.read().await;
        Ok(guard.keys().next_back().copied().unwrap_or(0))
    }

    async fn earliest(&self) -> Result<u64, Self::Error> {
        let guard = self.inner.read().await;
        Ok(guard.keys().next().copied().unwrap_or(0))
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
        store.store(5, &msg(EventCode::StartOfMessages)).await.unwrap();
        store.store(2, &msg(EventCode::StartOfSystemHours)).await.unwrap();
        store.store(8, &msg(EventCode::EndOfMessages)).await.unwrap();
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

    /// 100 k-store stress per the #50 acceptance criteria. Marked
    /// `#[ignore]` so the default `cargo test` (and the
    /// tarpaulin/Codecov coverage job, which instruments every
    /// allocation) skip it. Run locally via
    /// `cargo test -p itch-source -- --ignored`.
    #[tokio::test]
    #[ignore = "stress: 100k stores; runs under --ignored"]
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

    /// Smaller stress (10 k) that runs by default — verifies the
    /// `BTreeMap`-backed implementation is `O(log n)` enough for CI
    /// to finish well under the per-test timeout.
    #[tokio::test]
    async fn stress_10k_stores_in_default_run() {
        let store = RingBufferSeqStore::with_capacity(10_000);
        for seq in 1..=10_000u64 {
            store
                .store(seq, &msg(EventCode::StartOfMessages))
                .await
                .unwrap();
        }
        assert_eq!(store.earliest().await.unwrap(), 1);
        assert_eq!(store.latest().await.unwrap(), 10_000);
    }
}
