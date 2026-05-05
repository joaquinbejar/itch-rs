//! Durable per-frame storage for gap recovery.
//!
//! `SeqStore` is the trait every transport publisher consumes to back
//! two protocol features:
//!
//! - **SoupBinTCP reconnect with sequence resume** —
//!   `SeqStore::range(from, count)` answers a recovering client
//!   (`docs/ITCH-SOURCE.md` §5).
//! - **MoldUDP64 request-server retransmission** — same call shape,
//!   different transport.
//!
//! Two default implementations ship in this crate:
//!
//! - [`crate::impls::RingBufferSeqStore`] — bounded in-memory ring
//!   keyed by sequence number; drops oldest on overflow.
//! - [`crate::impls::NullSeqStore`] — drops everything; use when
//!   reconnect / retransmit is not needed.
//!
//! Third-party backends should self-verify with
//! [`crate::testing::assert_seq_store_contract`].
//!
//! See `docs/ITCH-SOURCE.md` §3.2 (contract), §11.3 (testing
//! helpers), §13 (FAQ on `&self` vs `&mut self`).

use itch_protocol::Message;

/// Durable frame storage for gap recovery.
///
/// # Contract
///
/// The contract verified by [`crate::testing::assert_seq_store_contract`]
/// requires:
///
/// - **Empty store** — `latest = 0`, `earliest = 0`,
///   `range(_, _)` returns an empty `Vec`.
/// - **After `store(seq, m)`** — `latest >= seq`,
///   `earliest <= seq`, `range(seq, 1) = [(seq, m)]`.
/// - **`range(from, count)`** returns up to `count` consecutive
///   messages starting at `from`. If `from < earliest()` or
///   `from > latest()`, the result is empty (no error). The result
///   stops at the first gap — implementations MUST NOT skip over
///   missing sequences.
/// - **Idempotent re-store** of `(seq, msg)` is a no-op
///   (implementations MAY error on `(seq, different_msg)`).
///
/// # `&self` not `&mut self`
///
/// Concurrency is the implementor's problem (interior mutability via
/// `tokio::sync::RwLock` or sharded Dashmaps, etc.). The transport
/// holds many `Arc<dyn SeqStore>` references and writes / reads can
/// interleave; an `&mut self` API would force a runtime mutex on
/// every caller. See `docs/ITCH-SOURCE.md` §13.
#[async_trait::async_trait]
pub trait SeqStore: Send + Sync {
    /// Backend-specific error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Persist `msg` at sequence number `seq`.
    ///
    /// Idempotent on `(seq, msg)`. Implementations MAY return an
    /// error if the same `seq` is stored with a different `msg`.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on backend failure.
    async fn store(&self, seq: u64, msg: &Message) -> Result<(), Self::Error>;

    /// Return up to `count` consecutive messages from `from`.
    ///
    /// Returns fewer than `count` if `from + count > latest() + 1`
    /// or if `from < earliest()`. Stops at the first gap. Returns
    /// an empty `Vec` if `from > latest()` or the store is empty.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on backend failure.
    async fn range(&self, from: u64, count: usize) -> Result<Vec<(u64, Message)>, Self::Error>;

    /// Return the highest sequence number ever stored. `0` if the
    /// store is empty.
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on backend failure.
    async fn latest(&self) -> Result<u64, Self::Error>;

    /// Return the lowest sequence number currently retained. `0` if
    /// the store is empty. May be greater than the lowest ever
    /// stored if the implementation evicts (e.g.,
    /// [`crate::impls::RingBufferSeqStore`]).
    ///
    /// # Errors
    ///
    /// Returns `Self::Error` on backend failure.
    async fn earliest(&self) -> Result<u64, Self::Error>;
}
