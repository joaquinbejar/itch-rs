//! Default implementations of the `itch-source` traits.

pub mod channel;
pub mod iterator;
/// Message-merge combinators (`MergeByTimestamp`, `PendingMessage`).
pub mod merge;
pub mod null_seq_store;
/// Pacing combinator (`Pacing`, `paced`, `PacedSource`).
pub mod paced;
pub mod ring_buffer_seq_store;
/// Broadcast adapter (`Tee`) for publishing to multiple transports from one source.
pub mod tee;
/// Warmup policy backed by a `SeqStore`.
pub mod warmup_from_seqstore;

pub use channel::{ChannelSource, UnboundedChannelSource};
pub use iterator::IteratorSource;
pub use merge::{merge_by_timestamp, MergeByTimestamp, PendingMessage};
pub use null_seq_store::NullSeqStore;
pub use paced::{paced, PacedSource, Pacing};
pub use ring_buffer_seq_store::{RingBufferSeqStore, RingBufferSeqStoreError};
pub use tee::Tee;
pub use warmup_from_seqstore::WarmupFromSeqStore;
