#![forbid(unsafe_code)]
#![warn(missing_docs)]

//! # itch-source: Pluggable Message Sources for ITCH Servers
//!
//! This crate provides a **marker-trait abstraction** for ITCH message sources,
//! enabling applications to plug in custom data providers (orderbooks, files,
//! databases, live feeds) without modifying transport code.
//!
//! ## Design
//!
//! Per [ADR-0012](https://github.com/joaquinbejar/itch-rs/blob/main/docs/adr/0012-data-source-abstraction.md)
//! and [`docs/ITCH-SOURCE.md`](https://github.com/joaquinbejar/itch-rs/blob/main/docs/ITCH-SOURCE.md),
//! the `MessageSource` trait is intentionally a **marker** over
//! `Stream<Item = Result<Message, SourceError>> + Send + Unpin`, not a custom method trait.
//! This allows any `Stream` of ITCH messages to become a source for free via a blanket impl,
//! keeping the API ergonomic for users who already speak `futures::Stream`.
//!
//! Traits in this crate:
//! - [`MessageSource`] — marker over message stream
//! - [`SeqStore`] — durable frame storage (async, with gap recovery)
//! - [`SubscriptionPolicy`] — warmup-only policy (auth TBD)
//!
//! Default implementations provided in follow-up issues.
//!
//! ## Error Handling
//!
//! [`SourceError`] has three variants:
//! - `Exhausted` — source produced fewer messages than expected (cleanly)
//! - `Backend(dyn Error)` — backing store failed (transient or fatal)
//! - `Invariant(&'static str)` — internal contract violated (panic-level)
//!
//! ## Backpressure & Ownership
//!
//! - **Sequence numbers** are owned by the **transport**, not the source
//!   (prevents dual heartbeat logic, simplifies recovery).
//! - **Backpressure** is handled by the async runtime
//!   (if the sink is slow, the stream drains via async channels).
//! - **No mutable borrows** in the source trait
//!   (all state lives behind interior mutability or separate structs).
//!
//! See § 7, 8, 10 of `docs/ITCH-SOURCE.md` for details.
//!
//! ## Worked example
//!
//! For an end-to-end matching-engine publisher wiring all three
//! traits together — `ChannelSource` fed by a simulated engine,
//! `RingBufferSeqStore` for retransmission, `StaticPolicy::empty()`
//! for warmup — see `examples/matching_engine_publisher.rs` and
//! the longer-form walk-through in `docs/source-example.md`. Run
//! the example with:
//!
//! ```bash
//! cargo run -p itch-source --example matching_engine_publisher
//! ```

/// Default `MessageSource` / `SeqStore` / `SubscriptionPolicy`
/// implementations.
pub mod impls;
/// Subscription policy for message filtering and warmup.
pub mod policy;
/// Durable frame storage for gap recovery.
pub mod seqstore;
/// Message sources and error types.
pub mod source;
/// Canonical synthetic session fixture (all 20 ITCH message kinds).
pub mod synthetic;
/// Contract-test helpers third-party backends can use to self-verify.
pub mod testing;

pub use impls::{
    merge_by_timestamp, paced, ChannelSource, IteratorSource, MergeByTimestamp, NullSeqStore,
    PacedSource, Pacing, PendingMessage, RingBufferSeqStore, RingBufferSeqStoreError, Tee,
    UnboundedChannelSource, WarmupFromSeqStore,
};
pub use policy::{StaticPolicy, SubscriptionPolicy};
pub use seqstore::SeqStore;
pub use source::{MessageSource, SourceError};
pub use synthetic::canonical_session;
