# itch-source Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `Pacing` enum + `paced(inner, pacing) -> PacedSource<S>`
  combinator. Three modes per `docs/ITCH-SOURCE.md` §13:
  - `Pacing::MaxSpeed` — yield as fast as the inner stream allows.
  - `Pacing::Fixed { period }` — one message per `period`.
  - `Pacing::Realtime` — sleep `t_{n+1} - t_n` between successive
    `Message::header().timestamp` values; first message yields
    immediately; out-of-order timestamps yield without delay.
  Backed by `tokio::time::sleep`. Works on any
  `MessageSource` (`IteratorSource`, `ChannelSource`, future
  `FileReplaySource`, …) without re-implementing the sleep logic.
  Full `.itch` `FileReplaySource` lands with `itch-replay` in
  v0.5 (issue #32).
- `examples/matching_engine_publisher.rs` — runnable end-to-end
  example wiring all three traits together: a tokio task simulates
  a matching engine emitting `AddOrder` / `OrderExecuted` /
  `OrderDelete` under a deterministic seed, pushed through a
  `ChannelSource`, persisted into a `RingBufferSeqStore`, with
  `StaticPolicy::empty()` for warmup. Self-contained — no
  `OrderBook-rs` dependency. Run with
  `cargo run -p itch-source --example matching_engine_publisher`.
- Crate-level rustdoc cross-links to the example.

- `SeqStore` trait now ships its full async API: `store(&self, seq,
  &Message)`, `range(&self, from, count)`, `latest()`, `earliest()`.
  All four take `&self` so concurrency is the implementor's problem
  (interior mutability, not `&mut self` mutex). Returns `0` from
  `latest`/`earliest` on an empty store; `range` stops at the first
  gap (never skips a missing sequence).
- `crate::impls::RingBufferSeqStore` — bounded in-memory ring keyed
  by sequence. Default capacity 65 536; clamps capacity to ≥ 1.
  Idempotent on `(seq, msg)`; rejects re-binding a different `msg`
  to the same `seq` via `RingBufferSeqStoreError::SequenceConflict`.
  Uses `tokio::sync::RwLock<VecDeque<(u64, Message)>>` (no
  `parking_lot` — banned by `rules/global_rules.md`).
- `crate::impls::NullSeqStore` — drop-everything impl with
  `type Error = std::convert::Infallible`. Use when reconnect /
  retransmission is not relevant.
- `crate::testing::assert_seq_store_contract<S: SeqStore>` —
  contract test third-party backends can call from their own test
  suite. Special-cases drop-everything stores
  (`NullSeqStore`-style) by checking `latest()` after the first
  `store` call.
- 12 new unit tests (overflow, idempotency, conflict, gap,
  concurrent store + range, 100k stress).
- `crate::impls::ChannelSource` — `MessageSource` over a
  `tokio::sync::mpsc::Receiver<Message>`. Construct via
  `ChannelSource::bounded(capacity)` (returns `(Sender, Self)`) or
  `ChannelSource::unbounded()` (returns
  `(UnboundedSender, UnboundedChannelSource)`). Clean
  end-of-stream is `Ready(None)` when the sender drops — *not*
  `SourceError::Exhausted`.
- `crate::impls::IteratorSource<I>` — `MessageSource` over any
  `Iterator<Item = Message> + Send + Unpin`. Convenience
  `From<Vec<Message>>`. Pair with `canonical_session()` for
  fixture-based replays.
- `crate::impls::WarmupFromSeqStore<S: SeqStore>` — `SubscriptionPolicy`
  impl that reads all stored messages (earliest through latest) as warmup.
  Useful for publishing the current L2 book snapshot or end-of-day state
  to new sessions.
- `crate::impls::Tee` — broadcast adapter that pulls from one message
  source and broadcasts to N consumers via `tokio::sync::broadcast`. Lets
  one source feed multiple transports (SoupBinTCP + MoldUDP64) in the same
  publisher process. Takes ownership of a pinned boxed stream.
- `crate::impls::MergeByTimestamp` — message-merge combinator that yields
  from N input sources ordered by smallest timestamp (tie-break by source
  order). Placeholder structure for v0.2; full async concurrent polling of
  N sources deferred to v0.3+.
- `crate::impls::merge_by_timestamp()` — helper function that creates an
  empty `MergeByTimestamp` (v0.2 signature placeholder).

## [0.1.0] — 2026-05-05

### Added

- `MessageSource` marker trait over `Stream<Item = Result<Message, SourceError>> + Send + Unpin`
  (blanket impl for any qualifying Stream per ADR-0012)
- `SourceError` enum with three variants: `Exhausted`, `Backend`, `Invariant`
- `SeqStore` trait stub (full impl in #50)
- `SubscriptionPolicy` trait stub with warmup-only scope (full impl in #51)
- Crate scaffold with `#![forbid(unsafe_code)]` and `#![warn(missing_docs)]`
- Unit tests for blanket impl, error variants, object-safety
