# itch-source Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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

## [0.1.0] — 2026-05-05

### Added

- `MessageSource` marker trait over `Stream<Item = Result<Message, SourceError>> + Send + Unpin`
  (blanket impl for any qualifying Stream per ADR-0012)
- `SourceError` enum with three variants: `Exhausted`, `Backend`, `Invariant`
- `SeqStore` trait stub (full impl in #50)
- `SubscriptionPolicy` trait stub with warmup-only scope (full impl in #51)
- Crate scaffold with `#![forbid(unsafe_code)]` and `#![warn(missing_docs)]`
- Unit tests for blanket impl, error variants, object-safety
