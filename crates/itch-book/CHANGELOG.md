# Changelog

All notable changes to `itch-book` will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this crate adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Initial single-symbol L2 (price-level) book reconstruction.
  - `L2Book` with `new`, `stock_locate`, `best_bid`, `best_ask`,
    `levels`, `order_count`, `levels_count`, and the exhaustive
    `apply(&Message)` entry point.
  - `OrderEntry` per-order bookkeeping record (re-exported from the
    crate root; not part of the stable surface).
  - `BookError` with `OverExecution`, `UnknownOrderRef`, and
    `MismatchedSide` variants.
- Optional `tokio-stream` feature (off by default) gating the async
  `BookManager::run(stream)` adapter that drains a
  `futures::Stream<Item = Result<Message, ProtocolError>>` into the
  manager.
- L3 (per-order) book — `L3Book`, `L3OrderEntry`, FIFO queue
  priority, `summary_l2()` cross-check, `BookError::FifoViolation`.
- `BookManager` — multi-symbol routing by `StockLocate`, lazy
  per-symbol L2 / L3 book creation, `R` Stock Directory →
  `directory` cache, `symbol(locate)` lookup,
  `tokio-stream`-gated async `run(stream)` adapter.
- `BookError::Protocol(#[from] ProtocolError)` — additive variant for
  the async runner's stream errors.
- `l3` Cargo feature (default ON) gating the L3 fields and methods on
  `BookManager`. Disable via `--no-default-features` for an L2-only
  build; the `l3` module itself is always compiled.
- `OhlcvAccumulator` + `OhlcvBar` — per-symbol open/high/low/
  close/volume + trade-count accumulator over P / Q / C
  printable trade prints. Drives the v0.4 acceptance EOD
  validation harness.
- Synthetic-day EOD validation test in
  `tests/eod_validation.rs` (3 symbols, ~50 messages, expected
  OHLCV asserted to 0 cents).
- `vendor-captures` Cargo feature (off by default) gating the
  future public-capture EOD validator. The synthetic-day path
  runs unconditionally; the placeholder test is `#[ignore]` until
  a NASDAQ-licensed capture lands under `vendor/captures/`.
