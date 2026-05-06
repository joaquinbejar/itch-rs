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
- Optional `tokio-stream` feature (off by default) to gate a future
  thin async adapter over `futures::Stream<Item = Result<Message, _>>`.

### Scope

- L2 only. L3 per-order book reconstruction is tracked under issue
  #37; the multi-symbol book manager indexed by `StockLocate` is
  tracked under issue #38.
