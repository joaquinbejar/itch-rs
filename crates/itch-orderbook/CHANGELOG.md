# Changelog

All notable changes to `itch-orderbook` are documented here. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this crate adheres to [Semantic Versioning](https://semver.org/).
Per ADR-0007 every published surface (types, traits, error variants)
participates in SemVer; per ADR-0013 the bridge crate's mapping table
between `OrderBook-rs` events and ITCH 5.0 messages is part of that
public surface.

## [Unreleased]

### Added

- New crate `itch-orderbook` (issue #49) as a **design-only stub**.
  No production code yet: the crate ships `lib.rs` with
  `#![forbid(unsafe_code)]` and a single `#[doc = include_str!]`
  pointing at `README.md`. The README is the deliverable and embeds
  ADR-0013 (the bridge decision record) plus the long-form design
  document for the future `OrderBook-rs` to ITCH 5.0 bridge.
- ADR-0013 added to the local `docs/adr/` tree as
  `0013-orderbook-bridge.md` (Section 1 of the README) and the
  long-form design as `docs/orderbook-bridge.md` (Sections 2 and 3
  of the README). Both are mirrored from `README.md` for the
  maintainer's local docs tree.
- Mapping table covering every documented `OrderBook-rs` event type
  (new resting order, partial cancel, full cancel, replace, taker /
  maker fill, non-cross trade print, broken trade, book-change,
  cross / NOII / circuit breaker / regulatory state) with one
  worked example per row.

### Future work (tracked as separate issues, post-merge of #49)

- "Implement `itch-orderbook` v0.1 (full bridge per ADR-0013)" -
  production code, depends on a stable `OrderBook-rs` release.
- "`DirectoryProvider` adapters (in-memory map, JSON-file,
  user-trait-impl)" - reference-data adapters for the `R` Stock
  Directory message.
- "`itch-orderbook` round-trip integration test
  (OrderBook-rs to bridge to itch-book)" - end-to-end harness
  asserting reconstructed L2 / L3 book equals original.
- "`MpidProvider` design plus adapters" - resolves Open Question 1
  on attribution mapping.
- "Cross trade / auction support" - depends on `OrderBook-rs`
  adding an auction module; surfaces ITCH `Q` and `I` messages.
