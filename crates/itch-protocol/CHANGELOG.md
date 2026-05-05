# Changelog — `itch-protocol`

All notable changes to this crate will be documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to per-crate [SemVer](https://semver.org/spec/v2.0.0.html).

## Unreleased

## 0.1.0 — 2026-05-05

### Added

- 10 primitive newtypes covering every ITCH 5.0 wire field —
  `StockLocate`, `TrackingNumber`, `OrderReference`, `MatchNumber`,
  `Shares`, `Timestamp` (u48 with validated constructor), `Stock`
  (8-byte ASCII, space-padded), `Mpid` (4-byte ASCII, space-padded),
  `Price4` (fixed-point u32 / 10⁴), `Price8` (fixed-point u64 / 10⁸).
  All `#[repr(transparent)]`, `Hash`, and `Default`-friendly
  (`Stock` / `Mpid` default to all-spaces).
- 18 closed-set ASCII enums driven by the `enum_alpha!` macro and
  the `AlphaCoded` trait — `EventCode`, `Side`, `MarketCategory`,
  `FinancialStatus`, `TradingState`, `RegShoAction`,
  `MarketMakerMode`, `MarketParticipantState`, `BreachedLevel`,
  `IpoReleaseQualifier`, `Printable`, `Authenticity`, `LuldTier`,
  `PriceVariation`, `ImbalanceDirection`, `CrossType`,
  `RpiInterestFlag`, `YesNo`. Each round-trips a single ASCII byte;
  unknown codes return `ProtocolError::InvalidEnumCode`.
- 10-byte `Header` (`stock_locate`, `tracking_number`, `timestamp`).
- 20 message DTOs (one per ITCH 5.0 kind: `S`, `R`, `H`, `Y`, `L`,
  `V`, `W`, `K`, `A`, `F`, `E`, `C`, `X`, `D`, `U`, `P`, `Q`, `B`,
  `I`, `N`) and an exhaustive `Message` enum with `tag()`,
  `header()`, `body_len()`, `encoded_len()` accessors.
- Hand-rolled big-endian codec (`Encode` / `Decode` traits) with
  zero allocation on the hot path. Public `Message::encode` /
  `Message::decode` for whole-frame I/O.
- `ProtocolError` (`#[non_exhaustive]`, `thiserror`) with
  `Truncated`, `BufferTooSmall`, `UnknownMessageType`, and
  `InvalidEnumCode { field, code }` variants.
- 51 in-crate unit tests, 30 per-message roundtrip tests
  (`tests/roundtrip.rs`), 21 hand-crafted golden hex vectors
  (`tests/golden.rs`) including the post-2014-07-14 and pre-2014
  `TradeNonCross` quirks.

### Documented

- Cross-references to ADR-0001 (domain modeling), ADR-0003
  (fixed-point prices), ADR-0004 (hand-rolled codec), and ADR-0005
  (`std`-by-default, `no_std`-friendly).
