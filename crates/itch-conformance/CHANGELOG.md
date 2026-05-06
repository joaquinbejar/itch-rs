# Changelog

All notable changes to `itch-conformance` are documented here. The
format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this crate adheres to [Semantic Versioning](https://semver.org/).
Per ADR-0007 the published byte vectors are immutable wire-format
contracts: any change to an existing vector is a breaking change
and ships only as a major release.

## [Unreleased]

### Added

- New crate `itch-conformance` (issue #33). Publishes canonical
  conformance vectors and async test drivers for any third-party
  Rust ITCH 5.0 implementation.
- `vectors::all()` returns 21 `ConformanceVector` entries — one per
  ITCH 5.0 message kind (`S`, `R`, `H`, `Y`, `L`, `V`, `W`, `K`,
  `A`, `F`, `E`, `C`, `X`, `D`, `U`, `P`, `Q`, `B`, `I`, `N`) plus
  the documented `TradeNonCross` post-2014-07-14 quirk shape. Every
  vector mirrors the corresponding golden in
  `crates/itch-protocol/tests/golden.rs` byte-for-byte and
  field-for-field.
- `enum_vectors` exposes one
  `<enum>_variants() -> &'static [(u8, Enum)]` and one
  `<enum>_unknown_bytes() -> &'static [u8]` accessor for each of
  the 18 closed-set ASCII enums in `itch-protocol`
  (`EventCode`, `Side`, `MarketCategory`, `FinancialStatus`,
  `Authenticity`, `YesNo`, `LuldTier`, `TradingState`,
  `RegShoAction`, `MarketMakerMode`, `MarketParticipantState`,
  `BreachedLevel`, `IpoReleaseQualifier`, `Printable`,
  `CrossType`, `ImbalanceDirection`, `PriceVariation`,
  `RpiInterestFlag`).
- `soup` feature: async test driver `soup::drive_server` plus
  `SoupScenario` / `SoupResult` types. Scenarios:
  `LoginAccept`, `LoginReject`, `SequencedFlow`,
  `HeartbeatUnderSilence`, `DeadLink`,
  `ReconnectWithSequenceResume`, `Logout`, `EndOfSession`.
- `mold` feature: async test driver `mold::drive_publisher` plus
  `MoldScenario` / `MoldResult` types. Scenarios:
  `InOrderFlow`, `SinglePacketGap`, `MultiPacketGap`,
  `RequestServerUnreachable`, `SessionMismatch`, `BadInnerFrame`,
  `HeartbeatUnderSilence`, `EndOfSession`,
  `MultiServerFailover`.
- `tests/self_test.rs` exercises every vector (decode + encode
  roundtrip), every enum table (`AlphaCoded` byte ↔ variant +
  unknown-byte rejection), and a corrupt-`Side`-byte injection on
  the `A` Add Order vector.
