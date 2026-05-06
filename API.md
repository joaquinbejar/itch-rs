# Public API audit — pre-1.0

This document is the authoritative public-API manifest for every
crate promoted to 1.0 in this workspace. It is committed at the repo
root (the canonical `docs/` folder is local-only per
`.git/info/exclude`); when a crate is published to crates.io, the
audit captured here is what its 1.0.0 promises.

Generated: 2026-05-06.

Closes the audit pass from #42. The companion 1.0 work lives in #43
(MSRV policy) and #44 (coordinated release).

## How to reproduce

```bash
cargo install cargo-public-api --locked
cargo install cargo-semver-checks --locked

for crate in itch-protocol itch-tcp itch-soup itch-mold; do
    cargo public-api -p "$crate"     > "API-$crate.txt"
    cargo semver-checks check-release -p "$crate"
done
```

`cargo public-api` prints the full public surface for one crate;
diff against this file across releases to catch unintended changes.
`cargo semver-checks check-release` validates that no change since
the last published version is breaking under SemVer.

Both tools are wired into the `make pre-publish` target (see
`Makefile`).

## `#[non_exhaustive]` policy

Applied to every error / event enum so future variants ship in a
minor release without breaking downstream `match` arms.

Concretely, all the following enums are `#[non_exhaustive]`:

- `itch_protocol::ProtocolError`
- `itch_protocol::primitives::TimestampError` (added 2026-05-06)
- `itch_tcp::TransportError`
- `itch_soup::SoupError`
- `itch_soup::LoginRejectReason` (added 2026-05-06)
- `itch_mold::MoldError`
- `itch_mold::MoldEvent`
- `itch_replay::ReplayError`
- `itch_book::BookError`
- `itch_source::SourceError`
- `itch_source::impls::RingBufferSeqStoreError`
- `itch_source_redis::RedisSeqStoreError`
- `itch_source_postgres::PostgresSeqStoreError`
- `itch_source_kafka::KafkaSourceError`

NOT applied to closed-set ITCH 5.0 enums — exhaustive `match` is
intentional for forward-incompatible protocol revisions:

- `itch_protocol::Message` (the 20 ITCH 5.0 message kinds)
- `itch_protocol::EventCode`
- `itch_protocol::Side`
- `itch_protocol::CrossType`
- `itch_protocol::ImbalanceDirection`
- `itch_protocol::MarketCategory`
- `itch_protocol::FinancialStatus`
- `itch_protocol::Authenticity`
- `itch_protocol::YesNo`
- `itch_protocol::LuldTier`
- `itch_protocol::TradingState`
- `itch_protocol::RegShoAction`
- `itch_protocol::MarketMakerMode`
- `itch_protocol::MarketParticipantState`
- `itch_protocol::BreachedLevel`
- `itch_protocol::IpoReleaseQualifier`
- `itch_protocol::Printable`
- `itch_protocol::PriceVariation`
- `itch_protocol::RpiInterestFlag`

A new ITCH revision lands as a new module (per ADR direction);
`Message` itself stays exhaustive so the codec catches every
new variant at compile time.

`itch_soup::SoupPacket` is also exhaustive — it is the SoupBinTCP
3.00 wire shape; new SoupBinTCP versions land as a separate
module.

`itch_replay::CaptureFormat` is exhaustive — application-level
flag covering Glimpse vs RawBodies; users select one explicitly.

## Per-crate public surface (summarised)

The full output of `cargo public-api -p <crate>` is reproducible
from a clean `cargo build`; this section names the headline shapes
that downstream code is most likely to touch.

### `itch-protocol`

- 20 message DTOs (`SystemEvent`, `StockDirectory`, …,
  `RetailPriceImprovement`) + `Message` enum + `Header`.
- 18 closed-set ASCII enums in `enums` (see `#[non_exhaustive]`
  policy above).
- Primitive newtypes: `Stock`, `Mpid`, `Price4`, `Price8`,
  `Timestamp`, `StockLocate`, `TrackingNumber`, `OrderReference`,
  `MatchNumber`, `Shares`. Every newtype is
  `#[repr(transparent)]` so future versions can rely on layout.
- `Encode` / `Decode` traits + `Message::encode` / `decode` +
  `ProtocolError`.

### `itch-tcp`

- `ItchCodec` (length-prefix Decoder / Encoder).
- `ItchConnection` type alias.
- `connect`, `bind`, `accept`.
- `Server` (issue #18 onwards).
- `TransportError` (`#[non_exhaustive]`).

### `itch-soup`

- `SoupConnection` + `login` / `login_with_timeout` +
  `SoupCredentials`.
- `ResilientSoupClient` + `ResilientSoupConfig`.
- `SoupServer` + `Authenticator` trait + `AllowAllAuthenticator` /
  `StaticAuthenticator`.
- `HeartbeatConfig`, `OutboundHeartbeat`, `InboundHeartbeat`.
- `SoupCodec` + `SoupPacket` (10 wire packet types).
- `LoginRequest`, `LoginAccepted`, `LoginRejectReason`
  (`#[non_exhaustive]`).
- `SoupError` (`#[non_exhaustive]`).
- `DEFAULT_LOGIN_TIMEOUT`, `DEFAULT_SEND_TIMEOUT`,
  `DEFAULT_BACKOFF_MIN`, `DEFAULT_BACKOFF_MAX`,
  `DEFAULT_RESILIENT_LOGIN_TIMEOUT`, `DEFAULT_BROADCAST_CAPACITY`,
  `DEFAULT_SHUTDOWN_GRACE`, `DEFAULT_UNSEQUENCED_INBOX_CAPACITY`.

### `itch-mold`

- `MoldStream` + `MoldConfig` + `MoldEvent` (`#[non_exhaustive]`).
- `MoldPublisher`, `MoldRequestServer`.
- `MoldPacket` + `MoldPacketHeader` + `MessageBlock` (downstream
  packet codec).
- Constants: `HEADER_LEN`, `MSG_COUNT_HEARTBEAT`,
  `MSG_COUNT_END_OF_SESSION`, `MAX_BLOCK_LEN`,
  `DEFAULT_PACKING_MTU`, `BLOCK_LEN_PREFIX`.
- `MoldError` (`#[non_exhaustive]`).
- `PendingOverflowPolicy` (issue #22).

## Doc coverage

`cargo doc --workspace --no-deps` is required to be zero-warning
under `RUSTDOCFLAGS=-D warnings` (CI gate). Every `pub` item
carries a `///` doc comment; every fallible `pub fn` carries an
`# Errors` section. The doc gate is enforced on every PR.

## SemVer commitment (effective at 1.0.0)

From the moment a crate ships its 1.0.0 release:

- Adding a new `pub fn` / `pub const` / `pub struct` / `pub enum`
  variant to a `#[non_exhaustive]` enum is **non-breaking**.
- Renaming, removing, or changing the signature of any item in
  this manifest is **breaking** (major bump).
- Raising MSRV is **breaking-by-policy** (see `MSRV.md`) — a
  one-minor-version deprecation window applies.
- Changing wire format (`Message::encode` / `decode` byte shape)
  is **breaking**.

These rules are mechanically validated on every release PR by
`cargo semver-checks check-release`.

## References

- ADR-0007 (independent crates).
- `MSRV.md` (MSRV policy and current floors).
- `CHANGELOG.md` per crate (per-release deltas).
