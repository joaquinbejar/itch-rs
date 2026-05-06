# Changelog — itch-source-kafka

All notable changes to this project will be documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- `KafkaSource` — `futures::Stream<Item = Result<Message, SourceError>>`
  adapter over `rdkafka`'s `StreamConsumer`. Slots into
  `itch_source::MessageSource` for free via the marker-trait blanket
  impl, so any code generic over `MessageSource` accepts it without
  modification.
- `KafkaSourceConfig` with `brokers`, `group_id`, `topic`,
  `auto_offset_reset`, and `additional_props` for runtime
  configuration. `additional_props` passes arbitrary key / value
  pairs through to `rdkafka::ClientConfig` and may override the
  defaults applied by `KafkaSource::connect`
  (`enable.auto.commit=true`).
- `AutoOffsetReset { Earliest, Latest }` — `Copy + Debug + PartialEq +
  Eq`, with an `as_str` helper that maps onto the rdkafka
  `auto.offset.reset` literal.
- `KafkaSource::connect` — async constructor that builds the
  rdkafka client, subscribes to the configured topic, logs at
  `INFO`, and surfaces invalid configuration through
  `KafkaSourceError::Config { reason }` before any I/O runs.
- `KafkaSource::with_decoder` — installs a custom
  `Fn(&[u8]) -> Result<Message, ProtocolError>` to support framed /
  enveloped record payloads on top of `Message::decode`.
- `KafkaSourceError` (`#[non_exhaustive]`) covering `Kafka`
  (auto-converted from `rdkafka::error::KafkaError`), `Decode`, and
  `Config { reason: &'static str }`.
- Integration tests covering connect, end-to-end produce / consume
  round-trip, recovery from a corrupted record, and a custom
  decoder. Tests skip-not-fail when `ITCH_KAFKA_BROKERS` is unset.
- Unit tests covering `AutoOffsetReset` rendering and configuration
  validation in `KafkaSource::connect`.

### Notes

- `rdkafka` is enabled with the `cmake-build` feature, which compiles
  `librdkafka` from source via CMake. This eliminates the system
  `librdkafka` dependency at the cost of roughly two extra minutes
  on a cold build.
- v0.1 ships with `enable.auto.commit=true`. Manual commit
  ergonomics tracked as a follow-up.

## [0.1.0] — 2026-05-06

### Initial Release

- Crate published as part of itch-rs v0.1.0.
