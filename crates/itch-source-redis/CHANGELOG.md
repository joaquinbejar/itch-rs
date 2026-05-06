# Changelog — itch-source-redis

All notable changes to this project will be documented in this file.

## [Unreleased]

### Added

- `RedisSeqStore` — async `SeqStore` implementation backed by Redis (deadpool-redis) for distributed ITCH publishers.
- Support for message storage with TTL-based eviction.
- Integration tests with testcontainers-based Redis.
- `RedisSeqStoreConfig` builder for runtime configuration.
- Full compliance with `itch_source::SeqStore` contract.

## [0.1.0] — 2026-05-06

### Initial Release

- Crate published as part of itch-rs v0.1.0.
