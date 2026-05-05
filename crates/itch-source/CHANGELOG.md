# itch-source Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] — 2026-05-05

### Added

- `MessageSource` marker trait over `Stream<Item = Result<Message, SourceError>> + Send + Unpin`
  (blanket impl for any qualifying Stream per ADR-0012)
- `SourceError` enum with three variants: `Exhausted`, `Backend`, `Invariant`
- `SeqStore` trait stub (full impl in #50)
- `SubscriptionPolicy` trait stub with warmup-only scope (full impl in #51)
- Crate scaffold with `#![forbid(unsafe_code)]` and `#![warn(missing_docs)]`
- Unit tests for blanket impl, error variants, object-safety
