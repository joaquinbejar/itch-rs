# Changelog — `itch-server`

All notable changes to this crate will be documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to per-crate [SemVer](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `--transport tcp|soup|mold` flag (default `tcp`) — runtime
  dispatch across sibling transport crates per ADR-0008. The same
  `(addr, source, store, policy)` glue routes into
  `itch_tcp::Server::bind` or `itch_soup::SoupServer::bind`. The
  `mold` branch returns `io::ErrorKind::Unsupported` until
  `MoldPublisher` lands (issues #25 / #26).
- `itch-soup` and `itch-mold` are now hard dependencies — runtime
  dispatch (no `#[cfg(feature)]`) per ADR-0005 / ADR-0008.
- Unit tests for transport-flag parsing and the mold-not-available
  error path.

### Changed

- Rewrite main glue layer to use `itch_tcp::Server::bind(addr, source, store, policy).serve()`.
  **~30 LoC main per ADR-0012 acceptance**: source + store + policy constructor calls,
  no bespoke accept/send loops.
- Add `clap` CLI parser; `--bind ADDR` flag (default `127.0.0.1:9100`, fallback to
  `ITCH_BIND` env var for backward compat).
- Add `--source iterator|replay-glimpse|replay-raw` flag (default `iterator`).
  Iterator source wraps `canonical_session()` from `itch-source` crate.
  `replay-glimpse` / `replay-raw` return `Unsupported` until `itch-replay`
  ships in v0.5 (issue #47).
- Add `--cache-size N` flag (default 65 536) for the
  `RingBufferSeqStore` capacity.
- Add `--source-path PATH` flag (placeholder for the v0.5 replay
  sources).
- Use `itch-source` traits directly: `IteratorSource`,
  `RingBufferSeqStore`, `StaticPolicy`. Store and policy bound to
  `Server::bind` generics for zero-cost abstraction.

## 0.1.0 — 2026-05-05

### Added

- Demo replay binary that streams a hand-built synthetic ITCH 5.0
  session over `itch-tcp` to every connected client.
- `canonical_session()` returns a deterministic synthetic session
  exercising **all** 20 ITCH 5.0 message kinds (`S`, `R`, `H`, `Y`,
  `L`, `V`, `W`, `K`, `A`, `F`, `E`, `C`, `X`, `D`, `U`, `P`, `Q`,
  `B`, `I`, `N`), verified by an exhaustive `variant_of(&Message)`
  match-based test.
- Per-connection tokio tasks so accepts run in parallel.
- `tokio::signal::ctrl_c()` graceful-shutdown path.
- `ITCH_BIND` (default `127.0.0.1:9100`) and `ITCH_DELAY_MS`
  (default `1` ms) env vars to control the bind address and
  inter-message delay.

### Documented

- Top-of-file rustdoc explains the env vars and how to run.
