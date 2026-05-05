# Changelog — `itch-server`

All notable changes to this crate will be documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to per-crate [SemVer](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Changed

- Rewrite main glue layer to use `itch_tcp::Server::bind(addr, source, store, policy).serve()`.
  **~30 LoC main per ADR-0012 acceptance**: source + store + policy constructor calls,
  no bespoke accept/send loops.
- Add `clap` CLI parser; `--bind ADDR` flag (default `127.0.0.1:9100`, fallback to
  `ITCH_BIND` env var for backward compat).
- Add `--source iterator|replay-glimpse|replay-raw` flag (default `iterator`).
  Iterator source wraps `canonical_session()` from `itch-source` crate.
- Use `itch-source` traits directly: `IteratorSource`, `NullSeqStore`, `StaticPolicy`.
  Store and policy bound to `Server::bind` generics for zero-cost abstraction.

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
