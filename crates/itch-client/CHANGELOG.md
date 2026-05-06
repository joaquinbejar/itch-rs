# Changelog — `itch-client`

All notable changes to this crate will be documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to per-crate [SemVer](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- `--transport tcp|soup|mold` flag (default `tcp`) — runtime
  dispatch across sibling transport crates per ADR-0008. `tcp`
  uses `itch_tcp::connect`; `soup` uses
  `itch_soup::ResilientSoupClient` (auto-reconnect + sequence
  resume per ADR-0009); `mold` is a stub returning exit code 1
  until `MoldStream` lands (issue #21).
- `--server ADDR` flag (defaults to `127.0.0.1:9100`, fallback
  to `ITCH_SERVER` env var for backward compat).
- `--soup-username` / `--soup-password` flags for the
  SoupBinTCP `Login Request` (only consulted when
  `--transport soup`).
- `clap` CLI parser; `itch-soup` and `itch-mold` hard
  dependencies (runtime dispatch, no `#[cfg(feature)]`).
- Unit tests for transport-flag parsing and the mold-not-available
  exit code.

## 0.1.0 — 2026-05-05

### Added

- Demo subscriber binary that connects to `itch-server`, streams
  framed messages over `itch-tcp`, and prints one human-readable
  line per message to stdout.
- `fmt_message(&Message) -> String` — exhaustive over all 20
  `Message` variants. Adding a new ITCH revision fails to compile
  here, which is the desired behavior.
- Helpers: `fmt_price4` (4 decimals), `fmt_price8` (8 decimals,
  `MwcbDeclineLevel` payload), `fmt_side`, `stock_str`, `mpid_str`.
- `tokio::signal::ctrl_c()` stop path.
- Bad inner frame (`Protocol(_)`) → WARN, continue.
- Oversized frame (`FrameTooLarge`) → WARN, continue.
- Fatal transport error → ERROR, exit code 3.
- Premature server disconnect → exit code 5; clean
  `EndOfMessages` → exit code 0.
- Broken-pipe handling: `writeln!` over `stdout().lock()` returns
  SUCCESS on `BrokenPipe` so `cargo run | head` exits cleanly.
- `tracing-subscriber` writes to **stderr**; stdout is reserved
  for the per-message lines so `… | grep` stays clean.
- `ITCH_SERVER` env var (default `127.0.0.1:9100`).

### Documented

- Top-of-file rustdoc explains the env var and how to run.
