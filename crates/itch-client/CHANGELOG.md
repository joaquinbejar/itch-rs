# Changelog — `itch-client`

All notable changes to this crate will be documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to per-crate [SemVer](https://semver.org/spec/v2.0.0.html).

## Unreleased

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
