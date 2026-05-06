# Changelog — `itch-rs`

All notable changes to this crate will be documented here. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to per-crate [SemVer](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- Initial meta-crate re-exporting the per-layer ITCH 5.0 crates behind feature flags (per ADR-0007). Features: `protocol` (default), `tcp`, `soup`, `mold`, `replay`, `full`. Each feature gates a sub-module (`itch_rs::protocol`, `itch_rs::tcp`, …) that pub-uses the underlying crate's public API verbatim.
- `examples/quickstart.rs` — connects to `itch-server` over TCP (`--features tcp`) and prints 5 decoded messages.
- `README.md` and rustdoc explain that the per-layer crates remain the canonical API; this crate is convenience-only.

### Notes

- `#![forbid(unsafe_code)]` and `#![warn(missing_docs)]` on the crate root.
- No new dependencies — every layer is consumed via the workspace `[workspace.dependencies]` table.
