//! NASDAQ TotalView-ITCH 5.0 message types and binary codec
//! (workspace scaffold; the public API lands incrementally across
//! issues #2 – #8).
//!
//! Once the v0.1 cohort merges, this crate will define the typed
//! domain model (primitives, enums, message DTOs, the `Message`
//! enum) and a hand-rolled big-endian codec (`Encode` / `Decode`),
//! all sync — no I/O, no `async`, no `tokio`.
//!
//! Subsequent crates layer on top:
//! - `itch-tcp` — naïve length-prefix TCP framing (demos)
//! - `itch-soup` — SoupBinTCP 3.00 (planned)
//! - `itch-mold` — MoldUDP64 V1.00 (planned)
//!
//! See the workspace `README.md` for the high-level architecture.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
