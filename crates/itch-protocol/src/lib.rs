//! NASDAQ TotalView-ITCH 5.0 message types and binary codec.
//!
//! `itch-protocol` is the leaf crate of the `itch-rs` workspace: it
//! defines the typed domain model (primitives, enums, message DTOs,
//! the `Message` enum) and a hand-rolled big-endian codec
//! (`Encode` / `Decode`). It does no I/O, no `async`, no `tokio`.
//!
//! Subsequent crates layer on top:
//! - `itch-tcp` — naïve length-prefix TCP framing (demos)
//! - `itch-soup` — SoupBinTCP 3.00 (planned)
//! - `itch-mold` — MoldUDP64 V1.00 (planned)
//!
//! See the workspace `README.md` for the high-level architecture.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
