//! `itch-rs` — convenience meta-crate re-exporting the per-layer
//! ITCH 5.0 crates behind feature flags.
//!
//! Per ADR-0007 the **canonical distribution** is the per-layer
//! crates (`itch-protocol`, `itch-tcp`, `itch-soup`, `itch-mold`,
//! `itch-replay`). They are independently versioned and are what
//! production users should depend on. This meta-crate exists for
//! casual consumers who would rather opt into a single dependency
//! and pick layers via Cargo features.
//!
//! # Layout
//!
//! Every layer is exposed as a sub-module with the same public API
//! as the underlying crate:
//!
//! | Feature   | Module           | Re-exports                     |
//! |-----------|------------------|--------------------------------|
//! | `protocol`| `itch_rs::protocol` | `itch-protocol` (always on)    |
//! | `tcp`     | `itch_rs::tcp`      | `itch-tcp`                     |
//! | `soup`    | `itch_rs::soup`     | `itch-soup`                    |
//! | `mold`    | `itch_rs::mold`     | `itch-mold`                    |
//! | `replay`  | `itch_rs::replay`   | `itch-replay`                  |
//! | `full`    | all of the above    |                                |
//!
//! Default feature set is `["protocol"]`. Pick the transport(s) you
//! need:
//!
//! ```toml
//! [dependencies]
//! itch-rs = { version = "0.1", features = ["soup"] }
//! # or, for everything in one line:
//! # itch-rs = { version = "0.1", features = ["full"] }
//! ```
//!
//! # Examples
//!
//! Decode a Glimpse `.itch` capture (feature `replay`):
//!
//! ```no_run
//! # #[cfg(feature = "replay")]
//! # fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use itch_rs::replay::{iter_messages, CaptureFormat};
//! use std::fs::File;
//!
//! let file = File::open("capture.itch")?;
//! for msg in iter_messages(file, CaptureFormat::Glimpse) {
//!     let msg = msg?;
//!     println!("{msg:?}");
//! }
//! # Ok(()) }
//! ```
//!
//! Subscribe over SoupBinTCP (feature `soup`):
//!
//! ```no_run
//! # #[cfg(feature = "soup")]
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! use itch_rs::soup::{login, SoupCredentials};
//! use tokio::net::TcpStream;
//!
//! let stream = TcpStream::connect("127.0.0.1:9000").await?;
//! let credentials = SoupCredentials::new("user".to_string(), "pass".to_string());
//! let mut conn = login(stream, credentials, "", 0).await?;
//! while let Some(item) = conn.next_message().await {
//!     let msg = item?;
//!     println!("{msg:?}");
//! }
//! # Ok(()) }
//! ```
//!
//! For end-to-end demos see the `itch-server` and `itch-client`
//! binaries in this workspace, and the runnable
//! `examples/quickstart.rs` (built with `--features tcp`).

#![forbid(unsafe_code)]
#![warn(missing_docs)]

/// `itch-protocol` re-export — sync, no I/O, no async; types,
/// codecs, and `ProtocolError` for the 20 ITCH 5.0 message kinds.
#[cfg(feature = "protocol")]
pub mod protocol {
    pub use itch_protocol::*;
}

/// `itch-tcp` re-export — naïve length-prefix TCP framing for
/// demos and integration testing. Not a production transport.
#[cfg(feature = "tcp")]
pub mod tcp {
    pub use itch_tcp::*;
}

/// `itch-soup` re-export — SoupBinTCP 3.00 (production unicast)
/// with login state machine, heartbeats, sequence-resume, and
/// `ResilientSoupClient`.
#[cfg(feature = "soup")]
pub mod soup {
    pub use itch_soup::*;
}

/// `itch-mold` re-export — MoldUDP64 V1.00 (production multicast)
/// with gap recovery, request server, publisher, and
/// end-of-session.
#[cfg(feature = "mold")]
pub mod mold {
    pub use itch_mold::*;
}

/// `itch-replay` re-export — `.itch` capture-file streamer
/// supporting Glimpse archive and raw-bodies formats.
#[cfg(feature = "replay")]
pub mod replay {
    pub use itch_replay::*;
}
