//! Canonical conformance vectors and async test drivers for any
//! third-party Rust ITCH 5.0 implementation.
//!
//! `itch-conformance` ships byte-for-byte identical wire vectors,
//! enum-byte tables, and (optionally) async transport scenario
//! drivers, so a downstream implementation can self-certify that it
//! decodes, encodes, and operates compatibly with the reference
//! implementation in this workspace.
//!
//! # Self-certifying as a downstream consumer
//!
//! Add `itch-conformance` as a `[dev-dependencies]` entry and write
//! a single integration test that loops over every published vector:
//!
//! ```ignore
//! use itch_conformance::vectors;
//! use itch_protocol::{Decode, Encode, Message};
//!
//! #[test]
//! fn third_party_decoder_matches_canonical_vectors() {
//!     for v in vectors::all() {
//!         let decoded = Message::decode(v.bytes).expect(v.name);
//!         assert_eq!(decoded, (v.message)(), "{}: decode mismatch", v.name);
//!
//!         let mut buf = vec![0u8; decoded.encoded_len()];
//!         let n = decoded.encode(&mut buf).expect(v.name);
//!         assert_eq!(&buf[..n], v.bytes, "{}: encode mismatch", v.name);
//!     }
//! }
//! ```
//!
//! The same loop drives both directions:
//! `decode(bytes) == message` AND `encode(message) == bytes`.
//!
//! # Immutability of the goldens
//!
//! Per [ADR-0007](https://github.com/joaquinbejar/itch-rs/blob/main/docs/adr/0007-a-la-carte-crates.md),
//! the byte vectors and the `Message` they decode to are **immutable
//! data**. Any change to a published vector is a wire-format
//! regression and ships only as a major bump. New vectors may be
//! added in minor bumps; never delete or mutate an existing one.
//!
//! # Closed-set ASCII enums
//!
//! For every `AlphaCoded` enum in `itch-protocol`, the
//! [`enum_vectors`] module exposes:
//!
//! - the full `(byte, variant)` table of documented variants, and
//! - a small set of `unknown_bytes` that any conformant decoder must
//!   surface as `ProtocolError::InvalidEnumCode` when injected into
//!   the matching field of a real message body.
//!
//! # Transport drivers (feature-gated)
//!
//! The `soup` and `mold` features add async best-effort drivers
//! ([`soup::drive_server`] and [`mold::drive_publisher`]) that walk a
//! third-party server / publisher through the documented session
//! scenarios (login accept / reject, sequenced flow, heartbeat,
//! reconnect, end of session, gap recovery, …). The drivers report
//! pass / fail per scenario and never panic on a misbehaving peer.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod enum_vectors;
pub mod vectors;

#[cfg(feature = "mold")]
pub mod mold;
#[cfg(feature = "soup")]
pub mod soup;

pub use vectors::ConformanceVector;
