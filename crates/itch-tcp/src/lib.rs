//! Naïve length-prefix TCP framing for `itch-protocol` (workspace
//! scaffold; the framing implementation lands in issue #9).
//!
//! Once issue #9 merges, this crate will expose `ItchCodec`,
//! `ItchConnection`, and `connect` / `bind` / `accept` over a 2-byte
//! big-endian length prefix that includes the 1-byte ITCH type tag.
//! Useful for tests, demos, and pre-production prototypes — **not**
//! a NASDAQ-conformant transport. For production unicast see
//! `itch-soup`; for production multicast see `itch-mold`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
