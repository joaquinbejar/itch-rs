//! Naïve length-prefix TCP framing for `itch-protocol`.
//!
//! A 2-byte big-endian length prefix (which includes the 1-byte ITCH
//! type tag) precedes each message. Useful for tests, demos, and
//! pre-production prototypes — **not** a NASDAQ-conformant transport.
//! For production unicast see `itch-soup`; for production multicast
//! see `itch-mold`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
