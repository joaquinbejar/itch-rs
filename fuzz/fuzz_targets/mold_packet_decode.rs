//! Fuzz target: `itch_mold::MoldPacket::decode`.
//!
//! `MoldPacket` is the structured downstream packet (header + N
//! message blocks). Per ADR-0010 the decoder must never panic on
//! arbitrary input — every framing failure surfaces as a typed
//! `MoldError`.

#![no_main]

use itch_mold::MoldPacket;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = MoldPacket::decode(data);
});
