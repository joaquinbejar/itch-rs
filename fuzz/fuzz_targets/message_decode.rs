//! Fuzz target: `itch_protocol::Message::decode`.
//!
//! Per `docs/TESTING.md` §3 the decoder must never panic on
//! arbitrary input. The `decode_no_panic` proptest covers this for
//! short slices; this target lets `libfuzzer-sys` explore the
//! coverage-guided long tail.

#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = itch_protocol::Message::decode(data);
});
