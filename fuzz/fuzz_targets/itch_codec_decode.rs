//! Fuzz target: `itch_tcp::ItchCodec` decoder.
//!
//! Wraps the codec in a `BytesMut` and asserts no panic on
//! arbitrary input. Mirrors how the codec is plugged into
//! `tokio_util::codec::Framed`.

#![no_main]

use bytes::BytesMut;
use itch_tcp::ItchCodec;
use libfuzzer_sys::fuzz_target;
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    let mut codec = ItchCodec::default();
    let mut buf = BytesMut::from(data);
    // Drain repeatedly until the decoder reports `Ok(None)` (need
    // more data) or `Err(_)`. Safety: bounded by buffer length so
    // the loop cannot run forever on a hostile input.
    let mut budget = 4 * 1024;
    loop {
        match codec.decode(&mut buf) {
            Ok(Some(_)) => {
                budget -= 1;
                if budget == 0 {
                    break;
                }
            }
            Ok(None) | Err(_) => break,
        }
    }
});
