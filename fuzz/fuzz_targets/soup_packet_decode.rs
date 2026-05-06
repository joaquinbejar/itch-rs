//! Fuzz target: `itch_soup::SoupCodec` decoder.

#![no_main]

use bytes::BytesMut;
use itch_soup::SoupCodec;
use libfuzzer_sys::fuzz_target;
use tokio_util::codec::Decoder;

fuzz_target!(|data: &[u8]| {
    let mut codec = SoupCodec::default();
    let mut buf = BytesMut::from(data);
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
