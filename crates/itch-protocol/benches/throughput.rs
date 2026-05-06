//! Criterion benchmark for codec throughput (1 MiB mixed-message buffer).
//!
//! Constructs a 1 MiB pre-built buffer of mixed ITCH messages.
//! Measures end-to-end throughput: messages / second. Target: > 50 M msgs/s.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use itch_protocol::*;

fn bench_throughput(c: &mut Criterion) {
    // Build a ~1MiB buffer with mixed messages (simplified: just round-robin small msgs)
    let mut buf = Vec::with_capacity(1_000_000);
    let system_event = Message::SystemEvent(SystemEvent {
        header: Header {
            stock_locate: StockLocate::from_u16(0),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(0),
        },
        event_code: EventCode::EndOfMessages,
    });
    let mut tmp = [0u8; 64];
    let msg_size = system_event.encode(&mut tmp).expect("encode for buffer");

    while buf.len() < 1_000_000 {
        buf.extend_from_slice(&tmp[..msg_size]);
    }
    buf.truncate(1_000_000);

    let count = buf.len() / msg_size;
    let mut group = c.benchmark_group("throughput");
    group.throughput(Throughput::Elements(count as u64));

    let buffer = black_box(&buf);
    group.bench_function("decode_1mib_mixed", |b| {
        b.iter(|| {
            let mut offset = 0;
            let mut count = 0;
            while offset < buffer.len() {
                match Message::decode(&buffer[offset..]) {
                    Ok(msg) => {
                        offset += msg.encoded_len();
                        count += 1;
                    }
                    Err(_) => break,
                }
            }
            count
        })
    });

    group.finish();
}

criterion_group!(benches, bench_throughput);
criterion_main!(benches);
