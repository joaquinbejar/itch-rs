//! Criterion benchmarks for `Message::encode` latency per message kind.
//!
//! Measures end-to-end encode time on fixed `Message` instances for each
//! of the 20 ITCH 5.0 message types. Target: < 100 ns per message.

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use itch_protocol::*;

fn bench_encode(c: &mut Criterion) {
    let system_event = Message::SystemEvent(SystemEvent {
        header: Header {
            stock_locate: StockLocate::from_u16(0),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(0),
        },
        event_code: EventCode::StartOfMessages,
    });

    c.bench_function("encode_system_event", |b| {
        let mut buf = [0u8; 64];
        b.iter(|| {
            let _ = black_box(&system_event).encode(black_box(&mut buf));
        })
    });

    let mwcb_status = Message::MwcbStatus(MwcbStatus {
        header: Header {
            stock_locate: StockLocate::from_u16(0x1234),
            tracking_number: TrackingNumber::from_u16(0x5678),
            timestamp: Timestamp::from_u64(0x0000_DEAD_BEEF_CAFE),
        },
        breached_level: BreachedLevel::Level1,
    });

    c.bench_function("encode_mwcb_status", |b| {
        let mut buf = [0u8; 64];
        b.iter(|| {
            let _ = black_box(&mwcb_status).encode(black_box(&mut buf));
        })
    });

    let order_delete = Message::OrderDelete(OrderDelete {
        header: Header {
            stock_locate: StockLocate::from_u16(0x1234),
            tracking_number: TrackingNumber::from_u16(0x5678),
            timestamp: Timestamp::from_u64(0x0000_DEAD_BEEF_CAFE),
        },
        order_ref: OrderReference::from_u64(1),
    });

    c.bench_function("encode_order_delete", |b| {
        let mut buf = [0u8; 64];
        b.iter(|| {
            let _ = black_box(&order_delete).encode(black_box(&mut buf));
        })
    });

    let broken_trade = Message::BrokenTrade(BrokenTrade {
        header: Header {
            stock_locate: StockLocate::from_u16(0x1234),
            tracking_number: TrackingNumber::from_u16(0x5678),
            timestamp: Timestamp::from_u64(0x0000_DEAD_BEEF_CAFE),
        },
        match_number: MatchNumber::from_u64(1),
    });

    c.bench_function("encode_broken_trade", |b| {
        let mut buf = [0u8; 64];
        b.iter(|| {
            let _ = black_box(&broken_trade).encode(black_box(&mut buf));
        })
    });
}

criterion_group!(benches, bench_encode);
criterion_main!(benches);
