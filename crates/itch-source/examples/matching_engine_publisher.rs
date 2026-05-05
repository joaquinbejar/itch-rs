//! Matching-engine publisher example for `itch-source`.
//!
//! Demonstrates the three `itch-source` traits wired together:
//!
//! - `MessageSource` — a `ChannelSource` fed by a simulated
//!   matching engine that emits `AddOrder` / `OrderExecuted` /
//!   `OrderDelete` messages on a fixed deterministic seed.
//! - `SeqStore` — a `RingBufferSeqStore` that captures every
//!   message keyed by sequence number for retransmission.
//! - `SubscriptionPolicy` — `StaticPolicy::empty()` (no warmup).
//!
//! No `OrderBook-rs` dependency — the "engine" is a self-contained
//! tokio task with a hand-rolled LCG.
//!
//! Run with:
//!
//! ```bash
//! cargo run -p itch-source --example matching_engine_publisher
//! ```
//!
//! See `docs/source-example.md` for the full integration patterns.

use std::time::Duration;

use futures::StreamExt;
use itch_protocol::{
    enums::Side, messages::Header, AddOrder, MatchNumber, Message, OrderDelete, OrderExecuted,
    OrderReference, Price4, Shares, Stock, StockLocate, Timestamp, TrackingNumber,
};
use itch_source::{
    ChannelSource, MessageSource, RingBufferSeqStore, SeqStore, StaticPolicy, SubscriptionPolicy,
};

/// Hand-rolled deterministic LCG so the example doesn't pull `rand`.
struct Rng(u64);

impl Rng {
    fn next_u32(&mut self) -> u32 {
        self.0 = self.0.wrapping_mul(1_103_515_245).wrapping_add(12_345);
        ((self.0 >> 16) & 0xFFFF_FFFF) as u32
    }
}

fn header(seq: u64) -> Header {
    Header {
        stock_locate: StockLocate::from_u16(1),
        tracking_number: TrackingNumber::from_u16(0),
        timestamp: Timestamp::from_u64(32_400_000_000_000 + seq * 1_000_000),
    }
}

fn synth(seq: u64, rng: &mut Rng) -> Message {
    match rng.next_u32() % 3 {
        0 => Message::AddOrder(AddOrder {
            header: header(seq),
            order_ref: OrderReference::from_u64(1000 + seq),
            side: if rng.next_u32() & 1 == 0 {
                Side::Buy
            } else {
                Side::Sell
            },
            shares: Shares::from_u32(100 + (rng.next_u32() % 900)),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_900_000 + (rng.next_u32() % 50_000)),
        }),
        1 => Message::OrderExecuted(OrderExecuted {
            header: header(seq),
            order_ref: OrderReference::from_u64(1000 + seq.saturating_sub(1)),
            executed_shares: Shares::from_u32(50 + (rng.next_u32() % 100)),
            match_number: MatchNumber::from_u64(seq),
        }),
        _ => Message::OrderDelete(OrderDelete {
            header: header(seq),
            order_ref: OrderReference::from_u64(1000 + seq.saturating_sub(2)),
        }),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("itch-source matching-engine publisher example");
    println!("---------------------------------------------");

    // 1) MessageSource — bounded channel.
    let (tx, mut source) = ChannelSource::bounded(64);

    // 2) SeqStore — bounded ring keyed by sequence number.
    let store = RingBufferSeqStore::with_capacity(65_536);

    // 3) SubscriptionPolicy — demo: no warmup.
    let policy = StaticPolicy::empty();
    let warmup = policy.warmup().await?;
    println!("policy.warmup() returned {} messages", warmup.len());

    // 4) Spawn the simulated matching engine.
    const N: u64 = 10;
    tokio::spawn(async move {
        let mut rng = Rng(0xC0FFEE42);
        for seq in 1..=N {
            let m = synth(seq, &mut rng);
            if tx.send(m).await.is_err() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        // Sender dropped here → ChannelSource yields Ready(None).
    });

    // 5) Compile-time witness that ChannelSource implements MessageSource.
    fn _is_source(_: &dyn MessageSource) {}
    _is_source(&source);

    // 6) Drain the source: print + persist into the SeqStore.
    let mut seq: u64 = 1;
    while let Some(item) = source.next().await {
        let m = item?;
        store.store(seq, &m).await?;
        println!(
            "  seq={seq:>3} tag={} body_len={}",
            char::from(m.tag()),
            m.body_len()
        );
        seq += 1;
    }
    let stored = seq - 1;

    // 7) Show the SeqStore would answer a retransmission request.
    let latest = store.latest().await?;
    let earliest = store.earliest().await?;
    println!();
    println!("SeqStore retained {stored} frames (earliest={earliest}, latest={latest})");
    let resume = store.range(latest.saturating_sub(2).max(1), 3).await?;
    println!(
        "retransmit window range(latest-2, 3) = {} frames",
        resume.len()
    );

    Ok(())
}
