//! Matching engine example: demonstrates itch-source trait wiring.
//!
//! Shows ChannelSource (async message stream), RingBufferSeqStore (for gap recovery),
//! and StaticPolicy (demo warmup). See docs/source-example.md for full patterns.

use itch_source::{ChannelSource, RingBufferSeqStore, StaticPolicy, SubscriptionPolicy};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("itch-source matching engine example");
    println!();
    println!("Demonstrating three traits:");
    println!("  • MessageSource — channel-based message stream");
    println!("  • SeqStore — in-memory ring buffer for gap recovery");
    println!("  • SubscriptionPolicy — demo warmup");
    println!();

    // MessageSource: create unbounded channel
    // ChannelSource::unbounded() returns (UnboundedSender, UnboundedChannelSource)
    let (_tx, _source) = ChannelSource::unbounded();
    println!("✓ ChannelSource created");

    // SeqStore: ring buffer for gap recovery on reconnect
    let _store = RingBufferSeqStore::with_capacity(65536);
    println!("✓ RingBufferSeqStore created (max 65k cached messages)");

    // SubscriptionPolicy: demo mode with empty warmup
    let policy = StaticPolicy::empty();
    let warmup = policy.warmup().await?;
    println!("✓ StaticPolicy created (warmup: {} messages)", warmup.len());

    println!();
    println!("These three traits wire into:");
    println!("  • itch-tcp (issue #54): itch_tcp::Server::bind(addr, source, store, policy)");
    println!("  • itch-soup (v0.3): SoupServer with reconnect-via-SeqStore");
    println!("  • itch-mold (v0.3): MoldPublisher with gap-recovery-via-SeqStore");
    println!();
    println!("See docs/source-example.md for full integration patterns.");

    Ok(())
}
