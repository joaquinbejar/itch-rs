//! Integration tests for `RedisSeqStore`.
//!
//! These tests connect to a real Redis instance. They are skip-not-fail
//! when no Redis is reachable: set the `ITCH_REDIS_URL` environment
//! variable (default `redis://127.0.0.1:6379`) to point at a running
//! Redis, otherwise the tests log and return Ok.
//!
//! ```bash
//! # In one terminal:
//! docker run --rm -p 6379:6379 redis:7
//!
//! # In another:
//! ITCH_REDIS_URL=redis://127.0.0.1:6379 \
//!     cargo test -p itch-source-redis --test integration
//! ```

use std::time::Duration;

use itch_protocol::{
    EventCode, Header, Message, StockLocate, SystemEvent, Timestamp, TrackingNumber,
};
use itch_source::testing::assert_seq_store_contract;
use itch_source::SeqStore;
use itch_source_redis::{RedisSeqStore, RedisSeqStoreConfig};

const REDIS_ENV: &str = "ITCH_REDIS_URL";

fn fixture_message() -> Message {
    Message::SystemEvent(SystemEvent {
        header: Header {
            stock_locate: StockLocate::from_u16(0),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(0),
        },
        event_code: EventCode::StartOfMessages,
    })
}

fn unique_prefix(suffix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    format!("itch:test:{nanos}:{suffix}")
}

async fn try_connect(prefix: &str, ttl: Duration) -> Option<RedisSeqStore> {
    let url = std::env::var(REDIS_ENV).unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());
    let cfg = RedisSeqStoreConfig {
        url,
        key_prefix: prefix.to_string(),
        ttl,
        max_connections: 4,
    };
    match RedisSeqStore::connect(cfg).await {
        Ok(store) => Some(store),
        Err(err) => {
            eprintln!("skipping: Redis unavailable ({err})");
            None
        }
    }
}

#[tokio::test]
async fn store_and_retrieve_single_message() {
    let Some(store) = try_connect(&unique_prefix("store"), Duration::from_secs(30)).await else {
        return;
    };
    let msg = fixture_message();
    store.store(1, &msg).await.expect("store");
    let msgs = store.range(1, 1).await.expect("range");
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].0, 1);
    assert_eq!(msgs[0].1, msg);
}

#[tokio::test]
async fn range_stops_at_first_gap() {
    let Some(store) = try_connect(&unique_prefix("gap"), Duration::from_secs(30)).await else {
        return;
    };
    let msg = fixture_message();
    store.store(1, &msg).await.expect("store 1");
    store.store(2, &msg).await.expect("store 2");
    store.store(4, &msg).await.expect("store 4 (gap at 3)");
    let msgs = store.range(1, 10).await.expect("range");
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].0, 1);
    assert_eq!(msgs[1].0, 2);
}

#[tokio::test]
async fn latest_tracks_max_observed_sequence() {
    let Some(store) = try_connect(&unique_prefix("latest"), Duration::from_secs(30)).await else {
        return;
    };
    let msg = fixture_message();
    assert_eq!(store.latest().await.expect("latest empty"), 0);
    store.store(1, &msg).await.expect("store 1");
    assert_eq!(store.latest().await.expect("latest 1"), 1);
    store.store(5, &msg).await.expect("store 5");
    assert_eq!(store.latest().await.expect("latest 5"), 5);
    store.store(3, &msg).await.expect("store 3 (out-of-order)");
    assert_eq!(
        store.latest().await.expect("latest still 5"),
        5,
        "out-of-order store must not regress latest"
    );
}

#[tokio::test]
async fn earliest_returns_lowest_floor_marker() {
    let Some(store) = try_connect(&unique_prefix("earliest"), Duration::from_secs(30)).await else {
        return;
    };
    let msg = fixture_message();
    store.store(10, &msg).await.expect("store 10");
    store.store(20, &msg).await.expect("store 20");
    store.store(30, &msg).await.expect("store 30");
    let earliest = store.earliest().await.expect("earliest");
    assert_eq!(earliest, 10);
}

#[tokio::test]
async fn ttl_expiry_drops_message_from_range() {
    let Some(store) = try_connect(&unique_prefix("ttl"), Duration::from_secs(1)).await else {
        return;
    };
    let msg = fixture_message();
    store.store(1, &msg).await.expect("store");
    let immediate = store.range(1, 1).await.expect("range immediate");
    assert_eq!(immediate.len(), 1);
    tokio::time::sleep(Duration::from_secs(2)).await;
    let after = store.range(1, 1).await.expect("range after ttl");
    assert!(after.is_empty(), "TTL expiry should drop the message");
}

#[tokio::test]
async fn reconnect_persistence_within_ttl() {
    let prefix = unique_prefix("persist");
    let Some(store1) = try_connect(&prefix, Duration::from_secs(30)).await else {
        return;
    };
    let msg = fixture_message();
    for i in 1u64..=10 {
        store1.store(i, &msg).await.expect("store via store1");
    }
    drop(store1);

    let Some(store2) = try_connect(&prefix, Duration::from_secs(30)).await else {
        return;
    };
    let msgs = store2.range(1, 10).await.expect("range via store2");
    assert_eq!(msgs.len(), 10);
    assert_eq!(msgs[0].0, 1);
    assert_eq!(msgs[9].0, 10);
}

#[tokio::test]
async fn passes_seq_store_contract() {
    let Some(store) = try_connect(&unique_prefix("contract"), Duration::from_secs(30)).await else {
        return;
    };
    assert_seq_store_contract(&store).await;
}
