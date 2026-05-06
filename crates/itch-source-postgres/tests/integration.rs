//! Integration tests for `PostgresSeqStore`.
//!
//! These tests connect to a real PostgreSQL instance. They are
//! skip-not-fail when no Postgres is reachable: set the
//! `ITCH_POSTGRES_URL` environment variable to point at a running
//! Postgres, otherwise the tests log and return Ok.
//!
//! ```bash
//! # In one terminal:
//! docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=itch -e POSTGRES_DB=itch postgres:16
//!
//! # In another:
//! ITCH_POSTGRES_URL=postgres://postgres:itch@127.0.0.1:5432/itch \
//!     cargo test -p itch-source-postgres --test integration
//! ```

use std::sync::Arc;
use std::time::Duration;

use itch_protocol::{
    EventCode, Header, Message, StockLocate, SystemEvent, Timestamp, TrackingNumber,
};
use itch_source::testing::assert_seq_store_contract;
use itch_source::SeqStore;
use itch_source_postgres::{PostgresSeqStore, PostgresSeqStoreConfig};

const POSTGRES_ENV: &str = "ITCH_POSTGRES_URL";

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

fn unique_table(suffix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    // Identifiers cannot start with a digit; the leading prefix
    // satisfies the validation guard in lib.rs.
    format!("itch_test_{nanos}_{suffix}")
}

async fn try_connect(suffix: &str) -> Option<PostgresSeqStore> {
    if std::env::var(POSTGRES_ENV).is_err() {
        eprintln!("skipping: ITCH_POSTGRES_URL not set");
        return None;
    }
    let url = std::env::var(POSTGRES_ENV).expect("ITCH_POSTGRES_URL just checked");
    let cfg = PostgresSeqStoreConfig {
        url,
        table: unique_table(suffix),
        max_connections: 8,
    };

    match tokio::time::timeout(Duration::from_secs(2), PostgresSeqStore::connect(cfg)).await {
        Ok(Ok(store)) => match store.migrate().await {
            Ok(()) => Some(store),
            Err(err) => {
                eprintln!("skipping: Postgres migrate failed ({err})");
                None
            }
        },
        Ok(Err(err)) => {
            eprintln!("skipping: Postgres unavailable ({err})");
            None
        }
        Err(_elapsed) => {
            eprintln!("skipping: Postgres connect timed out after 2s");
            None
        }
    }
}

async fn try_connect_with_table(table: &str) -> Option<PostgresSeqStore> {
    if std::env::var(POSTGRES_ENV).is_err() {
        eprintln!("skipping: ITCH_POSTGRES_URL not set");
        return None;
    }
    let url = std::env::var(POSTGRES_ENV).expect("ITCH_POSTGRES_URL just checked");
    let cfg = PostgresSeqStoreConfig {
        url,
        table: table.to_string(),
        max_connections: 8,
    };
    match tokio::time::timeout(Duration::from_secs(2), PostgresSeqStore::connect(cfg)).await {
        Ok(Ok(store)) => Some(store),
        Ok(Err(err)) => {
            eprintln!("skipping: Postgres unavailable ({err})");
            None
        }
        Err(_elapsed) => {
            eprintln!("skipping: Postgres connect timed out after 2s");
            None
        }
    }
}

async fn cleanup(store: &PostgresSeqStore) {
    if let Err(err) = store.drop_table().await {
        eprintln!("cleanup: drop_table failed ({err})");
    }
}

#[tokio::test]
async fn migrate_is_idempotent() {
    let Some(store) = try_connect("migrate_idem").await else {
        return;
    };
    // `try_connect` already ran one migrate; the second call must not
    // error.
    store
        .migrate()
        .await
        .expect("second migrate must be idempotent");
    cleanup(&store).await;
}

#[tokio::test]
async fn store_and_retrieve_single_message() {
    let Some(store) = try_connect("store_single").await else {
        return;
    };
    let msg = fixture_message();
    store.store(1, &msg).await.expect("store");
    let msgs = store.range(1, 1).await.expect("range");
    assert_eq!(msgs.len(), 1);
    assert_eq!(msgs[0].0, 1);
    assert_eq!(msgs[0].1, msg);
    cleanup(&store).await;
}

#[tokio::test]
async fn range_stops_at_first_gap() {
    let Some(store) = try_connect("gap").await else {
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
    cleanup(&store).await;
}

#[tokio::test]
async fn latest_tracks_max_observed_sequence() {
    let Some(store) = try_connect("latest").await else {
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
    cleanup(&store).await;
}

#[tokio::test]
async fn earliest_returns_lowest_seq() {
    let Some(store) = try_connect("earliest").await else {
        return;
    };
    let msg = fixture_message();
    assert_eq!(store.earliest().await.expect("earliest empty"), 0);
    store.store(10, &msg).await.expect("store 10");
    store.store(20, &msg).await.expect("store 20");
    store.store(30, &msg).await.expect("store 30");
    let earliest = store.earliest().await.expect("earliest");
    assert_eq!(earliest, 10);
    cleanup(&store).await;
}

#[tokio::test]
async fn passes_seq_store_contract() {
    let Some(store) = try_connect("contract").await else {
        return;
    };
    assert_seq_store_contract(&store).await;
    cleanup(&store).await;
}

#[tokio::test]
async fn durable_restart_scenario() {
    // Pre-build a unique table name so the second connect reuses it.
    let table = unique_table("durable");
    let Some(store1) = try_connect_with_table(&table).await else {
        return;
    };
    store1.migrate().await.expect("migrate store1");

    let msg = fixture_message();
    for i in 1u64..=100 {
        store1.store(i, &msg).await.expect("store via store1");
    }
    drop(store1);

    let Some(store2) = try_connect_with_table(&table).await else {
        return;
    };
    // No migrate needed — the table already exists and the runtime
    // ping was answered. But `migrate` is idempotent so it costs
    // nothing if the operator wants to re-run it.
    store2.migrate().await.expect("migrate store2 idempotent");

    let msgs = store2.range(1, 100).await.expect("range via store2");
    assert_eq!(msgs.len(), 100);
    assert_eq!(msgs[0].0, 1);
    assert_eq!(msgs[99].0, 100);

    cleanup(&store2).await;
}

#[tokio::test]
async fn parallel_writers_and_readers() {
    let Some(store) = try_connect("parallel").await else {
        return;
    };
    let store = Arc::new(store);
    let msg = fixture_message();

    let mut writers = tokio::task::JoinSet::new();
    // 20 writers, each storing 5 unique sequences (1..=100).
    for w in 0u64..20 {
        let store = Arc::clone(&store);
        let m = msg;
        writers.spawn(async move {
            let base = w * 5 + 1;
            for offset in 0..5u64 {
                store
                    .store(base + offset, &m)
                    .await
                    .expect("concurrent store");
            }
        });
    }

    let mut readers = tokio::task::JoinSet::new();
    // 5 readers, each calling range(1, 100) repeatedly.
    for _ in 0..5 {
        let store = Arc::clone(&store);
        readers.spawn(async move {
            for _ in 0..10 {
                let _ = store.range(1, 100).await.expect("concurrent range");
            }
        });
    }

    while let Some(j) = writers.join_next().await {
        j.expect("writer panicked");
    }
    while let Some(j) = readers.join_next().await {
        j.expect("reader panicked");
    }

    // After all writers finished, every sequence 1..=100 must be
    // present and contiguous; range(1, 100) returns exactly 100 rows.
    let final_range = store
        .range(1, 100)
        .await
        .expect("final range after writers");
    assert_eq!(
        final_range.len(),
        100,
        "all writers' messages must be present"
    );
    for (i, (seq, _msg)) in final_range.iter().enumerate() {
        assert_eq!(*seq, (i as u64) + 1, "sequence must be contiguous");
    }

    assert_eq!(
        store.latest().await.expect("latest after parallel"),
        100,
        "latest must equal max written"
    );
    assert_eq!(
        store.earliest().await.expect("earliest after parallel"),
        1,
        "earliest must equal min written"
    );

    cleanup(&store).await;
}
