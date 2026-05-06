//! `BookManager` integration tests.
//!
//! Drives a synthetic multi-symbol message stream through
//! [`BookManager`] and asserts on per-symbol routing, lazy book
//! creation, the `R` Stock Directory cache, the `last_seq` counter,
//! and (under `feature = "tokio-stream"`) the async runner.

use itch_book::{BookError, BookManager};
use itch_protocol::{
    AddOrder, Authenticity, FinancialStatus, Header, LuldTier, MarketCategory, Message,
    OrderReference, Price4, Shares, Side, Stock, StockDirectory, StockLocate, Timestamp,
    TrackingNumber, YesNo,
};

fn header_for(locate: u16) -> Header {
    Header {
        stock_locate: StockLocate::from_u16(locate),
        tracking_number: TrackingNumber::from_u16(0),
        timestamp: Timestamp::from_u64(32_400_000_000_000),
    }
}

fn add(locate: u16, order_ref: u64, side: Side, shares: u32, price: u32) -> Message {
    Message::AddOrder(AddOrder {
        header: header_for(locate),
        order_ref: OrderReference::from_u64(order_ref),
        side,
        shares: Shares::from_u32(shares),
        stock: Stock::default(),
        price: Price4::from_u32(price),
    })
}

fn directory(locate: u16, symbol: &str) -> Message {
    Message::StockDirectory(StockDirectory {
        header: header_for(locate),
        stock: Stock::new(symbol),
        market_category: MarketCategory::NasdaqGlobalSelect,
        financial_status: FinancialStatus::Normal,
        round_lot_size: Shares::from_u32(100),
        round_lots_only: YesNo::No,
        issue_classification: b'C',
        issue_subtype: [b' '; 2],
        authenticity: Authenticity::Live,
        short_sale_threshold: YesNo::No,
        ipo_flag: YesNo::No,
        luld_reference_price_tier: LuldTier::Tier1,
        etp_flag: YesNo::No,
        etp_leverage_factor: 0,
        inverse_indicator: YesNo::No,
    })
}

#[test]
fn test_three_symbol_routing_creates_independent_l2_books() {
    let mut mgr = BookManager::new();
    // 2 buys per symbol on locates 1, 2, 3 — different order refs so
    // every locate has 2 distinct orders in its index.
    let stream = [
        add(1, 1, Side::Buy, 100, 10_000),
        add(1, 2, Side::Buy, 200, 9_900),
        add(2, 3, Side::Buy, 300, 20_000),
        add(2, 4, Side::Buy, 400, 19_900),
        add(3, 5, Side::Buy, 500, 30_000),
        add(3, 6, Side::Buy, 600, 29_900),
    ];
    for m in &stream {
        let res = mgr.apply(m);
        assert!(res.is_ok(), "apply failed: {res:?}");
    }
    for locate in [1u16, 2, 3] {
        let book = mgr
            .l2(StockLocate::from_u16(locate))
            .expect("L2 book exists");
        assert_eq!(book.order_count(), 2, "locate {locate}: order count");
        assert_eq!(book.stock_locate(), StockLocate::from_u16(locate));
    }
}

#[test]
fn test_l2_lookup_unknown_locate_returns_none_until_message_arrives() {
    let mut mgr = BookManager::new();
    let unseen = StockLocate::from_u16(99);
    assert!(mgr.l2(unseen).is_none());

    let res = mgr.apply(&add(99, 1, Side::Buy, 100, 10_000));
    assert!(res.is_ok());
    assert!(mgr.l2(unseen).is_some());
}

#[test]
fn test_stock_directory_populates_symbol_cache() {
    let mut mgr = BookManager::new();
    let locate = StockLocate::from_u16(1);
    assert!(mgr.symbol(locate).is_none());

    let res = mgr.apply(&directory(1, "AAPL"));
    assert!(res.is_ok());
    assert_eq!(mgr.symbol(locate), Some(&Stock::new("AAPL")));
}

#[test]
fn test_directory_accessor_returns_full_map() {
    let mut mgr = BookManager::new();
    let _ = mgr.apply(&directory(1, "AAPL"));
    let _ = mgr.apply(&directory(2, "MSFT"));
    let _ = mgr.apply(&directory(3, "GOOG"));

    let dir = mgr.directory();
    assert_eq!(dir.len(), 3);
    assert_eq!(
        dir.get(&StockLocate::from_u16(1)),
        Some(&Stock::new("AAPL"))
    );
    assert_eq!(
        dir.get(&StockLocate::from_u16(2)),
        Some(&Stock::new("MSFT"))
    );
    assert_eq!(
        dir.get(&StockLocate::from_u16(3)),
        Some(&Stock::new("GOOG"))
    );
}

#[test]
fn test_locates_iterator_yields_every_seen_locate() {
    let mut mgr = BookManager::new();
    let _ = mgr.apply(&add(1, 1, Side::Buy, 100, 10_000));
    let _ = mgr.apply(&add(2, 2, Side::Sell, 100, 20_000));
    let _ = mgr.apply(&add(3, 3, Side::Buy, 100, 30_000));

    let mut seen: Vec<u16> = mgr.locates().map(StockLocate::as_u16).collect();
    seen.sort_unstable();
    assert_eq!(seen, vec![1, 2, 3]);
}

#[test]
fn test_last_seq_increments_per_apply_call() {
    let mut mgr = BookManager::new();
    assert_eq!(mgr.last_seq(), 0);

    let stream = [
        add(1, 1, Side::Buy, 100, 10_000),
        add(2, 2, Side::Sell, 100, 20_000),
        directory(1, "AAPL"),
        add(3, 3, Side::Buy, 100, 30_000),
    ];
    for m in &stream {
        let _ = mgr.apply(m);
    }
    assert_eq!(mgr.last_seq(), stream.len() as u64);
}

#[test]
fn test_default_constructs_empty_manager() {
    let mgr = BookManager::default();
    assert_eq!(mgr.last_seq(), 0);
    assert!(mgr.directory().is_empty());
    assert_eq!(mgr.locates().count(), 0);
}

#[cfg(feature = "l3")]
#[test]
fn test_l3_book_lazily_created_alongside_l2() {
    let mut mgr = BookManager::new();
    let locate = StockLocate::from_u16(1);
    assert!(mgr.l3(locate).is_none());

    let res = mgr.apply(&add(1, 1, Side::Buy, 100, 10_000));
    assert!(res.is_ok());

    let l3 = mgr.l3(locate).expect("L3 book exists");
    assert_eq!(l3.order_count(), 1);
    assert_eq!(l3.stock_locate(), locate);
}

#[cfg(feature = "l3")]
#[test]
fn test_l3_routing_isolates_symbols() {
    let mut mgr = BookManager::new();
    let _ = mgr.apply(&add(1, 1, Side::Buy, 100, 10_000));
    let _ = mgr.apply(&add(1, 2, Side::Buy, 200, 9_900));
    let _ = mgr.apply(&add(2, 3, Side::Sell, 300, 20_000));

    let l3_one = mgr
        .l3(StockLocate::from_u16(1))
        .expect("L3 book for locate 1");
    let l3_two = mgr
        .l3(StockLocate::from_u16(2))
        .expect("L3 book for locate 2");
    assert_eq!(l3_one.order_count(), 2);
    assert_eq!(l3_two.order_count(), 1);
}

#[test]
fn test_apply_returns_error_from_inner_book() {
    use itch_protocol::{MatchNumber, OrderExecuted};
    let mut mgr = BookManager::new();
    let _ = mgr.apply(&add(1, 1, Side::Buy, 100, 10_000));

    // Over-execute order 1: 200 > 100 remaining → OverExecution.
    let bad = Message::OrderExecuted(OrderExecuted {
        header: header_for(1),
        order_ref: OrderReference::from_u64(1),
        executed_shares: Shares::from_u32(200),
        match_number: MatchNumber::from_u64(1),
    });
    let res = mgr.apply(&bad);
    assert!(matches!(res, Err(BookError::OverExecution { .. })));
    // last_seq still bumped — manager treated the message as observed.
    assert_eq!(mgr.last_seq(), 2);
}

#[cfg(feature = "tokio-stream")]
#[tokio::test]
async fn test_run_drains_stream_into_books() {
    use futures::stream;
    use itch_protocol::ProtocolError;

    let messages: Vec<Result<Message, ProtocolError>> = vec![
        Ok(directory(1, "AAPL")),
        Ok(add(1, 1, Side::Buy, 100, 10_000)),
        Ok(add(2, 2, Side::Sell, 200, 20_000)),
        Ok(add(3, 3, Side::Buy, 300, 30_000)),
    ];
    let s = stream::iter(messages);

    let mut mgr = BookManager::new();
    let res = mgr.run(s).await;
    assert!(res.is_ok(), "run failed: {res:?}");

    assert_eq!(mgr.last_seq(), 4);
    assert!(mgr.l2(StockLocate::from_u16(1)).is_some());
    assert!(mgr.l2(StockLocate::from_u16(2)).is_some());
    assert!(mgr.l2(StockLocate::from_u16(3)).is_some());
    assert_eq!(
        mgr.symbol(StockLocate::from_u16(1)),
        Some(&Stock::new("AAPL"))
    );
}

#[cfg(feature = "tokio-stream")]
#[tokio::test]
async fn test_run_surfaces_protocol_error_via_from() {
    use futures::stream;
    use itch_protocol::ProtocolError;

    let messages: Vec<Result<Message, ProtocolError>> = vec![
        Ok(add(1, 1, Side::Buy, 100, 10_000)),
        Err(ProtocolError::UnknownMessageType(0xFF)),
    ];
    let s = stream::iter(messages);

    let mut mgr = BookManager::new();
    let res = mgr.run(s).await;
    assert!(matches!(res, Err(BookError::Protocol(_))));
    // First message still applied before the error — last_seq == 1.
    assert_eq!(mgr.last_seq(), 1);
}
