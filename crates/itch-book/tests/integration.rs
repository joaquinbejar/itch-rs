//! L2 book integration tests.
//!
//! Each test constructs `Message` values with literal `itch-protocol`
//! types and feeds them into an `L2Book`, asserting on the level
//! totals, order index, and error variants. The exhaustive match
//! over `Message` inside `L2Book::apply` is a compile-time invariant:
//! adding a new variant in `itch-protocol` makes this crate fail to
//! build until the new variant is handled.

use itch_book::{BookError, L2Book};
use itch_protocol::{
    AddOrder, AddOrderWithMpid, Header, Message, OrderCancel, OrderDelete, OrderExecuted,
    OrderExecutedWithPrice, OrderReference, OrderReplace, Price4, Printable, Shares, Side, Stock,
    StockLocate, Timestamp, TrackingNumber,
};

const SYMBOL_LOCATE: u16 = 42;
const OTHER_LOCATE: u16 = 99;

fn header_for(locate: u16) -> Header {
    Header {
        stock_locate: StockLocate::from_u16(locate),
        tracking_number: TrackingNumber::from_u16(0),
        timestamp: Timestamp::from_u64(32_400_000_000_000),
    }
}

fn add(order_ref: u64, side: Side, shares: u32, price: u32) -> Message {
    Message::AddOrder(AddOrder {
        header: header_for(SYMBOL_LOCATE),
        order_ref: OrderReference::from_u64(order_ref),
        side,
        shares: Shares::from_u32(shares),
        stock: Stock::new("AAPL"),
        price: Price4::from_u32(price),
    })
}

fn add_with_mpid(order_ref: u64, side: Side, shares: u32, price: u32) -> Message {
    Message::AddOrderWithMpid(AddOrderWithMpid {
        header: header_for(SYMBOL_LOCATE),
        order_ref: OrderReference::from_u64(order_ref),
        side,
        shares: Shares::from_u32(shares),
        stock: Stock::new("AAPL"),
        price: Price4::from_u32(price),
        attribution: itch_protocol::Mpid::default(),
    })
}

fn execute(order_ref: u64, executed_shares: u32) -> Message {
    Message::OrderExecuted(OrderExecuted {
        header: header_for(SYMBOL_LOCATE),
        order_ref: OrderReference::from_u64(order_ref),
        executed_shares: Shares::from_u32(executed_shares),
        match_number: itch_protocol::MatchNumber::from_u64(1),
    })
}

fn execute_with_price(order_ref: u64, executed_shares: u32, exec_price: u32) -> Message {
    Message::OrderExecutedWithPrice(OrderExecutedWithPrice {
        header: header_for(SYMBOL_LOCATE),
        order_ref: OrderReference::from_u64(order_ref),
        executed_shares: Shares::from_u32(executed_shares),
        match_number: itch_protocol::MatchNumber::from_u64(2),
        printable: Printable::Printable,
        execution_price: Price4::from_u32(exec_price),
    })
}

fn cancel(order_ref: u64, cancelled_shares: u32) -> Message {
    Message::OrderCancel(OrderCancel {
        header: header_for(SYMBOL_LOCATE),
        order_ref: OrderReference::from_u64(order_ref),
        cancelled_shares: Shares::from_u32(cancelled_shares),
    })
}

fn delete(order_ref: u64) -> Message {
    Message::OrderDelete(OrderDelete {
        header: header_for(SYMBOL_LOCATE),
        order_ref: OrderReference::from_u64(order_ref),
    })
}

fn replace(original: u64, new_ref: u64, new_shares: u32, new_price: u32) -> Message {
    Message::OrderReplace(OrderReplace {
        header: header_for(SYMBOL_LOCATE),
        original_order_ref: OrderReference::from_u64(original),
        new_order_ref: OrderReference::from_u64(new_ref),
        shares: Shares::from_u32(new_shares),
        price: Price4::from_u32(new_price),
    })
}

fn fresh() -> L2Book {
    L2Book::new(StockLocate::from_u16(SYMBOL_LOCATE))
}

#[test]
fn empty_book_has_no_top_of_book() {
    let book = fresh();
    assert!(book.best_bid().is_none());
    assert!(book.best_ask().is_none());
    assert_eq!(book.order_count(), 0);
    assert_eq!(book.levels_count(Side::Buy), 0);
    assert_eq!(book.levels_count(Side::Sell), 0);
}

#[test]
fn five_buy_adds_yield_descending_levels_with_best_bid_first() {
    let mut book = fresh();
    // Prices: 100, 101, 102, 103, 104. Best bid = 104.
    for (i, price) in [100, 101, 102, 103, 104].iter().enumerate() {
        let msg = add((i as u64) + 1, Side::Buy, 100, *price);
        book.apply(&msg).expect("clean add");
    }

    assert_eq!(book.order_count(), 5);
    assert_eq!(book.levels_count(Side::Buy), 5);
    assert_eq!(book.best_bid(), Some((Price4::from_u32(104), 100)));

    let levels: Vec<_> = book.levels(Side::Buy).collect();
    assert_eq!(levels.len(), 5);
    // Descending: 104 → 100.
    assert_eq!(levels[0].0, Price4::from_u32(104));
    assert_eq!(levels[4].0, Price4::from_u32(100));
}

#[test]
fn five_sell_adds_yield_ascending_levels_with_best_ask_first() {
    let mut book = fresh();
    for (i, price) in [200, 201, 202, 203, 204].iter().enumerate() {
        let msg = add((i as u64) + 1, Side::Sell, 50, *price);
        book.apply(&msg).expect("clean add");
    }

    assert_eq!(book.order_count(), 5);
    assert_eq!(book.levels_count(Side::Sell), 5);
    assert_eq!(book.best_ask(), Some((Price4::from_u32(200), 50)));

    let levels: Vec<_> = book.levels(Side::Sell).collect();
    assert_eq!(levels.len(), 5);
    // Ascending: 200 → 204.
    assert_eq!(levels[0].0, Price4::from_u32(200));
    assert_eq!(levels[4].0, Price4::from_u32(204));
}

#[test]
fn add_then_delete_removes_level_and_drains_index() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).expect("add");
    assert_eq!(book.order_count(), 1);

    book.apply(&delete(1)).expect("delete");
    assert_eq!(book.order_count(), 0);
    assert_eq!(book.levels_count(Side::Buy), 0);
    assert!(book.best_bid().is_none());
}

#[test]
fn add_then_partial_execute_reduces_level_shares_keeps_order() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).expect("add");

    book.apply(&execute(1, 200)).expect("partial exec");
    assert_eq!(book.order_count(), 1);
    assert_eq!(book.best_bid(), Some((Price4::from_u32(1_000), 300)));
}

#[test]
fn add_then_full_execute_drops_level_entirely() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).expect("add");
    book.apply(&execute(1, 500)).expect("full exec");

    assert_eq!(book.order_count(), 0);
    assert_eq!(book.levels_count(Side::Buy), 0);
    assert!(book.best_bid().is_none());
}

#[test]
fn over_execute_returns_over_execution_error_with_remaining_500() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).expect("add");
    let err = book
        .apply(&execute(1, 600))
        .expect_err("over-execution must error");
    match err {
        BookError::OverExecution {
            executed,
            remaining,
            order,
        } => {
            assert_eq!(executed, 600);
            assert_eq!(remaining, 500);
            assert_eq!(order, OrderReference::from_u64(1));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn replace_moves_to_new_price_level_and_new_ref() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).expect("add");
    book.apply(&replace(1, 2, 300, 1_010)).expect("replace");

    // Old level removed; new level present at new price with new shares.
    assert_eq!(book.order_count(), 1);
    assert_eq!(book.levels_count(Side::Buy), 1);
    assert_eq!(book.best_bid(), Some((Price4::from_u32(1_010), 300)));

    // Old ref is gone; the new ref drives the next exec.
    book.apply(&execute(2, 100)).expect("exec on new ref");
    assert_eq!(book.best_bid(), Some((Price4::from_u32(1_010), 200)));
}

#[test]
fn execute_with_price_does_not_create_a_level_at_the_trade_price() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).expect("add");
    // Trade prints at a midpoint price (1_005); the resting level
    // stays at 1_000 with 200 shares left.
    book.apply(&execute_with_price(1, 300, 1_005))
        .expect("exec with price");
    assert_eq!(book.levels_count(Side::Buy), 1);
    assert_eq!(book.best_bid(), Some((Price4::from_u32(1_000), 200)));
}

#[test]
fn cancel_partial_reduces_level_and_remaining_shares() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).expect("add");
    book.apply(&cancel(1, 200)).expect("cancel partial");
    assert_eq!(book.best_bid(), Some((Price4::from_u32(1_000), 300)));
    assert_eq!(book.order_count(), 1);
}

#[test]
fn cancel_over_subtracts_returns_over_execution_error() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).expect("add");
    let err = book
        .apply(&cancel(1, 700))
        .expect_err("over-cancel must error");
    assert!(matches!(
        err,
        BookError::OverExecution {
            executed: 700,
            remaining: 500,
            ..
        }
    ));
}

#[test]
fn add_for_other_symbol_is_silently_ignored() {
    let mut book = fresh();
    let other = Message::AddOrder(AddOrder {
        header: header_for(OTHER_LOCATE),
        order_ref: OrderReference::from_u64(1),
        side: Side::Buy,
        shares: Shares::from_u32(500),
        stock: Stock::new("MSFT"),
        price: Price4::from_u32(1_000),
    });
    book.apply(&other).expect("filtered");
    assert_eq!(book.order_count(), 0);
    assert!(book.best_bid().is_none());
}

#[test]
fn execute_for_unknown_order_in_global_stream_is_silently_dropped() {
    // No add for ref 1 — `E` for it must no-op rather than error,
    // because the global multi-symbol stream may carry mutations on
    // orders belonging to a different book's symbol.
    let mut book = fresh();
    book.apply(&execute(1, 100)).expect("dropped, not error");
    assert_eq!(book.order_count(), 0);
}

#[test]
fn add_with_mpid_behaves_identically_to_add_for_l2() {
    let mut book = fresh();
    book.apply(&add_with_mpid(1, Side::Buy, 500, 1_000))
        .expect("add F");
    assert_eq!(book.best_bid(), Some((Price4::from_u32(1_000), 500)));
    assert_eq!(book.order_count(), 1);
}

#[test]
fn multiple_orders_at_same_level_aggregate_share_total() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).expect("add 1");
    book.apply(&add(2, Side::Buy, 300, 1_000)).expect("add 2");
    assert_eq!(book.levels_count(Side::Buy), 1);
    assert_eq!(book.best_bid(), Some((Price4::from_u32(1_000), 800)));

    // Deleting one does not collapse the other.
    book.apply(&delete(1)).expect("delete 1");
    assert_eq!(book.best_bid(), Some((Price4::from_u32(1_000), 300)));
    assert_eq!(book.order_count(), 1);
}

#[test]
fn session_level_messages_are_no_ops_for_l2() {
    use itch_protocol::{
        BrokenTrade, CrossTrade, CrossType, EventCode, IpoQuotingPeriodUpdate, MatchNumber,
        MwcbDeclineLevel, MwcbStatus, RegShoRestriction, RetailPriceImprovement, StockDirectory,
        StockTradingAction, SystemEvent,
    };

    let mut book = fresh();

    // A representative no-op message of every session-level kind. The
    // book must remain empty after applying each.
    let messages = [
        Message::SystemEvent(SystemEvent {
            header: header_for(0),
            event_code: EventCode::StartOfMessages,
        }),
        Message::StockDirectory(StockDirectory {
            header: header_for(SYMBOL_LOCATE),
            ..Default::default()
        }),
        Message::StockTradingAction(StockTradingAction {
            header: header_for(SYMBOL_LOCATE),
            ..Default::default()
        }),
        Message::RegShoRestriction(RegShoRestriction {
            header: header_for(SYMBOL_LOCATE),
            ..Default::default()
        }),
        Message::MwcbDeclineLevel(MwcbDeclineLevel {
            header: header_for(0),
            ..Default::default()
        }),
        Message::MwcbStatus(MwcbStatus {
            header: header_for(0),
            ..Default::default()
        }),
        Message::IpoQuotingPeriodUpdate(IpoQuotingPeriodUpdate {
            header: header_for(0),
            ..Default::default()
        }),
        Message::CrossTrade(CrossTrade {
            header: header_for(SYMBOL_LOCATE),
            shares: 0,
            stock: Stock::new("AAPL"),
            cross_price: Price4::from_u32(1_000),
            match_number: MatchNumber::from_u64(1),
            cross_type: CrossType::Opening,
        }),
        Message::BrokenTrade(BrokenTrade {
            header: header_for(SYMBOL_LOCATE),
            match_number: MatchNumber::from_u64(2),
        }),
        Message::RetailPriceImprovement(RetailPriceImprovement {
            header: header_for(SYMBOL_LOCATE),
            ..Default::default()
        }),
    ];
    for m in &messages {
        book.apply(m).expect("session-level no-op");
    }
    assert_eq!(book.order_count(), 0);
    assert!(book.best_bid().is_none());
    assert!(book.best_ask().is_none());
}
