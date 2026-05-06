//! L3 (per-order) book integration tests.
//!
//! Each test constructs `Message` values with literal `itch-protocol`
//! types and feeds them into an `L3Book`, asserting on the FIFO
//! queue, order index, and error variants. The exhaustive match over
//! `Message` inside `L3Book::apply` is a compile-time invariant: any
//! new variant in `itch-protocol` makes this crate fail to build
//! until the new variant is handled.

use itch_book::{BookError, L2Book, L3Book};
use itch_protocol::{
    AddOrder, AddOrderWithMpid, Header, MatchNumber, Message, Mpid, OrderCancel, OrderDelete,
    OrderExecuted, OrderExecutedWithPrice, OrderReference, OrderReplace, Price4, Printable, Shares,
    Side, Stock, StockLocate, Timestamp, TrackingNumber,
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

fn add_with_mpid(order_ref: u64, side: Side, shares: u32, price: u32, mpid: Mpid) -> Message {
    Message::AddOrderWithMpid(AddOrderWithMpid {
        header: header_for(SYMBOL_LOCATE),
        order_ref: OrderReference::from_u64(order_ref),
        side,
        shares: Shares::from_u32(shares),
        stock: Stock::new("AAPL"),
        price: Price4::from_u32(price),
        attribution: mpid,
    })
}

fn execute(order_ref: u64, executed_shares: u32) -> Message {
    Message::OrderExecuted(OrderExecuted {
        header: header_for(SYMBOL_LOCATE),
        order_ref: OrderReference::from_u64(order_ref),
        executed_shares: Shares::from_u32(executed_shares),
        match_number: MatchNumber::from_u64(1),
    })
}

fn execute_with_price(order_ref: u64, executed_shares: u32, exec_price: u32) -> Message {
    Message::OrderExecutedWithPrice(OrderExecutedWithPrice {
        header: header_for(SYMBOL_LOCATE),
        order_ref: OrderReference::from_u64(order_ref),
        executed_shares: Shares::from_u32(executed_shares),
        match_number: MatchNumber::from_u64(2),
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

fn fresh() -> L3Book {
    L3Book::new(StockLocate::from_u16(SYMBOL_LOCATE))
}

// ---------------------------------------------------------------------------
// Empty-book invariants
// ---------------------------------------------------------------------------

#[test]
fn empty_book_returns_none_for_lookups() {
    let book = fresh();
    assert!(book.order(OrderReference::from_u64(1)).is_none());
    assert!(book.queue_position(OrderReference::from_u64(1)).is_none());
    assert_eq!(book.order_count(), 0);
    assert_eq!(book.levels_count(Side::Buy), 0);
    assert_eq!(book.levels_count(Side::Sell), 0);
    assert_eq!(book.stock_locate(), StockLocate::from_u16(SYMBOL_LOCATE));
}

// ---------------------------------------------------------------------------
// Distinct levels — every order is at queue front
// ---------------------------------------------------------------------------

#[test]
fn five_buys_at_descending_prices_each_at_queue_front() {
    let mut book = fresh();
    for (i, price) in [104, 103, 102, 101, 100].iter().enumerate() {
        let r = (i as u64) + 1;
        book.apply(&add(r, Side::Buy, 100, *price))
            .expect("clean add");
    }
    assert_eq!(book.order_count(), 5);
    assert_eq!(book.levels_count(Side::Buy), 5);
    for r in 1..=5u64 {
        assert_eq!(
            book.queue_position(OrderReference::from_u64(r)),
            Some(0),
            "order {r} should be alone at the front of its level"
        );
    }
}

// ---------------------------------------------------------------------------
// Same level — arrival order yields 0,1,2 queue positions
// ---------------------------------------------------------------------------

#[test]
fn three_buys_at_same_price_have_arrival_order_queue_positions() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 100, 1_000)).unwrap();
    book.apply(&add(2, Side::Buy, 200, 1_000)).unwrap();
    book.apply(&add(3, Side::Buy, 300, 1_000)).unwrap();

    assert_eq!(book.levels_count(Side::Buy), 1);
    assert_eq!(book.order_count(), 3);
    assert_eq!(book.queue_position(OrderReference::from_u64(1)), Some(0));
    assert_eq!(book.queue_position(OrderReference::from_u64(2)), Some(1));
    assert_eq!(book.queue_position(OrderReference::from_u64(3)), Some(2));

    let refs: Vec<_> = book
        .level_orders(Side::Buy, Price4::from_u32(1_000))
        .map(|e| e.shares)
        .collect();
    assert_eq!(refs, vec![100, 200, 300]);
}

// ---------------------------------------------------------------------------
// FIFO — full execute on front pops the front, others advance
// ---------------------------------------------------------------------------

#[test]
fn full_execute_on_front_pops_front_and_others_advance_one_position() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 100, 1_000)).unwrap();
    book.apply(&add(2, Side::Buy, 200, 1_000)).unwrap();
    book.apply(&add(3, Side::Buy, 300, 1_000)).unwrap();

    book.apply(&execute(1, 100)).expect("front fully exec'd");

    assert!(book.order(OrderReference::from_u64(1)).is_none());
    assert_eq!(book.order_count(), 2);
    assert_eq!(book.queue_position(OrderReference::from_u64(2)), Some(0));
    assert_eq!(book.queue_position(OrderReference::from_u64(3)), Some(1));
}

#[test]
fn execute_with_price_full_pops_front_same_as_execute() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 100, 1_000)).unwrap();
    book.apply(&add(2, Side::Buy, 200, 1_000)).unwrap();

    // C with a different print price must still pop the front
    // because resting price is unchanged.
    book.apply(&execute_with_price(1, 100, 998)).unwrap();
    assert!(book.order(OrderReference::from_u64(1)).is_none());
    assert_eq!(book.queue_position(OrderReference::from_u64(2)), Some(0));
}

// ---------------------------------------------------------------------------
// Mid-queue delete — front intact, back advances by 1
// ---------------------------------------------------------------------------

#[test]
fn delete_mid_queue_keeps_front_and_shifts_back_one() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 100, 1_000)).unwrap();
    book.apply(&add(2, Side::Buy, 200, 1_000)).unwrap();
    book.apply(&add(3, Side::Buy, 300, 1_000)).unwrap();

    book.apply(&delete(2)).expect("delete middle");

    assert!(book.order(OrderReference::from_u64(2)).is_none());
    assert_eq!(book.queue_position(OrderReference::from_u64(1)), Some(0));
    assert_eq!(book.queue_position(OrderReference::from_u64(3)), Some(1));
    assert_eq!(book.order_count(), 2);
}

// ---------------------------------------------------------------------------
// Partial cancel — shares reduced, queue position unchanged
// ---------------------------------------------------------------------------

#[test]
fn cancel_partial_reduces_shares_keeps_queue_position() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 100, 1_000)).unwrap();
    book.apply(&add(2, Side::Buy, 500, 1_000)).unwrap();
    book.apply(&add(3, Side::Buy, 300, 1_000)).unwrap();

    book.apply(&cancel(2, 200)).expect("partial cancel");

    let entry = book
        .order(OrderReference::from_u64(2))
        .expect("still there");
    assert_eq!(entry.shares, 300);
    // Queue position of 2 unchanged.
    assert_eq!(book.queue_position(OrderReference::from_u64(2)), Some(1));
    assert_eq!(book.queue_position(OrderReference::from_u64(3)), Some(2));
}

#[test]
fn cancel_full_removes_from_anywhere_in_queue() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 100, 1_000)).unwrap();
    book.apply(&add(2, Side::Buy, 200, 1_000)).unwrap();
    book.apply(&add(3, Side::Buy, 300, 1_000)).unwrap();

    // Cancel the *middle* order in full — must remove it without
    // touching the front.
    book.apply(&cancel(2, 200)).expect("full cancel");
    assert!(book.order(OrderReference::from_u64(2)).is_none());
    assert_eq!(book.queue_position(OrderReference::from_u64(1)), Some(0));
    assert_eq!(book.queue_position(OrderReference::from_u64(3)), Some(1));
}

// ---------------------------------------------------------------------------
// Replace — priority resets, new order at the BACK of new level's queue
// ---------------------------------------------------------------------------

#[test]
fn replace_resets_priority_to_back_of_new_level_queue() {
    let mut book = fresh();
    // Level X = 1_000 with 3 orders.
    book.apply(&add(1, Side::Buy, 100, 1_000)).unwrap();
    book.apply(&add(2, Side::Buy, 200, 1_000)).unwrap();
    book.apply(&add(3, Side::Buy, 300, 1_000)).unwrap();
    // Level Y = 1_010 with 1 order.
    book.apply(&add(4, Side::Buy, 400, 1_010)).unwrap();

    // Replace the FRONT of level X (ref=1) into level Y. The new
    // ref=5 must land at the back of Y's queue, behind ref=4.
    book.apply(&replace(1, 5, 150, 1_010)).expect("replace");

    assert!(book.order(OrderReference::from_u64(1)).is_none());
    let new_entry = book.order(OrderReference::from_u64(5)).expect("ref=5 in");
    assert_eq!(new_entry.price, Price4::from_u32(1_010));
    assert_eq!(new_entry.shares, 150);
    assert_eq!(new_entry.side, Side::Buy);

    // ref=4 was alone at level Y; ref=5 lands at position 1.
    assert_eq!(book.queue_position(OrderReference::from_u64(4)), Some(0));
    assert_eq!(book.queue_position(OrderReference::from_u64(5)), Some(1));

    // Level X queue: ref=2 advances to front, ref=3 to position 1.
    assert_eq!(book.queue_position(OrderReference::from_u64(2)), Some(0));
    assert_eq!(book.queue_position(OrderReference::from_u64(3)), Some(1));
}

#[test]
fn replace_into_empty_level_lands_alone_at_position_zero() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 100, 1_000)).unwrap();
    book.apply(&replace(1, 2, 100, 1_010)).expect("replace");

    assert_eq!(book.order_count(), 1);
    assert_eq!(book.queue_position(OrderReference::from_u64(2)), Some(0));
    let e = book.order(OrderReference::from_u64(2)).unwrap();
    assert_eq!(e.price, Price4::from_u32(1_010));
}

#[test]
fn replace_preserves_mpid_when_original_carried_one() {
    let mut book = fresh();
    let mpid = Mpid::new("BLAH");
    book.apply(&add_with_mpid(1, Side::Buy, 100, 1_000, mpid))
        .unwrap();
    book.apply(&replace(1, 2, 50, 1_005)).expect("replace");

    let e = book.order(OrderReference::from_u64(2)).expect("present");
    assert_eq!(e.mpid, Some(mpid));
}

// ---------------------------------------------------------------------------
// Add F (with MPID) sets the entry's mpid
// ---------------------------------------------------------------------------

#[test]
fn add_with_mpid_records_attribution() {
    let mut book = fresh();
    let mpid = Mpid::new("ARCA");
    book.apply(&add_with_mpid(1, Side::Buy, 100, 1_000, mpid))
        .unwrap();
    let e = book.order(OrderReference::from_u64(1)).expect("present");
    assert_eq!(e.mpid, Some(mpid));
}

#[test]
fn add_a_no_mpid_yields_none_for_attribution() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 100, 1_000)).unwrap();
    let e = book.order(OrderReference::from_u64(1)).expect("present");
    assert_eq!(e.mpid, None);
}

// ---------------------------------------------------------------------------
// Cross-symbol filtering and unknown-order silent drops
// ---------------------------------------------------------------------------

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
}

#[test]
fn execute_for_unknown_order_is_silently_dropped() {
    let mut book = fresh();
    book.apply(&execute(99, 100)).expect("dropped, not error");
    assert_eq!(book.order_count(), 0);
}

#[test]
fn cancel_for_unknown_order_is_silently_dropped() {
    let mut book = fresh();
    book.apply(&cancel(99, 100)).expect("dropped");
    assert_eq!(book.order_count(), 0);
}

#[test]
fn delete_for_unknown_order_is_silently_dropped() {
    let mut book = fresh();
    book.apply(&delete(99)).expect("dropped");
    assert_eq!(book.order_count(), 0);
}

#[test]
fn replace_for_unknown_original_is_silently_dropped() {
    let mut book = fresh();
    book.apply(&replace(99, 100, 50, 1_000)).expect("dropped");
    assert_eq!(book.order_count(), 0);
}

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[test]
fn over_execute_returns_over_execution_error() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).unwrap();
    let err = book
        .apply(&execute(1, 600))
        .expect_err("over-execute must error");
    assert!(matches!(
        err,
        BookError::OverExecution {
            executed: 600,
            remaining: 500,
            ..
        }
    ));
}

#[test]
fn over_cancel_returns_over_execution_error() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).unwrap();
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
fn fifo_violation_surfaces_when_front_of_queue_does_not_match() {
    // The FIFO invariant cannot be violated through clean A/F/E/C
    // sequences, so we hand-fabricate a malformed scenario: insert
    // two orders at the same price, then surgically swap the queue
    // front before issuing a full execute on the original front.
    //
    // We do this without `unsafe` and without touching private
    // fields — instead, we stage the violation by deleting the
    // front and re-adding it under a new ref behind the second
    // order, then issue a full execute on the FIRST (already
    // removed) ref. But that just no-ops because the order isn't in
    // the index.
    //
    // The cleanest way to surface the variant is to have two
    // distinct order_refs at the same price, then synthesise an
    // OrderExecuted for the *second* (back) order with a full-share
    // exec. The L3 book pops the front, sees it's not ref=2, and
    // returns FifoViolation. This is exactly the malformed-capture
    // case the variant exists for.
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 100, 1_000)).unwrap();
    book.apply(&add(2, Side::Buy, 200, 1_000)).unwrap();

    // Full execute on ref=2 (the back). The FIFO invariant says
    // executions hit the front; this represents a corrupted feed.
    let err = book
        .apply(&execute(2, 200))
        .expect_err("FIFO violation expected");
    match err {
        BookError::FifoViolation { expected, got } => {
            assert_eq!(expected, OrderReference::from_u64(2));
            assert_eq!(got, OrderReference::from_u64(1));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// summary_l2 cross-check — single sanity case
// ---------------------------------------------------------------------------

#[test]
fn summary_l2_single_order_matches_an_l2_book_with_the_same_input() {
    let mut book = fresh();
    book.apply(&add(1, Side::Buy, 500, 1_000)).unwrap();

    let mut l2 = L2Book::new(StockLocate::from_u16(SYMBOL_LOCATE));
    l2.apply(&add(1, Side::Buy, 500, 1_000)).unwrap();

    let summary = book.summary_l2();
    assert_eq!(summary.best_bid(), l2.best_bid());
    assert_eq!(summary.best_ask(), l2.best_ask());
    assert_eq!(summary.order_count(), l2.order_count());
}

// ---------------------------------------------------------------------------
// Cross-check vs L2 over a 1000-message synthetic stream
// ---------------------------------------------------------------------------

/// Tiny splitmix64 PRNG. Deterministic, no external dependency.
#[derive(Clone, Copy)]
struct SplitMix64 {
    state: u64,
}

impl SplitMix64 {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn next_u32(&mut self) -> u32 {
        self.next_u64() as u32
    }

    fn pick(&mut self, n: u32) -> u32 {
        self.next_u32() % n
    }
}

#[test]
fn cross_check_l2_vs_l3_summary_over_1000_messages() {
    let started = std::time::Instant::now();
    let mut rng = SplitMix64::new(0xDEAD_BEEF_CAFE_BABE);
    let mut l2 = L2Book::new(StockLocate::from_u16(SYMBOL_LOCATE));
    let mut l3 = fresh();

    // Track live order refs so we can pick a valid one for E / C / X
    // / D / U. Both books see the identical message stream, so the
    // sets stay in sync.
    let mut live: Vec<u64> = Vec::with_capacity(1024);
    let mut next_ref: u64 = 1;
    let mpid = Mpid::new("ARCA");

    for _ in 0..1_000 {
        // Pick an op kind. Bias toward A/F when no live orders so we
        // can populate.
        let op = if live.is_empty() {
            rng.pick(2) // 0=A, 1=F
        } else {
            rng.pick(7) // 0=A, 1=F, 2=E, 3=C, 4=X, 5=D, 6=U
        };

        let msg = match op {
            0 => {
                let r = next_ref;
                next_ref += 1;
                let side = if rng.pick(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let shares = (rng.pick(9) + 1) * 100; // 100..=900
                let price = 1_000 + rng.pick(20); // 1000..=1019
                live.push(r);
                add(r, side, shares, price)
            }
            1 => {
                let r = next_ref;
                next_ref += 1;
                let side = if rng.pick(2) == 0 {
                    Side::Buy
                } else {
                    Side::Sell
                };
                let shares = (rng.pick(9) + 1) * 100;
                let price = 1_000 + rng.pick(20);
                live.push(r);
                add_with_mpid(r, side, shares, price, mpid)
            }
            2 | 3 => {
                // Execute or ExecuteWithPrice. To stay FIFO-clean
                // (so the L3 book never raises FifoViolation on a
                // full execute), pick a random live order, find the
                // front-of-queue order at its level, and execute
                // that. Partial executes are allowed anywhere, but
                // we conservatively use the front for both partial
                // and full.
                let pick_idx = rng.pick(live.len() as u32) as usize;
                let chosen_ref = OrderReference::from_u64(live[pick_idx]);
                let (chosen_side, chosen_price) = match l3.order(chosen_ref) {
                    Some(e) => (e.side, e.price),
                    None => {
                        live.swap_remove(pick_idx);
                        continue;
                    }
                };
                // Find the front ref at (chosen_side, chosen_price)
                // by scanning `live`.
                let front_target = live.iter().copied().find(|r| {
                    let oref = OrderReference::from_u64(*r);
                    matches!(l3.order(oref), Some(e) if e.side == chosen_side
                        && e.price == chosen_price)
                        && l3.queue_position(oref) == Some(0)
                });
                let target = match front_target {
                    Some(t) => t,
                    None => continue,
                };
                let target_entry = match l3.order(OrderReference::from_u64(target)) {
                    Some(e) => *e,
                    None => continue,
                };
                let exec = if rng.pick(2) == 0 {
                    target_entry.shares // full
                } else {
                    1.max(target_entry.shares / 2) // partial
                };
                if exec == target_entry.shares {
                    if let Some(pos) = live.iter().position(|r| *r == target) {
                        live.swap_remove(pos);
                    }
                }
                if op == 2 {
                    execute(target, exec)
                } else {
                    execute_with_price(target, exec, target_entry.price.as_u32() + 1)
                }
            }
            4 => {
                // Cancel partial or full — works at any queue
                // position.
                let pick_idx = rng.pick(live.len() as u32) as usize;
                let chosen = live[pick_idx];
                let entry = match l3.order(OrderReference::from_u64(chosen)) {
                    Some(e) => e,
                    None => {
                        live.swap_remove(pick_idx);
                        continue;
                    }
                };
                let cancelled = if rng.pick(2) == 0 {
                    entry.shares // full cancel
                } else {
                    1.max(entry.shares / 3)
                };
                if cancelled == entry.shares {
                    live.swap_remove(pick_idx);
                }
                cancel(chosen, cancelled)
            }
            5 => {
                // Delete — works at any queue position.
                let pick_idx = rng.pick(live.len() as u32) as usize;
                let chosen = live.swap_remove(pick_idx);
                delete(chosen)
            }
            6 => {
                // Replace — works at any queue position.
                let pick_idx = rng.pick(live.len() as u32) as usize;
                let original = live.swap_remove(pick_idx);
                let new_ref = next_ref;
                next_ref += 1;
                let new_shares = (rng.pick(9) + 1) * 100;
                let new_price = 1_000 + rng.pick(20);
                live.push(new_ref);
                replace(original, new_ref, new_shares, new_price)
            }
            _ => unreachable!(),
        };

        // Apply to both books and compare top-of-book each step.
        let r2 = l2.apply(&msg);
        let r3 = l3.apply(&msg);
        assert_eq!(
            r2.is_ok(),
            r3.is_ok(),
            "L2 and L3 disagreed on apply outcome for {msg:?}: L2={r2:?} L3={r3:?}"
        );

        let summary = l3.summary_l2();
        assert_eq!(
            l2.best_bid(),
            summary.best_bid(),
            "best_bid drift after {msg:?}"
        );
        assert_eq!(
            l2.best_ask(),
            summary.best_ask(),
            "best_ask drift after {msg:?}"
        );
    }

    let elapsed = started.elapsed();
    eprintln!(
        "cross-check 1000 messages: {} ms ({} us/msg)",
        elapsed.as_millis(),
        elapsed.as_micros() / 1_000
    );
}
