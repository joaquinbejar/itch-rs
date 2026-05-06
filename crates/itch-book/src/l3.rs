//! Single-symbol L3 (per-order) order-book reconstruction with FIFO
//! queue priority.
//!
//! An [`L3Book`] tracks every resting order individually, preserving
//! arrival order at each price level so callers can ask "what is my
//! queue position for order X?" The implementation maintains:
//!
//! - `bids` / `asks`: per-side `BTreeMap<Price4,
//!   VecDeque<OrderReference>>` mapping each price level to a FIFO
//!   queue of order references in arrival order (front = most senior).
//! - `orders`: `HashMap<OrderReference, L3OrderEntry>` resolving a
//!   reference to its full record (price, shares, side, MPID,
//!   arrival sequence) without a queue scan.
//! - `arrival_seq`: a monotonic counter bumped on every successful
//!   `A` / `F` / `U` insert so consumers can deterministically order
//!   ties when a queue is rebuilt.
//!
//! The book is pinned to a single [`StockLocate`] at construction;
//! messages for other symbols are silently no-ops, mirroring the L2
//! contract so the same book can be driven by a global multi-symbol
//! stream without filtering upstream.
//!
//! ## Apply rules
//!
//! The 20 [`Message`] variants are matched exhaustively. The
//! per-variant L3 effect is documented inline on
//! [`L3Book::apply`]; new ITCH revisions surface as compile errors.
//!
//! ## Cross-check vs L2
//!
//! [`L3Book::summary_l2`] re-derives an [`L2Book`] from the current
//! L3 state by direct level/share aggregation: every order in
//! `self.orders` contributes its remaining shares to its `(side,
//! price)` cell. The method is allocation-friendly (it builds a
//! fresh [`L2Book`]) and is intended for inspection, validation, and
//! tests — not for the steady-state hot path.

use std::collections::{BTreeMap, HashMap, VecDeque};

use itch_protocol::{
    AddOrder, Header, Message, Mpid, OrderReference, Price4, Shares, Side, Stock, StockLocate,
    Timestamp, TrackingNumber,
};

use crate::book::L2Book;
use crate::error::BookError;

/// Per-order bookkeeping entry stored under each [`OrderReference`]
/// in the L3 index.
///
/// `arrival_seq` is the value of [`L3Book`]'s monotonic counter at
/// the moment this entry was inserted — useful for deterministic tie
/// breaking and external tooling that needs to reconstruct queue
/// order from a snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L3OrderEntry {
    /// Resting limit price of the order.
    pub price: Price4,
    /// Remaining open shares on the order.
    pub shares: u32,
    /// Buy or sell side.
    pub side: Side,
    /// Symbol identifier the order belongs to.
    pub stock_locate: StockLocate,
    /// Broker attribution if the order arrived via `F` Add Order
    /// With MPID; `None` otherwise (`A` Add Order or replace of a
    /// non-attributed order).
    pub mpid: Option<Mpid>,
    /// Monotonic arrival sequence assigned by the owning [`L3Book`]
    /// at insert time.
    pub arrival_seq: u64,
}

/// Single-symbol L3 (per-order) book with FIFO queue priority.
///
/// See the module-level documentation for the apply contract and the
/// internal data layout.
#[derive(Debug, Clone)]
pub struct L3Book {
    stock_locate: StockLocate,
    bids: BTreeMap<Price4, VecDeque<OrderReference>>,
    asks: BTreeMap<Price4, VecDeque<OrderReference>>,
    orders: HashMap<OrderReference, L3OrderEntry>,
    arrival_seq: u64,
}

impl L3Book {
    /// Construct an empty L3 book pinned to `stock_locate`.
    #[inline]
    #[must_use]
    pub fn new(stock_locate: StockLocate) -> Self {
        Self {
            stock_locate,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            orders: HashMap::new(),
            arrival_seq: 0,
        }
    }

    /// Symbol identifier this book is pinned to.
    #[inline]
    #[must_use]
    pub fn stock_locate(&self) -> StockLocate {
        self.stock_locate
    }

    /// Number of open orders currently in the index.
    #[inline]
    #[must_use]
    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    /// Number of distinct price levels on `side`.
    #[inline]
    #[must_use]
    pub fn levels_count(&self, side: Side) -> usize {
        match side {
            Side::Buy => self.bids.len(),
            Side::Sell => self.asks.len(),
        }
    }

    /// Look up an order by reference.
    ///
    /// Returns `None` if the reference is not in the index — either
    /// because no `A` / `F` ever inserted it (e.g., it belongs to a
    /// different symbol) or because it has since been fully
    /// executed, deleted, or replaced.
    #[inline]
    #[must_use]
    pub fn order(&self, order_ref: OrderReference) -> Option<&L3OrderEntry> {
        self.orders.get(&order_ref)
    }

    /// Position of `order_ref` in its price-level FIFO queue.
    ///
    /// `Some(0)` means front of the queue (most senior, first to be
    /// matched on the next aggressing trade). Returns `None` when
    /// the order is not in the index.
    #[must_use]
    pub fn queue_position(&self, order_ref: OrderReference) -> Option<usize> {
        let entry = self.orders.get(&order_ref)?;
        let queue = match entry.side {
            Side::Buy => self.bids.get(&entry.price)?,
            Side::Sell => self.asks.get(&entry.price)?,
        };
        queue.iter().position(|r| *r == order_ref)
    }

    /// Iterate orders at `(side, price)` from front to back.
    ///
    /// Yields nothing when the level is empty. The iterator looks up
    /// each [`OrderReference`] in `self.orders`; entries that fail
    /// the lookup (which would only happen on a corrupted book) are
    /// silently skipped.
    pub fn level_orders(
        &self,
        side: Side,
        price: Price4,
    ) -> Box<dyn Iterator<Item = &L3OrderEntry> + '_> {
        let queue = match side {
            Side::Buy => self.bids.get(&price),
            Side::Sell => self.asks.get(&price),
        };
        match queue {
            Some(q) => Box::new(q.iter().filter_map(move |r| self.orders.get(r))),
            None => Box::new(std::iter::empty()),
        }
    }

    /// Re-derive an [`L2Book`] from the current L3 state.
    ///
    /// Walks `self.orders` and synthesises an `A` Add Order per
    /// entry, applying each through the public [`L2Book::apply`]
    /// path. This keeps the cross-check authoritative — both books
    /// see equivalent inputs.
    ///
    /// # Performance
    ///
    /// Allocation-friendly. Intended for inspection / validation /
    /// tests, not the steady-state hot path.
    #[must_use]
    pub fn summary_l2(&self) -> L2Book {
        let mut l2 = L2Book::new(self.stock_locate);
        // A blank header carrying the right stock_locate is enough —
        // L2 only inspects header.stock_locate, order_ref, side,
        // shares, and price on `AddOrder`.
        let header = Header {
            stock_locate: self.stock_locate,
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(0),
        };
        // Stock value is irrelevant for L2's apply path; pick a
        // padded blank.
        let stock = Stock::default();
        for (order_ref, entry) in &self.orders {
            // Skip orders that somehow drifted onto a different
            // stock_locate (shouldn't happen — every insert pins to
            // self.stock_locate — but be defensive).
            if entry.stock_locate != self.stock_locate {
                continue;
            }
            let msg = Message::AddOrder(AddOrder {
                header,
                order_ref: *order_ref,
                side: entry.side,
                shares: Shares::from_u32(entry.shares),
                stock,
                price: entry.price,
            });
            // Ignore the result: synthesised AddOrders cannot fail
            // (no over-execution / over-cancel paths involved).
            let _ = l2.apply(&msg);
        }
        l2
    }

    /// Apply one ITCH 5.0 message to the L3 book.
    ///
    /// The 20 [`Message`] variants are matched exhaustively. Variants
    /// with no L3 effect (`S`, `R`, `H`, `Y`, `L`, `V`, `W`, `K`,
    /// `P`, `Q`, `B`, `I`, `N`) return `Ok(())` immediately.
    ///
    /// Per-variant L3 effect:
    ///
    /// | Variant | Effect |
    /// |---------|--------|
    /// | `A` | bump `arrival_seq`; insert entry with `mpid: None`; push back on its level queue |
    /// | `F` | same as `A` but `mpid: Some(attribution)` |
    /// | `E` | subtract `executed_shares`; if remaining hits zero, pop the *front* of the level queue and verify it matches |
    /// | `C` | identical to `E` for the book — execution price is a tape print, not a resting level |
    /// | `X` | subtract `cancelled_shares`; if exhausted, remove the order from anywhere in the queue |
    /// | `D` | remove the order from anywhere in the queue (may be mid-queue) |
    /// | `U` | remove the original from its (possibly mid-queue) position; insert the replacement under `new_order_ref` at the back of the new level's queue (priority is reset per spec) |
    /// | `S, R, H, Y, L, V, W, K, P, Q, B, I, N` | no L3 effect |
    ///
    /// Messages for a different `stock_locate` (or referencing an
    /// order not in the index) are silently dropped.
    ///
    /// # Errors
    ///
    /// - [`BookError::OverExecution`] if `E` / `C` / `X` would remove
    ///   more shares than the resting order has.
    /// - [`BookError::FifoViolation`] if a full execution finds an
    ///   unexpected reference at the front of the price-level queue.
    pub fn apply(&mut self, msg: &Message) -> Result<(), BookError> {
        match msg {
            // ----- L3-mutating variants -----
            Message::AddOrder(m) => {
                if m.header.stock_locate != self.stock_locate {
                    return Ok(());
                }
                self.insert_order(m.order_ref, m.side, m.shares.as_u32(), m.price, None);
                Ok(())
            }
            Message::AddOrderWithMpid(m) => {
                if m.header.stock_locate != self.stock_locate {
                    return Ok(());
                }
                self.insert_order(
                    m.order_ref,
                    m.side,
                    m.shares.as_u32(),
                    m.price,
                    Some(m.attribution),
                );
                Ok(())
            }
            Message::OrderExecuted(m) => self.execute(m.order_ref, m.executed_shares.as_u32()),
            Message::OrderExecutedWithPrice(m) => {
                // Execution price on `C` is the trade print, not a
                // resting level price — only the share count moves
                // the book. The `printable` flag is irrelevant here.
                self.execute(m.order_ref, m.executed_shares.as_u32())
            }
            Message::OrderCancel(m) => self.cancel(m.order_ref, m.cancelled_shares.as_u32()),
            Message::OrderDelete(m) => {
                self.delete(m.order_ref);
                Ok(())
            }
            Message::OrderReplace(m) => self.replace(
                m.original_order_ref,
                m.new_order_ref,
                m.shares.as_u32(),
                m.price,
            ),

            // ----- session / informational variants — no L3 effect -----
            Message::SystemEvent(_) => Ok(()), // S — no L3 effect
            Message::StockDirectory(_) => Ok(()), // R — no L3 effect
            Message::StockTradingAction(_) => Ok(()), // H — no L3 effect
            Message::RegShoRestriction(_) => Ok(()), // Y — no L3 effect
            Message::MarketParticipantPosition(_) => Ok(()), // L — no L3 effect
            Message::MwcbDeclineLevel(_) => Ok(()), // V — no L3 effect
            Message::MwcbStatus(_) => Ok(()),  // W — no L3 effect
            Message::IpoQuotingPeriodUpdate(_) => Ok(()), // K — no L3 effect
            Message::TradeNonCross(_) => Ok(()), // P — no L3 effect (trade print only)
            Message::CrossTrade(_) => Ok(()),  // Q — no L3 effect (cross print only)
            Message::BrokenTrade(_) => Ok(()), // B — no L3 effect
            Message::Noii(_) => Ok(()),        // I — no L3 effect
            Message::RetailPriceImprovement(_) => Ok(()), // N — no L3 effect
        }
    }

    // -----------------------------------------------------------------
    // Internal mutators
    // -----------------------------------------------------------------

    /// Insert a new order into the index and append it to the back
    /// of its price-level queue. Bumps `arrival_seq`.
    fn insert_order(
        &mut self,
        order_ref: OrderReference,
        side: Side,
        shares: u32,
        price: Price4,
        mpid: Option<Mpid>,
    ) {
        // Bump first so the entry's arrival_seq is non-zero on the
        // first ever insert (callers can use 0 as a sentinel). u64
        // is wide enough to cover any plausible session — a wrap
        // here would mean ~10^19 inserts, far beyond a trading day.
        self.arrival_seq += 1;
        let arrival_seq = self.arrival_seq;
        self.orders.insert(
            order_ref,
            L3OrderEntry {
                price,
                shares,
                side,
                stock_locate: self.stock_locate,
                mpid,
                arrival_seq,
            },
        );
        let queue = match side {
            Side::Buy => self.bids.entry(price).or_default(),
            Side::Sell => self.asks.entry(price).or_default(),
        };
        queue.push_back(order_ref);
    }

    /// Apply an `E` / `C` execution to the book.
    ///
    /// Subtracts `shares` from the order's remaining; if the
    /// remaining hits zero, pops the front of the level queue and
    /// asserts it matches `order_ref`, raising
    /// [`BookError::FifoViolation`] otherwise.
    fn execute(&mut self, order_ref: OrderReference, shares: u32) -> Result<(), BookError> {
        let entry = match self.orders.get_mut(&order_ref) {
            Some(e) => e,
            None => return Ok(()), // belongs to a different symbol
        };
        if shares > entry.shares {
            return Err(BookError::OverExecution {
                order: order_ref,
                executed: shares,
                remaining: entry.shares,
            });
        }
        entry.shares -= shares;
        let exhausted = entry.shares == 0;
        let price = entry.price;
        let side = entry.side;

        if exhausted {
            // FIFO invariant: the executed order is the oldest (front
            // of queue) at its level.
            let queue = match side {
                Side::Buy => self.bids.get_mut(&price),
                Side::Sell => self.asks.get_mut(&price),
            };
            let queue = match queue {
                Some(q) => q,
                // Defensive: an order without its level queue means
                // the book got out of sync earlier — nothing safe to
                // do here, fall back to dropping the order entry.
                None => {
                    self.orders.remove(&order_ref);
                    return Ok(());
                }
            };
            match queue.pop_front() {
                Some(front) if front == order_ref => {}
                Some(front) => {
                    // Put it back so the queue stays consistent if
                    // the caller continues; surface the violation.
                    queue.push_front(front);
                    return Err(BookError::FifoViolation {
                        expected: order_ref,
                        got: front,
                    });
                }
                None => {
                    // Empty queue but an order claimed this level —
                    // drop the entry, return Ok.
                    self.orders.remove(&order_ref);
                    return Ok(());
                }
            }
            if queue.is_empty() {
                match side {
                    Side::Buy => {
                        self.bids.remove(&price);
                    }
                    Side::Sell => {
                        self.asks.remove(&price);
                    }
                }
            }
            self.orders.remove(&order_ref);
        }
        Ok(())
    }

    /// Apply an `X` Order Cancel.
    ///
    /// Partial cancels keep the order at its current FIFO position;
    /// only a full cancel removes it from the queue. Cancel-removal
    /// may happen mid-queue, unlike execution, which is always
    /// front-of-queue.
    fn cancel(&mut self, order_ref: OrderReference, shares: u32) -> Result<(), BookError> {
        let entry = match self.orders.get_mut(&order_ref) {
            Some(e) => e,
            None => return Ok(()),
        };
        if shares > entry.shares {
            return Err(BookError::OverExecution {
                order: order_ref,
                executed: shares,
                remaining: entry.shares,
            });
        }
        entry.shares -= shares;
        if entry.shares == 0 {
            let price = entry.price;
            let side = entry.side;
            self.remove_from_queue(side, price, order_ref);
            self.orders.remove(&order_ref);
        }
        Ok(())
    }

    /// Apply a `D` Order Delete: remove the order from anywhere in
    /// its level queue.
    fn delete(&mut self, order_ref: OrderReference) {
        let entry = match self.orders.remove(&order_ref) {
            Some(e) => e,
            None => return,
        };
        self.remove_from_queue(entry.side, entry.price, order_ref);
    }

    /// Apply a `U` Order Replace.
    ///
    /// Removes the original from its (possibly mid-queue) position,
    /// then inserts the replacement under `new_ref` at the back of
    /// the new price level's queue. Per ITCH spec, queue priority is
    /// reset on replace.
    fn replace(
        &mut self,
        original_ref: OrderReference,
        new_ref: OrderReference,
        new_shares: u32,
        new_price: Price4,
    ) -> Result<(), BookError> {
        let original = match self.orders.remove(&original_ref) {
            Some(e) => e,
            None => return Ok(()), // belongs to a different symbol
        };
        self.remove_from_queue(original.side, original.price, original_ref);
        self.insert_order(new_ref, original.side, new_shares, new_price, original.mpid);
        Ok(())
    }

    /// Find-and-remove `order_ref` from `levels[price]`. Drops the
    /// level entry once the queue is empty.
    fn remove_from_queue(&mut self, side: Side, price: Price4, order_ref: OrderReference) {
        let levels = match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        };
        let Some(queue) = levels.get_mut(&price) else {
            return;
        };
        if let Some(idx) = queue.iter().position(|r| *r == order_ref) {
            queue.remove(idx);
        }
        if queue.is_empty() {
            levels.remove(&price);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn locate() -> StockLocate {
        StockLocate::from_u16(42)
    }

    fn header() -> Header {
        Header {
            stock_locate: locate(),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(0),
        }
    }

    fn add_msg(order_ref: u64, side: Side, shares: u32, price: u32) -> Message {
        Message::AddOrder(AddOrder {
            header: header(),
            order_ref: OrderReference::from_u64(order_ref),
            side,
            shares: Shares::from_u32(shares),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(price),
        })
    }

    #[test]
    fn fresh_book_is_empty() {
        let b = L3Book::new(locate());
        assert_eq!(b.order_count(), 0);
        assert_eq!(b.levels_count(Side::Buy), 0);
        assert_eq!(b.levels_count(Side::Sell), 0);
        assert!(b.order(OrderReference::from_u64(1)).is_none());
        assert!(b.queue_position(OrderReference::from_u64(1)).is_none());
    }

    #[test]
    fn arrival_seq_monotonic_across_inserts() {
        let mut b = L3Book::new(locate());
        b.apply(&add_msg(1, Side::Buy, 100, 1_000)).unwrap();
        b.apply(&add_msg(2, Side::Buy, 100, 1_000)).unwrap();
        let s1 = b.order(OrderReference::from_u64(1)).unwrap().arrival_seq;
        let s2 = b.order(OrderReference::from_u64(2)).unwrap().arrival_seq;
        assert!(s2 > s1);
    }
}
