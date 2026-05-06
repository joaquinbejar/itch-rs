# itch-book

Order-book reconstruction (L2 price levels) from a stream of NASDAQ
TotalView-ITCH 5.0 messages.

`itch-book` consumes decoded `itch_protocol::Message` values and
maintains a per-symbol level-2 book: total shares aggregated per price
level on each side, backed by an order-reference index that resolves
per-order mutations (`E` / `C` / `X` / `D` / `U`) without scanning the
levels.

## Scope

- **L2 price-level book** for a single symbol — `L2Book`.
- **L3 per-order book** with FIFO queue priority — `L3Book`.
- Sync apply path; no transport dependency, no allocator on the
  steady-state hot path beyond a bounded `BTreeMap` / `HashMap` insert
  per new order.
- Exhaustive match over every `Message` variant — new ITCH revisions
  surface as compile errors.

## Out of scope

- **Multi-symbol book manager** indexed by `StockLocate` — issue #38.
- Republication as a `MessageSource` — see `docs/ROADMAP.md` v0.5.

## Usage

```rust
use itch_book::L2Book;
use itch_protocol::{
    AddOrder, Header, Message, OrderReference, Price4, Shares, Side, Stock,
    StockLocate, Timestamp, TrackingNumber,
};

let stock_locate = StockLocate::from_u16(42);
let mut book = L2Book::new(stock_locate);

let add = Message::AddOrder(AddOrder {
    header: Header {
        stock_locate,
        tracking_number: TrackingNumber::from_u16(0),
        timestamp: Timestamp::from_u64(32_400_000_000_000),
    },
    order_ref: OrderReference::from_u64(1),
    side: Side::Buy,
    shares: Shares::from_u32(500),
    stock: Stock::new("AAPL"),
    price: Price4::from_u32(1_925_000),
});

book.apply(&add)?;
assert_eq!(book.best_bid(), Some((Price4::from_u32(1_925_000), 500)));
# Ok::<_, itch_book::BookError>(())
```

## Apply rules

| Variant                                       | L2 effect |
|-----------------------------------------------|-----------|
| `A` Add Order, `F` Add Order with MPID        | filter by `stock_locate`; insert into the order index, add shares to the price level |
| `E` Order Executed                            | subtract `executed_shares` from the level; remove order if exhausted |
| `C` Order Executed With Price                 | same as `E` — execution price is the trade print, not a resting level price |
| `X` Order Cancel                              | subtract `cancelled_shares` from the level and the order's remaining |
| `D` Order Delete                              | remove order; subtract its remaining shares from the level |
| `U` Order Replace                             | remove the original; insert a new entry under `new_order_ref` at the new price/shares (preserving side) |
| `S, R, H, Y, L, V, W, K, P, Q, B, I, N`       | no L2 effect |

## L3 (per-order) book

`L3Book` adds FIFO queue priority on top of the L2 abstraction. Each
price level is a `VecDeque<OrderReference>` in arrival order; full
executions pop the front of the queue, partial cancels keep the
order in place, deletes / full-cancels can remove from any position,
and replaces reset priority by appending the new order to the back
of its destination level's queue.

The L3 book exposes `order(...)`, `queue_position(...)`,
`level_orders(...)`, and a `summary_l2()` helper that re-derives an
`L2Book` from the current L3 state for cross-validation.
`BookError::FifoViolation` surfaces when a full execution does not
match the front-of-queue reference (malformed capture).

## Multi-symbol streams

An `L2Book` is pinned to one `StockLocate`. Messages for other symbols
are silently no-ops, so the same book can be driven by a global
multi-symbol stream without filtering upstream. The forthcoming
multi-symbol manager (issue #38) will index a
`HashMap<StockLocate, L2Book>` and dispatch on the message header.

## License

Licensed under either of MIT or Apache-2.0 at your option.
