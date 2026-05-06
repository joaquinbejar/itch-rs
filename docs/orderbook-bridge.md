# itch-orderbook Bridge Design

This document details the design of the `itch-orderbook` crate, which bridges `OrderBook-rs` event streams into NASDAQ TotalView-ITCH 5.0 messages. The bridge is a `MessageSource` implementation that can be plugged into `itch-server` or any other ITCH consumer.

## Overview

The bridge translates three classes of `OrderBook-rs` events into corresponding ITCH message types:

1. **Order-state transitions** (place, cancel, execute, replace) → ITCH messages `A`, `F`, `D`, `X`, `U`, `E`, `C`
2. **Trade reports** (executed fills, non-cross prints, broken trades) → ITCH `E`, `P`, `B`, `C`
3. **Book changes** (implicit in order-state events; no standalone ITCH analogue)

## Configuration

The bridge is configured via `BridgeBuilder`:

```rust
pub struct BridgeBuilder {
    pub locate_strategy: LocateStrategy,
    pub directory_provider: Arc<dyn DirectoryProvider>,
    pub timestamp_source: TimestampSource,
    pub enable_crosses: bool,
    pub sequence_reset_hour: u8,  // UTC hour for daily sequence reset (default: 13 = 9am ET)
}

pub enum LocateStrategy {
    /// Use the symbol's default locate from DirectoryProvider
    Default,
    /// Round-robin across a known set of MMs / brokers
    RoundRobin(Vec<Mpid>),
    /// Use a user-supplied function
    Custom(Arc<dyn Fn(&Symbol) -> Mpid>),
}

pub enum TimestampSource {
    /// Use OrderBook-rs event timestamp (TimestampMs) converted to ITCH Timestamp (ns since midnight ET)
    OrderBookEvent,
    /// Use system clock at encoding time
    SystemClock,
}

pub trait DirectoryProvider: Send + Sync {
    /// Resolve symbol metadata: MarketCategory, FinancialStatus, LuldTier, etc.
    fn get_symbol_info(&self, symbol: &Symbol) -> Option<SymbolInfo>;
}

pub struct SymbolInfo {
    pub market_category: MarketCategory,
    pub financial_status: FinancialStatus,
    pub luld_tier: LuldTier,
    pub round_lot_size: u32,
    pub test_instrument: bool,
    // … other fields
}
```

## OrderBook-rs → ITCH Mapping

| OrderBook-rs Event | ITCH Message | Notes |
|---|---|---|
| **New resting order** (placed, not filled) | `A` (Add Order) or `F` (Add Order (MPID))|  Use `F` if a market-maker ID (Mpid) is supplied; `A` otherwise. Resting price and size from order. |
| **Trade fill** (taker × maker) | `E` (Execution) or `C` (Cross Trade) | Use `E` for normal price-level matches. Use `C` if both orders are non-display (dark) orders or if crosses are enabled and both are marked cross-eligible. |
| **Partial cancel** (reduce shares, keep price) | `X` (ExecutedPartial) | New shares = previous shares − cancel amount. STP-reason lost (mapped to generic cancel). |
| **Full cancel** / deletion | `D` (Delete Order) | Remove from book entirely. |
| **Order replace** (modify price, shares, flags) | `U` (Replace Order) | Issues a new `OrderReference` per ITCH semantics; the old reference is implicitly cancelled. |
| **Non-cross trade report** (both sides non-display) | `P` (Trade) | Printed trade outside the book (or aggregate print). |
| **Broken trade** | `B` (Broken Trade) | Requires an explicit "break" operation in OrderBook-rs; not auto-detected. |
| **Book-change without trade** | _none_ | ITCH does not aggregate book changes; events drive `A`, `D`, `X`, `U` separately. |

## Timestamp Conversion

**OrderBook-rs timestamp** is `TimestampMs` (milliseconds since epoch).  
**ITCH timestamp** is `Timestamp` (nanoseconds since midnight ET, per `docs/PROTOCOL-SPEC.md`).

Conversion:

```rust
fn order_book_to_itch_timestamp(
    ob_ms: TimestampMs,
    tz: TimeZone, // = TimeZone::ET
) -> Timestamp {
    // Determine calendar date in ET, find "midnight ET" instant for that day
    let midnight_et = tz.midnight_for_ms(ob_ms);
    let ns_since_midnight = (ob_ms - midnight_et.as_millis()) * 1_000_000;
    Timestamp::try_new(ns_since_midnight).expect("valid ITCH timestamp")
}
```

**Assumption:** All timestamps are in Eastern Time (ET). Deployments in other timezones must override `TimestampSource` or pre-convert event timestamps.

## Sequence Policy

ITCH requires a monotonic message sequence counter, per the ITCH 5.0 spec (field `SequenceNumber` in the header). The counter:

- Starts at 1 each trading day (at 9:30 AM ET by default, configurable via `sequence_reset_hour`).
- Increments by 1 for each message emitted (including `SystemEvent` when the sequence resets, which is not visible to the OrderBook-rs event stream).
- Is owned by the bridge, not by the caller.

For replay or backtest scenarios, the bridge must:
1. Load a snapshot of the book (via `itch-book` L2/L3 reconstruction).
2. Emit a `SystemEvent` (`S`) with `EventCode::StartOfMessages` at sequence 1.
3. Process the OrderBook-rs event stream, incrementing the sequence for each message.
4. Emit `SystemEvent` with `EventCode::EndOfMessages` when the stream is exhausted or a daily boundary is crossed.

## OrderReference Mapping

OrderBook-rs uses **`pricelevel::Id`** (a library-internal order ID); ITCH uses **`OrderReference` (`u64`)**.

The bridge must maintain a bidirectional mapping:

```rust
pub struct OrderRefMap {
    order_book_id_to_itch_ref: HashMap<pricelevel::Id, OrderReference>,
    itch_ref_to_order_book_id: HashMap<OrderReference, pricelevel::Id>,
    next_ref: u64,
}

impl OrderRefMap {
    pub fn allocate(&mut self, ob_id: pricelevel::Id) -> OrderReference {
        let r = OrderReference::try_new(self.next_ref)
            .expect("valid OrderReference");
        self.order_book_id_to_itch_ref.insert(ob_id, r);
        self.itch_ref_to_order_book_id.insert(r, ob_id);
        self.next_ref += 1;
        r
    }
}
```

**Note:** OrderReference wraps at `u64::MAX`. Once the counter exhausts, the bridge must either:
- Halt and require manual intervention (safe but intrusive).
- Reuse stale mappings after a grace period (risky if replay is attempted).

The default is to halt with a loud error. Deployments with 24/7 trading may need custom wrap-around logic.

## DirectoryProvider Trait

Users must supply an implementation that resolves ITCH reference data:

```rust
pub trait DirectoryProvider: Send + Sync {
    fn get_symbol_info(&self, symbol: &Symbol) -> Option<SymbolInfo>;
}

pub struct SymbolInfo {
    pub market_category: MarketCategory,
    pub financial_status: FinancialStatus,
    pub luld_tier: LuldTier,
    pub round_lot_size: u32,
    pub test_instrument: bool,
    pub locate_code: StockLocate,  // for the 'R' Stock Directory message
}
```

Built-in implementations (v0.5+):

- **`JsonDirectoryProvider`**: Reads a static JSON file at startup (good for static symbols; no hot-reload).
- **`InMemoryDirectoryProvider`**: Preloaded HashMap (good for backtests; no external deps).
- **`DatabaseDirectoryProvider`** (future): Queries a database; can auto-refresh on signal.

## Open Questions & Resolutions

### Q1: How are STP cancels handled if ITCH has no STP classification?

**Resolution:** The bridge maps all cancels (including STP) to ITCH `X` (ExecutedPartial) or `D` (Delete). The STP reason is lost. If STP tracking is critical, users must:
1. Run OrderBook-rs in a separate process and consume its events directly (not via the bridge).
2. Or post a follow-up issue to add a custom metadata channel alongside ITCH messages.

### Q2: How do we handle `pricelevel::Id` → `OrderReference` collisions?

**Resolution:** The bridge increments a 64-bit counter for each new order. Collisions are theoretically possible after `2^64` orders, but:
- A typical exchange processes ~1 million orders/day.
- `2^64` orders = ~18 billion years of operation.
- The bridge halts with an error on overflow (safe default).

For 24/7 deployments, override with wrapping semantics and accept the risk.

### Q3: What if OrderBook-rs reports a trade fill without a corresponding resting order?

**Resolution:** This can occur if:
- The snapshot was stale and missed the order placement.
- A cross-system order (from an external venue) filled against the book.

The bridge emits an ITCH `E` (Execution) with as much info as available (price, size, sides) but leaves the taker's `OrderReference` unset or set to 0 (convention: unknown). The recipient must handle sparse references gracefully.

### Q4: How does the bridge handle book snapshots during backtest?

**Resolution:** 
1. Load the snapshot via `itch-book` L2 reconstruction.
2. Extract the book state and pre-populate the `OrderRefMap` with dummy references.
3. Process OrderBook-rs events sequentially.
4. Emit ITCH messages with the pre-populated references.

This ensures replay-ability: the reconstructed book at any point matches the input snapshot.

### Q5: What about `MarketCategory`, `FinancialStatus`, etc. changes mid-day?

**Resolution:** The bridge reads these values once at startup via `DirectoryProvider::get_symbol_info()`. If reference data changes intra-day (e.g., financial status downgrade), the bridge does NOT auto-update — users must restart or implement a hot-reload hook (future enhancement).

For now, assume reference data is stable per trading day.

## Examples

### Example 1: New Resting Order → ITCH `A`

**OrderBook-rs event:**
```
OrderStateEvent::PlaceOrder {
    order: Order {
        id: pricelevel::Id(42),
        symbol: Symbol("AAPL"),
        side: Side::Buy,
        price_level: PriceLevel(15000),  // 150.00
        size: 1000,
        …
    }
}
```

**ITCH message (simplified):**
```
Header {
    message_type: MessageType::A,
    stock_locate: 1,
    tracking_number: 0,
    timestamp: 12345678900,
    sequence_number: 10,
}
AddOrder {
    order_reference: 42,
    buy_sell: Side::Buy,
    shares: 1000,
    stock: Stock("AAPL"),
    price: Price4(150000),  // 150.00 * 10^4
    attribution: Mpid(0),
}
```

### Example 2: Trade Fill → ITCH `E`

**OrderBook-rs event:**
```
TradeResult {
    taker_order_id: pricelevel::Id(99),
    maker_order_id: pricelevel::Id(42),
    executed_price: PriceLevel(15000),
    executed_size: 500,
    timestamp: TimestampMs(…),
}
```

**ITCH messages:**
```
// First, execution against the maker:
Execution {
    order_reference: 42,  // maker
    executed_shares: 500,
    execution_price: Price4(150000),
    execution_id: MatchNumber::unique(),
    buy_sell_indicator: Buy,
    cross_trade_flag: N,
}

// Then, execution report for the taker (if it placed a resting order):
Execution {
    order_reference: 99,  // taker
    executed_shares: 500,
    execution_price: Price4(150000),
    execution_id: MatchNumber::same_as_above(),
    buy_sell_indicator: Sell,
    cross_trade_flag: N,
}
```

## Status

**This document is design-only.** Full implementation (`itch-orderbook` crate) is deferred until OrderBook-rs event shapes are stable (target: v0.4 of `OrderBook-rs` or later). Follow-up issues will detail:

1. `itch-orderbook` v0.1 implementation (codec + MessageSource impl).
2. DirectoryProvider adapters (JSON, in-memory, database).
3. Integration test: OrderBook-rs snapshot → ITCH → itch-book reconstruction round-trip.

## References

- **ADR-0013**: Bridge crate decision and rationale.
- **ADR-0012**: Source-trait crate (the bridge extends this).
- `docs/PROTOCOL-SPEC.md`: ITCH wire-format reference.
- `docs/DOMAIN-MODEL.md`: ITCH type definitions.
- `OrderBook-rs` repository: Event shapes, API surface.
