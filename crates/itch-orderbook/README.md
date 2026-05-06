# itch-orderbook

**Status: design-only stub.** This crate currently contains no
production code. It exists so the design for the future
`OrderBook-rs` to ITCH 5.0 bridge has a tracked home in the
workspace (issue #49). When the implementation lands - in a
post-v0.5 release tracked as a separate issue - the public surface
will follow ADR-0013 below, and the worked examples in Section 2
will become integration tests.

A reader landing on this page should walk away knowing:

- why a dedicated bridge crate is needed (Section 1, ADR-0013);
- what it will look like (Section 2, design doc);
- which follow-up issues will fill it in (Section 3).

The bridge consumes events from
[`OrderBook-rs`](https://github.com/joaquinbejar/OrderBook-rs) - a
matching engine - and yields `itch_protocol::Message` values
through the `MessageSource` trait defined in
[ADR-0012 / `itch-source`](../itch-source). Transports
(`itch-tcp`, `itch-soup`, `itch-mold`) plug onto the resulting
`Stream` without knowing or caring that the upstream is a live
matching engine rather than a synthetic fixture or a capture-file
replay.

---

## Section 1 - ADR-0013: `itch-orderbook` bridge

| Field      | Value                                                      |
|------------|------------------------------------------------------------|
| Status     | Proposed (will be Accepted on merge of issue #49)          |
| Date       | 2026-05-06                                                 |
| Supersedes | -                                                          |
| See also   | ADR-0007 (a la carte crates), ADR-0012 (`itch-source`)     |

### Context

The `itch-rs` workspace and
[`OrderBook-rs`](https://github.com/joaquinbejar/OrderBook-rs)
compose: `OrderBook-rs` is a matching engine that emits a typed
event stream (new orders, fills, cancels, replaces, broken
trades); `itch-rs` is a NASDAQ TotalView-ITCH 5.0 codec plus
transports. The natural composition is "drive `OrderBook-rs` from
the right inputs, ship the resulting events as ITCH on the wire,
then reconstruct an L2 / L3 book downstream with `itch-book`".

ADR-0012 (`itch-source`) anticipated this in its Future Work
section - "`itch-orderbook` (post-v0.5) - bridge between
`OrderBook-rs`'s events and ITCH messages (issue #49 design)" -
but deliberately did not specify the bridge there. The mapping is
non-trivial in several places:

- ITCH `OrderReference` is `u64`; `OrderBook-rs::pricelevel::Id`
  is a richer type. The bridge needs a stable, replayable mapping.
- ITCH distinguishes `A` (Add Order, no MPID) from `F` (Add Order
  with attribution). `OrderBook-rs` has `owner: String`. Translating
  `String` to `Mpid([u8; 4])` requires a user-supplied
  `MpidProvider`; failing that, the bridge degrades to `A`.
- ITCH `R` Stock Directory carries reference data
  (`MarketCategory`, `FinancialStatus`, `RoundLotSize`, `LuldTier`,
  ...) that `OrderBook-rs` does not own. The bridge needs a
  `DirectoryProvider` to fill it in.
- Trades that print at a price different from the resting display
  price (hidden / midpoint orders) must surface as ITCH `C` rather
  than `E`. Trades between two non-displayable orders surface as
  `P`. The bridge needs to detect both cases from the
  `OrderBook-rs` event payload.
- ITCH timestamps are nanoseconds since midnight ET;
  `OrderBook-rs` uses ms since UTC epoch. Conversion is
  session-local and DST-sensitive.
- Cross trades (`Q`), NOII (`I`), MWCB (`V` / `W`), Reg SHO (`Y`),
  trading state (`H`), and market participant position (`L`) are
  ITCH messages that have no `OrderBook-rs` event source. The
  bridge must either declare them out of scope (v0.1) or accept
  them through a side channel.

Carefully designing the bridge before code lands keeps both
upstreams stable: `OrderBook-rs` does not need to grow ITCH
awareness, and `itch-protocol` does not need to grow engine
awareness.

### Decision

Introduce **`itch-orderbook`**, a new a la carte crate (per
ADR-0007) that depends on `itch-source` and (a future stable)
`OrderBook-rs`. The crate exposes a struct that consumes
`OrderBook-rs` events and yields `itch_protocol::Message` values
through `MessageSource`; the crate ships an exhaustive mapping
table from `OrderBook-rs` event types to ITCH 5.0 message kinds,
plus the supporting `DirectoryProvider` and `MpidProvider` traits
needed to fill in fields the engine does not own.

#### Mapping table

Every documented `OrderBook-rs` event type appears below. Rows
that say "OUT OF SCOPE for v0.1" are documented but not
implemented in the first release; the table is exhaustive so a
later release that adds support has nowhere to silently drift.

| OrderBook-rs event              | ITCH       | Notes |
|---------------------------------|------------|-------|
| new resting order placed        | `A` or `F` | `F` if `MpidProvider` resolves the owner to an `Mpid`, else `A`. The `pricelevel::Id` is registry-mapped to a freshly allocated `OrderReference`. |
| trade fill (taker-maker)        | `E` or `C` | `C` if executed price differs from the resting display price (hidden / midpoint print); `E` otherwise. |
| partial cancel (`Cancel`)       | `X`        | Reduce display shares; remaining shares stay; `OrderReference` survives. |
| full cancel / deletion          | `D`        | Remove from book; `OrderReference` retired. |
| replace (`modify` op)           | `U`        | New `OrderReference` per ITCH `U` semantics: old ref retired, new ref allocated. The bridge mints the new ref atomically before emitting. |
| non-cross trade print           | `P`        | Both sides non-displayable (e.g. midpoint, hidden). The print uses `P`, not `E` / `C`. The bridge detects "both legs hidden" from the event payload. |
| broken trade                    | `B`        | Requires an explicit "break" op in `OrderBook-rs`; if the upstream lacks it, this row is OUT OF SCOPE for v0.1 and tracked as future work. |
| book-change without trade       | (none)     | ITCH does not aggregate book state - events drive `A` / `D` / `X` / `U` directly. No bridge output. |
| opening / closing cross         | `Q`        | OUT OF SCOPE for v0.1. Surfaces only when `OrderBook-rs` adds an auction module; documented and skipped. |
| NOII auction signal             | `I`        | OUT OF SCOPE. NASDAQ-specific market-state message; no `OrderBook-rs` source. |
| circuit breaker (MWCB)          | `V` / `W`  | OUT OF SCOPE. NASDAQ-specific; no `OrderBook-rs` source. |
| regulatory state (Reg SHO etc.) | `H` / `Y`  | OUT OF SCOPE for the event-driven path. Supplied via `DirectoryProvider` periodic refresh, NOT triggered by `OrderBook-rs` events. |

Twelve rows; eight rows produce ITCH output in v0.1 (`A`, `F`,
`E`, `C`, `X`, `D`, `U`, `P`); one row is documented as silent
(book-change); three are documented OUT OF SCOPE behind future-work
issues; one row (`B`) is conditionally OUT OF SCOPE pending an
upstream feature.

### Consequences

**Positive**

- A maintained crate exists for "live matching engine to ITCH
  stream". Consumers of `itch-server` reuse the bridge instead of
  re-implementing the mapping per project.
- The mapping table is reviewed once, in this ADR, and then in
  code. Future ITCH revisions or `OrderBook-rs` event-shape
  changes show up as compile errors against this table.
- A round-trip integration test becomes possible: drive
  `OrderBook-rs` with a deterministic order tape, capture the
  bridge's ITCH output, replay through `itch-book`, assert the
  reconstructed L2 / L3 book equals the original. This is tracked
  as a follow-up issue (Section 3).
- The bridge is a `MessageSource` (per ADR-0012). `itch-server`
  treats it as one option among many; users who do not need a
  matching engine pay nothing.

**Negative**

- The bridge crate tracks **two** upstream SemVers
  (`itch-protocol` and `OrderBook-rs`). A breaking change in
  either forces a major bump here. Mitigation: pin both upstreams
  in `Cargo.toml` and document the supported version window in
  the README.
- New public surface to maintain: at minimum `BridgeConfig`,
  `DirectoryProvider`, `DirectoryEntry`, `MpidProvider`,
  `LocateStrategy`, `TimestampSource`, plus the bridge struct
  itself. Each is a SemVer commitment.
- The mapping table embeds engine semantics (which event yields
  `E` vs `C` vs `P`) that are easy to get subtly wrong on edge
  cases (e.g. self-trade-prevention, modify-on-replace). The
  follow-up integration test is the safety net.
- DST handling around session start lives in `BridgeConfig`. Any
  bug in the user's `chrono::DateTime<Utc>` configuration shifts
  every timestamp on the wire. This is documented; the bridge does
  not silently correct it.

### Alternatives considered

#### A - macro-driven mapping

Rejected. A `match` arm per `OrderBook-rs` event type, hand-written
in plain Rust, is greppable and reviewable. A macro hides the wire
shape from review, which is exactly what ITCH discipline (rules:
no wildcard match, exhaustiveness everywhere) is designed to
prevent.

#### B - user-implemented adapter

Rejected. The mapping table has 12 rows and several edge cases
(`E` vs `C` vs `P`, replace semantics, attribution fall-through);
mistakes are likely if every user reimplements it. A maintained
crate centralizes the discipline.

#### C - code generation from a shared schema

Rejected. ITCH 5.0 is a fixed wire format with 20 message kinds,
not a moving target. A schema-driven generator buys nothing and
adds a build-time dependency. ADR-0004 (hand-rolled codec)
already established this stance.

#### D - fold the bridge into `itch-source`

Rejected. `itch-source` is a small trait crate with no engine
dependency. Bundling a `MessageSource` impl that pulls in
`OrderBook-rs` punishes every user of `itch-source` who only
wanted `ChannelSource` or `IteratorSource`. Per ADR-0007 (a la
carte crates) the bridge is its own crate.

#### E - depend on a snapshot of `OrderBook-rs` rather than a
release

Rejected. The bridge MUST be built against a tagged
`OrderBook-rs` release so SemVer makes sense. The implementation
issue is gated on `OrderBook-rs` v1.0 (or whatever release is
declared stable).

---

## Section 2 - design document

This is the implementation-shape preview. None of the types below
exist yet in code; they will land with the implementation issue.
The shape is locked here so reviewers of the implementation PR can
focus on faithfulness to this design rather than re-deriving the
design from scratch.

### Configuration shape

```rust,ignore
use std::sync::Arc;
use chrono::{DateTime, Utc};
use itch_protocol::{
    primitives::{Stock, Mpid, StockLocate},
    enums::{MarketCategory, FinancialStatus, YesNo, LuldTier},
};

pub struct BridgeConfig {
    /// Per-symbol reference data not provided by `OrderBook-rs`.
    /// Called once per newly observed symbol; the result is used
    /// to emit an `R` Stock Directory message and cached for
    /// subsequent header `stock_locate` lookups.
    pub directory: Arc<dyn DirectoryProvider>,

    /// Strategy for converting `OrderBook-rs::pricelevel::Id`
    /// into ITCH `OrderReference`. See "OrderReference derivation"
    /// below. Defaults to `LocateStrategy::Registry`.
    pub locate_strategy: LocateStrategy,

    /// Resolves an `OrderBook-rs::owner: String` to an
    /// `Mpid([u8; 4])`. Returning `None` makes the bridge emit
    /// `A` (no attribution) instead of `F`.
    pub mpid_provider: Arc<dyn MpidProvider>,

    /// Source of the wall-clock used to fill ITCH timestamps.
    /// Production wires a real clock; tests inject a fake.
    pub clock: Arc<dyn TimestampSource>,

    /// Start of the trading session in UTC. ITCH timestamps are
    /// "nanoseconds since midnight ET"; the bridge converts each
    /// event timestamp by subtracting this anchor. DST handling
    /// is the caller's responsibility - configure
    /// `session_start_et` for the correct local midnight in ET.
    pub session_start_et: DateTime<Utc>,
}
```

The traits referenced above:

```rust,ignore
pub trait DirectoryProvider: Send + Sync {
    fn directory_for(&self, symbol: &Stock) -> Option<DirectoryEntry>;
}

pub struct DirectoryEntry {
    pub stock_locate: StockLocate,
    pub market_category: MarketCategory,
    pub financial_status: FinancialStatus,
    pub round_lot_size: u32,
    pub round_lots_only: YesNo,
    pub luld_tier: LuldTier,
    /* ... other fields needed for `R` Stock Directory ... */
}

pub trait MpidProvider: Send + Sync {
    fn mpid_for(&self, owner: &str) -> Option<Mpid>;
}

pub trait TimestampSource: Send + Sync {
    /// Now, as a `chrono::DateTime<Utc>`. The bridge converts to
    /// "nanoseconds since session_start_et" exactly once at the
    /// edge.
    fn now(&self) -> DateTime<Utc>;
}

pub enum LocateStrategy {
    /// Maintain a registry mapping `pricelevel::Id` to a
    /// monotonically allocated `OrderReference`. Default.
    Registry,
    /// Hash `pricelevel::Id` to `OrderReference` statelessly.
    /// Documented for symmetry; not recommended (see below).
    Hash,
}
```

Concrete defaults (an `InMemoryDirectoryProvider`, an
`InMemoryMpidProvider`, a `SystemClock`) ship in the
implementation issue, behind a `defaults` Cargo feature so a user
who wants to provide all three from scratch pays nothing.

### `DirectoryProvider` trait

`OrderBook-rs` does not own the regulatory and reference fields
that ITCH `R` Stock Directory carries:
`market_category`, `financial_status`, `round_lot_size`,
`round_lots_only`, `luld_tier`, `etp_flag`, `etp_leverage_factor`,
`inverse_indicator`, `issue_classification`, `issue_subtype`,
`authenticity`, `short_sale_threshold_indicator`,
`ipo_flag`. The bridge calls `directory_for(symbol)` once per new
symbol observed in the event stream and emits an `R` message
before any subsequent `A` / `F` for that symbol. The result is
cached for the life of the session; the same `StockLocate` is
reused in every following header.

If `directory_for` returns `None`, the bridge emits a
`tracing::warn` and drops events for that symbol. Documented
fallback; the implementation issue tracks an alternative behavior
(emit a synthesized `R` with conservative defaults) if user
feedback warrants it.

### `OrderReference` derivation from `pricelevel::Id`

ITCH `OrderReference` is `u64`. `OrderBook-rs::pricelevel::Id` is
a typed identifier (today, an opaque struct around an internal
counter). The bridge MUST give every order on the wire a unique
`OrderReference` and MUST keep the mapping consistent across
`A` / `F` followed by `E` / `C` / `X` / `U` / `D` for the same
order.

#### Strategy A - registry (recommended; default)

Maintain a `BTreeMap<pricelevel::Id, OrderReference>` inside the
bridge. Allocate a fresh `OrderReference(next)` (counter starts
at 1, monotonic) on every "new resting order" event. Look the
order up on every subsequent event. On `D` (full cancel) or `U`
(replace, retire-old-allocate-new) remove the mapping.

Pros:

- predictable; replays of the same event stream produce the same
  ITCH stream;
- supports `U` cleanly: the new `pricelevel::Id` gets a new
  `OrderReference` allocated atomically;
- aligns with how real ITCH publishers emit `OrderReference` (a
  per-session monotonic counter).

Cons:

- bounded session state (the registry size grows with live orders);
- the registry resets on `BridgeConfig::session_start_et` rollover
  - documented behavior, NOT a bug.

#### Strategy B - hash (alternative; not recommended)

`OrderReference = stable_hash(pricelevel::Id) as u64`. Stateless;
no registry.

Pros:

- no state to manage;
- replay-friendly without a registry snapshot.

Cons:

- collision probability across a long session is non-zero;
- replace semantics break: the new `pricelevel::Id` after `U`
  hashes to a different value, but ITCH consumers expect the new
  `OrderReference` to be a fresh value AND the old one retired -
  hashing makes that work, but the loss of monotonicity makes
  surveillance and reconstruction harder.

#### Recommendation

Strategy A is the default. Strategy B is documented for symmetry
and may be useful for stateless distributed bridges in a future
release; it is not on the v0.1 roadmap.

### Timestamp policy

ITCH `Timestamp` is "nanoseconds since midnight ET" stored as a
u48 (six bytes on the wire, big-endian, high two bytes of the u64
always zero per ADR-0004 / `docs/PROTOCOL-SPEC.md`).

`OrderBook-rs` events carry a `TimestampMs` (ms since UTC epoch)
or equivalent. Conversion in the bridge:

```text
nanos_since_midnight_et =
    (event_timestamp_utc.timestamp_nanos_opt().unwrap_or(0))
    -
    (session_start_et.timestamp_nanos_opt().unwrap_or(0))
```

`BridgeConfig::session_start_et` is the authority for "midnight
ET on this trading day". DST handling lives outside the bridge:
the caller computes the correct `chrono::DateTime<Utc>` for local
midnight in `America/New_York` and passes it in. The bridge does
not import `chrono-tz`.

**Open question: events before session start.** If an event
timestamp is earlier than `session_start_et`, the subtraction
underflows. Recommendation: clamp to `Timestamp(0)` and emit
`tracing::warn!(symbol, event_ts, "event before session start;
clamping to 0")`. Tracked under "Open questions" in the
implementation issue.

**Bound check.** `Timestamp` is u48; the highest representable
value is approximately 78 hours in nanoseconds. A trading session
fits comfortably. Events beyond that bound (e.g. a clock skew
giving a timestamp 80 hours past session start) MUST return a
typed `BridgeError::TimestampOutOfRange`, not silently wrap.

### Sequence policy

The bridge owns a per-day monotonic counter, starting at 1, used
internally to populate any `Header` field that needs sequence
context (none of the 20 ITCH messages carry an explicit sequence
number in their body - sequence lives in the transport, per
ADR-0009 / ADR-0010 - so this counter is internal only).

The counter resets on `BridgeConfig::session_start_et` rollover.
Consumers of the `MessageSource` do not see the counter; it is
not part of the public surface.

This decision deliberately mirrors ADR-0012 / ADR-0009 / ADR-0010:
sequence numbers belong to the **transport** (SoupBinTCP /
MoldUDP64), not to the source. The bridge's internal counter
exists only to seed any future per-bridge analytics.

### Mapping examples (one per row of the mapping table)

Each example shows the input `OrderBook-rs` event, any bridge
state involved, and the output `itch_protocol::Message`. Field
values are illustrative; the implementation issue locks them
against a real `OrderBook-rs` release.

#### Example 1 - new resting order, `A` (no attribution)

```text
OrderBook-rs event: NewLimitOrder {
    id: 0xABC,
    side: Buy,
    price: 1925000,
    qty: 500,
    symbol: "AAPL",
    owner: "USER_42",
    t: 2026-05-06T13:30:00.000Z,
}

Bridge state:
    registry: { 0xABC -> OrderReference(7001) (newly allocated) }
    directory_for("AAPL ") -> DirectoryEntry { stock_locate: 1, ... }
    mpid_for("USER_42") -> None  (no attribution available)

Output:
    Message::AddOrder(AddOrder {
        header: Header {
            stock_locate: StockLocate(1),
            tracking_number: TrackingNumber(0),
            timestamp: Timestamp(34_200_000_000_000), // 09:30:00.000 ET
        },
        order_ref: OrderReference(7001),
        side: Side::Buy,
        shares: Shares(500),
        stock: Stock::new("AAPL"),       // padded to "AAPL    "
        price: Price4(1_925_000),
    })
```

#### Example 2 - new resting order, `F` (with attribution)

```text
OrderBook-rs event: NewLimitOrder {
    id: 0xABD,
    side: Sell,
    price: 1925100,
    qty: 200,
    symbol: "AAPL",
    owner: "NSDQ_DESK",
    t: 2026-05-06T13:30:00.001Z,
}

Bridge state:
    registry: { 0xABD -> OrderReference(7002) (newly allocated) }
    directory_for("AAPL ") -> DirectoryEntry { stock_locate: 1, ... }
    mpid_for("NSDQ_DESK") -> Some(Mpid::new("NSDQ"))

Output:
    Message::AddOrderMpid(AddOrderMpid {
        header: Header {
            stock_locate: StockLocate(1),
            tracking_number: TrackingNumber(0),
            timestamp: Timestamp(34_200_001_000_000), // 09:30:00.001 ET
        },
        order_ref: OrderReference(7002),
        side: Side::Sell,
        shares: Shares(200),
        stock: Stock::new("AAPL"),
        price: Price4(1_925_100),
        mpid: Mpid::new("NSDQ"),
    })
```

#### Example 3 - taker-maker fill at resting price, `E`

```text
OrderBook-rs event: Fill {
    maker_id: 0xABC,
    taker_id: 0xACE,
    shares: 100,
    executed_price: 1925000,
    resting_display_price: 1925000,
    match_id: 4_2_0001,
    t: 2026-05-06T13:30:05.000Z,
}

Bridge state:
    registry: { 0xABC -> OrderReference(7001) (still resting) }

Output:
    Message::OrderExecuted(OrderExecuted {
        header: Header {
            stock_locate: StockLocate(1),
            tracking_number: TrackingNumber(0),
            timestamp: Timestamp(34_205_000_000_000),
        },
        order_ref: OrderReference(7001),
        executed_shares: Shares(100),
        match_number: MatchNumber(4_2_0001),
    })
```

#### Example 4 - taker-maker fill at non-resting price, `C` (with-price)

```text
OrderBook-rs event: Fill {
    maker_id: 0xACF, // hidden midpoint resting order
    taker_id: 0xAD0,
    shares: 100,
    executed_price: 1925050, // midpoint
    resting_display_price: 1925000, // displayable BBO
    match_id: 4_2_0002,
    printable: true,
    t: 2026-05-06T13:30:05.100Z,
}

Bridge state:
    registry: { 0xACF -> OrderReference(7050) (still resting) }

Output:
    Message::OrderExecutedWithPrice(OrderExecutedWithPrice {
        header: Header {
            stock_locate: StockLocate(1),
            tracking_number: TrackingNumber(0),
            timestamp: Timestamp(34_205_100_000_000),
        },
        order_ref: OrderReference(7050),
        executed_shares: Shares(100),
        match_number: MatchNumber(4_2_0002),
        printable: Printable::Yes,
        execution_price: Price4(1_925_050),
    })
```

#### Example 5 - partial cancel, `X`

```text
OrderBook-rs event: Cancel {
    id: 0xABC,
    cancelled_shares: 200,
    t: 2026-05-06T13:30:10.000Z,
}

Bridge state:
    registry: { 0xABC -> OrderReference(7001) (still resting,
                                              now 300 / original 500) }

Output:
    Message::OrderCancel(OrderCancel {
        header: Header {
            stock_locate: StockLocate(1),
            tracking_number: TrackingNumber(0),
            timestamp: Timestamp(34_210_000_000_000),
        },
        order_ref: OrderReference(7001),
        cancelled_shares: Shares(200),
    })
```

#### Example 6 - full cancel / deletion, `D`

```text
OrderBook-rs event: Delete {
    id: 0xABC,
    t: 2026-05-06T13:30:11.000Z,
}

Bridge state:
    registry: { 0xABC -> OrderReference(7001) (about to be retired) }

Output:
    Message::OrderDelete(OrderDelete {
        header: Header {
            stock_locate: StockLocate(1),
            tracking_number: TrackingNumber(0),
            timestamp: Timestamp(34_211_000_000_000),
        },
        order_ref: OrderReference(7001),
    })

Side effect:
    registry.remove(&0xABC)
```

#### Example 7 - replace (modify), `U`

```text
OrderBook-rs event: Modify {
    old_id: 0xABD,
    new_id: 0xABE,
    new_price: 1925200,
    new_qty: 150,
    t: 2026-05-06T13:30:12.000Z,
}

Bridge state (before):
    registry: { 0xABD -> OrderReference(7002) }

Bridge state (after, atomic):
    registry: { 0xABE -> OrderReference(7100) }
    // 0xABD removed; OrderReference(7002) retired; 7100 fresh.

Output:
    Message::OrderReplace(OrderReplace {
        header: Header {
            stock_locate: StockLocate(1),
            tracking_number: TrackingNumber(0),
            timestamp: Timestamp(34_212_000_000_000),
        },
        original_order_ref: OrderReference(7002),
        new_order_ref: OrderReference(7100),
        shares: Shares(150),
        price: Price4(1_925_200),
    })
```

#### Example 8 - non-cross trade print, `P` (both legs hidden)

```text
OrderBook-rs event: Fill {
    maker_id: 0xACF, // hidden midpoint
    taker_id: 0xAD1, // hidden IOC midpoint
    shares: 100,
    executed_price: 1925050,
    resting_display_price: None, // both non-displayable
    match_id: 4_2_0003,
    printable: true,
    t: 2026-05-06T13:30:13.000Z,
}

Detection: bridge sees both legs are non-displayable, so emits
`P` (TradeNonCross) instead of `E` / `C`. Per
docs/PROTOCOL-SPEC.md section 8 backward-compat quirks:
post-2010-12-06 `order_ref` is always 0; post-2014-07-14 `side`
is always Side::Buy.

Output:
    Message::TradeNonCross(TradeNonCross {
        header: Header {
            stock_locate: StockLocate(1),
            tracking_number: TrackingNumber(0),
            timestamp: Timestamp(34_213_000_000_000),
        },
        order_ref: OrderReference(0),    // quirk: always 0
        side: Side::Buy,                 // quirk: always Buy
        shares: Shares(100),
        stock: Stock::new("AAPL"),
        price: Price4(1_925_050),
        match_number: MatchNumber(4_2_0003),
    })
```

#### Example 9 - broken trade, `B` (CONDITIONAL: requires upstream support)

```text
OrderBook-rs event: BreakTrade {
    match_id: 4_2_0001,
    t: 2026-05-06T13:35:00.000Z,
}

Output:
    Message::BrokenTrade(BrokenTrade {
        header: Header {
            stock_locate: StockLocate(1),
            tracking_number: TrackingNumber(0),
            timestamp: Timestamp(34_500_000_000_000),
        },
        match_number: MatchNumber(4_2_0001),
    })
```

If `OrderBook-rs` does not surface a break event, this row is OUT
OF SCOPE for v0.1 and the bridge emits no `B` messages. Tracked
as future work pending upstream support.

#### Example 10 - book-change without trade (NO output)

```text
OrderBook-rs event: BookSnapshot { ... }

The bridge MUST NOT emit an ITCH message in response. ITCH does
not aggregate; the resting-order add / cancel / delete / modify
events have already produced the appropriate `A` / `F` / `X` /
`D` / `U` messages. A snapshot event is a consistency hint, not
a state change.

Output: (none)
```

#### Examples 11-12 - OUT OF SCOPE rows

`Q` (Cross Trade), `I` (NOII), `V` / `W` (MWCB), `H` / `Y` (Reg
SHO / Trading State) all sit on the OUT OF SCOPE list for v0.1.
The bridge emits none of them in response to engine events. `H`
and `Y` may be supplied via the `DirectoryProvider`'s periodic
refresh (a hook documented in the implementation issue), NOT
triggered by an `OrderBook-rs` event. Worked examples for these
rows land with the follow-up issue that adds the auction module.

### Open questions

1. **`Mpid` from `owner_id`.** ITCH `Mpid` is a 4-char ASCII code
   (e.g. `NSDQ`); `OrderBook-rs::owner` is an opaque `String`.
   The bridge needs a `MpidProvider` trait users implement.
   Recommended fallback when `mpid_for(owner)` returns `None`:
   emit `A` (no attribution) instead of `F` and drop the
   attribution silently with a `tracing::debug`. Documented; the
   implementation issue tracks a follow-up "MpidProvider design
   plus adapters" (Section 3).

2. **STP cancel reason loss.**
   `OrderBook-rs::CancelReason::CancelTaker` /
   `CancelMaker` / `CancelBoth` carry semantic information that
   ITCH `D` / `X` cannot represent. The bridge issues `D` / `X`
   per the mapping table; the reason is lost on the wire.
   Recommendation: add an out-of-band `BridgeMetadata` channel for
   consumers that care about the reason. Tracked under
   "MpidProvider design plus adapters" plus a separate
   "BridgeMetadata for STP semantics" follow-up (Section 3).

3. **Replay from snapshot.** An `OrderBook-rs` snapshot does not
   preserve ITCH-level sequence; a fresh stream replayed from a
   snapshot starts at sequence 1. Recommendation: documented and
   accepted - replay scenarios that need original sequence numbers
   MUST use an `itch-replay` capture (ADR-0011) rather than a
   bridge-synthesised stream. No code change needed; the user
   chooses the right tool.

4. **Cross trades and auctions.** OUT OF SCOPE for v0.1; tracked
   as a follow-up issue gated on `OrderBook-rs` adding an auction
   module.

5. **NOII / circuit breakers / Reg SHO / trading state.** OUT OF
   SCOPE for v0.1 as event-driven outputs. `H` / `Y` may be
   supplied through a `DirectoryProvider` periodic refresh
   (documented hook, not a v0.1 deliverable). `I` / `V` / `W` are
   NASDAQ-specific market-state messages with no `OrderBook-rs`
   source; tracked as future work.

---

## Section 3 - follow-up issues to file

After issue #49 merges, file the following tickets so the
implementation has a discoverable path. The list intentionally
mirrors the structure of Section 2 so each open question has an
owner.

1. **"Implement `itch-orderbook` v0.1 (full bridge per
   ADR-0013)"** - production code. Depends on a tagged
   `OrderBook-rs` release. Lands the eight in-scope mapping rows
   (`A`, `F`, `E`, `C`, `X`, `D`, `U`, `P`), plus
   `BridgeConfig`, `LocateStrategy::Registry`, `TimestampSource`,
   and `BridgeError`.

2. **"`DirectoryProvider` adapters (in-memory map, JSON-file,
   user-trait-impl)"** - reference-data adapters for the `R`
   Stock Directory message. Ships behind a `defaults` Cargo
   feature so a user with their own provider pays nothing.

3. **"`itch-orderbook` round-trip integration test (OrderBook-rs
   to bridge to itch-book)"** - end-to-end harness. Drives
   `OrderBook-rs` with a deterministic order tape, captures the
   bridge's ITCH output, replays through `itch-book`, asserts the
   reconstructed L2 / L3 book equals the original tape's expected
   book. This is the safety net for the mapping table.

4. **"`MpidProvider` design plus adapters"** - resolves Open
   Question 1. Ships a trait, an in-memory adapter, and a
   documented fallback (emit `A` instead of `F` when no `Mpid` is
   available).

5. **"`BridgeMetadata` channel for STP semantics"** - resolves
   Open Question 2. Out-of-band side channel so consumers that
   care about `CancelReason` can receive it without polluting the
   ITCH stream.

6. **"Cross trade / auction support (`Q` / `I` messages)"** -
   resolves Open Questions 4 and 5 (partial). Depends on
   `OrderBook-rs` adding an auction module.

7. **"Broken-trade support (`B` message)"** - resolves the
   conditional row in the mapping table. Depends on
   `OrderBook-rs` exposing a break event.

8. **"`LocateStrategy::Hash` implementation"** - optional;
   provides the stateless alternative to the registry. Not on the
   v0.1 critical path.

When this file is committed, the bridge's design history is
preserved with the workspace. The implementation issue picks up
from here.
