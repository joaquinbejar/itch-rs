# ADR-0013: itch-orderbook Bridge Crate

**Status:** Accepted  
**Authors:** Rust + Market-Data Team  
**Date:** 2026-05-06

## Context

`itch-rs` provides production ITCH 5.0 codecs and transports; `OrderBook-rs` provides a high-performance matching engine with an event-driven API. Both are maintained by the same team and are designed to integrate: the engine emits `TradeResult`, `BookChangeEvent`, `OrderStateEvent` events that should be publishable as ITCH messages via `itch-source::MessageSource`.

However, the two event models have significant structural differences:

- OrderBook-rs has **order IDs** (`pricelevel::Id`); ITCH has **OrderReference** (`u64`), requiring a mapping function.
- OrderBook-rs distinguishes **STP cancels** with `CancelReason::STP*`; ITCH has only `Delete` (`D`) and `ExecutedPartial` (`X`), losing the STP classification.
- OrderBook-rs carries **fees** in `TradeResult`; ITCH does not.
- **Reference data** (`MarketCategory`, `FinancialStatus`, `LuldTier`) lives outside OrderBook-rs but is required by ITCH `R` Stock Directory; the bridge needs a pluggable source.
- **Timestamp** policy: OrderBook-rs emits `TimestampMs` (milliseconds); ITCH `Timestamp` is nanoseconds since midnight ET. Conversion and timezone assumptions must be explicit.
- **Sequence** policy: ITCH messages require a monotonic per-day sequence counter. The bridge owns this, not the OrderBook-rs consumer.
- **Market-state messages** (`Q` Cross Trade, `I` NOII, `L` Market Status) have no direct OrderBook-rs equivalents unless specialized modules exist.

## Decision

Introduce a new crate **`itch-orderbook`** that:

1. **Depends on** `itch-source`, `itch-protocol`, and (when stable) `OrderBook-rs`.
2. **Exposes** a `BridgeBuilder` configuration API that consumes:
   - An `OrderBook-rs` event stream (sync iterator or async stream).
   - A `DirectoryProvider` trait instance (user-supplied for symbol metadata).
   - Configuration flags: `LocateStrategy`, `TimestampSource`, `EnableCrossMessages`, etc.
3. **Implements** `MessageSource` so the output plugs into `itch-server` unchanged.
4. **Encodes** every OrderBook-rs event into the appropriate ITCH message(s) following the mapping table in the design doc.
5. **Owns** the ITCH sequence counter — increments monotonically, resets daily at midnight ET.
6. **Defers** full implementation until OrderBook-rs event shapes are stable (v0.4 target).

The bridge is a **maintained crate** — when OrderBook-rs events change, `itch-orderbook` has a corresponding release, coordinated via roadmap.

## Consequences

**Positive:**
- Single authoritative bridge — no user duplication.
- Composition is explicit: `itch-orderbook` is a library, not a template or macro.
- Extensibility via `DirectoryProvider` and config traits.

**Negative:**
- Maintenance burden if OrderBook-rs event API changes frequently.
- Lost information (fees, STP reason, market-state context) is documented but not recoverable post-bridge.

## Alternatives Considered

1. **Macro-driven code generation** (rejected)
   - Hides the mapping logic; users can't reason about it without reading macro expansion.
   - Harder to maintain; changes require template rewrites.

2. **User implements the bridge** (rejected)
   - Every integration duplicates the 20-message mapping.
   - Bugs and inconsistencies proliferate.
   - No single source of truth for ITCH sequence numbering.

3. **Code generation from specs** (rejected)
   - ITCH 5.0 is static (fixed as of 2010-12-06 + post-revision quirks).
   - Oversells a code-gen approach for a one-time bridge.

4. **OrderBook-rs emits ITCH directly** (rejected)
   - Couples the engine to the wire format.
   - Violates separation of concerns.

## Implementation Notes

- Start with design + ADR in v0.4; full implementation ships post-v0.4 once OrderBook-rs stabilizes.
- The `DirectoryProvider` trait must support both static (JSON) and dynamic (database) sources.
- Timestamp conversion (ms → ns, TZ-aware) is tested exhaustively.
- Sequence reset at midnight ET is mocked in tests; real-world deployments must coordinate with trading-day calendars.

## References

- ADR-0012: Source-trait crate design
- `docs/orderbook-bridge.md`: Full design and mapping table
- `OrderBook-rs/CHANGELOG.md`: Event stability tracking
