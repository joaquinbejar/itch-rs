//! ITCH 5.0 message DTOs and the top-level [`Message`] enum.
//!
//! Each per-kind struct embeds the 10-byte [`Header`] at field
//! offset 0 and exposes the per-message payload fields documented in
//! the spec. The codec impls (`Encode` / `Decode`) land in the
//! follow-up codec PR; this module owns just the data shapes.
//!
//! Total wire size for each kind = 1 (tag byte) + the per-struct
//! `BODY_LEN` constant.

// The hand-written `Default` impls for the closed-set enums below
// pick one canonical "blank" variant per field so the surrounding
// structs can `derive(Default)`. Clippy notices they "could be
// derived" — true, but only by extending `enum_alpha!` to add
// `Default` + `#[default]` to every enum, which would couple the
// macro to a specific default-variant choice the macro callers
// shouldn't have to commit to. Keeping the impls explicit and silent.
#![allow(clippy::derivable_impls)]

use crate::enums::{
    Authenticity, BreachedLevel, CrossType, EventCode, FinancialStatus, ImbalanceDirection,
    IpoReleaseQualifier, LuldTier, MarketCategory, MarketMakerMode, MarketParticipantState,
    PriceVariation, Printable, RegShoAction, RpiInterestFlag, Side, TradingState, YesNo,
};
use crate::primitives::{
    MatchNumber, Mpid, OrderReference, Price4, Price8, Shares, Stock, StockLocate, Timestamp,
    TrackingNumber,
};

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

/// 10-byte header at the start of every ITCH 5.0 message body.
///
/// Wire layout:
///
/// | Body offset | Length | Field             | Type             |
/// |------------:|-------:|-------------------|------------------|
/// | 0           | 2      | `stock_locate`    | [`StockLocate`]  |
/// | 2           | 2      | `tracking_number` | [`TrackingNumber`] |
/// | 4           | 6      | `timestamp`       | [`Timestamp`]    |
///
/// Per spec, `stock_locate` is always `0` on the wire for
/// session-level (non-stock) messages — `SystemEvent`,
/// `MwcbDeclineLevel`, `MwcbStatus`, `IpoQuotingPeriodUpdate`. This
/// is a session-level invariant **not** enforced by the codec:
/// consumers that care can validate the field after decode.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Header {
    /// Daily-assigned per-symbol array index.
    pub stock_locate: StockLocate,
    /// Opaque NASDAQ-internal id.
    pub tracking_number: TrackingNumber,
    /// Nanoseconds since midnight Eastern Time.
    pub timestamp: Timestamp,
}

impl Header {
    /// Wire size in bytes.
    pub const WIRE_LEN: usize = 10;
}

// ---------------------------------------------------------------------------
// 4.1 — System Event
// ---------------------------------------------------------------------------

/// `S` System Event. Body 11 B (10 header + 1 event code).
///
/// `Header.stock_locate` is `0` for this message (session-level).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct SystemEvent {
    /// Common header.
    pub header: Header,
    /// Lifecycle code: start / end of messages, system, market.
    pub event_code: EventCode,
}

impl SystemEvent {
    /// Body size in bytes (excludes the 1-byte tag).
    pub const BODY_LEN: usize = 11;
}

impl Default for EventCode {
    fn default() -> Self {
        Self::StartOfMessages
    }
}

// ---------------------------------------------------------------------------
// 4.2.1 — Stock Directory
// ---------------------------------------------------------------------------

/// `R` Stock Directory. Body 38 B (10 header + 28 payload).
///
/// Wire layout (body offsets, after the 10-byte header):
///
/// | Body offset | Length | Field                       |
/// |------------:|-------:|-----------------------------|
/// | 10          | 8      | `stock`                     |
/// | 18          | 1      | `market_category`           |
/// | 19          | 1      | `financial_status`          |
/// | 20          | 4      | `round_lot_size`            |
/// | 24          | 1      | `round_lots_only`           |
/// | 25          | 1      | `issue_classification`      |
/// | 26          | 2      | `issue_subtype`             |
/// | 28          | 1      | `authenticity`              |
/// | 29          | 1      | `short_sale_threshold`      |
/// | 30          | 1      | `ipo_flag`                  |
/// | 31          | 1      | `luld_reference_price_tier` |
/// | 32          | 1      | `etp_flag`                  |
/// | 33          | 4      | `etp_leverage_factor`       |
/// | 37          | 1      | `inverse_indicator`         |
///
/// `Default` returns a struct whose ASCII byte fields
/// (`issue_classification`, `issue_subtype`) are space-padded
/// (`0x20`), matching the spec's right-pad-with-spaces convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StockDirectory {
    /// Common header.
    pub header: Header,
    /// Symbol (8 B ASCII, right-padded with spaces).
    pub stock: Stock,
    /// Listing market category.
    pub market_category: MarketCategory,
    /// Listing-rule compliance code.
    pub financial_status: FinancialStatus,
    /// Round-lot size (number of shares per round lot).
    pub round_lot_size: Shares,
    /// `Y` if the symbol trades round-lots-only.
    pub round_lots_only: YesNo,
    /// NASDAQ-assigned issue classification code (single ASCII byte).
    pub issue_classification: u8,
    /// NASDAQ-assigned issue subtype (2 ASCII bytes, right-padded).
    pub issue_subtype: [u8; 2],
    /// Live vs Test listing.
    pub authenticity: Authenticity,
    /// `Y` if the symbol meets the SEC short-sale circuit-breaker
    /// threshold.
    pub short_sale_threshold: YesNo,
    /// `Y` if the symbol is in its IPO quoting period.
    pub ipo_flag: YesNo,
    /// LULD tier.
    pub luld_reference_price_tier: LuldTier,
    /// `Y` if the symbol is an Exchange-Traded Product.
    pub etp_flag: YesNo,
    /// ETP leverage factor (raw u32; 0 if not applicable).
    pub etp_leverage_factor: u32,
    /// `Y` if the ETP is an inverse leveraged product.
    pub inverse_indicator: YesNo,
}

impl StockDirectory {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 38;
}

impl Default for StockDirectory {
    fn default() -> Self {
        Self {
            header: Header::default(),
            stock: Stock::default(),
            market_category: MarketCategory::default(),
            financial_status: FinancialStatus::default(),
            round_lot_size: Shares::default(),
            round_lots_only: YesNo::default(),
            issue_classification: b' ',
            issue_subtype: [b' '; 2],
            authenticity: Authenticity::default(),
            short_sale_threshold: YesNo::default(),
            ipo_flag: YesNo::default(),
            luld_reference_price_tier: LuldTier::default(),
            etp_flag: YesNo::default(),
            etp_leverage_factor: 0,
            inverse_indicator: YesNo::default(),
        }
    }
}

impl Default for MarketCategory {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl Default for FinancialStatus {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl Default for Authenticity {
    fn default() -> Self {
        Self::Live
    }
}

impl Default for YesNo {
    fn default() -> Self {
        Self::Unavailable
    }
}

impl Default for LuldTier {
    fn default() -> Self {
        Self::Unavailable
    }
}

// ---------------------------------------------------------------------------
// 4.2.2 — Stock Trading Action
// ---------------------------------------------------------------------------

/// `H` Stock Trading Action. Body 24 B (10 header + 14 payload).
///
/// Wire layout (body offsets):
///
/// | Body offset | Length | Field           |
/// |------------:|-------:|-----------------|
/// | 10          | 8      | `stock`         |
/// | 18          | 1      | `trading_state` |
/// | 19          | 1      | `reserved`      |
/// | 20          | 4      | `reason`        |
///
/// `Default` returns a struct whose `reason` and `reserved` bytes
/// are space-padded (`0x20`) — matches the spec's
/// right-pad-with-spaces convention. The reserved byte's spec
/// default is also a space.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StockTradingAction {
    /// Common header.
    pub header: Header,
    /// Symbol.
    pub stock: Stock,
    /// New trading state.
    pub trading_state: TradingState,
    /// Reserved byte from the spec; preserved verbatim on the wire.
    pub reserved: u8,
    /// 4 ASCII bytes carrying the NASDAQ trading-action reason code,
    /// right-padded with spaces.
    pub reason: [u8; 4],
}

impl StockTradingAction {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 24;
}

impl Default for StockTradingAction {
    fn default() -> Self {
        Self {
            header: Header::default(),
            stock: Stock::default(),
            trading_state: TradingState::default(),
            reserved: b' ',
            reason: [b' '; 4],
        }
    }
}

impl Default for TradingState {
    fn default() -> Self {
        Self::Trading
    }
}

// ---------------------------------------------------------------------------
// 4.2.3 — Reg SHO Restriction
// ---------------------------------------------------------------------------

/// `Y` Reg SHO Short-Sale Price-Test Restriction. Body 19 B
/// (10 header + 9 payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct RegShoRestriction {
    /// Common header.
    pub header: Header,
    /// Symbol.
    pub stock: Stock,
    /// New Reg SHO action.
    pub reg_sho_action: RegShoAction,
}

impl RegShoRestriction {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 19;
}

impl Default for RegShoAction {
    fn default() -> Self {
        Self::NoPriceTest
    }
}

// ---------------------------------------------------------------------------
// 4.2.4 — Market Participant Position
// ---------------------------------------------------------------------------

/// `L` Market Participant Position. Body 25 B (10 header + 15 payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct MarketParticipantPosition {
    /// Common header.
    pub header: Header,
    /// Market participant ID (4 ASCII bytes, right-padded).
    pub mpid: Mpid,
    /// Symbol.
    pub stock: Stock,
    /// `Y` if the MPID is the primary market maker for the symbol.
    pub primary_market_maker: YesNo,
    /// Quoting mode.
    pub market_maker_mode: MarketMakerMode,
    /// Lifecycle state.
    pub market_participant_state: MarketParticipantState,
}

impl MarketParticipantPosition {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 25;
}

impl Default for MarketMakerMode {
    fn default() -> Self {
        Self::Normal
    }
}

impl Default for MarketParticipantState {
    fn default() -> Self {
        Self::Active
    }
}

// ---------------------------------------------------------------------------
// 4.2.5.1 — MWCB Decline Level
// ---------------------------------------------------------------------------

/// `V` MWCB Decline Level. Body 34 B (10 header + 24 payload).
///
/// Daily Level 1 / 2 / 3 trigger prices for the Market-Wide Circuit
/// Breaker. Session-level message — `Header.stock_locate` is `0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct MwcbDeclineLevel {
    /// Common header.
    pub header: Header,
    /// Level 1 (7%) trigger price.
    pub level1: Price8,
    /// Level 2 (13%) trigger price.
    pub level2: Price8,
    /// Level 3 (20%) trigger price.
    pub level3: Price8,
}

impl MwcbDeclineLevel {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 34;
}

// ---------------------------------------------------------------------------
// 4.2.5.2 — MWCB Status
// ---------------------------------------------------------------------------

/// `W` MWCB Status. Body 11 B (10 header + 1 payload).
///
/// Session-level message — `Header.stock_locate` is `0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct MwcbStatus {
    /// Common header.
    pub header: Header,
    /// Level that was breached.
    pub breached_level: BreachedLevel,
}

impl MwcbStatus {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 11;
}

impl Default for BreachedLevel {
    fn default() -> Self {
        Self::Level1
    }
}

// ---------------------------------------------------------------------------
// 4.2.6 — IPO Quoting Period Update
// ---------------------------------------------------------------------------

/// `K` IPO Quoting Period Update. Body 27 B (10 header + 17 payload).
///
/// Session-level message — `Header.stock_locate` is `0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct IpoQuotingPeriodUpdate {
    /// Common header.
    pub header: Header,
    /// Symbol.
    pub stock: Stock,
    /// Anticipated IPO release time, in seconds since midnight ET.
    pub ipo_quotation_release_time: u32,
    /// Whether the release is anticipated or cancelled.
    pub ipo_quotation_release_qualifier: IpoReleaseQualifier,
    /// Anticipated IPO price.
    pub ipo_price: Price4,
}

impl IpoQuotingPeriodUpdate {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 27;
}

impl Default for IpoReleaseQualifier {
    fn default() -> Self {
        Self::Anticipated
    }
}

// ---------------------------------------------------------------------------
// 4.3.1 — Add Order — No MPID
// ---------------------------------------------------------------------------

/// `A` Add Order — No MPID. Body 35 B (10 header + 25 payload).
///
/// Wire layout (body offsets):
///
/// | Body offset | Length | Field       |
/// |------------:|-------:|-------------|
/// | 10          | 8      | `order_ref` |
/// | 18          | 1      | `side`      |
/// | 19          | 4      | `shares`    |
/// | 23          | 8      | `stock`     |
/// | 31          | 4      | `price`     |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct AddOrder {
    /// Common header.
    pub header: Header,
    /// Day-unique reference number.
    pub order_ref: OrderReference,
    /// Buy / sell.
    pub side: Side,
    /// Quantity.
    pub shares: Shares,
    /// Symbol.
    pub stock: Stock,
    /// Limit price.
    pub price: Price4,
}

impl AddOrder {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 35;
}

impl Default for Side {
    fn default() -> Self {
        Self::Buy
    }
}

// ---------------------------------------------------------------------------
// 4.3.2 — Add Order — With MPID
// ---------------------------------------------------------------------------

/// `F` Add Order — With MPID. Body 39 B (10 header + 29 payload).
///
/// Same shape as [`AddOrder`] plus a 4-byte broker attribution
/// suffix.
///
/// Wire layout (body offsets):
///
/// | Body offset | Length | Field         |
/// |------------:|-------:|---------------|
/// | 10          | 8      | `order_ref`   |
/// | 18          | 1      | `side`        |
/// | 19          | 4      | `shares`      |
/// | 23          | 8      | `stock`       |
/// | 31          | 4      | `price`       |
/// | 35          | 4      | `attribution` |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct AddOrderWithMpid {
    /// Common header.
    pub header: Header,
    /// Day-unique reference number.
    pub order_ref: OrderReference,
    /// Buy / sell.
    pub side: Side,
    /// Quantity.
    pub shares: Shares,
    /// Symbol.
    pub stock: Stock,
    /// Limit price.
    pub price: Price4,
    /// Attribution (MPID).
    pub attribution: Mpid,
}

impl AddOrderWithMpid {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 39;
}

// ---------------------------------------------------------------------------
// 4.4.1 — Order Executed
// ---------------------------------------------------------------------------

/// `E` Order Executed. Body 30 B (10 header + 20 payload).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct OrderExecuted {
    /// Common header.
    pub header: Header,
    /// Reference number of the resting order being executed.
    pub order_ref: OrderReference,
    /// Number of shares filled.
    pub executed_shares: Shares,
    /// Day-unique execution identifier.
    pub match_number: MatchNumber,
}

impl OrderExecuted {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 30;
}

// ---------------------------------------------------------------------------
// 4.4.2 — Order Executed With Price
// ---------------------------------------------------------------------------

/// `C` Order Executed With Price. Body 35 B (10 header + 25 payload).
///
/// Same as [`OrderExecuted`] but signals that the execution price
/// differs from the resting display price (e.g., midpoint, RPI).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct OrderExecutedWithPrice {
    /// Common header.
    pub header: Header,
    /// Reference number of the resting order.
    pub order_ref: OrderReference,
    /// Number of shares filled.
    pub executed_shares: Shares,
    /// Day-unique execution identifier.
    pub match_number: MatchNumber,
    /// Whether this execution should appear on the consolidated tape.
    pub printable: Printable,
    /// Actual execution price (may differ from the resting display
    /// price).
    pub execution_price: Price4,
}

impl OrderExecutedWithPrice {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 35;
}

impl Default for Printable {
    fn default() -> Self {
        Self::Printable
    }
}

// ---------------------------------------------------------------------------
// 4.4.3 — Order Cancel
// ---------------------------------------------------------------------------

/// `X` Order Cancel. Body 22 B (10 header + 12 payload).
///
/// Partial cancellation — reduces the displayed shares of the
/// resting order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct OrderCancel {
    /// Common header.
    pub header: Header,
    /// Reference number of the resting order.
    pub order_ref: OrderReference,
    /// Number of shares cancelled.
    pub cancelled_shares: Shares,
}

impl OrderCancel {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 22;
}

// ---------------------------------------------------------------------------
// 4.4.4 — Order Delete
// ---------------------------------------------------------------------------

/// `D` Order Delete. Body 18 B (10 header + 8 payload).
///
/// Removes the order from the book entirely.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct OrderDelete {
    /// Common header.
    pub header: Header,
    /// Reference number of the resting order.
    pub order_ref: OrderReference,
}

impl OrderDelete {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 18;
}

// ---------------------------------------------------------------------------
// 4.4.5 — Order Replace
// ---------------------------------------------------------------------------

/// `U` Order Replace. Body 34 B (10 header + 24 payload).
///
/// Cancel + new order in one message; the new order receives a new
/// reference number and (per spec) loses queue priority.
///
/// Wire layout (body offsets):
///
/// | Body offset | Length | Field                |
/// |------------:|-------:|----------------------|
/// | 10          | 8      | `original_order_ref` |
/// | 18          | 8      | `new_order_ref`      |
/// | 26          | 4      | `shares`             |
/// | 30          | 4      | `price`              |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct OrderReplace {
    /// Common header.
    pub header: Header,
    /// Reference number of the order being replaced.
    pub original_order_ref: OrderReference,
    /// Reference number of the new resting order.
    pub new_order_ref: OrderReference,
    /// New displayed quantity.
    pub shares: Shares,
    /// New limit price.
    pub price: Price4,
}

impl OrderReplace {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 34;
}

// ---------------------------------------------------------------------------
// 4.5.1 — Trade (Non-Cross)
// ---------------------------------------------------------------------------

/// `P` Trade (Non-Cross). Body 43 B (10 header + 33 payload).
///
/// Backward-compatibility quirks per the ITCH 5.0 spec:
/// - Effective 2010-12-06, `order_ref` is always `0`.
/// - Effective 2014-07-14, `side` is always [`Side::Buy`] regardless
///   of the actual resting side.
///
/// The codec preserves both fields verbatim — consumers should not
/// rely on their values for sessions after those dates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct TradeNonCross {
    /// Common header.
    pub header: Header,
    /// Resting-order reference (always `0` post-2010-12-06).
    pub order_ref: OrderReference,
    /// Resting side (always `Buy` post-2014-07-14).
    pub side: Side,
    /// Number of shares filled.
    pub shares: Shares,
    /// Symbol.
    pub stock: Stock,
    /// Execution price.
    pub price: Price4,
    /// Day-unique execution identifier.
    pub match_number: MatchNumber,
}

impl TradeNonCross {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 43;
}

// ---------------------------------------------------------------------------
// 4.5.2 — Cross Trade
// ---------------------------------------------------------------------------

/// `Q` Cross Trade. Body 39 B (10 header + 29 payload).
///
/// `shares` is a u64 (not [`Shares`]) per spec — auction crosses can
/// involve quantities beyond the u32 range.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct CrossTrade {
    /// Common header.
    pub header: Header,
    /// Number of shares matched in the cross (u64).
    pub shares: u64,
    /// Symbol.
    pub stock: Stock,
    /// Cross clearing price.
    pub cross_price: Price4,
    /// Day-unique execution identifier.
    pub match_number: MatchNumber,
    /// Which cross produced the print (opening / closing /
    /// halted-IPO / intraday).
    pub cross_type: CrossType,
}

impl CrossTrade {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 39;
}

impl Default for CrossType {
    fn default() -> Self {
        Self::Opening
    }
}

// ---------------------------------------------------------------------------
// 4.5.3 — Broken Trade
// ---------------------------------------------------------------------------

/// `B` Broken Trade. Body 18 B (10 header + 8 payload).
///
/// Indicates that a previously-reported execution was broken and
/// should be removed from downstream tape feeds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct BrokenTrade {
    /// Common header.
    pub header: Header,
    /// Day-unique identifier of the execution that was broken.
    pub match_number: MatchNumber,
}

impl BrokenTrade {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 18;
}

// ---------------------------------------------------------------------------
// 4.6 — NOII
// ---------------------------------------------------------------------------

/// `I` Net Order Imbalance Indicator (NOII). Body 49 B
/// (10 header + 39 payload).
///
/// Issued every 5 seconds prior to a NASDAQ cross.
///
/// Wire layout (body offsets):
///
/// | Body offset | Length | Field                     |
/// |------------:|-------:|---------------------------|
/// | 10          | 8      | `paired_shares`           |
/// | 18          | 8      | `imbalance_shares`        |
/// | 26          | 1      | `imbalance_direction`     |
/// | 27          | 8      | `stock`                   |
/// | 35          | 4      | `far_price`               |
/// | 39          | 4      | `near_price`              |
/// | 43          | 4      | `current_reference_price` |
/// | 47          | 1      | `cross_type`              |
/// | 48          | 1      | `price_variation`         |
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Noii {
    /// Common header.
    pub header: Header,
    /// Number of paired shares (u64 — auction quantities can exceed
    /// u32).
    pub paired_shares: u64,
    /// Number of unpaired (imbalance) shares.
    pub imbalance_shares: u64,
    /// Direction of the imbalance.
    pub imbalance_direction: ImbalanceDirection,
    /// Symbol.
    pub stock: Stock,
    /// Far indicative price.
    pub far_price: Price4,
    /// Near indicative price.
    pub near_price: Price4,
    /// Current reference price.
    pub current_reference_price: Price4,
    /// Which cross this NOII is for.
    pub cross_type: CrossType,
    /// Rounded percentage variation between reference and near
    /// indicative price.
    pub price_variation: PriceVariation,
}

impl Noii {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 49;
}

impl Default for ImbalanceDirection {
    fn default() -> Self {
        Self::NoImbalance
    }
}

impl Default for PriceVariation {
    fn default() -> Self {
        Self::CannotBeCalculated
    }
}

// ---------------------------------------------------------------------------
// 4.7 — Retail Price Improvement
// ---------------------------------------------------------------------------

/// `N` Retail Price Improvement Indicator (RPII). Body 19 B
/// (10 header + 9 payload).
///
/// NASDAQ's RPI program ceased on 2014-12-31, but pre-cessation
/// captures still contain this message — the codec preserves it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct RetailPriceImprovement {
    /// Common header.
    pub header: Header,
    /// Symbol.
    pub stock: Stock,
    /// Which side(s) have RPII interest.
    pub interest_flag: RpiInterestFlag,
}

impl RetailPriceImprovement {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 19;
}

impl Default for RpiInterestFlag {
    fn default() -> Self {
        Self::None
    }
}

// ---------------------------------------------------------------------------
// Message enum
// ---------------------------------------------------------------------------

/// Top-level enum: one variant per ITCH 5.0 message kind.
///
/// Exhaustive over the 20 message kinds. Match arms must cover every
/// variant — adding a wildcard `_` is forbidden by the workspace
/// coding rules, because new ITCH revisions should surface as
/// compile errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Message {
    /// `S` System Event.
    SystemEvent(SystemEvent),
    /// `R` Stock Directory.
    StockDirectory(StockDirectory),
    /// `H` Stock Trading Action.
    StockTradingAction(StockTradingAction),
    /// `Y` Reg SHO Restriction.
    RegShoRestriction(RegShoRestriction),
    /// `L` Market Participant Position.
    MarketParticipantPosition(MarketParticipantPosition),
    /// `V` MWCB Decline Level.
    MwcbDeclineLevel(MwcbDeclineLevel),
    /// `W` MWCB Status.
    MwcbStatus(MwcbStatus),
    /// `K` IPO Quoting Period Update.
    IpoQuotingPeriodUpdate(IpoQuotingPeriodUpdate),
    /// `A` Add Order — No MPID.
    AddOrder(AddOrder),
    /// `F` Add Order — With MPID.
    AddOrderWithMpid(AddOrderWithMpid),
    /// `E` Order Executed.
    OrderExecuted(OrderExecuted),
    /// `C` Order Executed With Price.
    OrderExecutedWithPrice(OrderExecutedWithPrice),
    /// `X` Order Cancel.
    OrderCancel(OrderCancel),
    /// `D` Order Delete.
    OrderDelete(OrderDelete),
    /// `U` Order Replace.
    OrderReplace(OrderReplace),
    /// `P` Trade (Non-Cross).
    TradeNonCross(TradeNonCross),
    /// `Q` Cross Trade.
    CrossTrade(CrossTrade),
    /// `B` Broken Trade.
    BrokenTrade(BrokenTrade),
    /// `I` NOII.
    Noii(Noii),
    /// `N` Retail Price Improvement.
    RetailPriceImprovement(RetailPriceImprovement),
}

impl Message {
    /// One-byte ASCII tag identifying the message kind on the wire.
    #[inline]
    #[must_use]
    pub const fn tag(&self) -> u8 {
        match self {
            Self::SystemEvent(_) => b'S',
            Self::StockDirectory(_) => b'R',
            Self::StockTradingAction(_) => b'H',
            Self::RegShoRestriction(_) => b'Y',
            Self::MarketParticipantPosition(_) => b'L',
            Self::MwcbDeclineLevel(_) => b'V',
            Self::MwcbStatus(_) => b'W',
            Self::IpoQuotingPeriodUpdate(_) => b'K',
            Self::AddOrder(_) => b'A',
            Self::AddOrderWithMpid(_) => b'F',
            Self::OrderExecuted(_) => b'E',
            Self::OrderExecutedWithPrice(_) => b'C',
            Self::OrderCancel(_) => b'X',
            Self::OrderDelete(_) => b'D',
            Self::OrderReplace(_) => b'U',
            Self::TradeNonCross(_) => b'P',
            Self::CrossTrade(_) => b'Q',
            Self::BrokenTrade(_) => b'B',
            Self::Noii(_) => b'I',
            Self::RetailPriceImprovement(_) => b'N',
        }
    }

    /// Borrow the common 10-byte header.
    #[inline]
    #[must_use]
    pub const fn header(&self) -> &Header {
        match self {
            Self::SystemEvent(m) => &m.header,
            Self::StockDirectory(m) => &m.header,
            Self::StockTradingAction(m) => &m.header,
            Self::RegShoRestriction(m) => &m.header,
            Self::MarketParticipantPosition(m) => &m.header,
            Self::MwcbDeclineLevel(m) => &m.header,
            Self::MwcbStatus(m) => &m.header,
            Self::IpoQuotingPeriodUpdate(m) => &m.header,
            Self::AddOrder(m) => &m.header,
            Self::AddOrderWithMpid(m) => &m.header,
            Self::OrderExecuted(m) => &m.header,
            Self::OrderExecutedWithPrice(m) => &m.header,
            Self::OrderCancel(m) => &m.header,
            Self::OrderDelete(m) => &m.header,
            Self::OrderReplace(m) => &m.header,
            Self::TradeNonCross(m) => &m.header,
            Self::CrossTrade(m) => &m.header,
            Self::BrokenTrade(m) => &m.header,
            Self::Noii(m) => &m.header,
            Self::RetailPriceImprovement(m) => &m.header,
        }
    }

    /// Body size in bytes (excludes the 1-byte tag).
    #[inline]
    #[must_use]
    pub const fn body_len(&self) -> usize {
        match self {
            Self::SystemEvent(_) => SystemEvent::BODY_LEN,
            Self::StockDirectory(_) => StockDirectory::BODY_LEN,
            Self::StockTradingAction(_) => StockTradingAction::BODY_LEN,
            Self::RegShoRestriction(_) => RegShoRestriction::BODY_LEN,
            Self::MarketParticipantPosition(_) => MarketParticipantPosition::BODY_LEN,
            Self::MwcbDeclineLevel(_) => MwcbDeclineLevel::BODY_LEN,
            Self::MwcbStatus(_) => MwcbStatus::BODY_LEN,
            Self::IpoQuotingPeriodUpdate(_) => IpoQuotingPeriodUpdate::BODY_LEN,
            Self::AddOrder(_) => AddOrder::BODY_LEN,
            Self::AddOrderWithMpid(_) => AddOrderWithMpid::BODY_LEN,
            Self::OrderExecuted(_) => OrderExecuted::BODY_LEN,
            Self::OrderExecutedWithPrice(_) => OrderExecutedWithPrice::BODY_LEN,
            Self::OrderCancel(_) => OrderCancel::BODY_LEN,
            Self::OrderDelete(_) => OrderDelete::BODY_LEN,
            Self::OrderReplace(_) => OrderReplace::BODY_LEN,
            Self::TradeNonCross(_) => TradeNonCross::BODY_LEN,
            Self::CrossTrade(_) => CrossTrade::BODY_LEN,
            Self::BrokenTrade(_) => BrokenTrade::BODY_LEN,
            Self::Noii(_) => Noii::BODY_LEN,
            Self::RetailPriceImprovement(_) => RetailPriceImprovement::BODY_LEN,
        }
    }

    /// Total wire size in bytes (`1 + body_len()`).
    #[inline]
    #[must_use]
    pub const fn encoded_len(&self) -> usize {
        1 + self.body_len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn header() -> Header {
        Header {
            stock_locate: StockLocate::from_u16(7),
            tracking_number: TrackingNumber::from_u16(13),
            timestamp: Timestamp::from_u64(0x1234_5678_9abc),
        }
    }

    #[test]
    fn add_order_construction() {
        let m = AddOrder {
            header: header(),
            order_ref: OrderReference::from_u64(1001),
            side: Side::Buy,
            shares: Shares::from_u32(500),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_925_000),
        };
        assert_eq!(m.order_ref.as_u64(), 1001);
        assert_eq!(m.side, Side::Buy);
        assert_eq!(m.shares.as_u32(), 500);
        assert_eq!(m.stock.as_bytes(), b"AAPL    ");
        assert_eq!(m.price.as_u32(), 1_925_000);
    }

    #[test]
    fn order_replace_distinct_refs() {
        let m = OrderReplace {
            header: header(),
            original_order_ref: OrderReference::from_u64(100),
            new_order_ref: OrderReference::from_u64(200),
            shares: Shares::from_u32(750),
            price: Price4::from_u32(1_927_500),
        };
        assert_ne!(m.original_order_ref, m.new_order_ref);
    }

    #[test]
    fn trade_non_cross_post_2014_quirk_field_shapes() {
        let m = TradeNonCross {
            header: header(),
            order_ref: OrderReference::from_u64(0),
            side: Side::Buy,
            shares: Shares::from_u32(100),
            stock: Stock::new("MSFT"),
            price: Price4::from_u32(3_500_000),
            match_number: MatchNumber::from_u64(42),
        };
        assert_eq!(m.order_ref.as_u64(), 0);
        assert_eq!(m.side, Side::Buy);
    }

    #[test]
    fn cross_trade_uses_u64_shares() {
        let m = CrossTrade {
            header: header(),
            shares: u64::MAX,
            stock: Stock::new("SPY"),
            cross_price: Price4::from_u32(4_000_000),
            match_number: MatchNumber::from_u64(99),
            cross_type: CrossType::Opening,
        };
        assert_eq!(m.shares, u64::MAX);
    }

    #[test]
    fn noii_uses_u64_share_counts() {
        let m = Noii {
            header: header(),
            paired_shares: 1_000_000,
            imbalance_shares: 500_000,
            imbalance_direction: ImbalanceDirection::Buy,
            stock: Stock::new("QQQ"),
            far_price: Price4::from_u32(3_700_000),
            near_price: Price4::from_u32(3_710_000),
            current_reference_price: Price4::from_u32(3_705_000),
            cross_type: CrossType::Closing,
            price_variation: PriceVariation::Pct1To2,
        };
        assert_eq!(m.paired_shares, 1_000_000);
        assert_eq!(m.imbalance_shares, 500_000);
    }

    #[test]
    fn stock_directory_full_field_set() {
        let m = StockDirectory {
            header: header(),
            stock: Stock::new("AAPL"),
            market_category: MarketCategory::NasdaqGlobalSelect,
            financial_status: FinancialStatus::Normal,
            round_lot_size: Shares::from_u32(100),
            round_lots_only: YesNo::No,
            issue_classification: b'C',
            issue_subtype: *b"  ",
            authenticity: Authenticity::Live,
            short_sale_threshold: YesNo::No,
            ipo_flag: YesNo::No,
            luld_reference_price_tier: LuldTier::Tier1,
            etp_flag: YesNo::No,
            etp_leverage_factor: 0,
            inverse_indicator: YesNo::No,
        };
        assert_eq!(m.stock.as_bytes(), b"AAPL    ");
        assert_eq!(m.round_lot_size.as_u32(), 100);
        assert_eq!(m.market_category, MarketCategory::NasdaqGlobalSelect);
    }

    fn one_of_each() -> [(Message, u8, usize); 20] {
        let h = header();
        [
            (
                Message::SystemEvent(SystemEvent {
                    header: h,
                    event_code: EventCode::StartOfMessages,
                }),
                b'S',
                11,
            ),
            (
                Message::StockDirectory(StockDirectory {
                    header: h,
                    ..Default::default()
                }),
                b'R',
                38,
            ),
            (
                Message::StockTradingAction(StockTradingAction {
                    header: h,
                    ..Default::default()
                }),
                b'H',
                24,
            ),
            (
                Message::RegShoRestriction(RegShoRestriction {
                    header: h,
                    ..Default::default()
                }),
                b'Y',
                19,
            ),
            (
                Message::MarketParticipantPosition(MarketParticipantPosition {
                    header: h,
                    ..Default::default()
                }),
                b'L',
                25,
            ),
            (
                Message::MwcbDeclineLevel(MwcbDeclineLevel {
                    header: h,
                    ..Default::default()
                }),
                b'V',
                34,
            ),
            (
                Message::MwcbStatus(MwcbStatus {
                    header: h,
                    ..Default::default()
                }),
                b'W',
                11,
            ),
            (
                Message::IpoQuotingPeriodUpdate(IpoQuotingPeriodUpdate {
                    header: h,
                    ..Default::default()
                }),
                b'K',
                27,
            ),
            (
                Message::AddOrder(AddOrder {
                    header: h,
                    ..Default::default()
                }),
                b'A',
                35,
            ),
            (
                Message::AddOrderWithMpid(AddOrderWithMpid {
                    header: h,
                    ..Default::default()
                }),
                b'F',
                39,
            ),
            (
                Message::OrderExecuted(OrderExecuted {
                    header: h,
                    ..Default::default()
                }),
                b'E',
                30,
            ),
            (
                Message::OrderExecutedWithPrice(OrderExecutedWithPrice {
                    header: h,
                    ..Default::default()
                }),
                b'C',
                35,
            ),
            (
                Message::OrderCancel(OrderCancel {
                    header: h,
                    ..Default::default()
                }),
                b'X',
                22,
            ),
            (
                Message::OrderDelete(OrderDelete {
                    header: h,
                    ..Default::default()
                }),
                b'D',
                18,
            ),
            (
                Message::OrderReplace(OrderReplace {
                    header: h,
                    ..Default::default()
                }),
                b'U',
                34,
            ),
            (
                Message::TradeNonCross(TradeNonCross {
                    header: h,
                    ..Default::default()
                }),
                b'P',
                43,
            ),
            (
                Message::CrossTrade(CrossTrade {
                    header: h,
                    ..Default::default()
                }),
                b'Q',
                39,
            ),
            (
                Message::BrokenTrade(BrokenTrade {
                    header: h,
                    ..Default::default()
                }),
                b'B',
                18,
            ),
            (
                Message::Noii(Noii {
                    header: h,
                    ..Default::default()
                }),
                b'I',
                49,
            ),
            (
                Message::RetailPriceImprovement(RetailPriceImprovement {
                    header: h,
                    ..Default::default()
                }),
                b'N',
                19,
            ),
        ]
    }

    #[test]
    fn message_tag_and_body_len_match_spec_for_every_kind() {
        for (msg, tag, body) in one_of_each() {
            assert_eq!(msg.tag(), tag, "tag mismatch: {msg:?}");
            assert_eq!(msg.body_len(), body, "body_len mismatch: {msg:?}");
            assert_eq!(msg.encoded_len(), body + 1);
        }
    }

    #[test]
    fn message_distinct_tags() {
        let kinds = one_of_each();
        let mut tags: Vec<u8> = kinds.iter().map(|(m, _, _)| m.tag()).collect();
        tags.sort_unstable();
        let dedup_count = {
            let mut t = tags.clone();
            t.dedup();
            t.len()
        };
        assert_eq!(dedup_count, 20);
    }
}
