//! ITCH 5.0 message DTOs and the top-level [`Message`] enum.
//!
//! This module contains the 10-byte [`Header`] common to every
//! message body, twenty placeholder structs (one per ITCH 5.0
//! message kind), and the [`Message`] enum that fans out to those
//! structs. Today the placeholder structs carry only the
//! [`Header`]; the per-message payload fields land in the
//! follow-up DTO PR, and the `Encode` / `Decode` impls land in the
//! codec PR.
//!
//! Total wire size for each kind = 1 (tag byte) + the per-struct
//! `BODY_LEN` constant.

use crate::primitives::{StockLocate, Timestamp, TrackingNumber};

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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
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
// Per-message placeholder structs (issue #5 fills in the fields)
// ---------------------------------------------------------------------------

/// `S` System Event. See `docs/PROTOCOL-SPEC.md` §4.1. Body 11 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SystemEvent {
    /// Common header.
    pub header: Header,
}

impl SystemEvent {
    /// Body size in bytes (excludes the 1-byte tag).
    pub const BODY_LEN: usize = 11;
}

/// `R` Stock Directory. See `docs/PROTOCOL-SPEC.md` §4.2.1. Body 38 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StockDirectory {
    /// Common header.
    pub header: Header,
}

impl StockDirectory {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 38;
}

/// `H` Stock Trading Action. See `docs/PROTOCOL-SPEC.md` §4.2.2. Body 24 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct StockTradingAction {
    /// Common header.
    pub header: Header,
}

impl StockTradingAction {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 24;
}

/// `Y` Reg SHO Restriction. See `docs/PROTOCOL-SPEC.md` §4.2.3. Body 19 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RegShoRestriction {
    /// Common header.
    pub header: Header,
}

impl RegShoRestriction {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 19;
}

/// `L` Market Participant Position. See `docs/PROTOCOL-SPEC.md` §4.2.4. Body 25 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MarketParticipantPosition {
    /// Common header.
    pub header: Header,
}

impl MarketParticipantPosition {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 25;
}

/// `V` MWCB Decline Level. See `docs/PROTOCOL-SPEC.md` §4.2.5.1. Body 34 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MwcbDeclineLevel {
    /// Common header.
    pub header: Header,
}

impl MwcbDeclineLevel {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 34;
}

/// `W` MWCB Status. See `docs/PROTOCOL-SPEC.md` §4.2.5.2. Body 11 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MwcbStatus {
    /// Common header.
    pub header: Header,
}

impl MwcbStatus {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 11;
}

/// `K` IPO Quoting Period Update. See `docs/PROTOCOL-SPEC.md` §4.2.6. Body 27 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IpoQuotingPeriodUpdate {
    /// Common header.
    pub header: Header,
}

impl IpoQuotingPeriodUpdate {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 27;
}

/// `A` Add Order — No MPID. See `docs/PROTOCOL-SPEC.md` §4.3.1. Body 35 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AddOrder {
    /// Common header.
    pub header: Header,
}

impl AddOrder {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 35;
}

/// `F` Add Order — With MPID. See `docs/PROTOCOL-SPEC.md` §4.3.2. Body 39 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct AddOrderWithMpid {
    /// Common header.
    pub header: Header,
}

impl AddOrderWithMpid {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 39;
}

/// `E` Order Executed. See `docs/PROTOCOL-SPEC.md` §4.4.1. Body 30 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OrderExecuted {
    /// Common header.
    pub header: Header,
}

impl OrderExecuted {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 30;
}

/// `C` Order Executed With Price. See `docs/PROTOCOL-SPEC.md` §4.4.2. Body 35 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OrderExecutedWithPrice {
    /// Common header.
    pub header: Header,
}

impl OrderExecutedWithPrice {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 35;
}

/// `X` Order Cancel. See `docs/PROTOCOL-SPEC.md` §4.4.3. Body 22 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OrderCancel {
    /// Common header.
    pub header: Header,
}

impl OrderCancel {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 22;
}

/// `D` Order Delete. See `docs/PROTOCOL-SPEC.md` §4.4.4. Body 18 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OrderDelete {
    /// Common header.
    pub header: Header,
}

impl OrderDelete {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 18;
}

/// `U` Order Replace. See `docs/PROTOCOL-SPEC.md` §4.4.5. Body 34 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OrderReplace {
    /// Common header.
    pub header: Header,
}

impl OrderReplace {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 34;
}

/// `P` Trade (Non-Cross). See `docs/PROTOCOL-SPEC.md` §4.5.1. Body 43 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TradeNonCross {
    /// Common header.
    pub header: Header,
}

impl TradeNonCross {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 43;
}

/// `Q` Cross Trade. See `docs/PROTOCOL-SPEC.md` §4.5.2. Body 39 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct CrossTrade {
    /// Common header.
    pub header: Header,
}

impl CrossTrade {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 39;
}

/// `B` Broken Trade. See `docs/PROTOCOL-SPEC.md` §4.5.3. Body 18 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct BrokenTrade {
    /// Common header.
    pub header: Header,
}

impl BrokenTrade {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 18;
}

/// `I` NOII. See `docs/PROTOCOL-SPEC.md` §4.6. Body 49 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Noii {
    /// Common header.
    pub header: Header,
}

impl Noii {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 49;
}

/// `N` Retail Price Improvement. See `docs/PROTOCOL-SPEC.md` §4.7. Body 19 B.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RetailPriceImprovement {
    /// Common header.
    pub header: Header,
}

impl RetailPriceImprovement {
    /// Body size in bytes.
    pub const BODY_LEN: usize = 19;
}

// ---------------------------------------------------------------------------
// Message enum
// ---------------------------------------------------------------------------

/// Top-level enum: one variant per ITCH 5.0 message kind.
///
/// Exhaustive over the 20 message kinds spelled out in
/// `docs/PROTOCOL-SPEC.md` §3. Match arms must cover every variant —
/// adding a wildcard `_` is forbidden by the workspace coding rules,
/// because new ITCH revisions should surface as compile errors.
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

    fn header_of_kind(locate: u16, ts: u64) -> Header {
        Header {
            stock_locate: StockLocate::from_u16(locate),
            tracking_number: TrackingNumber::from_u16(0),
            timestamp: Timestamp::from_u64(ts),
        }
    }

    /// Convenience: every kind wrapped with a fixed header for the
    /// per-variant tag / body_len smoke checks below.
    fn one_of_each() -> [(Message, u8, usize); 20] {
        let h = header_of_kind(1, 1);
        [
            (Message::SystemEvent(SystemEvent { header: h }), b'S', 11),
            (
                Message::StockDirectory(StockDirectory { header: h }),
                b'R',
                38,
            ),
            (
                Message::StockTradingAction(StockTradingAction { header: h }),
                b'H',
                24,
            ),
            (
                Message::RegShoRestriction(RegShoRestriction { header: h }),
                b'Y',
                19,
            ),
            (
                Message::MarketParticipantPosition(MarketParticipantPosition { header: h }),
                b'L',
                25,
            ),
            (
                Message::MwcbDeclineLevel(MwcbDeclineLevel { header: h }),
                b'V',
                34,
            ),
            (Message::MwcbStatus(MwcbStatus { header: h }), b'W', 11),
            (
                Message::IpoQuotingPeriodUpdate(IpoQuotingPeriodUpdate { header: h }),
                b'K',
                27,
            ),
            (Message::AddOrder(AddOrder { header: h }), b'A', 35),
            (
                Message::AddOrderWithMpid(AddOrderWithMpid { header: h }),
                b'F',
                39,
            ),
            (
                Message::OrderExecuted(OrderExecuted { header: h }),
                b'E',
                30,
            ),
            (
                Message::OrderExecutedWithPrice(OrderExecutedWithPrice { header: h }),
                b'C',
                35,
            ),
            (Message::OrderCancel(OrderCancel { header: h }), b'X', 22),
            (Message::OrderDelete(OrderDelete { header: h }), b'D', 18),
            (Message::OrderReplace(OrderReplace { header: h }), b'U', 34),
            (
                Message::TradeNonCross(TradeNonCross { header: h }),
                b'P',
                43,
            ),
            (Message::CrossTrade(CrossTrade { header: h }), b'Q', 39),
            (Message::BrokenTrade(BrokenTrade { header: h }), b'B', 18),
            (Message::Noii(Noii { header: h }), b'I', 49),
            (
                Message::RetailPriceImprovement(RetailPriceImprovement { header: h }),
                b'N',
                19,
            ),
        ]
    }

    #[test]
    fn header_default_is_zero() {
        let h = Header::default();
        assert_eq!(h.stock_locate.as_u16(), 0);
        assert_eq!(h.tracking_number.as_u16(), 0);
        assert_eq!(h.timestamp.as_u64(), 0);
    }

    #[test]
    fn header_wire_len_is_ten() {
        assert_eq!(Header::WIRE_LEN, 10);
    }

    #[test]
    fn message_tag_matches_spec_for_every_kind() {
        for (msg, tag, _body) in one_of_each() {
            assert_eq!(msg.tag(), tag, "tag mismatch: {msg:?}");
        }
    }

    #[test]
    fn message_body_len_matches_spec_for_every_kind() {
        for (msg, _tag, body) in one_of_each() {
            assert_eq!(msg.body_len(), body, "body_len mismatch: {msg:?}");
        }
    }

    #[test]
    fn message_encoded_len_is_body_plus_one() {
        for (msg, _tag, body) in one_of_each() {
            assert_eq!(msg.encoded_len(), body + 1);
        }
    }

    #[test]
    fn message_header_returns_inner_header() {
        let h = header_of_kind(7, 0x1234);
        let m = Message::AddOrder(AddOrder { header: h });
        assert_eq!(*m.header(), h);
    }

    #[test]
    fn one_of_each_has_distinct_tags_and_covers_twenty_kinds() {
        let kinds = one_of_each();
        assert_eq!(kinds.len(), 20, "must cover 20 ITCH 5.0 message kinds");
        // Distinct tags: every kind has a unique on-wire byte.
        let mut tags: Vec<u8> = kinds.iter().map(|(m, _, _)| m.tag()).collect();
        tags.sort_unstable();
        let dedup_count = {
            let mut t = tags.clone();
            t.dedup();
            t.len()
        };
        assert_eq!(dedup_count, 20, "every kind must have a distinct tag");
        // Real compile-time exhaustiveness guard lives in the
        // exhaustive match arms of `Message::tag` / `header` /
        // `body_len`; a future ITCH revision that adds a 21st kind
        // will fail to compile there before this assert is reached.
    }
}
