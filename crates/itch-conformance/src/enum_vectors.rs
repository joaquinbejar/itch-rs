//! Closed-set ASCII enum vectors.
//!
//! For every [`AlphaCoded`](itch_protocol::AlphaCoded) enum exposed
//! by `itch-protocol`, this module publishes:
//!
//! - The full `(byte, variant)` table of valid wire codes — use it to
//!   smoke-check that your decoder maps every documented byte to the
//!   right variant (and vice versa).
//! - A short `unknown_bytes` list — bytes that are NOT in the spec
//!   set. A conformant decoder, when one of these bytes lands in the
//!   matching field of a real message body, must surface
//!   [`itch_protocol::ProtocolError::InvalidEnumCode`] (rather than
//!   silently coercing to a default variant).
//!
//! The unknown-bytes lists deliberately cover space (`0x20`), null
//! (`0x00`), high-bit (`0xFF`), and one printable byte that does not
//! appear in the variant set, so a conformant decoder is exercised
//! across both ASCII-graphic and non-graphic codepoints.

use itch_protocol::{
    Authenticity, BreachedLevel, CrossType, EventCode, FinancialStatus, ImbalanceDirection,
    IpoReleaseQualifier, LuldTier, MarketCategory, MarketMakerMode, MarketParticipantState,
    PriceVariation, Printable, RegShoAction, RpiInterestFlag, Side, TradingState, YesNo,
};

// ---------------------------------------------------------------------------
// EventCode
// ---------------------------------------------------------------------------

const EVENT_CODE_VARIANTS: &[(u8, EventCode)] = &[
    (b'O', EventCode::StartOfMessages),
    (b'S', EventCode::StartOfSystemHours),
    (b'Q', EventCode::StartOfMarketHours),
    (b'M', EventCode::EndOfMarketHours),
    (b'E', EventCode::EndOfSystemHours),
    (b'C', EventCode::EndOfMessages),
];

const EVENT_CODE_UNKNOWN: &[u8] = &[0x00, b' ', b'Z', 0xFF];

/// Documented `(byte, variant)` table for [`EventCode`].
#[must_use]
#[inline]
pub fn event_code_variants() -> &'static [(u8, EventCode)] {
    EVENT_CODE_VARIANTS
}

/// Bytes outside the documented [`EventCode`] set; a conformant
/// decoder must reject each via `InvalidEnumCode`.
#[must_use]
#[inline]
pub fn event_code_unknown_bytes() -> &'static [u8] {
    EVENT_CODE_UNKNOWN
}

// ---------------------------------------------------------------------------
// Side
// ---------------------------------------------------------------------------

const SIDE_VARIANTS: &[(u8, Side)] = &[(b'B', Side::Buy), (b'S', Side::Sell)];

const SIDE_UNKNOWN: &[u8] = &[0x00, b' ', b'X', 0xFF];

/// Documented `(byte, variant)` table for [`Side`].
#[must_use]
#[inline]
pub fn side_variants() -> &'static [(u8, Side)] {
    SIDE_VARIANTS
}

/// Bytes outside the documented [`Side`] set.
#[must_use]
#[inline]
pub fn side_unknown_bytes() -> &'static [u8] {
    SIDE_UNKNOWN
}

// ---------------------------------------------------------------------------
// MarketCategory
// ---------------------------------------------------------------------------

const MARKET_CATEGORY_VARIANTS: &[(u8, MarketCategory)] = &[
    (b'Q', MarketCategory::NasdaqGlobalSelect),
    (b'G', MarketCategory::NasdaqGlobalMarket),
    (b'S', MarketCategory::NasdaqCapitalMarket),
    (b'A', MarketCategory::NyseMkt),
    (b'N', MarketCategory::Nyse),
    (b'P', MarketCategory::NyseArca),
    (b'Z', MarketCategory::BatsZ),
    (b'V', MarketCategory::Iex),
    (b' ', MarketCategory::Unavailable),
];

// `MarketCategory` claims `0x20` (space) for `Unavailable`, so we
// pick a different printable that is NOT in the table.
const MARKET_CATEGORY_UNKNOWN: &[u8] = &[0x00, b'X', b'!', 0xFF];

/// Documented `(byte, variant)` table for [`MarketCategory`].
#[must_use]
#[inline]
pub fn market_category_variants() -> &'static [(u8, MarketCategory)] {
    MARKET_CATEGORY_VARIANTS
}

/// Bytes outside the documented [`MarketCategory`] set.
#[must_use]
#[inline]
pub fn market_category_unknown_bytes() -> &'static [u8] {
    MARKET_CATEGORY_UNKNOWN
}

// ---------------------------------------------------------------------------
// FinancialStatus
// ---------------------------------------------------------------------------

const FINANCIAL_STATUS_VARIANTS: &[(u8, FinancialStatus)] = &[
    (b'D', FinancialStatus::Deficient),
    (b'E', FinancialStatus::Delinquent),
    (b'Q', FinancialStatus::Bankrupt),
    (b'S', FinancialStatus::Suspended),
    (b'G', FinancialStatus::DeficientAndBankrupt),
    (b'H', FinancialStatus::DeficientAndDelinquent),
    (b'J', FinancialStatus::DelinquentAndBankrupt),
    (b'K', FinancialStatus::DeficientDelinquentBankrupt),
    (b'C', FinancialStatus::CreationsRedemptionsSuspended),
    (b'N', FinancialStatus::Normal),
    (b' ', FinancialStatus::Unavailable),
];

// `FinancialStatus` claims space; pick another printable.
const FINANCIAL_STATUS_UNKNOWN: &[u8] = &[0x00, b'X', b'?', 0xFF];

/// Documented `(byte, variant)` table for [`FinancialStatus`].
#[must_use]
#[inline]
pub fn financial_status_variants() -> &'static [(u8, FinancialStatus)] {
    FINANCIAL_STATUS_VARIANTS
}

/// Bytes outside the documented [`FinancialStatus`] set.
#[must_use]
#[inline]
pub fn financial_status_unknown_bytes() -> &'static [u8] {
    FINANCIAL_STATUS_UNKNOWN
}

// ---------------------------------------------------------------------------
// Authenticity
// ---------------------------------------------------------------------------

const AUTHENTICITY_VARIANTS: &[(u8, Authenticity)] =
    &[(b'P', Authenticity::Live), (b'T', Authenticity::Test)];

const AUTHENTICITY_UNKNOWN: &[u8] = &[0x00, b' ', b'X', 0xFF];

/// Documented `(byte, variant)` table for [`Authenticity`].
#[must_use]
#[inline]
pub fn authenticity_variants() -> &'static [(u8, Authenticity)] {
    AUTHENTICITY_VARIANTS
}

/// Bytes outside the documented [`Authenticity`] set.
#[must_use]
#[inline]
pub fn authenticity_unknown_bytes() -> &'static [u8] {
    AUTHENTICITY_UNKNOWN
}

// ---------------------------------------------------------------------------
// YesNo
// ---------------------------------------------------------------------------

const YES_NO_VARIANTS: &[(u8, YesNo)] = &[
    (b'Y', YesNo::Yes),
    (b'N', YesNo::No),
    (b' ', YesNo::Unavailable),
];

// `YesNo::Unavailable` is space; pick another printable.
const YES_NO_UNKNOWN: &[u8] = &[0x00, b'X', b'?', 0xFF];

/// Documented `(byte, variant)` table for [`YesNo`].
#[must_use]
#[inline]
pub fn yes_no_variants() -> &'static [(u8, YesNo)] {
    YES_NO_VARIANTS
}

/// Bytes outside the documented [`YesNo`] set.
#[must_use]
#[inline]
pub fn yes_no_unknown_bytes() -> &'static [u8] {
    YES_NO_UNKNOWN
}

// ---------------------------------------------------------------------------
// LuldTier
// ---------------------------------------------------------------------------

const LULD_TIER_VARIANTS: &[(u8, LuldTier)] = &[
    (b'1', LuldTier::Tier1),
    (b'2', LuldTier::Tier2),
    (b' ', LuldTier::Unavailable),
];

// `LuldTier::Unavailable` is space; pick a different printable.
const LULD_TIER_UNKNOWN: &[u8] = &[0x00, b'9', b'X', 0xFF];

/// Documented `(byte, variant)` table for [`LuldTier`].
#[must_use]
#[inline]
pub fn luld_tier_variants() -> &'static [(u8, LuldTier)] {
    LULD_TIER_VARIANTS
}

/// Bytes outside the documented [`LuldTier`] set.
#[must_use]
#[inline]
pub fn luld_tier_unknown_bytes() -> &'static [u8] {
    LULD_TIER_UNKNOWN
}

// ---------------------------------------------------------------------------
// TradingState
// ---------------------------------------------------------------------------

const TRADING_STATE_VARIANTS: &[(u8, TradingState)] = &[
    (b'H', TradingState::Halted),
    (b'P', TradingState::Paused),
    (b'Q', TradingState::QuotationOnly),
    (b'T', TradingState::Trading),
];

const TRADING_STATE_UNKNOWN: &[u8] = &[0x00, b' ', b'X', 0xFF];

/// Documented `(byte, variant)` table for [`TradingState`].
#[must_use]
#[inline]
pub fn trading_state_variants() -> &'static [(u8, TradingState)] {
    TRADING_STATE_VARIANTS
}

/// Bytes outside the documented [`TradingState`] set.
#[must_use]
#[inline]
pub fn trading_state_unknown_bytes() -> &'static [u8] {
    TRADING_STATE_UNKNOWN
}

// ---------------------------------------------------------------------------
// RegShoAction
// ---------------------------------------------------------------------------

const REG_SHO_ACTION_VARIANTS: &[(u8, RegShoAction)] = &[
    (b'0', RegShoAction::NoPriceTest),
    (b'1', RegShoAction::InEffectIntradayDrop),
    (b'2', RegShoAction::InEffect),
];

const REG_SHO_ACTION_UNKNOWN: &[u8] = &[0x00, b' ', b'9', 0xFF];

/// Documented `(byte, variant)` table for [`RegShoAction`].
#[must_use]
#[inline]
pub fn reg_sho_action_variants() -> &'static [(u8, RegShoAction)] {
    REG_SHO_ACTION_VARIANTS
}

/// Bytes outside the documented [`RegShoAction`] set.
#[must_use]
#[inline]
pub fn reg_sho_action_unknown_bytes() -> &'static [u8] {
    REG_SHO_ACTION_UNKNOWN
}

// ---------------------------------------------------------------------------
// MarketMakerMode
// ---------------------------------------------------------------------------

const MARKET_MAKER_MODE_VARIANTS: &[(u8, MarketMakerMode)] = &[
    (b'N', MarketMakerMode::Normal),
    (b'P', MarketMakerMode::Passive),
    (b'S', MarketMakerMode::Syndicate),
    (b'R', MarketMakerMode::PreSyndicate),
    (b'L', MarketMakerMode::Penalty),
];

const MARKET_MAKER_MODE_UNKNOWN: &[u8] = &[0x00, b' ', b'X', 0xFF];

/// Documented `(byte, variant)` table for [`MarketMakerMode`].
#[must_use]
#[inline]
pub fn market_maker_mode_variants() -> &'static [(u8, MarketMakerMode)] {
    MARKET_MAKER_MODE_VARIANTS
}

/// Bytes outside the documented [`MarketMakerMode`] set.
#[must_use]
#[inline]
pub fn market_maker_mode_unknown_bytes() -> &'static [u8] {
    MARKET_MAKER_MODE_UNKNOWN
}

// ---------------------------------------------------------------------------
// MarketParticipantState
// ---------------------------------------------------------------------------

const MARKET_PARTICIPANT_STATE_VARIANTS: &[(u8, MarketParticipantState)] = &[
    (b'A', MarketParticipantState::Active),
    (b'E', MarketParticipantState::ExcusedOrWithdrawn),
    (b'W', MarketParticipantState::Withdrawn),
    (b'S', MarketParticipantState::Suspended),
    (b'D', MarketParticipantState::Deleted),
];

const MARKET_PARTICIPANT_STATE_UNKNOWN: &[u8] = &[0x00, b' ', b'X', 0xFF];

/// Documented `(byte, variant)` table for [`MarketParticipantState`].
#[must_use]
#[inline]
pub fn market_participant_state_variants() -> &'static [(u8, MarketParticipantState)] {
    MARKET_PARTICIPANT_STATE_VARIANTS
}

/// Bytes outside the documented [`MarketParticipantState`] set.
#[must_use]
#[inline]
pub fn market_participant_state_unknown_bytes() -> &'static [u8] {
    MARKET_PARTICIPANT_STATE_UNKNOWN
}

// ---------------------------------------------------------------------------
// BreachedLevel
// ---------------------------------------------------------------------------

const BREACHED_LEVEL_VARIANTS: &[(u8, BreachedLevel)] = &[
    (b'1', BreachedLevel::Level1),
    (b'2', BreachedLevel::Level2),
    (b'3', BreachedLevel::Level3),
];

const BREACHED_LEVEL_UNKNOWN: &[u8] = &[0x00, b' ', b'9', 0xFF];

/// Documented `(byte, variant)` table for [`BreachedLevel`].
#[must_use]
#[inline]
pub fn breached_level_variants() -> &'static [(u8, BreachedLevel)] {
    BREACHED_LEVEL_VARIANTS
}

/// Bytes outside the documented [`BreachedLevel`] set.
#[must_use]
#[inline]
pub fn breached_level_unknown_bytes() -> &'static [u8] {
    BREACHED_LEVEL_UNKNOWN
}

// ---------------------------------------------------------------------------
// IpoReleaseQualifier
// ---------------------------------------------------------------------------

const IPO_RELEASE_QUALIFIER_VARIANTS: &[(u8, IpoReleaseQualifier)] = &[
    (b'A', IpoReleaseQualifier::Anticipated),
    (b'C', IpoReleaseQualifier::Cancelled),
];

const IPO_RELEASE_QUALIFIER_UNKNOWN: &[u8] = &[0x00, b' ', b'X', 0xFF];

/// Documented `(byte, variant)` table for [`IpoReleaseQualifier`].
#[must_use]
#[inline]
pub fn ipo_release_qualifier_variants() -> &'static [(u8, IpoReleaseQualifier)] {
    IPO_RELEASE_QUALIFIER_VARIANTS
}

/// Bytes outside the documented [`IpoReleaseQualifier`] set.
#[must_use]
#[inline]
pub fn ipo_release_qualifier_unknown_bytes() -> &'static [u8] {
    IPO_RELEASE_QUALIFIER_UNKNOWN
}

// ---------------------------------------------------------------------------
// Printable
// ---------------------------------------------------------------------------

const PRINTABLE_VARIANTS: &[(u8, Printable)] = &[
    (b'N', Printable::NonPrintable),
    (b'Y', Printable::Printable),
];

const PRINTABLE_UNKNOWN: &[u8] = &[0x00, b' ', b'X', 0xFF];

/// Documented `(byte, variant)` table for [`Printable`].
#[must_use]
#[inline]
pub fn printable_variants() -> &'static [(u8, Printable)] {
    PRINTABLE_VARIANTS
}

/// Bytes outside the documented [`Printable`] set.
#[must_use]
#[inline]
pub fn printable_unknown_bytes() -> &'static [u8] {
    PRINTABLE_UNKNOWN
}

// ---------------------------------------------------------------------------
// CrossType
// ---------------------------------------------------------------------------

const CROSS_TYPE_VARIANTS: &[(u8, CrossType)] = &[
    (b'O', CrossType::Opening),
    (b'C', CrossType::Closing),
    (b'H', CrossType::Halted),
    (b'I', CrossType::Intraday),
];

const CROSS_TYPE_UNKNOWN: &[u8] = &[0x00, b' ', b'X', 0xFF];

/// Documented `(byte, variant)` table for [`CrossType`].
#[must_use]
#[inline]
pub fn cross_type_variants() -> &'static [(u8, CrossType)] {
    CROSS_TYPE_VARIANTS
}

/// Bytes outside the documented [`CrossType`] set.
#[must_use]
#[inline]
pub fn cross_type_unknown_bytes() -> &'static [u8] {
    CROSS_TYPE_UNKNOWN
}

// ---------------------------------------------------------------------------
// ImbalanceDirection
// ---------------------------------------------------------------------------

const IMBALANCE_DIRECTION_VARIANTS: &[(u8, ImbalanceDirection)] = &[
    (b'B', ImbalanceDirection::Buy),
    (b'S', ImbalanceDirection::Sell),
    (b'N', ImbalanceDirection::NoImbalance),
    (b'O', ImbalanceDirection::InsufficientToCalc),
];

const IMBALANCE_DIRECTION_UNKNOWN: &[u8] = &[0x00, b' ', b'X', 0xFF];

/// Documented `(byte, variant)` table for [`ImbalanceDirection`].
#[must_use]
#[inline]
pub fn imbalance_direction_variants() -> &'static [(u8, ImbalanceDirection)] {
    IMBALANCE_DIRECTION_VARIANTS
}

/// Bytes outside the documented [`ImbalanceDirection`] set.
#[must_use]
#[inline]
pub fn imbalance_direction_unknown_bytes() -> &'static [u8] {
    IMBALANCE_DIRECTION_UNKNOWN
}

// ---------------------------------------------------------------------------
// PriceVariation
// ---------------------------------------------------------------------------

const PRICE_VARIATION_VARIANTS: &[(u8, PriceVariation)] = &[
    (b'L', PriceVariation::LessThan1Percent),
    (b'1', PriceVariation::Pct1To2),
    (b'2', PriceVariation::Pct2To3),
    (b'3', PriceVariation::Pct3To4),
    (b'4', PriceVariation::Pct4To5),
    (b'5', PriceVariation::Pct5To6),
    (b'6', PriceVariation::Pct6To7),
    (b'7', PriceVariation::Pct7To8),
    (b'8', PriceVariation::Pct8To9),
    (b'9', PriceVariation::Pct9To10),
    (b'A', PriceVariation::Pct10To20),
    (b'B', PriceVariation::Pct20To30),
    (b'C', PriceVariation::Pct30OrGreater),
    (b' ', PriceVariation::CannotBeCalculated),
];

// `PriceVariation::CannotBeCalculated` claims space; pick a different
// printable that is also not 'A'..'C' or '1'..'9'.
const PRICE_VARIATION_UNKNOWN: &[u8] = &[0x00, b'X', b'?', 0xFF];

/// Documented `(byte, variant)` table for [`PriceVariation`].
#[must_use]
#[inline]
pub fn price_variation_variants() -> &'static [(u8, PriceVariation)] {
    PRICE_VARIATION_VARIANTS
}

/// Bytes outside the documented [`PriceVariation`] set.
#[must_use]
#[inline]
pub fn price_variation_unknown_bytes() -> &'static [u8] {
    PRICE_VARIATION_UNKNOWN
}

// ---------------------------------------------------------------------------
// RpiInterestFlag
// ---------------------------------------------------------------------------

const RPI_INTEREST_FLAG_VARIANTS: &[(u8, RpiInterestFlag)] = &[
    (b'B', RpiInterestFlag::BuySide),
    (b'S', RpiInterestFlag::SellSide),
    (b'A', RpiInterestFlag::BothSides),
    (b'N', RpiInterestFlag::None),
];

const RPI_INTEREST_FLAG_UNKNOWN: &[u8] = &[0x00, b' ', b'X', 0xFF];

/// Documented `(byte, variant)` table for [`RpiInterestFlag`].
#[must_use]
#[inline]
pub fn rpi_interest_flag_variants() -> &'static [(u8, RpiInterestFlag)] {
    RPI_INTEREST_FLAG_VARIANTS
}

/// Bytes outside the documented [`RpiInterestFlag`] set.
#[must_use]
#[inline]
pub fn rpi_interest_flag_unknown_bytes() -> &'static [u8] {
    RPI_INTEREST_FLAG_UNKNOWN
}
