//! Hand-rolled big-endian binary codec for ITCH 5.0 messages.
//!
//! Two traits — [`Encode`] and [`Decode`] — operate on the message
//! body (everything after the 1-byte type tag). [`Message::encode`]
//! and [`Message::decode`] add and consume the tag respectively, so
//! callers who already know the message kind don't pay the dispatch
//! cost.
//!
//! Big-endian once at the boundary: every multi-byte read / write
//! goes through `u16::from_be_bytes` / `to_be_bytes` etc. The u48
//! `Timestamp` wire form is decoded by copying 6 bytes into a
//! `[u8; 8]` and calling `u64::from_be_bytes`. No manual shift-and-OR.
//!
//! All decode failures are reported via [`ProtocolError`]
//! (`#[non_exhaustive]`).

use crate::enums::AlphaCoded;
use crate::error::ProtocolError;
use crate::messages::{
    AddOrder, AddOrderWithMpid, BrokenTrade, CrossTrade, Header, IpoQuotingPeriodUpdate,
    MarketParticipantPosition, Message, MwcbDeclineLevel, MwcbStatus, Noii, OrderCancel,
    OrderDelete, OrderExecuted, OrderExecutedWithPrice, OrderReplace, RegShoRestriction,
    RetailPriceImprovement, StockDirectory, StockTradingAction, SystemEvent, TradeNonCross,
};
use crate::primitives::{
    MatchNumber, Mpid, OrderReference, Price4, Price8, Shares, Stock, StockLocate, Timestamp,
    TrackingNumber,
};

// ---------------------------------------------------------------------------
// Public Encode / Decode traits
// ---------------------------------------------------------------------------

/// Encode self into the message body (after the 1-byte tag).
///
/// Implementors are expected to write exactly [`body_len`](Self::body_len)
/// bytes into `buf`. The caller is responsible for prepending the tag
/// when wrapping the body in a complete on-wire message; see
/// [`Message::encode`].
pub trait Encode {
    /// Body size in bytes (excludes the 1-byte tag).
    fn body_len(&self) -> usize;

    /// Write the body into `buf`. Returns
    /// [`ProtocolError::BufferTooSmall`] when `buf.len() < body_len()`.
    ///
    /// # Errors
    ///
    /// - [`ProtocolError::BufferTooSmall`] if `buf` is shorter than
    ///   the body's wire length.
    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError>;
}

/// Decode the message body (everything after the 1-byte tag).
pub trait Decode: Sized {
    /// Read a body from `buf`.
    ///
    /// # Errors
    ///
    /// - [`ProtocolError::Truncated`] if `buf` is shorter than the
    ///   body's wire length.
    /// - [`ProtocolError::InvalidEnumCode`] if a closed-set ASCII
    ///   field carries an unknown byte.
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError>;
}

// ---------------------------------------------------------------------------
// Big-endian byte helpers (#[inline] — proven hot per CLAUDE.md)
// ---------------------------------------------------------------------------

#[inline]
fn need(buf: &[u8], n: usize) -> Result<(), ProtocolError> {
    if buf.len() < n {
        return Err(ProtocolError::Truncated {
            need: n,
            got: buf.len(),
        });
    }
    Ok(())
}

/// Borrow `[off, off + n)` of `buf` or return `Truncated`.
///
/// Every read helper goes through this — the codec contains no
/// unchecked indexing, satisfying the "ZERO unchecked `[]`" rule
/// from `rules/global_rules.md`.
#[inline]
fn slice_n(buf: &[u8], off: usize, n: usize) -> Result<&[u8], ProtocolError> {
    let end = off.checked_add(n).ok_or(ProtocolError::Truncated {
        need: usize::MAX,
        got: buf.len(),
    })?;
    buf.get(off..end).ok_or(ProtocolError::Truncated {
        need: end,
        got: buf.len(),
    })
}

/// Mutably borrow `[off, off + n)` of `buf` or return
/// `BufferTooSmall` (used by every encode helper).
#[inline]
fn slice_n_mut(buf: &mut [u8], off: usize, n: usize) -> Result<&mut [u8], ProtocolError> {
    let end = off.checked_add(n).ok_or(ProtocolError::BufferTooSmall {
        need: usize::MAX,
        got: buf.len(),
    })?;
    let len = buf.len();
    buf.get_mut(off..end).ok_or(ProtocolError::BufferTooSmall {
        need: end,
        got: len,
    })
}

#[inline]
fn read_u8(buf: &[u8], off: usize) -> Result<u8, ProtocolError> {
    let s = slice_n(buf, off, 1)?;
    Ok(s[0])
}

#[inline]
fn read_u16(buf: &[u8], off: usize) -> Result<u16, ProtocolError> {
    let s = slice_n(buf, off, 2)?;
    Ok(u16::from_be_bytes([s[0], s[1]]))
}

#[inline]
fn read_u32(buf: &[u8], off: usize) -> Result<u32, ProtocolError> {
    let s = slice_n(buf, off, 4)?;
    Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

#[inline]
fn read_u64(buf: &[u8], off: usize) -> Result<u64, ProtocolError> {
    let s = slice_n(buf, off, 8)?;
    Ok(u64::from_be_bytes([
        s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
    ]))
}

/// Read a u48 (6-byte big-endian) by zero-padding the high two bytes
/// into a `[u8; 8]` and using `u64::from_be_bytes`.
#[inline]
fn read_u48(buf: &[u8], off: usize) -> Result<u64, ProtocolError> {
    let s = slice_n(buf, off, 6)?;
    let mut wide = [0u8; 8];
    wide[2..].copy_from_slice(s);
    Ok(u64::from_be_bytes(wide))
}

#[inline]
fn read_bytes_n<const N: usize>(buf: &[u8], off: usize) -> Result<[u8; N], ProtocolError> {
    let s = slice_n(buf, off, N)?;
    let mut out = [0u8; N];
    out.copy_from_slice(s);
    Ok(out)
}

#[inline]
fn write_u8(buf: &mut [u8], off: usize, v: u8) -> Result<(), ProtocolError> {
    let s = slice_n_mut(buf, off, 1)?;
    s[0] = v;
    Ok(())
}

#[inline]
fn write_u16(buf: &mut [u8], off: usize, v: u16) -> Result<(), ProtocolError> {
    let s = slice_n_mut(buf, off, 2)?;
    s.copy_from_slice(&v.to_be_bytes());
    Ok(())
}

#[inline]
fn write_u32(buf: &mut [u8], off: usize, v: u32) -> Result<(), ProtocolError> {
    let s = slice_n_mut(buf, off, 4)?;
    s.copy_from_slice(&v.to_be_bytes());
    Ok(())
}

#[inline]
fn write_u64(buf: &mut [u8], off: usize, v: u64) -> Result<(), ProtocolError> {
    let s = slice_n_mut(buf, off, 8)?;
    s.copy_from_slice(&v.to_be_bytes());
    Ok(())
}

/// Write the low 6 bytes of `v` (big-endian).
#[inline]
fn write_u48(buf: &mut [u8], off: usize, v: u64) -> Result<(), ProtocolError> {
    let s = slice_n_mut(buf, off, 6)?;
    let bytes = v.to_be_bytes();
    s.copy_from_slice(&bytes[2..8]);
    Ok(())
}

#[inline]
fn write_bytes(buf: &mut [u8], off: usize, src: &[u8]) -> Result<(), ProtocolError> {
    let s = slice_n_mut(buf, off, src.len())?;
    s.copy_from_slice(src);
    Ok(())
}

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

impl Header {
    /// Decode a 10-byte header from `buf` starting at offset 0.
    #[inline]
    fn decode_at(buf: &[u8]) -> Result<Self, ProtocolError> {
        Ok(Self {
            stock_locate: StockLocate::from_u16(read_u16(buf, 0)?),
            tracking_number: TrackingNumber::from_u16(read_u16(buf, 2)?),
            timestamp: Timestamp::from_u64(read_u48(buf, 4)?),
        })
    }

    /// Encode self at offset 0 of `buf`. Returns
    /// [`ProtocolError::BufferTooSmall`] when `buf.len() < 10`.
    #[inline]
    fn encode_at(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        write_u16(buf, 0, self.stock_locate.as_u16())?;
        write_u16(buf, 2, self.tracking_number.as_u16())?;
        write_u48(buf, 4, self.timestamp.as_u64())?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Per-message Encode / Decode
// ---------------------------------------------------------------------------

impl Encode for SystemEvent {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u8(buf, 10, self.event_code.to_byte())?;
        Ok(())
    }
}

impl Decode for SystemEvent {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            event_code: AlphaCoded::from_byte(read_u8(buf, 10)?)?,
        })
    }
}

impl Encode for StockDirectory {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_bytes(buf, 10, self.stock.as_bytes())?;
        write_u8(buf, 18, self.market_category.to_byte())?;
        write_u8(buf, 19, self.financial_status.to_byte())?;
        write_u32(buf, 20, self.round_lot_size.as_u32())?;
        write_u8(buf, 24, self.round_lots_only.to_byte())?;
        write_u8(buf, 25, self.issue_classification)?;
        write_bytes(buf, 26, &self.issue_subtype)?;
        write_u8(buf, 28, self.authenticity.to_byte())?;
        write_u8(buf, 29, self.short_sale_threshold.to_byte())?;
        write_u8(buf, 30, self.ipo_flag.to_byte())?;
        write_u8(buf, 31, self.luld_reference_price_tier.to_byte())?;
        write_u8(buf, 32, self.etp_flag.to_byte())?;
        write_u32(buf, 33, self.etp_leverage_factor)?;
        write_u8(buf, 37, self.inverse_indicator.to_byte())?;
        Ok(())
    }
}

impl Decode for StockDirectory {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            stock: Stock::from_bytes(read_bytes_n::<8>(buf, 10)?),
            market_category: AlphaCoded::from_byte(read_u8(buf, 18)?)?,
            financial_status: AlphaCoded::from_byte(read_u8(buf, 19)?)?,
            round_lot_size: Shares::from_u32(read_u32(buf, 20)?),
            round_lots_only: AlphaCoded::from_byte(read_u8(buf, 24)?)?,
            issue_classification: read_u8(buf, 25)?,
            issue_subtype: read_bytes_n::<2>(buf, 26)?,
            authenticity: AlphaCoded::from_byte(read_u8(buf, 28)?)?,
            short_sale_threshold: AlphaCoded::from_byte(read_u8(buf, 29)?)?,
            ipo_flag: AlphaCoded::from_byte(read_u8(buf, 30)?)?,
            luld_reference_price_tier: AlphaCoded::from_byte(read_u8(buf, 31)?)?,
            etp_flag: AlphaCoded::from_byte(read_u8(buf, 32)?)?,
            etp_leverage_factor: read_u32(buf, 33)?,
            inverse_indicator: AlphaCoded::from_byte(read_u8(buf, 37)?)?,
        })
    }
}

impl Encode for StockTradingAction {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_bytes(buf, 10, self.stock.as_bytes())?;
        write_u8(buf, 18, self.trading_state.to_byte())?;
        write_u8(buf, 19, self.reserved)?;
        write_bytes(buf, 20, &self.reason)?;
        Ok(())
    }
}

impl Decode for StockTradingAction {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            stock: Stock::from_bytes(read_bytes_n::<8>(buf, 10)?),
            trading_state: AlphaCoded::from_byte(read_u8(buf, 18)?)?,
            reserved: read_u8(buf, 19)?,
            reason: read_bytes_n::<4>(buf, 20)?,
        })
    }
}

impl Encode for RegShoRestriction {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_bytes(buf, 10, self.stock.as_bytes())?;
        write_u8(buf, 18, self.reg_sho_action.to_byte())?;
        Ok(())
    }
}

impl Decode for RegShoRestriction {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            stock: Stock::from_bytes(read_bytes_n::<8>(buf, 10)?),
            reg_sho_action: AlphaCoded::from_byte(read_u8(buf, 18)?)?,
        })
    }
}

impl Encode for MarketParticipantPosition {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_bytes(buf, 10, self.mpid.as_bytes())?;
        write_bytes(buf, 14, self.stock.as_bytes())?;
        write_u8(buf, 22, self.primary_market_maker.to_byte())?;
        write_u8(buf, 23, self.market_maker_mode.to_byte())?;
        write_u8(buf, 24, self.market_participant_state.to_byte())?;
        Ok(())
    }
}

impl Decode for MarketParticipantPosition {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            mpid: Mpid::from_bytes(read_bytes_n::<4>(buf, 10)?),
            stock: Stock::from_bytes(read_bytes_n::<8>(buf, 14)?),
            primary_market_maker: AlphaCoded::from_byte(read_u8(buf, 22)?)?,
            market_maker_mode: AlphaCoded::from_byte(read_u8(buf, 23)?)?,
            market_participant_state: AlphaCoded::from_byte(read_u8(buf, 24)?)?,
        })
    }
}

impl Encode for MwcbDeclineLevel {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.level1.as_u64())?;
        write_u64(buf, 18, self.level2.as_u64())?;
        write_u64(buf, 26, self.level3.as_u64())?;
        Ok(())
    }
}

impl Decode for MwcbDeclineLevel {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            level1: Price8::from_u64(read_u64(buf, 10)?),
            level2: Price8::from_u64(read_u64(buf, 18)?),
            level3: Price8::from_u64(read_u64(buf, 26)?),
        })
    }
}

impl Encode for MwcbStatus {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u8(buf, 10, self.breached_level.to_byte())?;
        Ok(())
    }
}

impl Decode for MwcbStatus {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            breached_level: AlphaCoded::from_byte(read_u8(buf, 10)?)?,
        })
    }
}

impl Encode for IpoQuotingPeriodUpdate {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_bytes(buf, 10, self.stock.as_bytes())?;
        write_u32(buf, 18, self.ipo_quotation_release_time)?;
        write_u8(buf, 22, self.ipo_quotation_release_qualifier.to_byte())?;
        write_u32(buf, 23, self.ipo_price.as_u32())?;
        Ok(())
    }
}

impl Decode for IpoQuotingPeriodUpdate {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            stock: Stock::from_bytes(read_bytes_n::<8>(buf, 10)?),
            ipo_quotation_release_time: read_u32(buf, 18)?,
            ipo_quotation_release_qualifier: AlphaCoded::from_byte(read_u8(buf, 22)?)?,
            ipo_price: Price4::from_u32(read_u32(buf, 23)?),
        })
    }
}

impl Encode for AddOrder {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.order_ref.as_u64())?;
        write_u8(buf, 18, self.side.to_byte())?;
        write_u32(buf, 19, self.shares.as_u32())?;
        write_bytes(buf, 23, self.stock.as_bytes())?;
        write_u32(buf, 31, self.price.as_u32())?;
        Ok(())
    }
}

impl Decode for AddOrder {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            order_ref: OrderReference::from_u64(read_u64(buf, 10)?),
            side: AlphaCoded::from_byte(read_u8(buf, 18)?)?,
            shares: Shares::from_u32(read_u32(buf, 19)?),
            stock: Stock::from_bytes(read_bytes_n::<8>(buf, 23)?),
            price: Price4::from_u32(read_u32(buf, 31)?),
        })
    }
}

impl Encode for AddOrderWithMpid {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.order_ref.as_u64())?;
        write_u8(buf, 18, self.side.to_byte())?;
        write_u32(buf, 19, self.shares.as_u32())?;
        write_bytes(buf, 23, self.stock.as_bytes())?;
        write_u32(buf, 31, self.price.as_u32())?;
        write_bytes(buf, 35, self.attribution.as_bytes())?;
        Ok(())
    }
}

impl Decode for AddOrderWithMpid {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            order_ref: OrderReference::from_u64(read_u64(buf, 10)?),
            side: AlphaCoded::from_byte(read_u8(buf, 18)?)?,
            shares: Shares::from_u32(read_u32(buf, 19)?),
            stock: Stock::from_bytes(read_bytes_n::<8>(buf, 23)?),
            price: Price4::from_u32(read_u32(buf, 31)?),
            attribution: Mpid::from_bytes(read_bytes_n::<4>(buf, 35)?),
        })
    }
}

impl Encode for OrderExecuted {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.order_ref.as_u64())?;
        write_u32(buf, 18, self.executed_shares.as_u32())?;
        write_u64(buf, 22, self.match_number.as_u64())?;
        Ok(())
    }
}

impl Decode for OrderExecuted {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            order_ref: OrderReference::from_u64(read_u64(buf, 10)?),
            executed_shares: Shares::from_u32(read_u32(buf, 18)?),
            match_number: MatchNumber::from_u64(read_u64(buf, 22)?),
        })
    }
}

impl Encode for OrderExecutedWithPrice {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.order_ref.as_u64())?;
        write_u32(buf, 18, self.executed_shares.as_u32())?;
        write_u64(buf, 22, self.match_number.as_u64())?;
        write_u8(buf, 30, self.printable.to_byte())?;
        write_u32(buf, 31, self.execution_price.as_u32())?;
        Ok(())
    }
}

impl Decode for OrderExecutedWithPrice {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            order_ref: OrderReference::from_u64(read_u64(buf, 10)?),
            executed_shares: Shares::from_u32(read_u32(buf, 18)?),
            match_number: MatchNumber::from_u64(read_u64(buf, 22)?),
            printable: AlphaCoded::from_byte(read_u8(buf, 30)?)?,
            execution_price: Price4::from_u32(read_u32(buf, 31)?),
        })
    }
}

impl Encode for OrderCancel {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.order_ref.as_u64())?;
        write_u32(buf, 18, self.cancelled_shares.as_u32())?;
        Ok(())
    }
}

impl Decode for OrderCancel {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            order_ref: OrderReference::from_u64(read_u64(buf, 10)?),
            cancelled_shares: Shares::from_u32(read_u32(buf, 18)?),
        })
    }
}

impl Encode for OrderDelete {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.order_ref.as_u64())?;
        Ok(())
    }
}

impl Decode for OrderDelete {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            order_ref: OrderReference::from_u64(read_u64(buf, 10)?),
        })
    }
}

impl Encode for OrderReplace {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.original_order_ref.as_u64())?;
        write_u64(buf, 18, self.new_order_ref.as_u64())?;
        write_u32(buf, 26, self.shares.as_u32())?;
        write_u32(buf, 30, self.price.as_u32())?;
        Ok(())
    }
}

impl Decode for OrderReplace {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            original_order_ref: OrderReference::from_u64(read_u64(buf, 10)?),
            new_order_ref: OrderReference::from_u64(read_u64(buf, 18)?),
            shares: Shares::from_u32(read_u32(buf, 26)?),
            price: Price4::from_u32(read_u32(buf, 30)?),
        })
    }
}

impl Encode for TradeNonCross {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.order_ref.as_u64())?;
        write_u8(buf, 18, self.side.to_byte())?;
        write_u32(buf, 19, self.shares.as_u32())?;
        write_bytes(buf, 23, self.stock.as_bytes())?;
        write_u32(buf, 31, self.price.as_u32())?;
        write_u64(buf, 35, self.match_number.as_u64())?;
        Ok(())
    }
}

impl Decode for TradeNonCross {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            order_ref: OrderReference::from_u64(read_u64(buf, 10)?),
            side: AlphaCoded::from_byte(read_u8(buf, 18)?)?,
            shares: Shares::from_u32(read_u32(buf, 19)?),
            stock: Stock::from_bytes(read_bytes_n::<8>(buf, 23)?),
            price: Price4::from_u32(read_u32(buf, 31)?),
            match_number: MatchNumber::from_u64(read_u64(buf, 35)?),
        })
    }
}

impl Encode for CrossTrade {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.shares)?;
        write_bytes(buf, 18, self.stock.as_bytes())?;
        write_u32(buf, 26, self.cross_price.as_u32())?;
        write_u64(buf, 30, self.match_number.as_u64())?;
        write_u8(buf, 38, self.cross_type.to_byte())?;
        Ok(())
    }
}

impl Decode for CrossTrade {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            shares: read_u64(buf, 10)?,
            stock: Stock::from_bytes(read_bytes_n::<8>(buf, 18)?),
            cross_price: Price4::from_u32(read_u32(buf, 26)?),
            match_number: MatchNumber::from_u64(read_u64(buf, 30)?),
            cross_type: AlphaCoded::from_byte(read_u8(buf, 38)?)?,
        })
    }
}

impl Encode for BrokenTrade {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.match_number.as_u64())?;
        Ok(())
    }
}

impl Decode for BrokenTrade {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            match_number: MatchNumber::from_u64(read_u64(buf, 10)?),
        })
    }
}

impl Encode for Noii {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_u64(buf, 10, self.paired_shares)?;
        write_u64(buf, 18, self.imbalance_shares)?;
        write_u8(buf, 26, self.imbalance_direction.to_byte())?;
        write_bytes(buf, 27, self.stock.as_bytes())?;
        write_u32(buf, 35, self.far_price.as_u32())?;
        write_u32(buf, 39, self.near_price.as_u32())?;
        write_u32(buf, 43, self.current_reference_price.as_u32())?;
        write_u8(buf, 47, self.cross_type.to_byte())?;
        write_u8(buf, 48, self.price_variation.to_byte())?;
        Ok(())
    }
}

impl Decode for Noii {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            paired_shares: read_u64(buf, 10)?,
            imbalance_shares: read_u64(buf, 18)?,
            imbalance_direction: AlphaCoded::from_byte(read_u8(buf, 26)?)?,
            stock: Stock::from_bytes(read_bytes_n::<8>(buf, 27)?),
            far_price: Price4::from_u32(read_u32(buf, 35)?),
            near_price: Price4::from_u32(read_u32(buf, 39)?),
            current_reference_price: Price4::from_u32(read_u32(buf, 43)?),
            cross_type: AlphaCoded::from_byte(read_u8(buf, 47)?)?,
            price_variation: AlphaCoded::from_byte(read_u8(buf, 48)?)?,
        })
    }
}

impl Encode for RetailPriceImprovement {
    #[inline]
    fn body_len(&self) -> usize {
        Self::BODY_LEN
    }

    fn encode_body(&self, buf: &mut [u8]) -> Result<(), ProtocolError> {
        let _ = slice_n_mut(buf, 0, Self::BODY_LEN)?;
        self.header.encode_at(buf)?;
        write_bytes(buf, 10, self.stock.as_bytes())?;
        write_u8(buf, 18, self.interest_flag.to_byte())?;
        Ok(())
    }
}

impl Decode for RetailPriceImprovement {
    fn decode_body(buf: &[u8]) -> Result<Self, ProtocolError> {
        need(buf, Self::BODY_LEN)?;
        Ok(Self {
            header: Header::decode_at(buf)?,
            stock: Stock::from_bytes(read_bytes_n::<8>(buf, 10)?),
            interest_flag: AlphaCoded::from_byte(read_u8(buf, 18)?)?,
        })
    }
}

// ---------------------------------------------------------------------------
// Top-level Message::encode / decode (handles the 1-byte tag)
// ---------------------------------------------------------------------------

impl Message {
    /// Encode self into `buf`, prepending the 1-byte tag. Returns the
    /// total number of bytes written (= [`encoded_len`](Self::encoded_len)).
    ///
    /// # Errors
    ///
    /// - [`ProtocolError::BufferTooSmall`] when `buf.len() <
    ///   self.encoded_len()`.
    pub fn encode(&self, buf: &mut [u8]) -> Result<usize, ProtocolError> {
        let total = self.encoded_len();
        let out = slice_n_mut(buf, 0, total)?;
        out[0] = self.tag();
        let body = &mut out[1..total];
        match self {
            Self::SystemEvent(m) => m.encode_body(body)?,
            Self::StockDirectory(m) => m.encode_body(body)?,
            Self::StockTradingAction(m) => m.encode_body(body)?,
            Self::RegShoRestriction(m) => m.encode_body(body)?,
            Self::MarketParticipantPosition(m) => m.encode_body(body)?,
            Self::MwcbDeclineLevel(m) => m.encode_body(body)?,
            Self::MwcbStatus(m) => m.encode_body(body)?,
            Self::IpoQuotingPeriodUpdate(m) => m.encode_body(body)?,
            Self::AddOrder(m) => m.encode_body(body)?,
            Self::AddOrderWithMpid(m) => m.encode_body(body)?,
            Self::OrderExecuted(m) => m.encode_body(body)?,
            Self::OrderExecutedWithPrice(m) => m.encode_body(body)?,
            Self::OrderCancel(m) => m.encode_body(body)?,
            Self::OrderDelete(m) => m.encode_body(body)?,
            Self::OrderReplace(m) => m.encode_body(body)?,
            Self::TradeNonCross(m) => m.encode_body(body)?,
            Self::CrossTrade(m) => m.encode_body(body)?,
            Self::BrokenTrade(m) => m.encode_body(body)?,
            Self::Noii(m) => m.encode_body(body)?,
            Self::RetailPriceImprovement(m) => m.encode_body(body)?,
        }
        Ok(total)
    }

    /// Decode the next message from `buf`.
    ///
    /// # Errors
    ///
    /// - [`ProtocolError::Truncated`] if `buf` is empty or shorter than
    ///   the body announced by the tag.
    /// - [`ProtocolError::UnknownMessageType`] if the tag byte does
    ///   not match any of the 20 ITCH 5.0 kinds.
    /// - [`ProtocolError::InvalidEnumCode`] if a closed-set ASCII
    ///   field within the body carries an unknown byte.
    pub fn decode(buf: &[u8]) -> Result<Self, ProtocolError> {
        let tag = *buf
            .first()
            .ok_or(ProtocolError::Truncated { need: 1, got: 0 })?;
        let body = &buf[1..];
        Ok(match tag {
            b'S' => Self::SystemEvent(SystemEvent::decode_body(body)?),
            b'R' => Self::StockDirectory(StockDirectory::decode_body(body)?),
            b'H' => Self::StockTradingAction(StockTradingAction::decode_body(body)?),
            b'Y' => Self::RegShoRestriction(RegShoRestriction::decode_body(body)?),
            b'L' => Self::MarketParticipantPosition(MarketParticipantPosition::decode_body(body)?),
            b'V' => Self::MwcbDeclineLevel(MwcbDeclineLevel::decode_body(body)?),
            b'W' => Self::MwcbStatus(MwcbStatus::decode_body(body)?),
            b'K' => Self::IpoQuotingPeriodUpdate(IpoQuotingPeriodUpdate::decode_body(body)?),
            b'A' => Self::AddOrder(AddOrder::decode_body(body)?),
            b'F' => Self::AddOrderWithMpid(AddOrderWithMpid::decode_body(body)?),
            b'E' => Self::OrderExecuted(OrderExecuted::decode_body(body)?),
            b'C' => Self::OrderExecutedWithPrice(OrderExecutedWithPrice::decode_body(body)?),
            b'X' => Self::OrderCancel(OrderCancel::decode_body(body)?),
            b'D' => Self::OrderDelete(OrderDelete::decode_body(body)?),
            b'U' => Self::OrderReplace(OrderReplace::decode_body(body)?),
            b'P' => Self::TradeNonCross(TradeNonCross::decode_body(body)?),
            b'Q' => Self::CrossTrade(CrossTrade::decode_body(body)?),
            b'B' => Self::BrokenTrade(BrokenTrade::decode_body(body)?),
            b'I' => Self::Noii(Noii::decode_body(body)?),
            b'N' => Self::RetailPriceImprovement(RetailPriceImprovement::decode_body(body)?),
            other => return Err(ProtocolError::UnknownMessageType(other)),
        })
    }
}

// ---------------------------------------------------------------------------
// Tests — single-shot smoke for one message kind. Full per-kind
// roundtrips live in tests/roundtrip.rs (issue #7).
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::enums::Side;

    fn add_order_fixture() -> AddOrder {
        AddOrder {
            header: Header {
                stock_locate: StockLocate::from_u16(7),
                tracking_number: TrackingNumber::from_u16(13),
                timestamp: Timestamp::from_u64(0x1234_5678_9abc),
            },
            order_ref: OrderReference::from_u64(1001),
            side: Side::Buy,
            shares: Shares::from_u32(500),
            stock: Stock::new("AAPL"),
            price: Price4::from_u32(1_925_000),
        }
    }

    #[test]
    fn add_order_round_trip_via_message_encode_decode() {
        let m = Message::AddOrder(add_order_fixture());
        let mut buf = [0u8; 64];
        let n = m.encode(&mut buf).expect("encode");
        assert_eq!(n, AddOrder::BODY_LEN + 1);
        let decoded = Message::decode(&buf[..n]).expect("decode");
        assert_eq!(decoded, m);
    }

    #[test]
    fn decode_unknown_tag_returns_unknown_message_type() {
        let bytes = [b'Z', 0u8, 0u8, 0u8, 0u8];
        match Message::decode(&bytes) {
            Err(ProtocolError::UnknownMessageType(b)) => assert_eq!(b, b'Z'),
            other => panic!("expected UnknownMessageType, got {other:?}"),
        }
    }

    #[test]
    fn decode_empty_buffer_returns_truncated() {
        match Message::decode(&[]) {
            Err(ProtocolError::Truncated { need: 1, got: 0 }) => (),
            other => panic!("expected Truncated, got {other:?}"),
        }
    }

    #[test]
    fn decode_truncated_body_returns_truncated() {
        // Tag 'A' (AddOrder) but only 5 bytes of body — needs 35.
        let bytes = [b'A', 0u8, 0u8, 0u8, 0u8, 0u8];
        match Message::decode(&bytes) {
            Err(ProtocolError::Truncated { need, got }) => {
                assert_eq!(need, AddOrder::BODY_LEN);
                assert_eq!(got, 5);
            }
            other => panic!("expected Truncated, got {other:?}"),
        }
    }

    #[test]
    fn encode_into_too_small_buffer_returns_buffer_too_small() {
        let m = Message::AddOrder(add_order_fixture());
        let mut buf = [0u8; 10];
        match m.encode(&mut buf) {
            Err(ProtocolError::BufferTooSmall { need, got }) => {
                assert_eq!(need, m.encoded_len());
                assert_eq!(got, 10);
            }
            other => panic!("expected BufferTooSmall, got {other:?}"),
        }
    }

    #[test]
    fn decode_invalid_enum_code_returns_typed_error() {
        // Build a System Event body with a bogus event_code byte 'Z'.
        let mut buf = [0u8; 12];
        buf[0] = b'S'; // tag
                       // header: 10 zero bytes
        buf[11] = b'Z'; // event_code = unknown
        match Message::decode(&buf) {
            Err(ProtocolError::InvalidEnumCode { field, code }) => {
                assert_eq!(field, "EventCode");
                assert_eq!(code, b'Z');
            }
            other => panic!("expected InvalidEnumCode, got {other:?}"),
        }
    }
}
