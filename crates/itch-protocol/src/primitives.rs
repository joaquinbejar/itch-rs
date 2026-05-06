//! Primitive newtypes for the ITCH 5.0 wire format.
//!
//! See `docs/DOMAIN-MODEL.md` §2 and `docs/PROTOCOL-SPEC.md` §1 for
//! the canonical type table and wire conventions.
//!
//! Every primitive is `#[repr(transparent)]` over its underlying
//! integer or fixed-size byte array, so the optimizer treats it as
//! the raw type at the machine level. Constructors validate where
//! the spec admits invalid values; accessors are `#[inline]` and
//! never allocate.

/// Daily-assigned per-symbol array index (2 bytes, big-endian on the
/// wire).
///
/// Identifier — derives `Hash` so it can be used directly as a map
/// key for per-symbol state.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct StockLocate(u16);

impl StockLocate {
    /// Wire size in bytes.
    pub const WIRE_LEN: usize = 2;

    /// Construct from the raw u16 (host-endian after wire decode).
    #[inline]
    #[must_use]
    pub const fn from_u16(v: u16) -> Self {
        Self(v)
    }

    /// Underlying u16 value.
    #[inline]
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

/// Opaque NASDAQ-internal id (2 bytes, big-endian on the wire).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct TrackingNumber(u16);

impl TrackingNumber {
    /// Wire size in bytes.
    pub const WIRE_LEN: usize = 2;

    /// Construct from the raw u16.
    #[inline]
    #[must_use]
    pub const fn from_u16(v: u16) -> Self {
        Self(v)
    }

    /// Underlying u16 value.
    #[inline]
    #[must_use]
    pub const fn as_u16(self) -> u16 {
        self.0
    }
}

/// Day-unique identifier of a resting order (8 bytes, big-endian).
///
/// Identifier — derives `Hash` so it can be used directly as a key in
/// the order index of an order-book reconstruction layer.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct OrderReference(u64);

impl OrderReference {
    /// Wire size in bytes.
    pub const WIRE_LEN: usize = 8;

    /// Construct from the raw u64.
    #[inline]
    #[must_use]
    pub const fn from_u64(v: u64) -> Self {
        Self(v)
    }

    /// Underlying u64 value.
    #[inline]
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Day-unique identifier of an execution (8 bytes, big-endian).
///
/// Identifier — derives `Hash` so executions can be looked up by
/// match number directly.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct MatchNumber(u64);

impl MatchNumber {
    /// Wire size in bytes.
    pub const WIRE_LEN: usize = 8;

    /// Construct from the raw u64.
    #[inline]
    #[must_use]
    pub const fn from_u64(v: u64) -> Self {
        Self(v)
    }

    /// Underlying u64 value.
    #[inline]
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// Quantity in shares (4 bytes, big-endian).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Shares(u32);

impl Shares {
    /// Wire size in bytes.
    pub const WIRE_LEN: usize = 4;

    /// Construct from the raw u32.
    #[inline]
    #[must_use]
    pub const fn from_u32(v: u32) -> Self {
        Self(v)
    }

    /// Underlying u32 value.
    #[inline]
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }
}

/// Nanoseconds since midnight Eastern Time (u48, 6 bytes on the
/// wire; stored in u64 with the high two bytes always zero).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Timestamp(u64);

/// Reasons a `Timestamp::try_new` can reject the input.
///
/// `#[non_exhaustive]` is intentional: future ITCH revisions or
/// stricter validation may add reject reasons; consumers must
/// match with a fallback arm.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TimestampError {
    /// Value exceeds the u48 range `[0, 2^48 - 1]`.
    OutOfRange {
        /// The rejected value.
        value: u64,
    },
}

impl core::fmt::Display for TimestampError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::OutOfRange { value } => {
                write!(f, "timestamp value {value} exceeds u48 range")
            }
        }
    }
}

impl std::error::Error for TimestampError {}

impl Timestamp {
    /// Wire size in bytes (u48 → 6 bytes).
    pub const WIRE_LEN: usize = 6;

    /// Maximum representable value (`2^48 - 1`).
    pub const MAX: u64 = (1u64 << 48) - 1;

    /// Construct without bounds-checking.
    ///
    /// Use this only when the value comes from a source that already
    /// validated the u48 range (e.g., the codec). For untrusted
    /// input, prefer [`Timestamp::try_new`].
    #[inline]
    #[must_use]
    pub const fn from_u64(v: u64) -> Self {
        Self(v)
    }

    /// Construct with bounds-checking against the u48 range.
    ///
    /// # Errors
    ///
    /// Returns [`TimestampError::OutOfRange`] when `v >= 1 << 48`.
    #[inline]
    #[must_use = "ignoring the Result will discard a possible OutOfRange error"]
    pub const fn try_new(v: u64) -> Result<Self, TimestampError> {
        if v > Self::MAX {
            Err(TimestampError::OutOfRange { value: v })
        } else {
            Ok(Self(v))
        }
    }

    /// Underlying u64 value (always within `[0, 2^48 - 1]`).
    #[inline]
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

/// 8-byte ASCII ticker symbol, right-padded with `0x20` (space).
///
/// Stored verbatim with padding — equality is byte-equality. The
/// codec encodes / decodes the full fixed-size field; padding is
/// preserved on the wire.
///
/// `Default` returns an all-space buffer (matching the spec's
/// padding byte), NOT all-zero, so a default-constructed `Stock`
/// represents "blank symbol" on the wire.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Stock([u8; Self::WIRE_LEN]);

impl Default for Stock {
    #[inline]
    fn default() -> Self {
        Self([b' '; Self::WIRE_LEN])
    }
}

impl Stock {
    /// Wire size in bytes.
    pub const WIRE_LEN: usize = 8;

    /// Construct from a string slice. Pads with `0x20` (space) if
    /// shorter than 8 bytes; truncates if longer. Never panics.
    #[inline]
    #[must_use]
    pub fn new(s: &str) -> Self {
        let mut buf = [b' '; Self::WIRE_LEN];
        let bytes = s.as_bytes();
        let n = bytes.len().min(Self::WIRE_LEN);
        buf[..n].copy_from_slice(&bytes[..n]);
        Self(buf)
    }

    /// Construct from raw bytes (no padding / validation).
    #[inline]
    #[must_use]
    pub const fn from_bytes(bytes: [u8; Self::WIRE_LEN]) -> Self {
        Self(bytes)
    }

    /// Underlying byte array.
    #[inline]
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::WIRE_LEN] {
        &self.0
    }

    /// Trimmed string view of the symbol. Strips trailing space
    /// padding (`0x20`) per spec — embedded spaces, if any, are
    /// preserved. Does NOT allocate.
    ///
    /// # Errors
    ///
    /// Returns [`core::str::Utf8Error`] if the bytes are not valid
    /// UTF-8.
    #[inline]
    pub fn as_str(&self) -> Result<&str, core::str::Utf8Error> {
        let trimmed_end = match self.0.iter().rposition(|&b| b != b' ') {
            Some(idx) => &self.0[..=idx],
            None => &self.0[..0],
        };
        core::str::from_utf8(trimmed_end)
    }
}

/// 4-byte ASCII broker code (MPID), right-padded with `0x20`.
///
/// `Default` returns an all-space buffer.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Mpid([u8; Self::WIRE_LEN]);

impl Default for Mpid {
    #[inline]
    fn default() -> Self {
        Self([b' '; Self::WIRE_LEN])
    }
}

impl Mpid {
    /// Wire size in bytes.
    pub const WIRE_LEN: usize = 4;

    /// Construct from a string slice. Pads with `0x20` if shorter
    /// than 4 bytes; truncates if longer. Never panics.
    #[inline]
    #[must_use]
    pub fn new(s: &str) -> Self {
        let mut buf = [b' '; Self::WIRE_LEN];
        let bytes = s.as_bytes();
        let n = bytes.len().min(Self::WIRE_LEN);
        buf[..n].copy_from_slice(&bytes[..n]);
        Self(buf)
    }

    /// Construct from raw bytes (no padding / validation).
    #[inline]
    #[must_use]
    pub const fn from_bytes(bytes: [u8; Self::WIRE_LEN]) -> Self {
        Self(bytes)
    }

    /// Underlying byte array.
    #[inline]
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; Self::WIRE_LEN] {
        &self.0
    }

    /// Trimmed string view. Strips trailing space padding (`0x20`)
    /// per spec — embedded spaces, if any, are preserved. Does NOT
    /// allocate.
    ///
    /// # Errors
    ///
    /// Returns [`core::str::Utf8Error`] if the bytes are not valid
    /// UTF-8.
    #[inline]
    pub fn as_str(&self) -> Result<&str, core::str::Utf8Error> {
        let trimmed_end = match self.0.iter().rposition(|&b| b != b' ') {
            Some(idx) => &self.0[..=idx],
            None => &self.0[..0],
        };
        core::str::from_utf8(trimmed_end)
    }
}

/// Fixed-point price with 4 decimal places (4 bytes u32 on the
/// wire; decimal value = `wire / 10_000`).
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Price4(u32);

impl Price4 {
    /// Wire size in bytes.
    pub const WIRE_LEN: usize = 4;

    /// Per-spec maximum price (`200_000.0000` → `2_000_000_000` in
    /// raw u32 units).
    pub const MAX_PER_SPEC: u32 = 2_000_000_000;

    /// Implicit scale factor (`10_000` for 4 decimal places).
    pub const SCALE: u32 = 10_000;

    /// Construct from the raw u32 (no validation; the codec uses
    /// this).
    #[inline]
    #[must_use]
    pub const fn from_u32(v: u32) -> Self {
        Self(v)
    }

    /// Underlying u32 value (in ten-thousandths of a dollar).
    #[inline]
    #[must_use]
    pub const fn as_u32(self) -> u32 {
        self.0
    }

    /// Construct from a `f64` decimal price (display / test
    /// convenience only — production code stays integer).
    #[inline]
    #[must_use]
    pub fn from_decimal(d: f64) -> Self {
        Self((d * f64::from(Self::SCALE)).round() as u32)
    }

    /// Decimal representation as `f64` (display / test only).
    #[inline]
    #[must_use]
    pub fn as_decimal(self) -> f64 {
        f64::from(self.0) / f64::from(Self::SCALE)
    }
}

/// Fixed-point price with 8 decimal places (8 bytes u64 on the
/// wire; decimal value = `wire / 10^8`). Used for MWCB level
/// prices.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Price8(u64);

impl Price8 {
    /// Wire size in bytes.
    pub const WIRE_LEN: usize = 8;

    /// Implicit scale factor (`10^8` for 8 decimal places).
    pub const SCALE: u64 = 100_000_000;

    /// Construct from the raw u64.
    #[inline]
    #[must_use]
    pub const fn from_u64(v: u64) -> Self {
        Self(v)
    }

    /// Underlying u64 value (in 10⁻⁸ units).
    #[inline]
    #[must_use]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Construct from a `f64` decimal price (display / test
    /// convenience only).
    #[inline]
    #[must_use]
    pub fn from_decimal(d: f64) -> Self {
        Self((d * (Self::SCALE as f64)).round() as u64)
    }

    /// Decimal representation as `f64` (display / test only).
    #[inline]
    #[must_use]
    pub fn as_decimal(self) -> f64 {
        (self.0 as f64) / (Self::SCALE as f64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stock_new_pads_short_input() {
        let s = Stock::new("AAPL");
        assert_eq!(s.as_bytes(), b"AAPL    ");
        assert_eq!(s.as_str().expect("utf-8"), "AAPL");
    }

    #[test]
    fn stock_new_truncates_long_input() {
        let s = Stock::new("LONGSYMBOL");
        assert_eq!(s.as_bytes(), b"LONGSYMB");
    }

    #[test]
    fn stock_new_exact_fit_no_padding() {
        let s = Stock::new("LONGSYMB");
        assert_eq!(s.as_bytes(), b"LONGSYMB");
        assert_eq!(s.as_str().expect("utf-8"), "LONGSYMB");
    }

    #[test]
    fn stock_byte_equality_preserves_padding() {
        // Stock stores verbatim; "AAPL" and "AAPL    " (already-padded
        // construction via from_bytes) produce equal values, while a
        // construction with a different padding pattern would not.
        assert_eq!(Stock::new("AAPL"), Stock::from_bytes(*b"AAPL    "));
    }

    #[test]
    fn stock_default_is_space_padded() {
        // Stock::default() returns the spec's space-padded blank
        // buffer (NOT all-zero, which would be the auto-derived
        // [u8; 8] default).
        let blank = Stock::default();
        assert_eq!(blank.as_bytes(), b"        ");
        assert_eq!(Stock::new(""), blank);
    }

    #[test]
    fn mpid_default_is_space_padded() {
        let blank = Mpid::default();
        assert_eq!(blank.as_bytes(), b"    ");
        assert_eq!(Mpid::new(""), blank);
    }

    #[test]
    fn stock_as_str_preserves_embedded_chars_strips_trailing_space() {
        // Trailing-space stripping only — embedded chars survive.
        let s = Stock::from_bytes(*b"AB CD   ");
        assert_eq!(s.as_str().expect("utf-8"), "AB CD");
    }

    #[test]
    fn stock_as_str_all_spaces_returns_empty() {
        let s = Stock::from_bytes(*b"        ");
        assert_eq!(s.as_str().expect("utf-8"), "");
    }

    #[test]
    fn mpid_new_pads_short_input() {
        let m = Mpid::new("CIT");
        assert_eq!(m.as_bytes(), b"CIT ");
        assert_eq!(m.as_str().expect("utf-8"), "CIT");
    }

    #[test]
    fn mpid_new_exact_fit() {
        let m = Mpid::new("NSDQ");
        assert_eq!(m.as_bytes(), b"NSDQ");
        assert_eq!(m.as_str().expect("utf-8"), "NSDQ");
    }

    #[test]
    fn mpid_truncates_overlong() {
        let m = Mpid::new("LONGER");
        assert_eq!(m.as_bytes(), b"LONG");
    }

    #[test]
    fn timestamp_round_trip_within_u48() {
        let v = 0x0000_1234_5678_9abc_u64;
        let ts = Timestamp::try_new(v).expect("within u48");
        assert_eq!(ts.as_u64(), v);
    }

    #[test]
    fn timestamp_max_is_u48_max() {
        assert_eq!(Timestamp::MAX, (1u64 << 48) - 1);
        assert!(Timestamp::try_new(Timestamp::MAX).is_ok());
    }

    #[test]
    fn timestamp_rejects_above_u48() {
        let v = 1u64 << 48;
        let err = Timestamp::try_new(v).expect_err("must reject");
        assert_eq!(err, TimestampError::OutOfRange { value: v });
    }

    #[test]
    fn price4_integer_roundtrip() {
        let raw = 1_927_184_u32; // 192.7184
        let p = Price4::from_u32(raw);
        assert_eq!(p.as_u32(), raw);
    }

    #[test]
    fn price4_decimal_smoketest() {
        let p = Price4::from_decimal(192.5000);
        assert_eq!(p.as_u32(), 1_925_000);
        assert!((p.as_decimal() - 192.5).abs() < 1e-9);
    }

    #[test]
    fn price8_integer_roundtrip() {
        let raw = 1_234_567_890_123_u64;
        let p = Price8::from_u64(raw);
        assert_eq!(p.as_u64(), raw);
    }

    #[test]
    fn order_reference_roundtrip() {
        let r = OrderReference::from_u64(1001);
        assert_eq!(r.as_u64(), 1001);
    }

    #[test]
    fn match_number_roundtrip() {
        let m = MatchNumber::from_u64(42);
        assert_eq!(m.as_u64(), 42);
    }

    #[test]
    fn stock_locate_roundtrip() {
        let s = StockLocate::from_u16(7);
        assert_eq!(s.as_u16(), 7);
    }

    #[test]
    fn tracking_number_roundtrip() {
        let t = TrackingNumber::from_u16(123);
        assert_eq!(t.as_u16(), 123);
    }

    #[test]
    fn shares_roundtrip() {
        let s = Shares::from_u32(500);
        assert_eq!(s.as_u32(), 500);
    }

    #[test]
    fn wire_lens_match_spec() {
        assert_eq!(StockLocate::WIRE_LEN, 2);
        assert_eq!(TrackingNumber::WIRE_LEN, 2);
        assert_eq!(OrderReference::WIRE_LEN, 8);
        assert_eq!(MatchNumber::WIRE_LEN, 8);
        assert_eq!(Shares::WIRE_LEN, 4);
        assert_eq!(Timestamp::WIRE_LEN, 6);
        assert_eq!(Stock::WIRE_LEN, 8);
        assert_eq!(Mpid::WIRE_LEN, 4);
        assert_eq!(Price4::WIRE_LEN, 4);
        assert_eq!(Price8::WIRE_LEN, 8);
    }
}
