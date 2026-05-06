//! Property-based invariants for `itch-protocol`.
//!
//! See `docs/TESTING.md` §3 and `.claude/skills/proptest-invariant`.
//! Catches the bugs roundtrip + golden tests miss: ambiguous decoding,
//! panics on malformed input, lossy fixed-point conversions, padding
//! drift on `Stock` / `Mpid`.

#![forbid(unsafe_code)]

mod common;

use common::*;
use itch_protocol::{
    AlphaCoded, Authenticity, BreachedLevel, CrossType, EventCode, FinancialStatus,
    ImbalanceDirection, IpoReleaseQualifier, LuldTier, MarketCategory, MarketMakerMode,
    MarketParticipantState, Message, Mpid, Price4, Price8, PriceVariation, Printable,
    ProtocolError, RegShoAction, RpiInterestFlag, Side, Stock, TradingState, YesNo,
};
use proptest::prelude::*;

// ---------- helpers ----------

/// Encode `m` into a fresh `Vec<u8>` of exactly `m.encoded_len()`
/// bytes; returns the buffer.
fn encode_to_vec(m: &Message) -> Vec<u8> {
    let mut buf = vec![0u8; m.encoded_len()];
    let n = m.encode(&mut buf).expect("encode");
    assert_eq!(
        n,
        buf.len(),
        "encode wrote a different size than encoded_len"
    );
    buf
}

// ---------- per-test config ----------
//
// 1024 cases per invariant is the project default per
// `docs/TESTING.md` §3. Enums and primitives shrink fast so we cap
// shrinker iterations to avoid pathological reductions.
const PROP_CASES: u32 = 1024;
const PROP_MAX_SHRINK: u32 = 50_000;

proptest! {
    #![proptest_config(ProptestConfig {
        cases: PROP_CASES,
        max_shrink_iters: PROP_MAX_SHRINK,
        ..ProptestConfig::default()
    })]

    /// Encode → decode is a left-inverse: decoding the wire bytes
    /// produced by `encode()` returns the original `Message`.
    #[test]
    fn encode_decode_identity(m in any_message()) {
        let buf = encode_to_vec(&m);
        let decoded = Message::decode(&buf).expect("decode");
        prop_assert_eq!(decoded, m);
    }

    /// Decode → encode is a right-inverse: re-encoding a successfully
    /// decoded `Message` reproduces the original wire bytes byte-for-
    /// byte. This catches the case where two distinct DTO field
    /// shapes encode to the same bytes (ambiguous decoding).
    #[test]
    fn decode_encode_identity(m in any_message()) {
        let original = encode_to_vec(&m);
        let decoded = Message::decode(&original).expect("decode");
        let reencoded = encode_to_vec(&decoded);
        prop_assert_eq!(reencoded, original);
    }

    /// `Message::decode` must never panic on arbitrary input. It can
    /// return `Ok(_)` (interpretation as a valid message) or
    /// `Err(ProtocolError::*)`, but not abort the process.
    #[test]
    fn decode_no_panic(bytes in proptest::collection::vec(any::<u8>(), 0..2048)) {
        // Just calling decode is enough: a panic aborts the test.
        let _ = Message::decode(&bytes);
    }

    /// `Stock::from_bytes(bs).as_bytes() == bs` for every 8-byte
    /// ASCII printable input (round-trip identity, padding preserved).
    #[test]
    fn stock_roundtrip(bs in proptest::array::uniform8(0x20u8..0x7Fu8)) {
        let s = Stock::from_bytes(bs);
        prop_assert_eq!(s.as_bytes(), &bs);
    }

    /// `Mpid::from_bytes(bs).as_bytes() == bs` for every 4-byte
    /// ASCII printable input.
    #[test]
    fn mpid_roundtrip(bs in proptest::array::uniform4(0x20u8..0x7Fu8)) {
        let m = Mpid::from_bytes(bs);
        prop_assert_eq!(m.as_bytes(), &bs);
    }

    /// `Price4::from_u32(v).as_u32() == v` — fixed-point integer
    /// conversion is lossless across the full `u32` range.
    #[test]
    fn price4_lossless_integer(v in any::<u32>()) {
        let p = Price4::from_u32(v);
        prop_assert_eq!(p.as_u32(), v);
    }

    /// `Price8::from_u64(v).as_u64() == v` — same property over u64.
    #[test]
    fn price8_lossless_integer(v in any::<u64>()) {
        let p = Price8::from_u64(v);
        prop_assert_eq!(p.as_u64(), v);
    }

    /// Header field roundtrips losslessly. Verified end-to-end by
    /// embedding in the smallest Message variant (SystemEvent =
    /// header + 1-byte event_code) and asserting the decoded header
    /// matches the original.
    #[test]
    fn header_roundtrip(h in any_header()) {
        use itch_protocol::messages::SystemEvent;
        let m = Message::SystemEvent(SystemEvent {
            header: h,
            event_code: EventCode::StartOfMessages,
        });
        let buf = encode_to_vec(&m);
        let decoded = Message::decode(&buf).expect("decode");
        prop_assert_eq!(decoded.header(), &h);
    }

    /// Encoding the full `m.encoded_len()` then truncating to any
    /// shorter length must yield `Truncated { .. }` from
    /// `Message::decode`. No panic, no `Ok(_)`, no other error
    /// variant.
    #[test]
    fn truncation_returns_error(m in any_message()) {
        let full = encode_to_vec(&m);
        // Limit the truncation sweep so the proptest stays fast on
        // large messages. We sample 8 cuts evenly across the
        // buffer; one of them is always 0 (empty). Per-message
        // exhaustive truncation is covered by the unit tests in
        // `roundtrip.rs`.
        let total = full.len();
        let cuts: Vec<usize> = (0..8).map(|i| (total * i) / 8).collect();
        for n in cuts {
            if n == total {
                continue; // The full length is the happy path; skip.
            }
            match Message::decode(&full[..n]) {
                Err(ProtocolError::Truncated { .. }) => {}
                Err(other) => prop_assert!(
                    false,
                    "expected Truncated for prefix of len {n} of {total}, got {other:?}",
                ),
                Ok(_) => prop_assert!(
                    false,
                    "decoder accepted truncated bytes (len {n} of {total})",
                ),
            }
        }
    }
}

// ---------- enum byte roundtrips (one block per enum) ----------

macro_rules! enum_byte_roundtrip_test {
    ($name:ident, $strategy:expr, $enum:ident) => {
        proptest! {
            #![proptest_config(ProptestConfig {
                cases: 256,
                ..ProptestConfig::default()
            })]
            #[test]
            fn $name(value in $strategy) {
                let byte = value.to_byte();
                let decoded = $enum::from_byte(byte).expect("from_byte");
                prop_assert_eq!(decoded, value);
            }
        }
    };
}

enum_byte_roundtrip_test!(event_code_byte_roundtrip, any_event_code(), EventCode);
enum_byte_roundtrip_test!(
    market_category_byte_roundtrip,
    any_market_category(),
    MarketCategory
);
enum_byte_roundtrip_test!(
    financial_status_byte_roundtrip,
    any_financial_status(),
    FinancialStatus
);
enum_byte_roundtrip_test!(
    authenticity_byte_roundtrip,
    any_authenticity(),
    Authenticity
);
enum_byte_roundtrip_test!(yes_no_byte_roundtrip, any_yes_no(), YesNo);
enum_byte_roundtrip_test!(luld_tier_byte_roundtrip, any_luld_tier(), LuldTier);
enum_byte_roundtrip_test!(
    trading_state_byte_roundtrip,
    any_trading_state(),
    TradingState
);
enum_byte_roundtrip_test!(
    reg_sho_action_byte_roundtrip,
    any_reg_sho_action(),
    RegShoAction
);
enum_byte_roundtrip_test!(
    market_maker_mode_byte_roundtrip,
    any_market_maker_mode(),
    MarketMakerMode
);
enum_byte_roundtrip_test!(
    market_participant_state_byte_roundtrip,
    any_market_participant_state(),
    MarketParticipantState
);
enum_byte_roundtrip_test!(
    breached_level_byte_roundtrip,
    any_breached_level(),
    BreachedLevel
);
enum_byte_roundtrip_test!(
    ipo_release_qualifier_byte_roundtrip,
    any_ipo_release_qualifier(),
    IpoReleaseQualifier
);
enum_byte_roundtrip_test!(side_byte_roundtrip, any_side(), Side);
enum_byte_roundtrip_test!(printable_byte_roundtrip, any_printable(), Printable);
enum_byte_roundtrip_test!(cross_type_byte_roundtrip, any_cross_type(), CrossType);
enum_byte_roundtrip_test!(
    imbalance_direction_byte_roundtrip,
    any_imbalance_direction(),
    ImbalanceDirection
);
enum_byte_roundtrip_test!(
    price_variation_byte_roundtrip,
    any_price_variation(),
    PriceVariation
);
enum_byte_roundtrip_test!(
    rpi_interest_flag_byte_roundtrip,
    any_rpi_interest_flag(),
    RpiInterestFlag
);

// ---------- enum unknown-byte rejection ----------
//
// A byte that is *not* one of the documented variant codes must be
// rejected with `InvalidEnumCode { field: <enum name>, code }`.
// `prop_filter`s out the variant set per enum, then asserts the
// from_byte error is the typed variant.

macro_rules! enum_unknown_byte_test {
    ($name:ident, $enum:ident, $field_name:literal, $valid_bytes:expr) => {
        proptest! {
            #![proptest_config(ProptestConfig {
                cases: 256,
                ..ProptestConfig::default()
            })]
            #[test]
            fn $name(b in any::<u8>().prop_filter(
                "valid variant byte filtered out",
                |&b| !$valid_bytes.contains(&b),
            )) {
                match $enum::from_byte(b) {
                    Err(ProtocolError::InvalidEnumCode { field, code }) => {
                        prop_assert_eq!(field, $field_name);
                        prop_assert_eq!(code, b);
                    }
                    Ok(v) => prop_assert!(
                        false,
                        "from_byte accepted unknown byte 0x{:02X} → {:?}", b, v
                    ),
                    Err(other) => prop_assert!(
                        false,
                        "expected InvalidEnumCode, got {:?}", other
                    ),
                }
            }
        }
    };
}

enum_unknown_byte_test!(
    event_code_unknown_byte_errors,
    EventCode,
    "EventCode",
    [b'O', b'S', b'Q', b'M', b'E', b'C']
);
enum_unknown_byte_test!(
    market_category_unknown_byte_errors,
    MarketCategory,
    "MarketCategory",
    [b'Q', b'G', b'S', b'A', b'N', b'P', b'Z', b'V', b' ']
);
enum_unknown_byte_test!(
    financial_status_unknown_byte_errors,
    FinancialStatus,
    "FinancialStatus",
    [b'D', b'E', b'Q', b'S', b'G', b'H', b'J', b'K', b'C', b'N', b' ']
);
enum_unknown_byte_test!(
    authenticity_unknown_byte_errors,
    Authenticity,
    "Authenticity",
    [b'P', b'T']
);
enum_unknown_byte_test!(
    yes_no_unknown_byte_errors,
    YesNo,
    "YesNo",
    [b'Y', b'N', b' ']
);
enum_unknown_byte_test!(
    luld_tier_unknown_byte_errors,
    LuldTier,
    "LuldTier",
    [b'1', b'2', b' ']
);
enum_unknown_byte_test!(
    trading_state_unknown_byte_errors,
    TradingState,
    "TradingState",
    [b'H', b'P', b'Q', b'T']
);
enum_unknown_byte_test!(
    reg_sho_action_unknown_byte_errors,
    RegShoAction,
    "RegShoAction",
    [b'0', b'1', b'2']
);
enum_unknown_byte_test!(side_unknown_byte_errors, Side, "Side", [b'B', b'S']);
enum_unknown_byte_test!(
    printable_unknown_byte_errors,
    Printable,
    "Printable",
    [b'N', b'Y']
);
enum_unknown_byte_test!(
    breached_level_unknown_byte_errors,
    BreachedLevel,
    "BreachedLevel",
    [b'1', b'2', b'3']
);
enum_unknown_byte_test!(
    ipo_release_qualifier_unknown_byte_errors,
    IpoReleaseQualifier,
    "IpoReleaseQualifier",
    [b'A', b'C']
);
