//! Self-test for `itch-conformance`.
//!
//! Validates every public vector and enum table against the
//! reference codec in `itch-protocol`. A downstream consumer that
//! depends on `itch-conformance` runs the equivalent loop against
//! its own decoder / encoder.

#![forbid(unsafe_code)]

use itch_conformance::enum_vectors;
use itch_conformance::vectors::{self, ConformanceVector};
use itch_protocol::{AlphaCoded, Message, ProtocolError};

// ---------------------------------------------------------------------------
// Vector roundtrip
// ---------------------------------------------------------------------------

fn check_vector(v: &ConformanceVector) {
    let expected = (v.message)();

    // decode(bytes) == message
    let decoded = match Message::decode(v.bytes) {
        Ok(m) => m,
        Err(err) => panic!("{}: decode failed: {err:?}", v.name),
    };
    assert_eq!(decoded, expected, "{}: decoded mismatch", v.name);

    // encode(message) == bytes
    let mut buf = vec![0u8; expected.encoded_len()];
    let n = match expected.encode(&mut buf) {
        Ok(n) => n,
        Err(err) => panic!("{}: encode failed: {err:?}", v.name),
    };
    assert_eq!(n, v.bytes.len(), "{}: encoded length mismatch", v.name);
    assert_eq!(&buf[..n], v.bytes, "{}: encoded bytes mismatch", v.name);
}

#[test]
fn every_vector_roundtrips_through_itch_protocol() {
    let all = vectors::all();
    assert!(
        all.len() >= 21,
        "expected at least 21 vectors (20 message kinds + 1 quirk), got {}",
        all.len()
    );
    for v in all {
        check_vector(v);
    }
}

#[test]
fn vectors_have_unique_names() {
    let mut names: Vec<&str> = vectors::all().iter().map(|v| v.name).collect();
    names.sort_unstable();
    let dedup_count = {
        let mut n = names.clone();
        n.dedup();
        n.len()
    };
    assert_eq!(dedup_count, names.len(), "duplicate vector name detected");
}

#[test]
fn vectors_cover_all_twenty_message_tags() {
    use std::collections::BTreeSet;
    let mut tags: BTreeSet<u8> = BTreeSet::new();
    for v in vectors::all() {
        // First byte of the wire form is always the type tag.
        if let Some(&tag) = v.bytes.first() {
            tags.insert(tag);
        }
    }
    let expected: BTreeSet<u8> = [
        b'S', b'R', b'H', b'Y', b'L', b'V', b'W', b'K', b'A', b'F', b'E', b'C', b'X', b'D', b'U',
        b'P', b'Q', b'B', b'I', b'N',
    ]
    .into_iter()
    .collect();
    assert_eq!(tags, expected, "missing tag coverage in vectors::all()");
}

// ---------------------------------------------------------------------------
// Enum vectors
// ---------------------------------------------------------------------------

/// Per-enum closure: round-trip every documented `(byte, variant)`
/// pair and reject every documented `unknown` byte via
/// `InvalidEnumCode`.
fn check_enum<T>(field: &'static str, variants: &[(u8, T)], unknown: &[u8])
where
    T: AlphaCoded + PartialEq + std::fmt::Debug,
{
    for (byte, variant) in variants {
        // byte → variant
        match T::from_byte(*byte) {
            Ok(v) => assert_eq!(&v, variant, "{field}: byte {byte:#04x} decoded incorrectly"),
            Err(err) => panic!("{field}: byte {byte:#04x} should decode but errored: {err:?}"),
        }
        // variant → byte
        assert_eq!(
            variant.to_byte(),
            *byte,
            "{field}: {variant:?} encoded to wrong byte"
        );
    }
    for &byte in unknown {
        match T::from_byte(byte) {
            Err(ProtocolError::InvalidEnumCode { field: f, code }) => {
                assert_eq!(f, field, "{field}: error carried wrong field name");
                assert_eq!(code, byte, "{field}: error carried wrong byte");
            }
            Ok(v) => panic!("{field}: byte {byte:#04x} unexpectedly decoded to {v:?}"),
            Err(other) => panic!("{field}: byte {byte:#04x} returned wrong error: {other:?}"),
        }
    }
}

#[test]
fn event_code_table_matches_alpha_coded() {
    check_enum(
        "EventCode",
        enum_vectors::event_code_variants(),
        enum_vectors::event_code_unknown_bytes(),
    );
}

#[test]
fn side_table_matches_alpha_coded() {
    check_enum(
        "Side",
        enum_vectors::side_variants(),
        enum_vectors::side_unknown_bytes(),
    );
}

#[test]
fn market_category_table_matches_alpha_coded() {
    check_enum(
        "MarketCategory",
        enum_vectors::market_category_variants(),
        enum_vectors::market_category_unknown_bytes(),
    );
}

#[test]
fn financial_status_table_matches_alpha_coded() {
    check_enum(
        "FinancialStatus",
        enum_vectors::financial_status_variants(),
        enum_vectors::financial_status_unknown_bytes(),
    );
}

#[test]
fn authenticity_table_matches_alpha_coded() {
    check_enum(
        "Authenticity",
        enum_vectors::authenticity_variants(),
        enum_vectors::authenticity_unknown_bytes(),
    );
}

#[test]
fn yes_no_table_matches_alpha_coded() {
    check_enum(
        "YesNo",
        enum_vectors::yes_no_variants(),
        enum_vectors::yes_no_unknown_bytes(),
    );
}

#[test]
fn luld_tier_table_matches_alpha_coded() {
    check_enum(
        "LuldTier",
        enum_vectors::luld_tier_variants(),
        enum_vectors::luld_tier_unknown_bytes(),
    );
}

#[test]
fn trading_state_table_matches_alpha_coded() {
    check_enum(
        "TradingState",
        enum_vectors::trading_state_variants(),
        enum_vectors::trading_state_unknown_bytes(),
    );
}

#[test]
fn reg_sho_action_table_matches_alpha_coded() {
    check_enum(
        "RegShoAction",
        enum_vectors::reg_sho_action_variants(),
        enum_vectors::reg_sho_action_unknown_bytes(),
    );
}

#[test]
fn market_maker_mode_table_matches_alpha_coded() {
    check_enum(
        "MarketMakerMode",
        enum_vectors::market_maker_mode_variants(),
        enum_vectors::market_maker_mode_unknown_bytes(),
    );
}

#[test]
fn market_participant_state_table_matches_alpha_coded() {
    check_enum(
        "MarketParticipantState",
        enum_vectors::market_participant_state_variants(),
        enum_vectors::market_participant_state_unknown_bytes(),
    );
}

#[test]
fn breached_level_table_matches_alpha_coded() {
    check_enum(
        "BreachedLevel",
        enum_vectors::breached_level_variants(),
        enum_vectors::breached_level_unknown_bytes(),
    );
}

#[test]
fn ipo_release_qualifier_table_matches_alpha_coded() {
    check_enum(
        "IpoReleaseQualifier",
        enum_vectors::ipo_release_qualifier_variants(),
        enum_vectors::ipo_release_qualifier_unknown_bytes(),
    );
}

#[test]
fn printable_table_matches_alpha_coded() {
    check_enum(
        "Printable",
        enum_vectors::printable_variants(),
        enum_vectors::printable_unknown_bytes(),
    );
}

#[test]
fn cross_type_table_matches_alpha_coded() {
    check_enum(
        "CrossType",
        enum_vectors::cross_type_variants(),
        enum_vectors::cross_type_unknown_bytes(),
    );
}

#[test]
fn imbalance_direction_table_matches_alpha_coded() {
    check_enum(
        "ImbalanceDirection",
        enum_vectors::imbalance_direction_variants(),
        enum_vectors::imbalance_direction_unknown_bytes(),
    );
}

#[test]
fn price_variation_table_matches_alpha_coded() {
    check_enum(
        "PriceVariation",
        enum_vectors::price_variation_variants(),
        enum_vectors::price_variation_unknown_bytes(),
    );
}

#[test]
fn rpi_interest_flag_table_matches_alpha_coded() {
    check_enum(
        "RpiInterestFlag",
        enum_vectors::rpi_interest_flag_variants(),
        enum_vectors::rpi_interest_flag_unknown_bytes(),
    );
}

// ---------------------------------------------------------------------------
// Unknown-byte propagation through full message decode
// ---------------------------------------------------------------------------

/// Sanity: injecting a known-bad enum byte into a constructed
/// message buffer surfaces as `InvalidEnumCode` rather than silently
/// coercing to a default variant. We check this for `Side` (offset
/// 19 in the body of an `A` message) — the only enum with a
/// stable, well-known offset that doesn't collide with other
/// closed-set bytes.
#[test]
fn corrupt_side_byte_in_add_order_surfaces_invalid_enum_code() {
    let v = &vectors::ADD_ORDER;
    // Wire offset of `side` is `1 (tag) + 10 (header) + 8 (order_ref) = 19`.
    let mut buf = v.bytes.to_vec();
    buf[19] = b'X';
    match Message::decode(&buf) {
        Err(ProtocolError::InvalidEnumCode { field, code }) => {
            assert_eq!(field, "Side");
            assert_eq!(code, b'X');
        }
        Ok(m) => panic!("expected InvalidEnumCode, decoded {m:?}"),
        Err(other) => panic!("expected InvalidEnumCode, got {other:?}"),
    }
}
