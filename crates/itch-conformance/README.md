# itch-conformance

Canonical conformance vectors and async test drivers for any
third-party Rust NASDAQ TotalView-ITCH 5.0 implementation.

`itch-conformance` ships:

- **Wire vectors** — one byte literal + expected `Message`
  constructor per ITCH 5.0 message kind, plus the documented
  `TradeNonCross` quirk shapes. Iterate `vectors::all()` to assert
  your decoder / encoder match the reference workspace.
- **Enum tables** — for every closed-set ASCII enum in
  `itch-protocol`, the documented `(byte, variant)` table plus a
  short list of `unknown_bytes` your decoder must reject as
  `ProtocolError::InvalidEnumCode`.
- **Async test drivers** (feature-gated) — `soup::drive_server`
  walks a SoupBinTCP server through login / heartbeat / reconnect /
  logout / end-of-session scenarios; `mold::drive_publisher` walks
  a MoldUDP64 publisher through in-order flow, gap recovery,
  session mismatch, and end-of-session scenarios.

The byte vectors mirror the goldens in
`crates/itch-protocol/tests/golden.rs` byte-for-byte. They are
**immutable**: per ADR-0007 any change to a published vector is a
wire-format regression and ships only as a major release.

## Usage

Add as a dev dependency:

```toml
[dev-dependencies]
itch-conformance = "0.1"
itch-protocol    = "0.1"
```

Wire up a single integration test:

```rust,ignore
use itch_conformance::vectors;
use itch_protocol::{Decode, Encode, Message};

#[test]
fn third_party_decoder_matches_reference() {
    for v in vectors::all() {
        let decoded = Message::decode(v.bytes).expect(v.name);
        assert_eq!(decoded, (v.message)(), "{}: decode mismatch", v.name);

        let mut buf = vec![0u8; decoded.encoded_len()];
        let n = decoded.encode(&mut buf).expect(v.name);
        assert_eq!(&buf[..n], v.bytes, "{}: encode mismatch", v.name);
    }
}
```

Drive a SoupBinTCP server (with the `soup` feature):

```rust,ignore
use itch_conformance::soup::{drive_server, SoupScenario};
use itch_soup::SoupCredentials;

# async fn run(addr: std::net::SocketAddr) {
let creds = SoupCredentials::new("user", "pass");
let scenarios = [
    SoupScenario::LoginAccept,
    SoupScenario::HeartbeatUnderSilence,
    SoupScenario::Logout,
];
let report = drive_server(addr, creds, &scenarios).await;
for outcome in &report {
    println!("{outcome:?}");
}
# }
```

Drive a MoldUDP64 publisher (with the `mold` feature):

```rust,ignore
use itch_conformance::mold::{drive_publisher, MoldScenario};

# async fn run(group: std::net::SocketAddr, request_server: std::net::SocketAddr) {
let scenarios = [
    MoldScenario::InOrderFlow { count: 16 },
    MoldScenario::HeartbeatUnderSilence,
    MoldScenario::EndOfSession,
];
let report = drive_publisher(group, request_server, &scenarios).await;
for outcome in &report {
    println!("{outcome:?}");
}
# }
```

## Features

| Feature  | What it adds                                        |
|----------|-----------------------------------------------------|
| `soup`   | `soup::drive_server` + `SoupScenario` / `SoupResult` |
| `mold`   | `mold::drive_publisher` + `MoldScenario` / `MoldResult` |

The default feature set is empty, so a downstream consumer that only
needs the byte vectors and enum tables pulls in just `itch-protocol`
and `thiserror`.

## License

Dual-licensed under MIT or Apache-2.0, matching the rest of the
workspace.
