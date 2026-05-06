# Changelog — `itch-soup`

All notable changes to this crate will be documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to per-crate [SemVer](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- **Login state machine** (`SoupConnection`, `login`,
  `login_with_timeout`, `SoupCredentials`,
  `DEFAULT_LOGIN_TIMEOUT`). Performs the
  `LoginRequest → LoginAccepted | LoginRejected` handshake on any
  `AsyncRead + AsyncWrite + Unpin` (typically a `TcpStream`) and
  returns a `SoupConnection<S>` whose three async methods cover the
  data phase: `next_message()` (filters out heartbeats / debug
  packets, converts `EndOfSession` → `SessionEnded`, decodes
  sequenced-data payloads into `itch_protocol::Message`),
  `send(Message)` (emits `UnsequencedData`), and `logout()`
  (sends `LogoutRequest` and closes). Per ADR-0009 the base
  connection does NOT auto-reconnect — every fatal session event is
  surfaced as a typed error so the higher-level
  `ResilientSoupClient` (later issue) can drive the retry loop.
- Sequence-resume support: `SoupConnection::next_expected_sequence()`
  returns the next sequenced-data sequence number, initialised from
  the server's `LoginAccepted` reply and incremented per delivered
  message. Pass it back into `login`'s `requested_sequence` argument
  on reconnect. `SoupConnection::session()` exposes the negotiated
  session id for the same purpose.
- **Heartbeat scheduler** (`HeartbeatConfig`, `OutboundHeartbeat`,
  `InboundHeartbeat`): configurable 1 s outbound interval (resets on
  real traffic) + 15 s dead-link timeout. `OutboundHeartbeat` sends
  `H` (server) or `R` (client) automatically; `InboundHeartbeat`
  detects peer silence and emits `SoupError::PeerSilent { since }`.
  4 unit tests using `tokio::time::pause` + `tokio::time::advance`
  to avoid wall-clock flakiness. Primitives can be integrated into
  `SoupConnection` via a future `login_with_heartbeat()` variant or
  used standalone.
- Four new structured `SoupError` variants:
  `SessionMismatch { requested, got }` (server's `LoginAccepted`
  returned a different session than an explicit non-empty request),
  `PrematureClose` (peer hung up before sending `A` / `J`),
  `UnexpectedHandshakePacket { tag }` (non-`A`/`J`/`+` during the
  handshake, or `A`/`J`/`L`/`U`/`R`/`O` during the data phase),
  `LoginTimeout(Duration)` (handshake exceeded the configured
  bound; default 10 s).
- 14 unit tests in `connection::tests` (over-deliver vs. the 8 the
  issue requires): successful login + state assertions, login
  rejected for both `NotAuthorized` and `SessionUnavailable`,
  premature close before reply, premature close after
  `LoginRequest` write, login timeout (uses `tokio::time::pause`
  via `#[tokio::test(start_paused = true)]`), session mismatch on
  explicit requested-session, blank-session accept-anything
  semantics, unexpected handshake packet (`H`), debug-packet
  swallowed during handshake, sequenced-data delivery + counter
  increment + `SessionEnded`, `logout()` emits `LogoutRequest` then
  closes, `send()` emits `UnsequencedData`, and a `Framed`
  wire-shape sanity check. Total crate test count: 32 passing.

- Initial crate skeleton: SoupBinTCP 3.00 packet envelope codec
  (length-type-payload framing). See `docs/specs/soupbintcp-3.0.md`
  and `docs/TRANSPORT-SPEC.md` §3.
- `SoupCodec` — `tokio_util::codec::Decoder<Item = SoupPacket>` +
  `Encoder<SoupPacket>`. Mirrors the resilience model of
  `itch-tcp::ItchCodec`: a bad inner frame is dropped and the codec
  resumes parsing at the next length prefix; oversized frames whose
  announced length had not fully arrived are drained across
  subsequent reads via a `pending_skip` counter.
- `SoupPacket` enum covering all 9 wire types: `Debug` (`+`),
  `LoginRequest` (`L`), `LoginAccepted` (`A`), `LoginRejected`
  (`J`), `SequencedData` (`S`), `UnsequencedData` (`U`),
  `ServerHeartbeat` (`H`), `ClientHeartbeat` (`R`), `EndOfSession`
  (`Z`), `LogoutRequest` (`O`).
- `LoginRequest` (4 fields: `username`, `password`,
  `requested_session`, `requested_sequence`) and `LoginAccepted`
  (2 fields: `session`, `sequence`) DTOs with the spec's
  6/10/10/20-byte and 10/20-byte ASCII field widths preserved on
  the wire (right-pad / left-pad with spaces; ASCII decimal for
  `u64` sequence).
- `LoginRejectReason` enum (`NotAuthorized` = `'A'`,
  `SessionUnavailable` = `'S'`) with `to_byte` / `from_byte`
  helpers and a `Display` impl.
- `SoupError` (`#[non_exhaustive]`, `thiserror`) with 7 structured
  variants: `Io`, `Protocol(#[from] ProtocolError)`,
  `FrameTooLarge { got, max }`, `UnknownPacketType { tag }`,
  `BadPayloadLength { tag, expected, got }`,
  `LoginRejected(LoginRejectReason)`, `SessionEnded`,
  `PeerSilent(Duration)`. The last three are reserved for the
  session layer (next issue) but exported now so the API surface
  is stable.
- `MAX_MESSAGE_LEN = 1024` — total wire bytes per packet (length
  prefix + tag + payload). Mirrors `itch-tcp` so a malicious peer
  cannot force unbounded buffering at any framing layer.
- 18 unit tests covering: every packet variant roundtrip, blank-
  session / sequence-0 idiom wire shape, login-rejected with both
  reasons + an unknown reason, decoder starvation across partial
  prefixes / partial bodies, truncated `L` payload, oversized
  frames present in one read AND drained across multiple reads,
  unknown packet type, zero-length prefix rejection, two packets
  in one buffer, username truncation on encode, non-digit reject
  in the ASCII sequence field, and `LoginRejectReason` byte
  roundtrip.

### Documented

- Cross-reference to ADR-0008 (three sibling transports —
  `itch-soup` is the production unicast sibling) and ADR-0009
  (SoupBinTCP session model — landing in subsequent issues).
- `#![forbid(unsafe_code)]` and `#![warn(missing_docs)]` on the
  crate root.
