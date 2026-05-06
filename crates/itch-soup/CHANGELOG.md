# Changelog — `itch-soup`

All notable changes to this crate will be documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to per-crate [SemVer](https://semver.org/spec/v2.0.0.html).

## 1.0.0 — 2026-05-06

### Stability commitment

From this version forward, the public API of `itch-soup` does not
change without a major bump. See `API.md` at the workspace root.

### Changed

- **API audit for 1.0** (#42). Added `#[non_exhaustive]` to
  `LoginRejectReason` so NASDAQ's future SoupBinTCP reject codes
  can ship in a minor release without breaking downstream `match`
  arms. Updated `itch-client::run_soup` to add a fallback arm
  ("unknown reject code"). See the workspace `API.md` for the
  full enum-by-enum policy.

### Added

- **Criterion bench `soup_codec`** (issue #31): per-packet
  encode + decode plus back-to-back streaming decode of all 10
  packet kinds. Run with
  `cargo bench -p itch-soup --bench soup_codec`. Covers the spec-
  fixed `LoginRequest` (46 B) / `LoginAccepted` (33 B) variants
  plus the variable-payload `SequencedData` / `UnsequencedData` /
  `Debug`. Live-server / `bench-hdr` p99/p99.9 variants deferred
  (see `BENCH.md`).
- **`SoupServer`** (issue #18). Server-side SoupBinTCP publisher
  generic over the three `itch-source` traits (`MessageSource`,
  `SeqStore`, `SubscriptionPolicy`) per ADR-0012.
- New hard dependency: `itch-source` (per ADR-0012).
- **End-to-end integration tests** (`tests/integration.rs`,
  issue #19). Drives a real `SoupServer` from a real
  `SoupConnection` / `ResilientSoupClient` through every
  documented exchange: happy-path (1000 sequenced messages),
  login rejection (NotAuthorized + SessionUnavailable),
  server-killed mid-session, reconnect-with-resume,
  ResilientSoupClient recovers across socket drops, 2 concurrent
  clients fan-out, logout cleanly, graceful EOS, heartbeat
  packets filtered. Gated behind the `integration` Cargo feature.
- New dev-deps: `tracing-subscriber` (test-only).
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
- **`ResilientSoupClient` + `ResilientSoupConfig`** (issue #17,
  ADR-0009 resilience layer). Wraps `SoupConnection` with auto-
  reconnect, sequence resume, and exponential backoff with jitter
  (per `docs/TRANSPORT-SPEC.md` §3.7,
  `rules/global_rules.md`):
  - Round-robin across `addrs: Vec<SocketAddr>` on each reconnect.
  - Pinned `requested_session: Option<String>` carried unchanged
    across reconnects; `initial_sequence: u64` honoured on the
    first connect, `next_expected_sequence` from the live
    connection on every subsequent reconnect.
  - Exponential backoff with uniform jitter sampled from
    `[backoff_min, min(backoff_max, backoff_min << attempt)]`
    (default 100 ms → 30 s).
  - `max_attempts: Option<u32>` retry budget; `None` retries
    forever.
  - `LoginRejected(_)` and `SessionMismatch { .. }` are fatal:
    surfaced once then `next_message()` returns `None`.
  - `Protocol(_)` errors are forwarded but do NOT trigger
    reconnect — the inner `SoupConnection` resumes in place.
  - Observability: `last_session()`, `next_expected_sequence()`.
  - `into_stream() -> impl Stream<Item = Result<Message,
    SoupError>> + Send + Unpin` adapter for combinator usage.
  - 9 unit tests via in-process `tokio::net::TcpListener` mock
    server: socket-drop resume, fatal `LoginRejected`,
    `max_attempts` exhausted, multi-addr round-robin failover,
    backoff-bounds invariant, initial-state observability,
    `Send + Unpin` and `Arc<Mutex<_>>` compile checks,
    `into_stream()` smoke.
- New `rand = "0.8"` dependency, used **only** in `resilient.rs`
  for backoff jitter (no other entry points). Approved by issue
  #17 ticket text.
- **`Stream<Item = Result<Message, SoupError>>` for `SoupConnection`**
  (issue #16). `SoupConnection` now implements `futures::Stream`
  directly; the existing `next_message()` method is a thin async
  wrapper around it. Filter rules (per
  `docs/TRANSPORT-SPEC.md` §3.4):
  - `S` Sequenced Data → decoded into `itch_protocol::Message`;
    `next_expected_sequence` advances **on success only** so a
    failed inner ITCH decode doesn't desync reconnect.
  - `H` Server Heartbeat → consumed silently; the heartbeat
    scheduler tracks liveness on its own.
  - `+` Debug → routed to a lazy
    `SoupConnection::debug_packets() -> mpsc::Receiver<Vec<u8>>`
    if subscribed, otherwise dropped silently. Bounded buffer
    (16 packets) so the connection can never block waiting for
    a slow debug consumer.
  - `Z` End-of-Session → emitted **once** as
    `Some(Err(SessionEnded))`; subsequent polls yield `None`.
  - Stray `A` / `J` outside the handshake →
    `Err(UnexpectedHandshakePacket { tag })`.
  - Client-direction packet (`L` / `U` / `R` / `O`) on the read
    half → typed `Err(SoupFraming { reason: "client-direction
    packet on a server stream" })`.
- **`SoupConnection::send_unsequenced(Message)`** (canonical name
  for the existing `send` alias) wraps the outbound write in a
  configurable timeout (`DEFAULT_SEND_TIMEOUT = 250 ms`,
  overridable via `with_send_timeout`). A writer that cannot
  drain within the budget returns
  `Err(SoupError::Io(io::ErrorKind::WouldBlock))` rather than
  blocking forever — honours `docs/TRANSPORT-SPEC.md` §7.1.
- New `SoupError::SoupFraming { reason: &'static str }` variant
  for spec-violation events that don't fit a more specific
  variant (currently: client-direction packet on a server
  stream; reserved for future broadcast-lag drops in `SoupServer`).
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
