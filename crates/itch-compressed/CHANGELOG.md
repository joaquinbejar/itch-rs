# Changelog — `itch-compressed`

All notable changes to this crate will be documented here. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to per-crate
[SemVer](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- **Initial crate** (issue #40, ADR-0011). Compressed via
  SoupBinTCP — zstd-decompressed ITCH 5.0 layered on top of
  `itch-soup`. Each `S Sequenced Data` payload is treated as a
  self-contained zstd frame whose decompressed body holds one or
  more concatenated ITCH messages (`tag + body`, no inner length
  prefix between messages).
- `CompressedSoupConnection` — generic over an
  `AsyncRead + AsyncWrite + Unpin` stream (defaults to
  `tokio::net::TcpStream`). Performs the SoupBinTCP login
  handshake directly via the public `itch_soup::SoupCodec` /
  `SoupPacket` packet-level surface (so we can read raw `S`
  payload bytes for decompression — `SoupConnection`'s `Stream`
  impl auto-decodes ITCH from the payload, which is wrong for
  compressed feeds). Implements
  `futures::Stream<Item = Result<Message, CompressedError>>` and
  exposes `connect`, `connect_with_timeout`, `handshake`,
  `next_message`, `logout`, `session`, `next_expected_sequence`.
- `ResilientCompressedSoupClient` — auto-reconnect wrapper around
  `CompressedSoupConnection`. Mirrors the contract of
  `itch_soup::ResilientSoupClient` (issue #17, ADR-0009) — same
  reconnect policy, sequence-resume, round-robin across `addrs`,
  exponential backoff with deterministic mid-point jitter,
  `max_attempts` budget, fatal-on-`LoginRejected` /
  `SessionMismatch`. `next_message`, `last_session`,
  `next_expected_sequence`, `into_stream`.
- `CompressedError` (`#[non_exhaustive]`, `thiserror`) with three
  structured variants: `Soup(#[from] SoupError)` (forwards every
  Soup-layer error verbatim including `Io`, `Protocol`,
  `LoginRejected`, `SessionEnded`), `Compression { reason:
  &'static str }` (zstd corruption / truncation / oversize),
  `Protocol(#[from] ProtocolError)` (bad inner ITCH after
  successful decompression).
- `compress_messages(&[Message], level: i32) -> Result<Vec<u8>,
  CompressedError>` and `decompress_messages(&[u8]) ->
  Result<Vec<Message>, CompressedError>` — public helpers that
  test code and custom publishers can use to round-trip the wire
  shape without going through a `CompressedSoupConnection`.
- `MAX_DECOMPRESSED_LEN = 64 KiB` cap on per-packet decompressed
  output. Defends against zstd-bomb payloads. The compressed
  input is itself bounded by `itch_soup::MAX_MESSAGE_LEN` (1 KiB).
- `DEFAULT_LOGIN_TIMEOUT = 10 s` matches `itch_soup`'s default.
- **Stream-poison resistance** at every layer:
  - Corrupted zstd frame → `CompressedError::Compression { reason
    }`; the rest of that compressed payload is discarded; the
    next `S` packet decodes cleanly.
  - Bad inner ITCH frame → `CompressedError::Protocol(_)`; the
    rest of the decompressed buffer is discarded (we cannot
    resync inside a length-prefix-less frame); the next `S`
    packet decodes cleanly.
  - Heartbeat (`H`), debug (`+`) packets are filtered silently.
  - `Z EndOfSession` is surfaced once as
    `CompressedError::Soup(SoupError::SessionEnded)`; subsequent
    polls return `Ready(None)`.
  - Stray `A` / `J` outside the handshake →
    `Soup(UnexpectedHandshakePacket { tag })`.
  - Client-direction packet on the read half →
    `Soup(SoupFraming { reason: "client-direction packet on a
    server stream" })`.
- 22 tests total (11 unit + 11 integration). Unit tests exercise
  the zstd round-trip, error paths, and `Send`/`Sync` /
  `Send`/`Unpin` bounds. Integration tests drive a real TCP
  server speaking SoupBinTCP via `itch_soup::SoupCodec` through:
  - Happy path (100 messages packed in one compressed `S`).
  - Boundary (5 messages in one compressed payload yields 5
    items in order).
  - Corrupted compressed frame → next valid packet decodes.
  - Heartbeat / debug / EOS forwarding.
  - Login-rejected during handshake.
  - Bad inner ITCH inside decompressed payload → next valid
    packet decodes.
  - Resilient client resumes across mid-stream socket drop.
  - Resilient client terminates on fatal `LoginRejected`.
  - Multiple compressed `S` packets across one connection.
  - Empty compressed payload consumed silently.
  - `into_stream()` smoke test.
- `#![forbid(unsafe_code)]` and `#![warn(missing_docs)]` on the
  crate root.
- New dependencies: `zstd = "0.13"` (default-features off — only
  the streaming codec, no CLI), `pin-project-lite = "0.2"`. The
  rest of the dependency surface is the workspace standard
  (`itch-protocol`, `itch-soup`, `thiserror`, `tokio`,
  `tokio-util`, `bytes`, `futures`, `tracing`).

### Documented

- ADR-0011 status flipped from "deferred" to "accepted /
  implemented". Compression scheme **locked to zstd** for v0.1;
  alternative schemes (zlib, LZ4, …) would land as separate
  `itch-compressed-<scheme>` sibling crates.
- Cross-reference to ADR-0008 (three sibling transports —
  `itch-compressed` adapts `itch-soup` only and never reaches
  for `itch-tcp` or `itch-mold`) and ADR-0009 (SoupBinTCP
  session model — reconnect contract reused verbatim by
  `ResilientCompressedSoupClient`).
