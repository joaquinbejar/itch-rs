# Changelog — `itch-tcp`

All notable changes to this crate will be documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to per-crate [SemVer](https://semver.org/spec/v2.0.0.html).

## 1.0.0 — 2026-05-06

### Stability commitment

From this version forward, the public API of `itch-tcp` does not
change without a major bump. See `API.md` at the workspace root.

### Added

- `Server<S, St, P>` — generic publisher entry point taking a
  `MessageSource`, a `SeqStore`, and a `SubscriptionPolicy`.
  `Server::bind(addr, source, store, policy).await?.serve().await?`
  is the canonical 0.2 shape (per `docs/ITCH-SOURCE.md` §5).
- One ingest task drains the source, assigns sequence numbers
  (`store.latest() + 1`), persists each frame **before** any
  subscriber sees the bytes, then broadcasts to every connected
  client via `tokio::sync::broadcast` (default capacity 4 096).
- Per-subscriber writers call `policy.warmup()` first; warmup
  messages precede any live broadcast.
- Slow subscribers that lag past the broadcast capacity are
  dropped with `tracing::warn!(subscriber, skipped)` per
  `docs/ITCH-SOURCE.md` §8.
- `Server::with_broadcast_capacity` builder method to override the
  default fan-out capacity.
- Hard dep on `itch-source`. Existing `connect` / `bind` / `accept`
  free functions and `ItchCodec` / `ItchConnection` unchanged.
- Integration tests in `crates/itch-tcp/tests/server_integration.rs`
  exercise single-client, two-client, and warmup-then-live
  ordering against a `ChannelSource`-backed publisher.

## 0.1.0 — 2026-05-05

### Added

- `ItchCodec` — `tokio_util::codec::Decoder` +
  `Encoder<Message>` over a 2-byte big-endian length prefix that
  includes the 1-byte ITCH type tag.
- `ItchConnection` alias (`Framed<TcpStream, ItchCodec>`) plus
  async helpers `connect`, `bind`, `accept`. `connect` and `accept`
  set `TCP_NODELAY` on the resulting `TcpStream` and propagate the
  result via `?`; `bind` returns a `TcpListener` (no socket to
  configure).
- `MAX_MESSAGE_LEN = 1024` cap. The codec carries a `pending_skip`
  recovery counter so a `FrameTooLarge` reject drains the rest of
  the bad frame across multiple reads — partial oversized frames
  cannot corrupt the stream.
- `TransportError` (`#[non_exhaustive]`, `thiserror`) with `Io`,
  `Protocol(#[from] ProtocolError)`, and
  `FrameTooLarge { got, max }`. A bad inner frame yields one error
  and the codec resumes at the next length prefix; the stream is
  **not** poisoned.
- 7 unit tests including a partial-oversized-frame recovery test.

### Documented

- Cross-reference to ADR-0006 (Streams + Sinks) and ADR-0008
  (three sibling transports — `itch-tcp` is the demo sibling, with
  `itch-soup` and `itch-mold` landing in v0.3).
