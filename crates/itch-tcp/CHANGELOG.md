# Changelog — `itch-tcp`

All notable changes to this crate will be documented here. The format is
based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and
this project adheres to per-crate [SemVer](https://semver.org/spec/v2.0.0.html).

## Unreleased

## 0.1.0 — 2026-05-05

### Added

- `ItchCodec` — `tokio_util::codec::Decoder` +
  `Encoder<Message>` over a 2-byte big-endian length prefix that
  includes the 1-byte ITCH type tag.
- `ItchConnection` alias (`Framed<TcpStream, ItchCodec>`) plus
  async helpers `connect`, `bind`, `accept`. All three set
  `TCP_NODELAY` and propagate the result via `?`.
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
