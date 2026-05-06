# Changelog — `itch-mold`

All notable changes to this crate will be documented here. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to per-crate
[SemVer](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

- **MoldUDP64 V1.00 downstream packet codec** (`MoldPacket`,
  `MoldPacketHeader`, `MessageBlock`, plus constants `HEADER_LEN`,
  `MSG_COUNT_HEARTBEAT`, `MSG_COUNT_END_OF_SESSION`, `MAX_BLOCK_LEN`,
  `DEFAULT_PACKING_MTU`, `BLOCK_LEN_PREFIX`). The codec encodes /
  decodes the on-wire downstream packet:

  ```text
  ┌──────────────────┬──────────────────┬───────────────────┐
  │ Session (10 B)   │ SeqNo (8 B u64)  │ MsgCount (2 B u16)│
  └──────────────────┴──────────────────┴───────────────────┘
  followed by N message blocks, each
  ┌─────────────────┬───────────────────────────────┐
  │ Length (2 B u16)│        Message Data           │
  └─────────────────┴───────────────────────────────┘
  ```

  Heartbeat packets carry `MsgCount = 0`; end-of-session packets
  carry `MsgCount = 0xFFFF`; both encode as a header-only datagram.
  Per-block length is bounded by `MAX_BLOCK_LEN = 1024` bytes —
  matching `itch-tcp` / `itch-soup` so a malicious peer cannot
  force unbounded buffering at the codec layer.
- **`MoldError`** (`#[non_exhaustive]`, `#[from]` over
  `std::io::Error` and `itch_protocol::ProtocolError`) with
  structured variants for every documented failure mode:
  `Truncated`, `BufferTooSmall`, `BlockTooLarge`, `EmptyBlock`,
  `TrailingBytes`, `TooManyBlocks`, `SessionMismatch`,
  `SessionTooLong`, `PendingBufferFull`, `GapTooLarge`,
  `RequestTimeout`, `SourceExhausted`, `PeerSilent`. Higher-level
  receiver / publisher PRs (#21–#26) populate the remaining
  variants without breaking matchers.
- `session_from_str`: ASCII helper that right-pads a string with
  spaces into a 10-byte session id and rejects anything longer
  than 10 bytes (`MoldError::SessionTooLong`).
- 24 unit tests cover every documented codec failure mode plus
  encode → decode equality for data, heartbeat, and
  end-of-session packets.
- **`MoldStream` receiver + `MoldEvent`** (`Message`, `Heartbeat`,
  `EndOfSession`, `Gap`) plus `MoldConfig` (#21).
- **Heartbeat / silent-link detection** (1 s warning, 15 s
  dead-link).
- **In-order delivery + foundational gap detection** per
  ADR-0010 with bounded pending buffer (default 10 000).
- **End-of-session** (`MsgCount = 0xFFFF`).
- **Bounded pending buffer with selectable overflow policy
  (issue #22).** `PendingOverflowPolicy::{DropOldest, Error}` +
  `MoldConfig::with_pending_overflow` +
  `MoldStream::pending_overflow_drops()`. Default DropOldest; the
  `Error` policy yields `MoldError::PendingBufferFull { size }`
  without poisoning the stream. Duplicate out-of-order sequences
  deduplicated. 4 new unit tests.

### Notes

- `#![forbid(unsafe_code)]` on every module.
- Crate is registered in the workspace and exposes a `MoldResult<T>`
  alias for ergonomic `?` propagation.
- Receiver (`MoldStream`) + bounded pending buffer (#21, #22) in
  this release; gap recovery, request server, publisher, and
  integration tests land in #23–#26.
