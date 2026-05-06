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
  `EndOfSession`, `Gap`) plus `MoldConfig`. `MoldStream::join` opens
  a UDP socket, joins the multicast group, and yields
  `Result<MoldEvent, MoldError>` per `futures::Stream`. Public state
  accessors: `current_session`, `next_expected_sequence`,
  `pending_count`, `silent_warned`. A `from_socket` constructor
  takes any pre-bound `UdpSocket` for tests / custom socket
  options.
- **Heartbeat / silent-link detection** with default thresholds
  `silence_warning = 1 s` (soft `tracing::warn!` + flag) and
  `silence_dead_link = 15 s` (yields one
  `MoldError::PeerSilent` then resumes). Configurable via
  `MoldConfig::with_silence`.
- **In-order delivery + foundational gap detection.** The
  receiver tracks `next_expected_sequence` per ADR-0010. On a
  packet whose first sequence is greater than expected the
  receiver emits `MoldEvent::Gap { from, to }`, stashes the
  out-of-order blocks in a bounded `BTreeMap<u64, Bytes>` capped
  at `max_pending_messages` (default `10_000`), and flushes them
  consecutively once the gap is filled by a later packet. Issue
  #22 turns the cap into a hard `PendingBufferFull` error and
  #24 wires up retransmission-request recovery.
- **End-of-session** (`MsgCount = 0xFFFF`) is delivered as
  `MoldEvent::EndOfSession { next_seq }`; subsequent stream polls
  resolve to `None`.
- 9 receiver unit tests using `tokio::time::pause` cover in-order
  delivery, session lock, heartbeat, EOS, gap + flush, session
  mismatch, retransmission drop, silent dead-link, and silent
  soft-warning.
- **Bounded pending buffer with selectable overflow policy
  (issue #22).** Adds `PendingOverflowPolicy::{DropOldest, Error}`
  plus `MoldConfig::with_pending_overflow` and
  `MoldStream::pending_overflow_drops()` (cumulative observability
  counter). Default behaviour matches `docs/TRANSPORT-SPEC.md` §4
  (drop oldest, log warn, continue); the `Error` policy yields a
  single typed `MoldError::PendingBufferFull { size }` per
  overflow without poisoning the stream — caller decides whether
  to propagate or reset state. Duplicate out-of-order sequences
  are deduplicated and never inflate the pending buffer. 4 new
  unit tests cover drop-oldest eviction, error-policy yield-and-
  resume, duplicate-seq no-op, and full-buffer flush on resync.

### Notes

- `#![forbid(unsafe_code)]` on every module.
- Crate is registered in the workspace and exposes a `MoldResult<T>`
  alias for ergonomic `?` propagation.
- Receiver (`MoldStream`), gap recovery, request server, publisher,
  and integration tests land in issues #21–#26 (with #21 in this
  release).
