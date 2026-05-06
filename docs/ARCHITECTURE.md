# Architecture — `itch-rs`

| Field      | Value                          |
|------------|--------------------------------|
| Status     | Draft v1.0                     |
| Last edit  | 2026-05-05                     |
| Audience   | Engineers integrating or extending `itch-rs` |

## 1. Guiding principles

1. **À la carte crates.** Every layer is its own crate, publishable
   independently, with its own SemVer cadence. Consumers pay only the
   dependency cost they need. ([ADR-0007](adr/0007-independent-crates.md))
2. **Domain modeling first.** Every protocol concept maps to a Rust type.
   Bytes become structs at the boundary; the rest of the codebase speaks
   business language. ([ADR-0001](adr/0001-domain-modeling-first.md))
3. **Transport-agnostic core.** `itch-protocol` does no I/O, no async, no
   `tokio`. It can run in `no_std`, in WASM, and in offline tools.
   ([ADR-0002](adr/0002-transport-agnostic-core.md))
4. **Standard abstractions over custom traits.** Where `futures::Stream` /
   `Sink` already model what we need, we use them instead of inventing a
   new trait. ([ADR-0006](adr/0006-streams-and-sinks.md))
5. **Zero-cost types.** Newtypes and enums lower to a single primitive at
   the machine level. No `Box`, no `Vec`, no `String` in hot paths.
6. **`std`-by-default, `no_std` opt-in.** `itch-protocol` is designed
   so its `std` feature can be removed without rewriting the parser.
   ([ADR-0005](adr/0005-std-by-default-no-std-opt-in.md))
7. **Three sibling transport crates** — `itch-tcp`, `itch-soup`,
   `itch-mold` — instead of one monolithic `itch-transport`. Each has
   its own SemVer cadence and never depends on the others.
   ([ADR-0008](adr/0008-three-sibling-transports.md))
8. **OrderBook-rs bridge** via the `itch-orderbook` crate. Pluggable
   matching-engine adapter that translates OrderBook-rs events into
   ITCH 5.0 messages. ([ADR-0013](adr/0013-orderbook-bridge.md))

## 2. Crate map

```
                        ┌─────────────────────────────┐
                        │        Applications         │
                        │  itch-server   itch-client  │
                        └─────────────┬───────────────┘
                                      │
        ┌─────────────────┬───────────┴───────────┬─────────────────┐
        │                 │                       │                 │
┌───────▼──────┐  ┌───────▼───────┐  ┌────────────▼──────┐  ┌───────▼────────┐
│  itch-tcp    │  │  itch-soup    │  │   itch-mold        │  │ itch-compressed│
│ length-prefix│  │  SoupBinTCP   │  │   MoldUDP64        │  │  (planned)     │
│  TCP demo    │  │  unicast TCP  │  │   UDP multicast    │  │ wraps soup     │
│              │  │  + login +    │  │   + gap recovery   │  │                │
│              │  │  heartbeats   │  │   + heartbeats     │  │                │
└───────┬──────┘  └───────┬───────┘  └────────┬──────────┘  └───────┬────────┘
        │                 │                   │                     │
        │  source/store   │  source/store     │  source/store       │
        ▼                 ▼                   ▼                     ▼
                       ┌──────────────────────────┐
                       │       itch-source        │
                       │ MessageSource  SeqStore  │
                       │ SubscriptionPolicy       │
                       └────────────┬─────────────┘
                                    │
                       ┌────────────▼─────────────┐
                       │      itch-protocol       │
                       │   types  enums  codec    │
                       │   (no I/O, no async)     │
                       └──────────────────────────┘
                                    ▲
                                    │ optional, post-MVP
        ┌──────────────┬────────────┼─────────────┬──────────────────┐
        │              │            │             │                  │
┌───────┴──────┐ ┌─────┴──────┐ ┌──┴───────┐ ┌────┴────────┐  ┌──────┴───────┐
│  itch-book   │ │itch-replay │ │ -redis   │ │  -postgres  │  │  -kafka      │
│  order book  │ │.itch file  │ │ SeqStore │ │  SeqStore   │  │  Source      │
│  rebuild     │ │ replay     │ │ adapter  │ │   adapter   │  │  adapter     │
└──────────────┘ └────────────┘ └──────────┘ └─────────────┘  └──────────────┘
                  (impls Source) (impls Store)(impls Store)    (impls Source)
```

Additional layer (not shown, depends on itch-source + itch-protocol):

```
                    ┌──────────────────────┐
                    │  itch-orderbook      │
                    │ OrderBook-rs → ITCH  │
                    │ bridge (design v0.4; │
                    │ impl post-v0.4)      │
                    └──────────────────────┘
```

Each crate is published independently to crates.io with its own SemVer
lifecycle. The fan-out at the top of the diagram is the key design move:
the three production-relevant transports are **siblings**, not a
monolithic `itch-transport`. A user who only needs MoldUDP64 never pulls
in TCP / SoupBinTCP code.

### 2.1 Crate responsibilities

| Crate              | Responsibility                                                              | Depends on                                          | Async? |
|--------------------|-----------------------------------------------------------------------------|----------------------------------------------------|--------|
| `itch-protocol`    | Types, enums, binary encode/decode                                          | `thiserror`                                        | No     |
| `itch-source`      | Provider abstraction: `MessageSource`, `SeqStore`, `SubscriptionPolicy`     | `itch-protocol`, `futures`                         | Yes    |
| `itch-tcp`         | Naïve length-prefix TCP framing — for tests and local demos                 | `itch-protocol`, `itch-source`, `tokio`, `tokio-util`, `bytes` | Yes    |
| `itch-soup`        | SoupBinTCP 3.00 (login, sequenced data, heartbeats, EOS)                    | `itch-protocol`, `itch-source`, `tokio`, `tokio-util`, `bytes` | Yes    |
| `itch-mold`        | MoldUDP64 V1.00 (multicast, gaps, request server)                           | `itch-protocol`, `itch-source`, `tokio`, `bytes`   | Yes    |
| `itch-server`      | Demo replay binary (uses `itch-tcp` by default; pluggable)                  | one of the transports, `tokio`                     | Yes    |
| `itch-client`      | Demo subscriber binary                                                      | one of the transports, `tokio`                     | Yes    |
| `itch-compressed` *| Compressed via SoupBinTCP                                                   | `itch-soup`, codec (likely `zstd` or as spec'd)    | Yes    |
| `itch-book` *      | L2/L3 order-book reconstruction; also implements `MessageSource`            | `itch-protocol`, `itch-source`                     | No     |
| `itch-replay` *    | `.itch` file replay; implements `MessageSource`                             | `itch-protocol`, `itch-source`, `tokio`            | Yes    |
| `itch-orderbook` * | OrderBook-rs → ITCH bridge (design); full impl pending OrderBook-rs stabilization | `itch-protocol`, `itch-source`, `OrderBook-rs` (optional) | No    |
| `itch-source-redis` * | `RedisSeqStore` for distributed publishers                               | `itch-source`, `redis`                             | Yes    |
| `itch-source-postgres` * | `PostgresSeqStore` for durable replay storage                         | `itch-source`, `sqlx`                              | Yes    |
| `itch-source-kafka` * | `KafkaSource` consuming an upstream Kafka topic                          | `itch-source`, `rdkafka`                           | Yes    |

\* = planned / post-MVP.

Two architectural rules these crates obey:

- **Transports never depend on each other.** They each pull
  `itch-protocol` + `itch-source` + an async runtime, and that's it.
- **`itch-source` is the integration seam.** Anybody wiring a real data
  provider into the publishers implements its three traits — they
  don't touch the transport crates. ADR-0012 formalises this.

See [**source-example.md**](source-example.md) for a worked example: a matching engine
that emits trades and orders, wired into `itch-source` with `ChannelSource` /
`RingBufferSeqStore` / `StaticPolicy`, and published via `itch-tcp`.

## 3. Layered view

The layering mirrors the protocol stack and the cognitive layers of a
trading system:

| Layer                | Concern                                                  | Crate            |
|----------------------|----------------------------------------------------------|------------------|
| **Application**      | What does my system *do* with these messages?            | (your code, or `itch-server`/`itch-client`) |
| **Session/replay**   | Wire framing, file replay, gap recovery                  | `itch-transport`, `itch-replay`, `itch-mold` |
| **Protocol**         | Bytes ⇆ structs, validation                              | `itch-protocol`  |
| **Domain**           | Newtypes, invariants                                     | `itch-protocol::primitives`, `enums`, `messages` |

Each layer depends only downward. Each layer has stronger guarantees than
the one above: by the time bytes reach the application, they are typed,
validated, and free of any framing concern.

## 4. Data flows

### 4.1 Server publishes a message

```
SystemEvent { event_code: StartOfMessages, .. }
        │
        ▼ Encoder::encode()  (itch-transport)
[ 0x00 0x0B  S  0x00 0x00  0x00 0x00  ts(6 bytes)  O ]
        │
        ▼ Framed<TcpStream, ItchCodec>
        ▼ tokio writes to socket
   [TCP segment, 14 bytes payload]
```

### 4.2 Client receives the same message

```
   [TCP segment, 14 bytes payload]
        │
        ▼ tokio reads into BytesMut
        ▼ ItchCodec::decode():
            ├── peek length prefix → 0x000B (11 bytes follow)
            ├── if buffer < 13 bytes, return Ok(None) and wait for more
            ├── advance(2), split_to(11)
            └── Message::decode(&bytes)  (itch-protocol)
                    │
                    ▼ tag = 'S' → SystemEvent::decode_body()
                    ▼ decode_header() reads StockLocate, TrackingNumber, Timestamp
                    ▼ EventCode::from_byte('O') → EventCode::StartOfMessages
        │
        ▼ Stream::poll_next yields:
SystemEvent { event_code: StartOfMessages, .. }
```

The boundary is sharp: nothing async happens inside `itch-protocol`,
nothing protocol-specific happens inside `tokio`.

## 5. Concurrency model

- `Framed<TcpStream, ItchCodec>` is `Send + Unpin`, fits any tokio
  executor.
- The demo server spawns one task per accepted client — classic
  per-connection actor model. Inter-client fan-out (e.g., real
  multi-subscriber publication) is out of scope and would belong to a
  publishing layer above `itch-transport`.
- `itch-protocol` types are all `Send + Sync + Copy`; messages can be
  shared across threads or stored in lock-free queues.
- All buffers are stack-allocated in the codec hot path — no `Arc`, no
  `Mutex`, no global state.

## 6. Error model

Three layers of errors, each only converting **down** to its own kind:

```
TransportError                       ◄── what consumers handle
    ├── Io(std::io::Error)
    ├── FrameTooLarge { got, max }
    └── Protocol(ProtocolError)      ◄── from `?` conversion
                ├── Truncated { need, got }
                ├── BufferTooSmall { need, got }
                ├── UnknownMessageType(u8)
                └── InvalidEnumCode { field, code }
```

Decisions:

- `itch-protocol::ProtocolError` carries enough context to write a useful
  log line without reaching for the source bytes.
- `TransportError` does **not** wrap `String` or `&'static str` for ad-hoc
  messages: every variant is a structured enum so consumers can match.
- A bad frame **does not** poison the stream. The codec advances past the
  bad bytes and the next call resumes parsing.

## 7. Architecture Decision Records

Lightweight ADRs live under [`docs/adr/`](adr/). Created so far:

| #     | Title                                                                |
|-------|----------------------------------------------------------------------|
| 0001  | Domain modeling first                                                |
| 0002  | Transport-agnostic core (no I/O in `itch-protocol`)                  |
| 0003  | Fixed-point integer prices, not `rust_decimal`                       |
| 0004  | Hand-rolled codec instead of `nom`                                   |
| 0005  | `std`-by-default, `no_std` opt-in                                    |
| 0006  | Standard `Stream` / `Sink` over a custom trait                       |
| 0007  | Independent crates per layer                                         |
| 0008  | Three sibling transport crates: `itch-tcp`, `itch-soup`, `itch-mold` |
| 0009  | SoupBinTCP session model: explicit reconnect with sequence resume    |
| 0010  | MoldUDP64 gap recovery: separate `RequestClient` + multicast `Stream`|
| 0011  | Compressed transport deferred and built on top of `itch-soup`        |
| 0012  | Data-source abstraction (`itch-source`) — surface in [ITCH-SOURCE.md](ITCH-SOURCE.md) |

Each ADR follows the format **Context → Decision → Consequences →
Alternatives**.

## 8. Cross-cutting concerns

### 8.1 Observability

- `itch-transport`, `itch-server`, `itch-client` use `tracing` (no logging
  macros from `log`).
- `itch-protocol` is silent: instrumentation belongs to the consumer, not
  the parser.
- Future: per-message-type counters via `metrics` crate behind a feature.

### 8.2 Performance

- Encode and decode are zero-allocation in steady state; the codec
  resizes the output `BytesMut` once per message.
- All multi-byte integer reads/writes are inlined and use `from_be_bytes`
  / `to_be_bytes`, which compiles to a single `bswap` on x86 / `rev` on
  ARM.
- Benchmarks live in `crates/itch-protocol/benches/` (Criterion-based,
  post-MVP). Targets are listed in [PRD.md](PRD.md#4-non-functional-requirements).

### 8.3 Security

- `#![forbid(unsafe_code)]` in `itch-protocol`.
- Frame size is bounded by `MAX_MESSAGE_LEN` (1 KiB) so a malicious peer
  cannot force unbounded buffering.
- No input is interpreted as a path, command, or evaluated string.

### 8.4 Versioning policy

- Each crate is versioned independently following SemVer.
- A breaking change in `itch-protocol` causes major bumps in everything
  that depends on it; we coordinate releases via a changelog entry per
  crate.
- The MSRV (Minimum Supported Rust Version) is documented per crate and
  raised in a minor release with explicit notice.

## 9. Open architectural questions

- **Q-A1** Should the order-book layer be `Send` or single-threaded with
  explicit channels? Pending benchmarks.
- **Q-A2** Should `itch-mold` reuse `itch-transport`'s `Framed` machinery,
  or fork its own datagram-oriented codec? Likely the latter.
- **Q-A3** Do we need a `serde` feature on `itch-protocol` for
  capture/replay tooling, or is binary-only enough? See PRD Q-4.
