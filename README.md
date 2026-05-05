# itch-rs

A small, idiomatic Rust client/server implementation of the
**NASDAQ TotalView-ITCH 5.0** market-data protocol.

> ITCH is a one-way binary feed that describes everything happening on the
> NASDAQ order book: orders being added, executed, cancelled, replaced; trades
> printing; cross sessions clearing; system-wide circuit breakers; and so on.

## Layout

```
itch-rs/
├── Cargo.toml                 # workspace
└── crates/
    ├── itch-protocol/         # pure types + binary codec, no I/O
    ├── itch-tcp/              # length-prefix TCP framing (demos)
    ├── itch-server/           # demo bin: replays a synthetic session
    └── itch-client/           # demo bin: prints decoded messages
```

The split mirrors the layered design of the protocol itself:

| Layer                | Crate            | Knows about                              |
|----------------------|------------------|------------------------------------------|
| Domain (messages)    | `itch-protocol`  | Bytes ⇆ structs                          |
| Framing & I/O (demo) | `itch-tcp`       | TcpStream, futures Stream/Sink           |
| Application          | `itch-server`,   | Business logic: session replay, printing |
|                      | `itch-client`    |                                          |

Production transports — `itch-soup` (SoupBinTCP 3.00 unicast) and
`itch-mold` (MoldUDP64 V1.00 multicast) — are siblings of `itch-tcp`
landing in v0.3. They never depend on each other; consumers pick the
one they need.

`itch-protocol` is `no_std`-friendly in spirit (we only use `std` for `Error`
plumbing) and has zero async dependencies. You could reuse it to read a `.itch`
capture file or to write a feed handler for a different transport.

## Design choices

1. **Domain modeling first.** Every ITCH primitive — `Stock`, `OrderReference`,
   `Price4`, `Timestamp`, `Mpid`, ... — is a Rust newtype, and every closed set
   of one-byte ASCII codes is an `enum`. The codec validates once at decode
   time so the rest of your program can rely on the type system.

2. **Transport abstracted by trait composition, not a custom trait.** Our
   `ItchCodec` plugs into `tokio_util::codec::Framed`, which already implements
   `futures::Stream<Item = Result<Message, _>>` and `futures::Sink<Message>`.
   Code that consumes/produces ITCH messages can be generic over those two
   traits and works equally well over TCP today or MoldUDP64 tomorrow.

3. **Length-prefixed framing on TCP.** Each message is preceded by a 2-byte
   big-endian length (the same convention used by SoupBinTCP and MoldUDP64 to
   carry the inner ITCH payload), so wire captures are easy to reason about.

4. **Bounded buffering.** The codec rejects any frame larger than
   `MAX_MESSAGE_LEN` (1 KiB), which is plenty above the largest ITCH 5.0
   message (50 bytes) but caps memory if a peer sends garbage.

## Quick start

```bash
# Build everything
cargo build

# Run all unit + roundtrip tests
cargo test

# Terminal A — start the demo server
cargo run -p itch-server

# Terminal B — connect with the demo client
cargo run -p itch-client
```

You should see something like this in terminal B:

```
[ 32400000000000 ns] S system_event       code=StartOfMessages
[ 32400001000000 ns] S system_event       code=StartOfSystemHours
[ 32400002000000 ns] R stock_directory    AAPL     cat=NasdaqGlobalSelect status=Normal round_lot=100
[ 32400003000000 ns] H trading_action     AAPL     state=Trading
[ 32400004000000 ns] S system_event       code=StartOfMarketHours
[ 32400005000000 ns] A add_order          ref=1001 Buy    500 AAPL     @ 192.5000
[ 32400006000000 ns] F add_order_mpid     ref=1002 Sell   300 AAPL     @ 192.5500 mpid=NSDQ
...
```

Override the bind / connect address with `ITCH_BIND` and `ITCH_SERVER`:

```bash
ITCH_BIND=0.0.0.0:9100   cargo run -p itch-server
ITCH_SERVER=host:9100    cargo run -p itch-client
```

## Supported messages (ITCH 5.0)

All 20 message types from the spec are implemented end-to-end (encode +
decode + roundtrip test):

| Tag | Name                          | Body |
|-----|-------------------------------|-----:|
| `S` | System Event                  |   11 |
| `R` | Stock Directory               |   38 |
| `H` | Stock Trading Action          |   24 |
| `Y` | Reg SHO Restriction           |   19 |
| `L` | Market Participant Position   |   25 |
| `V` | MWCB Decline Level            |   34 |
| `W` | MWCB Status                   |   11 |
| `K` | IPO Quoting Period Update     |   27 |
| `A` | Add Order                     |   35 |
| `F` | Add Order with MPID           |   39 |
| `E` | Order Executed                |   30 |
| `C` | Order Executed with Price     |   35 |
| `X` | Order Cancel                  |   22 |
| `D` | Order Delete                  |   18 |
| `U` | Order Replace                 |   34 |
| `P` | Trade (Non-Cross)             |   43 |
| `Q` | Cross Trade                   |   39 |
| `B` | Broken Trade                  |   18 |
| `I` | NOII                          |   49 |
| `N` | Retail Price Improvement      |   19 |

(Body length excludes the 1-byte type tag.)

## What's next

- **v0.2 — `itch-source`.** Three traits — `MessageSource` (the feed),
  `SeqStore` (gap-recovery storage), `SubscriptionPolicy` (warmup) —
  that let anyone plug a matching engine, capture file, or database
  behind `itch-server`. See [`docs/ITCH-SOURCE.md`](docs/ITCH-SOURCE.md).
- **v0.3 — `itch-soup` + `itch-mold`.** Production unicast (SoupBinTCP
  3.00, with login + heartbeat + sequence-resume) and multicast
  (MoldUDP64 V1.00, with gap detection and a request server).
- **v0.5 — `itch-book`.** L2/L3 order-book reconstruction on top of
  `itch-protocol`. **`itch-replay`** for `.itch` capture files lands
  in the same milestone.

The full plan is in [`docs/ROADMAP.md`](docs/ROADMAP.md).

## License

Dual-licensed under MIT or Apache-2.0, at your option.
