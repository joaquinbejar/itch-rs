# Bench results — itch-rs

Tracking page for the Criterion + bench-hdr scenarios called out in
`docs/TESTING.md` §5. Numbers below are filled by hand from a
specific build / hardware combination — re-run on the
documented baseline to update.

## Methodology

Closed-loop service-time, single-thread. No allocations in the
measured loop on the receiver / encoder side. All numbers are
from a `cargo bench --release` Criterion run, mean of three
samples per scenario.

| Field                | Value                                |
|----------------------|--------------------------------------|
| Toolchain            | stable (see `rust-toolchain.toml`)   |
| Workspace baseline   | `v0.4` development tip               |
| Last filled          | 2026-05-06                           |

## Published numbers

The most recent end-to-end run is committed under
[`docs/benchmarks/2026-05-06/`](docs/benchmarks/2026-05-06/methodology.md):

- [`methodology.md`](docs/benchmarks/2026-05-06/methodology.md) —
  hardware, governor, Rust version, `RUSTFLAGS`, LTO, allocator,
  reproduction commands.
- [`decode_per_message.csv`](docs/benchmarks/2026-05-06/decode_per_message.csv)
  / [`encode_per_message.csv`](docs/benchmarks/2026-05-06/encode_per_message.csv)
  — per ITCH 5.0 message kind, mean / low / high in nanoseconds.
- [`throughput_1mib.txt`](docs/benchmarks/2026-05-06/throughput_1mib.txt)
  — Criterion-rendered 1 MiB mixed-workload summary.
- [`itch_soup_raw.txt`](docs/benchmarks/2026-05-06/itch_soup_raw.txt)
  / [`itch_mold_raw.txt`](docs/benchmarks/2026-05-06/itch_mold_raw.txt)
  — `SoupCodec` / `MoldPacket` codec benches.
- `*_hdr.txt` — placeholders for the dedicated `bench-hdr` HDR-
  histogram harness (deferred to its own ticket; see each file's
  preamble for status).

The dated folder is the source of truth — the per-case tables below
remain as a quick-reference index. Re-run on a fresh host, drop the
output under `docs/benchmarks/<YYYY-MM-DD>/`, and link it here.

## Scenarios

### 1. `itch-protocol` — per-message decode / encode (case 1, 2)

**Bench targets:**

```
crates/itch-protocol/benches/decode.rs
crates/itch-protocol/benches/encode.rs
crates/itch-protocol/benches/throughput.rs
```

**Run:**

```bash
cargo bench -p itch-protocol -- --save-baseline v0.4.0
```

| Op                    | p50 | p99 | p99.9 | p99.99 | Notes              |
|-----------------------|----:|----:|------:|-------:|--------------------|
| `Message::decode`     | TBD | TBD | TBD   | TBD    | per-variant table  |
| `Message::encode`     | TBD | TBD | TBD   | TBD    | per-variant table  |
| 1 MiB throughput      | TBD | TBD | TBD   | TBD    | mixed workload     |

### 2. `itch-soup` — packet codec (case 4, partial)

**Bench target:**

```
crates/itch-soup/benches/soup_codec.rs
```

Covers `SoupCodec::encode` / `decode` for every packet variant plus
a back-to-back streaming scenario over the full workload.

**Run:**

```bash
cargo bench -p itch-soup -- --save-baseline v0.4.0
```

| Op                          | p50 | p99 | p99.9 | p99.99 | Notes                 |
|-----------------------------|----:|----:|------:|-------:|-----------------------|
| Encode `LoginRequest`       | TBD | TBD | TBD   | TBD    | 46-byte fixed payload |
| Encode `SequencedData`      | TBD | TBD | TBD   | TBD    | 12-byte ITCH inner    |
| Decode `ServerHeartbeat`    | TBD | TBD | TBD   | TBD    | 3-byte wire           |
| Decode `LoginAccepted`      | TBD | TBD | TBD   | TBD    | 33-byte wire          |
| Streaming decode (10 pkts)  | TBD | TBD | TBD   | TBD    | back-to-back          |

**Deferred (post-MVP):** in-process `SoupServer` ↔ `SoupConnection`
framed-receive throughput plus the `bench-hdr` p99 / p99.9 latency
variant per `.claude/skills/bench-hdr`. Tracking under a follow-up
issue.

### 3. `itch-mold` — packet codec (case 5, partial)

**Bench target:**

```
crates/itch-mold/benches/mold_codec.rs
```

Covers `MoldPacket::encode` / `decode` for heartbeat,
end-of-session, single-block (38 B), and 10-block (38 B each)
packets.

**Run:**

```bash
cargo bench -p itch-mold -- --save-baseline v0.4.0
```

| Op                                 | p50 | p99 | p99.9 | p99.99 | Notes              |
|------------------------------------|----:|----:|------:|-------:|--------------------|
| Encode heartbeat (header-only)     | TBD | TBD | TBD   | TBD    | 20 B               |
| Encode 1 × 38-B data packet        | TBD | TBD | TBD   | TBD    | 60 B               |
| Encode 10 × 38-B data packet       | TBD | TBD | TBD   | TBD    | 420 B              |
| Decode 10 × 38-B data packet       | TBD | TBD | TBD   | TBD    | 420 B              |

**Deferred (post-MVP):** in-process multicast-loopback
publisher ↔ receiver throughput plus the `bench-hdr` p99 / p99.9
latency variant. Skip-not-fail when the host cannot loopback
multicast (not all CI runners do). Tracking under a follow-up
issue.

## Comparing across baselines

```bash
cargo bench --workspace -- --baseline v0.4.0
```

Criterion writes to `target/criterion/`; `bench-hdr` numbers (when
the latency-variant benches land) write to `target/bench-hdr/*.hgrm`
and are uploaded as CI artifacts on benchmark-publish runs.

## See also

- `docs/TESTING.md` §5 — bench scenarios catalog
- `.claude/skills/bench-hdr/SKILL.md` — methodology for the latency
  bench variant
- `rules/global_rules.md` — Performance Discipline
