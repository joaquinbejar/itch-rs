# itch-rs

Convenience meta-crate re-exporting the per-layer NASDAQ TotalView-ITCH 5.0 crates behind feature flags.

Per ADR-0007 the canonical distribution is the per-layer crates (`itch-protocol`, `itch-tcp`, `itch-soup`, `itch-mold`, `itch-replay`). They are independently versioned and are what production users should depend on. This meta-crate exists for casual consumers who would rather opt into a single dependency and pick layers via Cargo features.

## Layout

| Feature   | Module               | Re-exports        |
|-----------|----------------------|-------------------|
| `protocol`| `itch_rs::protocol`  | `itch-protocol` (always on) |
| `tcp`     | `itch_rs::tcp`       | `itch-tcp`        |
| `soup`    | `itch_rs::soup`      | `itch-soup`       |
| `mold`    | `itch_rs::mold`      | `itch-mold`       |
| `replay`  | `itch_rs::replay`    | `itch-replay`     |
| `full`    | all of the above     |                   |

## Usage

```toml
[dependencies]
# Just the protocol types and codec:
itch-rs = "0.1"

# Plus a transport:
itch-rs = { version = "0.1", features = ["soup"] }

# Everything:
itch-rs = { version = "0.1", features = ["full"] }
```

## Quickstart

```bash
# In one terminal:
cargo run -p itch-server

# In another:
cargo run -p itch-rs --example quickstart --features tcp
```

The example connects to `127.0.0.1:9100` and prints the first 5 decoded messages.

## Notes

- `#![forbid(unsafe_code)]`.
- No code beyond re-exports and rustdoc — keep this crate dumb.
- For end-to-end demos see the `itch-server` and `itch-client` binaries in the workspace.
