# itch-source-redis

Redis-backed `SeqStore` implementation for distributed ITCH 5.0 publishers.

## Overview

`RedisSeqStore` allows multiple SoupBinTCP or MoldUDP64 frontends to share a single distributed message cache, enabling reconnect-with-resume and retransmission across instances.

## Quick Start

```rust,no_run
use itch_source_redis::{RedisSeqStore, RedisSeqStoreConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = RedisSeqStoreConfig {
        url: "redis://127.0.0.1:6379".to_string(),
        ..Default::default()
    };

    let store = RedisSeqStore::connect(config).await?;
    Ok(())
}
```

## Storage

- `itch:seq:msg:<seq>` — encoded Message bytes, EX ttl
- `itch:seq:latest` — latest sequence number, EX (ttl * 2)
- `itch:seq:floor:<seq>` — floor markers for earliest() computation, EX ttl

## Traits

Implements `itch_source::SeqStore` with full async support.

## Testing

Run integration tests against a local Redis. Tests are skip-not-fail when no reachable Redis is found, so they are safe to run in any environment:

```bash
# In one terminal:
docker run --rm -p 6379:6379 redis:7

# In another:
ITCH_REDIS_URL=redis://127.0.0.1:6379 \
    cargo test -p itch-source-redis --test integration
```

Without `ITCH_REDIS_URL` (or with an unreachable / authenticating peer) every test prints a "skipping" line and returns Ok; CI default is therefore green even on a host with no Redis.

## References

- `docs/ITCH-SOURCE.md` § 12.2: Worked example
- `docs/adr/0012-data-source-abstraction.md`: SeqStore trait
