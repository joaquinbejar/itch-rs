# itch-source-postgres

PostgreSQL-backed `SeqStore` implementation for durable ITCH 5.0 publisher replay.

## Overview

`PostgresSeqStore` persists encoded ITCH messages keyed by sequence number in a Postgres
table. Unlike the in-memory `RingBufferSeqStore` or the TTL-evicted `RedisSeqStore`,
PostgreSQL provides durability across publisher restart: a recovering SoupBinTCP client
can resume from any historical sequence, and a MoldUDP64 request server can answer
retransmission requests for messages that pre-date the current process lifetime.

## Quick Start

```rust,no_run
use itch_source_postgres::{PostgresSeqStore, PostgresSeqStoreConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = PostgresSeqStoreConfig {
        url: "postgres://localhost/itch".to_string(),
        ..Default::default()
    };

    let store = PostgresSeqStore::connect(config).await?;
    store.migrate().await?;
    Ok(())
}
```

## Schema

The store owns a single table with the schema below. The runtime `migrate()` call creates
it idempotently; alternatively, an operator can apply `migrations/0001_initial.sql`
directly via `psql -f`.

```sql
CREATE TABLE IF NOT EXISTS {table} (
    seq    BIGINT PRIMARY KEY,
    bytes  BYTEA  NOT NULL,
    stored TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_{table}_seq ON {table} (seq);
```

`{table}` is the value of `PostgresSeqStoreConfig::table` (default `itch_messages`).
Sequence numbers are stored as `BIGINT`, which is signed; this crate constrains the
domain to `0..=i64::MAX`. The 1-byte ITCH tag plus the message body is stored verbatim
in `bytes` — exactly what `Message::encode` produces.

## Traits

Implements `itch_source::SeqStore` with full async support.

## Testing

Run integration tests against a local Postgres. Tests are skip-not-fail when no reachable
Postgres is found, so they are safe to run in any environment:

```bash
# In one terminal:
docker run --rm -p 5432:5432 -e POSTGRES_PASSWORD=itch -e POSTGRES_DB=itch postgres:16

# In another:
ITCH_POSTGRES_URL=postgres://postgres:itch@127.0.0.1:5432/itch \
    cargo test -p itch-source-postgres --test integration
```

Without `ITCH_POSTGRES_URL` (or with an unreachable / authenticating peer) every test
prints a "skipping" line and returns Ok; CI default is therefore green even on a host
with no Postgres.

## References

- `docs/ITCH-SOURCE.md` § 12: SeqStore worked example
- `docs/adr/0012-data-source-abstraction.md`: SeqStore trait
