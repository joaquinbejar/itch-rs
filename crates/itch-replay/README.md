# itch-replay

Streams ITCH messages from `.itch` capture files. Supports NASDAQ Glimpse archive format (length-prefix sequence) and raw concatenated-bodies format.

## Quick Start

```rust
use itch_replay::{iter_messages, CaptureFormat};
use std::fs::File;

let file = File::open("capture.itch")?;
for msg in iter_messages(file, CaptureFormat::Glimpse) {
    let msg = msg?;
    println!("{:?}", msg);
}
```

## Features

- **Glimpse format** — each message preceded by u16 BE length (= 1 + body len).
- **RawBodies format** — concatenated message bodies with no length prefix.
- **Sync iterator** — decode on-demand without async runtime.
- **Async adapter** (feature `tokio`) — plug into transport streams.

## Error Handling

Messages are returned as `Result<Message, ReplayError>`. Errors distinguish:

- Truncated input at a specific byte offset.
- Protocol decoding errors.
- Frames exceeding `MAX_MESSAGE_LEN` (1024 bytes).
