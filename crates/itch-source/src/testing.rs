//! Contract tests every `SeqStore` implementation must pass.
//!
//! Third-party backends should call `assert_seq_store_contract`
//! from their own test suite to guarantee they obey the same
//! semantics as the in-tree default impls
//! ([`crate::impls::NullSeqStore`],
//! [`crate::impls::RingBufferSeqStore`]).

use itch_protocol::{enums::EventCode, messages::SystemEvent, Header, Message};

use crate::SeqStore;

/// Build a synthetic `SystemEvent` so contract tests have something
/// to store. Independent of any external fixture so the helper has
/// no upstream test dependency.
fn fixture(code: EventCode) -> Message {
    Message::SystemEvent(SystemEvent {
        header: Header::default(),
        event_code: code,
    })
}

/// Verify that `store` obeys the `SeqStore` contract.
///
/// The verification covers:
///
/// - **Empty store** — `latest = 0`, `earliest = 0`,
///   `range(_, _)` returns empty.
/// - **Single store** — after `store(1, m1)`, `latest = 1`,
///   `earliest <= 1`, `range(1, 1) = [(1, m1)]`.
/// - **Two consecutive stores** — `range(1, 2)` returns both;
///   `range(1, 100)` returns exactly the two stored.
/// - **Below earliest** — `range(0, 1)` returns empty (not an
///   error).
/// - **Idempotent re-store** of `(1, m1)` is a no-op.
///
/// `NullSeqStore` is exempt from the "store-then-read" assertions
/// because it advertises itself as drop-everything; this helper
/// special-cases that by reading `earliest()` after `store(1, _)` —
/// if it stays `0`, the implementation is treated as a "drop
/// everything" `SeqStore` and only the empty-store assertions are
/// enforced.
///
/// # Panics
///
/// Panics with a clear message on any contract violation.
pub async fn assert_seq_store_contract<S>(store: &S)
where
    S: SeqStore,
{
    let m1 = fixture(EventCode::StartOfMessages);
    let m2 = fixture(EventCode::StartOfSystemHours);

    // Empty store.
    let lat = store
        .latest()
        .await
        .expect("latest() on empty store must not error");
    let ear = store
        .earliest()
        .await
        .expect("earliest() on empty store must not error");
    assert_eq!(lat, 0, "latest() on empty store must return 0");
    assert_eq!(ear, 0, "earliest() on empty store must return 0");
    let r = store
        .range(1, 1)
        .await
        .expect("range() on empty store must not error");
    assert!(r.is_empty(), "range() on empty store must be empty");

    // Single store.
    store
        .store(1, &m1)
        .await
        .expect("store(1, m1) must not error");

    let lat_after_one = store.latest().await.expect("latest() must not error");
    if lat_after_one == 0 {
        // Drop-everything store (e.g., NullSeqStore). Only the
        // empty assertions are meaningful — verify they continue to
        // hold after store calls.
        let ear = store.earliest().await.expect("earliest() must not error");
        assert_eq!(ear, 0, "drop-everything store must keep earliest() at 0",);
        let r = store
            .range(1, 1)
            .await
            .expect("range() must not error on drop-everything store");
        assert!(
            r.is_empty(),
            "drop-everything store must keep range() empty",
        );
        // Idempotent re-store still must succeed.
        store
            .store(1, &m1)
            .await
            .expect("idempotent re-store must succeed");
        return;
    }

    assert_eq!(lat_after_one, 1, "latest() after store(1) must be 1");
    let ear = store.earliest().await.expect("earliest() must not error");
    assert!(
        ear <= 1,
        "earliest() after first store must be <= 1, got {ear}",
    );
    let r = store.range(1, 1).await.expect("range(1, 1) must not error");
    assert_eq!(r.len(), 1, "range(1, 1) must return exactly 1 message");
    assert_eq!(r[0].0, 1, "range(1, 1) must return seq 1");
    assert_eq!(r[0].1, m1, "range(1, 1) must return the stored message");

    // Two consecutive stores.
    store
        .store(2, &m2)
        .await
        .expect("store(2, m2) must not error");
    let r = store.range(1, 2).await.expect("range(1, 2) must not error");
    assert_eq!(r.len(), 2, "range(1, 2) must return both messages");
    assert_eq!(r[0].0, 1);
    assert_eq!(r[1].0, 2);
    let r = store
        .range(1, 100)
        .await
        .expect("range(1, 100) must not error");
    assert_eq!(r.len(), 2, "range(1, 100) must return exactly the 2 stored");

    // Below earliest.
    let r = store.range(0, 1).await.expect("range(0, 1) must not error");
    assert!(
        r.is_empty(),
        "range(below-earliest, _) must return empty (not an error)",
    );

    // Idempotent re-store.
    store
        .store(1, &m1)
        .await
        .expect("idempotent re-store of (1, m1) must succeed");
    let r = store
        .range(1, 100)
        .await
        .expect("range(1, 100) must not error after re-store");
    assert_eq!(r.len(), 2, "idempotent re-store must not duplicate");
}
