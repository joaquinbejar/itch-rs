//! Multi-symbol routing of an ITCH 5.0 message stream into per-symbol
//! [`L2Book`] (and, under `feature = "l3"`, [`L3Book`]) instances
//! keyed by [`StockLocate`].
//!
//! `BookManager` is the entry point for a global multi-symbol feed:
//! every message's `header.stock_locate` is read once and the message
//! is dispatched to the corresponding per-symbol books, lazily created
//! on first sight. The 20-variant exhaustive match over [`Message`]
//! lives inside [`L2Book::apply`] / [`L3Book::apply`] — the manager
//! itself only inspects [`Message::StockDirectory`] (`R`) to populate
//! a directory cache.
//!
//! ## Lazy creation
//!
//! When [`BookManager::apply`] sees a message for a [`StockLocate`]
//! not yet in its book index, it inserts a fresh [`L2Book::new`] (and
//! [`L3Book::new`] under `feature = "l3"`) **before** dispatching.
//! Unknown locates are not an error — the consumer can match the
//! symbol later via [`BookManager::symbol`] once an `R` Stock
//! Directory message arrives for that locate.
//!
//! ## Sequence counter
//!
//! [`BookManager::last_seq`] is a monotonic counter bumped on every
//! [`BookManager::apply`] call (regardless of whether the message
//! moved any per-symbol state). It is provided for observability —
//! the manager itself does not gap-detect; that is the transport
//! layer's job.
//!
//! ## Async runner
//!
//! When the `tokio-stream` feature is enabled, [`BookManager::run`]
//! drains a `futures::Stream<Item = Result<Message, ProtocolError>>`
//! into the manager. A decode error from the upstream stream surfaces
//! as [`BookError::Protocol`]; an apply-time error (e.g.
//! [`BookError::OverExecution`]) surfaces unchanged.

use std::collections::HashMap;

use itch_protocol::{Message, Stock, StockLocate};

use crate::book::L2Book;
use crate::error::BookError;
#[cfg(feature = "l3")]
use crate::l3::L3Book;

/// Multi-symbol router that dispatches an ITCH 5.0 message stream to
/// per-symbol [`L2Book`] (and, under `feature = "l3"`, [`L3Book`])
/// instances by [`StockLocate`].
///
/// See the module-level documentation for the dispatch rules.
#[derive(Debug, Default)]
pub struct BookManager {
    directory: HashMap<StockLocate, Stock>,
    l2: HashMap<StockLocate, L2Book>,
    #[cfg(feature = "l3")]
    l3: HashMap<StockLocate, L3Book>,
    last_seq: u64,
}

impl BookManager {
    /// Construct an empty manager. No books exist until messages are
    /// applied; the directory cache is empty.
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sequence of the last applied message.
    ///
    /// Starts at 0 on a fresh manager and increments by 1 on every
    /// [`Self::apply`] call. Provided for observability — the manager
    /// does not gap-detect; that is the transport layer's job.
    #[inline]
    #[must_use]
    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    /// Borrow the [`L2Book`] for `locate`, if one exists.
    ///
    /// Returns `None` until at least one message with
    /// `header.stock_locate == locate` has been applied. The L2 book
    /// is created lazily on the first matching [`Self::apply`] call.
    #[inline]
    #[must_use]
    pub fn l2(&self, locate: StockLocate) -> Option<&L2Book> {
        self.l2.get(&locate)
    }

    /// Borrow the [`L3Book`] for `locate`, if one exists.
    ///
    /// Returns `None` until at least one message with
    /// `header.stock_locate == locate` has been applied. The L3 book
    /// is created lazily on the first matching [`Self::apply`] call.
    ///
    /// Available only when the `l3` feature is enabled (default).
    #[cfg(feature = "l3")]
    #[inline]
    #[must_use]
    pub fn l3(&self, locate: StockLocate) -> Option<&L3Book> {
        self.l3.get(&locate)
    }

    /// Look up the [`Stock`] symbol associated with `locate`.
    ///
    /// Returns `None` until an `R` Stock Directory message for
    /// `locate` has been applied. Per ITCH 5.0 the daily
    /// `stock_locate -> stock` mapping arrives as `R` messages at
    /// session start; until then the manager only knows the locate
    /// integer.
    #[inline]
    #[must_use]
    pub fn symbol(&self, locate: StockLocate) -> Option<&Stock> {
        self.directory.get(&locate)
    }

    /// Borrow the full directory cache.
    ///
    /// Each entry is populated on the first `R` Stock Directory
    /// message for its [`StockLocate`].
    #[inline]
    #[must_use]
    pub fn directory(&self) -> &HashMap<StockLocate, Stock> {
        &self.directory
    }

    /// Iterate every [`StockLocate`] that has at least one book.
    ///
    /// The order is unspecified and stable only within a single
    /// snapshot of the manager.
    pub fn locates(&self) -> impl Iterator<Item = StockLocate> + '_ {
        self.l2.keys().copied()
    }

    /// Apply one ITCH 5.0 message to the manager.
    ///
    /// Reads `msg.header().stock_locate`, lazily creates the
    /// per-symbol [`L2Book`] (and [`L3Book`] under `feature = "l3"`)
    /// if absent, and dispatches the message to each book in turn.
    /// The 20-variant match lives inside [`L2Book::apply`] /
    /// [`L3Book::apply`]; the manager only inspects
    /// [`Message::StockDirectory`] to populate its directory cache.
    ///
    /// Always bumps [`Self::last_seq`] by 1, even if the message had
    /// no per-symbol effect.
    ///
    /// # Errors
    ///
    /// - Any [`BookError`] returned by [`L2Book::apply`] or, under
    ///   `feature = "l3"`, [`L3Book::apply`]. The L2 book is applied
    ///   first; if it errors, the L3 book is not consulted for that
    ///   message. [`Self::last_seq`] has already been bumped at the
    ///   point of error — the manager treats the message as observed
    ///   even when its application failed downstream.
    pub fn apply(&mut self, msg: &Message) -> Result<(), BookError> {
        self.last_seq += 1;
        if let Message::StockDirectory(r) = msg {
            self.directory.insert(r.header.stock_locate, r.stock);
        }
        let locate = msg.header().stock_locate;
        let l2 = self.l2.entry(locate).or_insert_with(|| L2Book::new(locate));
        l2.apply(msg)?;
        #[cfg(feature = "l3")]
        {
            let l3 = self.l3.entry(locate).or_insert_with(|| L3Book::new(locate));
            l3.apply(msg)?;
        }
        Ok(())
    }

    /// Drain a `futures::Stream` of decoded messages into the manager.
    ///
    /// Available only when the `tokio-stream` feature is enabled.
    /// The stream item type is `Result<Message, ProtocolError>` so
    /// upstream decode errors surface as [`BookError::Protocol`].
    ///
    /// # Errors
    ///
    /// - [`BookError::Protocol`] if the upstream stream yields a
    ///   decode error. The runner stops at the first such error.
    /// - Any [`BookError`] surfaced by [`Self::apply`] on a
    ///   successfully-decoded message.
    #[cfg(feature = "tokio-stream")]
    pub async fn run<S>(&mut self, mut stream: S) -> Result<(), BookError>
    where
        S: futures::Stream<Item = Result<Message, itch_protocol::ProtocolError>> + Unpin,
    {
        use futures::StreamExt;
        while let Some(msg) = stream.next().await {
            self.apply(&msg?)?;
        }
        Ok(())
    }
}
