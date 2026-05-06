#![forbid(unsafe_code)]
#![doc = include_str!("../README.md")]

use itch_protocol::{Message, ProtocolError};
use std::io::Read;
use thiserror::Error;

/// Captured message format.
#[derive(Copy, Clone, Debug)]
pub enum CaptureFormat {
    /// Glimpse archive format: each message preceded by u16 BE length (= 1 + body len).
    Glimpse,
    /// Raw concatenated message bodies (no length prefix).
    RawBodies,
}

/// Replay errors.
#[derive(Debug, Error, Clone)]
#[non_exhaustive]
pub enum ReplayError {
    #[error("I/O error: {0}")]
    Io(String),
    #[error("protocol error: {0}")]
    Protocol(#[from] ProtocolError),
    #[error("truncated at offset {offset}: {0}", .msg)]
    Truncated { offset: u64, msg: String },
    #[error("frame too large at offset {offset}: got {got}, max {max}")]
    FrameTooLarge { offset: u64, got: usize, max: usize },
}

impl From<std::io::Error> for ReplayError {
    fn from(e: std::io::Error) -> Self {
        ReplayError::Io(e.to_string())
    }
}

/// Iterator over ITCH messages from a reader in the given format.
pub struct MessageIterator<R: Read> {
    reader: R,
    format: CaptureFormat,
    offset: u64,
    buf: Vec<u8>,
}

impl<R: Read> MessageIterator<R> {
    /// Create a new iterator over messages in the given format.
    pub fn new(reader: R, format: CaptureFormat) -> Self {
        Self {
            reader,
            format,
            offset: 0,
            buf: vec![0u8; 1024],
        }
    }

    fn read_exact(&mut self, n: usize) -> Result<&[u8], ReplayError> {
        if self.buf.len() < n {
            self.buf.resize(n, 0);
        }
        let mut read_total = 0;
        while read_total < n {
            let nread = self
                .reader
                .read(&mut self.buf[read_total..n])
                .map_err(|e| ReplayError::Io(e.to_string()))?;
            if nread == 0 {
                return Err(ReplayError::Truncated {
                    offset: self.offset + read_total as u64,
                    msg: format!("expected {} bytes, got EOF", n - read_total),
                });
            }
            read_total += nread;
        }
        Ok(&self.buf[..n])
    }
}

impl<R: Read> Iterator for MessageIterator<R> {
    type Item = Result<Message, ReplayError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.format {
            CaptureFormat::Glimpse => self.next_glimpse(),
            CaptureFormat::RawBodies => self.next_raw_bodies(),
        }
    }
}

impl<R: Read> MessageIterator<R> {
    fn next_glimpse(&mut self) -> Option<Result<Message, ReplayError>> {
        // Read u16 BE length (= 1 + body len).
        let len_bytes = match self.read_exact(2) {
            Ok(b) => b,
            Err(e) => return Some(Err(e)),
        };
        let len = u16::from_be_bytes([len_bytes[0], len_bytes[1]]) as usize;
        if len == 0 {
            return None; // End of stream.
        }

        const MAX_MESSAGE_LEN: usize = 1024;
        if len > MAX_MESSAGE_LEN {
            return Some(Err(ReplayError::FrameTooLarge {
                offset: self.offset,
                got: len,
                max: MAX_MESSAGE_LEN,
            }));
        }

        let frame = match self.read_exact(len) {
            Ok(b) => b.to_vec(),
            Err(e) => return Some(Err(e)),
        };

        self.offset += 2 + len as u64;

        match Message::decode(&frame) {
            Ok(msg) => Some(Ok(msg)),
            Err(e) => Some(Err(ReplayError::Protocol(e))),
        }
    }

    fn next_raw_bodies(&mut self) -> Option<Result<Message, ReplayError>> {
        // Read bytes incrementally until we have a complete message.
        let mut frame = Vec::new();
        const MAX_MESSAGE_LEN: usize = 1024;

        loop {
            // Try to decode what we have so far.
            if !frame.is_empty() {
                match Message::decode(&frame) {
                    Ok(msg) => {
                        self.offset += frame.len() as u64;
                        return Some(Ok(msg));
                    }
                    Err(ProtocolError::Truncated { need, .. }) => {
                        // Need more bytes; keep reading.
                        let need_more = need - frame.len();
                        if need > MAX_MESSAGE_LEN {
                            return Some(Err(ReplayError::FrameTooLarge {
                                offset: self.offset,
                                got: need,
                                max: MAX_MESSAGE_LEN,
                            }));
                        }
                        let chunk = match self.read_exact(need_more) {
                            Ok(b) => b.to_vec(),
                            Err(e) => return Some(Err(e)),
                        };
                        frame.extend_from_slice(&chunk);
                    }
                    Err(e) => return Some(Err(ReplayError::Protocol(e))),
                }
            } else {
                // Read initial byte.
                let initial = match self.read_exact(1) {
                    Ok(b) => b.to_vec(),
                    Err(e) => {
                        // EOF on initial read is normal (no more messages).
                        if let ReplayError::Truncated { .. } = &e {
                            return None;
                        }
                        return Some(Err(e));
                    }
                };
                frame = initial;
            }
        }
    }
}

/// Create an iterator over ITCH messages in the given format.
#[must_use]
pub fn iter_messages<R: Read>(reader: R, format: CaptureFormat) -> MessageIterator<R> {
    MessageIterator::new(reader, format)
}

#[cfg(feature = "tokio")]
pub mod tokio_support {
    use super::*;
    use futures::stream::{self, Stream};
    use tokio::io::AsyncRead;

    /// Async stream over ITCH messages from an async reader.
    ///
    /// v0.4 ships a placeholder that yields an empty stream. The
    /// full async port lands alongside the v0.5 `itch-replay` API
    /// once the synchronous path has stabilised.
    pub fn stream_messages<R: AsyncRead + Unpin>(
        _reader: R,
        _format: CaptureFormat,
    ) -> impl Stream<Item = Result<Message, ReplayError>> {
        stream::iter(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use itch_protocol::enums::{Authenticity, FinancialStatus, MarketCategory, YesNo};
    use itch_protocol::primitives::{Shares, Stock, StockLocate, Timestamp, TrackingNumber};
    use itch_protocol::{Header, Message, StockDirectory};
    use std::io::Cursor;

    fn encode_and_write_glimpse(msg: &Message) -> Vec<u8> {
        let mut buf = vec![0u8; msg.encoded_len()];
        msg.encode(&mut buf).unwrap();

        let len = buf.len() as u16;
        let mut result = vec![0u8; 2 + len as usize];
        result[0..2].copy_from_slice(&len.to_be_bytes());
        result[2..].copy_from_slice(&buf);
        result
    }

    fn encode_raw(msg: &Message) -> Vec<u8> {
        let mut buf = vec![0u8; msg.encoded_len()];
        msg.encode(&mut buf).unwrap();
        buf
    }

    #[test]
    fn test_glimpse_format_round_trip() {
        let msgs = vec![Message::StockDirectory(StockDirectory {
            header: Header {
                stock_locate: StockLocate::from_u16(1),
                tracking_number: TrackingNumber::from_u16(1),
                timestamp: Timestamp::from_u64(1000),
            },
            stock: Stock::new("AAPL"),
            market_category: MarketCategory::NasdaqGlobalSelect,
            financial_status: FinancialStatus::Normal,
            round_lot_size: Shares::from_u32(100),
            round_lots_only: YesNo::No,
            issue_classification: b'D',
            issue_subtype: [b'Z', b' '],
            authenticity: Authenticity::Live,
            ..Default::default()
        })];

        let mut data = Vec::new();
        for msg in &msgs {
            data.extend(encode_and_write_glimpse(msg));
        }

        let cursor = Cursor::new(data);
        let iter = MessageIterator::new(cursor, CaptureFormat::Glimpse);
        let decoded: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(decoded.len(), msgs.len());
    }

    #[test]
    fn test_raw_bodies_format_round_trip() {
        let msgs = vec![Message::StockDirectory(StockDirectory {
            header: Header {
                stock_locate: StockLocate::from_u16(1),
                tracking_number: TrackingNumber::from_u16(1),
                timestamp: Timestamp::from_u64(1000),
            },
            stock: Stock::new("AAPL"),
            market_category: MarketCategory::NasdaqGlobalSelect,
            financial_status: FinancialStatus::Normal,
            round_lot_size: Shares::from_u32(100),
            round_lots_only: YesNo::No,
            issue_classification: b'D',
            issue_subtype: [b'Z', b' '],
            authenticity: Authenticity::Live,
            ..Default::default()
        })];

        let mut data = Vec::new();
        for msg in &msgs {
            data.extend(encode_raw(msg));
        }

        let cursor = Cursor::new(data);
        let iter = MessageIterator::new(cursor, CaptureFormat::RawBodies);
        let decoded: Vec<_> = iter.collect::<Result<Vec<_>, _>>().unwrap();

        assert_eq!(decoded.len(), msgs.len());
    }

    #[test]
    fn test_truncated_glimpse() {
        let data = vec![0x00, 0x10]; // Announce 16 bytes but provide nothing.
        let cursor = Cursor::new(data);
        let mut iter = MessageIterator::new(cursor, CaptureFormat::Glimpse);

        match iter.next() {
            Some(Err(ReplayError::Truncated { offset, .. })) => {
                assert_eq!(offset, 2);
            }
            _ => panic!("expected Truncated error"),
        }
    }

    #[test]
    fn test_frame_too_large() {
        let data = vec![0xFF, 0xFF]; // Announce 65535 bytes (exceeds MAX_MESSAGE_LEN).
        let cursor = Cursor::new(data);
        let mut iter = MessageIterator::new(cursor, CaptureFormat::Glimpse);

        match iter.next() {
            Some(Err(ReplayError::FrameTooLarge { got, max, .. })) => {
                assert_eq!(got, 65535);
                assert_eq!(max, 1024);
            }
            _ => panic!("expected FrameTooLarge error"),
        }
    }
}
