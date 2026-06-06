// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0
//
// File: stream.rs
// Description: Segment-based append-only log engine for stream queues.

//! Stream storage engine for `x-queue-type: stream` queues.
//!
//! Unlike classic queues where messages are removed on ACK, streams
//! use an append-only log. Consumers track offsets independently and
//! can replay from any position. Retention policies evict old segments.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

/// A contiguous chunk of stream messages.
///
/// Segments are append-only and become read-only once they reach
/// `max_segment_size`. Old segments are evicted by retention policies.
#[derive(Debug)]
pub struct StreamSegment {
    pub start_offset: u64,
    pub messages: Vec<StreamMessage>,
    pub created_at: Instant,
    pub size_bytes: u64,
}

impl StreamSegment {
    /// Creates a new segment starting at the given offset.
    pub fn new(start_offset: u64) -> Self {
        Self {
            start_offset,
            messages: Vec::new(),
            created_at: Instant::now(),
            size_bytes: 0,
        }
    }

    /// Appends a message and returns its offset.
    pub fn append(&mut self, body: Vec<u8>) -> u64 {
        let offset = self.start_offset + self.messages.len() as u64;
        self.size_bytes += body.len() as u64;
        self.messages.push(StreamMessage {
            offset,
            body,
            timestamp: Instant::now(),
        });
        offset
    }

    /// Returns the next offset that would be assigned.
    pub fn next_offset(&self) -> u64 {
        self.start_offset + self.messages.len() as u64
    }

    /// Returns the message at the given offset, if in this segment.
    pub fn get(&self, offset: u64) -> Option<&StreamMessage> {
        if offset < self.start_offset {
            return None;
        }
        let idx = (offset - self.start_offset) as usize;
        self.messages.get(idx)
    }
}

/// A single message in a stream segment.
#[derive(Debug, Clone)]
pub struct StreamMessage {
    pub offset: u64,
    pub body: Vec<u8>,
    pub timestamp: Instant,
}

/// Retention policy for stream segments.
///
/// ```ignore
/// let policy = RetentionPolicy {
///     max_age: Some(Duration::from_secs(3600)),
///     max_bytes: Some(1_000_000_000),
/// };
/// ```
#[derive(Debug, Clone)]
pub struct RetentionPolicy {
    /// Maximum age for segments. Segments older than this are evicted.
    pub max_age: Option<Duration>,
    /// Maximum total size in bytes. Oldest segments evicted first.
    pub max_bytes: Option<u64>,
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            max_age: Some(Duration::from_secs(7 * 24 * 3600)), // 7 days
            max_bytes: None,
        }
    }
}

/// Append-only log storage engine for stream queues.
///
/// Manages a sequence of segments, each containing a batch of messages.
/// Old segments are evicted based on the retention policy.
///
/// ```ignore
/// let mut store = StreamStore::new(1024 * 1024);
/// let offset = store.append(b"hello".to_vec());
/// let msg = store.read(offset).unwrap();
/// ```
pub struct StreamStore {
    /// Segments keyed by start offset, ordered.
    segments: BTreeMap<u64, StreamSegment>,
    /// Maximum size per segment before rolling to a new one.
    max_segment_size: u64,
    /// Retention policy for old segments.
    pub retention: RetentionPolicy,
    /// Global offset counter.
    next_offset: u64,
}

impl StreamStore {
    /// Creates a new stream store with the given segment size limit.
    pub fn new(max_segment_size: u64) -> Self {
        let mut segments = BTreeMap::new();
        segments.insert(0, StreamSegment::new(0));
        Self {
            segments,
            max_segment_size,
            retention: RetentionPolicy::default(),
            next_offset: 0,
        }
    }

    /// Appends a message to the stream. Returns the assigned offset.
    pub fn append(&mut self, body: Vec<u8>) -> u64 {
        // Roll segment if current one is full
        let current_start = self.current_segment_start();
        let needs_roll = self
            .segments
            .get(&current_start)
            .map(|s| s.size_bytes >= self.max_segment_size)
            .unwrap_or(true);

        if needs_roll {
            let new_start = self.next_offset;
            self.segments
                .insert(new_start, StreamSegment::new(new_start));
        }

        let current_start = self.current_segment_start();
        let segment = self.segments.get_mut(&current_start).unwrap();
        let offset = segment.append(body);
        self.next_offset = offset + 1;
        offset
    }

    /// Reads a message at the given offset.
    pub fn read(&self, offset: u64) -> Option<&StreamMessage> {
        // Find the segment containing this offset
        for (_, segment) in self.segments.iter().rev() {
            if offset >= segment.start_offset {
                return segment.get(offset);
            }
        }
        None
    }

    /// Returns messages starting from `from_offset`, up to `limit` entries.
    pub fn read_range(&self, from_offset: u64, limit: usize) -> Vec<&StreamMessage> {
        let mut results = Vec::new();
        let mut current = from_offset;

        'outer: for segment in self.segments.values() {
            if segment.next_offset() <= current {
                continue;
            }
            for msg in &segment.messages {
                if msg.offset >= current {
                    results.push(msg);
                    current = msg.offset + 1;
                    if results.len() >= limit {
                        break 'outer;
                    }
                }
            }
        }

        results
    }

    /// Returns the first (oldest) available offset in the stream.
    pub fn first_offset(&self) -> u64 {
        self.segments
            .values()
            .next()
            .map(|s| s.start_offset)
            .unwrap_or(0)
    }

    /// Returns the last (newest) offset written.
    pub fn last_offset(&self) -> u64 {
        self.next_offset.saturating_sub(1)
    }

    /// Applies the retention policy, evicting expired segments.
    pub fn apply_retention(&mut self) {
        let now = Instant::now();

        // Time-based retention
        if let Some(max_age) = self.retention.max_age {
            let expired: Vec<u64> = self
                .segments
                .iter()
                .filter(|(_, s)| now.duration_since(s.created_at) > max_age)
                .map(|(&k, _)| k)
                .collect();

            // Keep at least one segment
            for key in expired {
                if self.segments.len() > 1 {
                    self.segments.remove(&key);
                }
            }
        }

        // Size-based retention
        if let Some(max_bytes) = self.retention.max_bytes {
            while self.total_bytes() > max_bytes && self.segments.len() > 1 {
                let oldest_key = *self.segments.keys().next().unwrap();
                self.segments.remove(&oldest_key);
            }
        }
    }

    /// Returns the total size of all segments in bytes.
    pub fn total_bytes(&self) -> u64 {
        self.segments.values().map(|s| s.size_bytes).sum()
    }

    /// Returns the total number of messages across all segments.
    pub fn total_messages(&self) -> u64 {
        self.segments
            .values()
            .map(|s| s.messages.len() as u64)
            .sum()
    }

    /// Returns the number of segments.
    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    fn current_segment_start(&self) -> u64 {
        *self.segments.keys().next_back().unwrap_or(&0)
    }
}

/// Per-consumer offset tracking for stream queues.
///
/// Each consumer maintains its own position in the stream,
/// allowing independent reads without affecting other consumers.
#[derive(Debug, Clone)]
pub struct ConsumerOffset {
    pub consumer_tag: String,
    pub current_offset: u64,
}

/// Resolves the `x-stream-offset` argument to a concrete offset.
///
/// Supported values:
/// - `"first"` → oldest available offset
/// - `"last"` → newest offset
/// - `"next"` → newest offset + 1 (wait for new messages)
/// - numeric string → parsed as absolute offset
///
/// ```ignore
/// let offset = resolve_stream_offset("first", &store);
/// ```
pub fn resolve_stream_offset(value: &str, store: &StreamStore) -> u64 {
    match value {
        "first" => store.first_offset(),
        "last" => store.last_offset(),
        "next" => store.next_offset,
        _ => value.parse::<u64>().unwrap_or(store.first_offset()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_and_read_single_message() {
        let mut store = StreamStore::new(1024);
        let offset = store.append(b"hello".to_vec());
        assert_eq!(offset, 0);

        let msg = store.read(0).unwrap();
        assert_eq!(msg.body, b"hello");
        assert_eq!(msg.offset, 0);
    }

    #[test]
    fn append_multiple_messages() {
        let mut store = StreamStore::new(1024);
        let o1 = store.append(b"one".to_vec());
        let o2 = store.append(b"two".to_vec());
        let o3 = store.append(b"three".to_vec());

        assert_eq!(o1, 0);
        assert_eq!(o2, 1);
        assert_eq!(o3, 2);
        assert_eq!(store.total_messages(), 3);
    }

    #[test]
    fn read_range_returns_subset() {
        let mut store = StreamStore::new(1024);
        for i in 0..10 {
            store.append(format!("msg-{}", i).into_bytes());
        }

        let batch = store.read_range(3, 4);
        assert_eq!(batch.len(), 4);
        assert_eq!(batch[0].offset, 3);
        assert_eq!(batch[3].offset, 6);
    }

    #[test]
    fn segment_rolls_when_full() {
        // 10-byte segment limit → each message forces a roll
        let mut store = StreamStore::new(10);
        store.append(vec![0u8; 20]); // fills first segment
        store.append(vec![0u8; 5]); // rolls to second segment

        assert!(store.segment_count() >= 2);
    }

    #[test]
    fn size_based_retention_evicts_oldest() {
        // Segment size of 25 means each 30-byte message fills its own segment
        let mut store = StreamStore::new(25);
        store.retention.max_age = None; // disable time-based
        store.retention.max_bytes = Some(60);

        // Each append should trigger a roll (30 > 25 segment limit)
        store.append(vec![0u8; 30]); // segment 0: 30 bytes
        store.append(vec![0u8; 30]); // segment 1: 30 bytes
        store.append(vec![0u8; 30]); // segment 2: 30 bytes

        assert_eq!(store.segment_count(), 3);
        assert_eq!(store.total_bytes(), 90);

        store.apply_retention();

        // Should evict oldest segment to get under 60 bytes
        assert!(store.segment_count() <= 2);
        assert!(
            store.total_bytes() <= 60,
            "Expected <= 60 bytes, got {}",
            store.total_bytes()
        );
    }

    #[test]
    fn first_and_last_offset() {
        let mut store = StreamStore::new(1024);
        assert_eq!(store.first_offset(), 0);
        assert_eq!(store.last_offset(), 0); // underflow protection

        store.append(b"a".to_vec());
        store.append(b"b".to_vec());
        assert_eq!(store.first_offset(), 0);
        assert_eq!(store.last_offset(), 1);
    }

    #[test]
    fn resolve_offset_variants() {
        let mut store = StreamStore::new(1024);
        store.append(b"a".to_vec());
        store.append(b"b".to_vec());
        store.append(b"c".to_vec());

        assert_eq!(resolve_stream_offset("first", &store), 0);
        assert_eq!(resolve_stream_offset("last", &store), 2);
        assert_eq!(resolve_stream_offset("next", &store), 3);
        assert_eq!(resolve_stream_offset("1", &store), 1);
        assert_eq!(resolve_stream_offset("invalid", &store), 0);
    }

    #[test]
    fn read_nonexistent_offset_returns_none() {
        let store = StreamStore::new(1024);
        assert!(store.read(999).is_none());
    }
}
