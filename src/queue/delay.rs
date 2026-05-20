//! Delayed message delivery.
//!
//! Messages published with an `x-delay` header (milliseconds) are held in a
//! delay buffer and only enqueued into their target queue after the delay expires.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::queue::Message;

/// A delayed message waiting to be enqueued.
pub struct DelayedMessage {
    pub deliver_at: Instant,
    pub queue_name: String,
    pub message: Message,
}

/// Thread-safe delay buffer using a BTreeMap keyed by delivery time.
pub struct DelayQueue {
    inner: Mutex<BTreeMap<(Instant, u64), DelayedMessage>>,
    next_id: std::sync::atomic::AtomicU64,
}

impl DelayQueue {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(BTreeMap::new()),
            next_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Schedule a message for delayed delivery.
    pub fn schedule(&self, queue_name: String, message: Message, delay: Duration) {
        let deliver_at = Instant::now() + delay;
        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let delayed = DelayedMessage {
            deliver_at,
            queue_name,
            message,
        };
        self.inner.lock().unwrap().insert((deliver_at, id), delayed);
    }

    /// Drain all messages that are ready for delivery.
    pub fn drain_ready(&self) -> Vec<DelayedMessage> {
        let now = Instant::now();
        let mut inner = self.inner.lock().unwrap();

        // Split at the first key that is still in the future
        let split_key = (now, u64::MAX);
        let remaining = inner.split_off(&split_key);
        // Everything left in `inner` is ready (deliver_at <= now)
        let ready: Vec<DelayedMessage> = std::mem::replace(&mut *inner, remaining)
            .into_iter()
            .map(|(_, v)| v)
            .collect();

        ready
    }

    pub fn len(&self) -> usize {
        self.inner.lock().unwrap().len()
    }
}
