//! Background maintenance tasks for the broker.
//!
//! Each task runs on its own interval and performs housekeeping:
//! - Queue TTL expiration (x-expires)
//! - Message TTL expiration (message_ttl / per-message expiration)
//! - Deduplication cache eviction

use std::time::Instant;
use tracing::{debug, info};

use crate::state::Broker;
use crate::storage::wal::EntryType;

/// Spawn all background maintenance tasks.
pub fn spawn_all(broker: Broker) {
    tokio::spawn(queue_ttl_task(broker.clone()));
    tokio::spawn(message_ttl_task(broker.clone()));
    tokio::spawn(dedup_eviction_task(broker.clone()));
    tokio::spawn(delay_flush_task(broker.clone()));
    tokio::spawn(wal_compact_task(broker));
}

/// Periodically remove queues that have exceeded their x-expires TTL.
/// A queue expires when it has no consumers, no messages, and has been idle
/// longer than `options.expires`.
async fn queue_ttl_task(broker: Broker) {
    let mut interval = tokio::time::interval(crate::config::QUEUE_TTL_CHECK_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let mut expired = Vec::new();
        for entry in broker.queues.iter() {
            let (name, queue) = entry.pair();
            if let Some(ttl) = queue.options.expires {
                let is_idle = queue.listeners.is_empty() && queue.messages.len() == 0;
                if is_idle && queue.last_activity.elapsed() >= ttl {
                    expired.push(name.clone());
                }
            }
        }

        for name in &expired {
            broker.queues.remove(name);
            info!(queue = name.as_str(), "queue expired (x-expires)");
        }
    }
}

/// Periodically sweep queues and discard expired messages (message_ttl).
async fn message_ttl_task(broker: Broker) {
    let mut interval = tokio::time::interval(crate::config::MESSAGE_TTL_CHECK_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        for mut entry in broker.queues.iter_mut() {
            let queue = entry.value_mut();
            let mut expired_count = 0usize;
            // Drain expired messages from the front of the queue
            while let Some(msg) = queue.messages.peek_front() {
                if msg.is_expired() {
                    queue.messages.pop_front();
                    expired_count += 1;
                } else {
                    break;
                }
            }
            if expired_count > 0 {
                debug!(
                    queue = entry.key().as_str(),
                    expired_count, "expired messages removed"
                );
            }
        }
    }
}

/// Periodically evict stale entries from the dedup cache.
async fn dedup_eviction_task(broker: Broker) {
    let mut interval = tokio::time::interval(crate::config::DEDUP_EVICTION_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let now = Instant::now();
        let before = broker.dedup_cache.len();
        broker
            .dedup_cache
            .retain(|_, ts| now.duration_since(*ts) < crate::config::DEDUP_WINDOW);
        let evicted = before - broker.dedup_cache.len();
        if evicted > 0 {
            debug!(evicted, "dedup cache entries evicted");
        }
    }
}

/// Flush delayed messages that are ready for delivery.
async fn delay_flush_task(broker: Broker) {
    let mut interval = tokio::time::interval(crate::config::DELAY_FLUSH_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let ready = broker.delay_queue.drain_ready();
        for delayed in ready {
            if let Some(mut queue) = broker.queues.get_mut(&delayed.queue_name) {
                let msg_id = delayed.message.id;
                queue
                    .messages
                    .push_back(crate::queue::message::QueueMessage::Full(delayed.message));
                queue.last_activity = Instant::now();
                debug!(
                    queue = delayed.queue_name.as_str(),
                    msg_id, "delayed message enqueued"
                );
            }
        }
    }
}

/// Periodically compact the WAL by removing enqueue entries that have been acked.
/// This prevents unbounded WAL growth.
async fn wal_compact_task(broker: Broker) {
    let mut interval = tokio::time::interval(crate::config::WAL_COMPACT_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let wal = match broker.wal() {
            Some(w) => w,
            None => continue,
        };

        let entries = match wal.read_all() {
            Ok(e) => e,
            Err(_) => continue,
        };

        let entry_count = entries.len() as u64;
        if entry_count < crate::config::WAL_COMPACT_THRESHOLD {
            continue;
        }

        // Collect all acked message IDs
        let mut acked_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
        for entry in &entries {
            if entry.entry_type == EntryType::Ack && entry.data.len() >= 8 {
                let msg_id = u64::from_be_bytes(entry.data[..8].try_into().unwrap());
                acked_ids.insert(msg_id);
            }
        }

        if acked_ids.is_empty() {
            continue;
        }

        // Rebuild: keep only live entries (skip acked enqueues and their ack entries)
        let mut kept = 0u64;
        let mut removed = 0u64;
        if let Err(e) = wal.truncate() {
            debug!(error = %e, "WAL compaction truncate failed");
            continue;
        }

        for entry in &entries {
            match entry.entry_type {
                EntryType::Enqueue => {
                    // Parse msg_id from enqueue data (offset: 2+name_len -> 8 bytes)
                    let queue_len = u16::from_be_bytes([entry.data[0], entry.data[1]]) as usize;
                    if entry.data.len() >= 2 + queue_len + 8 {
                        let msg_id = u64::from_be_bytes(
                            entry.data[2 + queue_len..2 + queue_len + 8]
                                .try_into()
                                .unwrap(),
                        );
                        if acked_ids.contains(&msg_id) {
                            removed += 1;
                            continue; // Skip — this message was acked
                        }
                    }
                    let _ = wal.append(entry.entry_type, &entry.data);
                    kept += 1;
                }
                EntryType::Ack => {
                    // Skip ack entries for messages we just removed
                    if entry.data.len() >= 8 {
                        let msg_id = u64::from_be_bytes(entry.data[..8].try_into().unwrap());
                        if acked_ids.contains(&msg_id) {
                            removed += 1;
                            continue;
                        }
                    }
                    let _ = wal.append(entry.entry_type, &entry.data);
                    kept += 1;
                }
                _ => {
                    // Keep all declarations and bindings
                    let _ = wal.append(entry.entry_type, &entry.data);
                    kept += 1;
                }
            }
        }

        info!(before = entry_count, after = kept, removed, "WAL compacted");
    }
}
