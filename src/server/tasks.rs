// Copyright (c) 2026 Edilson Pateguana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: Edilson Pateguana
// Year: 2026
// File: tasks.rs
// Description: Background maintenance tasks (TTL expiration, dedup cache eviction, etc.).

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

/// Launches all background maintenance tasks on the Tokio runtime.
///
/// Spawns independent tasks for queue TTL expiration, message TTL
/// expiration, deduplication cache eviction, delayed message flushing,
/// and WAL compaction.
pub fn spawn_all(broker: Broker) {
    tokio::spawn(queue_ttl_task(broker.clone()));
    tokio::spawn(message_ttl_task(broker.clone()));
    tokio::spawn(dedup_eviction_task(broker.clone()));
    tokio::spawn(delay_flush_task(broker.clone()));
    tokio::spawn(wal_compact_task(broker));
}

/// Periodically removes queues that have exceeded their `x-expires` TTL
/// while idle (no consumers, no messages).
async fn queue_ttl_task(broker: Broker) {
    let mut interval = tokio::time::interval(crate::config::queue_ttl_check_interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let mut expired = Vec::new();
        for entry in broker.queues.iter() {
            let (name, queue) = entry.pair();
            if let Some(ttl) = queue.options.expires {
                let is_idle = queue.listeners.is_empty() && queue.messages.is_empty();
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

/// Periodically drains expired messages from the head of each queue,
/// respecting both queue-level and per-message TTL settings.
async fn message_ttl_task(broker: Broker) {
    let mut interval = tokio::time::interval(crate::config::message_ttl_check_interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        for mut entry in broker.queues.iter_mut() {
            let queue = entry.value_mut();
            let mut expired_count = 0usize;

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

/// Periodically evicts stale entries from the broker-wide message
/// deduplication cache based on the configured time window.
async fn dedup_eviction_task(broker: Broker) {
    let mut interval = tokio::time::interval(crate::config::dedup_eviction_interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let now = Instant::now();
        let before = broker.dedup_cache.len();
        broker
            .dedup_cache
            .retain(|_, ts| now.duration_since(*ts) < crate::config::dedup_window());
        let evicted = before - broker.dedup_cache.len();
        if evicted > 0 {
            debug!(evicted, "dedup cache entries evicted");
        }
    }
}

/// Periodically moves messages whose delay has elapsed from the
/// global delay queue into their target queues for delivery.
async fn delay_flush_task(broker: Broker) {
    let mut interval = tokio::time::interval(crate::config::delay_flush_interval());
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

/// Periodically compacts the WAL by rebuilding it without entries
/// for messages that have already been acknowledged.
async fn wal_compact_task(broker: Broker) {
    let mut interval = tokio::time::interval(crate::config::wal_compact_interval());
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        interval.tick().await;

        let wal = broker.wal().clone();
        let threshold = crate::config::wal_compact_threshold();

        // Offload CPU-bound WAL compaction to the blocking thread pool
        // to avoid stalling the tokio event loop.
        let result = tokio::task::spawn_blocking(move || {
            let entries = match wal.read_all() {
                Ok(e) => e,
                Err(_) => return None,
            };

            let entry_count = entries.len() as u64;
            if entry_count < threshold {
                return None;
            }

            let mut acked_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();

            for entry in &entries {
                if entry.entry_type == EntryType::Ack && entry.data.len() >= 8 {
                    let msg_id = u64::from_be_bytes(entry.data[..8].try_into().unwrap());
                    acked_ids.insert(msg_id);
                }
            }

            if acked_ids.is_empty() {
                return None;
            }

            let mut kept = 0u64;
            let mut removed = 0u64;
            if wal.truncate().is_err() {
                return None;
            }

            // TODO: Too nested code
            for entry in &entries {
                match entry.entry_type {
                    EntryType::Enqueue => {
                        let queue_len = u16::from_be_bytes([entry.data[0], entry.data[1]]) as usize;
                        if entry.data.len() >= 2 + queue_len + 8 {
                            let msg_id = u64::from_be_bytes(
                                entry.data[2 + queue_len..2 + queue_len + 8]
                                    .try_into()
                                    .unwrap(),
                            );
                            if acked_ids.contains(&msg_id) {
                                removed += 1;
                                continue;
                            }
                        }
                        let _ = wal.append(entry.entry_type, &entry.data);
                        kept += 1;
                    }
                    EntryType::Ack => {
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
                        let _ = wal.append(entry.entry_type, &entry.data);
                        kept += 1;
                    }
                }
            }

            Some((entry_count, kept, removed))
        })
        .await;

        if let Ok(Some((before, after, removed))) = result {
            info!(before, after, removed, "WAL compacted");
        }
    }
}
