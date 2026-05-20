use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::core::protocol::{Event, Frame};
use crate::queue::Message;
use crate::state::Broker;

pub async fn publish(conn_id: u64, channel_id: u16, broker: &Broker, headers: &[u8], body: &[u8]) {
    let headers_str = match std::str::from_utf8(headers) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid headers in publish");
            return;
        }
    };

    let mut exchange_name = "";
    let mut routing_key = "";
    let mut priority: u8 = 0;
    let mut per_msg_ttl: Option<Duration> = None;
    let mut user_headers = Vec::new();
    let mut message_id: Option<String> = None;

    for line in headers_str.split("\r\n") {
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            match k {
                "exchange" => exchange_name = v,
                "routing_key" => routing_key = v,
                "queue" => {
                    if exchange_name.is_empty() {
                        exchange_name = "";
                        routing_key = v;
                    }
                }
                "priority" => priority = v.parse().unwrap_or(0),
                "expiration" => {
                    per_msg_ttl = v.parse::<u64>().ok().map(Duration::from_millis);
                }
                "message-id" => message_id = Some(v.to_string()),
                _ => {
                    user_headers.extend_from_slice(line.as_bytes());
                    user_headers.extend_from_slice(b"\r\n");
                }
            }
        }
    }

    // Deduplication check
    if let Some(ref mid) = message_id {
        if broker.dedup_cache.contains_key(mid) {
            debug!(
                conn_id,
                message_id = mid.as_str(),
                "duplicate message skipped"
            );
            send_confirm(conn_id, broker, true).await;
            return;
        }
        broker.dedup_cache.insert(mid.clone(), Instant::now());
    }

    // Route through exchange (read lock on exchanges only)
    let target_queues = {
        let exchanges = broker.exchanges.read().await;
        let exchange = match exchanges.get(exchange_name) {
            Some(ex) => ex,
            None => {
                warn!(conn_id, exchange = exchange_name, "exchange does not exist");
                send_confirm(conn_id, broker, false).await;
                return;
            }
        };

        let msg_headers: HashMap<String, String> = headers_str
            .split("\r\n")
            .filter(|l| !l.is_empty())
            .filter_map(|l| l.split_once(':'))
            .filter(|(k, _)| *k != "exchange" && *k != "routing_key" && *k != "queue")
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        exchange.route(routing_key, &msg_headers)
    }; // exchange lock released

    if target_queues.is_empty() {
        debug!(
            conn_id,
            exchange = exchange_name,
            routing_key,
            "no matching bindings"
        );
        send_confirm(conn_id, broker, true).await;
        return;
    }

    let msg_body = body.to_vec();

    for queue_name in &target_queues {
        let msg_id = broker.alloc_msg_id();

        // Scope: lock only this queue via DashMap
        let delivery = {
            let mut queue_ref = match broker.queues.get_mut(queue_name.as_str()) {
                Some(q) => q,
                None => {
                    warn!(
                        conn_id,
                        queue = queue_name.as_str(),
                        "bound queue does not exist"
                    );
                    continue;
                }
            };
            let queue = queue_ref.value_mut();

            let expiration = match (queue.options.message_ttl, per_msg_ttl) {
                (Some(qt), Some(pt)) => Some(Instant::now() + qt.min(pt)),
                (Some(t), None) | (None, Some(t)) => Some(Instant::now() + t),
                (None, None) => None,
            };

            let effective_priority = if queue.options.max_priority > 0 {
                priority.min(queue.options.max_priority)
            } else {
                0
            };

            // Overflow: evict oldest if at max_length
            if let Some(max_len) = queue.options.max_length {
                while queue.messages.len() >= max_len {
                    if let Some(evicted) = queue.messages.pop_oldest() {
                        // Dead-letter evicted message
                        dead_letter_sync(broker, queue_name, evicted).await;
                    } else {
                        break;
                    }
                }
            }

            let msg = Message {
                id: msg_id,
                headers: user_headers.clone(),
                body: msg_body.clone(),
                priority: effective_priority,
                expiration,
                redelivered: false,
                delivery_count: 0,
            };

            // Touch queue activity timestamp
            queue.last_activity = Instant::now();

            match queue.next_target(broker) {
                None => {
                    queue.messages.push_back(msg);
                    debug!(conn_id, msg_id, queue = queue_name.as_str(), "queued");
                    None
                }
                Some((target_id, target_channel_id)) => {
                    queue.inflight.insert(msg_id, msg);
                    let msg_ref = queue.inflight.get(&msg_id).unwrap();
                    let frame = Frame::with_deliver(
                        target_channel_id,
                        msg_id,
                        &msg_ref.headers,
                        &msg_ref.body,
                    );

                    if let Some(mut cs) = broker.conn_state.get_mut(&target_id) {
                        if let Some(ch) = cs.channels.get_mut(&target_channel_id) {
                            ch.unacked_count += 1;
                        }
                    }

                    // Get connection handle (separate DashMap, no conflict)
                    broker
                        .connections
                        .get(&target_id)
                        .map(|h| (msg_id, frame, h.clone(), target_channel_id))
                }
            }
        }; // queue lock released

        // WAL: log the enqueue for durable queues
        if let Some(wal) = broker.wal() {
            let is_durable = broker
                .queues
                .get(queue_name.as_str())
                .map_or(false, |q| q.options.durable);
            if is_durable {
                let _ = wal.log_enqueue(queue_name, msg_id, &user_headers, &msg_body);
            }
        }

        if let Some((msg_id, frame, handle, target_channel_id)) = delivery {
            let _ = handle.tx.send(frame).await;
            info!(
                conn_id,
                msg_id,
                target = handle.id,
                target_channel_id,
                "delivered"
            );
        }
    }

    send_confirm(conn_id, broker, true).await;
}

pub async fn ack(conn_id: u64, channel_id: u16, broker: &Broker, headers: &[u8]) {
    let msg_id = match super::parse_msg_id(headers) {
        Some(id) => id,
        None => {
            warn!(conn_id, "invalid ack headers");
            return;
        }
    };

    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        if let Some(ch) = cs.channels.get_mut(&channel_id) {
            if ch.unacked_count > 0 {
                ch.unacked_count -= 1;
            }
        }
    }

    for mut entry in broker.queues.iter_mut() {
        if entry.value_mut().inflight.remove(&msg_id).is_some() {
            // WAL: log the ack
            if let Some(wal) = broker.wal() {
                let _ = wal.log_ack(msg_id);
            }
            info!(conn_id, msg_id, "acked");
            return;
        }
    }
    warn!(conn_id, msg_id, "ack for unknown message");
}

pub async fn nack(conn_id: u64, channel_id: u16, broker: &Broker, headers: &[u8]) {
    let headers_str = match std::str::from_utf8(headers) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid nack headers");
            return;
        }
    };

    let msg_id = match super::parse_msg_id(headers) {
        Some(id) => id,
        None => {
            warn!(conn_id, "invalid nack headers");
            return;
        }
    };

    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        if let Some(ch) = cs.channels.get_mut(&channel_id) {
            if ch.unacked_count > 0 {
                ch.unacked_count -= 1;
            }
        }
    }

    let requeue = headers_str
        .split("\r\n")
        .filter_map(|l| l.split_once(':'))
        .any(|(k, v)| k == "requeue" && v == "true");

    // Find and remove the inflight message
    let found = {
        let mut result = None;
        for mut entry in broker.queues.iter_mut() {
            let (name, queue) = entry.pair_mut();
            if let Some(msg) = queue.inflight.remove(&msg_id) {
                result = Some((name.clone(), msg));
                break;
            }
        }
        result
    };

    let (qname, mut msg) = match found {
        Some(pair) => pair,
        None => {
            warn!(conn_id, msg_id, "nack for unknown message");
            return;
        }
    };

    if requeue {
        msg.delivery_count += 1;
        msg.redelivered = true;

        // Check max_retries: if exceeded, route to DLX instead of requeuing
        let max_retries = broker
            .queues
            .get(&qname)
            .and_then(|q| q.options.max_retries);
        if let Some(max) = max_retries {
            if msg.delivery_count > max {
                info!(
                    conn_id,
                    msg_id,
                    delivery_count = msg.delivery_count,
                    "max retries exceeded, dead-lettering"
                );
                dead_letter_sync(broker, &qname, msg).await;
                return;
            }
        }

        let new_id = broker.alloc_msg_id();

        let redelivery = {
            let mut queue_ref = broker.queues.get_mut(&qname).unwrap();
            let queue = queue_ref.value_mut();
            queue.messages.push_front(msg);
            info!(conn_id, msg_id, "requeued");

            match queue.next_target(broker) {
                None => None,
                Some((target_id, target_channel_id)) => {
                    if let Some(mut requeued) = queue.messages.pop_front() {
                        requeued.id = new_id;
                        queue.inflight.insert(new_id, requeued);
                        let msg_ref = queue.inflight.get(&new_id).unwrap();
                        let frame = Frame::with_deliver(
                            target_channel_id,
                            new_id,
                            &msg_ref.headers,
                            &msg_ref.body,
                        );

                        if let Some(mut cs) = broker.conn_state.get_mut(&target_id) {
                            if let Some(ch) = cs.channels.get_mut(&target_channel_id) {
                                ch.unacked_count += 1;
                            }
                        }

                        broker
                            .connections
                            .get(&target_id)
                            .map(|h| (new_id, frame, h.clone(), target_channel_id))
                    } else {
                        None
                    }
                }
            }
        };

        if let Some((new_id, frame, handle, target_channel_id)) = redelivery {
            let _ = handle.tx.send(frame).await;
            info!(
                conn_id,
                msg_id,
                new_id,
                target = handle.id,
                target_channel_id,
                "redelivered"
            );
        }
    } else {
        // Dead-letter
        let dlx_info = broker.queues.get(&qname).and_then(|q| {
            q.options.dead_letter_exchange.as_ref().map(|dlx| {
                (
                    dlx.clone(),
                    q.options
                        .dead_letter_routing_key
                        .clone()
                        .unwrap_or_default(),
                )
            })
        });

        if let Some((dlx_name, dlx_rk)) = dlx_info {
            let target_queues = {
                let exchanges = broker.exchanges.read().await;
                match exchanges.get(&dlx_name) {
                    Some(ex) => {
                        let empty = HashMap::new();
                        ex.route(&dlx_rk, &empty)
                    }
                    None => {
                        warn!(conn_id, exchange = dlx_name.as_str(), "DLX does not exist");
                        return;
                    }
                }
            };

            for target_queue in &target_queues {
                if let Some(mut queue) = broker.queues.get_mut(target_queue.as_str()) {
                    let dead_msg = Message::new(msg.id, msg.headers.clone(), msg.body.clone());
                    queue.messages.push_back(dead_msg);
                }
            }
            info!(conn_id, msg_id, dlx = dlx_name.as_str(), "dead-lettered");
        } else {
            info!(conn_id, msg_id, "nacked and discarded (no DLX)");
        }
    }
}

async fn send_confirm(conn_id: u64, broker: &Broker, success: bool) {
    let is_confirm = broker
        .conn_state
        .get(&conn_id)
        .map_or(false, |cs| cs.confirm_mode);

    if !is_confirm {
        return;
    }

    let conn = match broker.connections.get(&conn_id) {
        Some(c) => c.clone(),
        None => return,
    };

    let tag = broker.alloc_delivery_tag(conn_id);
    let event = if success {
        Event::PublishAck
    } else {
        Event::PublishNack
    };

    let tag_str = format!("delivery_tag:{}\r\n", tag);
    let _ = conn
        .tx
        .send(Frame::with_body(event, tag_str.into_bytes()))
        .await;
}

async fn dead_letter_sync(broker: &Broker, source_queue: &str, msg: Message) {
    let (dlx_name, dlx_rk) = {
        let queue = match broker.queues.get(source_queue) {
            Some(q) => q,
            None => return,
        };
        match &queue.options.dead_letter_exchange {
            Some(dlx) => (
                dlx.clone(),
                queue
                    .options
                    .dead_letter_routing_key
                    .clone()
                    .unwrap_or_default(),
            ),
            None => return,
        }
    }; // queue lock released

    let target_queues = {
        let exchanges = broker.exchanges.read().await;
        let exchange = match exchanges.get(&dlx_name) {
            Some(ex) => ex,
            None => return,
        };
        let empty = HashMap::new();
        exchange.route(&dlx_rk, &empty)
    }; // exchange lock released

    for target_queue in target_queues {
        if let Some(mut queue) = broker.queues.get_mut(&target_queue) {
            let new_msg = Message::new(msg.id, msg.headers.clone(), msg.body.clone());
            queue.messages.push_back(new_msg);
        }
    }
}
