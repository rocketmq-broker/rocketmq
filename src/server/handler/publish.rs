use std::collections::HashMap;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

use crate::state::Broker;
use crate::queue::Message;
use crate::core::protocol::{Event, Frame};

pub async fn handle(conn_id: u64, broker: &Broker, headers: &[u8], body: &[u8]) {
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
                _ => {
                    user_headers.extend_from_slice(line.as_bytes());
                    user_headers.extend_from_slice(b"\r\n");
                }
            }
        }
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
        debug!(conn_id, exchange = exchange_name, routing_key, "no matching bindings");
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
                    warn!(conn_id, queue = queue_name.as_str(), "bound queue does not exist");
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
            };

            match queue.next_target() {
                None => {
                    queue.messages.push_back(msg);
                    debug!(conn_id, msg_id, queue = queue_name.as_str(), "queued");
                    None
                }
                Some(target_id) => {
                    queue.inflight.insert(msg_id, msg);
                    let msg_ref = queue.inflight.get(&msg_id).unwrap();
                    let frame = Frame::with_deliver(msg_id, &msg_ref.headers, &msg_ref.body);

                    // Get connection handle (separate DashMap, no conflict)
                    broker
                        .connections
                        .get(&target_id)
                        .map(|h| (msg_id, frame, h.clone()))
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

        if let Some((msg_id, frame, handle)) = delivery {
            let _ = handle.tx.send(frame).await;
            info!(conn_id, msg_id, target = handle.id, "delivered");
        }
    }

    send_confirm(conn_id, broker, true).await;
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
