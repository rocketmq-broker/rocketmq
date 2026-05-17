use std::collections::HashMap;
use tracing::{info, warn};

use crate::broker::{Broker, Message};
use crate::protocol::Frame;

pub async fn handle(conn_id: u64, broker: &Broker, headers: &[u8]) {
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
        msg.redelivered = true;
        let new_id = broker.alloc_msg_id();

        let redelivery = {
            let mut queue_ref = broker.queues.get_mut(&qname).unwrap();
            let queue = queue_ref.value_mut();
            queue.messages.push_front(msg);
            info!(conn_id, msg_id, "requeued");

            match queue.next_target() {
                None => None,
                Some(target_id) => {
                    if let Some(mut requeued) = queue.messages.pop_front() {
                        requeued.id = new_id;
                        queue.inflight.insert(new_id, requeued);
                        let msg_ref = queue.inflight.get(&new_id).unwrap();
                        let frame = Frame::with_deliver(new_id, &msg_ref.headers, &msg_ref.body);
                        broker
                            .connections
                            .get(&target_id)
                            .map(|h| (new_id, frame, h.clone()))
                    } else {
                        None
                    }
                }
            }
        };

        if let Some((new_id, frame, handle)) = redelivery {
            let _ = handle.tx.send(frame).await;
            info!(conn_id, msg_id, new_id, target = handle.id, "redelivered");
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
