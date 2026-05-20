use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::core::protocol::{Event, Frame};
use crate::queue::{QueueOptions, QueueState};
use crate::state::Broker;

pub async fn assert_queue(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
    let raw = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid queue name encoding");
            return;
        }
    };

    let (qname, options) = if raw.contains("\r\n") {
        let (name, opts) = QueueOptions::from_headers(raw);
        if name.is_empty() {
            warn!(conn_id, "missing queue name in assert_queue headers");
            return;
        }
        (name, opts)
    } else {
        (raw.to_string(), QueueOptions::default())
    };

    // Check exclusive queue ownership
    if let Some(existing) = broker.queues.get(&qname) {
        if existing.options.exclusive && existing.owner_conn_id != Some(conn_id) {
            warn!(
                conn_id,
                queue = qname.as_str(),
                "queue is exclusive to another connection"
            );
            return;
        }
    }

    broker.queues.entry(qname.clone()).or_insert_with(|| {
        let mut q = QueueState::with_options(options);
        if q.options.exclusive {
            q.owner_conn_id = Some(conn_id);
        }
        q
    });

    broker.auto_bind_default_exchange(&qname);

    info!(conn_id, queue = qname.as_str(), "queue asserted");
    let _ = tx
        .send(Frame::with_body(
            Event::AssertQueueOk,
            b"assert.queue.ok".to_vec(),
        ))
        .await;
}

pub async fn listen(conn_id: u64, channel_id: u16, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
    let body_str = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid queue name encoding");
            return;
        }
    };

    // Parse queue name and optional consumer_tag from headers
    let mut qname = body_str;
    let mut consumer_tag: Option<String> = None;

    if body_str.contains("\r\n") {
        for line in body_str.split("\r\n") {
            if line.is_empty() {
                continue;
            }
            if let Some((k, v)) = line.split_once(':') {
                match k {
                    "queue" => qname = v,
                    "consumer_tag" => consumer_tag = Some(v.to_string()),
                    _ => {}
                }
            } else if qname == body_str {
                // bare queue name as first line
                qname = line;
            }
        }
    }

    let assigned_tag = match broker.queues.get_mut(qname) {
        Some(mut queue) => queue.add_consumer(conn_id, channel_id, consumer_tag),
        None => {
            warn!(conn_id, queue = qname, "queue does not exist");
            return;
        }
    };

    info!(conn_id, channel_id, queue = qname, consumer_tag = assigned_tag.as_str(), "listening");
    let reply = format!("consumer_tag:{}\r\n", assigned_tag);
    let _ = tx
        .send(Frame::with_body(Event::ListenOk, reply.into_bytes()))
        .await;
}

pub async fn basic_cancel(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
    let body_str = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid cancel body");
            return;
        }
    };

    // Parse consumer_tag
    let mut consumer_tag = body_str.trim();
    for line in body_str.split("\r\n") {
        if let Some((k, v)) = line.split_once(':') {
            if k == "consumer_tag" {
                consumer_tag = v;
            }
        }
    }

    let mut cancelled = false;
    for mut entry in broker.queues.iter_mut() {
        if entry.value_mut().cancel_consumer(consumer_tag) {
            cancelled = true;
            break;
        }
    }

    if cancelled {
        info!(conn_id, consumer_tag, "consumer cancelled");
    } else {
        warn!(conn_id, consumer_tag, "cancel: consumer tag not found");
    }

    let reply = format!("consumer_tag:{}\r\n", consumer_tag);
    let _ = tx
        .send(Frame::with_body(Event::BasicCancelOk, reply.into_bytes()))
        .await;
}
