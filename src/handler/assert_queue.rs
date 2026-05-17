use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::state::Broker;
use crate::queue::{QueueOptions, QueueState};
use crate::core::protocol::{Event, Frame};

pub async fn handle(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
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
            warn!(conn_id, queue = qname.as_str(), "queue is exclusive to another connection");
            return;
        }
    }

    broker
        .queues
        .entry(qname.clone())
        .or_insert_with(|| {
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
