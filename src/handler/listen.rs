use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::broker::Broker;
use crate::core::protocol::{Event, Frame};

pub async fn handle(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
    let qname = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid queue name encoding");
            return;
        }
    };

    match broker.queues.get_mut(qname) {
        Some(mut queue) => {
            if !queue.listeners.contains(&conn_id) {
                queue.listeners.push(conn_id);
            }
        }
        None => {
            warn!(conn_id, queue = qname, "queue does not exist");
            return;
        }
    }

    info!(conn_id, queue = qname, "listening");
    let _ = tx
        .send(Frame::with_body(Event::ListenOk, b"listen.ok".to_vec()))
        .await;
}
