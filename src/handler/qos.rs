use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::broker::Broker;
use crate::protocol::{Event, Frame};

pub async fn handle(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
    let body_str = match std::str::from_utf8(body) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid qos body");
            return;
        }
    };

    let mut prefetch_count: u16 = 0;
    for line in body_str.split("\r\n") {
        if let Some((k, v)) = line.split_once(':') {
            if k == "prefetch_count" {
                prefetch_count = v.parse().unwrap_or(0);
            }
        }
    }

    if let Some(mut conn_state) = broker.conn_state.get_mut(&conn_id) {
        for ch in conn_state.channels.values_mut() {
            ch.prefetch_count = prefetch_count;
        }
    }

    info!(conn_id, prefetch_count, "qos set");
    let _ = tx.send(Frame::empty(Event::QosOk)).await;
}
