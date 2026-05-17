use tracing::{info, warn};

use crate::state::Broker;

pub async fn handle(conn_id: u64, broker: &Broker, headers: &[u8]) {
    let msg_id = match super::parse_msg_id(headers) {
        Some(id) => id,
        None => {
            warn!(conn_id, "invalid ack headers");
            return;
        }
    };

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
