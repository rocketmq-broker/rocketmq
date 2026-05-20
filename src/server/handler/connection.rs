use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::core::protocol::{Event, Frame};
use crate::state::Broker;

pub async fn nop(conn_id: u64) {
    debug!(conn_id, "nop");
}

pub async fn heartbeat(_tx: &mpsc::Sender<Frame>) {
    info!("Heartbeat received");
}

/// Open a virtual host for this connection.
pub async fn vhost_open(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, body: &[u8]) {
    let vhost_name = match std::str::from_utf8(body) {
        Ok(s) => s.trim(),
        Err(_) => {
            warn!(conn_id, "invalid vhost name encoding");
            return;
        }
    };

    // Check if vhost exists
    if !broker.vhosts.contains_key(vhost_name) {
        warn!(conn_id, vhost = vhost_name, "vhost does not exist");
        // Could send an error frame here; for now just return
        return;
    }

    // Set vhost on connection state
    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        cs.vhost = vhost_name.to_string();
        info!(conn_id, vhost = vhost_name, "vhost opened");
    }

    let _ = tx
        .send(Frame::with_body(Event::VHostOpenOk, vhost_name.as_bytes().to_vec()))
        .await;
}
