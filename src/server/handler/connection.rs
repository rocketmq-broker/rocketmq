use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::core::protocol::Frame;

pub async fn nop(conn_id: u64) {
    debug!(conn_id, "nop");
}

pub async fn heartbeat(_tx: &mpsc::Sender<Frame>) {
    info!("Heartbeat received");
}
