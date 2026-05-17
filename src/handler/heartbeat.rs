use tokio::sync::mpsc;
use tracing::info;

use crate::core::protocol::Frame;

pub async fn handle(_tx: &mpsc::Sender<Frame>) {
    info!("Heartbeat received");
}
