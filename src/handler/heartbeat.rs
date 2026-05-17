use tokio::sync::mpsc;
use tracing::info;

use crate::protocol::Frame;

pub async fn handle(_tx: &mpsc::Sender<Frame>) {
    info!("Heartbeat received");
}
