use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::broker::Broker;
use crate::core::protocol::{Event, Frame};

pub async fn handle(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, headers: &[u8]) {
    let headers_str = match std::str::from_utf8(headers) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid delete_exchange headers");
            return;
        }
    };

    let mut name = "";
    for line in headers_str.split("\r\n") {
        if let Some((k, v)) = line.split_once(':') {
            if k == "name" {
                name = v;
            }
        }
    }

    if name.is_empty() || name.starts_with("amq.") {
        warn!(conn_id, exchange = name, "cannot delete protected exchange");
        return;
    }

    {
        let mut exchanges = broker.exchanges.write().await;
        exchanges.remove(name);
    }

    info!(conn_id, exchange = name, "exchange deleted");
    let _ = tx
        .send(Frame::with_body(
            Event::DeleteExchangeOk,
            b"delete.exchange.ok".to_vec(),
        ))
        .await;
}
