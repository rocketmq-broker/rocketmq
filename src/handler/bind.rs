use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::broker::Broker;
use crate::routing::exchange::Binding;
use crate::core::protocol::{Event, Frame};

pub async fn handle(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, headers: &[u8]) {
    let headers_str = match std::str::from_utf8(headers) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid bind headers");
            return;
        }
    };

    let mut exchange = "";
    let mut queue = "";
    let mut routing_key = "";

    for line in headers_str.split("\r\n") {
        if let Some((k, v)) = line.split_once(':') {
            match k {
                "exchange" => exchange = v,
                "queue" => queue = v,
                "routing_key" => routing_key = v,
                _ => {}
            }
        }
    }

    {
        let mut exchanges = broker.exchanges.write().await;
        match exchanges.get_mut(exchange) {
            Some(ex) => {
                ex.add_binding(Binding {
                    queue_name: queue.to_string(),
                    routing_key: routing_key.to_string(),
                    headers_match: None,
                });
            }
            None => {
                warn!(conn_id, exchange, "exchange does not exist for bind");
                return;
            }
        }
    }

    info!(conn_id, exchange, queue, routing_key, "bound");
    let _ = tx
        .send(Frame::with_body(Event::BindOk, b"bind.ok".to_vec()))
        .await;
}
