use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::broker::Broker;
use crate::core::protocol::{Event, Frame};

pub async fn handle(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, headers: &[u8]) {
    let headers_str = match std::str::from_utf8(headers) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid unbind headers");
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
        if let Some(ex) = exchanges.get_mut(exchange) {
            ex.remove_binding(queue, routing_key);
        }
    }

    info!(conn_id, exchange, queue, routing_key, "unbound");
    let _ = tx
        .send(Frame::with_body(Event::UnbindOk, b"unbind.ok".to_vec()))
        .await;
}
