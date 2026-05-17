use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::broker::Broker;
use crate::exchange::{Exchange, ExchangeType};
use crate::core::protocol::{Event, Frame};

pub async fn handle(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, headers: &[u8]) {
    let headers_str = match std::str::from_utf8(headers) {
        Ok(s) => s,
        Err(_) => {
            warn!(conn_id, "invalid declare_exchange headers");
            return;
        }
    };

    let mut name = "";
    let mut kind_str = "direct";
    let mut durable = false;

    for line in headers_str.split("\r\n") {
        if let Some((k, v)) = line.split_once(':') {
            match k {
                "name" => name = v,
                "type" => kind_str = v,
                "durable" => durable = v == "true",
                _ => {}
            }
        }
    }

    let kind = match ExchangeType::from_str(kind_str) {
        Some(k) => k,
        None => {
            warn!(conn_id, kind = kind_str, "unknown exchange type");
            return;
        }
    };

    {
        let mut exchanges = broker.exchanges.write().await;
        exchanges
            .entry(name.to_string())
            .or_insert_with(|| Exchange::new(name.to_string(), kind, durable));
    }

    info!(conn_id, exchange = name, "exchange declared");
    let _ = tx
        .send(Frame::with_body(
            Event::DeclareExchangeOk,
            b"declare.exchange.ok".to_vec(),
        ))
        .await;
}
