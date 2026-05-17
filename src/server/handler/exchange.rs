use tokio::sync::mpsc;
use tracing::{info, warn};

use crate::core::protocol::{Event, Frame};
use crate::routing::exchange::{Binding, Exchange, ExchangeType};
use crate::state::Broker;

pub async fn declare_exchange(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, headers: &[u8]) {
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

pub async fn delete_exchange(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, headers: &[u8]) {
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

pub async fn bind(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, headers: &[u8]) {
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

pub async fn unbind(conn_id: u64, tx: &mpsc::Sender<Frame>, broker: &Broker, headers: &[u8]) {
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
