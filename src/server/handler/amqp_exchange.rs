// Copyright (c) 2026 Edilson Pateguana
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// Author: Edilson Pateguana
// Year: 2026
// File: amqp_exchange.rs
// Description: AMQP Exchange class method handlers (declare, delete, bind).

//! AMQP 0-9-1 Exchange class handlers (class 40).

use std::io::Cursor;
use tokio::io::AsyncWriteExt;
use tracing::info;

use crate::core::amqp_codec::*;
use crate::core::method::*;
use crate::core::types::*;
use crate::routing::exchange::{Binding, Exchange, ExchangeType};
use crate::state::Broker;

use super::auth_check::send_channel_error;

pub async fn handle_declare(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::server::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let name = read_shortstr(&mut r).unwrap_or_default();
    let kind_str = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let passive = flags & 0x01 != 0;
    let durable = flags & 0x02 != 0;
    let _auto_delete = flags & 0x04 != 0;
    let _internal = flags & 0x08 != 0;
    let no_wait = flags & 0x10 != 0;
    let _arguments = read_field_table(&mut r).unwrap_or_default();

    if passive {
        // Passive declare: only assert the exchange exists, never create.
        let exists = broker.exchanges.read().await.contains_key(&name);
        if !exists {
            send_channel_error(
                writer,
                channel,
                NOT_FOUND,
                "NOT_FOUND - no such exchange",
                CLASS_EXCHANGE,
                METHOD_EXCHANGE_DECLARE,
            )
            .await;
            return;
        }
    } else {
        // Active declare: validate name/type, then create if needed.

        // Guard "amq." reserved namespace
        if name.starts_with("amq.") {
            send_channel_error(
                writer,
                channel,
                ACCESS_REFUSED,
                "ACCESS_REFUSED - exchange names starting with 'amq.' are reserved",
                CLASS_EXCHANGE,
                METHOD_EXCHANGE_DECLARE,
            )
            .await;
            return;
        }

        // Permission check: configure permission needed for declare
        if super::auth_check::check_configure(
            conn_id,
            channel,
            &name,
            CLASS_EXCHANGE,
            METHOD_EXCHANGE_DECLARE,
            writer,
            broker,
        )
        .await
        {
            return;
        }

        // Validate exchange type
        let kind = match ExchangeType::from_str(&kind_str) {
            Some(k) => k,
            None => {
                send_channel_error(
                    writer,
                    channel,
                    COMMAND_INVALID,
                    "COMMAND_INVALID - unsupported exchange type",
                    CLASS_EXCHANGE,
                    METHOD_EXCHANGE_DECLARE,
                )
                .await;
                return;
            }
        };

        let kind_byte = kind.to_byte();
        let mut exchanges = broker.exchanges.write().await;
        let is_new = !exchanges.contains_key(&name);
        exchanges
            .entry(name.clone())
            .or_insert_with(|| Exchange::new(name.clone(), kind, durable));

        if is_new && let Some(c) = broker.cluster() {
            let c = c.clone();
            let name_clone = name.clone();
            let kind_clone = kind_str.clone();
            tokio::spawn(async move {
                c.broadcast(crate::cluster::ClusterFrame::DeclareExchange {
                    name: name_clone,
                    kind: kind_clone,
                    durable,
                })
                .await;
            });
        }

        // WAL: persist durable exchange declarations
        if durable
            && is_new
            && let Some(wal) = broker.wal()
        {
            let _ = wal.log_declare_exchange(&name, kind_byte, true);
        }
    }

    info!(
        conn_id,
        channel,
        exchange = name.as_str(),
        "exchange declared"
    );
    if !no_wait {
        let reply = encode_method_frame(channel, CLASS_EXCHANGE, METHOD_EXCHANGE_DECLARE_OK, &[]);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

pub async fn handle_delete(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::server::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let name = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let _if_unused = flags & 0x01 != 0;
    let no_wait = flags & 0x02 != 0;

    if name.starts_with("amq.") {
        send_channel_error(
            writer,
            channel,
            ACCESS_REFUSED,
            "ACCESS_REFUSED - cannot delete default exchange",
            CLASS_EXCHANGE,
            METHOD_EXCHANGE_DELETE,
        )
        .await;
        return;
    }

    broker.exchanges.write().await.remove(&name);
    info!(
        conn_id,
        channel,
        exchange = name.as_str(),
        "exchange deleted"
    );
    if !no_wait {
        let reply = encode_method_frame(channel, CLASS_EXCHANGE, METHOD_EXCHANGE_DELETE_OK, &[]);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

pub async fn handle_bind(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::server::AmqpWriter,
    broker: &Broker,
) {
    // Not to be confused with Queue.Bind — this is exchange-to-exchange binding
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let destination = read_shortstr(&mut r).unwrap_or_default();
    let source = read_shortstr(&mut r).unwrap_or_default();
    let routing_key = read_shortstr(&mut r).unwrap_or_default();
    let flags = read_octet(&mut r).unwrap_or(0);
    let no_wait = flags & 0x01 != 0;

    // For now, treat as queue bind on the source exchange
    {
        let mut exchanges = broker.exchanges.write().await;
        if let Some(ex) = exchanges.get_mut(&source) {
            ex.add_binding(Binding {
                queue_name: destination.clone(),
                routing_key: routing_key.clone(),
                headers_match: None,
            });
        }
    }

    info!(
        conn_id,
        source = source.as_str(),
        dest = destination.as_str(),
        "exchange bound"
    );
    if !no_wait {
        let reply = encode_method_frame(channel, CLASS_EXCHANGE, METHOD_EXCHANGE_BIND_OK, &[]);
        let _ = writer.write_all(&reply).await;
        let _ = writer.flush().await;
    }
}

pub async fn handle_unbind(
    conn_id: u64,
    channel: u16,
    args: &[u8],
    writer: &mut crate::server::AmqpWriter,
    broker: &Broker,
) {
    let mut r = Cursor::new(args);
    let _ticket = read_short(&mut r).unwrap_or(0);
    let destination = read_shortstr(&mut r).unwrap_or_default();
    let source = read_shortstr(&mut r).unwrap_or_default();
    let routing_key = read_shortstr(&mut r).unwrap_or_default();

    {
        let mut exchanges = broker.exchanges.write().await;
        if let Some(ex) = exchanges.get_mut(&source) {
            ex.remove_binding(&destination, &routing_key);
        }
    }

    info!(
        conn_id,
        source = source.as_str(),
        dest = destination.as_str(),
        "exchange unbound"
    );
    let reply = encode_method_frame(channel, CLASS_EXCHANGE, METHOD_EXCHANGE_UNBIND_OK, &[]);
    let _ = writer.write_all(&reply).await;
    let _ = writer.flush().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Executes the standard exchange declare args encode lifecycle step.
    ///
    /// Executes the required business logic for exchange declare args encode.
    #[test]
    fn exchange_declare_args_encode() {
        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap(); // ticket
        write_shortstr(&mut args, "test.ex").unwrap();
        write_shortstr(&mut args, "direct").unwrap();
        write_octet(&mut args, 0x02).unwrap(); // durable
        write_field_table(&mut args, &FieldTable::new()).unwrap();

        let mut r = Cursor::new(&args);
        assert_eq!(read_short(&mut r).unwrap(), 0);
        assert_eq!(read_shortstr(&mut r).unwrap(), "test.ex");
        assert_eq!(read_shortstr(&mut r).unwrap(), "direct");
        assert_eq!(read_octet(&mut r).unwrap(), 0x02);
    }

    /// Executes the standard exchange delete args encode lifecycle step.
    ///
    /// Executes the required business logic for exchange delete args encode.
    #[test]
    fn exchange_delete_args_encode() {
        let mut args = Vec::new();
        write_short(&mut args, 0).unwrap();
        write_shortstr(&mut args, "my.exchange").unwrap();
        write_octet(&mut args, 0).unwrap();

        let mut r = Cursor::new(&args);
        let _ = read_short(&mut r).unwrap();
        assert_eq!(read_shortstr(&mut r).unwrap(), "my.exchange");
    }

    /// Executes the standard channel close error builds lifecycle step.
    ///
    /// Executes the required business logic for channel close error builds.
    #[test]
    fn channel_close_error_builds() {
        use super::super::auth_check::build_channel_close;
        let args = build_channel_close(
            NOT_FOUND,
            "NOT_FOUND",
            CLASS_EXCHANGE,
            METHOD_EXCHANGE_DECLARE,
        );
        let mut r = Cursor::new(&args);
        assert_eq!(read_short(&mut r).unwrap(), 404);
        assert_eq!(read_shortstr(&mut r).unwrap(), "NOT_FOUND");
        assert_eq!(read_short(&mut r).unwrap(), CLASS_EXCHANGE);
        assert_eq!(read_short(&mut r).unwrap(), METHOD_EXCHANGE_DECLARE);
    }
}