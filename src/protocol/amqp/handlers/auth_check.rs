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
// File: auth_check.rs
// Description: AMQP authentication and connection handshake validation helpers.

//! Shared permission check helpers for AMQP handlers.
//!
//! Provides a `deny_access` helper that sends Channel.Close(403)
//! and per-operation permission check functions.

use tokio::io::AsyncWriteExt;
use tracing::warn;

use crate::protocol::amqp::codec::encode_method_frame;
use crate::protocol::amqp::method::*;
use crate::state::Broker;

pub async fn deny_access(
    conn_id: u64,
    channel: u16,
    resource: &str,
    operation: &str,
    class_id: u16,
    method_id: u16,
    writer: &mut crate::protocol::amqp::AmqpWriter,
) -> bool {
    warn!(
        conn_id,
        channel, resource, operation, "ACCESS_REFUSED - permission denied"
    );
    send_channel_error(
        writer,
        channel,
        ACCESS_REFUSED,
        "ACCESS_REFUSED - permission denied",
        class_id,
        method_id,
    )
    .await;
    true
}

pub fn build_channel_close(code: u16, text: &str, class_id: u16, method_id: u16) -> Vec<u8> {
    use crate::protocol::amqp::types::{write_short, write_shortstr};
    let mut buf = Vec::with_capacity(4 + 1 + text.len() + 4);
    write_short(&mut buf, code).unwrap();
    write_shortstr(&mut buf, text).unwrap();
    write_short(&mut buf, class_id).unwrap();
    write_short(&mut buf, method_id).unwrap();
    buf
}

pub async fn send_channel_error(
    writer: &mut crate::protocol::amqp::AmqpWriter,
    channel: u16,
    code: u16,
    text: &str,
    class_id: u16,
    method_id: u16,
) {
    let close = build_channel_close(code, text, class_id, method_id);
    let frame = encode_method_frame(channel, CLASS_CHANNEL, METHOD_CHANNEL_CLOSE, &close);
    let _ = writer.write_all(&frame).await;
    let _ = writer.flush().await;
}

use crate::protocol::amqp::session::ConnectionState;

pub fn get_conn_auth(broker: &Broker, conn_id: u64) -> (String, String) {
    broker
        .conn_state
        .get(&conn_id)
        .and_then(|guard| {
            guard
                .value()
                .as_any()
                .downcast_ref::<ConnectionState>()
                .map(|cs| (cs.username.clone(), cs.vhost.clone()))
        })
        .unwrap_or_default()
}

pub async fn check_configure(
    conn_id: u64,
    channel: u16,
    resource: &str,
    class_id: u16,
    method_id: u16,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) -> bool {
    let (user, vhost) = get_conn_auth(broker, conn_id);
    if !broker.auth.check_configure(&user, &vhost, resource) {
        deny_access(
            conn_id,
            channel,
            resource,
            "configure",
            class_id,
            method_id,
            writer,
        )
        .await;
        return true;
    }
    false
}

pub async fn check_write(
    conn_id: u64,
    channel: u16,
    resource: &str,
    class_id: u16,
    method_id: u16,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) -> bool {
    let (user, vhost) = get_conn_auth(broker, conn_id);
    if !broker.auth.check_write(&user, &vhost, resource) {
        deny_access(
            conn_id, channel, resource, "write", class_id, method_id, writer,
        )
        .await;
        return true;
    }
    false
}

pub async fn check_read(
    conn_id: u64,
    channel: u16,
    resource: &str,
    class_id: u16,
    method_id: u16,
    writer: &mut crate::protocol::amqp::AmqpWriter,
    broker: &Broker,
) -> bool {
    let (user, vhost) = get_conn_auth(broker, conn_id);
    if !broker.auth.check_read(&user, &vhost, resource) {
        deny_access(
            conn_id, channel, resource, "read", class_id, method_id, writer,
        )
        .await;
        return true;
    }
    false
}
