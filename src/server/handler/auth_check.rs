//! Shared permission check helpers for AMQP handlers.
//!
//! Provides a `deny_access` helper that sends Channel.Close(403)
//! and per-operation permission check functions.

use tokio::io::AsyncWriteExt;
use tracing::warn;

use crate::core::amqp_codec::encode_method_frame;
use crate::core::method::*;
use crate::state::Broker;

/// Send a Channel.Close with ACCESS_REFUSED and flush.
/// Returns true (meaning "the caller should return early").
pub async fn deny_access(
    conn_id: u64,
    channel: u16,
    resource: &str,
    operation: &str,
    class_id: u16,
    method_id: u16,
    writer: &mut crate::server::AmqpWriter,
) -> bool {
    warn!(
        conn_id,
        channel, resource, operation, "ACCESS_REFUSED - permission denied"
    );
    let close = build_channel_close(
        ACCESS_REFUSED,
        "ACCESS_REFUSED - permission denied",
        class_id,
        method_id,
    );
    let frame = encode_method_frame(channel, CLASS_CHANNEL, METHOD_CHANNEL_CLOSE, &close);
    let _ = writer.write_all(&frame).await;
    let _ = writer.flush().await;
    true
}

fn build_channel_close(code: u16, text: &str, class_id: u16, method_id: u16) -> Vec<u8> {
    use crate::core::types::{write_short, write_shortstr};
    let mut buf = Vec::new();
    write_short(&mut buf, code).unwrap();
    write_shortstr(&mut buf, text).unwrap();
    write_short(&mut buf, class_id).unwrap();
    write_short(&mut buf, method_id).unwrap();
    buf
}

/// Get the username and vhost for a connection from broker state.
pub fn get_conn_auth(broker: &Broker, conn_id: u64) -> (String, String) {
    broker
        .conn_state
        .get(&conn_id)
        .map(|cs| (cs.username.clone(), cs.vhost.clone()))
        .unwrap_or_default()
}

/// Check if a user has 'configure' permission on a resource.
/// If denied, sends Channel.Close(403) and returns true.
pub async fn check_configure(
    conn_id: u64,
    channel: u16,
    resource: &str,
    class_id: u16,
    method_id: u16,
    writer: &mut crate::server::AmqpWriter,
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
        return true; // denied
    }
    false // allowed
}

/// Check if a user has 'write' permission on a resource.
/// If denied, sends Channel.Close(403) and returns true.
pub async fn check_write(
    conn_id: u64,
    channel: u16,
    resource: &str,
    class_id: u16,
    method_id: u16,
    writer: &mut crate::server::AmqpWriter,
    broker: &Broker,
) -> bool {
    let (user, vhost) = get_conn_auth(broker, conn_id);
    if !broker.auth.check_write(&user, &vhost, resource) {
        deny_access(
            conn_id, channel, resource, "write", class_id, method_id, writer,
        )
        .await;
        return true; // denied
    }
    false // allowed
}

/// Check if a user has 'read' permission on a resource.
/// If denied, sends Channel.Close(403) and returns true.
pub async fn check_read(
    conn_id: u64,
    channel: u16,
    resource: &str,
    class_id: u16,
    method_id: u16,
    writer: &mut crate::server::AmqpWriter,
    broker: &Broker,
) -> bool {
    let (user, vhost) = get_conn_auth(broker, conn_id);
    if !broker.auth.check_read(&user, &vhost, resource) {
        deny_access(
            conn_id, channel, resource, "read", class_id, method_id, writer,
        )
        .await;
        return true; // denied
    }
    false // allowed
}
