//! AMQP 0-9-1 connection handshake.
//!
//! Implements the full connection lifecycle:
//! 1. Protocol header → Connection.Start
//! 2. Connection.StartOk (SASL PLAIN) → Connection.Tune
//! 3. Connection.TuneOk → (wait for) Connection.Open
//! 4. Connection.OpenOk → ready for channel operations
//!
//! Also handles Connection.Close/CloseOk.

use std::io::Cursor;
use std::net::SocketAddr;

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::tcp::OwnedWriteHalf;
use tracing::{info, warn};

use crate::core::amqp_codec::*;
use crate::core::method::*;
use crate::core::types::*;
use crate::state::Broker;

/// Perform the AMQP 0-9-1 handshake on a raw TCP stream.
/// Returns Ok(()) if handshake succeeds, Err if the connection should be dropped.
pub async fn perform_handshake(
    conn_id: u64,
    peer_addr: SocketAddr,
    reader: &mut (impl AsyncReadExt + Unpin),
    writer: &mut BufWriter<OwnedWriteHalf>,
    broker: &Broker,
) -> Result<(), ()> {
    // ── Step 1: Read protocol header ──────────────────
    let mut proto_header = [0u8; 8];
    if reader.read_exact(&mut proto_header).await.is_err() {
        return Err(());
    }

    if proto_header != PROTOCOL_HEADER {
        // Send our supported protocol header and close
        let _ = writer.write_all(&PROTOCOL_HEADER).await;
        let _ = writer.flush().await;
        warn!(conn_id, "protocol header mismatch, sent ours and closing");
        return Err(());
    }

    // ── Step 2: Send Connection.Start ─────────────────
    let start_args = build_connection_start();
    let start_frame =
        encode_method_frame(0, CLASS_CONNECTION, METHOD_CONNECTION_START, &start_args);
    if writer.write_all(&start_frame).await.is_err() {
        return Err(());
    }
    let _ = writer.flush().await;

    // ── Step 3: Read Connection.StartOk ───────────────
    let frame = read_amqp_frame(reader).await?;
    let method = decode_method(&frame.payload).map_err(|_| ())?;
    if method.class_id != CLASS_CONNECTION || method.method_id != METHOD_CONNECTION_START_OK {
        warn!(
            conn_id,
            class = method.class_id,
            method = method.method_id,
            "expected Connection.StartOk"
        );
        return Err(());
    }

    let (username, password) = parse_start_ok_credentials(&method.arguments)?;

    // Authenticate via the auth backend (bcrypt verification + localhost check)
    if let Err(reason) = broker.auth.authenticate(&username, &password, peer_addr) {
        warn!(conn_id, user = username.as_str(), %reason, "authentication failed");
        let close = build_connection_close(
            ACCESS_REFUSED,
            &format!("ACCESS_REFUSED - {}", reason),
            CLASS_CONNECTION,
            METHOD_CONNECTION_START_OK,
        );
        let close_frame = encode_method_frame(0, CLASS_CONNECTION, METHOD_CONNECTION_CLOSE, &close);
        let _ = writer.write_all(&close_frame).await;
        let _ = writer.flush().await;
        return Err(());
    }

    info!(
        conn_id,
        user = username.as_str(),
        "SASL PLAIN authenticated"
    );

    // Store auth info
    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        cs.username = username.clone();
        cs.authenticated = true;
    }

    // ── Step 4: Send Connection.Tune ──────────────────
    let tune_args =
        build_connection_tune(DEFAULT_CHANNEL_MAX, DEFAULT_FRAME_MAX, DEFAULT_HEARTBEAT);
    let tune_frame = encode_method_frame(0, CLASS_CONNECTION, METHOD_CONNECTION_TUNE, &tune_args);
    if writer.write_all(&tune_frame).await.is_err() {
        return Err(());
    }
    let _ = writer.flush().await;

    // ── Step 5: Read Connection.TuneOk ────────────────
    let frame = read_amqp_frame(reader).await?;
    let method = decode_method(&frame.payload).map_err(|_| ())?;
    if method.class_id != CLASS_CONNECTION || method.method_id != METHOD_CONNECTION_TUNE_OK {
        warn!(conn_id, "expected Connection.TuneOk");
        return Err(());
    }

    let (channel_max, frame_max, heartbeat) = parse_tune_ok(&method.arguments)?;
    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        cs.channel_max = channel_max;
        cs.frame_max = frame_max;
        cs.heartbeat = heartbeat;
    }
    info!(
        conn_id,
        channel_max, frame_max, heartbeat, "tune negotiated"
    );

    // ── Step 6: Read Connection.Open ──────────────────
    let frame = read_amqp_frame(reader).await?;
    let method = decode_method(&frame.payload).map_err(|_| ())?;
    if method.class_id != CLASS_CONNECTION || method.method_id != METHOD_CONNECTION_OPEN {
        warn!(conn_id, "expected Connection.Open");
        return Err(());
    }

    let vhost = parse_connection_open(&method.arguments)?;
    // Validate vhost exists
    if !broker.vhosts.contains_key(&vhost) {
        warn!(conn_id, vhost = vhost.as_str(), "vhost not found");
        let close = build_connection_close(
            NOT_ALLOWED,
            "NOT_ALLOWED - vhost not found",
            CLASS_CONNECTION,
            METHOD_CONNECTION_OPEN,
        );
        let close_frame = encode_method_frame(0, CLASS_CONNECTION, METHOD_CONNECTION_CLOSE, &close);
        let _ = writer.write_all(&close_frame).await;
        let _ = writer.flush().await;
        return Err(());
    }

    // Check user has access to this vhost
    let username = broker
        .conn_state
        .get(&conn_id)
        .map(|cs| cs.username.clone())
        .unwrap_or_default();
    if !broker.auth.check_vhost_access(&username, &vhost) {
        warn!(
            conn_id,
            user = username.as_str(),
            vhost = vhost.as_str(),
            "vhost access denied"
        );
        let close = build_connection_close(
            ACCESS_REFUSED,
            "ACCESS_REFUSED - no access to vhost",
            CLASS_CONNECTION,
            METHOD_CONNECTION_OPEN,
        );
        let close_frame = encode_method_frame(0, CLASS_CONNECTION, METHOD_CONNECTION_CLOSE, &close);
        let _ = writer.write_all(&close_frame).await;
        let _ = writer.flush().await;
        return Err(());
    }

    if let Some(mut cs) = broker.conn_state.get_mut(&conn_id) {
        cs.vhost = vhost.clone();
    }

    // ── Step 7: Send Connection.OpenOk ────────────────
    let open_ok_args = build_connection_open_ok();
    let open_ok_frame = encode_method_frame(
        0,
        CLASS_CONNECTION,
        METHOD_CONNECTION_OPEN_OK,
        &open_ok_args,
    );
    if writer.write_all(&open_ok_frame).await.is_err() {
        return Err(());
    }
    let _ = writer.flush().await;

    info!(conn_id, vhost = vhost.as_str(), "AMQP connection open");
    Ok(())
}

/// Build Connection.Close method frame arguments.
pub fn build_connection_close(
    reply_code: u16,
    reply_text: &str,
    class_id: u16,
    method_id: u16,
) -> Vec<u8> {
    let mut buf = Vec::new();
    write_short(&mut buf, reply_code).unwrap();
    write_shortstr(&mut buf, reply_text).unwrap();
    write_short(&mut buf, class_id).unwrap();
    write_short(&mut buf, method_id).unwrap();
    buf
}

/// Build Connection.CloseOk (empty arguments).
pub fn build_connection_close_ok() -> Vec<u8> {
    Vec::new()
}

// ─── Internal builders ────────────────────────────────

fn build_connection_start() -> Vec<u8> {
    let mut buf = Vec::new();
    // version-major, version-minor
    write_octet(&mut buf, 0).unwrap(); // major
    write_octet(&mut buf, 9).unwrap(); // minor

    // server-properties (field-table)
    let mut props = FieldTable::new();
    props.insert(
        "product".into(),
        FieldValue::LongString(b"RocketMQ".to_vec()),
    );
    props.insert("version".into(), FieldValue::LongString(b"0.1.0".to_vec()));
    props.insert(
        "platform".into(),
        FieldValue::LongString(b"Rust/Tokio".to_vec()),
    );
    props.insert(
        "capabilities".into(),
        FieldValue::FieldTable({
            let mut caps = FieldTable::new();
            caps.insert("publisher_confirms".into(), FieldValue::Boolean(true));
            caps.insert("consumer_cancel_notify".into(), FieldValue::Boolean(true));
            caps.insert("basic.nack".into(), FieldValue::Boolean(true));
            caps.insert("connection.blocked".into(), FieldValue::Boolean(false));
            caps
        }),
    );
    write_field_table(&mut buf, &props).unwrap();

    // mechanisms (long-string)
    write_longstr(&mut buf, b"PLAIN AMQPLAIN").unwrap();

    // locales (long-string)
    write_longstr(&mut buf, b"en_US").unwrap();

    buf
}

fn build_connection_tune(channel_max: u16, frame_max: u32, heartbeat: u16) -> Vec<u8> {
    let mut buf = Vec::new();
    write_short(&mut buf, channel_max).unwrap();
    write_long(&mut buf, frame_max).unwrap();
    write_short(&mut buf, heartbeat).unwrap();
    buf
}

fn build_connection_open_ok() -> Vec<u8> {
    let mut buf = Vec::new();
    // known-hosts (shortstr) — deprecated, send empty
    write_shortstr(&mut buf, "").unwrap();
    buf
}

/// Parse Connection.StartOk to extract credentials.
/// Does NOT validate — that's the AuthBackend's job.
fn parse_start_ok_credentials(args: &[u8]) -> Result<(String, String), ()> {
    let mut r = Cursor::new(args);

    // client-properties (field-table) — read and discard
    let _client_props = read_field_table(&mut r).map_err(|_| ())?;

    // mechanism (shortstr)
    let mechanism = read_shortstr(&mut r).map_err(|_| ())?;

    // response (longstr) — SASL PLAIN: \0user\0pass
    let response = read_longstr(&mut r).map_err(|_| ())?;

    // locale (shortstr)
    let _locale = read_shortstr(&mut r).map_err(|_| ())?;

    if mechanism == "PLAIN" {
        // PLAIN format: \0username\0password
        let parts: Vec<&[u8]> = response.split(|b| *b == 0).collect();
        if parts.len() >= 3 {
            let user = String::from_utf8_lossy(parts[1]).to_string();
            let pass = String::from_utf8_lossy(parts[2]).to_string();
            return Ok((user, pass));
        }
        warn!("malformed SASL PLAIN response");
        return Err(());
    } else if mechanism == "AMQPLAIN" {
        // AMQPLAIN: field-table with LOGIN and PASSWORD
        let mut table_r = Cursor::new(&response);
        if let Ok(table) = read_field_table(&mut table_r) {
            let login = match table.get("LOGIN") {
                Some(FieldValue::LongString(s)) => String::from_utf8_lossy(s).to_string(),
                _ => String::new(),
            };
            let password = match table.get("PASSWORD") {
                Some(FieldValue::LongString(s)) => String::from_utf8_lossy(s).to_string(),
                _ => String::new(),
            };
            return Ok((login, password));
        }
        warn!("malformed AMQPLAIN response");
        return Err(());
    }

    warn!(mechanism = mechanism.as_str(), "unsupported SASL mechanism");
    Err(())
}

fn parse_tune_ok(args: &[u8]) -> Result<(u16, u32, u16), ()> {
    let mut r = Cursor::new(args);
    let channel_max = read_short(&mut r).map_err(|_| ())?;
    let frame_max = read_long(&mut r).map_err(|_| ())?;
    let heartbeat = read_short(&mut r).map_err(|_| ())?;

    // Client may lower but not raise values
    let ch = if channel_max == 0 {
        DEFAULT_CHANNEL_MAX
    } else {
        channel_max.min(DEFAULT_CHANNEL_MAX)
    };
    let fm = if frame_max == 0 {
        DEFAULT_FRAME_MAX
    } else {
        frame_max.min(DEFAULT_FRAME_MAX)
    };
    Ok((ch, fm, heartbeat))
}

fn parse_connection_open(args: &[u8]) -> Result<String, ()> {
    let mut r = Cursor::new(args);
    let vhost = read_shortstr(&mut r).map_err(|_| ())?;
    // capabilities (shortstr) — deprecated, ignore
    // insist (bit) — deprecated, ignore
    Ok(vhost)
}

/// Read a single AMQP frame from the async reader.
pub async fn read_amqp_frame(reader: &mut (impl AsyncReadExt + Unpin)) -> Result<AmqpFrame, ()> {
    // Read 7-byte header
    let mut header = [0u8; 7];
    reader.read_exact(&mut header).await.map_err(|_| ())?;

    let frame_type = header[0];
    let channel = u16::from_be_bytes([header[1], header[2]]);
    let size = u32::from_be_bytes([header[3], header[4], header[5], header[6]]) as usize;

    // Read payload + frame-end
    let mut payload = vec![0u8; size + 1];
    reader.read_exact(&mut payload).await.map_err(|_| ())?;

    let frame_end = payload.pop().unwrap_or(0);
    if frame_end != FRAME_END {
        warn!(frame_end, "invalid frame-end byte");
        return Err(());
    }

    Ok(AmqpFrame {
        frame_type,
        channel,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connection_start_builds() {
        let args = build_connection_start();
        let mut r = Cursor::new(&args);
        assert_eq!(read_octet(&mut r).unwrap(), 0); // major
        assert_eq!(read_octet(&mut r).unwrap(), 9); // minor
        let props = read_field_table(&mut r).unwrap();
        assert!(props.contains_key("product"));
        assert!(props.contains_key("capabilities"));
        let mechs = read_longstr(&mut r).unwrap();
        assert_eq!(std::str::from_utf8(&mechs).unwrap(), "PLAIN AMQPLAIN");
        let locales = read_longstr(&mut r).unwrap();
        assert_eq!(std::str::from_utf8(&locales).unwrap(), "en_US");
    }

    #[test]
    fn connection_tune_roundtrip() {
        let args = build_connection_tune(2047, 131072, 60);
        let (ch, fm, hb) = parse_tune_ok(&args).unwrap();
        assert_eq!(ch, 2047);
        assert_eq!(fm, 131072);
        assert_eq!(hb, 60);
    }

    #[test]
    fn tune_ok_client_lowers() {
        let mut buf = Vec::new();
        write_short(&mut buf, 100).unwrap(); // lower channel_max
        write_long(&mut buf, 65536).unwrap(); // lower frame_max
        write_short(&mut buf, 30).unwrap(); // heartbeat
        let (ch, fm, hb) = parse_tune_ok(&buf).unwrap();
        assert_eq!(ch, 100);
        assert_eq!(fm, 65536);
        assert_eq!(hb, 30);
    }

    #[test]
    fn tune_ok_zero_means_server_default() {
        let mut buf = Vec::new();
        write_short(&mut buf, 0).unwrap();
        write_long(&mut buf, 0).unwrap();
        write_short(&mut buf, 0).unwrap();
        let (ch, fm, hb) = parse_tune_ok(&buf).unwrap();
        assert_eq!(ch, DEFAULT_CHANNEL_MAX);
        assert_eq!(fm, DEFAULT_FRAME_MAX);
        assert_eq!(hb, 0);
    }

    #[test]
    fn connection_open_parse() {
        let mut buf = Vec::new();
        write_shortstr(&mut buf, "/staging").unwrap();
        write_shortstr(&mut buf, "").unwrap(); // capabilities (deprecated)
        write_octet(&mut buf, 0).unwrap(); // insist
        let vhost = parse_connection_open(&buf).unwrap();
        assert_eq!(vhost, "/staging");
    }

    #[test]
    fn connection_close_builds() {
        let args =
            build_connection_close(NOT_FOUND, "NOT-FOUND", CLASS_QUEUE, METHOD_QUEUE_DECLARE);
        let mut r = Cursor::new(&args);
        assert_eq!(read_short(&mut r).unwrap(), NOT_FOUND);
        let text = read_shortstr(&mut r).unwrap();
        assert_eq!(text, "NOT-FOUND");
        assert_eq!(read_short(&mut r).unwrap(), CLASS_QUEUE);
        assert_eq!(read_short(&mut r).unwrap(), METHOD_QUEUE_DECLARE);
    }

    #[test]
    fn plain_auth_parse() {
        let mut buf = Vec::new();
        // client-properties
        write_field_table(&mut buf, &FieldTable::new()).unwrap();
        // mechanism
        write_shortstr(&mut buf, "PLAIN").unwrap();
        // response: \0guest\0guest
        write_longstr(&mut buf, b"\x00guest\x00guest").unwrap();
        // locale
        write_shortstr(&mut buf, "en_US").unwrap();

        let (user, pass) = parse_start_ok_credentials(&buf).unwrap();
        assert_eq!(user, "guest");
        assert_eq!(pass, "guest");
    }

    #[test]
    fn plain_auth_extracts_any_credentials() {
        // parse_start_ok_credentials no longer validates — it just extracts
        let mut buf = Vec::new();
        write_field_table(&mut buf, &FieldTable::new()).unwrap();
        write_shortstr(&mut buf, "PLAIN").unwrap();
        write_longstr(&mut buf, b"\x00alice\x00s3cret").unwrap();
        write_shortstr(&mut buf, "en_US").unwrap();
        let (user, pass) = parse_start_ok_credentials(&buf).unwrap();
        assert_eq!(user, "alice");
        assert_eq!(pass, "s3cret");
    }

    #[test]
    fn unsupported_mechanism() {
        let mut buf = Vec::new();
        write_field_table(&mut buf, &FieldTable::new()).unwrap();
        write_shortstr(&mut buf, "EXTERNAL").unwrap();
        write_longstr(&mut buf, b"").unwrap();
        write_shortstr(&mut buf, "en_US").unwrap();
        assert!(parse_start_ok_credentials(&buf).is_err());
    }

    #[test]
    fn connection_open_ok_builds() {
        let args = build_connection_open_ok();
        let mut r = Cursor::new(&args);
        let known_hosts = read_shortstr(&mut r).unwrap();
        assert_eq!(known_hosts, "");
    }
}
