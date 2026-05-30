// Copyright (c) 2026 Edilson Pateguana
// Licensed under the Apache License, Version 2.0

//! Confluent-compatible wire format for schema-registry-encoded messages.
//!
//! Every message carries a 5-byte prefix:
//! ```text
//! [0x00] [schema_id: u32 big-endian] [payload...]
//! ```
//!
//! - Byte 0: magic byte `0x00` — identifies a schema-registry-encoded message.
//! - Bytes 1–4: schema ID as big-endian u32.
//! - Bytes 5+: the actual protobuf-encoded payload.

/// Magic byte at the start of every schema-registry-encoded message.
pub const WIRE_MAGIC: u8 = 0x00;

/// Total size of the wire prefix (magic + 4-byte schema ID).
pub const WIRE_PREFIX_LEN: usize = 5;

/// Encodes the 5-byte wire prefix for a given schema ID.
///
/// ```
/// use rocketmq::schema::wire::encode_prefix;
/// let prefix = encode_prefix(42);
/// assert_eq!(prefix, [0x00, 0, 0, 0, 42]);
/// ```
pub fn encode_prefix(schema_id: u32) -> [u8; WIRE_PREFIX_LEN] {
    let id_bytes = schema_id.to_be_bytes();
    [
        WIRE_MAGIC,
        id_bytes[0],
        id_bytes[1],
        id_bytes[2],
        id_bytes[3],
    ]
}

/// Decodes the wire prefix from a message body.
///
/// Returns `Some((schema_id, payload_slice))` if the prefix is valid,
/// or `None` if the body is too short or the magic byte doesn't match.
///
/// ```
/// use rocketmq::schema::wire::{encode_prefix, decode_prefix};
/// let mut msg = encode_prefix(7).to_vec();
/// msg.extend_from_slice(b"hello");
/// let (id, payload) = decode_prefix(&msg).unwrap();
/// assert_eq!(id, 7);
/// assert_eq!(payload, b"hello");
/// ```
pub fn decode_prefix(body: &[u8]) -> Option<(u32, &[u8])> {
    if body.len() < WIRE_PREFIX_LEN {
        return None;
    }
    if body[0] != WIRE_MAGIC {
        return None;
    }
    let schema_id = u32::from_be_bytes([body[1], body[2], body[3], body[4]]);
    Some((schema_id, &body[WIRE_PREFIX_LEN..]))
}

/// Returns true if the body starts with the schema-registry wire prefix.
pub fn has_wire_prefix(body: &[u8]) -> bool {
    body.len() >= WIRE_PREFIX_LEN && body[0] == WIRE_MAGIC
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let prefix = encode_prefix(12345);
        let mut msg = prefix.to_vec();
        msg.extend_from_slice(b"proto-payload");

        let (id, payload) = decode_prefix(&msg).unwrap();
        assert_eq!(id, 12345);
        assert_eq!(payload, b"proto-payload");
    }

    #[test]
    fn too_short() {
        assert!(decode_prefix(&[0x00, 1, 2]).is_none());
    }

    #[test]
    fn wrong_magic() {
        assert!(decode_prefix(&[0xFF, 0, 0, 0, 1, 10]).is_none());
    }

    #[test]
    fn has_prefix_check() {
        assert!(has_wire_prefix(&encode_prefix(1)));
        assert!(!has_wire_prefix(&[0xFF, 0, 0, 0, 1]));
        assert!(!has_wire_prefix(&[0x00, 1]));
    }

    #[test]
    fn zero_id() {
        let buf = encode_prefix(0);
        let (id, payload) = decode_prefix(&buf).unwrap();
        assert_eq!(id, 0);
        assert!(payload.is_empty());
    }

    #[test]
    fn max_id() {
        let (id, _) = decode_prefix(&encode_prefix(u32::MAX)).unwrap();
        assert_eq!(id, u32::MAX);
    }
}
