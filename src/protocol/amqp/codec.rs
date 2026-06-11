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
// File: amqp_codec.rs
// Description: AMQP frame encoder and decoder implementation over AsyncRead/AsyncWrite.

//! AMQP 0-9-1 frame codec.
//!
//! Frame format: type(1) | channel(2) | size(4) | payload(size) | frame-end(1=0xCE)
//! Total overhead: 8 bytes per frame.
//!
//! Frame types:
//!   1 = METHOD   — carries class_id + method_id + arguments
//!   2 = HEADER   — content header (properties)
//!   3 = BODY     — content body (opaque bytes)
//!   8 = HEARTBEAT

use std::io::{self, Cursor};

use crate::protocol::amqp::properties::BasicProperties;

pub const FRAME_METHOD: u8 = 1;
pub const FRAME_HEADER: u8 = 2;
pub const FRAME_BODY: u8 = 3;
pub const FRAME_HEARTBEAT: u8 = 8;
pub const FRAME_END: u8 = 0xCE;

pub const PROTOCOL_HEADER: [u8; 8] = [b'A', b'M', b'Q', b'P', 0, 0, 9, 1];

pub const DEFAULT_FRAME_MAX: u32 = 131_072;

pub const DEFAULT_CHANNEL_MAX: u16 = 2047;

pub const DEFAULT_HEARTBEAT: u16 = 60;

#[derive(Clone, Debug)]
pub struct AmqpFrame {
    pub frame_type: u8,
    pub channel: u16,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct MethodFrame {
    pub class_id: u16,
    pub method_id: u16,
    pub arguments: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct ContentHeader {
    pub class_id: u16,
    pub body_size: u64,
    pub properties: BasicProperties,
}

/// Writes the 7-byte AMQP frame header (type + channel + size) directly
/// into a pre-allocated buffer using a single memcpy. Avoids per-field
/// extend_from_slice overhead on the hot path.
#[inline(always)]
fn write_frame_header(buf: &mut Vec<u8>, frame_type: u8, channel: u16, payload_size: u32) {
    let ch = channel.to_be_bytes();
    let sz = payload_size.to_be_bytes();
    buf.extend_from_slice(&[frame_type, ch[0], ch[1], sz[0], sz[1], sz[2], sz[3]]);
}

#[inline]
pub fn encode_method_frame(channel: u16, class_id: u16, method_id: u16, args: &[u8]) -> Vec<u8> {
    let payload_size = 4 + args.len();
    let total = 8 + payload_size;
    let mut buf = Vec::with_capacity(total);
    write_frame_header(&mut buf, FRAME_METHOD, channel, payload_size as u32);
    buf.extend_from_slice(&class_id.to_be_bytes());
    buf.extend_from_slice(&method_id.to_be_bytes());
    buf.extend_from_slice(args);
    buf.push(FRAME_END);
    buf
}

/// OPT-9: Encodes properties directly into the frame buffer.
/// Previous impl allocated an intermediate `prop_buf` Vec then copied.
#[inline]
pub fn encode_content_header(
    channel: u16,
    class_id: u16,
    body_size: u64,
    properties: &BasicProperties,
) -> Vec<u8> {
    // First pass: encode props into the buffer at the right offset.
    // We reserve a generous estimate, then backpatch the size field.
    let mut buf = Vec::with_capacity(128);
    write_frame_header(&mut buf, FRAME_HEADER, channel, 0); // size placeholder
    buf.extend_from_slice(&class_id.to_be_bytes());
    buf.extend_from_slice(&0u16.to_be_bytes());
    buf.extend_from_slice(&body_size.to_be_bytes());
    let prop_start = buf.len();
    properties.encode(&mut buf).expect("properties encode");
    // Backpatch the payload size in the frame header (bytes 3..7)
    let payload_size = (buf.len() - 7) as u32;
    buf[3..7].copy_from_slice(&payload_size.to_be_bytes());
    let _ = prop_start; // suppress unused warning
    buf.push(FRAME_END);
    buf
}

#[inline]
pub fn encode_body_frame(channel: u16, body: &[u8]) -> Vec<u8> {
    let total = 8 + body.len();
    let mut buf = Vec::with_capacity(total);
    write_frame_header(&mut buf, FRAME_BODY, channel, body.len() as u32);
    buf.extend_from_slice(body);
    buf.push(FRAME_END);
    buf
}

/// Static heartbeat frame — zero allocation on every heartbeat.
static HEARTBEAT_FRAME: [u8; 8] = [FRAME_HEARTBEAT, 0, 0, 0, 0, 0, 0, FRAME_END];

#[inline(always)]
pub fn heartbeat_bytes() -> &'static [u8; 8] {
    &HEARTBEAT_FRAME
}

/// Legacy API — returns a Vec copy for callers that need owned data.
/// Prefer `heartbeat_bytes()` on hot paths.
#[inline]
pub fn encode_heartbeat() -> Vec<u8> {
    HEARTBEAT_FRAME.to_vec()
}

#[inline]
pub fn decode_frame(data: &[u8]) -> io::Result<(AmqpFrame, usize)> {
    if data.len() < 8 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "frame too short",
        ));
    }

    let frame_type = data[0];
    let channel = u16::from_be_bytes([data[1], data[2]]);
    let size = u32::from_be_bytes([data[3], data[4], data[5], data[6]]) as usize;

    let total = 7 + size + 1;
    if data.len() < total {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "incomplete frame",
        ));
    }

    if data[7 + size] != FRAME_END {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("invalid frame-end: 0x{:02X}", data[7 + size]),
        ));
    }

    let payload = data[7..7 + size].to_vec();
    Ok((
        AmqpFrame {
            frame_type,
            channel,
            payload,
        },
        total,
    ))
}

#[inline]
pub fn decode_method(payload: &[u8]) -> io::Result<MethodFrame> {
    if payload.len() < 4 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "method payload too short",
        ));
    }
    let class_id = u16::from_be_bytes([payload[0], payload[1]]);
    let method_id = u16::from_be_bytes([payload[2], payload[3]]);
    let arguments = payload[4..].to_vec();
    Ok(MethodFrame {
        class_id,
        method_id,
        arguments,
    })
}

pub fn decode_content_header(payload: &[u8]) -> io::Result<ContentHeader> {
    if payload.len() < 14 {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "content header too short",
        ));
    }
    let class_id = u16::from_be_bytes([payload[0], payload[1]]);

    let body_size = u64::from_be_bytes([
        payload[4],
        payload[5],
        payload[6],
        payload[7],
        payload[8],
        payload[9],
        payload[10],
        payload[11],
    ]);
    let mut cursor = Cursor::new(&payload[12..]);
    let properties = BasicProperties::decode(&mut cursor)?;

    Ok(ContentHeader {
        class_id,
        body_size,
        properties,
    })
}

pub fn split_body_frames(channel: u16, body: &[u8], frame_max: u32) -> Vec<Vec<u8>> {
    let max_body_per_frame = if frame_max > 8 {
        (frame_max - 8) as usize
    } else {
        body.len()
    };

    if body.is_empty() {
        return vec![];
    }

    body.chunks(max_body_per_frame)
        .map(|chunk| encode_body_frame(channel, chunk))
        .collect()
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    use crate::protocol::amqp::method;

    #[test]
    fn protocol_header_correct() {
        assert_eq!(&PROTOCOL_HEADER, b"AMQP\x00\x00\x09\x01");
    }

    #[test]
    fn heartbeat_frame_encode_decode() {
        let hb = encode_heartbeat();
        assert_eq!(hb.len(), 8);
        assert_eq!(hb[0], FRAME_HEARTBEAT);
        assert_eq!(hb[7], FRAME_END);

        let (frame, consumed) = decode_frame(&hb).unwrap();
        assert_eq!(consumed, 8);
        assert_eq!(frame.frame_type, FRAME_HEARTBEAT);
        assert_eq!(frame.channel, 0);
        assert!(frame.payload.is_empty());
    }

    #[test]
    fn method_frame_roundtrip() {
        let args = vec![0x00, 0x0A, 0x41, 0x42];
        let wire = encode_method_frame(1, 10, 10, &args);

        let (frame, consumed) = decode_frame(&wire).unwrap();
        assert_eq!(consumed, wire.len());
        assert_eq!(frame.frame_type, FRAME_METHOD);
        assert_eq!(frame.channel, 1);

        let method = decode_method(&frame.payload).unwrap();
        assert_eq!(method.class_id, 10);
        assert_eq!(method.method_id, 10);
        assert_eq!(method.arguments, args);
    }

    #[test]
    fn content_header_roundtrip() {
        let props = BasicProperties {
            content_type: Some("text/plain".into()),
            delivery_mode: Some(2),
            ..Default::default()
        };
        let wire = encode_content_header(1, method::CLASS_BASIC, 42, &props);

        let (frame, _) = decode_frame(&wire).unwrap();
        assert_eq!(frame.frame_type, FRAME_HEADER);

        let header = decode_content_header(&frame.payload).unwrap();
        assert_eq!(header.class_id, method::CLASS_BASIC);
        assert_eq!(header.body_size, 42);
        assert_eq!(header.properties.content_type, Some("text/plain".into()));
        assert_eq!(header.properties.delivery_mode, Some(2));
    }

    #[test]
    fn body_frame_roundtrip() {
        let body = b"hello world";
        let wire = encode_body_frame(1, body);

        let (frame, _) = decode_frame(&wire).unwrap();
        assert_eq!(frame.frame_type, FRAME_BODY);
        assert_eq!(frame.channel, 1);
        assert_eq!(frame.payload, body);
    }

    #[test]
    fn body_split_single() {
        let body = b"small";
        let frames = split_body_frames(1, body, 1024);
        assert_eq!(frames.len(), 1);
    }

    #[test]
    fn body_split_multiple() {
        let body = vec![0x41; 100];
        let frames = split_body_frames(1, &body, 50);
        assert!(frames.len() >= 3);

        let mut reconstructed = Vec::new();
        for wire in &frames {
            let (frame, _) = decode_frame(wire).unwrap();
            reconstructed.extend_from_slice(&frame.payload);
        }
        assert_eq!(reconstructed, body);
    }

    #[test]
    fn body_split_empty() {
        let frames = split_body_frames(1, b"", 1024);
        assert!(frames.is_empty());
    }

    #[test]
    fn frame_bad_end_marker() {
        let mut wire = encode_heartbeat();
        wire[7] = 0x00;
        assert!(decode_frame(&wire).is_err());
    }

    #[test]
    fn frame_too_short() {
        assert!(decode_frame(&[0, 0, 0]).is_err());
    }

    #[test]
    fn frame_incomplete_payload() {
        let wire = vec![1, 0, 1, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xCE];
        assert!(decode_frame(&wire).is_err());
    }

    #[test]
    fn method_payload_too_short() {
        assert!(decode_method(&[0, 0]).is_err());
    }

    #[test]
    fn content_header_too_short() {
        assert!(decode_content_header(&[0; 10]).is_err());
    }

    #[test]
    fn method_frame_channel_zero() {
        let wire = encode_method_frame(
            0,
            method::CLASS_CONNECTION,
            method::METHOD_CONNECTION_START,
            &[],
        );
        let (frame, _) = decode_frame(&wire).unwrap();
        assert_eq!(frame.channel, 0);
    }
}
