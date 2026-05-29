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
// File: validate.rs
// Description: Message payload schema validation engine.

use crate::schema::CompiledSchema;
use prost::Message;
use prost_reflect::DynamicMessage;

/// Represents validation errors for messages checked against a schema.
#[derive(Debug)]
pub enum SchemaValidationError {
    /// Decoding the binary payload against the message descriptor failed.
    DecodeFailed(String),

    /// Payload contains trailing, unconsumed bytes.
    TrailingBytes {
        /// Number of unconsumed bytes at the end of the payload.
        unconsumed_len: usize,
    },

    /// The payload's content-type does not indicate Protobuf encoding.
    WrongContentType {
        /// The content-type that was found on the message properties.
        got: Option<String>,
    },
}

impl std::fmt::Display for SchemaValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DecodeFailed(err) => write!(f, "Protobuf decode failed: {}", err),
            Self::TrailingBytes { unconsumed_len } => write!(
                f,
                "Protobuf payload contains {} trailing, unconsumed bytes",
                unconsumed_len
            ),
            Self::WrongContentType { got } => write!(
                f,
                "Message properties have invalid content_type: '{:?}'. Must contain 'protobuf'.",
                got
            ),
        }
    }
}

impl std::error::Error for SchemaValidationError {}

/// Helper to determine if a given content type string indicates a Protobuf encoded payload.
///
/// Accepts standard MIME type strings such as `application/protobuf`, `application/x-protobuf`,
/// or vendor specific types like `application/vnd.company.user+protobuf`.
pub fn is_protobuf_content(content_type: &Option<String>) -> bool {
    match content_type {
        Some(ct) => ct.contains("protobuf"),
        None => false,
    }
}

/// Validates that the raw payload matches the schema.
///
/// Decodes the payload into a dynamic message to ensure all fields match the cached descriptor,
/// and checks that the entire byte slice is fully consumed.
pub fn validate_message(schema: &CompiledSchema, body: &[u8]) -> Result<(), SchemaValidationError> {
    let mut buf = body;
    let mut msg = DynamicMessage::decode(schema.message_descriptor.clone(), &mut buf)
        .map_err(|e| SchemaValidationError::DecodeFailed(e.to_string()))?;

    let has_unknown = msg.unknown_fields().next().is_some();

    if has_unknown || !buf.is_empty() {
        // DynamicMessage::take_unknown_fields returns a lazy iterator; we must consume it (e.g. using count())
        // to actually drain the unknown fields from the message.
        let _ = msg.take_unknown_fields().count();
        let clean_len = msg.encoded_len();
        let unconsumed_len = body.len().saturating_sub(clean_len);
        return Err(SchemaValidationError::TrailingBytes { unconsumed_len });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::compile_proto;

    const SIMPLE_PROTO: &str = r#"
        syntax = "proto3";
        package mypackage;

        message UserCreated {
            string name = 1;
            int32 age = 2;
        }
    "#;

    #[test]
    fn test_is_protobuf_content() {
        assert!(is_protobuf_content(&Some(
            "application/protobuf".to_string()
        )));
        assert!(is_protobuf_content(&Some(
            "application/x-protobuf".to_string()
        )));
        assert!(is_protobuf_content(&Some(
            "application/vnd.company.user+protobuf".to_string()
        )));
        assert!(!is_protobuf_content(&Some("application/json".to_string())));
        assert!(!is_protobuf_content(&None));
    }

    #[test]
    fn test_validate_valid_message() {
        let compiled = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();

        // Let's build a valid binary payload for UserCreated.

        let mut body = vec![0x0A, 5];
        body.extend_from_slice(b"Alice");
        body.extend_from_slice(&[0x10, 0x1E]);

        let res = validate_message(&compiled, &body);
        assert!(res.is_ok());
    }

    #[test]
    fn test_validate_invalid_message() {
        let compiled = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();

        let body = vec![0xFF, 0xFF, 0xFF];
        let res = validate_message(&compiled, &body);
        assert!(res.is_err());
        match res.unwrap_err() {
            SchemaValidationError::DecodeFailed(_) => {}
            _ => panic!("Expected DecodeFailed error"),
        }
    }

    #[test]
    fn test_validate_trailing_bytes() {
        let compiled = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();

        let mut body = vec![0x0A, 5];
        body.extend_from_slice(b"Alice");
        body.extend_from_slice(&[0x10, 0x1E]);

        body.extend_from_slice(b"extra");

        let res = validate_message(&compiled, &body);
        assert!(res.is_err());
        match res.unwrap_err() {
            SchemaValidationError::TrailingBytes { unconsumed_len } => {
                assert_eq!(unconsumed_len, 5);
            }
            _ => panic!("Expected TrailingBytes error"),
        }
    }
}
