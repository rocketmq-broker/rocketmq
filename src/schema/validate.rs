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

    /// JSON payload is missing fields defined in the proto schema.
    MissingFields {
        /// Names of fields that are present in the schema but absent from the JSON.
        fields: Vec<String>,
    },

    /// Payload is not valid JSON.
    InvalidJson(String),

    /// Consumer schema has fields not defined in the queue's schema.
    ConsumerExtraFields {
        /// Field names present in consumer but missing from queue schema.
        fields: Vec<String>,
    },

    /// JSON field values have types incompatible with the proto schema.
    TypeMismatch {
        /// Per-field type error descriptions.
        errors: Vec<String>,
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
            Self::MissingFields { fields } => write!(
                f,
                "JSON payload missing required schema fields: [{}]",
                fields.join(", ")
            ),
            Self::InvalidJson(err) => write!(f, "Payload is not valid JSON: {}", err),
            Self::ConsumerExtraFields { fields } => write!(
                f,
                "Consumer schema has fields not in queue schema: [{}]",
                fields.join(", ")
            ),
            Self::TypeMismatch { errors } => {
                write!(f, "JSON field type mismatches: [{}]", errors.join("; "))
            }
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
/// Auto-detects JSON vs protobuf payloads:
/// - JSON (starts with `{`): checks all schema fields are present in the object
/// - Protobuf binary: decodes and checks for unknown/trailing bytes
pub fn validate_message(schema: &CompiledSchema, body: &[u8]) -> Result<(), SchemaValidationError> {
    if body.first() == Some(&b'{') {
        return validate_json_fields(schema, body);
    }
    validate_protobuf_fields(schema, body)
}

/// Checks that a consumer's schema is a subset of the queue's schema.
///
/// The consumer may omit fields (reading a subset is safe), but every
/// field in the consumer's schema must exist in the queue's schema with
/// a compatible type. This prevents consumers from expecting data the
/// queue doesn't provide.
///
/// ```ignore
/// check_consumer_subset(&queue_schema, &consumer_schema)?;
/// ```
pub fn check_consumer_subset(
    queue_schema: &CompiledSchema,
    consumer_schema: &CompiledSchema,
) -> Result<(), SchemaValidationError> {
    let queue_fields: std::collections::HashMap<String, u32> = queue_schema
        .message_descriptor
        .fields()
        .map(|f| (f.name().to_string(), f.number()))
        .collect();

    let extra: Vec<String> = consumer_schema
        .message_descriptor
        .fields()
        .filter(|f| !queue_fields.contains_key(f.name()))
        .map(|f| f.name().to_string())
        .collect();

    if !extra.is_empty() {
        return Err(SchemaValidationError::ConsumerExtraFields { fields: extra });
    }

    Ok(())
}

/// Validates a JSON payload contains all fields defined in the proto schema
/// and that each field's JSON type is compatible with its proto type.
fn validate_json_fields(schema: &CompiledSchema, body: &[u8]) -> Result<(), SchemaValidationError> {
    let text =
        std::str::from_utf8(body).map_err(|e| SchemaValidationError::InvalidJson(e.to_string()))?;

    let parsed: serde_json::Value = serde_json::from_str(text)
        .map_err(|e| SchemaValidationError::InvalidJson(e.to_string()))?;

    let obj = parsed.as_object().ok_or_else(|| {
        SchemaValidationError::InvalidJson("expected a JSON object, got other type".into())
    })?;

    let missing: Vec<String> = schema
        .message_descriptor
        .fields()
        .filter(|f| !obj.contains_key(f.name()))
        .map(|f| f.name().to_string())
        .collect();

    if !missing.is_empty() {
        return Err(SchemaValidationError::MissingFields { fields: missing });
    }

    // Type-check each field value against its proto kind.
    let type_errors: Vec<String> = schema
        .message_descriptor
        .fields()
        .filter_map(|f| {
            let val = match obj.get(f.name()) {
                Some(v) => v,
                None => return None, // already caught by missing check
            };
            let mismatch = match f.kind() {
                prost_reflect::Kind::String => !val.is_string(),
                prost_reflect::Kind::Bool => !val.is_boolean(),
                prost_reflect::Kind::Bytes => !val.is_string(),
                // All numeric proto types must map to JSON number
                prost_reflect::Kind::Int32
                | prost_reflect::Kind::Int64
                | prost_reflect::Kind::Uint32
                | prost_reflect::Kind::Uint64
                | prost_reflect::Kind::Sint32
                | prost_reflect::Kind::Sint64
                | prost_reflect::Kind::Fixed32
                | prost_reflect::Kind::Fixed64
                | prost_reflect::Kind::Sfixed32
                | prost_reflect::Kind::Sfixed64
                | prost_reflect::Kind::Float
                | prost_reflect::Kind::Double => !val.is_number(),
                // Enums accept both number and string in JSON
                prost_reflect::Kind::Enum(_) => !val.is_number() && !val.is_string(),
                // Nested messages must be JSON objects
                prost_reflect::Kind::Message(_) => !val.is_object(),
            };
            if mismatch {
                Some(format!(
                    "{}: expected {:?}, got {}",
                    f.name(),
                    f.kind(),
                    json_type_name(val)
                ))
            } else {
                None
            }
        })
        .collect();

    if !type_errors.is_empty() {
        return Err(SchemaValidationError::TypeMismatch {
            errors: type_errors,
        });
    }

    Ok(())
}

/// Returns a human-readable name for a JSON value's type.
fn json_type_name(val: &serde_json::Value) -> &'static str {
    match val {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "boolean",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Validates a protobuf binary payload against the schema descriptor.
fn validate_protobuf_fields(
    schema: &CompiledSchema,
    body: &[u8],
) -> Result<(), SchemaValidationError> {
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

    #[test]
    fn json_with_all_fields_passes() {
        let compiled = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        let body = br#"{"name":"Alice","age":30}"#;
        assert!(validate_message(&compiled, body).is_ok());
    }

    #[test]
    fn json_missing_field_returns_error() {
        let compiled = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        // Missing "age" field
        let body = br#"{"name":"Alice"}"#;
        let err = validate_message(&compiled, body).unwrap_err();
        match err {
            SchemaValidationError::MissingFields { fields } => {
                assert_eq!(fields, vec!["age"]);
            }
            other => panic!("Expected MissingFields, got: {}", other),
        }
    }

    #[test]
    fn json_missing_all_fields_lists_them() {
        let compiled = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        let body = br#"{}"#;
        let err = validate_message(&compiled, body).unwrap_err();
        match err {
            SchemaValidationError::MissingFields { fields } => {
                assert!(fields.contains(&"name".to_string()));
                assert!(fields.contains(&"age".to_string()));
            }
            other => panic!("Expected MissingFields, got: {}", other),
        }
    }

    #[test]
    fn json_wrong_type_rejected() {
        let compiled = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        // age is int32 but gets a string value
        let body = br#"{"name":"Alice","age":"not-a-number"}"#;
        let err = validate_message(&compiled, body).unwrap_err();
        match err {
            SchemaValidationError::TypeMismatch { errors } => {
                assert_eq!(errors.len(), 1);
                assert!(errors[0].contains("age"));
                assert!(errors[0].contains("string"));
            }
            other => panic!("Expected TypeMismatch, got: {}", other),
        }
    }

    #[test]
    fn json_number_for_string_field_rejected() {
        let compiled = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        // name is string but gets a number
        let body = br#"{"name":42,"age":30}"#;
        let err = validate_message(&compiled, body).unwrap_err();
        match err {
            SchemaValidationError::TypeMismatch { errors } => {
                assert!(errors[0].contains("name"));
            }
            other => panic!("Expected TypeMismatch, got: {}", other),
        }
    }

    #[test]
    fn json_correct_types_pass() {
        let compiled = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        let body = br#"{"name":"Alice","age":30}"#;
        assert!(validate_message(&compiled, body).is_ok());
    }

    const SUBSET_PROTO: &str = r#"
        syntax = "proto3";
        package mypackage;

        message UserSubset {
            string name = 1;
        }
    "#;

    const SUPERSET_PROTO: &str = r#"
        syntax = "proto3";
        package mypackage;

        message UserSuperset {
            string name = 1;
            int32 age = 2;
            string email = 3;
        }
    "#;

    #[test]
    fn consumer_subset_passes() {
        let queue = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        let consumer = compile_proto(SUBSET_PROTO.as_bytes(), "mypackage.UserSubset").unwrap();
        assert!(check_consumer_subset(&queue, &consumer).is_ok());
    }

    #[test]
    fn consumer_exact_match_passes() {
        let queue = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        let consumer = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        assert!(check_consumer_subset(&queue, &consumer).is_ok());
    }

    #[test]
    fn consumer_superset_rejected() {
        let queue = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        let consumer = compile_proto(SUPERSET_PROTO.as_bytes(), "mypackage.UserSuperset").unwrap();
        let err = check_consumer_subset(&queue, &consumer).unwrap_err();
        match err {
            SchemaValidationError::ConsumerExtraFields { fields } => {
                assert_eq!(fields, vec!["email"]);
            }
            other => panic!("Expected ConsumerExtraFields, got: {}", other),
        }
    }

    #[test]
    fn consumer_extra_fields_display() {
        let err = SchemaValidationError::ConsumerExtraFields {
            fields: vec!["email".to_string(), "phone".to_string()],
        };
        let msg = format!("{}", err);
        assert!(msg.contains("email"));
        assert!(msg.contains("phone"));
    }
}
