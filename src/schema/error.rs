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
// File: error.rs
// Description: Structured error payloads for broker → client communication.

//! Structured error types serialized as JSON into AMQP reply_text.
//!
//! The AMQP 0-9-1 `Channel.Close` frame only has `reply_code` (u16) and
//! `reply_text` (shortstr, max 255 bytes). To send machine-readable errors,
//! we serialize a compact [`BrokerError`] struct as JSON into `reply_text`.
//!
//! The broker returns raw proto type names (e.g. "double", "int32").
//! The client is responsible for mapping them to language-specific names
//! (e.g. "number") and formatting the final user-facing message.

use serde::Serialize;

use super::validate::SchemaValidationError;

/// Maximum AMQP shortstr length in bytes.
const MAX_REPLY_TEXT_LEN: usize = 255;

/// Machine-readable error codes — the contract between broker and client.
///
/// The TypeScript client mirrors this enum 1:1 in `BrokerErrorCode`.
/// Adding a variant here requires updating the client enum too.
///
/// ```
/// use rocketmq::schema::error::ErrorCode;
/// let code = ErrorCode::SchemaTypeMismatch;
/// assert_eq!(serde_json::to_string(&code).unwrap(), "\"SchemaTypeMismatch\"");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum ErrorCode {
    /// Consumer/publisher field type doesn't match queue schema.
    SchemaTypeMismatch,
    /// Consumer has fields not present in queue schema.
    SchemaExtraFields,
    /// JSON payload is missing required schema fields.
    SchemaMissingFields,
    /// Re-declaration schema conflicts with existing queue schema.
    SchemaConflict,
    /// Proto compilation failed (syntax error, etc.).
    SchemaCompileFailed,
    /// Unsupported schema type (not protobuf).
    SchemaUnsupportedType,
    /// Schema validation on publish: wrong JSON value types.
    ValidationTypeMismatch,
    /// Payload is not valid JSON.
    ValidationInvalidJson,
    /// Required AMQP argument missing (x-schema-type, x-schema-message).
    MissingArgument,
}

/// Per-field error detail included in [`BrokerError`].
///
/// Type names are raw proto kind strings (e.g. "double", "int32", "string").
/// The client maps these to language-specific names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct FieldError {
    /// Field name in the schema.
    pub name: String,
    /// Raw proto type that the queue schema expects (e.g. "double").
    pub expected: String,
    /// Raw proto type that was actually received (e.g. "string").
    pub got: String,
}

/// Structured error payload serialized as JSON into AMQP `reply_text`.
///
/// WHY no human-readable message: the broker is language-agnostic.
/// It returns the error code, queue name, and raw field details.
/// The client is responsible for formatting the final user-facing message
/// and mapping proto types to language-specific names.
///
/// If `fields` would exceed the 255-byte AMQP shortstr limit, they are
/// truncated and `truncated` is set to `true`.
///
/// ```
/// use rocketmq::schema::error::{BrokerError, ErrorCode};
/// let err = BrokerError {
///     code: ErrorCode::SchemaMissingFields,
///     queue: "orders".into(),
///     fields: vec![],
///     truncated: false,
/// };
/// let json = err.to_reply_text();
/// assert!(json.len() <= 255);
/// ```
#[derive(Debug, Clone, Serialize)]
pub struct BrokerError {
    /// Machine-readable error code.
    pub code: ErrorCode,
    /// Target queue name.
    pub queue: String,
    /// Per-field details (type mismatches, missing fields, etc.).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub fields: Vec<FieldError>,
    /// True if `fields` was truncated to fit the 255-byte limit.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub truncated: bool,
}

impl BrokerError {
    /// Serializes to JSON, truncating `fields` if needed to fit 255 bytes.
    ///
    /// WHY truncation: AMQP shortstr is capped at 255 bytes. With many
    /// field errors, the JSON would exceed this. We iteratively remove
    /// fields from the end until it fits, then set `truncated = true`.
    pub fn to_reply_text(&self) -> String {
        let full = serde_json::to_string(self).unwrap_or_else(|_| "{}".into());
        if full.len() <= MAX_REPLY_TEXT_LEN {
            return full;
        }

        let mut shrunk = self.clone();
        while shrunk.fields.len() > 1 {
            shrunk.fields.pop();
            shrunk.truncated = true;
            let json = serde_json::to_string(&shrunk).unwrap_or_default();
            if json.len() <= MAX_REPLY_TEXT_LEN {
                return json;
            }
        }

        // Last resort: drop all fields, keep code + queue.
        shrunk.fields.clear();
        shrunk.truncated = true;
        serde_json::to_string(&shrunk).unwrap_or_else(|_| "{}".into())
    }
}

/// Returns the lowercase proto kind name for a `prost_reflect::Kind`.
///
/// WHY lowercase: the proto3 spec uses lowercase type names (e.g. "double",
/// "int32", "string"). The broker returns these raw — the client maps them
/// to language-specific names (e.g. "number" in TypeScript).
///
/// ```
/// use prost_reflect::Kind;
/// use rocketmq::schema::error::proto_kind_name;
/// assert_eq!(proto_kind_name(&Kind::Double), "double");
/// assert_eq!(proto_kind_name(&Kind::Int32), "int32");
/// assert_eq!(proto_kind_name(&Kind::String), "string");
/// ```
pub fn proto_kind_name(kind: &prost_reflect::Kind) -> &'static str {
    use prost_reflect::Kind;
    match kind {
        Kind::Double => "double",
        Kind::Float => "float",
        Kind::Int32 => "int32",
        Kind::Int64 => "int64",
        Kind::Uint32 => "uint32",
        Kind::Uint64 => "uint64",
        Kind::Sint32 => "sint32",
        Kind::Sint64 => "sint64",
        Kind::Fixed32 => "fixed32",
        Kind::Fixed64 => "fixed64",
        Kind::Sfixed32 => "sfixed32",
        Kind::Sfixed64 => "sfixed64",
        Kind::Bool => "bool",
        Kind::String => "string",
        Kind::Bytes => "bytes",
        Kind::Enum(_) => "enum",
        Kind::Message(_) => "message",
    }
}

/// Converts a [`SchemaValidationError`] into a structured [`BrokerError`].
///
/// Takes the queue name as context since `SchemaValidationError` doesn't carry it.
///
/// ```ignore
/// let broker_err = to_broker_error("my-queue", &validation_err);
/// let json = broker_err.to_reply_text();
/// ```
pub fn to_broker_error(queue: &str, err: &SchemaValidationError) -> BrokerError {
    match err {
        SchemaValidationError::TypeMismatch { errors } => BrokerError {
            code: ErrorCode::SchemaTypeMismatch,
            queue: queue.to_string(),
            fields: errors
                .iter()
                .map(|e| FieldError {
                    name: e.field_name.clone(),
                    expected: proto_kind_name(&e.queue_kind).into(),
                    got: proto_kind_name(&e.got_kind).into(),
                })
                .collect(),
            truncated: false,
        },

        SchemaValidationError::ConsumerExtraFields { fields } => BrokerError {
            code: ErrorCode::SchemaExtraFields,
            queue: queue.to_string(),
            fields: fields
                .iter()
                .map(|name| FieldError {
                    name: name.clone(),
                    expected: "undefined".into(),
                    got: "declared".into(),
                })
                .collect(),
            truncated: false,
        },

        SchemaValidationError::MissingFields { fields } => BrokerError {
            code: ErrorCode::SchemaMissingFields,
            queue: queue.to_string(),
            fields: fields
                .iter()
                .map(|name| FieldError {
                    name: name.clone(),
                    expected: "required".into(),
                    got: "missing".into(),
                })
                .collect(),
            truncated: false,
        },

        SchemaValidationError::SchemaConflict { .. } => BrokerError {
            code: ErrorCode::SchemaConflict,
            queue: queue.to_string(),
            fields: vec![],
            truncated: false,
        },

        SchemaValidationError::InvalidJson(_) => BrokerError {
            code: ErrorCode::ValidationInvalidJson,
            queue: queue.to_string(),
            fields: vec![],
            truncated: false,
        },

        SchemaValidationError::DecodeFailed(_) => BrokerError {
            code: ErrorCode::ValidationTypeMismatch,
            queue: queue.to_string(),
            fields: vec![],
            truncated: false,
        },

        SchemaValidationError::TrailingBytes { .. } => BrokerError {
            code: ErrorCode::ValidationTypeMismatch,
            queue: queue.to_string(),
            fields: vec![],
            truncated: false,
        },

        SchemaValidationError::WrongContentType { .. } => BrokerError {
            code: ErrorCode::ValidationTypeMismatch,
            queue: queue.to_string(),
            fields: vec![],
            truncated: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_serializes_as_string() {
        let code = ErrorCode::SchemaTypeMismatch;
        let json = serde_json::to_string(&code).unwrap();
        assert_eq!(json, "\"SchemaTypeMismatch\"");
    }

    #[test]
    fn broker_error_fits_in_255_bytes() {
        let err = BrokerError {
            code: ErrorCode::SchemaMissingFields,
            queue: "orders".into(),
            fields: vec![
                FieldError {
                    name: "id".into(),
                    expected: "required".into(),
                    got: "missing".into(),
                },
                FieldError {
                    name: "name".into(),
                    expected: "required".into(),
                    got: "missing".into(),
                },
            ],
            truncated: false,
        };
        let json = err.to_reply_text();
        assert!(json.len() <= 255, "JSON was {} bytes: {}", json.len(), json);
    }

    #[test]
    fn truncation_removes_fields_to_fit() {
        let many_fields: Vec<FieldError> = (0..20)
            .map(|i| FieldError {
                name: format!("field_{}", i),
                expected: "double".into(),
                got: "string".into(),
            })
            .collect();

        let err = BrokerError {
            code: ErrorCode::SchemaTypeMismatch,
            queue: "very-long-queue-name-for-testing".into(),
            fields: many_fields,
            truncated: false,
        };

        let json = err.to_reply_text();
        assert!(json.len() <= 255, "JSON was {} bytes", json.len());
        assert!(json.contains("\"truncated\":true"));
    }

    #[test]
    fn empty_fields_not_serialized() {
        let err = BrokerError {
            code: ErrorCode::ValidationInvalidJson,
            queue: "q".into(),
            fields: vec![],
            truncated: false,
        };
        let json = serde_json::to_string(&err).unwrap();
        assert!(!json.contains("\"fields\""));
        assert!(!json.contains("\"truncated\""));
    }

    #[test]
    fn proto_kind_name_returns_lowercase() {
        assert_eq!(proto_kind_name(&prost_reflect::Kind::Double), "double");
        assert_eq!(proto_kind_name(&prost_reflect::Kind::Int32), "int32");
        assert_eq!(proto_kind_name(&prost_reflect::Kind::String), "string");
        assert_eq!(proto_kind_name(&prost_reflect::Kind::Bool), "bool");
        assert_eq!(proto_kind_name(&prost_reflect::Kind::Bytes), "bytes");
        assert_eq!(proto_kind_name(&prost_reflect::Kind::Float), "float");
        assert_eq!(proto_kind_name(&prost_reflect::Kind::Int64), "int64");
        assert_eq!(proto_kind_name(&prost_reflect::Kind::Uint32), "uint32");
    }

    #[test]
    fn to_broker_error_type_mismatch() {
        use super::super::validate::FieldTypeMismatch;

        let validation = SchemaValidationError::TypeMismatch {
            errors: vec![FieldTypeMismatch {
                field_name: "id".into(),
                queue_kind: prost_reflect::Kind::Double,
                got_kind: prost_reflect::Kind::String,
            }],
        };

        let broker_err = to_broker_error("test-queue", &validation);
        assert_eq!(broker_err.code, ErrorCode::SchemaTypeMismatch);
        assert_eq!(broker_err.queue, "test-queue");
        assert_eq!(broker_err.fields.len(), 1);
        assert_eq!(broker_err.fields[0].name, "id");
        assert_eq!(broker_err.fields[0].expected, "double");
        assert_eq!(broker_err.fields[0].got, "string");
    }

    #[test]
    fn to_broker_error_extra_fields() {
        let validation = SchemaValidationError::ConsumerExtraFields {
            fields: vec!["email".into(), "phone".into()],
        };

        let broker_err = to_broker_error("users", &validation);
        assert_eq!(broker_err.code, ErrorCode::SchemaExtraFields);
        assert_eq!(broker_err.fields.len(), 2);
        assert_eq!(broker_err.fields[0].name, "email");
    }

    #[test]
    fn to_broker_error_missing_fields() {
        let validation = SchemaValidationError::MissingFields {
            fields: vec!["age".into()],
        };

        let broker_err = to_broker_error("users", &validation);
        assert_eq!(broker_err.code, ErrorCode::SchemaMissingFields);
        assert_eq!(broker_err.fields.len(), 1);
    }

    #[test]
    fn to_broker_error_schema_conflict() {
        let validation = SchemaValidationError::SchemaConflict {
            differences: vec!["field 'age': type Int32 vs String".into()],
        };

        let broker_err = to_broker_error("orders", &validation);
        assert_eq!(broker_err.code, ErrorCode::SchemaConflict);
        assert!(broker_err.fields.is_empty());
    }

    #[test]
    fn to_broker_error_invalid_json() {
        let validation = SchemaValidationError::InvalidJson("unexpected token".into());
        let broker_err = to_broker_error("q", &validation);
        assert_eq!(broker_err.code, ErrorCode::ValidationInvalidJson);
    }

    #[test]
    fn to_broker_error_reply_text_is_valid_json() {
        use super::super::validate::FieldTypeMismatch;

        let validation = SchemaValidationError::TypeMismatch {
            errors: vec![FieldTypeMismatch {
                field_name: "count".into(),
                queue_kind: prost_reflect::Kind::Int32,
                got_kind: prost_reflect::Kind::Double,
            }],
        };

        let json = to_broker_error("q", &validation).to_reply_text();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["code"], "SchemaTypeMismatch");
        assert_eq!(parsed["queue"], "q");
        assert_eq!(parsed["fields"][0]["expected"], "int32");
        assert_eq!(parsed["fields"][0]["got"], "double");
    }
}
