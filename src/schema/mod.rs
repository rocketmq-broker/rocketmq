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
// File: mod.rs
// Description: Core schema registry, compilation pipeline, and validation entrypoints.

pub mod validate;

use prost::Message;
use prost_reflect::{DescriptorPool, MessageDescriptor};
use std::sync::atomic::{AtomicU64, Ordering};

/// Global atomic counter for schema IDs.
static SCHEMA_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Allocates a globally unique, monotonically increasing schema ID.
pub fn alloc_schema_id() -> u64 {
    SCHEMA_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Supported schema formats.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SchemaFormat {
    /// Protobuf (.proto) schema definition.
    Protobuf,
}

/// A compiled, cached schema that can validate messages.
///
/// Wrapped in `Arc` within `QueueState` to avoid cloning the large underlying
/// descriptor pool and message descriptors on every publish operation.
pub struct CompiledSchema {
    /// Unique ID allocated for this schema version.
    pub id: u64,
    /// Origin format of the schema.
    pub format: SchemaFormat,
    /// Raw string/bytes content of the schema as provided by the client.
    pub raw: Vec<u8>,
    /// Serialized FileDescriptorSet bytes for WAL replication and recovery.
    pub descriptor_set_bytes: Vec<u8>,
    /// The decoded DescriptorPool.
    pub pool: DescriptorPool,
    /// The specific message type cached descriptor to validate against.
    pub message_descriptor: MessageDescriptor,
}

impl std::fmt::Debug for CompiledSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompiledSchema")
            .field("id", &self.id)
            .field("format", &self.format)
            .field("message_name", &self.message_descriptor.full_name())
            .finish_non_exhaustive()
    }
}

/// Represents errors that can occur during the schema compilation pipeline.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum SchemaCompileError {
    /// The raw schema provided is not valid UTF-8.
    InvalidProto(String),

    /// Compiling the raw proto source into a FileDescriptorSet failed.
    CompilationFailed(String),

    /// The compiled DescriptorPool did not contain the requested message type.
    MessageNotFound {
        /// The fully qualified message name that was requested.
        requested: String,
        /// The fully qualified names of all messages found in the schema.
        available: Vec<String>,
    },
}

impl std::fmt::Display for SchemaCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidProto(err) => write!(f, "Schema proto string is not valid UTF-8: {}", err),
            Self::CompilationFailed(err) => write!(f, "Proto compilation failed: {}", err),
            Self::MessageNotFound {
                requested,
                available,
            } => write!(
                f,
                "Message type '{}' not found in the schema. Available messages: {:?}",
                requested, available
            ),
        }
    }
}

impl std::error::Error for SchemaCompileError {}

/// Compiles a raw protobuf schema string and validates that the requested message name exists in it.
///
/// Uses `protox` to compile the schema dynamically at runtime, converts it to
/// a `FileDescriptorSet`, builds the `DescriptorPool`, and extracts the `MessageDescriptor`.
pub fn compile_proto(
    raw_proto: &[u8],
    message_name: &str,
) -> Result<CompiledSchema, SchemaCompileError> {
    let proto_str = std::str::from_utf8(raw_proto)
        .map_err(|e| SchemaCompileError::InvalidProto(e.to_string()))?;

    let schema_id = alloc_schema_id();
    let temp_filename = format!("target/temp_schema_{}.proto", schema_id);
    let temp_path = std::path::Path::new(&temp_filename);

    if let Some(parent) = temp_path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }

    std::fs::write(temp_path, proto_str).map_err(|e| {
        SchemaCompileError::CompilationFailed(format!(
            "Failed to write temporary proto file: {}",
            e
        ))
    })?;

    let file_descriptor_set_res = protox::compile([temp_path], ["target", "."]);

    let _ = std::fs::remove_file(temp_path);

    let file_descriptor_set = file_descriptor_set_res
        .map_err(|e| SchemaCompileError::CompilationFailed(e.to_string()))?;

    let mut descriptor_set_bytes = Vec::new();
    file_descriptor_set
        .encode(&mut descriptor_set_bytes)
        .map_err(|e| SchemaCompileError::InvalidProto(e.to_string()))?;

    let pool = DescriptorPool::decode(descriptor_set_bytes.as_slice())
        .map_err(|e| SchemaCompileError::InvalidProto(e.to_string()))?;

    let message_descriptor = pool.get_message_by_name(message_name).ok_or_else(|| {
        let available = pool
            .all_messages()
            .map(|m| m.full_name().to_string())
            .collect::<Vec<_>>();
        SchemaCompileError::MessageNotFound {
            requested: message_name.to_string(),
            available,
        }
    })?;

    Ok(CompiledSchema {
        id: schema_id,
        format: SchemaFormat::Protobuf,
        raw: raw_proto.to_vec(),
        descriptor_set_bytes,
        pool,
        message_descriptor,
    })
}

/// Reconstructs a CompiledSchema from already compiled FileDescriptorSet bytes.
/// Useful during WAL recovery to avoid recompiling via protox.
pub fn reconstruct_schema(
    schema_id: u64,
    raw_proto: Vec<u8>,
    descriptor_set_bytes: Vec<u8>,
    message_name: &str,
) -> Result<CompiledSchema, SchemaCompileError> {
    let pool = DescriptorPool::decode(descriptor_set_bytes.as_slice())
        .map_err(|e| SchemaCompileError::InvalidProto(e.to_string()))?;

    let message_descriptor = pool.get_message_by_name(message_name).ok_or_else(|| {
        let available = pool
            .all_messages()
            .map(|m| m.full_name().to_string())
            .collect::<Vec<_>>();
        SchemaCompileError::MessageNotFound {
            requested: message_name.to_string(),
            available,
        }
    })?;

    Ok(CompiledSchema {
        id: schema_id,
        format: SchemaFormat::Protobuf,
        raw: raw_proto,
        descriptor_set_bytes,
        pool,
        message_descriptor,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const SIMPLE_PROTO: &str = r#"
        syntax = "proto3";
        package mypackage;

        message UserCreated {
            string name = 1;
            int32 age = 2;
        }

        message UserDeleted {
            string id = 1;
        }
    "#;

    #[test]
    fn test_alloc_schema_id() {
        let id1 = alloc_schema_id();
        let id2 = alloc_schema_id();
        assert!(id2 > id1);
    }

    #[test]
    fn test_compile_valid_proto() {
        let compiled = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        assert_eq!(compiled.format, SchemaFormat::Protobuf);
        assert_eq!(
            compiled.message_descriptor.full_name(),
            "mypackage.UserCreated"
        );
        assert!(!compiled.descriptor_set_bytes.is_empty());
    }

    #[test]
    fn test_reconstruct_schema() {
        let compiled = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.UserCreated").unwrap();
        let reconstructed = reconstruct_schema(
            compiled.id,
            compiled.raw.clone(),
            compiled.descriptor_set_bytes.clone(),
            "mypackage.UserCreated",
        )
        .unwrap();
        assert_eq!(reconstructed.id, compiled.id);
        assert_eq!(reconstructed.raw, compiled.raw);
        assert_eq!(
            reconstructed.descriptor_set_bytes,
            compiled.descriptor_set_bytes
        );
        assert_eq!(
            reconstructed.message_descriptor.full_name(),
            "mypackage.UserCreated"
        );
    }

    #[test]
    fn test_compile_missing_message() {
        let res = compile_proto(SIMPLE_PROTO.as_bytes(), "mypackage.NonExistent");
        assert!(res.is_err());
        match res.unwrap_err() {
            SchemaCompileError::MessageNotFound {
                requested,
                available,
            } => {
                assert_eq!(requested, "mypackage.NonExistent");
                assert!(available.contains(&"mypackage.UserCreated".to_string()));
                assert!(available.contains(&"mypackage.UserDeleted".to_string()));
            }
            _ => panic!("Expected MessageNotFound error"),
        }
    }

    #[test]
    fn test_compile_invalid_syntax() {
        let malformed = "syntax = proto3; message X {}";
        let res = compile_proto(malformed.as_bytes(), "X");
        assert!(res.is_err());
        match res.unwrap_err() {
            SchemaCompileError::CompilationFailed(err) => {
                assert!(!err.is_empty());
            }
            _ => panic!("Expected CompilationFailed error"),
        }
    }
}
