# Sprint 1 — Core Schema Module

> Build the foundational schema compilation and validation engine.
> No integration with the broker yet — pure library code with full unit test coverage.

---

## Goal

A standalone `src/schema/` module that can:

1. Accept raw `.proto` text
2. Compile it into a `DescriptorPool` + `MessageDescriptor` using `protox`
3. Validate an arbitrary `&[u8]` body against that descriptor
4. Return structured errors on failure

---

## Deliverables

### 1.1 — Add Dependencies to `Cargo.toml`

```toml
prost = "0.13"
prost-types = "0.13"
prost-reflect = "0.14"
protox = "0.7"
bytes = "1"
```

**Acceptance criteria:** `cargo check` passes with new dependencies.

---

### 1.2 — Create `src/schema/mod.rs`

**Types:**

```rust
use std::sync::atomic::{AtomicU64, Ordering};

static SCHEMA_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub enum SchemaFormat {
    Protobuf,
}

pub struct CompiledSchema {
    pub id: u64,
    pub format: SchemaFormat,
    pub raw: Vec<u8>,
    pub descriptor_set_bytes: Vec<u8>,
    pub pool: prost_reflect::DescriptorPool,
    pub message_descriptor: prost_reflect::MessageDescriptor,
}
```

**Compilation function:**

```rust
pub fn compile_proto(
    raw_proto: &[u8],
    message_name: &str,
) -> Result<CompiledSchema, SchemaCompileError>
```

Implementation flow:
1. Write raw `.proto` bytes to a virtual file for `protox`
2. Call `protox::compile(...)` → `FileDescriptorSet`
3. Encode `FileDescriptorSet` to bytes → `descriptor_set_bytes`
4. Build `DescriptorPool::decode(descriptor_set_bytes)`
5. Look up `pool.get_message_by_name(message_name)` → `MessageDescriptor`
6. If message not found → return `SchemaCompileError::MessageNotFound`
7. Return `CompiledSchema` with all fields populated

**Error type:**

```rust
#[derive(Debug)]
pub enum SchemaCompileError {
    CompilationFailed(String),
    MessageNotFound { requested: String, available: Vec<String> },
    InvalidProto(String),
}
```

**Registration:** Add `pub mod schema;` to `src/main.rs`.

---

### 1.3 — Create `src/schema/validate.rs`

**Validation function:**

```rust
pub fn validate_message(
    schema: &CompiledSchema,
    body: &[u8],
) -> Result<(), SchemaValidationError>
```

Implementation:
1. Call `DynamicMessage::decode(schema.message_descriptor.clone(), body)`
2. `prost-reflect`'s `DynamicMessage::decode` consumes the full buffer
   and returns `DecodeError` on malformed input or trailing bytes
3. On success → `Ok(())`
4. On decode error → `Err(SchemaValidationError::DecodeFailed(...))`

**Error type:**

```rust
#[derive(Debug)]
pub enum SchemaValidationError {
    DecodeFailed(String),
    WrongContentType { expected: String, got: String },
}

impl std::fmt::Display for SchemaValidationError { ... }
```

**Content-type check helper:**

```rust
pub fn is_protobuf_content(content_type: &Option<String>) -> bool {
    content_type.as_ref().is_some_and(|ct| ct.contains("protobuf"))
}
```

---

### 1.4 — Unit Tests

All tests in `src/schema/mod.rs` and `src/schema/validate.rs`:

| Test | Description |
|---|---|
| `compile_valid_proto` | Compile a simple `.proto` with one message, verify `MessageDescriptor` is obtained |
| `compile_multi_message_proto` | `.proto` with multiple messages, select correct root by name |
| `compile_missing_message` | Request non-existent message name → `MessageNotFound` with available names |
| `compile_invalid_syntax` | Malformed `.proto` → `CompilationFailed` |
| `compile_with_package` | `.proto` with `package mypackage;` → message name must be `mypackage.MyMessage` |
| `validate_valid_body` | Encode a `DynamicMessage`, validate the bytes → `Ok(())` |
| `validate_invalid_body` | Random garbage bytes → `DecodeFailed` |
| `validate_empty_body` | Empty `&[]` → should succeed (all-optional proto3 message) |
| `validate_wrong_message_type` | Body encoded as message A, validated against message B → error |
| `is_protobuf_content_variations` | Test `application/protobuf`, `application/x-protobuf`, `application/vnd.foo+protobuf`, `None`, `application/json` |

**Acceptance criteria:** `cargo test --lib schema` — all tests pass.

---

## Files Changed

| File | Action |
|---|---|
| `Cargo.toml` | Modify — add 5 dependencies |
| `src/main.rs` | Modify — add `pub mod schema;` |
| `src/schema/mod.rs` | Create — types, `compile_proto()`, error types |
| `src/schema/validate.rs` | Create — `validate_message()`, `is_protobuf_content()` |

---

## Risks & Mitigations

| Risk | Mitigation |
|---|---|
| `protox` API may differ from documented examples | Pin exact version `0.7`, verify API with `cargo doc --open` before coding |
| `protox` requires file system for imports | Use `protox::Compiler` with in-memory file system (virtual files) — no temp files |
| `prost-reflect` descriptor pool decode may fail on edge cases | Test with proto2 vs proto3, nested messages, enums, imports |
| Compilation time increase from new deps | Acceptable — these are compile-time only costs, runtime is fast |

---

## Definition of Done

- [ ] `cargo check` passes
- [ ] `cargo test --lib schema` — all 10+ tests pass
- [ ] `cargo clippy` — no warnings in `src/schema/`
- [ ] No integration with broker handlers yet (pure library)
- [ ] Code has license headers matching project convention
