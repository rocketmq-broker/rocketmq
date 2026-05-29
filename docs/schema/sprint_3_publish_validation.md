# Sprint 3 — Publish Validation Gate

> Wire schema validation into the publish path. After this sprint,
> messages are validated against queue schemas before enqueueing,
> with atomic multi-queue semantics and correct AMQP error responses.

**Depends on:** Sprint 2 (queues must store compiled schemas).

---

## Goal

1. Validate message bodies against ALL target queue schemas before enqueueing to ANY
2. Check `content_type` before attempting protobuf decode
3. Return correct AMQP errors per publish mode (mandatory, confirms, tx)
4. Validate at `Tx.Commit` time, not during `Basic.Publish` inside a transaction
5. Hold routing snapshot across resolve → validate → enqueue

---

## Deliverables

### 3.1 — Atomic Validation in `handle_publish()`

**File:** `src/server/handler/amqp_basic.rs`

Restructure the publish flow: validate ALL queues before enqueueing to ANY.

1. Hold `exchanges.read()` lock from routing resolution through end of enqueue
2. After resolving target queues, iterate and validate each schema
3. If ANY fails → call `reject_publish()` and return immediately
4. If ALL pass → proceed with existing enqueue loop

### 3.2 — Rejection Helper + `Basic.Nack`

Add `reject_publish()` that sends `Basic.Return` for mandatory, `Basic.Nack` for confirms.

Add `send_confirm_nack()` (currently only `send_confirm_ack` exists).

### 3.3 — Content-Type Rules

| Queue Has Schema | `content_type` | Behavior |
|---|---|---|
| No | Any / None | Pass through |
| Yes | Contains `"protobuf"` | Validate |
| Yes | Missing | Reject |
| Yes | Other | Reject |

### 3.4 — Transaction Commit Validation

**File:** `src/server/handler/amqp_tx.rs`

At `Tx.Commit`: re-route each `PendingOp::Publish`, validate against current schemas.
If any fails → reject commit, clear buffer. Do NOT validate during `Basic.Publish` inside tx.

### 3.5 — Routing Snapshot Atomicity

Hold `exchanges.read()` from resolve through enqueue to prevent binding changes
between validation and enqueue.

---

## Tests

| Test | Description |
|---|---|
| `publish_valid_protobuf` | Valid body → enqueued |
| `publish_invalid_body` | Bad bytes → `Basic.Return` (mandatory) |
| `publish_invalid_confirm` | Bad bytes + confirms → `Basic.Nack` |
| `publish_no_content_type` | Missing → rejected |
| `publish_wrong_content_type` | `application/json` → rejected |
| `publish_no_schema_queue` | Non-schema queue → unchanged behavior |
| `fanout_all_pass` | All queues pass → all enqueued |
| `fanout_one_fails` | One fails → NONE enqueued |
| `tx_commit_valid` | Valid messages → commit OK |
| `tx_commit_invalid` | Invalid message → commit rejected |

---

## Files Changed

| File | Action |
|---|---|
| `src/server/handler/amqp_basic.rs` | Modify — pre-validation loop, rejection, nack |
| `src/server/handler/amqp_tx.rs` | Modify — commit-time validation |

---

## Definition of Done

- [ ] Valid protobuf → enqueued; invalid → rejected with correct AMQP error
- [ ] Atomic multi-queue semantics (all-or-nothing)
- [ ] Content-type enforced; tx validates at commit only
- [ ] Non-schema queues work exactly as before
- [ ] All 10 tests pass; `cargo clippy` clean
