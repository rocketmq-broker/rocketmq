//! AMQP 0-9-1 frame and protocol validation.
//!
//! Enforces wire-level constraints from the AMQP spec:
//! - Channel 0 reserved for Connection class only
//! - frame_max enforcement
//! - Valid frame types
//! - Channel number within negotiated range

use crate::core::amqp_codec::*;
use crate::core::method::*;

/// Validate that a method is legal on the given channel.
/// Returns an error string if invalid, None if OK.
pub fn validate_channel(channel: u16, class_id: u16) -> Option<&'static str> {
    // Connection class MUST be on channel 0
    if class_id == CLASS_CONNECTION && channel != 0 {
        return Some("connection method on non-zero channel");
    }
    // Non-connection class MUST NOT be on channel 0
    if class_id != CLASS_CONNECTION && channel == 0 {
        return Some("non-connection method on channel 0");
    }
    None
}

/// Validate frame type is known.
pub fn validate_frame_type(frame_type: u8) -> Option<&'static str> {
    match frame_type {
        FRAME_METHOD | FRAME_HEADER | FRAME_BODY | FRAME_HEARTBEAT => None,
        _ => Some("unknown frame type"),
    }
}

/// Validate payload size against negotiated frame_max.
/// frame_max includes the 8-byte overhead (7 header + 1 frame-end).
pub fn validate_frame_size(payload_len: usize, frame_max: u32) -> Option<&'static str> {
    if frame_max == 0 {
        return None; // 0 means unlimited
    }
    let total = payload_len + 8;
    if total > frame_max as usize {
        return Some("frame exceeds negotiated frame-max");
    }
    None
}

/// Validate channel number against negotiated channel_max.
pub fn validate_channel_number(channel: u16, channel_max: u16) -> Option<&'static str> {
    if channel_max == 0 {
        return None; // 0 means unlimited
    }
    if channel > channel_max {
        return Some("channel number exceeds negotiated channel-max");
    }
    None
}

/// Validate that a heartbeat frame is on channel 0 and has empty payload.
pub fn validate_heartbeat(channel: u16, payload_len: usize) -> Option<&'static str> {
    if channel != 0 {
        return Some("heartbeat on non-zero channel");
    }
    if payload_len != 0 {
        return Some("heartbeat with non-empty payload");
    }
    None
}

/// Validate that content frames (HEADER, BODY) arrive on non-zero channel.
pub fn validate_content_channel(channel: u16) -> Option<&'static str> {
    if channel == 0 {
        return Some("content frame on channel 0");
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Channel validation ────────────────────────────

    #[test]
    fn connection_class_must_be_channel_zero() {
        assert!(validate_channel(1, CLASS_CONNECTION).is_some());
        assert!(validate_channel(0, CLASS_CONNECTION).is_none());
    }

    #[test]
    fn non_connection_must_not_be_channel_zero() {
        assert!(validate_channel(0, CLASS_CHANNEL).is_some());
        assert!(validate_channel(0, CLASS_EXCHANGE).is_some());
        assert!(validate_channel(0, CLASS_QUEUE).is_some());
        assert!(validate_channel(0, CLASS_BASIC).is_some());
        assert!(validate_channel(0, CLASS_TX).is_some());
    }

    #[test]
    fn non_connection_on_valid_channel() {
        assert!(validate_channel(1, CLASS_CHANNEL).is_none());
        assert!(validate_channel(5, CLASS_EXCHANGE).is_none());
        assert!(validate_channel(2047, CLASS_BASIC).is_none());
    }

    // ── Frame type validation ─────────────────────────

    #[test]
    fn valid_frame_types() {
        assert!(validate_frame_type(FRAME_METHOD).is_none());
        assert!(validate_frame_type(FRAME_HEADER).is_none());
        assert!(validate_frame_type(FRAME_BODY).is_none());
        assert!(validate_frame_type(FRAME_HEARTBEAT).is_none());
    }

    #[test]
    fn invalid_frame_types() {
        assert!(validate_frame_type(0).is_some());
        assert!(validate_frame_type(4).is_some());
        assert!(validate_frame_type(7).is_some());
        assert!(validate_frame_type(9).is_some());
        assert!(validate_frame_type(255).is_some());
    }

    // ── Frame size validation ─────────────────────────

    #[test]
    fn frame_size_within_limit() {
        assert!(validate_frame_size(100, 131072).is_none());
        assert!(validate_frame_size(131064, 131072).is_none()); // exactly at limit
    }

    #[test]
    fn frame_size_exceeds_limit() {
        assert!(validate_frame_size(131065, 131072).is_some()); // 131065 + 8 > 131072
    }

    #[test]
    fn frame_size_unlimited() {
        assert!(validate_frame_size(1_000_000, 0).is_none());
    }

    // ── Channel number validation ─────────────────────

    #[test]
    fn channel_number_within_limit() {
        assert!(validate_channel_number(1, 2047).is_none());
        assert!(validate_channel_number(2047, 2047).is_none());
    }

    #[test]
    fn channel_number_exceeds_limit() {
        assert!(validate_channel_number(2048, 2047).is_some());
    }

    #[test]
    fn channel_number_unlimited() {
        assert!(validate_channel_number(65535, 0).is_none());
    }

    // ── Heartbeat validation ──────────────────────────

    #[test]
    fn heartbeat_valid() {
        assert!(validate_heartbeat(0, 0).is_none());
    }

    #[test]
    fn heartbeat_wrong_channel() {
        assert!(validate_heartbeat(1, 0).is_some());
    }

    #[test]
    fn heartbeat_non_empty() {
        assert!(validate_heartbeat(0, 5).is_some());
    }

    // ── Content channel validation ────────────────────

    #[test]
    fn content_on_channel_zero_invalid() {
        assert!(validate_content_channel(0).is_some());
    }

    #[test]
    fn content_on_non_zero_valid() {
        assert!(validate_content_channel(1).is_none());
        assert!(validate_content_channel(100).is_none());
    }
}
