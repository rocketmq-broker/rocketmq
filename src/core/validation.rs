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
// File: validation.rs
// Description: AMQP naming and state constraint validations.

//! AMQP 0-9-1 frame and protocol validation.
//!
//! Enforces wire-level constraints from the AMQP spec:
//! - Channel 0 reserved for Connection class only
//! - frame_max enforcement
//! - Valid frame types
//! - Channel number within negotiated range

use crate::core::amqp_codec::*;
use crate::core::method::*;

/// Executes the standard validate channel lifecycle step.
///
/// Executes the required business logic for validate channel.
///
/// # Arguments
///
/// * `channel` - `u16`: The `channel` argument.
/// * `class_id` - `u16`: The `class_id` argument.
///
/// # Returns
///
/// * `Option<&'static str>` - The evaluated outcome or operation handle.
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

/// Executes the standard validate frame type lifecycle step.
///
/// Executes the required business logic for validate frame type.
///
/// # Arguments
///
/// * `frame_type` - `u8`: The `frame_type` argument.
///
/// # Returns
///
/// * `Option<&'static str>` - The evaluated outcome or operation handle.
pub fn validate_frame_type(frame_type: u8) -> Option<&'static str> {
    match frame_type {
        FRAME_METHOD | FRAME_HEADER | FRAME_BODY | FRAME_HEARTBEAT => None,
        _ => Some("unknown frame type"),
    }
}

/// Executes the standard validate frame size lifecycle step.
///
/// Executes the required business logic for validate frame size.
///
/// # Arguments
///
/// * `payload_len` - `usize`: The `payload_len` argument.
/// * `frame_max` - `u32`: The `frame_max` argument.
///
/// # Returns
///
/// * `Option<&'static str>` - The evaluated outcome or operation handle.
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

/// Executes the standard validate channel number lifecycle step.
///
/// Executes the required business logic for validate channel number.
///
/// # Arguments
///
/// * `channel` - `u16`: The `channel` argument.
/// * `channel_max` - `u16`: The `channel_max` argument.
///
/// # Returns
///
/// * `Option<&'static str>` - The evaluated outcome or operation handle.
pub fn validate_channel_number(channel: u16, channel_max: u16) -> Option<&'static str> {
    if channel_max == 0 {
        return None; // 0 means unlimited
    }
    if channel > channel_max {
        return Some("channel number exceeds negotiated channel-max");
    }
    None
}

/// Executes the standard validate heartbeat lifecycle step.
///
/// Executes the required business logic for validate heartbeat.
///
/// # Arguments
///
/// * `channel` - `u16`: The `channel` argument.
/// * `payload_len` - `usize`: The `payload_len` argument.
///
/// # Returns
///
/// * `Option<&'static str>` - The evaluated outcome or operation handle.
pub fn validate_heartbeat(channel: u16, payload_len: usize) -> Option<&'static str> {
    if channel != 0 {
        return Some("heartbeat on non-zero channel");
    }
    if payload_len != 0 {
        return Some("heartbeat with non-empty payload");
    }
    None
}

/// Executes the standard validate content channel lifecycle step.
///
/// Executes the required business logic for validate content channel.
///
/// # Arguments
///
/// * `channel` - `u16`: The `channel` argument.
///
/// # Returns
///
/// * `Option<&'static str>` - The evaluated outcome or operation handle.
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

    /// Executes the standard connection class must be channel zero lifecycle step.
    ///
    /// Executes the required business logic for connection class must be channel zero.
    #[test]
    fn connection_class_must_be_channel_zero() {
        assert!(validate_channel(1, CLASS_CONNECTION).is_some());
        assert!(validate_channel(0, CLASS_CONNECTION).is_none());
    }

    /// Executes the standard non connection must not be channel zero lifecycle step.
    ///
    /// Executes the required business logic for non connection must not be channel zero.
    #[test]
    fn non_connection_must_not_be_channel_zero() {
        assert!(validate_channel(0, CLASS_CHANNEL).is_some());
        assert!(validate_channel(0, CLASS_EXCHANGE).is_some());
        assert!(validate_channel(0, CLASS_QUEUE).is_some());
        assert!(validate_channel(0, CLASS_BASIC).is_some());
        assert!(validate_channel(0, CLASS_TX).is_some());
    }

    /// Executes the standard non connection on valid channel lifecycle step.
    ///
    /// Executes the required business logic for non connection on valid channel.
    #[test]
    fn non_connection_on_valid_channel() {
        assert!(validate_channel(1, CLASS_CHANNEL).is_none());
        assert!(validate_channel(5, CLASS_EXCHANGE).is_none());
        assert!(validate_channel(2047, CLASS_BASIC).is_none());
    }

    // ── Frame type validation ─────────────────────────

    /// Executes the standard valid frame types lifecycle step.
    ///
    /// Executes the required business logic for valid frame types.
    #[test]
    fn valid_frame_types() {
        assert!(validate_frame_type(FRAME_METHOD).is_none());
        assert!(validate_frame_type(FRAME_HEADER).is_none());
        assert!(validate_frame_type(FRAME_BODY).is_none());
        assert!(validate_frame_type(FRAME_HEARTBEAT).is_none());
    }

    /// Executes the standard invalid frame types lifecycle step.
    ///
    /// Executes the required business logic for invalid frame types.
    #[test]
    fn invalid_frame_types() {
        assert!(validate_frame_type(0).is_some());
        assert!(validate_frame_type(4).is_some());
        assert!(validate_frame_type(7).is_some());
        assert!(validate_frame_type(9).is_some());
        assert!(validate_frame_type(255).is_some());
    }

    // ── Frame size validation ─────────────────────────

    /// Executes the standard frame size within limit lifecycle step.
    ///
    /// Executes the required business logic for frame size within limit.
    #[test]
    fn frame_size_within_limit() {
        assert!(validate_frame_size(100, 131072).is_none());
        assert!(validate_frame_size(131064, 131072).is_none()); // exactly at limit
    }

    /// Executes the standard frame size exceeds limit lifecycle step.
    ///
    /// Executes the required business logic for frame size exceeds limit.
    #[test]
    fn frame_size_exceeds_limit() {
        assert!(validate_frame_size(131065, 131072).is_some()); // 131065 + 8 > 131072
    }

    /// Executes the standard frame size unlimited lifecycle step.
    ///
    /// Executes the required business logic for frame size unlimited.
    #[test]
    fn frame_size_unlimited() {
        assert!(validate_frame_size(1_000_000, 0).is_none());
    }

    // ── Channel number validation ─────────────────────

    /// Executes the standard channel number within limit lifecycle step.
    ///
    /// Executes the required business logic for channel number within limit.
    #[test]
    fn channel_number_within_limit() {
        assert!(validate_channel_number(1, 2047).is_none());
        assert!(validate_channel_number(2047, 2047).is_none());
    }

    /// Executes the standard channel number exceeds limit lifecycle step.
    ///
    /// Executes the required business logic for channel number exceeds limit.
    #[test]
    fn channel_number_exceeds_limit() {
        assert!(validate_channel_number(2048, 2047).is_some());
    }

    /// Executes the standard channel number unlimited lifecycle step.
    ///
    /// Executes the required business logic for channel number unlimited.
    #[test]
    fn channel_number_unlimited() {
        assert!(validate_channel_number(65535, 0).is_none());
    }

    // ── Heartbeat validation ──────────────────────────

    /// Executes the standard heartbeat valid lifecycle step.
    ///
    /// Executes the required business logic for heartbeat valid.
    #[test]
    fn heartbeat_valid() {
        assert!(validate_heartbeat(0, 0).is_none());
    }

    /// Executes the standard heartbeat wrong channel lifecycle step.
    ///
    /// Executes the required business logic for heartbeat wrong channel.
    #[test]
    fn heartbeat_wrong_channel() {
        assert!(validate_heartbeat(1, 0).is_some());
    }

    /// Executes the standard heartbeat non empty lifecycle step.
    ///
    /// Executes the required business logic for heartbeat non empty.
    #[test]
    fn heartbeat_non_empty() {
        assert!(validate_heartbeat(0, 5).is_some());
    }

    // ── Content channel validation ────────────────────

    /// Executes the standard content on channel zero invalid lifecycle step.
    ///
    /// Executes the required business logic for content on channel zero invalid.
    #[test]
    fn content_on_channel_zero_invalid() {
        assert!(validate_content_channel(0).is_some());
    }

    /// Executes the standard content on non zero valid lifecycle step.
    ///
    /// Executes the required business logic for content on non zero valid.
    #[test]
    fn content_on_non_zero_valid() {
        assert!(validate_content_channel(1).is_none());
        assert!(validate_content_channel(100).is_none());
    }
}
