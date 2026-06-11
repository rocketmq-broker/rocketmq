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
// File: method.rs
// Description: AMQP Method frame definitions and generated decoders.

//! AMQP 0-9-1 class and method ID constants.
//!
//! These correspond exactly to the AMQP 0-9-1 specification.
//! Class IDs are u16, method IDs are u16.

pub const CLASS_CONNECTION: u16 = 10;
pub const CLASS_CHANNEL: u16 = 20;
pub const CLASS_EXCHANGE: u16 = 40;
pub const CLASS_QUEUE: u16 = 50;
pub const CLASS_BASIC: u16 = 60;
pub const CLASS_CONFIRM: u16 = 85;
pub const CLASS_TX: u16 = 90;

pub const METHOD_CONNECTION_START: u16 = 10;
pub const METHOD_CONNECTION_START_OK: u16 = 11;
pub const METHOD_CONNECTION_SECURE: u16 = 20;
pub const METHOD_CONNECTION_SECURE_OK: u16 = 21;
pub const METHOD_CONNECTION_TUNE: u16 = 30;
pub const METHOD_CONNECTION_TUNE_OK: u16 = 31;
pub const METHOD_CONNECTION_OPEN: u16 = 40;
pub const METHOD_CONNECTION_OPEN_OK: u16 = 41;
pub const METHOD_CONNECTION_CLOSE: u16 = 50;
pub const METHOD_CONNECTION_CLOSE_OK: u16 = 51;

pub const METHOD_CHANNEL_OPEN: u16 = 10;
pub const METHOD_CHANNEL_OPEN_OK: u16 = 11;
pub const METHOD_CHANNEL_FLOW: u16 = 20;
pub const METHOD_CHANNEL_FLOW_OK: u16 = 21;
pub const METHOD_CHANNEL_CLOSE: u16 = 40;
pub const METHOD_CHANNEL_CLOSE_OK: u16 = 41;

pub const METHOD_EXCHANGE_DECLARE: u16 = 10;
pub const METHOD_EXCHANGE_DECLARE_OK: u16 = 11;
pub const METHOD_EXCHANGE_DELETE: u16 = 20;
pub const METHOD_EXCHANGE_DELETE_OK: u16 = 21;
pub const METHOD_EXCHANGE_BIND: u16 = 30;
pub const METHOD_EXCHANGE_BIND_OK: u16 = 31;
pub const METHOD_EXCHANGE_UNBIND: u16 = 50;
pub const METHOD_EXCHANGE_UNBIND_OK: u16 = 51;

pub const METHOD_QUEUE_DECLARE: u16 = 10;
pub const METHOD_QUEUE_DECLARE_OK: u16 = 11;
pub const METHOD_QUEUE_BIND: u16 = 20;
pub const METHOD_QUEUE_BIND_OK: u16 = 21;
pub const METHOD_QUEUE_PURGE: u16 = 30;
pub const METHOD_QUEUE_PURGE_OK: u16 = 31;
pub const METHOD_QUEUE_DELETE: u16 = 40;
pub const METHOD_QUEUE_DELETE_OK: u16 = 41;
pub const METHOD_QUEUE_UNBIND: u16 = 50;
pub const METHOD_QUEUE_UNBIND_OK: u16 = 51;

pub const METHOD_BASIC_QOS: u16 = 10;
pub const METHOD_BASIC_QOS_OK: u16 = 11;
pub const METHOD_BASIC_CONSUME: u16 = 20;
pub const METHOD_BASIC_CONSUME_OK: u16 = 21;
pub const METHOD_BASIC_CANCEL: u16 = 30;
pub const METHOD_BASIC_CANCEL_OK: u16 = 31;
pub const METHOD_BASIC_PUBLISH: u16 = 40;
pub const METHOD_BASIC_RETURN: u16 = 50;
pub const METHOD_BASIC_DELIVER: u16 = 60;
pub const METHOD_BASIC_GET: u16 = 70;
pub const METHOD_BASIC_GET_OK: u16 = 71;
pub const METHOD_BASIC_GET_EMPTY: u16 = 72;
pub const METHOD_BASIC_ACK: u16 = 80;
pub const METHOD_BASIC_REJECT: u16 = 90;
pub const METHOD_BASIC_RECOVER_ASYNC: u16 = 100;
pub const METHOD_BASIC_RECOVER: u16 = 110;
pub const METHOD_BASIC_RECOVER_OK: u16 = 111;
pub const METHOD_BASIC_NACK: u16 = 120;

pub const METHOD_CONFIRM_SELECT: u16 = 10;
pub const METHOD_CONFIRM_SELECT_OK: u16 = 11;

pub const METHOD_TX_SELECT: u16 = 10;
pub const METHOD_TX_SELECT_OK: u16 = 11;
pub const METHOD_TX_COMMIT: u16 = 20;
pub const METHOD_TX_COMMIT_OK: u16 = 21;
pub const METHOD_TX_ROLLBACK: u16 = 30;
pub const METHOD_TX_ROLLBACK_OK: u16 = 31;

pub const REPLY_SUCCESS: u16 = 200;
pub const CONTENT_TOO_LARGE: u16 = 311;
pub const NO_ROUTE: u16 = 312;
pub const NO_CONSUMERS: u16 = 313;
pub const CONNECTION_FORCED: u16 = 320;
pub const INVALID_PATH: u16 = 402;
pub const ACCESS_REFUSED: u16 = 403;
pub const NOT_FOUND: u16 = 404;
pub const RESOURCE_LOCKED: u16 = 405;
pub const PRECONDITION_FAILED: u16 = 406;
pub const FRAME_ERROR: u16 = 501;
pub const SYNTAX_ERROR: u16 = 502;
pub const COMMAND_INVALID: u16 = 503;
pub const CHANNEL_ERROR: u16 = 504;
pub const UNEXPECTED_FRAME: u16 = 505;
pub const RESOURCE_ERROR: u16 = 506;
pub const NOT_ALLOWED: u16 = 530;
pub const NOT_IMPLEMENTED: u16 = 540;
pub const INTERNAL_ERROR: u16 = 541;

pub fn is_connection_error(code: u16) -> bool {
    matches!(
        code,
        CONNECTION_FORCED
            | INVALID_PATH
            | FRAME_ERROR
            | SYNTAX_ERROR
            | COMMAND_INVALID
            | CHANNEL_ERROR
            | UNEXPECTED_FRAME
            | RESOURCE_ERROR
            | NOT_ALLOWED
            | NOT_IMPLEMENTED
            | INTERNAL_ERROR
    )
}

pub fn reply_text(code: u16) -> &'static str {
    match code {
        REPLY_SUCCESS => "REPLY-SUCCESS",
        CONTENT_TOO_LARGE => "CONTENT-TOO-LARGE",
        NO_ROUTE => "NO-ROUTE",
        NO_CONSUMERS => "NO-CONSUMERS",
        CONNECTION_FORCED => "CONNECTION-FORCED",
        INVALID_PATH => "INVALID-PATH",
        ACCESS_REFUSED => "ACCESS-REFUSED",
        NOT_FOUND => "NOT-FOUND",
        RESOURCE_LOCKED => "RESOURCE-LOCKED",
        PRECONDITION_FAILED => "PRECONDITION-FAILED",
        FRAME_ERROR => "FRAME-ERROR",
        SYNTAX_ERROR => "SYNTAX-ERROR",
        COMMAND_INVALID => "COMMAND-INVALID",
        CHANNEL_ERROR => "CHANNEL-ERROR",
        UNEXPECTED_FRAME => "UNEXPECTED-FRAME",
        RESOURCE_ERROR => "RESOURCE-ERROR",
        NOT_ALLOWED => "NOT-ALLOWED",
        NOT_IMPLEMENTED => "NOT-IMPLEMENTED",
        INTERNAL_ERROR => "INTERNAL-ERROR",
        _ => "UNKNOWN",
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn connection_errors_classified() {
        assert!(is_connection_error(FRAME_ERROR));
        assert!(is_connection_error(COMMAND_INVALID));
        assert!(is_connection_error(NOT_ALLOWED));
        assert!(!is_connection_error(NOT_FOUND));
        assert!(!is_connection_error(ACCESS_REFUSED));
        assert!(!is_connection_error(REPLY_SUCCESS));
    }

    #[test]
    fn reply_text_known() {
        assert_eq!(reply_text(NOT_FOUND), "NOT-FOUND");
        assert_eq!(reply_text(FRAME_ERROR), "FRAME-ERROR");
        assert_eq!(reply_text(REPLY_SUCCESS), "REPLY-SUCCESS");
    }

    #[test]
    fn reply_text_unknown() {
        assert_eq!(reply_text(999), "UNKNOWN");
    }

    #[test]
    fn class_ids_correct() {
        assert_eq!(CLASS_CONNECTION, 10);
        assert_eq!(CLASS_CHANNEL, 20);
        assert_eq!(CLASS_EXCHANGE, 40);
        assert_eq!(CLASS_QUEUE, 50);
        assert_eq!(CLASS_BASIC, 60);
        assert_eq!(CLASS_TX, 90);
    }

    #[test]
    fn method_ids_connection() {
        assert_eq!(METHOD_CONNECTION_START, 10);
        assert_eq!(METHOD_CONNECTION_TUNE, 30);
        assert_eq!(METHOD_CONNECTION_OPEN, 40);
        assert_eq!(METHOD_CONNECTION_CLOSE, 50);
    }

    #[test]
    fn method_ids_basic() {
        assert_eq!(METHOD_BASIC_PUBLISH, 40);
        assert_eq!(METHOD_BASIC_DELIVER, 60);
        assert_eq!(METHOD_BASIC_ACK, 80);
        assert_eq!(METHOD_BASIC_NACK, 120);
    }
}
