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
// File: properties.rs
// Description: AMQP Content Header properties serialization and structures.

//! AMQP 0-9-1 Basic content header properties.
//!
//! The 14 standard properties are transmitted in content header frames
//! using a property-flags bitmap followed by present values in order.

use std::io::{self, Read, Write};

use crate::core::types::*;

/// Represents the schema or state for basic properties.
///
/// Defines details for basic properties inside the broker ecosystem.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BasicProperties {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub headers: Option<FieldTable>,
    pub delivery_mode: Option<u8>,
    pub priority: Option<u8>,
    pub correlation_id: Option<String>,
    pub reply_to: Option<String>,
    pub expiration: Option<String>,
    pub message_id: Option<String>,
    pub timestamp: Option<u64>,
    pub type_field: Option<String>,
    pub user_id: Option<String>,
    pub app_id: Option<String>,
    pub cluster_id: Option<String>,
}

impl BasicProperties {
    /// Executes the standard flags lifecycle step.
    ///
    /// Executes the required business logic for flags.
    ///
    /// # Returns
    ///
    /// * `u16` - The evaluated outcome or operation handle.
    pub fn flags(&self) -> u16 {
        let mut f: u16 = 0;
        if self.content_type.is_some() {
            f |= 1 << 15;
        }
        if self.content_encoding.is_some() {
            f |= 1 << 14;
        }
        if self.headers.is_some() {
            f |= 1 << 13;
        }
        if self.delivery_mode.is_some() {
            f |= 1 << 12;
        }
        if self.priority.is_some() {
            f |= 1 << 11;
        }
        if self.correlation_id.is_some() {
            f |= 1 << 10;
        }
        if self.reply_to.is_some() {
            f |= 1 << 9;
        }
        if self.expiration.is_some() {
            f |= 1 << 8;
        }
        if self.message_id.is_some() {
            f |= 1 << 7;
        }
        if self.timestamp.is_some() {
            f |= 1 << 6;
        }
        if self.type_field.is_some() {
            f |= 1 << 5;
        }
        if self.user_id.is_some() {
            f |= 1 << 4;
        }
        if self.app_id.is_some() {
            f |= 1 << 3;
        }
        if self.cluster_id.is_some() {
            f |= 1 << 2;
        }
        f
    }

    /// Executes the standard encode lifecycle step.
    ///
    /// Executes the required business logic for encode.
    ///
    /// # Arguments
    ///
    /// * `w` - `&mut impl Write`: The `w` argument.
    ///
    /// # Returns
    ///
    /// * `io::Result<()>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn encode(&self, w: &mut impl Write) -> io::Result<()> {
        write_short(w, self.flags())?;

        if let Some(ref v) = self.content_type {
            write_shortstr(w, v)?;
        }
        if let Some(ref v) = self.content_encoding {
            write_shortstr(w, v)?;
        }
        if let Some(ref v) = self.headers {
            write_field_table(w, v)?;
        }
        if let Some(v) = self.delivery_mode {
            write_octet(w, v)?;
        }
        if let Some(v) = self.priority {
            write_octet(w, v)?;
        }
        if let Some(ref v) = self.correlation_id {
            write_shortstr(w, v)?;
        }
        if let Some(ref v) = self.reply_to {
            write_shortstr(w, v)?;
        }
        if let Some(ref v) = self.expiration {
            write_shortstr(w, v)?;
        }
        if let Some(ref v) = self.message_id {
            write_shortstr(w, v)?;
        }
        if let Some(v) = self.timestamp {
            write_timestamp(w, v)?;
        }
        if let Some(ref v) = self.type_field {
            write_shortstr(w, v)?;
        }
        if let Some(ref v) = self.user_id {
            write_shortstr(w, v)?;
        }
        if let Some(ref v) = self.app_id {
            write_shortstr(w, v)?;
        }
        if let Some(ref v) = self.cluster_id {
            write_shortstr(w, v)?;
        }
        Ok(())
    }

    /// Executes the standard decode lifecycle step.
    ///
    /// Executes the required business logic for decode.
    ///
    /// # Arguments
    ///
    /// * `r` - `&mut impl Read`: The `r` argument.
    ///
    /// # Returns
    ///
    /// * `io::Result<Self>` - A standard rust Result wrapping the status payloads or server failure codes.
    pub fn decode(r: &mut impl Read) -> io::Result<Self> {
        let flags = read_short(r)?;
        let mut p = Self::default();

        if flags & (1 << 15) != 0 {
            p.content_type = Some(read_shortstr(r)?);
        }
        if flags & (1 << 14) != 0 {
            p.content_encoding = Some(read_shortstr(r)?);
        }
        if flags & (1 << 13) != 0 {
            p.headers = Some(read_field_table(r)?);
        }
        if flags & (1 << 12) != 0 {
            p.delivery_mode = Some(read_octet(r)?);
        }
        if flags & (1 << 11) != 0 {
            p.priority = Some(read_octet(r)?);
        }
        if flags & (1 << 10) != 0 {
            p.correlation_id = Some(read_shortstr(r)?);
        }
        if flags & (1 << 9) != 0 {
            p.reply_to = Some(read_shortstr(r)?);
        }
        if flags & (1 << 8) != 0 {
            p.expiration = Some(read_shortstr(r)?);
        }
        if flags & (1 << 7) != 0 {
            p.message_id = Some(read_shortstr(r)?);
        }
        if flags & (1 << 6) != 0 {
            p.timestamp = Some(read_timestamp(r)?);
        }
        if flags & (1 << 5) != 0 {
            p.type_field = Some(read_shortstr(r)?);
        }
        if flags & (1 << 4) != 0 {
            p.user_id = Some(read_shortstr(r)?);
        }
        if flags & (1 << 3) != 0 {
            p.app_id = Some(read_shortstr(r)?);
        }
        if flags & (1 << 2) != 0 {
            p.cluster_id = Some(read_shortstr(r)?);
        }
        Ok(p)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    /// Executes the standard empty properties roundtrip lifecycle step.
    ///
    /// Executes the required business logic for empty properties roundtrip.
    #[test]
    fn empty_properties_roundtrip() {
        let p = BasicProperties::default();
        assert_eq!(p.flags(), 0);
        let mut buf = Vec::new();
        p.encode(&mut buf).unwrap();
        let decoded = BasicProperties::decode(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(decoded, p);
    }

    /// Executes the standard all properties roundtrip lifecycle step.
    ///
    /// Executes the required business logic for all properties roundtrip.
    #[test]
    fn all_properties_roundtrip() {
        let mut headers = FieldTable::new();
        headers.insert("x-custom".into(), FieldValue::LongString(b"val".to_vec()));

        let p = BasicProperties {
            content_type: Some("application/json".into()),
            content_encoding: Some("utf-8".into()),
            headers: Some(headers),
            delivery_mode: Some(2),
            priority: Some(5),
            correlation_id: Some("corr-123".into()),
            reply_to: Some("reply.queue".into()),
            expiration: Some("60000".into()),
            message_id: Some("msg-456".into()),
            timestamp: Some(1700000000),
            type_field: Some("order.created".into()),
            user_id: Some("guest".into()),
            app_id: Some("myapp".into()),
            cluster_id: Some("cluster1".into()),
        };
        assert_eq!(p.flags(), 0xFFFC); // all 14 flags set (bits 15..2)

        let mut buf = Vec::new();
        p.encode(&mut buf).unwrap();
        let decoded = BasicProperties::decode(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(decoded, p);
    }

    /// Executes the standard partial properties roundtrip lifecycle step.
    ///
    /// Executes the required business logic for partial properties roundtrip.
    #[test]
    fn partial_properties_roundtrip() {
        let p = BasicProperties {
            delivery_mode: Some(1),
            priority: Some(0),
            reply_to: Some("q1".into()),
            ..Default::default()
        };
        let expected_flags = (1 << 12) | (1 << 11) | (1 << 9);
        assert_eq!(p.flags(), expected_flags);

        let mut buf = Vec::new();
        p.encode(&mut buf).unwrap();
        let decoded = BasicProperties::decode(&mut Cursor::new(&buf)).unwrap();
        assert_eq!(decoded, p);
    }

    /// Executes the standard flags bitmap individual lifecycle step.
    ///
    /// Executes the required business logic for flags bitmap individual.
    #[test]
    fn flags_bitmap_individual() {
        let mut p = BasicProperties::default();
        p.content_type = Some("text/plain".into());
        assert_eq!(p.flags(), 1 << 15);

        p.content_type = None;
        p.timestamp = Some(0);
        assert_eq!(p.flags(), 1 << 6);
    }
}