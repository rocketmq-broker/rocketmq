use crate::core::error::{Error, Result};

pub const MAGIC: [u8; 2] = [82, 81]; // "RQ"
pub const HEADER_SIZE: usize = 14;
pub const VERSION: u8 = 0x01;
pub const MAX_BODY: usize = 1024 * 1024;

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event {
    Nop = 0x00,
    AssertQueue = 0x01,
    AssertQueueOk = 0x02,
    Listen = 0x03,
    ListenOk = 0x04,
    Publish = 0x05,
    Deliver = 0x06,
    Ack = 0x07,
    Nack = 0x08,
    Heartbeat = 0x09,

    // Exchange operations
    DeclareExchange = 0x10,
    DeclareExchangeOk = 0x11,
    DeleteExchange = 0x12,
    DeleteExchangeOk = 0x13,
    Bind = 0x14,
    BindOk = 0x15,
    Unbind = 0x16,
    UnbindOk = 0x17,

    // Channel operations
    ChannelOpen = 0x20,
    ChannelOpenOk = 0x21,
    ChannelClose = 0x22,
    ChannelCloseOk = 0x23,

    // QoS & confirms
    Qos = 0x28,
    QosOk = 0x29,
    ConfirmSelect = 0x2A,
    ConfirmSelectOk = 0x2B,
    PublishAck = 0x2C,
    PublishNack = 0x2D,
}

impl TryFrom<u8> for Event {
    type Error = Error;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            0x00 => Ok(Self::Nop),
            0x01 => Ok(Self::AssertQueue),
            0x02 => Ok(Self::AssertQueueOk),
            0x03 => Ok(Self::Listen),
            0x04 => Ok(Self::ListenOk),
            0x05 => Ok(Self::Publish),
            0x06 => Ok(Self::Deliver),
            0x07 => Ok(Self::Ack),
            0x08 => Ok(Self::Nack),
            0x09 => Ok(Self::Heartbeat),
            0x10 => Ok(Self::DeclareExchange),
            0x11 => Ok(Self::DeclareExchangeOk),
            0x12 => Ok(Self::DeleteExchange),
            0x13 => Ok(Self::DeleteExchangeOk),
            0x14 => Ok(Self::Bind),
            0x15 => Ok(Self::BindOk),
            0x16 => Ok(Self::Unbind),
            0x17 => Ok(Self::UnbindOk),
            0x20 => Ok(Self::ChannelOpen),
            0x21 => Ok(Self::ChannelOpenOk),
            0x22 => Ok(Self::ChannelClose),
            0x23 => Ok(Self::ChannelCloseOk),
            0x28 => Ok(Self::Qos),
            0x29 => Ok(Self::QosOk),
            0x2A => Ok(Self::ConfirmSelect),
            0x2B => Ok(Self::ConfirmSelectOk),
            0x2C => Ok(Self::PublishAck),
            0x2D => Ok(Self::PublishNack),
            _ => Err(Error::BadPayload),
        }
    }
}

#[derive(Debug)]
pub struct Header {
    pub event: Event,
    pub channel_id: u16,
    pub bodylen: u32,
    pub bodyoff: u32,
}

impl Header {
    pub fn new(event: Event, bodylen: u32, bodyoff: u32) -> Self {
        Self {
            event,
            channel_id: 0,
            bodylen,
            bodyoff,
        }
    }

    pub fn with_channel(event: Event, channel_id: u16, bodylen: u32, bodyoff: u32) -> Self {
        Self {
            event,
            channel_id,
            bodylen,
            bodyoff,
        }
    }

    pub fn from_bytes(buf: &[u8; HEADER_SIZE]) -> Result<Self> {
        let mut p = Parser::new(buf);
        p.expect_magic()?;
        let _version = p.read_u8()?;
        let channel_id = p.read_u16()?;
        let event = Event::try_from(p.read_u8()?)?;
        let bodylen = p.read_u32()?;
        let bodyoff = p.read_u32()?;
        Ok(Self {
            event,
            channel_id,
            bodylen,
            bodyoff,
        })
    }

    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut buf = [0u8; HEADER_SIZE];
        buf[0..2].copy_from_slice(&MAGIC);
        buf[2] = VERSION;
        buf[3..5].copy_from_slice(&self.channel_id.to_be_bytes());
        buf[5] = self.event as u8;
        buf[6..10].copy_from_slice(&self.bodylen.to_be_bytes());
        buf[10..14].copy_from_slice(&self.bodyoff.to_be_bytes());
        buf
    }
}

pub struct Frame {
    pub header: Header,
    pub payload: Vec<u8>,
}

impl Frame {
    pub fn empty(event: Event) -> Self {
        Self {
            header: Header::new(event, 0, 0),
            payload: Vec::new(),
        }
    }

    pub fn with_body(event: Event, body: Vec<u8>) -> Self {
        Self {
            header: Header::new(event, body.len() as u32, 0),
            payload: body,
        }
    }

    pub fn with_deliver(msg_id: u64, user_headers: &[u8], body: &[u8]) -> Self {
        use std::io::Write;
        let mut prefix_buf = [0u8; 32];
        let mut cursor = &mut prefix_buf[..];
        write!(cursor, "id:{}\r\n", msg_id).unwrap();
        let written = 32 - cursor.len();
        let id_str = &prefix_buf[..written];

        let bodyoff = id_str.len() as u32 + user_headers.len() as u32;
        let mut payload = Vec::with_capacity(bodyoff as usize + body.len());
        payload.extend_from_slice(id_str);
        payload.extend_from_slice(user_headers);
        payload.extend_from_slice(body);

        Self {
            header: Header::new(Event::Deliver, payload.len() as u32, bodyoff),
            payload,
        }
    }
}

pub struct Parser<'b> {
    buffer: &'b [u8],
    offset: usize,
}

impl<'b> Parser<'b> {
    pub fn new(buffer: &'b [u8]) -> Self {
        Self { buffer, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.buffer.len() - self.offset
    }

    pub fn read_n<const N: usize>(&mut self) -> Result<[u8; N]> {
        if self.remaining() < N {
            return Err(Error::BadPayload);
        }
        let bytes: [u8; N] = self.buffer[self.offset..self.offset + N]
            .try_into()
            .map_err(|_| Error::BadPayload)?;
        self.offset += N;
        Ok(bytes)
    }

    pub fn read_u8(&mut self) -> Result<u8> {
        Ok(self.read_n::<1>()?[0])
    }

    pub fn read_u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.read_n::<2>()?))
    }

    pub fn read_u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.read_n::<4>()?))
    }

    fn expect_magic(&mut self) -> Result<()> {
        let magic = self.read_n::<2>()?;
        if magic != MAGIC {
            return Err(Error::BadPayload);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── Header roundtrip ────────────────────────────────────────────────

    #[test]
    fn header_roundtrip_default_channel() {
        let h = Header::new(Event::Publish, 42, 10);
        let bytes = h.to_bytes();
        let parsed = Header::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.event, Event::Publish);
        assert_eq!(parsed.channel_id, 0);
        assert_eq!(parsed.bodylen, 42);
        assert_eq!(parsed.bodyoff, 10);
    }

    #[test]
    fn header_roundtrip_with_channel() {
        let h = Header::with_channel(Event::Deliver, 7, 128, 16);
        let bytes = h.to_bytes();
        let parsed = Header::from_bytes(&bytes).unwrap();
        assert_eq!(parsed.event, Event::Deliver);
        assert_eq!(parsed.channel_id, 7);
        assert_eq!(parsed.bodylen, 128);
        assert_eq!(parsed.bodyoff, 16);
    }

    #[test]
    fn header_size_is_14() {
        assert_eq!(HEADER_SIZE, 14);
        let h = Header::new(Event::Nop, 0, 0);
        assert_eq!(h.to_bytes().len(), 14);
    }

    #[test]
    fn header_magic_bytes() {
        let h = Header::new(Event::Nop, 0, 0);
        let bytes = h.to_bytes();
        assert_eq!(bytes[0], b'R');
        assert_eq!(bytes[1], b'Q');
        assert_eq!(bytes[2], VERSION);
    }

    #[test]
    fn header_bad_magic_rejected() {
        let mut bytes = Header::new(Event::Nop, 0, 0).to_bytes();
        bytes[0] = 0xFF;
        assert!(Header::from_bytes(&bytes).is_err());
    }

    // ── Event TryFrom ───────────────────────────────────────────────────

    #[test]
    fn event_all_known_values() {
        let known: Vec<(u8, Event)> = vec![
            (0x00, Event::Nop),
            (0x01, Event::AssertQueue),
            (0x02, Event::AssertQueueOk),
            (0x03, Event::Listen),
            (0x04, Event::ListenOk),
            (0x05, Event::Publish),
            (0x06, Event::Deliver),
            (0x07, Event::Ack),
            (0x08, Event::Nack),
            (0x09, Event::Heartbeat),
            (0x10, Event::DeclareExchange),
            (0x11, Event::DeclareExchangeOk),
            (0x12, Event::DeleteExchange),
            (0x13, Event::DeleteExchangeOk),
            (0x14, Event::Bind),
            (0x15, Event::BindOk),
            (0x16, Event::Unbind),
            (0x17, Event::UnbindOk),
            (0x20, Event::ChannelOpen),
            (0x21, Event::ChannelOpenOk),
            (0x22, Event::ChannelClose),
            (0x23, Event::ChannelCloseOk),
            (0x28, Event::Qos),
            (0x29, Event::QosOk),
            (0x2A, Event::ConfirmSelect),
            (0x2B, Event::ConfirmSelectOk),
            (0x2C, Event::PublishAck),
            (0x2D, Event::PublishNack),
        ];

        for (byte, expected) in known {
            let event = Event::try_from(byte).unwrap();
            assert_eq!(event, expected, "mismatch for 0x{:02X}", byte);
        }
    }

    #[test]
    fn event_unknown_byte_errors() {
        assert!(Event::try_from(0xFF).is_err());
        assert!(Event::try_from(0x0A).is_err());
        assert!(Event::try_from(0x50).is_err());
    }

    // ── Parser ──────────────────────────────────────────────────────────

    #[test]
    fn parser_read_u8() {
        let buf = [0x42];
        let mut p = Parser::new(&buf);
        assert_eq!(p.read_u8().unwrap(), 0x42);
    }

    #[test]
    fn parser_read_u16_big_endian() {
        let buf = [0x01, 0x00]; // 256 in BE
        let mut p = Parser::new(&buf);
        assert_eq!(p.read_u16().unwrap(), 256);
    }

    #[test]
    fn parser_read_u32_big_endian() {
        let buf = [0x00, 0x01, 0x00, 0x00]; // 65536 in BE
        let mut p = Parser::new(&buf);
        assert_eq!(p.read_u32().unwrap(), 65536);
    }

    #[test]
    fn parser_sequential_reads() {
        let buf = [0xAA, 0x00, 0x0B, 0x00, 0x00, 0x00, 0x2A];
        let mut p = Parser::new(&buf);
        assert_eq!(p.read_u8().unwrap(), 0xAA);
        assert_eq!(p.read_u16().unwrap(), 11);
        assert_eq!(p.read_u32().unwrap(), 42);
    }

    #[test]
    fn parser_overflow_errors() {
        let buf = [0x01];
        let mut p = Parser::new(&buf);
        assert!(p.read_u16().is_err());

        let empty: [u8; 0] = [];
        let mut p = Parser::new(&empty);
        assert!(p.read_u8().is_err());
    }

    #[test]
    fn parser_magic_validation() {
        let good = [b'R', b'Q'];
        let mut p = Parser::new(&good);
        assert!(p.expect_magic().is_ok());

        let bad = [b'X', b'Y'];
        let mut p = Parser::new(&bad);
        assert!(p.expect_magic().is_err());
    }

    // ── Frame constructors ──────────────────────────────────────────────

    #[test]
    fn frame_empty() {
        let f = Frame::empty(Event::Heartbeat);
        assert_eq!(f.header.event, Event::Heartbeat);
        assert_eq!(f.header.bodylen, 0);
        assert_eq!(f.header.bodyoff, 0);
        assert!(f.payload.is_empty());
    }

    #[test]
    fn frame_with_body() {
        let body = b"hello".to_vec();
        let f = Frame::with_body(Event::AssertQueueOk, body);
        assert_eq!(f.header.event, Event::AssertQueueOk);
        assert_eq!(f.header.bodylen, 5);
        assert_eq!(f.header.bodyoff, 0);
        assert_eq!(&f.payload, b"hello");
    }

    #[test]
    fn frame_with_deliver_assembly() {
        let headers = b"trace:abc\r\n";
        let body = b"payload";
        let f = Frame::with_deliver(42, headers, body);

        assert_eq!(f.header.event, Event::Deliver);

        // Check that the id header is at the start
        let payload_str = std::str::from_utf8(&f.payload).unwrap();
        assert!(payload_str.starts_with("id:42\r\n"));
        assert!(payload_str.contains("trace:abc\r\n"));

        // Body should be at bodyoff
        let body_part = &f.payload[f.header.bodyoff as usize..];
        assert_eq!(body_part, b"payload");
    }

    #[test]
    fn frame_deliver_bodyoff_correct() {
        let headers = b"";
        let body = b"data";
        let f = Frame::with_deliver(1, headers, body);

        // bodyoff should be the length of "id:1\r\n" = 5
        let id_header = format!("id:1\r\n");
        assert_eq!(f.header.bodyoff as usize, id_header.len());
    }
}
