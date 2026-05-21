use std::time::Instant;

#[derive(Clone, Debug)]
pub struct Message {
    pub id: u64,
    pub headers: Vec<u8>,
    pub body: Vec<u8>,
    pub priority: u8,
    pub expiration: Option<Instant>,
    pub redelivered: bool,
    pub delivery_count: u32,
    pub exchange: String,
    pub routing_key: String,
}

impl Message {
    pub fn new(id: u64, headers: Vec<u8>, body: Vec<u8>) -> Self {
        Self {
            id,
            headers,
            body,
            priority: 0,
            expiration: None,
            redelivered: false,
            delivery_count: 0,
            exchange: String::new(),
            routing_key: String::new(),
        }
    }

    pub fn new_routed(
        id: u64,
        headers: Vec<u8>,
        body: Vec<u8>,
        exchange: String,
        routing_key: String,
    ) -> Self {
        Self {
            id,
            headers,
            body,
            priority: 0,
            expiration: None,
            redelivered: false,
            delivery_count: 0,
            exchange,
            routing_key,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expiration.is_some_and(|exp| Instant::now() >= exp)
    }
}

#[derive(Clone, Debug)]
pub struct MessageRef {
    pub id: u64,
    pub segment_id: u64,
    pub offset: u64,
    pub length: u32,
    pub priority: u8,
    pub expiration: Option<Instant>,
    pub redelivered: bool,
    pub delivery_count: u32,
    pub exchange: String,
    pub routing_key: String,
}

#[derive(Clone, Debug)]
pub enum QueueMessage {
    Ref(MessageRef),
    Full(Message),
}

impl QueueMessage {
    pub fn id(&self) -> u64 {
        match self {
            QueueMessage::Ref(r) => r.id,
            QueueMessage::Full(m) => m.id,
        }
    }

    pub fn priority(&self) -> u8 {
        match self {
            QueueMessage::Ref(r) => r.priority,
            QueueMessage::Full(m) => m.priority,
        }
    }

    pub fn expiration(&self) -> Option<Instant> {
        match self {
            QueueMessage::Ref(r) => r.expiration,
            QueueMessage::Full(m) => m.expiration,
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expiration().is_some_and(|exp| Instant::now() >= exp)
    }

    pub fn redelivered(&self) -> bool {
        match self {
            QueueMessage::Ref(r) => r.redelivered,
            QueueMessage::Full(m) => m.redelivered,
        }
    }

    pub fn set_redelivered(&mut self, val: bool) {
        match self {
            QueueMessage::Ref(r) => r.redelivered = val,
            QueueMessage::Full(m) => m.redelivered = val,
        }
    }

    pub fn delivery_count(&self) -> u32 {
        match self {
            QueueMessage::Ref(r) => r.delivery_count,
            QueueMessage::Full(m) => m.delivery_count,
        }
    }

    pub fn set_delivery_count(&mut self, val: u32) {
        match self {
            QueueMessage::Ref(r) => r.delivery_count = val,
            QueueMessage::Full(m) => m.delivery_count = val,
        }
    }

    /// Resolve a `QueueMessage` to a full `Message`, loading from segment if needed.
    pub fn resolve(self, wal: &crate::storage::wal::Wal) -> std::io::Result<Message> {
        match self {
            QueueMessage::Full(m) => Ok(m),
            QueueMessage::Ref(r) => {
                let (headers, body) =
                    wal.read_message_payload(r.segment_id, r.offset, r.length as usize)?;
                let mut msg = Message::new_routed(
                    r.id,
                    headers,
                    body,
                    r.exchange.clone(),
                    r.routing_key.clone(),
                );
                msg.priority = r.priority;
                msg.expiration = r.expiration;
                msg.redelivered = r.redelivered;
                msg.delivery_count = r.delivery_count;
                Ok(msg)
            }
        }
    }

    /// Unwrap the full message. Panics if it is a Ref.
    pub fn unwrap_full(self) -> Message {
        match self {
            QueueMessage::Full(m) => m,
            QueueMessage::Ref(_) => panic!("expected QueueMessage::Full"),
        }
    }
}
