use std::time::Instant;

pub struct Message {
    pub id: u64,
    pub headers: Vec<u8>,
    pub body: Vec<u8>,
    pub priority: u8,
    pub expiration: Option<Instant>,
    pub redelivered: bool,
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
        }
    }

    pub fn is_expired(&self) -> bool {
        self.expiration.map_or(false, |exp| Instant::now() >= exp)
    }
}
