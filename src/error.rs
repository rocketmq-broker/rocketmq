use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("bad payload")]
    BadPayload,

    #[error("disconnected")]
    Disconnected,

    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    #[error("channel closed")]
    ChannelClosed,
}

pub type Result<T> = std::result::Result<T, Error>;
