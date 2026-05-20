pub mod amqp_connection;
pub mod amqp_delivery;
pub mod amqp_loop;
pub mod handler;
pub mod tasks;
pub mod tls;

use tokio::io::{BufWriter, WriteHalf};

/// Unified writer type for AMQP handlers — works with both plain TCP and TLS streams.
/// All handlers use this instead of hardcoded `crate::server::AmqpWriter`.
pub type AmqpWriter = BufWriter<WriteHalf<Box<dyn crate::server::AsyncStream>>>;

/// Trait alias for streams that can be used as AMQP transports.
pub trait AsyncStream: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send {}
impl<T: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send> AsyncStream for T {}
