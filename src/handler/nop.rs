use tracing::debug;

pub async fn handle(conn_id: u64) {
    debug!(conn_id, "nop");
}
