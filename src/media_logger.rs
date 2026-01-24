use serde::Serialize;
use std::net::IpAddr;

#[derive(Debug, Serialize)]
pub enum MediaOperationType {
    Upload,
    Download,
}

pub async fn log_media_operation(
    tx: &tokio_postgres::Transaction<'_>,
    operation_type: MediaOperationType,
    user_id: Option<i32>,
    ip_address: IpAddr,
    file_hash: Option<String>,
    file_name: Option<String>,
    file_size: Option<i64>,
) -> Result<(), tokio_postgres::Error> {
    let operation: i32 = match operation_type {
        MediaOperationType::Upload => 1,
        MediaOperationType::Download => 2,
    };

    tx.execute(
        "INSERT INTO media_operations_log 
         (operation_type, user_id, ip_address, timestamp, file_hash, file_name, file_size) 
         VALUES ($1, $2, $3, NOW(), $4, $5, $6)",
        &[
            &operation,
            &user_id,
            &ip_address.to_string(),
            &file_hash,
            &file_name,
            &file_size,
        ],
    )
    .await?;

    Ok(())
}
