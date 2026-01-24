use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::env;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use bb8_postgres::bb8::Pool;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::NoTls;

use base64::engine::general_purpose;

use crate::cache_tokens::CacheTokenService;
use crate::media_tokens::MediaTokenService;
use crate::rate_limiter::RateLimiter;
use crate::s3_throttle::S3Throttle;
use aws_sdk_s3::Client as S3Client;

#[derive(Clone, Debug)]
pub struct BulkOperationInfo {
    pub deck_hash: String,
    pub note_guids: Vec<String>,
    pub created_at: DateTime<Utc>,
    pub note_insertion_done: bool,
}

pub type BulkOperationCache = Arc<RwLock<HashMap<Uuid, BulkOperationInfo>>>;

pub struct AppState {
    pub db_pool: Arc<Pool<PostgresConnectionManager<NoTls>>>,
    pub base64_engine: Arc<general_purpose::GeneralPurpose>,
    pub s3_client: S3Client,
    pub rate_limiter: RateLimiter,
    pub bulk_operations: BulkOperationCache,
    pub media_token_service: Arc<MediaTokenService>,
    pub media_base_url: Arc<String>,
    pub media_token_replay_cache: Arc<Mutex<HashMap<String, (i64, u8)>>>,
    pub cache_token_service: Arc<CacheTokenService>,
    pub cache_base_url: Arc<String>,
    pub cache_token_replay_cache: Arc<Mutex<HashMap<String, (i64, u8)>>>,
    pub s3_throttle: Arc<S3Throttle>,
}

pub type SharedConn =
    bb8_postgres::bb8::PooledConnection<'static, PostgresConnectionManager<tokio_postgres::NoTls>>;

pub async fn establish_pool_connection() -> Result<
    Pool<PostgresConnectionManager<NoTls>>,
    Box<dyn std::error::Error + Send + Sync + 'static>,
> {
    let conn_manager = PostgresConnectionManager::new_from_stringlike(
        env::var("DATABASE_URL").expect("Expected DATABASE_URL to exist in the environment"),
        NoTls,
    )
    .unwrap();

    let pool = Pool::builder()
        .min_idle(Some(1))
        .max_size(15)
        .build(conn_manager)
        .await?;
    Ok(pool)
}
