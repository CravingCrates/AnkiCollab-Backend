use std::collections::hash_map::Entry;
use std::sync::Arc;

use axum::{
    body::Body,
    extract::{Query, State},
    http::StatusCode,
    response::Response,
    routing::get,
    Router,
};
use chrono::Utc;
use serde::Deserialize;
use tokio_util::io::ReaderStream;

use crate::cache_manager::deck_cache_bucket;
use crate::cache_tokens::CacheTokenError;
use crate::database::AppState;
use crate::s3_ops::{self, S3OpError};

const CACHE_TOKEN_MAX_REUSES: u8 = 3;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new().route("/v1/cache/object", get(download_cache_object))
}

async fn download_cache_object(
    State(state): State<Arc<AppState>>,
    Query(query): Query<CacheQuery>,
) -> Result<Response, (StatusCode, String)> {
    let claims = state
        .cache_token_service
        .verify_token(&query.token)
        .map_err(|err| match err {
            CacheTokenError::Expired => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            _ => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
        })?;

    register_token_use(&state, &query.token, claims.exp, CACHE_TOKEN_MAX_REUSES).await?;

    let object = s3_ops::get_object(&state, deck_cache_bucket(), &claims.s3_key)
        .await
        .map_err(|err| match err {
            S3OpError::NotFound => (StatusCode::NOT_FOUND, "Not found".to_string()),
            S3OpError::TooManyRequests => (
                StatusCode::TOO_MANY_REQUESTS,
                "Storage busy, please retry".to_string(),
            ),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "Internal error".to_string()),
        })?;

    let content_type = claims
        .content_type
        .unwrap_or_else(|| infer_content_type(&claims.s3_key).to_string());

    let body = Body::from_stream(ReaderStream::new(object.body.into_async_read()));

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", content_type)
        .header("Cache-Control", "public, max-age=3600")
        .header("Access-Control-Allow-Origin", "*")
        .body(body)
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error".to_string(),
            )
        })
}

fn infer_content_type(key: &str) -> &'static str {
    if key.ends_with(".json") {
        "application/json"
    } else if key.ends_with(".zip") {
        "application/zip"
    } else if key.ends_with(".gz") {
        "application/gzip"
    } else {
        "application/octet-stream"
    }
}

async fn register_token_use(
    state: &Arc<AppState>,
    token: &str,
    exp: i64,
    max_uses: u8,
) -> Result<(), (StatusCode, String)> {
    let mut cache = state.cache_token_replay_cache.lock().await;
    let now = Utc::now().timestamp();

    cache.retain(|_, (expiry, _)| *expiry >= now);

    match cache.entry(token.to_string()) {
        Entry::Occupied(mut entry) => {
            let (entry_expiry, uses) = entry.get_mut();
            if *entry_expiry < now {
                *entry_expiry = exp;
                *uses = 1;
            } else if *uses >= max_uses {
                return Err((StatusCode::UNAUTHORIZED, "Unauthorized".to_string()));
            } else {
                *uses += 1;
            }
        }
        Entry::Vacant(vacant) => {
            vacant.insert((exp, 1));
        }
    }

    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CacheQuery {
    token: String,
}
