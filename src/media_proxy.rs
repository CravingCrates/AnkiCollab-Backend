use std::collections::hash_map::Entry;
use std::sync::Arc;

use bytes::Bytes;
use axum::{
    body::Body,
    extract::{Path, Query, Request, State},
    http::StatusCode,
    response::Response,
    routing::get,
    Json, Router,
};
use axum_client_ip::ClientIp;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use chrono::Utc;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::warn;
use crate::media_tokens::MediaTokenError;

use crate::database::AppState;
use crate::media_manager::{determine_content_type_by_name, media_bucket, MAX_FILE_SIZE_BYTES};
use crate::s3_ops::{self, S3OpError};

const DOWNLOAD_TOKEN_MAX_REUSES: u8 = 3;
const UPLOAD_TOKEN_MAX_REUSES: u8 = 3; // Allow retries for rate limiting (client has @retry(max_tries=3))

// CORS allowed origins (matches upload configuration)
fn get_cors_origin() -> &'static str {
    "*"
}

pub fn routes() -> Router<Arc<AppState>> {
    Router::new().route(
        "/v1/media/{hash}",
        get(download_media_file).put(upload_media_file),
    )
}

// Start background task to clean up expired tokens from cache
pub async fn start_token_cleanup_task(state: Arc<AppState>) {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300)); // 5 minutes

        loop {
            interval.tick().await;

            let mut cache = state.media_token_replay_cache.lock().await;
            let now = Utc::now().timestamp();
            cache.retain(|_, (expiry, _)| *expiry >= now);
        }
    });
}

// Download media file - streams from S3 through Rust backend
pub(crate) async fn download_media_file(
    Path(hash): Path<String>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<DownloadQuery>,
    ClientIp(client_ip): ClientIp,
) -> Result<Response, (StatusCode, String)> {
    validate_hash(&hash)?;

    let claims = state
        .media_token_service
        .verify_download_token(&query.token)
        .map_err(|e| match e {
            MediaTokenError::Expired => (StatusCode::UNAUTHORIZED, "Unauthorized".to_string()),
            other => {
                let err_debug = format!("{:?}", other);
                sentry::with_scope(
                    |scope| {
                        scope.set_fingerprint(Some(["download-token-invalid"].as_ref()));
                        scope.set_extra("hash", hash.clone().into());
                        scope.set_extra("error", err_debug.clone().into());
                        scope.set_tag("operation", "download");
                    },
                    || {
                        sentry::capture_message(
                            &format!(
                                "Download token verification failed: hash={}, error={}",
                                hash, err_debug
                            ),
                            sentry::Level::Warning,
                        );
                    },
                );
                (StatusCode::UNAUTHORIZED, "Unauthorized".to_string())
            }
        })?;

    if claims.hash != hash {
        return Err((StatusCode::UNAUTHORIZED, "Unauthorized".to_string()));
    }
    let user_id = claims.user_id;

    if let Err(e) =
        register_token_use(&state, &query.token, claims.exp, DOWNLOAD_TOKEN_MAX_REUSES).await
    {
        sentry::with_scope(
            |scope| {
                scope.set_fingerprint(Some(["download-token-replay"].as_ref()));
                scope.set_extra("hash", hash.clone().into());
                scope.set_tag("operation", "download");
                scope.set_tag("security", "replay_attack");
            },
            || {
                sentry::capture_message(
                    "Download token replay detected",
                    sentry::Level::Warning,
                );
            },
        );
        return Err(e);
    }

    // Get file from S3
    let s3_key = format!("{}/{}", &hash[0..2], hash);
    let object = s3_ops::get_object(&state, media_bucket(), &s3_key)
        .await
        .map_err(|err| match err {
            S3OpError::TooManyRequests => (
                StatusCode::TOO_MANY_REQUESTS,
                "Storage rate limit exceeded. Please retry with exponential backoff.".to_string(),
            ),
            S3OpError::NotFound => (StatusCode::NOT_FOUND, "File not found".to_string()),
            _ => (StatusCode::INTERNAL_SERVER_ERROR, "Failed to retrieve file".to_string()),
        })?;

    // Track download for rate limiting
    state
        .rate_limiter
        .track_user_download(user_id, client_ip, 1)
        .await;

    // Determine content type
    let content_type = claims
        .filename
        .as_ref()
        .and_then(|f| determine_content_type_by_name(f))
        .unwrap_or_else(|| "application/octet-stream".to_string());

    // Convert ByteStream to axum Body
    let byte_stream = object.body;
    let stream = byte_stream.into_async_read();
    let body = Body::from_stream(tokio_util::io::ReaderStream::new(stream));

    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", content_type)
        .header("Cache-Control", "public, max-age=31536000, immutable")
        .header("Access-Control-Allow-Origin", get_cors_origin())
        .header("Access-Control-Allow-Methods", "GET, PUT, OPTIONS")
        .header(
            "Access-Control-Allow-Headers",
            "Content-Type, Authorization",
        )
        .body(body)
        .map_err(|err| {
            sentry::with_scope(
                |scope| {
                    scope.set_fingerprint(Some(["download-response-build-failed"].as_ref()));
                    scope.set_user(Some(sentry::User {
                        id: Some(user_id.to_string()),
                        ..Default::default()
                    }));
                    scope.set_extra("hash", hash.clone().into());
                    scope.set_extra("error", format!("{:?}", err).into());
                    scope.set_tag("operation", "download");
                },
                || {
                    sentry::capture_message(
                        &format!(
                            "Failed to build download response: hash={}, error={:?}",
                            hash, err
                        ),
                        sentry::Level::Error,
                    );
                },
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error".to_string(),
            )
        })
}

// Upload media file - streams to S3 through Rust backend
pub(crate) async fn upload_media_file(
    Path(hash): Path<String>,
    State(state): State<Arc<AppState>>,
    Query(query): Query<UploadQuery>,
    ClientIp(client_ip): ClientIp,
    request: Request<Body>,
) -> Result<Json<UploadResponse>, (StatusCode, String)> {
    validate_hash(&hash)?;

    let claims = state
        .media_token_service
        .verify_upload_token(&query.token)
        .map_err(|e| {
            sentry::with_scope(
                |scope| {
                    scope.set_fingerprint(Some(["upload-token-invalid"].as_ref()));
                    scope.set_extra("hash", hash.clone().into());
                    scope.set_extra("client_ip", client_ip.to_string().into());
                    scope.set_extra("error", format!("{:?}", e).into());
                    scope.set_tag("operation", "upload");
                },
                || {
                    sentry::capture_message(
                        &format!(
                            "Upload token verification failed: hash={}, error={:?}",
                            hash, e
                        ),
                        sentry::Level::Warning,
                    );
                },
            );
            (StatusCode::UNAUTHORIZED, "Unauthorized".to_string())
        })?;

    let token_hash = claims.hash.clone();
    if token_hash != hash {
        return Err((StatusCode::UNAUTHORIZED, "Unauthorized".to_string()));
    }

    let expected_size = claims.file_size;
    let filename = claims.filename.clone();
    let batch_id = claims.batch_id.clone();
    let user_id = claims.user_id;

    if expected_size <= 0 {
        return Err((StatusCode::BAD_REQUEST, "Invalid file size".to_string()));
    }

    let expected_size_usize: usize = expected_size.try_into().map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "Upload size exceeds platform limits".to_string(),
        )
    })?;

    if expected_size_usize > MAX_FILE_SIZE_BYTES {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "File exceeds maximum size".to_string(),
        ));
    }

    let content_type = determine_content_type_by_name(&filename)
        .unwrap_or_else(|| "application/octet-stream".to_string());

    let expected_md5_bytes = hex::decode(&hash)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid hash format".to_string()))?;
    let md5_base64 = BASE64_STANDARD.encode(expected_md5_bytes);

    let s3_key = format!("{}/{}", &hash[0..2], hash);
    let anon_filename = crate::media_manager::anonymize_filename(&filename);

    let body_stream = request.into_body().into_data_stream();
    let file_bytes = collect_with_size_limit(body_stream, expected_size_usize).await.map_err(|e| {
        sentry::with_scope(
            |scope| {
                scope.set_fingerprint(Some(["upload-body-read-failed"].as_ref()));
                scope.set_user(Some(sentry::User {
                    id: Some(user_id.to_string()),
                    ip_address: Some(client_ip.into()),
                    ..Default::default()
                }));
                scope.set_extra("hash", hash.clone().into());
                scope.set_extra("filename_anon", anon_filename.clone().into());
                scope.set_extra("file_extension", filename.rsplit('.').next().unwrap_or("").into());
                scope.set_extra("expected_size", expected_size.into());
                scope.set_extra("error", format!("{:?}", e).into());
                scope.set_tag("operation", "upload");
            },
            || {
                sentry::capture_message(
                    &format!(
                        "Failed to read upload body: hash={}, file={}, expected_size={}, error={:?}",
                        hash, anon_filename, expected_size, e
                    ),
                    sentry::Level::Error,
                );
            }
        );
        e
    })?;

    let body_bytes = Bytes::from(file_bytes);

    // Upload to S3 with shared throttle and retries
    if let Err(err) = s3_ops::put_object(
        &state,
        media_bucket(),
        &s3_key,
        expected_size as i64,
        content_type,
        md5_base64,
        body_bytes,
    )
    .await
    {
        let (status, message) = match err {
            S3OpError::TooManyRequests => (
                StatusCode::TOO_MANY_REQUESTS,
                "Storage rate limit exceeded. Please retry with exponential backoff.".to_string(),
            ),
            S3OpError::Forbidden => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Storage authentication error. Please contact support.".to_string(),
            ),
            S3OpError::NotFound => (
                StatusCode::NOT_FOUND,
                "Storage bucket not found".to_string(),
            ),
            S3OpError::Other(_, Some(500)) | S3OpError::Other(_, Some(503)) => (
                StatusCode::SERVICE_UNAVAILABLE,
                "Storage service temporarily unavailable. Please retry later.".to_string(),
            ),
            _ => (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to store media file. Please try again.".to_string(),
            ),
        };

        return Err((status, message));
    }

    // consume the token AFTER successful S3 upload - prevents token burning on S3 failures
    register_token_use(&state, &query.token, claims.exp, UPLOAD_TOKEN_MAX_REUSES)
        .await
        .map_err(|e| {
            sentry::with_scope(
                |scope| {
                    scope.set_fingerprint(Some(["upload-token-replay"].as_ref()));
                    scope.set_user(Some(sentry::User {
                        id: Some(user_id.to_string()),
                        ip_address: Some(client_ip.into()),
                        ..Default::default()
                    }));
                    scope.set_extra("hash", hash.clone().into());
                    scope.set_extra("filename_anon", anon_filename.clone().into());
                    scope.set_extra(
                        "file_extension",
                        filename.rsplit('.').next().unwrap_or("").into(),
                    );
                    scope.set_tag("operation", "upload");
                    scope.set_tag("security", "replay_attack");
                },
                || {
                    sentry::capture_message(
                        &format!(
                            "Upload token replay detected: hash={}, file={}, user_id={}",
                            hash, anon_filename, user_id
                        ),
                        sentry::Level::Warning,
                    );
                },
            );
            e
        })?;

    // NOTE: We do NOT insert into media_files here to avoid race condition with cleanup_orphaned_media
    // The media_files entry will be created in confirm_media_bulk_upload along with media_references
    // This ensures atomicity and prevents orphaned files from being cleaned up before confirmation

    // Track upload for rate limiting (after successful upload)
    state
        .rate_limiter
        .track_user_upload(user_id, client_ip, expected_size as u64, 1)
        .await;

    Ok(Json(UploadResponse {
        status: "stored",
        hash,
        media_id: 0, // Will be assigned during confirmation
        batch_id,
    }))
}

// Helper to collect upload with size limit (prevents DOS while being memory-efficient)
async fn collect_with_size_limit(
    mut stream: impl futures::Stream<Item = Result<Bytes, axum::Error>> + Unpin,
    max_size: usize,
) -> Result<Vec<u8>, (StatusCode, String)> {
    let mut collected = Vec::new();
    let mut total_bytes = 0usize;

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|err| {
            warn!(error = %err, "Failed to read upload body");
            (
                StatusCode::BAD_REQUEST,
                "Failed to read upload body".to_string(),
            )
        })?;

        total_bytes = total_bytes
            .checked_add(chunk.len())
            .ok_or((StatusCode::BAD_REQUEST, "File size overflow".to_string()))?;

        if total_bytes > max_size {
            return Err((
                StatusCode::PAYLOAD_TOO_LARGE,
                "File exceeds maximum size".to_string(),
            ));
        }

        collected.extend_from_slice(&chunk);
    }

    if total_bytes == 0 {
        return Err((StatusCode::BAD_REQUEST, "Empty upload body".to_string()));
    }

    Ok(collected)
}

fn validate_hash(hash: &str) -> Result<(), (StatusCode, String)> {
    if hash.len() != 32
        || !hash
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
    {
        return Err((StatusCode::BAD_REQUEST, "Invalid media hash".to_string()));
    }
    Ok(())
}

async fn register_token_use(
    state: &Arc<AppState>,
    token: &str,
    exp: i64,
    max_uses: u8,
) -> Result<(), (StatusCode, String)> {
    let mut cache = state.media_token_replay_cache.lock().await;
    let now = Utc::now().timestamp();

    // Clean up expired tokens
    cache.retain(|_, (expiry, _)| *expiry >= now);

    match cache.entry(token.to_string()) {
        Entry::Occupied(mut entry) => {
            let (entry_expiry, uses) = entry.get_mut();
            if *entry_expiry < now {
                // Token expired, reset
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
pub(crate) struct UploadQuery {
    token: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DownloadQuery {
    token: String,
}

#[derive(Serialize)]
pub(crate) struct UploadResponse {
    status: &'static str,
    hash: String,
    media_id: i64,
    batch_id: Option<String>,
}
