use std::time::Duration;

use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::Delete;
use crate::s3_throttle::S3Throttle;
use bytes::Bytes;
use sentry::Level;
use tokio::time::sleep;

use crate::database::AppState;

fn parse_retry_delay<E>(err: &SdkError<E>) -> Option<Duration> {
    let resp = err.raw_response()?;
    let headers = resp.headers();

    // Prefer standard header if present.
    // Retry-After can be either seconds or an HTTP-date; we only support seconds.
    if let Some(v) = headers.get("retry-after") {
        let s = v.to_string();
        if let Ok(secs) = s.trim().parse::<u64>() {
            return Some(Duration::from_secs(secs));
        }
    }

    // Ceph RGW often provides these.
    for key in ["ratelimit-reset", "x-ratelimit-reset", "x-rate-limit-reset"] {
        if let Some(v) = headers.get(key) {
            let s = v.to_string();
            // Common formats are integer seconds ("1") or fractional seconds.
            let trimmed = s.trim();
            if let Ok(secs) = trimmed.parse::<u64>() {
                return Some(Duration::from_secs(secs));
            }
            if let Ok(secs_f) = trimmed.parse::<f64>() {
                if secs_f.is_finite() && secs_f >= 0.0 {
                    return Some(Duration::from_secs_f64(secs_f));
                }
            }
        }
    }

    None
}

#[derive(Debug, Clone)]
pub enum S3OpError {
    TooManyRequests,
    NotFound,
    Forbidden,
    Other(String, Option<u16>),
}

const MAX_RETRIES: usize = 3;
const BASE_BACKOFF_MS: u64 = 120; // quicker recovery while still backing off on 429

async fn call_with_retry<T, E, F, Fut>(state: &AppState, make_call: F) -> Result<T, S3OpError>
where
    E: Send + Sync + std::fmt::Debug + 'static,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, SdkError<E>>>,
{
    call_with_retry_with_throttle(&*state.s3_throttle, make_call).await
}

async fn call_with_retry_with_throttle<T, E, F, Fut>(
    s3_throttle: &S3Throttle,
    make_call: F,
) -> Result<T, S3OpError>
where
    E: Send + Sync + std::fmt::Debug + 'static,
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T, SdkError<E>>>,
{
    let mut attempt = 0;
    loop {
        let _slot = s3_throttle.acquire().await;
        match make_call().await {
            Ok(res) => return Ok(res),
            Err(err) => {
                let status = err.raw_response().map(|r| r.status().as_u16());
                match status {
                    // Treat common transient 5xx as retryable (bounded).
                    Some(429 | 500 | 502 | 503 | 504) if attempt < MAX_RETRIES => {
                        attempt += 1;
                        let exp_backoff = Duration::from_millis(
                            BASE_BACKOFF_MS * 2u64.saturating_pow(attempt as u32),
                        );
                        let header_delay = parse_retry_delay(&err);
                        let delay = match header_delay {
                            Some(h) if h > exp_backoff => h,
                            _ => exp_backoff,
                        };
                        sleep(delay).await;
                        continue;
                    }
                    Some(404) => return Err(S3OpError::NotFound),
                    Some(403) => return Err(S3OpError::Forbidden),
                    Some(429) => return Err(S3OpError::TooManyRequests),
                    _ => return Err(S3OpError::Other(format!("{err:?}"), status)),
                }
            }
        }
    }
}

fn log_s3_error(op: &str, bucket: &str, key_or_prefix: Option<&str>, err: &S3OpError) {
    // Skip logging NotFound for get_object - it's expected behavior (checking if file exists)
    if op == "get_object" && matches!(err, S3OpError::NotFound) {
        return;
    }

    let location = key_or_prefix
        .map(|k| format!("{bucket}/{k}"))
        .unwrap_or_else(|| bucket.to_string());
    let breadcrumb_message = format!("s3_ops {op} failed for {location}: {err:?}");

    // Capture messages without the specific key so Sentry groups them together.
    let grouped_message = match err {
        S3OpError::TooManyRequests => format!("s3_ops {op} TooManyRequests"),
        S3OpError::NotFound => format!("s3_ops {op} NotFound"),
        S3OpError::Forbidden => format!("s3_ops {op} Forbidden"),
        S3OpError::Other(_, Some(status)) => format!("s3_ops {op} status {status}"),
        S3OpError::Other(_, None) => format!("s3_ops {op} Other"),
    };

    sentry::add_breadcrumb(sentry::Breadcrumb {
        category: Some("s3_ops".into()),
        message: Some(breadcrumb_message.clone()),
        level: Level::Warning,
        ..Default::default()
    });

    match err {
        S3OpError::Other(_, _) | S3OpError::Forbidden => {
            sentry::capture_message(&grouped_message, Level::Error);
        }
        S3OpError::TooManyRequests => {
            sentry::capture_message(&grouped_message, Level::Warning);
        }
        S3OpError::NotFound => {
            // NotFound is often expected; no breadcrumb or capture needed
        }
    }
}

pub async fn get_object(
    state: &AppState,
    bucket: &str,
    key: &str,
) -> Result<aws_sdk_s3::operation::get_object::GetObjectOutput, S3OpError> {
    let res = call_with_retry::<_, aws_sdk_s3::operation::get_object::GetObjectError, _, _>(state, || {
        state
            .s3_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
    })
    .await;

    if let Err(ref err) = res {
        log_s3_error("get_object", bucket, Some(key), err);
    }

    res
}

pub async fn head_object(
    state: &AppState,
    bucket: &str,
    key: &str,
) -> Result<aws_sdk_s3::operation::head_object::HeadObjectOutput, S3OpError> {
    let res = call_with_retry::<_, aws_sdk_s3::operation::head_object::HeadObjectError, _, _>(state, || {
        state
            .s3_client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
    })
    .await;

    if let Err(ref err) = res {
        log_s3_error("head_object", bucket, Some(key), err);
    }

    res
}

pub async fn delete_object(
    state: &AppState,
    bucket: &str,
    key: &str,
) -> Result<aws_sdk_s3::operation::delete_object::DeleteObjectOutput, S3OpError> {
    let res = call_with_retry::<_, aws_sdk_s3::operation::delete_object::DeleteObjectError, _, _>(
        state,
        || state.s3_client.delete_object().bucket(bucket).key(key).send(),
    )
    .await;

    if let Err(ref err) = res {
        log_s3_error("delete_object", bucket, Some(key), err);
    }

    res
}

pub async fn delete_objects(
    state: &AppState,
    bucket: &str,
    delete: Delete,
) -> Result<aws_sdk_s3::operation::delete_objects::DeleteObjectsOutput, S3OpError> {
    let res = call_with_retry::<_, aws_sdk_s3::operation::delete_objects::DeleteObjectsError, _, _>(
        state,
        || {
            state
                .s3_client
                .delete_objects()
                .bucket(bucket)
                .delete(delete.clone())
                .send()
        },
    )
    .await;

    if let Err(ref err) = res {
        log_s3_error("delete_objects", bucket, None, err);
    }

    res
}

pub async fn list_objects_v2(
    state: &AppState,
    bucket: &str,
    prefix: Option<String>,
    continuation_token: Option<String>,
    delimiter: Option<String>,
) -> Result<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output, S3OpError> {
    let res = call_with_retry::<_, aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error, _, _>(
        state,
        || {
            let mut builder = state.s3_client.list_objects_v2().bucket(bucket);
            if let Some(p) = prefix.clone() {
                builder = builder.prefix(p);
            }
            if let Some(ct) = continuation_token.clone() {
                builder = builder.continuation_token(ct);
            }
            if let Some(d) = delimiter.clone() {
                builder = builder.delimiter(d);
            }
            builder.send()
        },
    )
    .await;

    if let Err(ref err) = res {
        log_s3_error("list_objects_v2", bucket, prefix.as_deref(), err);
    }

    res
}

pub async fn list_objects_v2_with_client(
    s3_client: &aws_sdk_s3::Client,
    s3_throttle: &S3Throttle,
    bucket: &str,
    prefix: Option<String>,
    continuation_token: Option<String>,
    delimiter: Option<String>,
) -> Result<aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output, S3OpError> {
    let res = call_with_retry_with_throttle::<_, aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Error, _, _>(
        s3_throttle,
        || {
            let mut builder = s3_client.list_objects_v2().bucket(bucket);
            if let Some(p) = prefix.clone() {
                builder = builder.prefix(p);
            }
            if let Some(ct) = continuation_token.clone() {
                builder = builder.continuation_token(ct);
            }
            if let Some(d) = delimiter.clone() {
                builder = builder.delimiter(d);
            }
            builder.send()
        },
    )
    .await;

    if let Err(ref err) = res {
        log_s3_error("list_objects_v2", bucket, prefix.as_deref(), err);
    }

    res
}

pub async fn put_object(
    state: &AppState,
    bucket: &str,
    key: &str,
    content_length: i64,
    content_type: String,
    content_md5: String,
    body: Bytes,
) -> Result<aws_sdk_s3::operation::put_object::PutObjectOutput, S3OpError> {
    let res = call_with_retry::<_, aws_sdk_s3::operation::put_object::PutObjectError, _, _>(state, || {
        state
            .s3_client
            .put_object()
            .bucket(bucket)
            .key(key)
            .content_length(content_length)
            .content_type(content_type.clone())
            .content_md5(content_md5.clone())
            .body(ByteStream::from(body.clone()))
            .send()
    })
    .await;

    if let Err(ref err) = res {
        log_s3_error("put_object", bucket, Some(key), err);
    }

    res
}
