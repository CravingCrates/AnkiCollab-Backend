use serde_json::Value;

use once_cell::sync::Lazy;
use std::{env, time::Duration};

use crate::cache_tokens::CacheTokenParams;
use crate::database::AppState;
use crate::s3_ops;

static DECK_CACHE_BUCKET: Lazy<String> =
    Lazy::new(|| std::env::var("S3_BUCKET_NAME").expect("S3_BUCKET_NAME must be set"));

static DECK_CACHE_PREFIX: Lazy<String> =
    Lazy::new(|| env::var("DECK_CACHE_PREFIX").unwrap_or_else(|_| "decks".to_string()));

static DECK_CACHE_MAGIC_TIMESTAMP: Lazy<String> = Lazy::new(|| {
    env::var("DECK_CACHE_MAGIC_TIMESTAMP").unwrap_or_else(|_| "2022-12-31 23:59:59".to_string())
});

static DECK_CACHE_PRESIGNED_TTL: Lazy<Duration> = Lazy::new(|| {
    env::var("DECK_CACHE_PRESIGNED_TTL_SECS")
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .map(Duration::from_secs)
        .unwrap_or_else(|| Duration::from_secs(900))
});

pub(crate) fn deck_cache_bucket() -> &'static str {
    &DECK_CACHE_BUCKET
}

fn deck_cache_prefix() -> &'static str {
    &DECK_CACHE_PREFIX
}

pub(crate) fn deck_cache_token_ttl() -> Duration {
    *DECK_CACHE_PRESIGNED_TTL
}

pub fn is_cache_bootstrap_timestamp(timestamp: &str) -> bool {
    timestamp == DECK_CACHE_MAGIC_TIMESTAMP.as_str()
}

pub fn deck_cache_pointer_key(deck_hash: &str) -> String {
    let mut key = String::new();
    let prefix = deck_cache_prefix().trim_end_matches('/');
    if !prefix.is_empty() {
        key.push_str(prefix);
        key.push('/');
    }
    key.push_str(deck_hash);
    key.push_str("/manifest/latest.json");
    key
}

fn cache_key_is_scoped_to_deck(deck_hash: &str, key: &str) -> bool {
    let normalized = key.trim_start_matches('/');
    if normalized.is_empty() || normalized.contains('\\') {
        return false;
    }

    if normalized
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return false;
    }

    let base_prefix = deck_cache_prefix().trim_matches('/');
    let expected_prefix = if base_prefix.is_empty() {
        format!("{deck_hash}/")
    } else {
        format!("{base_prefix}/{deck_hash}/")
    };

    normalized.starts_with(&expected_prefix)
}

pub async fn fetch_cache_bootstrap_response(
    state: &AppState,
    deck_hash: &str,
) -> Result<Value, String> {
    let pointer_key = deck_cache_pointer_key(deck_hash);
    let object = s3_ops::get_object(state, deck_cache_bucket(), &pointer_key)
        .await
        .map_err(|err| format!("failed to fetch cache pointer from S3: deck_hash={deck_hash}; key={pointer_key}; err={err:?}"))?;

    let body = object
        .body
        .collect()
        .await
        .map_err(|err| {
            format!(
                "failed to read cache pointer body: deck_hash={deck_hash}; key={pointer_key}; err={err}"
            )
        })?;

    let pointer: crate::structs::LatestPointer = serde_json::from_slice(&body.into_bytes())
        .map_err(|err| format!("failed to parse cache pointer JSON for {deck_hash}: {err}"))?;

    let manifest_key = pointer.manifest_key.trim_start_matches('/').to_string();
    let manifest_last_modified = pointer.version_ts.clone();

    if !cache_key_is_scoped_to_deck(deck_hash, &manifest_key) {
        return Err(format!(
            "cache pointer manifest key escaped deck scope: deck_hash={deck_hash}; key={manifest_key}"
        ));
    }

    let manifest_token = state
        .cache_token_service
        .generate_token(CacheTokenParams {
            deck_hash: deck_hash.to_string(),
            s3_key: manifest_key.clone(),
            content_type: Some("application/json".to_string()),
        })
        .map_err(|err| format!("failed to create manifest token for {deck_hash}: {err}"))?;

    let manifest_url = format!(
        "{}/v1/cache/object?token={}",
        state.cache_base_url.as_str(),
        manifest_token
    );

    let mut archive_key_from_manifest: Option<String> = None;

    match s3_ops::get_object(state, deck_cache_bucket(), &manifest_key).await {
        Ok(manifest_obj) => match manifest_obj.body.collect().await {
            Ok(body) => {
                if let Ok(manifest_json) = serde_json::from_slice::<Value>(&body.into_bytes()) {
                    if let Some(archive_value) = manifest_json.get("archive") {
                        if let Some(archive_obj) = archive_value.as_object() {
                            if let Some(raw_archive_key) =
                                archive_obj.get("s3_key").and_then(|val| val.as_str())
                            {
                                let normalized_archive_key =
                                    raw_archive_key.trim_start_matches('/').to_string();

                                if cache_key_is_scoped_to_deck(deck_hash, &normalized_archive_key)
                                {
                                    archive_key_from_manifest = Some(normalized_archive_key);
                                } else {
                                    sentry::add_breadcrumb(sentry::Breadcrumb {
                                        category: Some("cache".into()),
                                        message: Some(format!(
                                            "manifest archive key escaped deck scope: deck_hash={deck_hash}; key={raw_archive_key}"
                                        )),
                                        level: sentry::Level::Warning,
                                        ..Default::default()
                                    });
                                }
                            }
                        }
                    }
                }
            }
            Err(err) => {
                sentry::add_breadcrumb(sentry::Breadcrumb {
                    category: Some("cache".into()),
                    message: Some(format!(
                        "failed to read manifest while deriving archive key: deck_hash={deck_hash}; key={manifest_key}; err={err}"
                    )),
                    level: sentry::Level::Warning,
                    ..Default::default()
                });
            }
        },
        Err(err) => {
            sentry::add_breadcrumb(sentry::Breadcrumb {
                category: Some("cache".into()),
                message: Some(format!(
                    "failed to fetch manifest while deriving archive key: deck_hash={deck_hash}; key={manifest_key}; err={err:?}"
                )),
                level: sentry::Level::Warning,
                ..Default::default()
            });
        }
    }

    let archive_presigned_url = if let Some(key) = archive_key_from_manifest.as_ref() {
        let content_type = if key.ends_with(".zip") {
            "application/zip"
        } else if key.ends_with(".json") {
            "application/json"
        } else if key.ends_with(".gz") {
            "application/gzip"
        } else {
            "application/octet-stream"
        };

        match state.cache_token_service.generate_token(CacheTokenParams {
            deck_hash: deck_hash.to_string(),
            s3_key: key.to_string(),
            content_type: Some(content_type.to_string()),
        }) {
            Ok(token) => Some(format!(
                "{}/v1/cache/object?token={}",
                state.cache_base_url.as_str(),
                token
            )),
            Err(err) => {
                sentry::add_breadcrumb(sentry::Breadcrumb {
                    category: Some("cache".into()),
                    message: Some(format!(
                        "failed to create archive token: deck_hash={deck_hash}; key={key}; err={err}"
                    )),
                    level: sentry::Level::Warning,
                    ..Default::default()
                });
                None
            }
        }
    } else {
        None
    };

    let mut manifest = serde_json::Map::new();

    manifest.insert(
        "manifest_presigned_url".to_string(),
        serde_json::Value::String(manifest_url),
    );

    if let Some(url) = archive_presigned_url.as_ref() {
        manifest.insert(
            "archive_presigned_url".to_string(),
            serde_json::Value::String(url.clone()),
        );
    }
    manifest.insert(
        "manifest_last_modified".to_string(),
        serde_json::Value::String(manifest_last_modified),
    );

    let mut response = serde_json::Map::new();
    response.insert(
        "deck_hash".to_string(),
        serde_json::Value::String(deck_hash.to_string()),
    );
    response.insert(
        "mode".to_string(),
        serde_json::Value::String("cache-bootstrap".to_string()),
    );
    response.insert("manifest".to_string(), Value::Object(manifest));

    Ok(Value::Object(response))
}
