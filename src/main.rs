#![warn(clippy::all, clippy::pedantic, clippy::nursery)]
pub mod auth;
pub mod cache_manager;
pub mod cache_proxy;
pub mod cache_tokens;
pub mod changelog;
pub mod cleanser;
pub mod database;
pub mod error;
pub mod human_hash;
pub mod inheritance;
pub mod media;
pub mod media_logger;
pub mod media_manager;
pub mod media_proxy;
pub mod media_reference_manager;
pub mod media_tokens;
pub mod note_removal;
pub mod notetypes;
pub mod pull;
pub mod push;
pub mod rate_limiter;
pub mod s3_ops;
pub mod s3_throttle;
pub mod stats;
pub mod structs;
pub mod subscription;
pub mod suggestion;

use std::{collections::HashMap, env, io::Read, sync::Arc, time::Duration};

use database::AppState;

use rate_limiter::RateLimiter;
use s3_throttle::S3Throttle;
use tokio::signal;
use tokio::sync::RwLock;

use tower_http::{
    classify::ServerErrorsFailureClass,
    trace::TraceLayer,
};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post, Router},
    Json,
};
use axum_client_ip::{ClientIp, ClientIpSource};

use std::fs;
use std::net::SocketAddr;

use base64::{engine::general_purpose, Engine as _};
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;

use tower_governor::{
    governor::GovernorConfigBuilder, key_extractor::SmartIpKeyExtractor, GovernorLayer,
};

use aws_sdk_s3::Client as S3Client;
use media_tokens::MediaTokenService;

use serde_json::Value;
use uuid::Uuid;

async fn create_deck_link(
    State(db_state): State<Arc<AppState>>,
    Json(req): Json<structs::CreateDeckLinkRequest>,
) -> impl IntoResponse {
    if !validate_deck_hash(&req.subscriber_deck_hash) || !validate_deck_hash(&req.base_deck_hash) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid deck hash format".to_string(),
        );
    }

    match inheritance::create_deck_link(&db_state, req).await {
        Ok(()) => (StatusCode::OK, "Success".to_string()),
        Err(e) => (StatusCode::UNAUTHORIZED, e.to_string()),
    }
}

async fn create_note_links(
    State(db_state): State<Arc<AppState>>,
    Json(req): Json<structs::CreateNewNoteLinkRequest>,
) -> impl IntoResponse {
    if !validate_deck_hash(&req.subscriber_deck_hash) || !validate_deck_hash(&req.base_deck_hash) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid deck hash format".to_string(),
        );
    }

    match inheritance::create_note_links(&db_state, req).await {
        Ok((linked, skipped)) => {
            let payload = serde_json::json!({"linked": linked, "skipped": skipped});
            (StatusCode::OK, serde_json::to_string(&payload).unwrap())
        }
        Err(e) => (StatusCode::UNAUTHORIZED, e.to_string()),
    }
}

fn validate_deck_hash(deck_hash: &str) -> bool {
    let parts: Vec<&str> = deck_hash.split('-').collect();

    // Validate deck hash format: 3, 5, or 6 words (legacy and standard formats)
    if parts.len() != 3 && parts.len() != 5 && parts.len() != 6 {
        eprintln!(
            "Invalid deck hash format (expected 3, 5, or 6 words, got {}): {}",
            parts.len(),
            deck_hash
        );
        return false;
    }

    // Each part must be alphabetic only (lowercase for standard, mixed case for legacy)
    if !parts
        .iter()
        .all(|part| !part.is_empty() && part.chars().all(|c| c.is_ascii_alphabetic()))
    {
        eprintln!(
            "Invalid deck hash format (invalid characters): {}",
            deck_hash
        );
        return false;
    }

    true
}

fn read_cached_json(file_name: &str) -> Option<String> {
    // Validate that filename ends with .json
    if !file_name.ends_with(".json") {
        eprintln!("Invalid filename: must end with .json: {}", file_name);
        return None;
    }

    // Strip .json extension to get the deck hash
    let deck_hash = &file_name[..file_name.len() - 5]; // Remove ".json"

    if !validate_deck_hash(deck_hash) {
        return None;
    }

    let path = format!("/home/cached_files/{file_name}");
    match fs::read_to_string(path) {
        Ok(data) => Some(data),
        Err(_) => None,
    }
}

fn preview_str(s: &str, max: usize) -> String {
    let mut p = s.chars().take(max).collect::<String>();
    if s.len() > max {
        p.push_str("…");
    }
    p
}

fn decompress_data(engine: &general_purpose::GeneralPurpose, data: &str) -> Result<String, String> {
    // Standard decoding path: base64 -> gzip -> UTF-8 JSON
    // Client sends: gzip.compress(json.dumps(data).encode('utf-8')) -> base64.b64encode()

    let trimmed = data.trim();

    // Decode base64
    let bytes = engine.decode(trimmed).map_err(|e| {
        let preview = preview_str(trimmed, 64);
        sentry::add_breadcrumb(sentry::Breadcrumb {
            category: Some("decode".into()),
            message: Some(format!(
                "base64 decode failed; len={}, preview='{}', err={}",
                trimmed.len(),
                preview,
                e
            )),
            level: sentry::Level::Warning,
            ..Default::default()
        });
        format!("base64 decode error: {e}")
    })?;

    // Decompress gzip
    let mut decoder = GzDecoder::new(&bytes[..]);
    let mut decoded_data = String::new();
    decoder.read_to_string(&mut decoded_data).map_err(|e| {
        sentry::add_breadcrumb(sentry::Breadcrumb {
            category: Some("decode".into()),
            message: Some(format!(
                "gzip decompress failed; compressed_size={}, err={}",
                bytes.len(),
                e
            )),
            level: sentry::Level::Warning,
            ..Default::default()
        });
        format!("gzip decode error: {e}")
    })?;

    Ok(decoded_data)
}

// Handler to submit card suggestions; runs background work on the Tokio runtime.
pub async fn process_card(
    ClientIp(iip): ClientIp,
    State(state): State<Arc<AppState>>,
    deck: String,
) -> impl IntoResponse {
    let (tx, rx) = tokio::sync::oneshot::channel::<(StatusCode, String)>();
    let state_cloned = state.clone();
    let ip = iip.to_string();
    tokio::spawn(async move {
        let send = |pair: (StatusCode, String)| {
            let _ = tx.send(pair);
        };

        let decompressed_data = match decompress_data(&state_cloned.base64_engine, &deck) {
            Ok(s) => s,
            Err(e) => {
                // Invalid client payload is expected behavior - just breadcrumb, don't capture
                sentry::add_breadcrumb(sentry::Breadcrumb {
                    category: Some("decode".into()),
                    message: Some(format!(
                        "/submitCard decode failed: err={e}; ip={}; len={}",
                        ip,
                        deck.len(),
                    )),
                    level: sentry::Level::Info,
                    ..Default::default()
                });
                send((StatusCode::BAD_REQUEST, "Invalid data".to_string()));
                return;
            }
        };
        let info: structs::SubmitCardReq =
            match serde_json::from_str::<structs::SubmitCardReq>(&decompressed_data) {
                Ok(data) => data,
                Err(_) => {
                    send((StatusCode::BAD_REQUEST, "Invalid data".to_string()));
                    return;
                }
            };

        if !validate_deck_hash(&info.remote_deck) {
            send((
                StatusCode::BAD_REQUEST,
                "Invalid deck hash format".to_string(),
            ));
            return;
        }

        let mut anki_deck = match structs::AnkiDeck::from_json_string(&info.deck) {
            Ok(deck) => deck,
            Err(_) => {
                send((StatusCode::BAD_REQUEST, "Invalid deck data".to_string()));
                return;
            }
        };

        let committing_user: Option<i32> =
            match auth::get_user_from_token(&state_cloned, &info.token).await {
                Ok(u) => Some(u),
                Err(_) => None,
            };
        let commit_id = match suggestion::create_new_commit(
            &state_cloned,
            info.rationale,
            &info.commit_text,
            &ip,
            committing_user,
        )
        .await
        {
            Ok(val) => val,
            Err(_) => {
                send((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Error occurred. Damn!".to_string(),
                ));
                return;
            }
        };

        let access_token =
            (auth::is_valid_user_token(&state_cloned, &info.token, &info.remote_deck).await)
                .unwrap_or_default();
        let mut force_overwrite = false;
        if access_token {
            force_overwrite = info.force_overwrite;
        }

        let deck_path = match suggestion::fix_deck_name(
            &state_cloned,
            &info.deck_path,
            &info.new_name,
            &info.remote_deck,
        )
        .await
        {
            Ok(val) => val,
            Err(_) => {
                send((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Invalid Deck Name".to_string(),
                ));
                return;
            }
        };
        for deck in &mut anki_deck.children {
            suggestion::update_deck_names(deck).await;
        }

        let mut client: database::SharedConn = match state_cloned.db_pool.get_owned().await {
            Ok(pool) => pool,
            Err(err) => {
                sentry::capture_message(
                    &format!("process_card: Failed to get pool: {}", err),
                    sentry::Level::Error,
                );
                send((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    r#"{ \"status\": 0, \"message\": \"Server Error. Please notify us! (752)\" }"#
                        .to_string(),
                ));
                return;
            }
        };

        let mut notetype_cache = HashMap::new();
        if let Err(error) = suggestion::sanity_check_notetypes(
            &client,
            &mut notetype_cache,
            &info.remote_deck,
            &anki_deck,
        )
        .await
        {
            send((
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Notetype Error: {error}"),
            ));
            return;
        }

        let (deck_id, owner) = match suggestion::sanity_check(
            &client,
            &info.remote_deck,
            &deck_path,
            commit_id,
        )
        .await
        {
            Ok((deck_id, owner)) => (deck_id, owner),
            Err(_error) => {
                // Expected case - user tried to submit to non-existent deck
                return send((StatusCode::UNPROCESSABLE_ENTITY, r#"Deck not found. If this is a new subdeck, try to suggest the entire deck tree from the sideview."#.to_owned()));
            }
        };

        let deck_tree: Vec<i64> = match client
            .query(
                "
            WITH RECURSIVE up_tree AS (
                SELECT id, parent
                FROM decks
                WHERE id = $1
                UNION ALL
                SELECT d.id, d.parent
                FROM decks d
                JOIN up_tree ut ON d.id = ut.parent
            ),
            down_tree AS (
                SELECT id, parent
                FROM up_tree
                WHERE parent IS NULL
                UNION ALL
                SELECT d.id, d.parent
                FROM decks d
                JOIN down_tree dt ON d.parent = dt.id
            )
            SELECT DISTINCT id
            FROM down_tree
            ",
                &[&deck_id.unwrap()],
            )
            .await
        {
            Ok(rows) => rows.iter().map(|row| row.get::<_, i64>(0)).collect(),
            Err(e) => {
                error::AppError::db_query("process_card", format!("Deck tree query failed: {}", e))
                    .with_context("deck_hash", info.remote_deck.clone())
                    .with_context("deck_id", format!("{:?}", deck_id))
                    .report_to_sentry();
                send((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Error".to_string(),
                ));
                return;
            }
        };

        match suggestion::make(
            &mut client,
            &info.remote_deck,
            &mut notetype_cache,
            &deck_path,
            &anki_deck,
            &ip,
            commit_id,
            force_overwrite,
            deck_id,
            owner,
            &deck_tree,
        )
        .await
        {
            Ok(_res) => send((StatusCode::OK, "Success".to_string())),
            Err(err) => {
                // Note processing failures are ALWAYS bugs - they shouldn't happen
                // The only "expected" errors are user-input validation which should be caught earlier
                let err_str = err.to_string();
                
                // Only skip expected user input errors (deck hash format, auth)
                let is_user_error = err_str.contains("Invalid deck hash")
                    || err_str.contains("unauthorized")
                    || err_str.contains("permission denied");
                
                if !is_user_error {
                    // This is a bug - note processing failed unexpectedly
                    error::AppError::bug("process_card/suggestion", format!("Note processing failed: {}", err))
                        .with_context("deck_hash", info.remote_deck.clone())
                        .with_context("deck_path", deck_path.clone())
                        .with_context("commit_id", commit_id.to_string())
                        .with_context("error_type", if err_str.contains("validation") { "validation" } else if err_str.contains("duplicate") { "duplicate" } else { "unknown" })
                        .report_to_sentry();
                }
                send((StatusCode::UNPROCESSABLE_ENTITY, err.to_string()));
            }
        }
    });

    match rx.await {
        Ok(pair) => pair,
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal Error".to_string(),
        ),
    }
}
async fn post_login(
    State(db_state): State<Arc<AppState>>,
    form: axum::Form<auth::Login>,
) -> impl IntoResponse {
    match auth::login(&db_state, &form).await {
        Ok(res) => {
            let json = serde_json::to_string(&res).unwrap();
            (StatusCode::OK, json)
        }
        Err(error) => {
            // Auth failures are expected behavior, not server errors
            (StatusCode::UNAUTHORIZED, error)
        }
    }
}

pub async fn remove_token(
    State(db_state): State<Arc<AppState>>,
    Path(token): Path<String>,
) -> impl IntoResponse {
    match auth::remove_token(&db_state, &token).await {
        Ok(res) => (StatusCode::OK, res),
        Err(_error) => {
            // Token not found or already removed - expected behavior
            (StatusCode::NOT_FOUND, "Token not found".to_string())
        }
    }
}

pub async fn upload_deck_stats(
    State(state): State<Arc<AppState>>,
    deck: String,
) -> impl IntoResponse {
    let decompressed_data = match decompress_data(&state.base64_engine, &deck) {
        Ok(s) => s,
        Err(e) => {
            // Invalid client payload is expected behavior - just breadcrumb, don't capture
            sentry::add_breadcrumb(sentry::Breadcrumb {
                category: Some("decode".into()),
                message: Some(format!(
                    "/UploadDeckStats decode failed: err={e}; len={}",
                    deck.len(),
                )),
                level: sentry::Level::Info,
                ..Default::default()
            });
            return (StatusCode::BAD_REQUEST, "Invalid data".to_string());
        }
    };
    let info: structs::StatsInfo = match serde_json::from_str(&decompressed_data) {
        Ok(data) => data,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid data".to_string()),
    };

    if !validate_deck_hash(&info.deck_hash) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid deck hash format".to_string(),
        );
    }

    let db_state_clone = Arc::clone(&state);
    let deck_hash_for_error = info.deck_hash.clone();

    // Run on the Tokio runtime
    tokio::spawn(async move {
        match stats::new(&db_state_clone, info).await {
            Ok(()) => {}
            Err(err) => {
                // Stats insertion failure is usually not critical, but worth logging
                error::AppError::operational(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "upload_deck_stats",
                    "Stats upload failed",
                    format!("Stats insertion error: {}", err),
                )
                .with_context("deck_hash", deck_hash_for_error)
                .report_to_sentry();
            }
        }
    });

    (StatusCode::OK, "Thanks for sharing!".to_string())
}

pub async fn confirm_media_bulk_async(
    State(state): State<Arc<AppState>>,
    Json(req): Json<structs::MediaBulkConfirmRequest>,
) -> impl IntoResponse {
    // we handle the confirmation in a seperate thread, so the user doesn't have to wait for it to finish, lacking information on which files failed to upload, trading off speed for user experience
    let state_copy = state;
    tokio::spawn(async move {
        match media_manager::confirm_media_bulk_upload(state_copy, req).await {
            Ok(_) => {}
            Err(err) => {
                // Media confirmation failure is a bug worth investigating
                error::AppError::bug("confirm_media_bulk", format!("Media bulk confirm failed: {:?}", err))
                    .report_to_sentry();
            }
        }
    });
    (StatusCode::OK, "Thanks for sharing!".to_string())
}

pub async fn check_for_update(
    ClientIp(_iip): ClientIp,
    State(state): State<Arc<AppState>>,
    Json(input): Json<HashMap<String, structs::UpdateInfo>>,
) -> impl IntoResponse {
    let mut responses: Vec<Value> = Vec::with_capacity(input.iter().len());

    // if there is just one entry and its not a valid deck hash, return bad request
    if input.len() == 1 {
        let only_key = input.keys().next().unwrap();
        if !validate_deck_hash(only_key) {
            return (StatusCode::BAD_REQUEST, "Invalid deck key".to_string());
        }
    }

    for (deck_hash, update_info) in &input {
        if !validate_deck_hash(deck_hash) {
            continue;
        }
        if cache_manager::is_cache_bootstrap_timestamp(&update_info.timestamp) {
            match cache_manager::fetch_cache_bootstrap_response(&state, deck_hash).await {
                Ok(value) => {
                    responses.push(value);
                    continue;
                }
                Err(err) => {
                    sentry::add_breadcrumb(sentry::Breadcrumb {
                        category: Some("cache".into()),
                        message: Some(format!(
                            "cache bootstrap unavailable, falling back to legacy path: deck_hash={deck_hash}; err={err}"
                        )),
                        level: sentry::Level::Warning,
                        ..Default::default()
                    });
                    // Fall back to legacy cache file based approach before pulling live data.
                }
            }
            // Check if the result is already cached
            let file_name = format!("{deck_hash}.json");
            let json_data = read_cached_json(&file_name);

            if let Some(data) = json_data {
                match decompress_data(&state.base64_engine, &data) {
                    Ok(mut decompressed_data) => {
                        // See note: cached format is a single-entry map; strip brackets
                        if !decompressed_data.is_empty() {
                            decompressed_data.pop();
                        }
                        if !decompressed_data.is_empty() {
                            decompressed_data.remove(0);
                        }
                        match serde_json::from_str::<Value>(&decompressed_data) {
                            Ok(json) => {
                                responses.push(json);
                                continue;
                            }
                            Err(e) => {
                                sentry::add_breadcrumb(sentry::Breadcrumb {
                                    category: Some("decode".into()),
                                    message: Some(format!("/pullChanges cached JSON parse failed after decode: deck_hash={deck_hash}; err={e}")),
                                    level: sentry::Level::Warning,
                                    ..Default::default()
                                });
                                // fallthrough to live pull
                            }
                        }
                    }
                    Err(e) => {
                        // If the cached file is plain JSON, try parsing it directly
                        let mut direct = data.clone();
                        if direct.trim_start().starts_with('[')
                            || direct.trim_start().starts_with('{')
                        {
                            if !direct.is_empty() {
                                direct.pop();
                            }
                            if !direct.is_empty() {
                                direct.remove(0);
                            }
                            match serde_json::from_str::<Value>(&direct) {
                                Ok(json) => {
                                    sentry::add_breadcrumb(sentry::Breadcrumb {
                                        category: Some("decode".into()),
                                        message: Some(format!("/pullChanges used plain JSON cache fallback: deck_hash={deck_hash}")),
                                        level: sentry::Level::Info,
                                        ..Default::default()
                                    });
                                    responses.push(json);
                                    continue;
                                }
                                Err(e2) => {
                                    sentry::add_breadcrumb(sentry::Breadcrumb {
                                        category: Some("decode".into()),
                                        message: Some(format!("/pullChanges cache decode failed and JSON fallback failed: deck_hash={deck_hash}; decode_err={e}; json_err={e2}; preview='{}'", preview_str(&data, 64))),
                                        level: sentry::Level::Warning,
                                        ..Default::default()
                                    });
                                }
                            }
                        } else {
                            sentry::add_breadcrumb(sentry::Breadcrumb {
                                category: Some("decode".into()),
                                message: Some(format!("/pullChanges cache decode failed: deck_hash={deck_hash}; err={e}; preview='{}'", preview_str(&data, 64))),
                                level: sentry::Level::Warning,
                                ..Default::default()
                            });
                        }
                        // fallthrough to live pull
                    }
                }
            }
        }

        match pull::pull_changes(&state, deck_hash, &update_info.timestamp).await {
            Ok(val) => match serde_json::to_value(val) {
                Ok(json) => responses.push(json),
                Err(err) => {
                    sentry::add_breadcrumb(sentry::Breadcrumb {
                        category: Some("encode".into()),
                        message: Some(format!(
                            "failed to serialize pull_changes response: deck_hash={deck_hash}; err={err}"
                        )),
                        level: sentry::Level::Error,
                        ..Default::default()
                    });
                }
            },
            Err(error) => {
                // The pull implementation returns a generic error when there are no updates
                // or the deck doesn't exist (message: "No deck found"). Treat this as
                // a non-fatal, expected case and skip logging it to Sentry to reduce noise.
                // And note to myself, refactor it ffs this is horrible practice
                let err_str = format!("{error}");
                if err_str.contains("No deck found") {
                    // silently ignore -- no changes for this deck
                } else {
                    sentry::add_breadcrumb(sentry::Breadcrumb {
                        category: Some("pull".into()),
                        message: Some(format!(
                            "pull_changes failed: deck_hash={deck_hash}; err={error}"
                        )),
                        level: sentry::Level::Warning,
                        ..Default::default()
                    });
                }
            }
        };
    }

    let json_bytes = serde_json::to_vec(&responses).unwrap();
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&json_bytes).unwrap();
    let compressed_bytes = encoder.finish().unwrap();

    let encoded = state.base64_engine.encode(compressed_bytes);

    (StatusCode::OK, encoded)
}

/// Helper: Extract all note GUIDs recursively from a deck tree
fn extract_all_note_guids(deck: &structs::AnkiDeck) -> Vec<String> {
    let mut guids = Vec::new();

    // Add notes from current deck
    for note in &deck.notes {
        guids.push(note.guid.clone());
    }

    // Recursively add notes from children
    for child in &deck.children {
        guids.extend(extract_all_note_guids(child));
    }

    guids
}

pub async fn post_data(
    ClientIp(iip): ClientIp,
    State(state): State<Arc<AppState>>,
    deck: String,
) -> impl IntoResponse {
    let decompressed_data = match decompress_data(&state.base64_engine, &deck) {
        Ok(s) => s,
        Err(e) => {
            // Invalid client payload is expected behavior - just breadcrumb, don't capture
            sentry::add_breadcrumb(sentry::Breadcrumb {
                category: Some("decode".into()),
                message: Some(format!(
                    "/createDeck decode failed: err={e}; ip={}; len={}",
                    iip,
                    deck.len(),
                )),
                level: sentry::Level::Info,
                ..Default::default()
            });
            return (StatusCode::BAD_REQUEST, "Invalid data".to_string());
        }
    };
    let info: structs::CreateDeckReq = match serde_json::from_str(&decompressed_data) {
        Ok(data) => data,
        Err(_) => return (StatusCode::BAD_REQUEST, "Invalid data".to_string()),
    };

    let owner_id = (pull::get_id_from_username(&state, info.username).await).unwrap_or_default();

    if owner_id == 0 {
        return (
            StatusCode::OK,
            r#"{ "status": 0, "message": "Unknown username" }"#.to_string(),
        );
    }

    let anki_deck = match structs::AnkiDeck::from_json_string(&info.deck) {
        Ok(deck) => deck,
        Err(e) => {
            // Malformed client payload - expected behavior, just breadcrumb
            sentry::add_breadcrumb(sentry::Breadcrumb {
                category: Some("decode".into()),
                message: Some(format!("/createDeck AnkiDeck parse failed: {}", e)),
                level: sentry::Level::Info,
                ..Default::default()
            });
            return (
                StatusCode::BAD_REQUEST,
                "Invalid Deck. Try again or contact support.".to_string(),
            );
        }
    };
    let ip = iip.to_string();

    let commit_text = String::new();
    let commit_id =
        match suggestion::create_new_commit(&state, 1, &commit_text, &ip, Some(owner_id)).await {
            Ok(val) => val,
            Err(err) => {
                error::AppError::db_query("createDeck/commit", &err)
                    .with_context("owner_hash", error::hash_pii(&owner_id.to_string()))
                    .report_to_sentry();
                return (
                    StatusCode::OK,
                    r#"{ "status": 0, "message": "An error occurred processing your request" }"#
                        .to_string(),
                );
            }
        };

    let mut client: database::SharedConn = match state.db_pool.get_owned().await {
        Ok(pool) => pool,
        Err(err) => {
            error::AppError::db_connection("createDeck", &err)
                .with_context("owner_hash", error::hash_pii(&owner_id.to_string()))
                .report_to_sentry();
            return (
                StatusCode::OK,
                r#"{ "status": 0, "message": "Server Error. Please notify us! (752)" }"#
                    .to_string(),
            );
        }
    };

    // Create the root deck synchronously to reserve the final deck hash before background work
    let (root_deck_id, deck_status) = match push::create_root_deck(&mut client, &anki_deck, owner_id, &ip, commit_id).await {
        Ok((id, hash)) => (id, hash),
        Err(_e) => return (StatusCode::OK, r#"{ "status": 0, "message": "Deck already exists. Please submit suggestions instead." }"#.to_string()),
    };

    // Create bulk operation entry in cache for coordination
    let bulk_operation_id = Uuid::new_v4();
    let bulk_info = database::BulkOperationInfo {
        deck_hash: deck_status.clone(),
        note_guids: extract_all_note_guids(&anki_deck),
        created_at: chrono::Utc::now(),
        note_insertion_done: false,
    };
    // Insert into cache
    {
        let mut cache = state.bulk_operations.write().await;
        cache.insert(bulk_operation_id, bulk_info);
    }

    // Clone state for the background task
    let state_clone = state.clone();
    tokio::spawn(async move {
        let deck_tree: Vec<i64> = Vec::new(); // The tree is yet to be created. This is a new deck
        let mut notetype_cache = HashMap::new();

        // Acquire DB connection inside the task to keep the future Send
        let mut client2: database::SharedConn = match state_clone.db_pool.get_owned().await {
            Ok(pool) => pool,
            Err(err) => {
                println!("Error getting pool in background task: {err}");
                state_clone
                    .bulk_operations
                    .write()
                    .await
                    .remove(&bulk_operation_id);
                return;
            }
        };
        
        // Fill notes and any child decks; root is already created above
        match push::unpack_deck_data(
            &mut client2,
            &anki_deck,
            &mut notetype_cache,
            owner_id,
            &ip,
            Some(root_deck_id),
            true,
            commit_id,
            &deck_tree,
        )
        .await
        {
            Ok(_success) => {
                if let Some(info) = state_clone
                    .bulk_operations
                    .write()
                    .await
                    .get_mut(&bulk_operation_id)
                {
                    info.note_insertion_done = true;
                }
            }
            Err(err) => {
                error::AppError::bug("createDeck/unpack", format!("Deck unpacking failed: {}", err))
                    .with_context("root_deck_id", root_deck_id.to_string())
                    .with_context("owner_hash", error::hash_pii(&owner_id.to_string()))
                    .with_context("commit_id", commit_id.to_string())
                    .with_context("bulk_operation_id", bulk_operation_id.to_string())
                    .report_to_sentry();
                // Clean up the bulk operation on failure
                state_clone
                    .bulk_operations
                    .write()
                    .await
                    .remove(&bulk_operation_id);
            }
        }
    });

    let response = format!(
        r#"{{ "status": 1, "message": "{}", "bulk_operation_id": "{}" }}"#,
        deck_status, bulk_operation_id
    );
    (StatusCode::OK, response)
}

pub async fn request_removal(
    ClientIp(iip): ClientIp,
    State(db_state): State<Arc<AppState>>,
    Json(form): Json<structs::NoteRemovalReq>,
) -> impl IntoResponse {
    let info = form;

    if !validate_deck_hash(&info.remote_deck) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid deck hash format".to_string(),
        );
    }

    if info.note_guids.is_empty() {
        return (StatusCode::BAD_REQUEST, "No notes provided".to_string());
    }

    let ip = iip.to_string();

    let access_token = (auth::is_valid_user_token(&db_state, &info.token, &info.remote_deck).await)
        .unwrap_or_default();

    let mut force_overwrite = false;
    if access_token {
        force_overwrite = info.force_overwrite;
    }

    let committing_user: Option<i32> = match auth::get_user_from_token(&db_state, &info.token).await
    {
        Ok(user) => Some(user),
        Err(_error) => None,
    };

    match note_removal::new(
        &db_state,
        info.note_guids,
        info.commit_text,
        info.remote_deck.clone(),
        ip,
        force_overwrite,
        committing_user,
    )
    .await
    {
        Ok(_res) => (StatusCode::OK, "Success".to_string()),
        Err(err) => {
            // This is a real bug - note removal should work if inputs are valid
            error::AppError::bug("request_removal", format!("Note removal failed: {}", err))
                .with_context("deck_hash", info.remote_deck)
                .report_to_sentry();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Could not create removal request.".to_string(),
            )
        }
    }
}

pub async fn check_deck_alive(
    State(db_state): State<Arc<AppState>>,
    Json(request): Json<structs::CheckDeckAliveRequest>,
) -> impl IntoResponse {
    let deck_hashes = request.deck_hashes;

    // Validate all deck hashes in the request
    for deck_hash in &deck_hashes {
        if !validate_deck_hash(deck_hash) {
            return (
                StatusCode::BAD_REQUEST,
                "Invalid deck hash format".to_string(),
            );
        }
    }

    match pull::check_deck_alive(&db_state, deck_hashes.clone()).await {
        Ok(res) => {
            let json = serde_json::to_string(&res).unwrap();
            (StatusCode::OK, json)
        }
        Err(err) => {
            error::AppError::db_query("check_deck_alive", &err)
                .with_context("deck_count", deck_hashes.len().to_string())
                .report_to_sentry();
            (StatusCode::INTERNAL_SERVER_ERROR, "Error".to_string())
        }
    }
}

pub async fn add_subscription(
    State(db_state): State<Arc<AppState>>,
    Json(request): Json<structs::SubscriptionRequest>,
) -> impl IntoResponse {
    if !validate_deck_hash(&request.deck_hash) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid deck hash format".to_string(),
        );
    }

    let deck_hash = request.deck_hash;
    let user_hash = request.user_hash;
    match subscription::add(&db_state, deck_hash, user_hash).await {
        Ok(res) => (StatusCode::OK, format!("{:?}", res)),
        Err(error) => {
            // Subscription failures are usually duplicate entries or invalid deck - expected
            error::log_expected("add_subscription", StatusCode::UNPROCESSABLE_ENTITY, &error.to_string());
            (StatusCode::UNPROCESSABLE_ENTITY, "Could not add subscription".to_string())
        }
    }
}
pub async fn remove_subscription(
    State(db_state): State<Arc<AppState>>,
    Json(request): Json<structs::SubscriptionRequest>,
) -> impl IntoResponse {
    if !validate_deck_hash(&request.deck_hash) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid deck hash format".to_string(),
        );
    }

    let deck_hash = request.deck_hash;
    let user_hash = request.user_hash;
    match subscription::remove(&db_state, deck_hash, user_hash).await {
        Ok(res) => (StatusCode::OK, format!("{:?}", res)),
        Err(error) => {
            // Subscription not found - expected behavior
            error::log_expected("remove_subscription", StatusCode::NOT_FOUND, &error.to_string());
            (StatusCode::NOT_FOUND, "Subscription not found".to_string())
        }
    }
}

pub async fn get_deck_timestamp(
    State(db_state): State<Arc<AppState>>,
    Path(deck_hash): Path<String>,
) -> impl IntoResponse {
    if !validate_deck_hash(&deck_hash) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid deck hash format".to_string(),
        );
    }

    match pull::get_deck_last_update_unix(&db_state, &deck_hash).await {
        Ok(res) => (StatusCode::OK, format!("{res}")),
        Err(err) => {
            // Deck not found is expected, other errors are bugs
            let err_str = err.to_string();
            if err_str.contains("not found") || err_str.contains("No deck") {
                error::log_expected("get_deck_timestamp", StatusCode::NOT_FOUND, &err_str);
            } else {
                error::AppError::db_query("get_deck_timestamp", &err)
                    .with_context("deck_hash", deck_hash.clone())
                    .report_to_sentry();
            }
            (StatusCode::INTERNAL_SERVER_ERROR, "0.0".to_string())
        }
    }
}

pub async fn submit_changelog(
    State(db_state): State<Arc<AppState>>,
    Json(changelog_data): Json<structs::SubmitChangelog>,
) -> impl IntoResponse {
    if !validate_deck_hash(&changelog_data.deck_hash) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid deck hash format".to_string(),
        );
    }

    // check if they are authorized to add a changelog message to this deck
    let access_token =
        (auth::is_valid_user_token(&db_state, &changelog_data.token, &changelog_data.deck_hash)
            .await)
            .unwrap_or_default();
    if !access_token {
        return (
            StatusCode::UNAUTHORIZED,
            "You are not authorized to add a changelog message to this deck".to_string(),
        );
    }

    match changelog::insert_new_changelog(
        &db_state,
        &changelog_data.deck_hash,
        &changelog_data.changelog,
    )
    .await
    {
        Ok(_res) => (
            StatusCode::OK,
            "Changelog published successfully!".to_string(),
        ),
        Err(err) => {
            // This is a real bug if it fails - the user is authorized
            error::AppError::bug("submit_changelog", format!("Changelog insert failed: {}", err))
                .with_context("deck_hash", changelog_data.deck_hash.clone())
                .report_to_sentry();
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "An error occurred while publishing the changelog.".to_string(),
            )
        }
    }
}

pub async fn check_user_token(
    State(db_state): State<Arc<AppState>>,
    Json(info): Json<structs::TokenInfo>,
) -> impl IntoResponse {
    let quer = auth::get_user_from_token(&db_state, &info.token)
        .await
        .unwrap_or_default();
    let res = quer > 0;
    (StatusCode::OK, serde_json::to_string(&res).unwrap())
}

pub async fn refresh_auth_token(
    State(db_state): State<Arc<AppState>>,
    Json(refresh_req): Json<auth::TokenRefresh>,
) -> impl IntoResponse {
    match auth::refresh_token(&db_state, &refresh_req).await {
        Ok(res) => {
            let json = serde_json::to_string(&res).unwrap();
            (StatusCode::OK, json)
        }
        Err(error) => {
            if error != "Invalid or expired refresh token" {
                println!("Error occurred: {error}");
            }
            (StatusCode::UNAUTHORIZED, "Error".to_string())
        }
    }
}

async fn get_protected_fields_from_deck(
    State(db_state): State<Arc<AppState>>,
    Path(deck_hash): Path<String>,
) -> impl IntoResponse {
    if !validate_deck_hash(&deck_hash) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid deck hash format".to_string(),
        );
    }

    let client = db_state
        .db_pool
        .get_owned()
        .await
        .map_err(|e| {
            println!("Error getting pool: {e}");
            (StatusCode::INTERNAL_SERVER_ERROR, "Error".to_string())
        })
        .unwrap();
    let quer = notetypes::pull_protected_fields(&client, &deck_hash)
        .await
        .unwrap_or_default();
    (StatusCode::OK, serde_json::to_string(&quer).unwrap())
}

fn media_routes() -> Router<Arc<AppState>> {
    let media_api_ratelimit: u32 = std::env::var("MEDIA_API_RATE_LIMIT_PER_MINUTE")
        .expect("MEDIA_API_RATE_LIMIT_PER_MINUTE must be set")
        .parse()
        .expect("Rate limit must be a valid number");

    let base_router = Router::new()
        .route("/check/bulk", post(media_manager::check_media_bulk))
        .route("/confirm/bulk", post(confirm_media_bulk_async))
        .route("/manifest", post(media_manager::get_media_manifest))
        .route("/sanitize/svg", post(media_manager::sanitize_svg_batch))
        .route("/missing/{deck_hash}", get(get_all_deck_missing_media));

    // Allow disabling the HTTP governor by setting MEDIA_API_RATE_LIMIT_PER_MINUTE=0
    if media_api_ratelimit == 0 {
        return base_router;
    }

    let media_governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_second((f64::from(media_api_ratelimit) / 60.0) as u64)
            .burst_size(media_api_ratelimit / 2)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .unwrap(),
    );

    let governor_limiter = media_governor_conf.limiter().clone();
    let interval = Duration::from_secs(60);
    // background task to prune governor state
    std::thread::spawn(move || loop {
        std::thread::sleep(interval);
        governor_limiter.retain_recent();
    });

    base_router.layer(GovernorLayer::new(media_governor_conf))
}

async fn get_bucket_size(s3_client: &S3Client, s3_throttle: &S3Throttle, bucket: &str) -> i64 {
    let mut total_bytes: i64 = 0;
    let mut continuation_token: Option<String> = None;

    loop {
        let page = match s3_ops::list_objects_v2_with_client(
            s3_client,
            s3_throttle,
            bucket,
            None,
            continuation_token.clone(),
            None,
        )
        .await
        {
            Ok(p) => p,
            Err(err) => {
                println!("Error listing objects: {err:?}");
                return 0;
            }
        };

        total_bytes += page
            .contents()
            .iter()
            .map(|obj| obj.size.unwrap_or_default())
            .sum::<i64>();

        if page.is_truncated.unwrap_or(false) {
            continuation_token = page.next_continuation_token.clone();
            if continuation_token.is_none() {
                println!("Paginator indicated truncation but no continuation token returned");
                break;
            }
        } else {
            break;
        }
    }

    println!(
        "Total bucket size in GB: {} GB",
        total_bytes / 1024 / 1024 / 1024
    );

    total_bytes
}

async fn get_all_deck_missing_media(
    State(state): State<Arc<AppState>>,
    Path(deck_hash): Path<String>,
) -> impl IntoResponse {
    if !validate_deck_hash(&deck_hash) {
        return (
            StatusCode::BAD_REQUEST,
            "Invalid deck hash format".to_string(),
        );
    }

    let client: database::SharedConn = match state.db_pool.get_owned().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return (StatusCode::INTERNAL_SERVER_ERROR, "[]".to_string());
        }
    };
    let media_files = media_reference_manager::get_missing_media(&client, &deck_hash)
        .await
        .unwrap_or_else(|_| vec![]);

    let json = serde_json::to_string(&media_files).unwrap();
    (StatusCode::OK, json)
}

#[tokio::main]
async fn main() {
    dotenvy::dotenv().expect(
        "Expected .env file in the root directory containing the database connection string",
    );
    let _guard = sentry::init((
        env::var("SENTRY_URL").expect("SENTRY_URL must be set"),
        sentry::ClientOptions {
            release: sentry::release_name!(),
            traces_sample_rate: 0.2,
            max_breadcrumbs: 50,
            send_default_pii: false,
            before_send: Some(Arc::new(|mut event| {
                if let Some(user) = &mut event.user {
                    user.ip_address = None;
                    user.email = None;
                    user.username = None;
                    if let Some(id) = &user.id {
                        if !id.starts_with("user_") && !id.starts_with("hash_") {
                            user.id = None;
                        }
                    }
                }

                static LAST_ERRORS: once_cell::sync::Lazy<
                    std::sync::Mutex<std::collections::HashMap<String, std::time::Instant>>,
                > = once_cell::sync::Lazy::new(|| {
                    std::sync::Mutex::new(std::collections::HashMap::new())
                });

                // Create a unique key for this event based on message and level
                let event_key = format!(
                    "{}:{}:{}",
                    event.message.as_ref().map(|m| m.as_str()).unwrap_or(""),
                    event.level,
                    event
                        .exception
                        .values
                        .first()
                        .map(|e| e.ty.as_str())
                        .unwrap_or("")
                );

                let mut last_errors = LAST_ERRORS.lock().unwrap();
                let now = std::time::Instant::now();

                // Check if we've seen this error recently (within 60 seconds)
                if let Some(last_seen) = last_errors.get(&event_key) {
                    if now.duration_since(*last_seen).as_secs() < 60 {
                        // Drop this event - it's a duplicate within the rate limit window
                        return None;
                    }
                }

                // Update the last seen time
                last_errors.insert(event_key, now);

                // Clean up old entries (older than 5 minutes)
                last_errors.retain(|_, &mut v| now.duration_since(v).as_secs() < 300);

                Some(event)
            })),
            // Sample breadcrumbs to reduce data volume
            before_breadcrumb: Some(Arc::new(|breadcrumb| {
                // Only keep 30% of breadcrumbs to reduce noise
                use std::collections::hash_map::RandomState;
                use std::hash::{BuildHasher, Hash, Hasher};

                let mut hasher = RandomState::new().build_hasher();
                breadcrumb
                    .message
                    .as_ref()
                    .unwrap_or(&String::new())
                    .hash(&mut hasher);

                if hasher.finish() % 10 < 3 {
                    Some(breadcrumb)
                } else {
                    None
                }
            })),
            ..Default::default()
        },
    ));

    let s3_access_key_id = std::env::var("S3_ACCESS_KEY_ID").expect("S3_ACCESS_KEY_ID must be set");
    let s3_secret_access_key =
        std::env::var("S3_SECRET_ACCESS_KEY").expect("S3_SECRET_ACCESS_KEY must be set");
    let s3_domain = std::env::var("S3_DOMAIN").expect("S3_DOMAIN must be set");
    let bucket_name = std::env::var("S3_BUCKET_NAME").expect("S3_BUCKET_NAME must be set");
    let media_base_url_value = std::env::var("MEDIA_BASE_URL").expect("MEDIA_BASE_URL must be set");
    let media_base_url = Arc::new(media_base_url_value);
    let media_token_secret =
        std::env::var("MEDIA_TOKEN_SECRET").expect("MEDIA_TOKEN_SECRET must be set");
    let media_upload_ttl_secs = std::env::var("MEDIA_UPLOAD_TOKEN_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(900);
    let media_download_ttl_secs = std::env::var("MEDIA_DOWNLOAD_TOKEN_TTL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(900);

    let media_token_secret_bytes = media_token_secret.clone().into_bytes();
    let media_token_service = MediaTokenService::new(
        media_token_secret_bytes,
        Duration::from_secs(media_upload_ttl_secs),
        Duration::from_secs(media_download_ttl_secs),
    )
    .expect("Failed to initialize media token service");

    let cache_token_secret = std::env::var("CACHE_TOKEN_SECRET").unwrap_or(media_token_secret);
    let cache_token_service = cache_tokens::CacheTokenService::new(
        cache_token_secret.into_bytes(),
        cache_manager::deck_cache_token_ttl(),
    )
    .expect("Failed to initialize cache token service");

    let credentials = aws_sdk_s3::config::Credentials::new(
        s3_access_key_id,
        s3_secret_access_key,
        None,
        None,
        "s3-credentials",
    );

    let region_provider =
        aws_config::meta::region::RegionProviderChain::default_provider().or_else("eu-central-1"); // Europe (Frankfurt)
    let s3_config = aws_config::from_env()
        .region(region_provider)
        .credentials_provider(aws_sdk_s3::config::SharedCredentialsProvider::new(
            credentials,
        ))
        .endpoint_url(&s3_domain)
        .load()
        .await;

    let s3_service_config = aws_sdk_s3::config::Builder::from(&s3_config)
        .force_path_style(true) // Contabo is <special>
        .build();

    let s3_client = S3Client::from_conf(s3_service_config);

    // Shared S3 throttle to stay below provider-wide limits while keeping throughput high
    let s3_max_rps: u32 = std::env::var("S3_MAX_RPS")
        .ok()
        .and_then(|v| v.parse::<u32>().ok())
        .unwrap_or(220);
    let s3_max_concurrency: usize = std::env::var("S3_MAX_CONCURRENCY")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .unwrap_or(48); // allow more in-flight while still bounded
    let s3_throttle = Arc::new(S3Throttle::new(s3_max_concurrency, s3_max_rps));

    let pool = database::establish_pool_connection()
        .await
        .expect("Failed to establish database connection pool");

    // Initialize rate limiter
    // Get current bucket size of bucket bucket_name
    let bucket_size = get_bucket_size(&s3_client, s3_throttle.as_ref(), &bucket_name).await;

    let rate_limiter = RateLimiter::new(Arc::new(pool.clone()), bucket_size as u64);
    rate_limiter
        .load_user_quotas()
        .await
        .expect("Failed to load user quotas");

    let state = Arc::new(database::AppState {
        db_pool: Arc::new(pool),
        base64_engine: Arc::new(general_purpose::STANDARD),
        s3_client,
        rate_limiter,
        bulk_operations: Arc::new(RwLock::new(HashMap::new())),
        media_token_service: Arc::new(media_token_service),
        media_base_url: media_base_url.clone(),
        media_token_replay_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        cache_token_service: Arc::new(cache_token_service),
        cache_base_url: media_base_url.clone(),
        cache_token_replay_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        s3_throttle: s3_throttle.clone(),
    });

    // let is_dry_run = true; // <-- CHANGE TO false TO ENABLE DELETION
    // println!("Starting cleanup job wrapper...");
    // match media_manager::cleanup_orphaned_media_s3(state.clone(), is_dry_run).await {
    //     Ok(deleted_count) => {
    //         if is_dry_run {
    //             println!("[DRY RUN] Orphaned media cleanup simulation completed successfully.");
    //         } else {
    //             println!("Orphaned media cleanup completed successfully. Deleted {deleted_count} objects.");
    //         }
    //     }
    //     Err(e) => {
    //         println!("Orphaned media cleanup job failed: {e:?}");
    //         // Implement alerting or specific failure handling here
    //         // e.g., send notification to admin, increment metrics counter
    //     }
    // }
    // println!("Cleanup job wrapper finished.");

    // IP RAte limiter
    let global_api_ratelimit: u32 = std::env::var("STANDARD_API_RATE_LIMIT_PER_MINUTE")
        .expect("STANDARD_API_RATE_LIMIT_PER_MINUTE must be set")
        .parse()
        .expect("Rate limit must be a valid number");
    let global_governor_conf = Arc::new(
        GovernorConfigBuilder::default()
            .per_second((f64::from(global_api_ratelimit) / 60.0) as u64)
            .burst_size(global_api_ratelimit / 2)
            .key_extractor(SmartIpKeyExtractor)
            .finish()
            .unwrap(),
    );

    let governor_limiter = global_governor_conf.limiter().clone();
    let interval = Duration::from_secs(60);
    // a separate background task to clean up
    std::thread::spawn(move || loop {
        std::thread::sleep(interval);
        governor_limiter.retain_recent();
    });

    // start media cleanup task
    media_manager::start_cleanup_task(state.clone()).await;

    // start media token replay cache cleanup task
    media_proxy::start_token_cleanup_task(state.clone()).await;

    // start bulk operations cache cleanup task
    let bulk_cache_cleanup_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(300)); // 5 minutes

        loop {
            interval.tick().await;

            let now = chrono::Utc::now();
            let expiry_threshold = now - chrono::Duration::hours(1); // Remove operations older than 1 hour

            let mut cache = bulk_cache_cleanup_state.bulk_operations.write().await;
            let mut to_remove = Vec::new();

            for (id, info) in cache.iter() {
                if info.created_at < expiry_threshold {
                    to_remove.push(*id);
                }
            }

            for id in to_remove {
                cache.remove(&id);
            }
        }
    });

    // Schedule periodic cleanup of expired tokens
    let cleanup_state = state.clone();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(86400)); // Daily
        loop {
            interval.tick().await;

            // Clean up expired tokens
            match auth::cleanup_expired_tokens(&cleanup_state).await {
                Ok(count) => {
                    if count > 0 {
                        println!("Cleaned up {count} expired tokens");
                    }
                }
                Err(e) => println!("Error cleaning up tokens: {e}"),
            }

            // Delete unused notetypes and refresh deck stats materialized view
            let client: database::SharedConn = match cleanup_state.db_pool.get_owned().await {
                Ok(pool) => pool,
                Err(err) => {
                    println!("Error getting pool: {err}");
                    continue;
                }
            };

            match notetypes::delete_unused_notetypes(&client).await {
                Ok(count) => {
                    println!("Deleted {count} unused notetypes");
                }
                Err(e) => println!("Error deleting unused notetypes: {e}"),
            }

            client
                .execute("REFRESH MATERIALIZED VIEW deck_stats", &[])
                .await
                .expect("Error executing large decks statement 2");
        }
    });

    // Enable tracing.
    let env_filter = if cfg!(debug_assertions) {
        // Debug build
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            format!(
                "{}=debug,tower_http=debug,axum=trace",
                env!("CARGO_CRATE_NAME")
            )
            .into()
        })
    } else {
        // Release build
        tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
            format!(
                "{}=info,tower_http=info,axum=info",
                env!("CARGO_CRATE_NAME")
            )
            .into()
        })
    };

    tracing_subscriber::registry()
        .with(env_filter)
        .with(tracing_subscriber::fmt::layer().without_time())
        .init();

    let media_api_rate_limit_per_minute: u32 = std::env::var("MEDIA_API_RATE_LIMIT_PER_MINUTE")
        .expect("MEDIA_API_RATE_LIMIT_PER_MINUTE must be set")
        .parse()
        .expect("Rate limit must be a valid number");
    let media_requests_per_second = (f64::from(media_api_rate_limit_per_minute) / 60.0) as u64;
    let media_burst = media_api_rate_limit_per_minute / 2;

    let media_governor_layer = if media_requests_per_second == 0 {
        None
    } else {
        Some(GovernorLayer::new(Arc::new(
            GovernorConfigBuilder::default()
                .per_second(media_requests_per_second)
                .burst_size(media_burst)
                .key_extractor(SmartIpKeyExtractor)
                .finish()
                .expect("Failed to build media governor config"),
        )))
    };

    // Build our application with routes
    let app = Router::new()
        .route("/pullChanges", post(check_for_update))
        .route("/createDeck", post(post_data))
        .route("/submitCard", post(process_card))
        .route("/CheckDeckAlive", post(check_deck_alive))
        .route("/AddSubscription", post(add_subscription))
        .route("/RemoveSubscription", post(remove_subscription))
        .route("/GetDeckTimestamp/{deck_hash}", get(get_deck_timestamp))
        .route("/submitChangelog", post(submit_changelog))
        .route("/CreateDeckLink", post(create_deck_link))
        .route("/CreateNewNoteLink", post(create_note_links))
        .route("/login", post(post_login))
        .route("/removeToken/{token}", get(remove_token))
        .route("/UploadDeckStats", post(upload_deck_stats))
        .route("/requestRemoval", post(request_removal))
        .route("/CheckUserToken", post(check_user_token))
        .route("/refreshToken", post(refresh_auth_token))
        .route(
            "/GetProtectedFields/{deck_hash}",
            get(get_protected_fields_from_deck),
        )
        .nest("/media", media_routes())
        // standard rate limiting - applied to all routes EXCEPT media proxy
        .layer(GovernorLayer::new(global_governor_conf))
        .merge(match &media_governor_layer {
            Some(layer) => media_proxy::routes().layer(layer.clone()),
            None => media_proxy::routes(),
        })
        .merge(match &media_governor_layer {
            Some(layer) => cache_proxy::routes().layer(layer.clone()),
            None => cache_proxy::routes(),
        })
        .with_state(state)
        .layer(axum::extract::DefaultBodyLimit::disable())
        .layer((
            TraceLayer::new_for_http()
                .on_request(())
                .on_response(())
                .on_failure(
                    |error: ServerErrorsFailureClass, latency: std::time::Duration, _span: &tracing::Span| {
                        match &error {
                            ServerErrorsFailureClass::StatusCode(code) => {
                                // 5xx errors that reach here are UNHANDLED - these are real bugs
                                // Note: Our handlers should return proper error types that report
                                // to Sentry themselves. This is a safety net for unexpected panics.
                                if code.is_server_error() {
                                    // This indicates an unhandled error path - a bug!
                                    sentry::with_scope(
                                        |scope| {
                                            scope.set_fingerprint(Some(&["unhandled_server_error", code.as_str()]));
                                            scope.set_tag("error_type", "unhandled_5xx");
                                            scope.set_tag("status_code", code.as_str());
                                            scope.set_extra("latency_ms", (latency.as_millis() as u64).into());
                                        },
                                        || {
                                            sentry::capture_message(
                                                &format!(
                                                    "UNHANDLED SERVER ERROR: {} - This error was not caught by handlers. \
                                                    Check for missing error handling or panic recovery. latency={}ms",
                                                    code, latency.as_millis()
                                                ),
                                                sentry::Level::Error,
                                            );
                                        },
                                    );
                                    tracing::error!(
                                        status = %code,
                                        latency_ms = %latency.as_millis(),
                                        "Unhandled server error - check error handling"
                                    );
                                }
                                // 4xx errors are expected (auth failures, bad requests, etc.) - don't report to Sentry
                            }
                            ServerErrorsFailureClass::Error(msg) => {
                                // Connection/protocol errors (client disconnect, etc.)
                                // These are usually not bugs, but we log them as warnings
                                let msg_str = msg.to_string();
                                
                                // Filter out expected connection issues
                                let is_expected = msg_str.contains("connection reset")
                                    || msg_str.contains("broken pipe")
                                    || msg_str.contains("connection closed")
                                    || msg_str.contains("client disconnect");
                                
                                if !is_expected {
                                    sentry::with_scope(
                                        |scope| {
                                            scope.set_fingerprint(Some(&["protocol_error", &msg_str]));
                                            scope.set_tag("error_type", "protocol");
                                            scope.set_extra("latency_ms", (latency.as_millis() as u64).into());
                                        },
                                        || {
                                            sentry::capture_message(
                                                &format!("Protocol/connection error: {}", msg_str),
                                                sentry::Level::Warning,
                                            );
                                        },
                                    );
                                }
                                tracing::warn!(
                                    error = %msg,
                                    latency_ms = %latency.as_millis(),
                                    expected = is_expected,
                                    "Connection/protocol issue"
                                );
                            }
                        }
                    },
                ),
            // Graceful shutdown will wait for outstanding requests to complete. Add a timeout so
            // requests don't hang forever. Causes issues for streaming large decks that take more than 10secs to generate. hence i disabled it
            //TimeoutLayer::new(Duration::from_secs(10)),
        ))
        .layer(ClientIpSource::CfConnectingIp.into_extension());
    //.layer(ClientIpSource::ConnectInfo.into_extension());

    // run it
    let listener = tokio::net::TcpListener::bind("localhost:5555")
        .await
        .unwrap();
    println!("listening on {}", listener.local_addr().unwrap());
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .unwrap();
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}
