use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use axum::{extract::State, http::StatusCode, Json};
use axum_client_ip::ClientIp;
use chrono::{Duration, Utc};
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio::time;
use uuid::Uuid;

use md5::{Digest, Md5};
use std::collections::{HashMap, HashSet};
use tokio::io::AsyncReadExt;

use crate::media_logger::{self, MediaOperationType};
use crate::media_tokens::{DownloadTokenParams, UploadTokenParams};
use crate::structs::{
    MediaBulkCheckRequest, MediaBulkCheckResponse, MediaBulkConfirmRequest,
    MediaBulkConfirmResponse, MediaDownloadItem, MediaExistingFile, MediaManifestRequest,
    MediaManifestResponse, MediaMissingFile, MediaProcessedFile, SanitizedSvgItem,
    SvgSanitizeRequest, SvgSanitizeResponse,
};
use crate::{auth, media_reference_manager, AppState};
use crate::s3_ops;

use lazy_static::lazy_static;
use std::env::var;

// Helper functions for Sentry privacy and error handling
pub fn anonymize_filename(filename: &str) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    // Extract file extension (keep for debugging)
    let extension = filename.rsplit('.').next().unwrap_or("");

    // Hash the filename
    let mut hasher = DefaultHasher::new();
    filename.hash(&mut hasher);
    let hash = hasher.finish();

    if extension.is_empty() || extension == filename {
        format!("file_{:x}", hash)
    } else {
        format!("file_{:x}.{}", hash, extension)
    }
}

/// Create a safe, printable preview of file bytes for diagnostic logging.
/// Returns a string showing the first N bytes in a mix of ASCII and hex representation.
fn create_byte_preview(bytes: &[u8], max_bytes: usize) -> String {
    let preview_len = bytes.len().min(max_bytes);
    let preview_bytes = &bytes[..preview_len];
    
    // Create a mixed representation: printable ASCII chars shown as-is, others as hex
    let mut result = String::with_capacity(preview_len * 4);
    result.push_str("[");
    
    for (i, &b) in preview_bytes.iter().enumerate() {
        if i > 0 {
            result.push(' ');
        }
        if b.is_ascii_graphic() || b == b' ' {
            result.push(b as char);
        } else if b == b'\n' {
            result.push_str("\\n");
        } else if b == b'\r' {
            result.push_str("\\r");
        } else if b == b'\t' {
            result.push_str("\\t");
        } else {
            result.push_str(&format!("x{:02x}", b));
        }
    }
    
    if bytes.len() > max_bytes {
        result.push_str(&format!("... (+{} more bytes)", bytes.len() - max_bytes));
    }
    result.push(']');
    result
}

/// Create a text preview for files that appear to be text-based (like SVG)
fn create_text_preview(bytes: &[u8], max_chars: usize) -> Option<String> {
    // Try to interpret as UTF-8
    match std::str::from_utf8(bytes) {
        Ok(text) => {
            let trimmed = text.trim_start_matches('\u{feff}').trim_start();
            let preview: String = trimmed.chars().take(max_chars).collect();
            if trimmed.len() > max_chars {
                Some(format!("{}... (+{} more chars)", preview, trimmed.len() - max_chars))
            } else {
                Some(preview)
            }
        }
        Err(_) => None,
    }
}

// Media file type constants
pub(crate) const MAX_FILE_SIZE_BYTES: usize = 10 * 1024 * 1024; // 10 MB

const EXTENSION_TO_MIME: [(&str, &str); 12] = [
    ("jpg", "image/jpeg"),
    ("jpeg", "image/jpeg"),
    ("png", "image/png"),
    ("gif", "image/gif"),
    ("webp", "image/webp"),
    ("svg", "image/svg+xml"),
    ("bmp", "image/bmp"),
    ("tif", "image/tiff"),
    ("tiff", "image/tiff"),
    ("mp3", "audio/mpeg"),
    ("ogg", "audio/ogg"),
    ("oga", "audio/ogg"),
];

#[allow(dead_code)]
#[derive(Clone)]
struct DetectedMedia {
    is_svg: bool,
}

fn detect_mime(bytes: &[u8]) -> Option<&'static str> {
    infer::get(bytes).map(|t| t.mime_type())
}

// We only need to know if it is SVG, not to fully parse the document.
fn looks_like_svg(bytes: &[u8]) -> bool {
    // Limit scan to the first 8KB to avoid unnecessary work.
    let window = bytes.get(0..std::cmp::min(8192, bytes.len())).unwrap_or(bytes);

    // Handle potential BOM and whitespace before the root tag.
    let text = match std::str::from_utf8(window) {
        Ok(t) => t.trim_start_matches('\u{feff}').trim_start(),
        Err(_) => return false,
    };

    // Case-insensitive search for <svg in the early portion.
    let lower = text.to_lowercase();
    lower.contains("<svg")
}

fn detect_supported_media(bytes: &[u8]) -> Option<DetectedMedia> {
    let mime_guess = detect_mime(bytes);

    if infer::is_image(bytes) {
        let is_svg = looks_like_svg(bytes);
        return Some(DetectedMedia { is_svg });
    }

    if infer::is_audio(bytes) {
        return Some(DetectedMedia { is_svg: false });
    }

    if let Some(mime) = mime_guess {
        if matches!(mime, "text/xml" | "application/xml" | "text/plain") && looks_like_svg(bytes) {
            return Some(DetectedMedia { is_svg: true });
        }
        
        if mime == "application/x-riff" {
            return Some(DetectedMedia { is_svg: false });
        }

        // infer reports unknown binary as octet-stream; probe for common cases we allow.
        if mime == "application/octet-stream" {
            if looks_like_webp(bytes) {
                return Some(DetectedMedia { is_svg: false });
            }

            if looks_like_ogg(bytes) {
                return Some(DetectedMedia { is_svg: false });
            }
                        
            if looks_like_svg(bytes) {
                return Some(DetectedMedia { is_svg: true });    
            }
        }

    } else {
        // No mime detected; last attempt
        if looks_like_svg(bytes) {
            return Some(DetectedMedia { is_svg: true });    
        }
        if looks_like_mp3(bytes) {
            return Some(DetectedMedia { is_svg: false });
        }
        if looks_like_webp(bytes) {
            return Some(DetectedMedia { is_svg: false });
        }

        if looks_like_ogg(bytes) {
            return Some(DetectedMedia { is_svg: false });
        }
    }

    None
}

fn looks_like_ogg(bytes: &[u8]) -> bool {
    bytes.len() >= 4 && bytes.starts_with(b"OggS")
}

// WebP format: "RIFF" [4 bytes size] "WEBP" ...
fn looks_like_webp(bytes: &[u8]) -> bool {
    bytes.len() >= 12
        && bytes.starts_with(b"RIFF")
        && &bytes[8..12] == b"WEBP"
}


// Function to validate file signatures https://en.wikipedia.org/wiki/List_of_file_signatures
fn looks_like_mp3(bytes: &[u8]) -> bool {
    if bytes.len() >= 3 && &bytes[0..3] == b"ID3" {
        return true;
    }

    if bytes.len() < 4 {
        return false;
    }

    // Search for MP3 frame sync within first 512 bytes (handles padding/junk data)
    // Need at least 4 bytes from current position for validation
    let search_limit = bytes.len().saturating_sub(3).min(512);

    for i in 0..search_limit {
        // Check for frame sync: 11 bits set (0xFF followed by 0xE0-0xFF)
        if bytes[i] != 0xFF || (bytes[i + 1] & 0xE0) != 0xE0 {
            continue;
        }

        // Validate MPEG version bits
        let version_bits = (bytes[i + 1] >> 3) & 0x03;
        if version_bits == 0b01 {
            continue; // Reserved MPEG version
        }

        // Validate layer bits
        let layer_bits = (bytes[i + 1] >> 1) & 0x03;
        if layer_bits == 0b00 {
            continue; // Reserved layer
        }

        // Validate bitrate index
        let bitrate_index = (bytes[i + 2] >> 4) & 0x0F;
        if bitrate_index == 0x0F || bitrate_index == 0x00 {
            continue; // Bad or free bitrate
        }

        // Validate sample rate index
        let sample_rate_index = (bytes[i + 2] >> 2) & 0x03;
        if sample_rate_index == 0x03 {
            continue; // Reserved sample rate
        }

        // Found valid MP3 frame header
        return true;
    }

    false
}

// S3 bucket configuration
lazy_static! {
    static ref MEDIA_BUCKET: String = var("S3_BUCKET_NAME").unwrap();
}

pub(crate) fn media_bucket() -> &'static str {
    MEDIA_BUCKET.as_str()
}

const PRESIGNED_URL_EXPIRATION: u64 = 15; // minutes

// Get all objects from s3, see which ones aren't references in the media table (should be impossible to have this happen, but just in case) and remove them.
pub async fn cleanup_orphaned_media_s3(
    state: Arc<AppState>,
    dry_run: bool,
) -> Result<usize, (StatusCode, String)> {
    // Build a set of hashes known to the system. Include both committed media_files
    // and recent pending bulk uploads so we don't accidentally delete files that
    // are in-flight during a bulk upload confirmation (race window between S3 put
    // and DB insert).
    let mut db_hashes: HashSet<String> = HashSet::new();
    let conn = state.db_pool.get().await.map_err(|err| {
        sentry::capture_message(
            &format!("S3 cleanup: Failed to get database connection: {}", err),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Error getting database connection".to_string(),
        )
    })?;

    // Add existing, committed media file hashes
    match conn.query("SELECT hash FROM media_files", &[]).await {
        Ok(rows) => {
            for row in rows {
                let h: String = row.get::<usize, String>(0);
                db_hashes.insert(h);
            }
        }
        Err(err) => {
            sentry::capture_message(
                &format!("S3 cleanup: Failed to query media_files table: {}", err),
                sentry::Level::Error,
            );
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error".to_string(),
            ));
        }
    }

    // Also include hashes referenced by recent bulk upload metadata to protect
    // in-flight uploads from being considered orphaned. We only look back a day
    // to avoid protecting stale entries indefinitely.
    match conn
        .query(
            "SELECT metadata FROM media_bulk_uploads WHERE created_at >= NOW() - INTERVAL '1 day'",
            &[],
        )
        .await
    {
        Ok(rows) => {
            for row in rows {
                let metadata_json: serde_json::Value = row.get(0);
                match serde_json::from_value::<Vec<MediaMissingFile>>(metadata_json) {
                    Ok(files) => {
                        for f in files {
                            db_hashes.insert(f.hash);
                        }
                    }
                    Err(_) => {
                        // Non-fatal: if parsing fails, skip that batch
                        sentry::add_breadcrumb(sentry::Breadcrumb {
                            category: Some("s3_cleanup".into()),
                            message: Some("Failed to parse media_bulk_uploads metadata when protecting in-flight uploads".to_string()),
                            level: sentry::Level::Warning,
                            ..Default::default()
                        });
                    }
                }
            }
        }
        Err(err) => {
            // Non-fatal: log and continue; we still have committed media file hashes
            sentry::add_breadcrumb(sentry::Breadcrumb {
                category: Some("s3_cleanup".into()),
                message: Some(format!(
                    "Failed to query media_bulk_uploads for orphan protection: {}",
                    err
                )),
                level: sentry::Level::Warning,
                ..Default::default()
            });
        }
    }
    if db_hashes.is_empty() && !dry_run {
        sentry::capture_message(
            "S3 cleanup: No media hashes found in database",
            sentry::Level::Warning,
        );
        return Ok(0);
    }

    let mut orphaned_s3_keys: Vec<String> = Vec::new(); // Store the *full S3 keys* of orphans
    let mut continuation_token: Option<String> = None;
    let mut total_s3_objects_scanned = 0;
    let mut invalid_key_format_count = 0;
    let mut prefix_mismatch_count = 0;

    loop {
        let resp = match s3_ops::list_objects_v2(
            state.as_ref(),
            MEDIA_BUCKET.as_str(),
            None,
            continuation_token.clone(),
            None,
        )
        .await
        {
            Ok(resp) => resp,
            Err(err) => {
                sentry::capture_message(
                    &format!("S3 cleanup: Failed to list S3 objects: {err:?}"),
                    sentry::Level::Error,
                );
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to list objects in S3".to_string(),
                ));
            }
        };

        // Process the objects in the current page
        if let Some(objects) = resp.contents {
            let count = objects.len();
            total_s3_objects_scanned += count;

            for object in objects {
                if let Some(s3_key) = object.key {
                    // Expect key format: "ab/abcdef123..."
                    let parts: Vec<&str> = s3_key.splitn(2, '/').collect();

                    // Basic format check
                    if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                        sentry::add_breadcrumb(sentry::Breadcrumb {
                            category: Some("s3_cleanup".into()),
                            message: Some(format!(
                                "Invalid S3 key format (expected 'prefix/hash'): {}",
                                s3_key
                            )),
                            level: sentry::Level::Debug,
                            ..Default::default()
                        });
                        invalid_key_format_count += 1;
                        continue;
                    }
                    let s3_prefix = parts[0];
                    let s3_hash = parts[1];

                    // Check prefix length and that it matches the start of the hash
                    if s3_prefix.len() != 2 || !s3_hash.starts_with(s3_prefix) {
                        if s3_prefix == "decks" {
                            continue; // Skip deck prefixes; handled separately below
                        } else {
                            sentry::add_breadcrumb(sentry::Breadcrumb {
                                category: Some("s3_cleanup".into()),
                                message: Some(format!(
                                    "S3 key prefix does not match hash start: {}",
                                    s3_key
                                )),
                                level: sentry::Level::Debug,
                                ..Default::default()
                            });
                            prefix_mismatch_count += 1;
                            continue;
                        }
                    }

                    // Check if the extracted hash exists in our set of DB hashes
                    if !db_hashes.contains(s3_hash) {
                        orphaned_s3_keys.push(s3_key.clone());
                    }
                } else {
                    sentry::add_breadcrumb(sentry::Breadcrumb {
                        category: Some("s3_cleanup".into()),
                        message: Some("S3 object found with no key".to_string()),
                        level: sentry::Level::Warning,
                        ..Default::default()
                    });
                }
            }
        }

        // Check if the response is truncated (more pages exist)
        if resp.is_truncated.unwrap_or(false) {
            continuation_token = resp.next_continuation_token;
            if continuation_token.is_none() {
                // This shouldn't happen if is_truncated is true, but handle defensively
                sentry::capture_message(
                    "S3 cleanup: Response is truncated but no continuation token provided",
                    sentry::Level::Warning,
                );
                break;
            }
        } else {
            break;
        }
    }

    let mut orphaned_deck_object_count = 0usize;
    let mut orphaned_deck_prefix_count = 0usize;

    let mut deck_continuation_token: Option<String> = None;
    let mut discovered_deck_prefixes: HashMap<String, String> = HashMap::new();

    loop {
        let deck_resp = match s3_ops::list_objects_v2(
            state.as_ref(),
            MEDIA_BUCKET.as_str(),
            Some("decks/".to_string()),
            deck_continuation_token.clone(),
            Some("/".to_string()),
        )
        .await
        {
            Ok(resp) => resp,
            Err(err) => {
                sentry::capture_message(
                    &format!("S3 cleanup: Failed to list deck prefixes: {err:?}"),
                    sentry::Level::Warning,
                );
                break;
            }
        };

        if let Some(prefixes) = deck_resp.common_prefixes {
            for prefix in prefixes {
                if let Some(prefix_string) = prefix.prefix {
                    if let Some(deck_hash_with_slash) = prefix_string.strip_prefix("decks/") {
                        let deck_hash = deck_hash_with_slash.trim_end_matches('/');
                        if deck_hash.is_empty() {
                            continue;
                        }

                        discovered_deck_prefixes
                            .entry(deck_hash.to_string())
                            .or_insert(prefix_string.clone());
                    }
                }
            }
        }

        if deck_resp.is_truncated.unwrap_or(false) {
            deck_continuation_token = deck_resp.next_continuation_token;
            if deck_continuation_token.is_none() {
                sentry::capture_message(
                    "S3 cleanup: Deck prefix listing truncated without continuation token",
                    sentry::Level::Warning,
                );
                break;
            }
        } else {
            break;
        }
    }

    println!(
        "Discovered {} deck prefixes for verification",
        discovered_deck_prefixes.len()
    );

    if !discovered_deck_prefixes.is_empty() {
        let deck_hash_vec: Vec<String> = discovered_deck_prefixes.keys().cloned().collect();

        let existing_deck_hashes = match conn
            .query(
                "SELECT human_hash FROM decks WHERE human_hash = ANY($1)",
                &[&deck_hash_vec],
            )
            .await
        {
            Ok(rows) => Some(
                rows.iter()
                    .map(|row| row.get::<usize, String>(0))
                    .collect::<HashSet<_>>(),
            ),
            Err(err) => {
                sentry::capture_message(
                    &format!(
                        "S3 cleanup: Failed to query decks table while verifying prefixes: {}",
                        err
                    ),
                    sentry::Level::Warning,
                );
                None
            }
        };

        if let Some(existing_deck_hashes) = existing_deck_hashes {
            let mut orphaned_deck_prefixes: Vec<String> = Vec::new();

            for (deck_hash, prefix) in &discovered_deck_prefixes {
                if !existing_deck_hashes.contains(deck_hash) {
                    orphaned_deck_prefixes.push(prefix.clone());
                }
            }

            if !orphaned_deck_prefixes.is_empty() {
                orphaned_deck_prefix_count = orphaned_deck_prefixes.len();

                let mut deck_object_keys: Vec<String> = Vec::new();

                for prefix in &orphaned_deck_prefixes {
                    let mut prefix_continuation_token: Option<String> = None;

                    loop {
                        let prefix_resp = match s3_ops::list_objects_v2(
                            state.as_ref(),
                            MEDIA_BUCKET.as_str(),
                            Some(prefix.clone()),
                            prefix_continuation_token.clone(),
                            None,
                        )
                        .await
                        {
                            Ok(resp) => resp,
                            Err(err) => {
                                sentry::capture_message(
                                    &format!(
                                        "S3 cleanup: Failed to list objects for orphaned deck prefix {}: {err:?}",
                                        prefix
                                    ),
                                    sentry::Level::Warning,
                                );
                                break;
                            }
                        };

                        if let Some(objects) = prefix_resp.contents {
                            for object in objects {
                                if let Some(key) = object.key {
                                    deck_object_keys.push(key);
                                }
                            }
                        }

                        if prefix_resp.is_truncated.unwrap_or(false) {
                            prefix_continuation_token = prefix_resp.next_continuation_token;
                            if prefix_continuation_token.is_none() {
                                sentry::capture_message(
                                    &format!(
                                        "S3 cleanup: Deck object listing truncated without continuation token for prefix {}",
                                        prefix
                                    ),
                                    sentry::Level::Warning,
                                );
                                break;
                            }
                        } else {
                            break;
                        }
                    }
                }

                orphaned_deck_object_count = deck_object_keys.len();
                if orphaned_deck_object_count > 0 {
                    orphaned_s3_keys.extend(deck_object_keys);
                }
                sentry::add_breadcrumb(sentry::Breadcrumb {
                    category: Some("s3_cleanup".into()),
                    message: Some(format!(
                        "Identified {} orphaned deck prefixes totalling {} objects",
                        orphaned_deck_prefix_count, orphaned_deck_object_count
                    )),
                    level: sentry::Level::Info,
                    ..Default::default()
                });
            }
        }
    }

    sentry::add_breadcrumb(sentry::Breadcrumb {
        category: Some("s3_cleanup".into()),
        message: Some(format!(
            "Scan complete. Scanned {} objects, found {} potential orphans ({} from decks), {} invalid keys, {} prefix mismatches, {} orphaned deck prefixes",
            total_s3_objects_scanned,
            orphaned_s3_keys.len(),
            orphaned_deck_object_count,
            invalid_key_format_count,
            prefix_mismatch_count,
            orphaned_deck_prefix_count
        )),
        level: sentry::Level::Info,
        ..Default::default()
    });

    if dry_run {
        sentry::add_breadcrumb(sentry::Breadcrumb {
            category: Some("s3_cleanup".into()),
            message: Some(format!(
                "[DRY RUN] Would delete {} orphaned S3 objects",
                orphaned_s3_keys.len()
            )),
            level: sentry::Level::Info,
            ..Default::default()
        });

        println!(
            "[DRY RUN] Would delete {} orphaned S3 objects and {} deleted decks",
            orphaned_s3_keys.len(),
            orphaned_deck_prefix_count
        );
        return Ok(0);
    } else {
        let mut total_deleted_count = 0;
        let mut deletion_had_errors = false;

        for keys_chunk in orphaned_s3_keys.chunks(1000) {
            if keys_chunk.is_empty() {
                continue;
            } // Should not happen, but safe check

            let objects_to_delete: Vec<ObjectIdentifier> = keys_chunk
                .iter()
                .map(|k| {
                    ObjectIdentifier::builder()
                        .key(k)
                        .build()
                        .expect("Key must be valid UTF-8")
                }) // Build ObjectIdentifier for each key
                .collect();

            let delete_builder = Delete::builder()
                .set_objects(Some(objects_to_delete)) // Use set_objects for Vec<ObjectIdentifier>
                .quiet(false); // Set quiet=false to get results for each key

            let delete_request = delete_builder
                .build()
                .expect("Delete request structure is valid");

            sentry::add_breadcrumb(sentry::Breadcrumb {
                category: Some("s3_cleanup".into()),
                message: Some(format!(
                    "Attempting to delete batch of {} objects",
                    keys_chunk.len()
                )),
                level: sentry::Level::Info,
                ..Default::default()
            });

            match s3_ops::delete_objects(state.as_ref(), MEDIA_BUCKET.as_str(), delete_request).await {
                Ok(output) => {
                    if let Some(deleted_objects) = output.deleted {
                        let batch_deleted_count = deleted_objects.len();
                        sentry::add_breadcrumb(sentry::Breadcrumb {
                            category: Some("s3_cleanup".into()),
                            message: Some(format!(
                                "Successfully deleted batch of {} objects",
                                batch_deleted_count
                            )),
                            level: sentry::Level::Info,
                            ..Default::default()
                        });
                        total_deleted_count += batch_deleted_count;
                    }
                    if let Some(errors) = output.errors {
                        deletion_had_errors = true; // Mark that at least one error occurred
                        for error in errors {
                            sentry::capture_message(
                                &format!(
                                    "S3 cleanup: Failed to delete key '{}': Code={}, Message={}",
                                    error.key.as_deref().unwrap_or("N/A"),
                                    error.code.as_deref().unwrap_or("N/A"),
                                    error.message.as_deref().unwrap_or("N/A")
                                ),
                                sentry::Level::Warning,
                            );
                        }
                    }
                }
                Err(err) => {
                    // Error sending the entire batch delete request
                    deletion_had_errors = true;
                    sentry::capture_message(
                        &format!("S3 cleanup: Failed to send batch delete request: {err:?}"),
                        sentry::Level::Error,
                    );
                }
            }
        } // end batch deletion loop

        sentry::add_breadcrumb(sentry::Breadcrumb {
            category: Some("s3_cleanup".into()),
            message: Some(format!(
                "Orphan cleanup finished. Total objects deleted: {}",
                total_deleted_count
            )),
            level: sentry::Level::Info,
            ..Default::default()
        });

        // Return an error if any part of the deletion failed
        if deletion_had_errors {
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error during deletion".to_string(),
            ))
        } else {
            Ok(total_deleted_count)
        }
    }
}

// Housekeeping: Delete orphaned media files based on the postgres table
pub async fn cleanup_orphaned_media(state: Arc<AppState>) -> Result<(), (StatusCode, String)> {
    //info!("Starting orphaned media cleanup job");

    let mut db_client = state.db_pool.get().await.map_err(|err| {
        sentry::capture_message(
            &format!("Media cleanup: Failed to get database connection: {}", err),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Error getting database connection".to_string(),
        )
    })?;

    let tx = db_client.transaction().await.map_err(|err| {
        sentry::capture_message(
            &format!("Media cleanup: Failed to start transaction: {}", err),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Database error".to_string(),
        )
    })?;

    // Find orphaned media files (not referenced in media_references)
    let orphaned_files = tx
        .query(
            "SELECT id, hash FROM media_files m
         WHERE NOT EXISTS (SELECT 1 FROM media_references r WHERE r.media_id = m.id)",
            &[],
        )
        .await
        .map_err(|err| {
            sentry::capture_message(
                &format!(
                    "Media cleanup: Failed to query orphaned media files: {}",
                    err
                ),
                sentry::Level::Error,
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error".to_string(),
            )
        })?;

    //println!("Found {} orphaned media files to delete", orphaned_files.len());

    for row in orphaned_files {
        let id: i64 = row.get(0);
        let hash: String = row.get(1);

        let prefix = &hash[0..2];
        let s3_key = format!("{prefix}/{hash}");

        // Delete from database first (within transaction). This prevents the
        // situation where S3 deletion succeeds but the DB delete fails and we
        // end up with a database row pointing to a missing object.
        match tx
            .execute("DELETE FROM media_files WHERE id = $1", &[&id])
            .await
        {
            Ok(_) => {
                // Attempt to delete from S3. If this fails, we log and continue;
                // the S3 object can be retried/cleaned up later. Deleting the DB
                // row first avoids the observed "DB has entries but S3 is missing" case.
                if let Err(e) = s3_ops::delete_object(state.as_ref(), MEDIA_BUCKET.as_str(), &s3_key).await {
                    sentry::capture_message(
                        &format!(
                            "Media cleanup S3 deletion failed: hash={}, s3_key={}, err={:?}",
                            hash, s3_key, e
                        ),
                        sentry::Level::Warning,
                    );
                    // Don't attempt to roll back DB here; we'll rely on periodic cleanup later.
                }
            }
            Err(e) => {
                sentry::capture_message(
                    &format!(
                        "Media cleanup: Failed to delete media file {} from database: {}",
                        hash, e
                    ),
                    sentry::Level::Error,
                );
                // If DB delete fails, do not delete S3 object to avoid ending up
                // with a missing DB entry and an S3 object that might still be referenced.
                continue;
            }
        }
    }

    tx.commit().await.map_err(|err| {
        sentry::capture_message(
            &format!("Media cleanup: Failed to commit transaction: {}", err),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal error".to_string(),
        )
    })?;

    //info!("Completed orphaned media cleanup job");
    Ok(())
}

// Spawn a background task to periodically clean up orphaned media
pub async fn start_cleanup_task(state: Arc<AppState>) {
    let cleanup_interval = Duration::hours(4).to_std().unwrap();

    let orphan_clone = state.clone();
    tokio::spawn(async move {
        let mut interval = time::interval(cleanup_interval);

        loop {
            interval.tick().await;

            if let Err(e) = cleanup_orphaned_media(orphan_clone.clone()).await {
                sentry::capture_message(
                    &format!("Media cleanup task failed: {:?}", e),
                    sentry::Level::Error,
                );
            }
        }
    });

    //info!("Media cleanup task scheduled");

    // Add bulk upload cleanup
    let bulk_state = state;
    tokio::spawn(async move {
        let mut interval = time::interval(tokio::time::Duration::from_secs(3600 * 24)); // 24 hour

        loop {
            interval.tick().await;

            if let Ok(client) = bulk_state.db_pool.get().await {
                match client.query(
                    "SELECT id, metadata FROM media_bulk_uploads WHERE created_at < NOW() - INTERVAL '1 day'",
                    &[]
                ).await {
                    Ok(rows) => {
                        for row in rows {
                            // Check if there are any media references for this upload, delete all files that have no notes associated with them
                            let upload_id: Uuid = row.get(0);
                            let metadata: serde_json::Value = row.get(1);
                            let files: Vec<MediaMissingFile> = serde_json::from_value(metadata).unwrap();

                            let mut hash_vec = Vec::new();
                            for file in &files {
                                hash_vec.push(file.hash.clone());
                            }

                            let mut files_to_delete = Vec::new();
                            let mut files_to_keep = Vec::new();
                            let media_ids = client.query(
                                "SELECT id, hash FROM media_files WHERE hash = ANY($1)",
                                &[&hash_vec]
                            ).await.unwrap();

                            for row in media_ids {
                                let id: i64 = row.get(0);
                                let hash: String = row.get(1);
                                let note_id = client.query_opt(
                                    "SELECT id FROM media_references WHERE media_id = $1 LIMIT 1",
                                    &[&id]
                                ).await.unwrap();
                                if note_id.is_none() {
                                    files_to_delete.push(hash);
                                } else {
                                    files_to_keep.push(hash);
                                }
                            }

                            // Find orphans that dont exist anywhere in the database but are somehow related to this bulk
                            for file in &files {
                                if !files_to_keep.contains(&file.hash) && !files_to_delete.contains(&file.hash) {
                                    files_to_delete.push(file.hash.clone());
                                }
                            }

                            // Delete the upload record
                            let _ = client.execute(
                                "DELETE FROM media_bulk_uploads WHERE id = $1",
                                &[&upload_id]
                            ).await;

                            if files_to_delete.is_empty() {
                                continue;
                            }

                            // Delete the files from the database
                            let _ = client.execute(
                                "DELETE FROM media_files WHERE hash = ANY($1)",
                                &[&files_to_delete]
                            ).await;

                            // Delete the files from S3
                            for hash in files_to_delete {
                                let prefix = &hash[0..2];
                                let s3_key = format!("{prefix}/{hash}");
                                let _ = s3_ops::delete_object(
                                    bulk_state.as_ref(),
                                    MEDIA_BUCKET.as_str(),
                                    &s3_key,
                                )
                                .await;
                            }

                        }
                    },
                    Err(e) => {
                        sentry::capture_message(
                            &format!("Bulk upload cleanup: Error cleaning up expired bulk uploads: {}", e),
                            sentry::Level::Error,
                        );
                    }
                }
            }
        }
    });
}

pub async fn check_media_bulk(
    State(state): State<Arc<AppState>>,
    client_ip: ClientIp,
    Json(req): Json<MediaBulkCheckRequest>,
) -> Result<Json<MediaBulkCheckResponse>, (StatusCode, String)> {
    let user_id = auth::get_user_from_token(&state, &req.token)
        .await
        .map_err(|_| {
            sentry::add_breadcrumb(sentry::Breadcrumb {
                category: Some("media_upload".into()),
                message: Some("Failed to authenticate user token".to_string()),
                level: sentry::Level::Warning,
                ..Default::default()
            });
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error authenticating user".to_string(),
            )
        })?;

    if user_id == 0 {
        // Expected auth failure - don't log to Sentry (4xx is expected)
        return Err((StatusCode::UNAUTHORIZED, "Invalid token".to_string()));
    }
    let ip_address = client_ip.0;

    sentry::configure_scope(|scope| {
        scope.set_user(Some(sentry::User {
            id: Some(user_id.to_string()),
            ..Default::default()
        }));
        scope.set_tag("deck_hash", &req.deck_hash);
    });

    // Check if request is valid
    if req.files.is_empty() {
        // Expected user input error - client sent empty file list
        return Err((StatusCode::BAD_REQUEST, "No files provided".to_string()));
    }

    if req.files.len() > 100 {
        // Expected user input error - client sent too many files
        return Err((
            StatusCode::BAD_REQUEST,
            "Too many files in single request".to_string(),
        ));
    }

    let mut invalid_files = Vec::new();
    let mut valid_files = Vec::new();
    // basic check if the files are valid with is_allowed_extension
    for file in req.files {
        if is_allowed_extension_with_logging(&file.filename, user_id as i64) {
            valid_files.push(file);
        } else {
            invalid_files.push(file);
        }
    }
    
    // Log summary if any files were rejected due to extension/filename validation
    if !invalid_files.is_empty() {
        sentry::capture_message(
            &format!(
                "[MEDIA] {} files rejected (filename validation) for user {}, deck {}",
                invalid_files.len(), user_id, req.deck_hash
            ),
            sentry::Level::Info,
        );
    }

    let mut db_client = state.db_pool.get().await.map_err(|err| {
        sentry::capture_message(
            &format!(
                "check_media_bulk: Failed to get database connection for user {}: {}",
                user_id, err
            ),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal error".to_string(),
        )
    })?;

    let tx = db_client.transaction().await.map_err(|err| {
        sentry::capture_message(
            &format!(
                "check_media_bulk: Failed to start transaction for user {}: {}",
                user_id, err
            ),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal error".to_string(),
        )
    })?;

    // Get all note IDs that are in the deck or its descendants,
    // OR are part of ongoing bulk operations
    let note_guids: Vec<&str> = valid_files
        .iter()
        .map(|file| file.note_guid.as_str())
        .collect();

    let bulk_operation_uuid = if let Some(bulk_id_str) = &req.bulk_operation_id {
        match Uuid::parse_str(bulk_id_str) {
            Ok(uuid) => Some(uuid),
            Err(_) => None,
        }
    } else {
        None
    };

    let note_ids = tx
        .query(
            "WITH RECURSIVE deck_tree AS (
                SELECT id, parent
                FROM decks
                WHERE human_hash = $1            
                UNION ALL
                SELECT d.id, d.parent
                FROM decks d
                INNER JOIN deck_tree dt ON dt.id = d.parent
            )
            SELECT n.id, n.guid
            FROM notes n
            INNER JOIN deck_tree dt ON n.deck = dt.id
            WHERE n.deleted = false AND n.guid = ANY($2)",
            &[&req.deck_hash, &note_guids],
        )
        .await
        .map_err(|err| {
            sentry::capture_message(
                &format!(
                    "check_media_bulk: Failed to query notes for user {} deck {}: {}",
                    user_id, req.deck_hash, err
                ),
                sentry::Level::Error,
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error".to_string(),
            )
        })?;

    // Create a HashMap to store guid -> id mapping
    // Note: IDs of -1 indicate notes from ongoing bulk operations (not yet inserted)
    let mut note_id_map: HashMap<String, i64> = HashMap::new();
    for row in note_ids {
        let id: i64 = row.get(0);
        let guid: String = row.get(1);
        note_id_map.insert(guid, id);
    }

    // Add bulk operation note GUIDs from cache if applicable
    if let Some(bulk_uuid) = bulk_operation_uuid {
        let cache = state.bulk_operations.read().await;
        if let Some(info) = cache.get(&bulk_uuid) {
            for guid in &info.note_guids {
                if note_guids.contains(&guid.as_str()) && !note_id_map.contains_key(guid) {
                    note_id_map.insert(guid.clone(), -1); // -1 indicates bulk operation note
                }
            }
        }
    }

    // Get a list of all file hashes for checking existence
    let hashes: Vec<String> = valid_files.iter().map(|file| file.hash.clone()).collect();

    // Check which files already exist in the database
    let existing_files = tx
        .query(
            "SELECT id, hash FROM media_files WHERE hash = ANY($1)",
            &[&hashes],
        )
        .await
        .map_err(|err| {
            sentry::capture_message(
                &format!(
                    "check_media_bulk: Failed to query existing media files for user {}: {}",
                    user_id, err
                ),
                sentry::Level::Error,
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error".to_string(),
            )
        })?;

    // Create a map of hash -> media_id for existing files
    let mut existing_media_ids_map = HashMap::new();
    for row in existing_files {
        let id: i64 = row.get(0);
        let hash: String = row.get(1);
        existing_media_ids_map.insert(hash, id);
    }

    // Collect note_id and filename pairs for files that already exist and have valid notes
    let mut existing_file_note_pairs: Vec<(i64, String)> = Vec::new();
    for (hash, _media_id) in &existing_media_ids_map {
        for file in &valid_files {
            if file.hash == *hash {
                if let Some(&note_id) = note_id_map.get(&file.note_guid) {
                    // Skip bulk operation notes (ID -1) as they don't exist in DB yet
                    if note_id != -1 {
                        existing_file_note_pairs.push((note_id, file.filename.clone()));
                    }
                }
            }
        }
    }

    // we need a map of (note_id, filename) of all valid files to resolve ownership
    let nid_filename_map = valid_files.iter().filter_map(|file| {
        if let Some(&note_id) = note_id_map.get(&file.note_guid) {
            // Skip bulk operation notes (ID -1) as they don't exist in DB yet
            if note_id != -1 {
                Some((note_id, file.filename.clone()))
            } else {
                None
            }
        } else {
            None
        }
    }).collect::<Vec<(i64, String)>>();

    // Resolve ownership for inherited fields: if a file is referenced through an inherited field,
    // the media reference should be created for the base note (source of truth)
    let ownership_map = if !nid_filename_map.is_empty() {
        media_reference_manager::resolve_media_owners_batch_tx(&tx, &nid_filename_map)
            .await
            .map_err(|err| {
                sentry::capture_message(
                    &format!(
                        "check_media_bulk: Failed to resolve media ownership for user {}: {}",
                        user_id, err
                    ),
                    sentry::Level::Error,
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error".to_string(),
                )
            })?
    } else {
        HashMap::new()
    };

    // Insert references for existing files using resolved ownership
    for (hash, media_id) in &existing_media_ids_map {
        for file in &valid_files {
            if file.hash == *hash {
                let try_note_id = note_id_map.get(&file.note_guid);
                if try_note_id.is_none() {
                    continue;
                }
                let note_id = *try_note_id.unwrap();

                // Skip bulk operation notes (ID -1) as they don't exist in DB yet
                if note_id == -1 {
                    continue;
                }

                // Use resolved owner (may be base note for inherited fields)
                let owner_note_id = ownership_map
                    .get(&(note_id, file.filename.clone()))
                    .copied()
                    .unwrap_or(note_id);

                tx.execute(
                    "INSERT INTO media_references (media_id, note_id, file_name) VALUES ($1, $2, $3) 
                     ON CONFLICT (media_id, note_id) DO NOTHING",
                    &[&media_id, &owner_note_id, &file.filename],
                )
                .await
                .map_err(|err| {
                    sentry::capture_message(
                        &format!(
                            "check_media_bulk: Failed to insert media reference for user {} file {}: {}",
                            user_id, file.filename, err
                        ),
                        sentry::Level::Error,
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Internal error".to_string(),
                    )
                })?;
            }
        }
    }

    // Prepare lists of existing and missing files
    let mut existing_files_response = Vec::new();
    let mut missing_files_response = Vec::new();

    let mut invalid_hash_set = HashSet::new();

    // Calculate total potential storage cost
    let total_potential_bytes: u64 = valid_files.iter()
    .map(|f| // file size of the file.hash not in existing_media_ids_map
        if existing_media_ids_map.contains_key(&f.hash) {
            0
        } else {
            f.file_size as u64
        })
    .sum();
    let potential_file_count = valid_files.len() - existing_media_ids_map.len();

    // check user-specific quota
    if !state
        .rate_limiter
        .check_user_upload_allowed(
            user_id,
            ip_address,
            potential_file_count as u32,
            total_potential_bytes,
        )
        .await
    {
        sentry::capture_message(
            &format!(
                "User {} exceeded upload quota: {} files, {} bytes",
                user_id, potential_file_count, total_potential_bytes
            ),
            sentry::Level::Warning,
        );
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "You reached the upload limit. Please try again tomorrow.".to_string(),
        ));
    }

    // Generate presigned URLs for each missing file
    for file in &valid_files {
        if let Some(&media_id) = existing_media_ids_map.get(&file.hash) {
            existing_files_response.push(MediaExistingFile {
                hash: file.hash.clone(),
                media_id,
            });
        } else {
            let note_id = if let Some(&nid) = note_id_map.get(&file.note_guid) {
                if nid == -1 {
                    // Bulk operation note; use placeholder ID
                    -1
                } else {
                    ownership_map // Get true owner for the file (differs for inherited notes)
                        .get(&(nid, file.filename.clone()))
                        .copied()
                        .unwrap_or(nid)
                }
            } else {
                invalid_hash_set.insert(file.hash.clone());
                sentry::add_breadcrumb(sentry::Breadcrumb {
                    category: Some("media_upload".into()),
                    message: Some(format!(
                        "Note ID not found for file {} (note_guid: {}, user: {})",
                        file.filename, file.note_guid, user_id
                    )),
                    level: sentry::Level::Warning,
                    ..Default::default()
                });
                continue; // Skip if note ID is not found
            };

            // Note is valid if it exists in DB (note_id > 0) or is part of bulk operation (note_id == -1)

            if file.file_size <= 0 || file.file_size > MAX_FILE_SIZE_BYTES as i64 {
                invalid_hash_set.insert(file.hash.clone());
                sentry::add_breadcrumb(sentry::Breadcrumb {
                    category: Some("media_upload".into()),
                    message: Some(format!(
                        "Invalid file size for {} (size: {}, user: {})",
                        file.filename, file.file_size, user_id
                    )),
                    level: sentry::Level::Warning,
                    ..Default::default()
                });
                continue; // Skip if file size is invalid
            }

            if determine_content_type_by_name(&file.filename).is_none() {
                invalid_hash_set.insert(file.hash.clone());
                sentry::add_breadcrumb(sentry::Breadcrumb {
                    category: Some("media_upload".into()),
                    message: Some(format!(
                        "Invalid content type for {} (user: {})",
                        file.filename, user_id
                    )),
                    level: sentry::Level::Warning,
                    ..Default::default()
                });
                continue; // Skip if content type is invalid
            }

            // Log attempted file upload
            media_logger::log_media_operation(
                &tx,
                MediaOperationType::Upload,
                Some(user_id),
                ip_address,
                Some(file.hash.clone()),
                Some(file.filename.clone()),
                Some(file.file_size),
            )
            .await
            .unwrap();

            let missing_file = MediaMissingFile {
                hash: file.hash.clone(),
                filename: file.filename.clone(),
                note_id: note_id,
                file_size: file.file_size, // Store the file size
                upload_url: None,
            };

            missing_files_response.push(missing_file);
        }
    }

    for file in &valid_files {
        if invalid_hash_set.contains(&file.hash) {
            invalid_files.push(file.clone());
        }
    }
    valid_files.retain(|file| !invalid_hash_set.contains(&file.hash));

    // Store metadata about missing files for later confirmation
    let mut batch_id = None;
    if !missing_files_response.is_empty() {
        let batch_uuid = Uuid::new_v4();
        let batch_id_string = batch_uuid.to_string();
        let base_url_trimmed = state.media_base_url.as_ref().trim_end_matches('/');
        let deck_hash_for_tokens = req.deck_hash.clone();

        for missing_file in &mut missing_files_response {
            let upload_token = state
                .media_token_service
                .generate_upload_token(UploadTokenParams {
                    hash: missing_file.hash.clone(),
                    user_id,
                    note_id: missing_file.note_id,
                    filename: missing_file.filename.clone(),
                    file_size: missing_file.file_size,
                    deck_hash: deck_hash_for_tokens.clone(),
                    batch_id: Some(batch_id_string.clone()),
                })
                .map_err(|err| {
                    sentry::capture_message(
                        &format!(
                            "check_media_bulk: Failed to generate upload token for {} (user {}, deck {}): {}",
                            missing_file.hash, user_id, req.deck_hash, err
                        ),
                        sentry::Level::Error,
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Error generating upload link".to_string(),
                    )
                })?;

            missing_file.upload_url = Some(format!(
                "{}/v1/media/{}?token={}",
                base_url_trimmed, missing_file.hash, upload_token
            ));
        }

        // Convert to JSON
        let batch_metadata = serde_json::to_value(&missing_files_response).map_err(|err| {
            sentry::capture_message(
                &format!(
                    "check_media_bulk: Failed to serialize metadata for user {}: {}",
                    user_id, err
                ),
                sentry::Level::Error,
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error processing request".to_string(),
            )
        })?;

        // Store in database with expiration
        tx.execute(
            "INSERT INTO media_bulk_uploads (id, metadata, created_at) VALUES ($1, $2, NOW())",
            &[&batch_uuid, &batch_metadata],
        )
        .await
        .map_err(|err| {
            sentry::capture_message(
                &format!(
                    "check_media_bulk: Failed to store bulk upload metadata for user {}: {}",
                    user_id, err
                ),
                sentry::Level::Error,
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error storing upload metadata".to_string(),
            )
        })?;

        batch_id = Some(batch_uuid);
    }

    tx.commit().await.map_err(|err| {
        sentry::capture_message(
            &format!(
                "check_media_bulk: Failed to commit transaction for user {}: {}",
                user_id, err
            ),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Database commit failed".to_string(),
        )
    })?;

    if batch_id.is_none() {
        return Ok(Json(MediaBulkCheckResponse {
            existing_files: existing_files_response,
            missing_files: missing_files_response,
            failed_files: invalid_files,
            batch_id: None,
        }));
    }
    let batch_id_str = Some(batch_id.unwrap().to_string());

    // If there are no missing files, just return existing ones
    Ok(Json(MediaBulkCheckResponse {
        existing_files: existing_files_response,
        missing_files: missing_files_response,
        failed_files: invalid_files,
        batch_id: batch_id_str,
    }))
}

// Helper function to wait for bulk operation note insertion completion
async fn wait_for_bulk_operation_completion(
    state: &Arc<AppState>,
    bulk_operation_id: &str,
) -> Result<(), (StatusCode, String)> {
    // If bulk operation ID is empty, proceed immediately as if note insertion is done
    if bulk_operation_id.is_empty() {
        return Ok(());
    }

    let bulk_uuid = Uuid::parse_str(bulk_operation_id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "Invalid bulk operation ID".to_string(),
        )
    })?;

    let start_time = std::time::Instant::now();
    let max_wait_duration = std::time::Duration::from_secs(60);
    let poll_interval = std::time::Duration::from_secs(2);

    loop {
        if start_time.elapsed() > max_wait_duration {
            sentry::capture_message(
                &format!(
                    "Timeout waiting for note insertion for bulk operation: {}",
                    bulk_uuid
                ),
                sentry::Level::Warning,
            );
            break;
        }

        // Check if note insertion is complete using the cache
        {
            let cache = state.bulk_operations.read().await;
            if let Some(info) = cache.get(&bulk_uuid) {
                if info.note_insertion_done {
                    break;
                }
            } else {
                sentry::add_breadcrumb(sentry::Breadcrumb {
                    category: Some("media_upload".into()),
                    message: Some(format!("Bulk operation not found in cache: {}", bulk_uuid)),
                    level: sentry::Level::Warning,
                    ..Default::default()
                });
                break; // Operation doesn't exist, proceed anyway
            }
        }

        // Wait before next poll
        tokio::time::sleep(poll_interval).await;
    }

    Ok(())
}

// Helper function to retrieve bulk upload metadata
async fn retrieve_bulk_upload_metadata(
    state: &Arc<AppState>,
    batch_id_uuid: Uuid,
) -> Result<Vec<MediaMissingFile>, (StatusCode, String)> {
    let db_client = state.db_pool.get().await.map_err(|err| {
        sentry::capture_message(
            &format!(
                "retrieve_bulk_upload_metadata: Failed to get database connection for batch {}: {}",
                batch_id_uuid, err
            ),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal error".to_string(),
        )
    })?;

    // Retrieve the bulk upload metadata
    let metadata_row = db_client
        .query_opt(
            "SELECT metadata FROM media_bulk_uploads WHERE id = $1",
            &[&batch_id_uuid],
        )
        .await
        .map_err(|err| {
            sentry::capture_message(
                &format!(
                    "retrieve_bulk_upload_metadata: Failed to query metadata for batch {}: {}",
                    batch_id_uuid, err
                ),
                sentry::Level::Error,
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Error retrieving upload metadata".to_string(),
            )
        })?;

    let metadata_json: serde_json::Value = match metadata_row {
        Some(row) => row.get(0),
        None => {
            sentry::add_breadcrumb(sentry::Breadcrumb {
                category: Some("media_upload".into()),
                message: Some(format!("Batch not found or expired: {}", batch_id_uuid)),
                level: sentry::Level::Warning,
                ..Default::default()
            });
            return Err((
                StatusCode::NOT_FOUND,
                "Batch not found or expired".to_string(),
            ));
        }
    };

    serde_json::from_value(metadata_json).map_err(|err| {
        sentry::capture_message(
            &format!(
                "retrieve_bulk_upload_metadata: Failed to parse metadata for batch {}: {}",
                batch_id_uuid, err
            ),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Error parsing upload metadata".to_string(),
        )
    })
}

// Helper function to get bulk operation info
async fn get_bulk_operation_info(
    state: &Arc<AppState>,
    bulk_operation_id: &str,
) -> Result<(Option<String>, Vec<String>), (StatusCode, String)> {
    let bulk_uuid = Uuid::parse_str(bulk_operation_id).map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "Invalid bulk operation ID".to_string(),
        )
    })?;

    let cache = state.bulk_operations.read().await;
    if let Some(info) = cache.get(&bulk_uuid) {
        Ok((Some(info.deck_hash.clone()), info.note_guids.clone()))
    } else {
        Ok((None, Vec::new()))
    }
}

#[allow(dead_code)]
#[derive(Clone)]
struct ValidatedFile {
    hash: String,
    actual_size: i64,
    note_id: i64,
    filename: String,
}

// Helper function to escape LIKE patterns
fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

async fn build_filename_to_note_ids_mapping(
    tx: &tokio_postgres::Transaction<'_>,
    unknown_note_id_files: &[ValidatedFile],
    deck_hash: &Option<String>,
    all_note_guids: &[String],
) -> Result<HashMap<String, Vec<i64>>, (StatusCode, String)> {
    // Gather all note ids based on the guids and deck hash from database with a recursive query wit the topmost human_hash being the deck_hash
    let note_ids = tx
        .query(
            "WITH RECURSIVE deck_tree AS (
            SELECT id, parent
            FROM decks
            WHERE human_hash = $1            
            UNION ALL
            SELECT d.id, d.parent
            FROM decks d
            INNER JOIN deck_tree dt ON dt.id = d.parent
        )
        SELECT n.id
        FROM notes n
        INNER JOIN deck_tree dt ON n.deck = dt.id
        WHERE n.deleted = false AND n.guid = ANY($2)",
            &[&deck_hash, &all_note_guids],
        )
        .await
        .map_err(|err| {
            sentry::capture_message(
                &format!(
                    "build_filename_to_note_ids_mapping: Failed to query notes for deck {:?}: {}",
                    deck_hash, err
                ),
                sentry::Level::Error,
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error".to_string(),
            )
        })?;

    let note_id_map: Vec<i64> = note_ids.iter().map(|row| row.get::<_, i64>(0)).collect();

    // Find all the fields in table fields that contain any of the unknown_note_id_files file.filename and are in the note_ids
    let mut note_fields_map: HashMap<i64, Vec<String>> = HashMap::new();

    if !unknown_note_id_files.is_empty() && !note_id_map.is_empty() {
        // Collect all filenames for batch processing
        let filenames: Vec<String> = unknown_note_id_files
            .iter()
            .map(|f| escape_like(&f.filename))
            .collect();

        // Use single query with ANY to check all filenames at once
        let query = "
            SELECT f.note, f.content, uf.filename
            FROM fields f
            CROSS JOIN unnest($1::text[]) AS uf(filename)
            WHERE f.note = ANY($2) 
            AND f.content LIKE '%' || uf.filename || '%'";

        let rows = tx.query(query, &[&filenames, &note_id_map]).await;
        match rows {
            Ok(rows) => {
                for row in rows {
                    let note_id: i64 = row.get(0);
                    let field: String = row.get(1);
                    note_fields_map.entry(note_id).or_default().push(field);
                }
            }
            Err(e) => {
                sentry::add_breadcrumb(sentry::Breadcrumb {
                    category: Some("media_upload".into()),
                    message: Some(format!(
                        "Error fetching note fields with optimized query, falling back: {}",
                        e
                    )),
                    level: sentry::Level::Warning,
                    ..Default::default()
                });
                // Fallback to original method if optimized query fails
                for file_to_insert in unknown_note_id_files {
                    let query = "SELECT note, content FROM fields WHERE content LIKE $1 ESCAPE '\\' AND note = ANY($2)";
                    if let Ok(rows) = tx
                        .query(
                            query,
                            &[
                                &format!("%{}%", escape_like(&file_to_insert.filename)),
                                &note_id_map,
                            ],
                        )
                        .await
                    {
                        for row in rows {
                            let note_id: i64 = row.get(0);
                            let field: String = row.get(1);
                            note_fields_map.entry(note_id).or_default().push(field);
                        }
                    }
                }
            }
        }
    }

    // Create a hashmap note_id -> Media references.
    let mut note_references: HashMap<i64, HashSet<String>> = HashMap::new();
    for (note_id, fields) in note_fields_map {
        for field in fields {
            let references = media_reference_manager::extract_media_references(&field);
            note_references
                .entry(note_id)
                .or_default()
                .extend(references);
        }
    }

    // Resolve ownership for inherited fields: if a file is referenced through an inherited field,
    // the media reference should be created for the base note (source of truth)
    // Build list of (note_id, filename) pairs for batch resolution
    let note_file_pairs: Vec<(i64, String)> = note_references
        .iter()
        .flat_map(|(note_id, filenames)| {
            filenames.iter().map(move |filename| (*note_id, filename.clone()))
        })
        .collect();
    
    // Batch resolve ownership
    let ownership_map = media_reference_manager::resolve_media_owners_batch_tx(tx, &note_file_pairs)
        .await
        .map_err(|err| {
            sentry::capture_message(
                &format!(
                    "build_filename_to_note_ids_mapping: Failed to resolve media ownership: {}",
                    err
                ),
                sentry::Level::Error,
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error".to_string(),
            )
        })?;

    // Invert it to filename -> Vec<note_id>, using resolved ownership
    let mut filename_to_note_ids: HashMap<String, Vec<i64>> = HashMap::new();
    for (note_id, references) in note_references {
        for filename in references {
            // Use resolved owner if available, otherwise use original note_id
            let owner_note_id = ownership_map
                .get(&(note_id, filename.clone()))
                .copied()
                .unwrap_or(note_id);
            
            filename_to_note_ids
                .entry(filename)
                .or_default()
                .push(owner_note_id);
        }
    }

    Ok(filename_to_note_ids)
}

pub async fn confirm_media_bulk_upload(
    state: Arc<AppState>,
    req: MediaBulkConfirmRequest,
) -> Result<Json<MediaBulkConfirmResponse>, (StatusCode, String)> {
    if req.confirmed_files.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "Invalid Format".to_string()));
    }

    // If this is part of a bulk operation, wait for note insertion to complete
    if let Some(bulk_id_str) = &req.bulk_operation_id {
        wait_for_bulk_operation_completion(&state, bulk_id_str).await?;
    }

    let batch_id_uuid = Uuid::parse_str(&req.batch_id)
        .map_err(|_| (StatusCode::BAD_REQUEST, "Invalid batch ID".to_string()))?;

    // Retrieve bulk upload metadata
    let files = retrieve_bulk_upload_metadata(&state, batch_id_uuid).await?;

    // Find files that client claims were uploaded
    let confirmed_hashes: HashSet<String> = req.confirmed_files.into_iter().collect();
    let confirmed_files: Vec<&MediaMissingFile> = files
        .iter()
        .filter(|f| confirmed_hashes.contains(&f.hash) && is_allowed_extension(&f.filename))
        .collect();

    if confirmed_files.is_empty() {
        return Ok(Json(MediaBulkConfirmResponse {
            processed_files: vec![],
        }));
    }
    const VALIDATION_CONCURRENCY: usize = 32;

    let validation_semaphore = Arc::new(Semaphore::new(VALIDATION_CONCURRENCY));

    let validation_tasks: Vec<_> = confirmed_files.iter().map(|file| {
        let validation_semaphore = validation_semaphore.clone();
        let state = state.clone();
        let file = *file;
        
        async move {
            let _permit = validation_semaphore.acquire().await.unwrap();
            
            let prefix = &file.hash[0..2];
            let s3_key = format!("{}/{}", prefix, file.hash);
            
            let head_check = match s3_ops::head_object(&state, MEDIA_BUCKET.as_str(), &s3_key).await {
                Ok(head_response) => {
                    let actual_size = head_response.content_length.unwrap_or(0);
                    let actual_size_f64 = actual_size as f64;
                    let file_size_f64 = file.file_size as f64;
                    
                    // Validate file size (within 1% tolerance)
                    if actual_size > MAX_FILE_SIZE_BYTES as i64 
                        || actual_size == 0
                        || (actual_size_f64 > file_size_f64 * 1.01)
                        || (actual_size_f64 < file_size_f64 * 0.99)
                    {
                        Some("File size mismatch or invalid".to_string())
                    } else {
                        None // Size is valid, proceed to content validation
                    }
                }
                Err(_) => {
                    Some("File not found in storage".to_string())
                }
            };
            
            // If head check failed, skip content download
            if let Some(error_msg) = head_check {
                // Delete invalid file from S3
                let _ = s3_ops::delete_object(&state, MEDIA_BUCKET.as_str(), &s3_key).await;
                
                return Err(MediaProcessedFile {
                    hash: file.hash.clone(),
                    media_id: 0,
                    success: false,
                    error: Some(error_msg),
                });
            }
            
            // Head check passed, now download and validate file content
            let validation_result = match s3_ops::get_object(&state, MEDIA_BUCKET.as_str(), &s3_key).await {
                Ok(output) => {
                    // Validate the file content (signature and hash)
                    match validate_media_stream(output.body, &file.hash).await {
                        Ok(ContentValidationOutcome::Valid) => {
                            // File is valid
                            Ok(file)
                        }
                        Ok(ContentValidationOutcome::Invalid { reason, mime }) => {
                            let anon_filename = anonymize_filename(&file.filename);
                            sentry::with_scope(
                                |scope| {
                                    // Group all file validation failures together
                                    scope.set_fingerprint(Some(["file-validation-failed"].as_ref()));
                                    scope.set_extra("hash", file.hash.clone().into());
                                    scope.set_extra("filename_anon", anon_filename.clone().into());
                                    scope.set_extra("file_extension", file.filename.rsplit('.').next().unwrap_or("").into());
                                    scope.set_extra("validation_reason", reason.into());
                                    if let Some(mime_str) = mime.as_ref() {
                                        scope.set_extra("detected_mime", mime_str.clone().into());
                                    }
                                    scope.set_tag("validation_type", "content_mismatch");
                                },
                                || {
                                    // This is a BUG - file validation should not fail in normal operation
                                    sentry::capture_message(
                                        &format!(
                                            "[BUG] File validation failed: hash={}, filename={}, reason={}, mime={}",
                                            file.hash,
                                            anon_filename,
                                            reason,
                                            mime.as_deref().unwrap_or("unknown")
                                        ),
                                        sentry::Level::Error,
                                    );
                                }
                            );
                            Err(format!("File content validation failed ({})", reason))
                        }
                        Err(e) => {
                            let anon_filename = anonymize_filename(&file.filename);
                            sentry::with_scope(
                                |scope| {
                                    // Group validation errors together
                                    scope.set_fingerprint(Some(["file-validation-error"].as_ref()));
                                    scope.set_extra("hash", file.hash.clone().into());
                                    scope.set_extra("filename_anon", anon_filename.clone().into());
                                    scope.set_extra("file_extension", file.filename.rsplit('.').next().unwrap_or("").into());
                                    scope.set_extra("error_details", format!("{:?}", e).into());
                                    scope.set_tag("validation_type", "processing_error");
                                },
                                || {
                                    sentry::capture_message(
                                        &format!(
                                            "[SECURITY] Validation error: hash={}, filename={}, error_type={}, details={:?}",
                                            file.hash, anon_filename,
                                            std::any::type_name_of_val(&e),
                                            e
                                        ),
                                        sentry::Level::Error,
                                    );
                                }
                            );
                            Err(format!("Validation error: {}", e))
                        }
                    }
                }
                Err(_) => Err("File not found in storage".to_string()),
            };
            
            // If validation failed, delete from S3 and return error
            match validation_result {
                Ok(validated_file) => Ok(validated_file),
                Err(error_msg) => {
                    // Delete invalid file from S3
                    let _ = s3_ops::delete_object(&state, MEDIA_BUCKET.as_str(), &s3_key).await;
                    
                    Err(MediaProcessedFile {
                        hash: file.hash.clone(),
                        media_id: 0,
                        success: false,
                        error: Some(error_msg),
                    })
                }
            }
        }
    }).collect();

    // Wait for all validation tasks to complete
    let validation_results = futures::future::join_all(validation_tasks).await;

    // Separate successful validations from failures
    let mut validated_files = Vec::new();
    let mut processed_files = Vec::new();

    for result in validation_results {
        match result {
            Ok(validated_file) => {
                validated_files.push(validated_file);
            }
            Err(processed_file) => {
                processed_files.push(processed_file);
            }
        }
    }
    if validated_files.len() != confirmed_files.len() {
        let failed_count = confirmed_files.len() - validated_files.len();
        sentry::capture_message(
            &format!(
                "confirm_media_bulk_upload: {} of {} files failed validation (batch {})",
                failed_count,
                confirmed_files.len(),
                batch_id_uuid
            ),
            sentry::Level::Warning,
        );
    }

    // If no files passed validation, return early
    if validated_files.is_empty() {
        return Ok(Json(MediaBulkConfirmResponse { processed_files }));
    }

    let mut db_client = state.db_pool.get().await.map_err(|err| {
        sentry::capture_message(
            &format!(
                "confirm_media_bulk_upload: Failed to get database connection for batch {}: {}",
                batch_id_uuid, err
            ),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal error".to_string(),
        )
    })?;

    let tx = db_client.transaction().await.map_err(|err| {
        sentry::capture_message(
            &format!(
                "confirm_media_bulk_upload: Failed to start transaction for batch {}: {}",
                batch_id_uuid, err
            ),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal error".to_string(),
        )
    })?;

    // Separate validated files by whether they have known note_ids
    let mut files_with_known_notes = Vec::new();
    let mut files_with_unknown_notes = Vec::new();

    for file in &validated_files {
        if file.note_id == -1 {
            files_with_unknown_notes.push(file);
        } else {
            files_with_known_notes.push(file);
        }
    }

    let mut actual_bytes_stored: u64 = 0;

    // Process files with known note_ids
    for file in files_with_known_notes {
        // Get existing media_id from database
        let media_id_opt = tx
            .query_opt("SELECT id FROM media_files WHERE hash = $1", &[&file.hash])
            .await
            .map_err(|err| {
                sentry::capture_message(
                    &format!(
                        "confirm_media_bulk_upload: Failed to query media file for hash {}: {}",
                        file.hash, err
                    ),
                    sentry::Level::Error,
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error".to_string(),
                )
            })?;

        // Insert media_files entry if it doesn't exist, or get existing one
        // This creates the entry atomically with the reference to prevent race condition
        let media_id = match media_id_opt {
            Some(row) => row.get::<_, i64>(0),
            None => {
                // File doesn't exist yet, insert it
                tx.query_one(
                    "INSERT INTO media_files (hash, file_size)
                     VALUES ($1, $2)
                     ON CONFLICT (hash) DO UPDATE SET file_size = EXCLUDED.file_size
                     RETURNING id",
                    &[&file.hash, &file.file_size],
                )
                .await
                .map_err(|err| {
                    sentry::capture_message(
                        &format!(
                            "confirm_media_bulk_upload: Failed to insert media file {}: {}",
                            file.hash, err
                        ),
                        sentry::Level::Error,
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Internal error".to_string(),
                    )
                })?
                .get::<_, i64>(0)
            }
        };

        // Verify note exists and is not deleted
        let note_valid = tx
            .query_opt(
                "SELECT id FROM notes WHERE id = $1 AND deleted = false",
                &[&file.note_id],
            )
            .await
            .map_err(|err| {
                sentry::capture_message(
                    &format!(
                        "confirm_media_bulk_upload: Failed to query note {} for file {}: {}",
                        file.note_id, file.filename, err
                    ),
                    sentry::Level::Error,
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal error".to_string(),
                )
            })?
            .is_some();

        if !note_valid {
            sentry::add_breadcrumb(sentry::Breadcrumb {
                category: Some("media_upload".into()),
                message: Some(format!(
                    "Invalid note reference for file {}: note_id {}",
                    file.filename, file.note_id
                )),
                level: sentry::Level::Warning,
                ..Default::default()
            });
            processed_files.push(MediaProcessedFile {
                hash: file.hash.clone(),
                media_id: 0,
                success: false,
                error: Some("Invalid note reference".to_string()),
            });
            continue;
        }

        // Create media reference
        match tx
            .execute(
                "INSERT INTO media_references (media_id, note_id, file_name)
                 VALUES ($1, $2, $3)
                 ON CONFLICT (media_id, note_id) DO NOTHING",
                &[&media_id, &file.note_id, &file.filename],
            )
            .await
        {
            Ok(_) => {
                processed_files.push(MediaProcessedFile {
                    hash: file.hash.clone(),
                    media_id,
                    success: true,
                    error: None,
                });
                actual_bytes_stored += file.file_size as u64;
            }
            Err(err) => {
                sentry::capture_message(
                    &format!(
                        "confirm_media_bulk_upload: Failed to create media reference for file {} (media_id {}, note_id {}): {}",
                        file.filename, media_id, file.note_id, err
                    ),
                    sentry::Level::Error,
                );
                processed_files.push(MediaProcessedFile {
                    hash: file.hash.clone(),
                    media_id,
                    success: false,
                    error: Some("Error creating reference".to_string()),
                });
            }
        }
    }

    // Process files with unknown note_ids (note_id == -1)
    if !files_with_unknown_notes.is_empty() {
        // Get bulk operation info to resolve note_ids
        let (deck_hash, all_note_guids) = if let Some(bulk_id_str) = &req.bulk_operation_id {
            get_bulk_operation_info(&state, bulk_id_str).await?
        } else {
            (None, Vec::new())
        };

        // Build filename to note_ids mapping
        let filename_to_note_ids = build_filename_to_note_ids_mapping(
            &tx,
            &files_with_unknown_notes
                .iter()
                .map(|f| ValidatedFile {
                    hash: f.hash.clone(),
                    actual_size: f.file_size,
                    note_id: f.note_id,
                    filename: f.filename.clone(),
                })
                .collect::<Vec<_>>(),
            &deck_hash,
            &all_note_guids,
        )
        .await?;

        // Process each file with unknown note_id
        for file in files_with_unknown_notes {
            // Get existing media_id from database or create new entry
            let media_id_opt = tx
                .query_opt(
                    "SELECT id FROM media_files WHERE hash = $1",
                    &[&file.hash],
                )
                .await
                .map_err(|err| {
                    sentry::capture_message(
                        &format!(
                            "confirm_media_bulk_upload: Failed to query media file for unknown note hash {}: {}",
                            file.hash, err
                        ),
                        sentry::Level::Error,
                    );
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Internal error".to_string(),
                    )
                })?;

            // Insert media_files entry if it doesn't exist, or get existing one
            // This creates the entry atomically with the references to prevent race condition
            let media_id = match media_id_opt {
                Some(row) => row.get::<_, i64>(0),
                None => {
                    // File doesn't exist yet, insert it
                    tx.query_one(
                        "INSERT INTO media_files (hash, file_size)
                         VALUES ($1, $2)
                         ON CONFLICT (hash) DO UPDATE SET file_size = EXCLUDED.file_size
                         RETURNING id",
                        &[&file.hash, &file.file_size],
                    )
                    .await
                    .map_err(|err| {
                        sentry::capture_message(
                            &format!(
                                "confirm_media_bulk_upload: Failed to insert media file for unknown note {}: {}",
                                file.hash, err
                            ),
                            sentry::Level::Error,
                        );
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Internal error".to_string(),
                        )
                    })?
                    .get::<_, i64>(0)
                }
            };

            // Get all notes that reference this filename
            let referencing_notes = filename_to_note_ids
                .get(&file.filename)
                .cloned()
                .unwrap_or_default();

            if referencing_notes.is_empty() {
                processed_files.push(MediaProcessedFile {
                    hash: file.hash.clone(),
                    media_id,
                    success: false,
                    error: Some("No notes reference this file".to_string()),
                });
                continue;
            }

            // Create references for all notes that use this file
            let mut had_error = false;
            for note_id in referencing_notes {
                if let Err(err) = tx
                    .execute(
                        "INSERT INTO media_references (media_id, note_id, file_name)
                         VALUES ($1, $2, $3)
                         ON CONFLICT (media_id, note_id) DO NOTHING",
                        &[&media_id, &note_id, &file.filename],
                    )
                    .await
                {
                    sentry::capture_message(
                        &format!(
                            "confirm_media_bulk_upload: Failed to create media reference for unknown note file {} (note_id {}): {}",
                            file.filename, note_id, err
                        ),
                        sentry::Level::Error,
                    );
                    had_error = true;
                    break;
                }
            }

            if had_error {
                processed_files.push(MediaProcessedFile {
                    hash: file.hash.clone(),
                    media_id,
                    success: false,
                    error: Some("Error creating references".to_string()),
                });
            } else {
                processed_files.push(MediaProcessedFile {
                    hash: file.hash.clone(),
                    media_id,
                    success: true,
                    error: None,
                });
                actual_bytes_stored += file.file_size as u64;
            }
        }
    }

    // Delete batch metadata
    tx.execute(
        "DELETE FROM media_bulk_uploads WHERE id = $1",
        &[&batch_id_uuid],
    )
    .await
    .map_err(|err| {
        sentry::capture_message(
            &format!(
                "confirm_media_bulk_upload: Failed to delete batch metadata for {}: {}",
                batch_id_uuid, err
            ),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal error".to_string(),
        )
    })?;

    tx.commit().await.map_err(|err| {
        sentry::capture_message(
            &format!(
                "confirm_media_bulk_upload: Failed to commit transaction for batch {}: {}",
                batch_id_uuid, err
            ),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal error".to_string(),
        )
    })?;

    // Track the global storage used
    state
        .rate_limiter
        .storage_monitor()
        .track_operation(actual_bytes_stored);

    // All validation done before commit - no background validation needed

    Ok(Json(MediaBulkConfirmResponse { processed_files }))
}

use svg_hush::{data_url_filter, Filter};
fn sanitize_svg(data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    if data.len() > MAX_FILE_SIZE_BYTES || data.len() < 10 {
        sentry::add_breadcrumb(sentry::Breadcrumb {
            category: Some("svg_sanitize".into()),
            message: Some(format!(
                "SVG size rejected: len={} (min=10, max={})",
                data.len(), MAX_FILE_SIZE_BYTES
            )),
            level: sentry::Level::Debug,
            ..Default::default()
        });
        return out;
    }

    let is_svg = detect_supported_media(data)
        .map(|m| m.is_svg)
        .unwrap_or(false);

    if !is_svg {
        // Log why this wasn't detected as SVG
        let byte_preview = create_byte_preview(data, 64);
        let text_preview = create_text_preview(data, 200);
        let looks_svg = looks_like_svg(data);
        let detected_mime = detect_mime(data).unwrap_or("none");
        
        sentry::with_scope(
            |scope| {
                scope.set_fingerprint(Some(["svg-sanitize-not-svg"].as_ref()));
                scope.set_extra("file_size", data.len().into());
                scope.set_extra("detected_mime", detected_mime.into());
                scope.set_extra("looks_like_svg", looks_svg.into());
                scope.set_extra("byte_preview", byte_preview.clone().into());
                if let Some(ref tp) = text_preview {
                    scope.set_extra("text_preview", tp.clone().into());
                }
            },
            || {
                sentry::capture_message(
                    &format!(
                        "[SVG] Not detected as SVG: mime={}, looks_svg={}, bytes={}",
                        detected_mime, looks_svg, byte_preview
                    ),
                    sentry::Level::Warning,
                );
            }
        );
        return out;
    }

    let mut input = data;
    let mut filter = Filter::new();
    filter.set_data_url_filter(data_url_filter::allow_standard_images);
    match filter.filter(&mut input, &mut out) {
        Ok(_) => {},
        Err(e) => {
            let byte_preview = create_byte_preview(data, 64);
            sentry::with_scope(
                |scope| {
                    scope.set_fingerprint(Some(["svg-sanitize-filter-failed"].as_ref()));
                    scope.set_extra("file_size", data.len().into());
                    scope.set_extra("byte_preview", byte_preview.clone().into());
                    scope.set_extra("error", format!("{:?}", e).into());
                },
                || {
                    sentry::capture_message(
                        &format!("[SVG] Filter failed: error={:?}, bytes={}", e, byte_preview),
                        sentry::Level::Warning,
                    );
                }
            );
        }
    }
    out
}

// Content validation function for background file verification
enum ContentValidationOutcome {
    Valid,
    Invalid {
        reason: &'static str,
        mime: Option<String>,
    },
}

async fn validate_media_stream(
    byte_stream: ByteStream,
    expected_hash: &str,
) -> Result<ContentValidationOutcome, Box<dyn std::error::Error + Send + Sync>> {
    let mut reader = byte_stream.into_async_read();
    let mut buffer = [0u8; 8192];
    let mut content = Vec::new();

    loop {
        let bytes_read = reader.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }

        content.extend_from_slice(&buffer[..bytes_read]);

        if content.len() > MAX_FILE_SIZE_BYTES {
            return Ok(ContentValidationOutcome::Invalid {
                reason: "too_large",
                mime: None,
            });
        }
    }

    if content.is_empty() {
        return Ok(ContentValidationOutcome::Invalid {
            reason: "empty",
            mime: None,
        });
    }

    let detected = match detect_supported_media(&content) {
        Some(media) => media,
        None => {
            let detected_mime = detect_mime(&content).unwrap_or("unknown").to_string();
            
            // Enhanced diagnostic logging for false negatives
            let byte_preview = create_byte_preview(&content, 64);
            let text_preview = create_text_preview(&content, 200);
            let file_size = content.len();
            
            // Check what heuristics say about the file
            let is_svg_heuristic = looks_like_svg(&content);
            let is_webp_heuristic = looks_like_webp(&content);
            let is_ogg_heuristic = looks_like_ogg(&content);
            let is_mp3_heuristic = looks_like_mp3(&content);
            
            sentry::with_scope(
                |scope| {
                    scope.set_fingerprint(Some(["media-detection-failed"].as_ref()));
                    scope.set_extra("expected_hash", expected_hash.into());
                    scope.set_extra("file_size", file_size.into());
                    scope.set_extra("detected_mime", detected_mime.clone().into());
                    scope.set_extra("byte_preview", byte_preview.clone().into());
                    if let Some(ref tp) = text_preview {
                        scope.set_extra("text_preview", tp.clone().into());
                    }
                    scope.set_extra("heuristic_svg", is_svg_heuristic.into());
                    scope.set_extra("heuristic_webp", is_webp_heuristic.into());
                    scope.set_extra("heuristic_ogg", is_ogg_heuristic.into());
                    scope.set_extra("heuristic_mp3", is_mp3_heuristic.into());
                    scope.set_tag("detection_failure", "true");
                },
                || {
                    sentry::capture_message(
                        &format!(
                            "[MEDIA] Detection failed: mime={}, size={}, svg_heuristic={}, bytes={}",
                            detected_mime, file_size, is_svg_heuristic, byte_preview
                        ),
                        sentry::Level::Warning,
                    );
                }
            );
            
            return Ok(ContentValidationOutcome::Invalid {
                reason: "unsupported_media",
                mime: Some(detected_mime),
            });
        }
    };

    let mut hasher = Md5::new();

    if detected.is_svg {
        let sanitized = sanitize_svg(&content);

        if sanitized.is_empty() {
            return Ok(ContentValidationOutcome::Invalid {
                reason: "svg_sanitize_failed",
                mime: Some("image/svg+xml".to_string()),
            });
        }

        hasher.update(&sanitized);
    } else {
        hasher.update(&content);
    }

    let actual_hash = format!("{:x}", hasher.finalize());

    if actual_hash == expected_hash {
        Ok(ContentValidationOutcome::Valid)
    } else {
        Ok(ContentValidationOutcome::Invalid {
            reason: "content_hash_mismatch",
            mime: None,
        })
    }
}

pub(crate) fn determine_content_type_by_name(file_name: &str) -> Option<String> {
    let ext = file_name.split('.').last().unwrap_or("").to_lowercase();
    EXTENSION_TO_MIME
        .iter()
        .find(|(candidate, _)| *candidate == ext)
        .map(|(_, mime)| (*mime).to_string())
}

/// Detailed rejection reasons for filename validation
#[derive(Debug, Clone)]
#[allow(dead_code)] // Fields are used in Debug formatting for Sentry logs
enum FilenameRejectionReason {
    EmptyOrTooShort,
    TooLong,
    PathTraversal,
    EndsWithDotOrSpace,
    OnlyWhitespace,
    InvalidCharacter(char),
    InvalidStartChar,
    NoAlphanumeric,
    NoExtension,
    DisallowedExtension(String),
}

fn check_filename_validity(filename: &str) -> Result<(), FilenameRejectionReason> {
    // Minimum length 4 allows valid filenames like "a.png" (1 char + dot + 3 char extension)
    if filename.is_empty() || filename.len() < 4 {
        return Err(FilenameRejectionReason::EmptyOrTooShort);
    }
    
    if filename.len() > 255 {
        return Err(FilenameRejectionReason::TooLong);
    }

    if filename.contains('/') || filename.contains('\\') || filename.contains("..") {
        return Err(FilenameRejectionReason::PathTraversal);
    }
    
    if filename.ends_with('.') || filename.ends_with(' ') {
        return Err(FilenameRejectionReason::EndsWithDotOrSpace);
    }

    if filename.trim().is_empty() {
        return Err(FilenameRejectionReason::OnlyWhitespace);
    }

    for c in filename.chars() {
        if c.is_control() {
            return Err(FilenameRejectionReason::InvalidCharacter(c));
        }
        if !(c.is_alphanumeric()
            || c == '.'
            || c == '-'
            || c == '_'
            || c == ' '
            || c == '('
            || c == ')'
            || c == '+'
            || c == ','
            || c == '%'
            || c == '&')
        {
            return Err(FilenameRejectionReason::InvalidCharacter(c));
        }
    }

    // Name must start with a letter, number, or underscore (to avoid leading dots/spaces)
    if !filename
        .chars()
        .next()
        .map_or(false, |c| char::is_alphanumeric(c) || c == '_')
    {
        return Err(FilenameRejectionReason::InvalidStartChar);
    }

    // Name must contain at least one letter or number (to avoid only dots/spaces)
    if !filename.chars().any(char::is_alphanumeric) {
        return Err(FilenameRejectionReason::NoAlphanumeric);
    }

    let path = std::path::Path::new(filename);
    let ext = match path.extension().and_then(|s| s.to_str()) {
        Some(ext) => ext.to_lowercase(),
        None => return Err(FilenameRejectionReason::NoExtension),
    };

    const ALLOWED_EXTENSIONS: [&str; 11] = [
        "jpg", "jpeg", "png", "gif", "webp", "svg", "bmp", "tif", "tiff", "mp3", "ogg",
    ];

    if !ALLOWED_EXTENSIONS.contains(&ext.as_str()) {
        return Err(FilenameRejectionReason::DisallowedExtension(ext));
    }

    Ok(())
}

fn is_allowed_extension(filename: &str) -> bool {
    check_filename_validity(filename).is_ok()
}

/// Like is_allowed_extension, but logs the rejection reason to Sentry for diagnostics
fn is_allowed_extension_with_logging(filename: &str, user_id: i64) -> bool {
    match check_filename_validity(filename) {
        Ok(()) => true,
        Err(reason) => {
            let anon_filename = anonymize_filename(filename);
            let ext = filename.rsplit('.').next().unwrap_or("<none>");
            
            sentry::add_breadcrumb(sentry::Breadcrumb {
                category: Some("media_validation".into()),
                message: Some(format!(
                    "Filename rejected: reason={:?}, ext='{}', anon={}, user={}",
                    reason, ext, anon_filename, user_id
                )),
                level: sentry::Level::Info,
                ..Default::default()
            });
            
            false
        }
    }
}

pub async fn get_media_manifest(
    State(state): State<Arc<AppState>>,
    client_ip: ClientIp,
    Json(req): Json<MediaManifestRequest>,
) -> Result<Json<MediaManifestResponse>, (StatusCode, String)> {
    let user_id = auth::get_user_from_token(&state, &req.user_token)
        .await
        .map_err(|_| {
            sentry::add_breadcrumb(sentry::Breadcrumb {
                category: Some("media_download".into()),
                message: Some("Failed to authenticate user token for media manifest".to_string()),
                level: sentry::Level::Warning,
                ..Default::default()
            });
            (
                StatusCode::UNAUTHORIZED,
                "Error authenticating user (1)".to_string(),
            )
        })?;

    if user_id == 0 {
        sentry::add_breadcrumb(sentry::Breadcrumb {
            category: Some("media_download".into()),
            message: Some(format!(
                "Invalid token for media manifest: IP {}",
                client_ip.0
            )),
            level: sentry::Level::Warning,
            ..Default::default()
        });
        return Err((
            StatusCode::UNAUTHORIZED,
            "Error authenticating user (2)".to_string(),
        ));
    }
    let ip_address = client_ip.0;

    if req.filenames.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "No filenames provided".to_string()));
    }

    if req.filenames.len() > 500 {
        sentry::capture_message(
            &format!(
                "User {} requested {} files in manifest (limit: 500)",
                user_id,
                req.filenames.len()
            ),
            sentry::Level::Warning,
        );
        // Contabo api limit is 250 requests per second, so we should probably reduce this limit
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "Too many files requested.".to_string(),
        ));
    }

    sentry::configure_scope(|scope| {
        scope.set_user(Some(sentry::User {
            id: Some(user_id.to_string()),
            ..Default::default()
        }));
        scope.set_tag("deck_hash", &req.deck_hash);
    });

    let db_client = state.db_pool.get_owned().await.map_err(|err| {
        sentry::capture_message(
            &format!(
                "get_media_manifest: Failed to get database connection for user {}: {}",
                user_id, err
            ),
            sentry::Level::Error,
        );
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Internal error".to_string(),
        )
    })?;

    // Find media file hashes based on filenames and deck hash.
    // This query handles inheritance by:
    // 1. Finding direct media references for notes in the deck tree
    // 2. Finding media references from base notes for inherited fields
    let media_query = "
        WITH RECURSIVE deck_tree AS (
            SELECT id FROM decks WHERE human_hash = $1
            UNION ALL
            SELECT d.id FROM decks d
            INNER JOIN deck_tree dt ON d.parent = dt.id
        ),
        -- Notes in the subscriber deck tree
        deck_notes AS (
            SELECT id FROM notes WHERE deck IN (SELECT id FROM deck_tree) AND NOT deleted
        ),
        -- Direct media references for notes in deck tree
        direct_refs AS (
            SELECT DISTINCT mf.hash, mr.file_name, mf.file_size
            FROM media_references mr
            JOIN media_files mf ON mf.id = mr.media_id
            WHERE mr.note_id IN (SELECT id FROM deck_notes)
            AND mr.file_name = ANY($2)
        ),
        -- Base note media references for inherited fields
        inherited_refs AS (
            SELECT DISTINCT mf.hash, mr.file_name, mf.file_size
            FROM note_inheritance ni
            JOIN media_references mr ON mr.note_id = ni.base_note_id
            JOIN media_files mf ON mf.id = mr.media_id
            WHERE ni.subscriber_note_id IN (SELECT id FROM deck_notes)
            AND mr.file_name = ANY($2)
        )
        SELECT * FROM direct_refs
        UNION
        SELECT * FROM inherited_refs
    ";

    let media_rows = db_client
        .query(media_query, &[&req.deck_hash, &req.filenames])
        .await
        .map_err(|err| {
            sentry::capture_message(
                &format!(
                    "get_media_manifest: Failed to query media files for user {} deck {}: {}",
                    user_id, req.deck_hash, err
                ),
                sentry::Level::Error,
            );
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal error".to_string(),
            )
        })?;

    // Create the manifest data
    let now = Utc::now();
    let expires_at = now + Duration::minutes(PRESIGNED_URL_EXPIRATION as i64);

    let length_vec = media_rows.len() as u32;
    let mut file_entries: Vec<MediaDownloadItem> =
        Vec::with_capacity(length_vec.try_into().unwrap());

    // Check download limits for this IP using actual size
    if !state
        .rate_limiter
        .check_user_download_allowed(user_id, ip_address, length_vec)
        .await
    {
        sentry::capture_message(
            &format!(
                "Download limit exceeded for user {} (IP: {}, requested: {} files)",
                user_id, ip_address, length_vec
            ),
            sentry::Level::Warning,
        );
        //println!("Download limit exceeded for {client_ip:?}");
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            "Download limit exceeded. Please try again tomorrow.".to_string(),
        ));
    }

    let base_url_trimmed = state.media_base_url.as_ref().trim_end_matches('/');

    for row in media_rows {
        let hash: String = row.get(0);
        let filename: String = row.get(1);

        let download_token = state
            .media_token_service
            .generate_download_token(DownloadTokenParams {
                hash: hash.clone(),
                user_id,
                deck_hash: req.deck_hash.clone(),
                filename: Some(filename.clone()),
            })
            .map_err(|err| {
                sentry::capture_message(
                    &format!(
                        "get_media_manifest: Failed to generate download token for {} (user {}, deck {}): {}",
                        hash, user_id, req.deck_hash, err
                    ),
                    sentry::Level::Error,
                );
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Error generating download link".to_string(),
                )
            })?;

        file_entries.push(MediaDownloadItem {
            filename,
            download_url: format!(
                "{}/v1/media/{}?token={}",
                base_url_trimmed, hash, download_token
            ),
        });
    }

    let response = MediaManifestResponse {
        files: file_entries,
        file_count: length_vec as i32,
        expires_at: expires_at.to_rfc3339(),
    };

    Ok(Json(response))
}

pub async fn sanitize_svg_batch(
    Json(req): Json<SvgSanitizeRequest>,
) -> Result<Json<SvgSanitizeResponse>, (StatusCode, String)> {
    // Validate request
    if req.svg_files.is_empty() {
        return Err((StatusCode::BAD_REQUEST, "No SVG files provided".to_string()));
    }

    // Limit batch size
    const MAX_BATCH_SIZE: usize = 250;
    const MAX_TOTAL_SIZE: usize = 1024 * 1024; // 1 MB total batch size

    if req.svg_files.len() > MAX_BATCH_SIZE {
        return Err((
            StatusCode::BAD_REQUEST,
            "Too many files in single request".to_string(),
        ));
    }

    // Calculate total size
    let total_size: usize = req.svg_files.iter().map(|f| f.content.len()).sum();

    if total_size > MAX_TOTAL_SIZE {
        return Err((
            StatusCode::BAD_REQUEST,
            "Total batch size too large".to_string(),
        ));
    }

    let mut sanitized_files = Vec::with_capacity(req.svg_files.len());

    for file in req.svg_files {
        let content_bytes = file.content.as_bytes();
        let sanitized_bytes = sanitize_svg(content_bytes);
        if sanitized_bytes.is_empty() {
            continue;
        }

        // Convert back to string
        let sanitized_content = match String::from_utf8(sanitized_bytes) {
            Ok(content) => content,
            Err(_) => continue, // Skip if invalid UTF-8
        };

        let mut hasher = Md5::new();
        hasher.update(sanitized_content.as_bytes());
        let hash = format!("{:x}", hasher.finalize());

        sanitized_files.push(SanitizedSvgItem {
            content: sanitized_content,
            filename: file.filename,
            hash,
        });
    }

    Ok(Json(SvgSanitizeResponse { sanitized_files }))
}
