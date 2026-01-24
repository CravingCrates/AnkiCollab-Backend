// Duplicated code from the website. but since we have auto-approve (unfortunately) we have to handle this in the backend as well.
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::Value;
use std::collections::{HashMap, HashSet};

use rayon::prelude::*;

use bb8_postgres::bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use tokio_postgres::Error as PgError;

type SharedConn = PooledConnection<'static, PostgresConnectionManager<tokio_postgres::NoTls>>;

/// Inheritance info for a subscriber note: (base_note_id, subscribed_fields)
/// subscribed_fields = None means all fields are inherited
/// subscribed_fields = Some([0, 1, 2]) means only those field positions are inherited
pub struct NoteInheritanceInfo {
    pub base_note_id: i64,
    pub subscribed_fields: Option<Vec<i32>>,
}

static SOUND_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"\[sound:(.*?)\]").unwrap());
static IMG_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"<img[^>]*src=["']([^"']*)["'][^>]*>"#).unwrap());
static CSS_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r#"url\(["']?([^"')]+)["']?\)"#).unwrap());
static SRC_REGEX: Lazy<Regex> =
    Lazy::new(|| Regex::new(r#"(?i)(?:src|xlink:href)=["']([^"']+)["']"#).unwrap());
static LATEX_REGEX: Lazy<Regex> = Lazy::new(|| Regex::new(r"latex-image-\w+\.png").unwrap());

/// Extract all media references from a field content string as anki does
#[must_use]
pub fn extract_media_references(field_content: &str) -> HashSet<String> {
    let mut references = HashSet::new();

    // Sound references [sound:filename.mp3]
    for cap in SOUND_REGEX.captures_iter(field_content) {
        if let Some(filename) = cap.get(1) {
            references.insert(filename.as_str().to_string());
        }
    }

    // HTML img src
    for cap in IMG_REGEX.captures_iter(field_content) {
        if let Some(filename) = cap.get(1) {
            let src = filename.as_str();
            // Only consider local media files (not URLs)
            if !src.starts_with("http://")
                && !src.starts_with("https://")
                && !src.starts_with("data:")
            {
                references.insert(src.to_string());
            }
        }
    }

    // CSS url() references
    for cap in CSS_REGEX.captures_iter(field_content) {
        if let Some(filename) = cap.get(1) {
            let src = filename.as_str();
            if !src.starts_with("http://")
                && !src.starts_with("https://")
                && !src.starts_with("data:")
            {
                references.insert(src.to_string());
            }
        }
    }

    // Other HTML elements with src attribute
    for cap in SRC_REGEX.captures_iter(field_content) {
        if let Some(filename) = cap.get(1) {
            let src = filename.as_str();
            if !src.starts_with("http://")
                && !src.starts_with("https://")
                && !src.starts_with("data:")
            {
                references.insert(src.to_string());
            }
        }
    }

    // LaTeX image references
    for cap in LATEX_REGEX.captures_iter(field_content) {
        references.insert(cap.get(0).unwrap().as_str().to_string());
    }

    references
}

/// Get all fields of a note
async fn get_note_fields(client: &SharedConn, note_id: i64) -> Result<Vec<String>, PgError> {
    let rows = client
        .query("SELECT content FROM fields WHERE note = $1", &[&note_id])
        .await?;

    let fields = rows.iter().map(|row| row.get::<_, String>(0)).collect();

    Ok(fields)
}

/// Get all media references currently saved for a note
async fn get_existing_references(
    client: &SharedConn,
    note_id: i64,
) -> Result<HashSet<String>, PgError> {
    let rows = client
        .query(
            "SELECT file_name FROM media_references WHERE note_id = $1",
            &[&note_id],
        )
        .await?;

    let refs = rows.iter().map(|row| row.get::<_, String>(0)).collect();

    Ok(refs)
}

/// Get inheritance info for a batch of notes
/// Returns a map of subscriber_note_id -> NoteInheritanceInfo
async fn get_inheritance_info_batch(
    client: &SharedConn,
    note_ids: &[i64],
) -> Result<HashMap<i64, NoteInheritanceInfo>, PgError> {
    if note_ids.is_empty() {
        return Ok(HashMap::new());
    }

    let rows = client
        .query(
            "SELECT subscriber_note_id, base_note_id, subscribed_fields 
             FROM note_inheritance 
             WHERE subscriber_note_id = ANY($1)",
            &[&note_ids],
        )
        .await?;

    let mut result = HashMap::new();
    for row in rows {
        let subscriber_note_id: i64 = row.get(0);
        let base_note_id: i64 = row.get(1);
        let subscribed_fields: Option<Vec<i32>> = row.get(2);
        result.insert(
            subscriber_note_id,
            NoteInheritanceInfo {
                base_note_id,
                subscribed_fields,
            },
        );
    }

    Ok(result)
}

/// Batch version of resolve_media_owner_for_upload for use within a transaction
/// Returns a map of (subscriber_note_id, filename) -> owner_note_id
pub async fn resolve_media_owners_batch_tx(
    tx: &tokio_postgres::Transaction<'_>,
    note_files: &[(i64, String)], // Vec of (note_id, filename)
) -> Result<HashMap<(i64, String), i64>, Box<dyn std::error::Error + Send + Sync>> {
    let mut result: HashMap<(i64, String), i64> = HashMap::new();
    
    if note_files.is_empty() {
        return Ok(result);
    }
    
    // Get unique note IDs
    let note_ids: Vec<i64> = note_files.iter().map(|(id, _)| *id).collect::<HashSet<_>>().into_iter().collect();
    
    // Batch fetch inheritance info
    let inh_rows = tx
        .query(
            "SELECT subscriber_note_id, base_note_id, subscribed_fields 
             FROM note_inheritance 
             WHERE subscriber_note_id = ANY($1)",
            &[&note_ids],
        )
        .await?;
    
    let mut inheritance_map: HashMap<i64, NoteInheritanceInfo> = HashMap::new();
    for row in inh_rows {
        let subscriber_note_id: i64 = row.get(0);
        let base_note_id: i64 = row.get(1);
        let subscribed_fields: Option<Vec<i32>> = row.get(2);
        inheritance_map.insert(
            subscriber_note_id,
            NoteInheritanceInfo {
                base_note_id,
                subscribed_fields,
            },
        );
    }
    
    // Collect base note IDs we need to query
    let base_note_ids: Vec<i64> = inheritance_map.values().map(|info| info.base_note_id).collect();
    
    // Batch fetch base note fields
    let base_fields: HashMap<i64, HashMap<i32, String>> = if !base_note_ids.is_empty() {
        let rows = tx
            .query(
                "SELECT note, position, content FROM fields WHERE note = ANY($1)",
                &[&base_note_ids],
            )
            .await?;
        
        let mut map: HashMap<i64, HashMap<i32, String>> = HashMap::new();
        for row in rows {
            let note_id: i64 = row.get(0);
            let position: u32 = row.get(1);
            let content: String = row.get(2);
            map.entry(note_id).or_default().insert(position as i32, content);
        }
        map
    } else {
        HashMap::new()
    };
    
    // Process each (note_id, filename) pair
    for (note_id, filename) in note_files {
        let owner = if let Some(inheritance_info) = inheritance_map.get(note_id) {
            let base_note_id = inheritance_info.base_note_id;
            
            // Check if filename is in inherited fields
            let is_in_inherited = if let Some(fields) = base_fields.get(&base_note_id) {
                match &inheritance_info.subscribed_fields {
                    None => {
                        // All fields inherited - check all base fields
                        fields.values().any(|content| {
                            extract_media_references(content).contains(filename.as_str())
                        })
                    }
                    Some(positions) => {
                        // Only check subscribed positions
                        positions.iter().any(|pos| {
                            fields.get(pos).map_or(false, |content| {
                                extract_media_references(content).contains(filename.as_str())
                            })
                        })
                    }
                }
            } else {
                false
            };
            
            if is_in_inherited {
                base_note_id
            } else {
                *note_id
            }
        } else {
            // No inheritance
            *note_id
        };
        
        result.insert((*note_id, filename.clone()), owner);
    }
    
    Ok(result)
}

/// Update media references for a single note
pub async fn update_media_references_for_note(
    client: &mut SharedConn,
    note_id: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Get all field content for the note
    let fields = get_note_fields(client, note_id).await?;

    // Extract all media references from fields
    let mut all_references = HashSet::new();
    for field in &fields {
        let refs = extract_media_references(field);
        all_references.extend(refs);
    }

    // Get existing references
    let existing_refs = get_existing_references(client, note_id).await?;

    // Calculate differences
    let to_add: HashSet<_> = all_references.difference(&existing_refs).cloned().collect();
    let to_remove: HashSet<_> = existing_refs.difference(&all_references).cloned().collect();

    if to_add.is_empty() && to_remove.is_empty() {
        return Ok(());
    }

    let tx = client.transaction().await?;

    // to_add should be empty, bc the media gets uploaded upon submission. We can't do anything about missing media here, so we'll just log it.

    // Remove old references
    for filename in &to_remove {
        tx.execute(
            "DELETE FROM media_references 
            WHERE note_id = $1 AND file_name = $2",
            &[&note_id, &filename],
        )
        .await?;
    }

    tx.commit().await?;

    Ok(())
}

pub async fn get_missing_media(
    client: &SharedConn,
    deck_hash: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let notes_query = client
        .query(
            "WITH RECURSIVE deck_tree AS (
                SELECT id FROM decks WHERE human_hash = $1
                UNION ALL
                SELECT d.id FROM decks d
                JOIN deck_tree dt ON d.parent = dt.id
            )
            SELECT id FROM notes WHERE deck IN (SELECT id FROM deck_tree) AND NOT deleted",
            &[&deck_hash],
        )
        .await?;

    let note_ids: Vec<i64> = notes_query.iter().map(|row| row.get(0)).collect();
    if note_ids.is_empty() {
        return Ok(Vec::new());
    }

    const BATCH_SIZE: usize = 1000;
    let mut missing_media = HashSet::new();

    // Process notes in batches
    for chunk in note_ids.chunks(BATCH_SIZE) {
        let chunk_vec = chunk.to_vec();

        // Get inheritance info for this batch
        let inheritance_map = get_inheritance_info_batch(client, &chunk_vec).await?;
        
        // Collect all base note IDs we need to query
        let base_note_ids: Vec<i64> = inheritance_map.values().map(|info| info.base_note_id).collect();

        // Use JSON aggregation to fetch all fields and references in a single query per batch
        // Now also including the note ID in the result for inheritance lookup
        let query_rows = client
            .query(
                "WITH note_fields AS (
                    SELECT note, json_agg(content) as fields_json
                    FROM fields
                    WHERE note = ANY($1) AND reviewed = true
                    GROUP BY note
                ),
                note_refs AS (
                    SELECT note_id, json_agg(file_name) as refs_json
                    FROM media_references
                    WHERE note_id = ANY($1)
                    GROUP BY note_id
                )
                SELECT
                    nf.note,
                    nf.fields_json,
                    COALESCE(nr.refs_json, '[]'::json) as refs_json
                FROM note_fields nf
                LEFT JOIN note_refs nr ON nf.note = nr.note_id",
                &[&chunk_vec],
            )
            .await?;
        
        // Also fetch base note media references for inherited notes
        let base_refs_map: HashMap<i64, HashSet<String>> = if !base_note_ids.is_empty() {
            let base_refs_rows = client
                .query(
                    "SELECT note_id, json_agg(file_name) as refs_json
                     FROM media_references
                     WHERE note_id = ANY($1)
                     GROUP BY note_id",
                    &[&base_note_ids],
                )
                .await?;
            
            let mut map = HashMap::new();
            for row in base_refs_rows {
                let note_id: i64 = row.get(0);
                let refs_val: Value = row.get(1);
                let mut refs = HashSet::new();
                if let Value::Array(arr) = refs_val {
                    for ref_val in arr {
                        if let Value::String(file_name) = ref_val {
                            refs.insert(file_name);
                        }
                    }
                }
                map.insert(note_id, refs);
            }
            map
        } else {
            HashMap::new()
        };
        
        // Also fetch base note field contents for subscribed field filtering
        let base_fields_map: HashMap<i64, HashMap<i32, String>> = if !base_note_ids.is_empty() {
            let base_fields_rows = client
                .query(
                    "SELECT note, position, content FROM fields 
                     WHERE note = ANY($1) AND reviewed = true",
                    &[&base_note_ids],
                )
                .await?;
            
            let mut map: HashMap<i64, HashMap<i32, String>> = HashMap::new();
            for row in base_fields_rows {
                let note_id: i64 = row.get(0);
                let position: u32 = row.get(1);
                let content: String = row.get(2);
                map.entry(note_id).or_default().insert(position as i32, content);
            }
            map
        } else {
            HashMap::new()
        };

        let batch_missing_media: HashSet<String> = query_rows
            .par_iter()
            .flat_map(|row| {
                let note_id: i64 = row.get(0);
                let fields_val: Value = row.get(1);
                let refs_val: Value = row.get(2);

                let mut content_references = HashSet::new();
                if let Value::Array(fields) = fields_val {
                    for field_val in fields {
                        if let Value::String(content) = field_val {
                            content_references.extend(extract_media_references(&content));
                        }
                    }
                }

                let mut existing_references = HashSet::new();
                if let Value::Array(refs) = refs_val {
                    for ref_val in refs {
                        if let Value::String(file_name) = ref_val {
                            existing_references.insert(file_name);
                        }
                    }
                }
                
                // Check if this note inherits from a base note
                // If so, include the base note's media refs for inherited fields as "existing"
                if let Some(inheritance_info) = inheritance_map.get(&note_id) {
                    let base_note_id = inheritance_info.base_note_id;

                    match &inheritance_info.subscribed_fields {
                        None => {
                            // Subscribe all - include all base media refs
                            if let Some(base_refs) = base_refs_map.get(&base_note_id) {
                                existing_references.extend(base_refs.iter().cloned());
                            }
                            
                            // Also extract media refs from base note's field contents
                            // since those are the actual inherited field values
                            if let Some(base_fields) = base_fields_map.get(&base_note_id) {
                                for content in base_fields.values() {
                                    content_references.extend(extract_media_references(content));
                                }
                            }
                        }
                        Some(subscribed) => {
                            // Only subscribed fields - extract refs from those specific fields
                            if let Some(base_fields) = base_fields_map.get(&base_note_id) {
                                let mut inherited_content_refs = HashSet::new();
                                for pos in subscribed {
                                    if let Some(content) = base_fields.get(pos) {
                                        inherited_content_refs.extend(extract_media_references(content));
                                    }
                                }
                                // Include base refs that are in subscribed fields' content
                                
                                if let Some(base_refs) = base_refs_map.get(&base_note_id) {
                                    let relevant_base_refs: HashSet<String> = base_refs
                                        .intersection(&inherited_content_refs)
                                        .cloned()
                                        .collect();
                                    existing_references.extend(relevant_base_refs);
                                }
                                
                                // Also add to content_references since these are fields that will be shown
                                content_references.extend(inherited_content_refs);
                            }
                        }
                    }
                }

                content_references
                    .difference(&existing_references)
                    .cloned()
                    .collect::<HashSet<_>>()
            })
            .collect();

        missing_media.extend(batch_missing_media);
    }

    // Convert HashSet to Vec for the final result to make serde stop bitching
    let missing_media_vec: Vec<String> = missing_media.into_iter().collect();

    Ok(missing_media_vec)
}
