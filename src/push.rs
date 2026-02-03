use crate::database;
use crate::human_hash::humanize;
use uuid::Uuid;

use crate::cleanser;
use crate::notetypes::unpack_notetype;
use crate::structs::{AnkiDeck, Note};
use crate::suggestion::{
    get_topmost_deck_by_note_id, history, is_valid_optional_tag, overwrite_note, update_note,
};
use serde_json::json;
use tracing::error;

use std::collections::{HashMap, HashSet};

use database::SharedConn;

use async_recursion::async_recursion;

fn extract_db_error(err: &(dyn std::error::Error + 'static)) -> Option<serde_json::Value> {
    let mut current: Option<&(dyn std::error::Error + 'static)> = Some(err);

    while let Some(err) = current {
        if let Some(pg_err) = err.downcast_ref::<tokio_postgres::Error>() {
            if let Some(db_err) = pg_err.as_db_error() {
                let position = db_err.position().map(|pos| match pos {
                    tokio_postgres::error::ErrorPosition::Original(p) => {
                        json!({"kind": "original", "position": p})
                    }
                    tokio_postgres::error::ErrorPosition::Internal { position, query } => {
                        json!({"kind": "internal", "position": position, "query": query})
                    }
                });

                return Some(json!({
                    "code": db_err.code().code().to_string(),
                    "message": db_err.message(),
                    "detail": db_err.detail(),
                    "hint": db_err.hint(),
                    "table": db_err.table(),
                    "column": db_err.column(),
                    "constraint": db_err.constraint(),
                    "severity": db_err.severity(),
                    "position": position,
                }));
            }
        }

        current = err.source();
    }

    None
}

pub async fn unpack_notes(
    client: &mut SharedConn,
    notes: Vec<&Note>,
    notetype_map: &HashMap<String, String>,
    opt_deck: Option<i64>,
    reviewed: bool,
    req_ip: &str,
    commit: i32,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let deck = opt_deck.ok_or("Deck ID is None")?;
    let deck_owner_q = client
        .query("SELECT owner from decks where id = $1", &[&deck])
        .await?;
    let deck_owner: i32 = deck_owner_q[0].get(0);

    // Pre-fetch all notetype metadata at once to avoid N+1 queries
    let unique_notetype_guids: HashSet<&String> = notes
        .iter()
        .filter_map(|note| notetype_map.get(&note.note_model_uuid))
        .collect();

    let mut protected_fields_cache: HashMap<String, Vec<u32>> = HashMap::new();
    let mut max_field_cache: HashMap<String, u32> = HashMap::new();

    if !unique_notetype_guids.is_empty() {
        let guids: Vec<&String> = unique_notetype_guids.into_iter().collect();
        let fields_query = "
            SELECT nt.guid, ntf.position, ntf.protected
            FROM notetype_field ntf
            JOIN notetype nt ON ntf.notetype = nt.id
            WHERE nt.guid = ANY($1) AND nt.owner = $2";
        let fields_stmt = client.prepare(fields_query).await?;
        let rows = client.query(&fields_stmt, &[&guids, &deck_owner]).await?;
        for row in rows {
            let guid: String = row.get(0);
            let position: u32 = row.get(1);
            let protected: bool = row.get(2);

            max_field_cache
                .entry(guid.clone())
                .and_modify(|e| *e = (*e).max(position))
                .or_insert(position);

            if protected {
                protected_fields_cache
                    .entry(guid)
                    .or_default()
                    .push(position);
            }
        }
    }

    // Memory-efficient streaming batch processing - handles unlimited deck sizes
    let batch_size = 1000;
    let notes_batches = notes.chunks(batch_size);

    // Prepare hot statements once per connection; reuse inside each transaction
    let insert_notes_query = "
        INSERT INTO notes (anki_id, guid, notetype, deck, last_update, reviewed, creator_ip)
        SELECT u.anki_id, u.guid, nt.id, $3, NOW(), $4, $5
        FROM UNNEST($1::text[], $2::text[], $6::bigint[]) AS u(guid, notetype_guid, anki_id)
        JOIN notetype nt ON nt.guid = u.notetype_guid AND nt.owner = $7
        RETURNING id, guid";
    let insert_notes_stmt = client.prepare(insert_notes_query).await?;

    let delete_notes_query = "DELETE FROM notes WHERE id = ANY($1)";
    let delete_notes_stmt = client.prepare(delete_notes_query).await?;

    let insert_fields_query = "
        INSERT INTO fields (note, position, content, reviewed, creator_ip, commit)
        SELECT * FROM UNNEST($1::bigint[], $2::integer[], $3::text[], $4::boolean[], $5::text[], $6::integer[])";
    let insert_fields_stmt = client.prepare(insert_fields_query).await?;

    let insert_tags_query = "
        INSERT INTO tags (note, content, reviewed, creator_ip, action, commit)
        SELECT * FROM UNNEST($1::bigint[], $2::text[], $3::boolean[], $4::text[], $5::boolean[], $6::integer[])";
    let insert_tags_stmt = client.prepare(insert_tags_query).await?;

    let update_notes_ts = "UPDATE notes SET last_update = NOW() WHERE id = ANY($1::bigint[])";
    let update_notes_ts_stmt = client.prepare(update_notes_ts).await?;

    let update_decks_ts = "
        WITH RECURSIVE roots AS (
            SELECT DISTINCT deck AS id FROM notes WHERE id = ANY($1::bigint[])
        ),
        up AS (
            SELECT d.id, d.parent FROM decks d JOIN roots r ON d.id = r.id
            UNION ALL
            SELECT d.id, d.parent FROM decks d JOIN up ON d.id = up.parent
        )
        UPDATE decks SET last_update = NOW() WHERE id IN (SELECT id FROM up)
    ";
    let update_decks_ts_stmt = client.prepare(update_decks_ts).await?;

    for batch in notes_batches {
        let tx = client.transaction().await?;

        // Pre-allocate with exact capacity to avoid reallocations
        let mut note_guids = Vec::with_capacity(batch.len());
        let mut note_notetype_guids = Vec::with_capacity(batch.len());
        let mut notes_to_process = Vec::with_capacity(batch.len());
        let mut notes_anki_ids = Vec::with_capacity(batch.len());

        // Validate and prepare notes for insertion
        for note in batch {
            let notetype_guid = match notetype_map.get(&note.note_model_uuid) {
                Some(guid) => guid,
                None => {
                    return Err(
                        format!("Note type not found in cache: {}", note.note_model_uuid).into(),
                    )
                }
            };

            if note.fields.is_empty() {
                return Err(
                    format!("Error inserting note {}: note has no fields", &note.guid).into(),
                );
            }

            note_guids.push(note.guid.clone());
            note_notetype_guids.push(notetype_guid.clone());
            notes_to_process.push((note, notetype_guid));
            notes_anki_ids.push(note.id);
        }

        let inserted_note_rows = tx
            .query(
                &insert_notes_stmt,
                &[
                    &note_guids,
                    &note_notetype_guids,
                    &deck,
                    &reviewed,
                    &req_ip,
                    &notes_anki_ids,
                    &deck_owner,
                ],
            )
            .await?;

        if inserted_note_rows.is_empty() {
            return Err("Error inserting notes: one or more notetypes do not exist".into());
        }

        let note_id_map: HashMap<String, i64> = inserted_note_rows
            .into_iter()
            .map(|r| (r.get::<_, String>(1), r.get::<_, i64>(0)))
            .collect();

        // Memory-efficient field and tag processing with controlled growth
        let mut field_data = Vec::with_capacity(batch.len() * 5); // Estimate 5 fields per note
        let mut tag_data = Vec::with_capacity(batch.len() * 3); // Estimate 3 tags per note

        // Get topmost deck for validation (using first inserted note)
        let first_note_id = *note_id_map.values().next().ok_or("No notes inserted")?;
        let topmost_deck_id = get_topmost_deck_by_note_id(&tx, first_note_id).await?;

        // Process notes in streaming fashion to control memory usage
        let mut notes_with_fields = HashSet::with_capacity(batch.len());

        for (note, notetype_guid) in notes_to_process {
            let note_id = match note_id_map.get(&note.guid) {
                Some(id) => *id,
                None => continue, // Note failed to insert (invalid notetype), skip its fields/tags
            };

            let protected_fields = protected_fields_cache
                .get(notetype_guid)
                .map(|v| v.as_slice())
                .unwrap_or(&[]);
            let max_allowed_position = *max_field_cache.get(notetype_guid).unwrap_or(&0);

            // Truncate fields to match notetype constraints (preserving original logic)
            let truncated_fields = if note.fields.len() > max_allowed_position as usize + 1 {
                &note.fields[0..=(max_allowed_position as usize)]
            } else {
                &note.fields
            };

            let mut has_valid_fields = false;
            let mut has_field_zero = false;

            // Stream process fields without storing intermediate vectors
            for (i, field_content) in truncated_fields.iter().enumerate() {
                let i_u32 = i as u32;
                if protected_fields.contains(&i_u32) || field_content.is_empty() {
                    continue;
                }

                let cleaned_content = cleanser::clean(field_content);
                if !cleaned_content.is_empty() {
                    field_data.push((note_id, i as i32, cleaned_content)); // Store as i32 for PostgreSQL compatibility
                    has_valid_fields = true;
                    if i == 0 {
                        has_field_zero = true;
                    }
                }
            }

            // A note must have field 0 present to be valid - this is the primary identifier
            if has_valid_fields && has_field_zero {
                notes_with_fields.insert(note_id);
            }

            // Stream process tags without intermediate storage
            for tag_content in &note.tags {
                let cleaned_tag = cleanser::clean(tag_content);
                if cleaned_tag.starts_with("AnkiCollab_Optional::")
                    && !is_valid_optional_tag(&tx, &topmost_deck_id, &cleaned_tag).await?
                {
                    continue; // Invalid Optional tag!
                }
                if !cleaned_tag.is_empty() {
                    tag_data.push((note_id, cleaned_tag));
                }
            }
        }

        // Remove notes that have no valid fields (preserving original logic)
        let notes_without_fields: Vec<i64> = note_id_map
            .values()
            .filter(|&&id| !notes_with_fields.contains(&id))
            .cloned()
            .collect();

        if !notes_without_fields.is_empty() {
            tx.execute(&delete_notes_stmt, &[&notes_without_fields])
                .await?;
        }

        // Bulk INSERT fields using memory-efficient approach
        if !field_data.is_empty() {
            let mut field_note_ids = Vec::with_capacity(field_data.len());
            let mut field_positions = Vec::with_capacity(field_data.len());
            let mut field_contents = Vec::with_capacity(field_data.len());

            for (id, pos, content) in field_data {
                field_note_ids.push(id);
                field_positions.push(pos); // Already i32, no conversion needed
                field_contents.push(content);
            }

            let field_reviewed_vec = vec![reviewed; field_note_ids.len()];
            let field_req_ip_vec = vec![req_ip; field_note_ids.len()];
            let field_commit_vec = vec![commit; field_note_ids.len()];

            tx.execute(
                &insert_fields_stmt,
                &[
                    &field_note_ids,
                    &field_positions,
                    &field_contents,
                    &field_reviewed_vec,
                    &field_req_ip_vec,
                    &field_commit_vec,
                ],
            )
            .await?;
        }

        // Bulk INSERT tags using memory-efficient approach
        if !tag_data.is_empty() {
            let mut tag_note_ids = Vec::with_capacity(tag_data.len());
            let mut tag_contents = Vec::with_capacity(tag_data.len());

            for (id, content) in tag_data {
                tag_note_ids.push(id);
                tag_contents.push(content);
            }

            let tag_reviewed_vec = vec![reviewed; tag_note_ids.len()];
            let tag_req_ip_vec = vec![req_ip; tag_note_ids.len()];
            let tag_action_vec = vec![true; tag_note_ids.len()];
            let tag_commit_vec = vec![commit; tag_note_ids.len()];

            tx.execute(
                &insert_tags_stmt,
                &[
                    &tag_note_ids,
                    &tag_contents,
                    &tag_reviewed_vec,
                    &tag_req_ip_vec,
                    &tag_action_vec,
                    &tag_commit_vec,
                ],
            )
            .await?;
        }

        if reviewed {
            let final_note_ids: Vec<i64> = notes_with_fields.into_iter().collect();

            if !final_note_ids.is_empty() {
                // 1) Batch-update all affected notes' timestamps in one statement
                tx.execute(&update_notes_ts_stmt, &[&final_note_ids])
                    .await?;

                // 2) Bump deck timestamps once for all involved decks
                tx.execute(&update_decks_ts_stmt, &[&final_note_ids])
                    .await?;
            }

            // Baseline history (single aggregated snapshot per note instead of per-field/tag explosion)
            if !note_id_map.is_empty() {
                let inserted_ids: Vec<i64> = note_id_map.values().cloned().collect();
                // Fetch all fields for inserted notes
                let field_rows = tx.query(
                    "SELECT note, position, content FROM fields WHERE note = ANY($1) AND reviewed = true ORDER BY note, position",
                    &[&inserted_ids]
                ).await?;
                let mut field_map: HashMap<i64, Vec<serde_json::Value>> = HashMap::new();
                for r in field_rows {
                    let nid: i64 = r.get(0);
                    let pos: u32 = r.get(1);
                    let content: String = r.get(2);
                    field_map
                        .entry(nid)
                        .or_default()
                        .push(json!({"position": pos, "content": content}));
                }
                // Fetch all tags
                let tag_rows = tx.query(
                    "SELECT note, content FROM tags WHERE note = ANY($1) AND reviewed = true AND action = true",
                    &[&inserted_ids]
                ).await?;
                let mut tag_map: HashMap<i64, Vec<String>> = HashMap::new();
                for r in tag_rows {
                    let nid: i64 = r.get(0);
                    let c_opt: Option<String> = r.get(1);
                    if let Some(c) = c_opt {
                        tag_map.entry(nid).or_default().push(c);
                    }
                }
                for nid in inserted_ids {
                    let snapshot = json!({
                        "fields": field_map.remove(&nid).unwrap_or_default(),
                        "tags": tag_map.remove(&nid).unwrap_or_default(),
                        "reviewed": true
                    });
                    let _ = history::log_event(
                        &tx,
                        nid,
                        history::EventType::NoteCreated,
                        None,
                        Some(&snapshot),
                        Some(commit),
                        Some(deck_owner),
                    )
                    .await?;
                }
            }
        }

        tx.commit().await?;
    }

    Ok("Success".to_string())
}

pub async fn handle_notes_and_media_update(
    client: &mut SharedConn,
    deck: &AnkiDeck,
    cache: &mut HashMap<String, String>,
    req_ip: &str,
    deck_id: Option<i64>,
    approved: bool,
    commit: i32,
    deck_tree: &Vec<i64>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // fix me later
    if deck_id.is_none() {
        return Err("Deck ID is None".into());
    }
    let safe_deck_id = deck_id.ok_or("Deck ID is None")?; // This is dumb af. idek why I used to keep all that shit in Option<>. There is no scenario in which it should be allowed to have a none deckid.. but who is going to rewrite all that code?

    // Fetch deck owner once for actor attribution on overwrites/new baselines
    let deck_owner_row = client
        .query("SELECT owner FROM decks WHERE id = $1", &[&safe_deck_id])
        .await?;
    let overwrite_actor: Option<i32> = if deck_owner_row.is_empty() {
        None
    } else {
        Some(deck_owner_row[0].get::<usize, i32>(0))
    };

    if let Some(nt) = &deck.note_models {
        for n in nt {
            if !cache.contains_key(&n.crowdanki_uuid) {
                let guid = unpack_notetype(client, n, deck_id).await?;
                cache.insert(n.crowdanki_uuid.clone(), guid);
            }
        }
    }

    let guids: Vec<&str> = deck.notes.iter().map(|note| note.guid.as_str()).collect();
    let note_query = client
        .prepare(
            "
        SELECT n.id, n.guid, n.deck
        FROM notes n
        WHERE n.deck = ANY($1) AND n.guid = ANY($2)
    ",
        )
        .await?;

    // cba to create a struct for this. 0 is the note id, 1 is the deck id where the note is currently stored
    let existing_notes: HashMap<String, (i64, i64)> = client
        .query(&note_query, &[&deck_tree, &guids])
        .await?
        .into_iter()
        .map(|row| {
            (
                row.get::<_, String>("guid"),
                (row.get::<_, i64>("id"), row.get::<_, i64>("deck")),
            )
        })
        .collect();

    if existing_notes.is_empty() {
        // If there are no existing notes, unpack all notes directly
        unpack_notes(
            client,
            deck.notes.iter().collect(),
            cache,
            deck_id,
            approved,
            req_ip,
            commit,
        )
        .await?;
    } else {
        let mut new_notes = Vec::new();
        // Cache a single base commit per base root deck across this bulk for aggregated forwarding
        let mut base_commit_cache: std::collections::HashMap<i64, i32> =
            std::collections::HashMap::new();
        // Cache base deck subtrees discovered in this bulk to avoid repeated recursive root lookups
        let mut base_deck_subtrees: std::collections::HashMap<i64, std::collections::HashSet<i64>> =
            std::collections::HashMap::new();

        for note in &deck.notes {
            match existing_notes.get(&note.guid) {
                None => new_notes.push(note),
                Some(&val) => {
                    if approved {
                        overwrite_note(
                            client,
                            note,
                            val.0,
                            req_ip,
                            commit,
                            safe_deck_id,
                            val.1,
                            overwrite_actor,
                        )
                        .await?;
                    } else {
                        update_note(
                            client,
                            note,
                            val.0,
                            req_ip,
                            commit,
                            safe_deck_id,
                            val.1,
                            &mut base_commit_cache,
                            &mut base_deck_subtrees,
                        )
                        .await?;
                    }
                }
            }
        }

        if !new_notes.is_empty() {
            unpack_notes(client, new_notes, cache, deck_id, approved, req_ip, commit).await?;
        }
    }

    //unpack_media(client, &deck.media_files, deck_id).await?;

    Ok(())
}

#[async_recursion]
pub async fn unpack_deck_data(
    client: &mut SharedConn,
    deck: &AnkiDeck,
    notetype_cache: &mut HashMap<String, String>,
    owner: i32,
    req_ip: &String,
    deck_id: Option<i64>,
    approved: bool,
    commit: i32,
    deck_tree: &Vec<i64>,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // TODO try async with multithreading: Create decks in one, fill in the notes in another
    handle_notes_and_media_update(
        client,
        deck,
        notetype_cache,
        req_ip,
        deck_id,
        approved,
        commit,
        deck_tree,
    )
    .await?;

    for child in &deck.children {
        unpack_deck_json(
            client,
            child,
            notetype_cache,
            owner,
            req_ip,
            deck_id,
            approved,
            commit,
            deck_tree,
        )
        .await?;
    }

    Ok("Success".into())
}

pub async fn check_deck_exists(
    client: &SharedConn,
    deck_name: &String,
    deck_uuid: &str,
    owner: i32,
    parent: Option<i64>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let my_uuid = Uuid::parse_str(deck_uuid)?;
    let hum = humanize(&my_uuid, 5);
    // Normalize deck name to match how we insert (cleaned), so the pre-check is consistent
    let cleaned_deck_name = cleanser::clean(deck_name).to_string();

    if cleaned_deck_name.is_empty() {
        return Err("Deck name is empty. Please provide a valid name.".into());
    }

    let deck_exist_check = {
        client.query("
                SELECT 1 FROM decks WHERE (name = $1 AND owner = $2 AND full_path = (coalesce( (SELECT full_path FROM decks WHERE id = $3 ) || '::' , '') || $1))
                ", &[&cleaned_deck_name, &owner, &parent]).await?
    };

    match deck_exist_check.first() {
        None => Ok(hum),
        Some(_row) => Err(format!(
            "Deck {} already exists. Please submit suggestions instead.",
            &deck_name
        )
        .into()),
    }
}

/// Create the root deck row synchronously and return (deck_id, human_hash).
/// This reserves the final deck hash before background processing starts,
/// avoiding mismatches in the initial response.
pub async fn create_root_deck(
    client: &mut SharedConn,
    deck: &AnkiDeck,
    owner: i32,
    req_ip: &String,
    commit: i32,
) -> Result<(i64, String), Box<dyn std::error::Error + Send + Sync>> {
    let cleaned_deck_name = cleanser::clean(&deck.name).to_string();
    let cleaned_deck_desc = cleanser::clean(&deck.desc).to_string();

    if cleaned_deck_name.is_empty() {
        return Err("Deck name is empty. Please provide a valid name.".into());
    }

    // Ensure no existing deck with same name at the root for this owner
    let deck_exist_check = client
        .query(
            "
                SELECT 1 FROM decks
                WHERE name = $1 AND owner = $2 AND parent IS NULL
            ",
            &[&cleaned_deck_name, &owner],
        )
        .await?;

    if deck_exist_check.first().is_some() {
        return Err(format!(
            "Deck {} already exists. Please submit suggestions instead.",
            &deck.name
        )
        .into());
    }

    // Prepare initial UUID/human hash
    let mut use_uuid = deck.crowdanki_uuid.clone();
    // If provided UUID is invalid, generate a fresh one
    let mut use_hum = match Uuid::parse_str(&use_uuid) {
        Ok(u) => humanize(&u, 5),
        Err(_) => {
            let new_uuid = Uuid::new_v4();
            use_uuid = new_uuid.to_string();
            humanize(&new_uuid, 5)
        }
    };

    let stmt = client
        .prepare(
            "
        INSERT INTO decks (name, description, owner, last_update, parent, crowdanki_uuid, human_hash, creator_ip, full_path)
        VALUES ($1, $2, $3, NOW(), NULL, $4, $5, $6,
            coalesce( (SELECT full_path FROM decks WHERE id = NULL ) || '::' , '') || $1
        )
        RETURNING id
    ",
        )
        .await?;

    // Try insert; on unique violation, retry once with a new UUID/human_hash
    let rows = match client
        .query(
            &stmt,
            &[
                &cleaned_deck_name,
                &cleaned_deck_desc,
                &owner,
                &use_uuid,
                &use_hum,
                &req_ip,
            ],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            let is_unique = e
                .as_db_error()
                .map(|d| d.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION)
                .unwrap_or(false);
            if !is_unique {
                return Err(e.into());
            }
            let new_uuid = Uuid::new_v4();
            use_uuid = new_uuid.to_string();
            use_hum = humanize(&new_uuid, 5);
            client
                .query(
                    &stmt,
                    &[
                        &cleaned_deck_name,
                        &cleaned_deck_desc,
                        &owner,
                        &use_uuid,
                        &use_hum,
                        &req_ip,
                    ],
                )
                .await?
        }
    };

    let id: Option<i64> = rows.first().map(|row| row.get(0));
    let id = id.ok_or_else(|| "Failed to create root deck".to_string())?;

    // Associate the commit with the created deck (only if still NULL)
    client
        .query(
            "UPDATE commits SET deck = $1 WHERE commit_id = $2 AND deck is NULL",
            &[&id, &commit],
        )
        .await?;

    Ok((id, use_hum))
}

#[async_recursion]
pub async fn unpack_deck_json(
    client: &mut SharedConn,
    deck: &AnkiDeck,
    notetype_cache: &mut HashMap<String, String>,
    owner: i32,
    req_ip: &String,
    parent: Option<i64>,
    approved: bool,
    commit: i32,
    deck_tree: &Vec<i64>,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let hum = check_deck_exists(client, &deck.name, &deck.crowdanki_uuid, owner, parent).await?;

    let stmt = client.prepare("
        INSERT INTO decks (name, description, owner, last_update, parent, crowdanki_uuid, human_hash, creator_ip, full_path)
        VALUES ($1, $2, $3, NOW(), $4, $5, $6, $7, 
        coalesce( (SELECT full_path FROM decks WHERE id = $4 ) || '::' , '') || $1
        )
        RETURNING id
    ").await?;

    let cleaned_deck_name = cleanser::clean(&deck.name).to_string();

    if cleaned_deck_name.is_empty() {
        return Err("Deck name is empty. Please provide a valid name.".into());
    }

    let cleaned_deck_desc = cleanser::clean(&deck.desc).to_string();
    // Attempt insert; on unique violation (crowdanki_uuid/human_hash), retry once with a new UUID/human_hash.
    let mut use_uuid = deck.crowdanki_uuid.clone();
    let mut use_hum = hum.clone();
    let rows = match client
        .query(
            &stmt,
            &[
                &cleaned_deck_name,
                &cleaned_deck_desc,
                &owner,
                &parent,
                &use_uuid,
                &use_hum,
                &req_ip,
            ],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            let is_unique = e
                .as_db_error()
                .map(|d| d.code() == &tokio_postgres::error::SqlState::UNIQUE_VIOLATION)
                .unwrap_or(false);
            if !is_unique {
                return Err(e.into());
            }
            // One retry only: fork UUID and derived human hash
            let new_uuid = Uuid::new_v4();
            use_uuid = new_uuid.to_string();
            use_hum = humanize(&new_uuid, 5);
            client
                .query(
                    &stmt,
                    &[
                        &cleaned_deck_name,
                        &cleaned_deck_desc,
                        &owner,
                        &parent,
                        &use_uuid,
                        &use_hum,
                        &req_ip,
                    ],
                )
                .await?
        }
    };

    let id: Option<i64> = rows.first().map(|row| row.get(0));

    if id.is_none() {
        return Err("Deck already exists. Please suggests cards instead (2).".into());
    }

    client
        .query(
            "UPDATE commits SET deck = $1 WHERE commit_id = $2 AND deck is NULL",
            &[&id, &commit],
        )
        .await?;

    match unpack_deck_data(
        client,
        deck,
        notetype_cache,
        owner,
        req_ip,
        id,
        approved,
        commit,
        deck_tree,
    )
    .await
    {
        Ok(_res) => {}
        Err(err) => {
            let db_details = extract_db_error(err.as_ref());
            let deck_params = json!({
                "deck_id": id,
                "owner": owner,
                "approved": approved,
                "commit_id": commit,
            });

            error!(
                deck_id = ?id,
                error = %err,
                db_error = ?db_details,
                params = ?deck_params,
                "Error unpacking deck data"
            );
            sentry::with_scope(
                |scope| {
                    scope.set_fingerprint(Some(&["unpack_deck_data", "failure"]));
                    scope.set_tag("operation", "unpack_deck_data");
                    scope.set_extra("deck_id", format!("{:?}", id).into());
                    scope.set_extra("commit_id", commit.into());
                    scope.set_extra("error_chain", format!("{:#}", err).into());
                    scope.set_extra("deck_params", deck_params.clone().into());
                    if let Some(db) = db_details {
                        scope.set_extra("db_error", db.into());
                    }
                },
                || {
                    sentry::capture_message(
                        &format!("[unpack_deck_data] Deck unpacking failed: {}", err),
                        sentry::Level::Error,
                    );
                },
            );
            // Don't propagate - partial success is acceptable
        }
    }

    Ok(use_hum)
}
