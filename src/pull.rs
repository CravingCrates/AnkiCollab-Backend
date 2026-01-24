use std::fmt::Write;
use std::sync::Arc;

use crate::database;
use crate::notetypes::{get_notetypes, pull_protected_fields};
use crate::structs::{AnkiDeck, Note, UpdateInfoResponse};
use std::collections::{HashMap, HashSet};

use database::SharedConn;

/// Optimized deck tree assembly that maintains deterministic ordering
fn assemble_deck_tree(
    parent_id: i64,
    all_decks: &mut HashMap<i64, AnkiDeck>,
    child_map: &HashMap<i64, Vec<i64>>,
) -> Vec<AnkiDeck> {
    if let Some(child_ids) = child_map.get(&parent_id) {
        // Sort child IDs to ensure deterministic ordering
        let mut sorted_child_ids = child_ids.clone();
        sorted_child_ids.sort();

        sorted_child_ids
            .into_iter()
            .filter_map(|child_id| {
                if let Some(mut child_deck) = all_decks.remove(&child_id) {
                    child_deck.children = assemble_deck_tree(child_id, all_decks, child_map);
                    Some(child_deck)
                } else {
                    None
                }
            })
            .collect()
    } else {
        Vec::new()
    }
}

/// Fetches deck hierarchy structure without notes (for building parent-child relationships)
async fn get_deck_hierarchy_structure(
    client: &SharedConn,
    root_id: i64,
    timestamp: &str,
) -> Result<
    (
        HashMap<i64, (String, String, String, i32)>,
        HashMap<i64, Vec<i64>>,
    ),
    Box<dyn std::error::Error + Send + Sync>,
> {
    let hierarchy_query = client
        .prepare(
            "
        WITH RECURSIVE descendant_decks AS (
            SELECT id, parent, name, description, crowdanki_uuid, owner
            FROM decks WHERE id = $1
            UNION ALL
            SELECT d.id, d.parent, d.name, d.description, d.crowdanki_uuid, d.owner
            FROM decks d 
            INNER JOIN descendant_decks dd ON d.parent = dd.id
        )
        SELECT d.id, d.parent, d.name, d.description, d.crowdanki_uuid, d.owner
        FROM descendant_decks d
        JOIN decks original_decks ON d.id = original_decks.id
        WHERE original_decks.last_update > to_timestamp($2, 'YYYY-MM-DD hh24:mi:ss')::timestamptz
        ORDER BY d.id
    ",
        )
        .await?;

    let deck_rows = client
        .query(&hierarchy_query, &[&root_id, &timestamp])
        .await?;

    let mut deck_metadata: HashMap<i64, (String, String, String, i32)> = HashMap::new();
    let mut child_map: HashMap<i64, Vec<i64>> = HashMap::new();

    for row in deck_rows {
        let id: i64 = row.get("id");
        let parent: Option<i64> = row.get("parent");

        deck_metadata.insert(
            id,
            (
                row.get("name"),
                row.get("description"),
                row.get("crowdanki_uuid"),
                row.get("owner"),
            ),
        );

        if let Some(p_id) = parent {
            child_map.entry(p_id).or_default().push(id);
        }
    }

    Ok((deck_metadata, child_map))
}

/// Creates a complete deck with notes and notetypes
async fn create_deck_with_content(
    client: &SharedConn,
    deck_id: i64,
    name: String,
    description: String,
    crowdanki_uuid: String,
    owner: i32,
    timestamp: &str,
) -> Result<AnkiDeck, Box<dyn std::error::Error + Send + Sync>> {
    let notes = match get_changed_notes(client, deck_id, timestamp).await {
        Ok(n) => n,
        Err(e) => {
            eprintln!("Error fetching changed notes for deck {}: {}", deck_id, e);
            return Err(e);
        }
    };
    let notetypes = if !notes.is_empty() {
        get_notetypes(client, &notes, owner).await
    } else {
        Vec::new()
    };

    Ok(AnkiDeck {
        crowdanki_uuid,
        name,
        desc: description,
        children: Vec::new(), // Will be filled by assemble_deck_tree
        notes,
        note_models: Some(notetypes),
    })
}

async fn discover_deleted_notes(
    client: &SharedConn,
    root_deck_id: i64,
    deleted_notes: &mut Vec<String>,
    timestamp: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let deleted_notes_query = client
        .prepare(
            "
        WITH RECURSIVE descendant_decks AS (
            SELECT id FROM decks WHERE id = $1
            UNION ALL
            SELECT d.id FROM decks d
            INNER JOIN descendant_decks dd ON d.parent = dd.id
        )
        SELECT guid
        FROM notes
        WHERE deck IN (SELECT id FROM descendant_decks)
          AND deleted = true
          AND last_update > to_timestamp($2, 'YYYY-MM-DD hh24:mi:ss')::timestamptz
    ",
        )
        .await?;

    let deleted_notes_rows = client
        .query(&deleted_notes_query, &[&root_deck_id, &timestamp])
        .await?;

    // Efficiently extend the vector from the query results.
    deleted_notes.extend(deleted_notes_rows.into_iter().map(|row| row.get(0)));

    Ok(())
}

async fn get_changed_notes(
    client: &SharedConn,
    deck_id: i64,
    input_date: &str,
) -> Result<Vec<Note>, Box<dyn std::error::Error + Send + Sync>> {
    let mut notes = Vec::new();
    let mut current_cursor: Option<i64> = None;

    // 5000 limit idk just a random number i picked, no research was done tbbh
    let notes_query = client
        .prepare(
            "
            SELECT n.id, n.guid, nt.guid, nt.id, n.anki_id
            FROM notes AS n
            LEFT JOIN notetype AS nt ON n.notetype = nt.id
            WHERE n.deck = $1 AND n.reviewed = true AND n.deleted = false
            AND n.last_update > to_timestamp($2, 'YYYY-MM-DD hh24:mi:ss')::timestamptz
            AND ($3::bigint IS NULL OR n.id > $3)
            ORDER BY n.id
            LIMIT 5000
        ",
        )
        .await?;

    let fields_query = client
        .prepare(
            "
            SELECT note, content, position
            FROM fields
            WHERE note = ANY($1) AND reviewed = true AND content <> ''
            ORDER BY note, position
        ",
        )
        .await?;

    let tags_query = client
        .prepare(
            "
            SELECT note, content
            FROM tags
            WHERE note = ANY($1) AND reviewed = true
        ",
        )
        .await?;

    let notetype_size_query = client
        .prepare("SELECT count(1)::int from notetype_field where notetype = $1")
        .await?;

    let mut notetype_sizes: HashMap<i64, i32> = HashMap::new();

    let inheritance_query = client
        .prepare("SELECT subscriber_note_id, base_note_id, subscribed_fields, removed_base_tags FROM anki.note_inheritance WHERE subscriber_note_id = ANY($1)")
        .await?;

    let base_fields_query = client
        .prepare("SELECT note, content, position FROM fields WHERE note = ANY($1) AND reviewed = true ORDER BY note, position")
        .await?;

    let base_tags_query = client
        .prepare("SELECT note, content FROM tags WHERE note = ANY($1) AND reviewed = true")
        .await?;

    loop {
        let notes_batch = client
            .query(&notes_query, &[&deck_id, &input_date, &current_cursor])
            .await?;

        if notes_batch.is_empty() {
            break;
        }

        let note_ids: Vec<i64> = notes_batch.iter().map(|row| row.get(0)).collect();

        // Fetch fields and tags for the entire batch
        let fields_rows = client.query(&fields_query, &[&note_ids]).await?;
        let tags_rows = client.query(&tags_query, &[&note_ids]).await?;

        // Fetch inheritance links for this batch
        let inh_rows = client.query(&inheritance_query, &[&note_ids]).await?;

        let mut inheritance_map: HashMap<i64, (i64, Option<Vec<i32>>, Vec<String>)> =
            HashMap::new();
        let mut base_ids: Vec<i64> = Vec::new();
        for r in inh_rows {
            let sub_id: i64 = r.get(0);
            let base_id: i64 = r.get(1);
            let subs: Option<Vec<i32>> = r.get(2);
            let removed: Option<Vec<String>> = r.get(3);
            inheritance_map.insert(sub_id, (base_id, subs, removed.unwrap_or_default()));
            base_ids.push(base_id);
        }

        // Batch fetch base fields/tags if needed
        let mut base_fields_map: HashMap<i64, Vec<(String, u32)>> = HashMap::new();
        let mut base_tags_map: HashMap<i64, Vec<String>> = HashMap::new();
        if !base_ids.is_empty() {
            let base_fields_rows = client.query(&base_fields_query, &[&base_ids]).await?;
            for row in base_fields_rows {
                base_fields_map
                    .entry(row.get(0))
                    .or_default()
                    .push((row.get(1), row.get(2)));
            }
            let base_tags_rows = client.query(&base_tags_query, &[&base_ids]).await?;
            for row in base_tags_rows {
                base_tags_map
                    .entry(row.get(0))
                    .or_default()
                    .push(row.get(1));
            }
        }

        // Process fields
        let mut fields_map: HashMap<i64, Vec<(String, u32)>> = HashMap::new();
        for row in fields_rows {
            let note_id: i64 = row.get(0);
            let content: String = row.get(1);
            let position: u32 = row.get(2);
            fields_map
                .entry(note_id)
                .or_default()
                .push((content, position));
        }

        // Process tags
        let mut tags_map: HashMap<i64, Vec<String>> = HashMap::new();
        for row in tags_rows {
            let note_id: i64 = row.get(0);
            let content: String = row.get(1);
            tags_map.entry(note_id).or_default().push(content);
        }

        // Process notes
        for note_row in &notes_batch {
            let note_id: i64 = note_row.get(0);
            let guid: String = note_row.get(1);
            let note_model_uuid: String = note_row.get(2);
            let note_model_id: i64 = note_row.get(3);
            let anki_id: i64 = note_row.get(4);

            let notetype_size = if let Some(&size) = notetype_sizes.get(&note_model_id) {
                size
            } else {
                let notetype_size_rows = client
                    .query(&notetype_size_query, &[&note_model_id])
                    .await?;
                let notetype_size: i32 = notetype_size_rows[0].get(0);
                notetype_sizes.insert(note_model_id, notetype_size);
                notetype_size
            };

            let mut fields = vec![String::new(); notetype_size as usize];
            if let Some(note_fields) = fields_map.get(&note_id) {
                for (content, position) in note_fields {
                    if (*position as usize) < fields.len() {
                        fields[*position as usize] = content.clone();
                    } else {
                        println!(
                            "Invalid field position: {position}; note_id: {note_id}; model id: {note_model_id}"
                        );
                    }
                }
            } else {
                println!("No fields found for note: {note_id}");
                continue;
            }

            let mut tags = tags_map.get(&note_id).cloned().unwrap_or_default();

            // Merge inheritance if exists for this note
            if let Some((base_id, subscribed_fields_opt, removed_base_tags)) =
                inheritance_map.get(&note_id)
            {
                // Prepare base fields vec as position->content map
                let mut base_pos_map: HashMap<u32, String> = HashMap::new();
                if let Some(list) = base_fields_map.get(base_id) {
                    for (content, pos) in list {
                        base_pos_map.insert(*pos, content.clone());
                    }
                }
                // Overwrite fields
                match subscribed_fields_opt {
                    None => {
                        // subscribe all
                        for idx in 0..fields.len() {
                            let pos = idx as u32;
                            if let Some(val) = base_pos_map.get(&pos) {
                                fields[idx] = val.clone();
                            } else {
                                // If no value found, clear the field
                                fields[idx] = String::new();
                            }
                        }
                    }
                    Some(ords) => {
                        for ord in ords {
                            if *ord >= 0 {
                                let idx = *ord as usize;
                                if idx < fields.len() {
                                    let pos = *ord as u32;
                                    if let Some(val) = base_pos_map.get(&pos) {
                                        fields[idx] = val.clone();
                                    }
                                    // If no value found, clear the field
                                    else {
                                        fields[idx] = String::new();
                                    }
                                }
                            }
                        }
                    }
                }
                // Merge tags: base minus removed_base_tags, then union local
                let mut final_tags = std::collections::HashSet::new();
                if let Some(base_tags) = base_tags_map.get(base_id) {
                    for t in base_tags {
                        if !removed_base_tags.contains(t) {
                            final_tags.insert(t.clone());
                        }
                    }
                }
                for t in tags {
                    final_tags.insert(t);
                }
                tags = final_tags.into_iter().collect();
            }

            // Skip all-empty fields
            if fields.iter().all(|f| f.is_empty()) {
                continue;
            }

            notes.push(Note {
                id: anki_id,
                fields,
                guid,
                note_model_uuid,
                tags,
            });
        }

        current_cursor = notes_batch.last().map(|row| row.get::<_, i64>(0));
    }

    Ok(notes)
}

pub async fn pull_changes(
    db_state: &Arc<database::AppState>,
    deck_hash: &String,
    timestamp: &String,
) -> std::result::Result<UpdateInfoResponse, Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get_owned().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Failed to retrieve a pooled connection".into());
        }
    };

    // Get root deck info and verify it exists with updates
    let root_deck_query = client.prepare("
        SELECT id, name, description, crowdanki_uuid, owner, stats_enabled
        FROM decks
        WHERE human_hash = $1 AND last_update > to_timestamp( $2 , 'YYYY-MM-DD hh24:mi:ss')::timestamptz
    ").await?;

    let rows = client
        .query(&root_deck_query, &[&deck_hash, &timestamp])
        .await?;
    let row = rows.first().ok_or("No deck found")?;

    let root_id: i64 = row.get("id");
    let name: String = row.get("name");
    let description: String = row.get("description");
    let crowdanki_uuid: String = row.get("crowdanki_uuid");
    let stats_enabled: bool = row.get("stats_enabled");
    let owner: i32 = row.get("owner");

    // Get hierarchy structure and build child decks
    let (deck_metadata, child_map) =
        get_deck_hierarchy_structure(&client, root_id, timestamp).await?;
    let mut all_decks_map = HashMap::new();

    // Create all child decks with their content
    for (&deck_id, (deck_name, deck_desc, deck_uuid, deck_owner)) in &deck_metadata {
        if deck_id != root_id {
            // Skip root deck, we'll handle it separately
            let deck = create_deck_with_content(
                &client,
                deck_id,
                deck_name.clone(),
                deck_desc.clone(),
                deck_uuid.clone(),
                *deck_owner,
                timestamp,
            )
            .await?;
            all_decks_map.insert(deck_id, deck);
        }
    }

    // Create root deck with content
    let mut root_deck = create_deck_with_content(
        &client,
        root_id,
        name,
        description,
        crowdanki_uuid,
        owner,
        timestamp,
    )
    .await?;

    // Assemble the hierarchy
    root_deck.children = assemble_deck_tree(root_id, &mut all_decks_map, &child_map);

    // Fetch remaining metadata
    let nt = pull_protected_fields(&client, deck_hash).await?;
    let optional_tags = get_optional_tag_groups(&client, root_id).await?;

    let mut deleted_notes = Vec::new();
    discover_deleted_notes(&client, root_id, &mut deleted_notes, timestamp).await?;

    Ok(UpdateInfoResponse {
        protected_fields: nt,
        deck: root_deck,
        changelog: get_changelog_info(&client, root_id, timestamp)
            .await
            .unwrap_or_else(|e| {
                eprintln!("Failed to retrieve changelog info: {}", e);
                String::new()
            }),
        deck_hash: deck_hash.to_string(),
        optional_tags,
        deleted_notes,
        stats_enabled,
    })
}

pub async fn get_changelog_info(
    client: &SharedConn,
    deck_id: i64,
    last_timestamp: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let rows = client.query(
        "SELECT message, TO_CHAR(timestamp, 'MM/DD/YYYY HH24:MI') AS timestamp FROM changelogs WHERE deck = $1 AND timestamp > to_timestamp($2, 'YYYY-MM-DD hh24:mi:ss')::timestamptz ORDER BY timestamp DESC",
        &[&deck_id, &last_timestamp],
    ).await?;

    let changelog_string = rows.iter().fold(String::new(), |mut acc, row| {
        let message: String = row.get(0);
        let timestamp: String = row.get(1);
        write!(acc, "--- Changes from {timestamp}: ---\n{message}\n\n").unwrap();
        acc
    });

    Ok(changelog_string)
}
async fn get_optional_tag_groups(
    client: &SharedConn,
    deck: i64,
) -> std::result::Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let rows = client
        .query(
            "SELECT tag_group FROM optional_tags WHERE deck = $1",
            &[&deck],
        )
        .await?
        .into_iter()
        .map(|row| row.get::<_, String>("tag_group"))
        .collect::<Vec<String>>();

    Ok(rows)
}

pub async fn get_id_from_username(
    db_state: &Arc<database::AppState>,
    username: String,
) -> std::result::Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Failed to retrieve a pooled connection".into());
        }
    };
    let normalized_username = username.to_lowercase();
    let rows = client
        .query(
            "SELECT id FROM users WHERE username = $1",
            &[&normalized_username],
        )
        .await?;

    if rows.is_empty() {
        return Err("User does not exist".to_string().into());
    }

    Ok(rows[0].get(0))
}

pub async fn get_deck_last_update_unix(
    db_state: &Arc<database::AppState>,
    deck_hash: &str,
) -> Result<f64, Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Failed to retrieve a pooled connection".into());
        }
    };

    let result = client.query_opt(
        "SELECT CAST(EXTRACT(EPOCH FROM last_update) AS numeric)::text FROM decks WHERE human_hash = $1",
        &[&deck_hash],
    ).await?;
    let result = result.map_or(0.0, |row| {
        row.get::<usize, String>(0).parse().unwrap_or(0.0)
    });
    Ok(result)
}

pub async fn check_deck_alive(
    db_state: &Arc<database::AppState>,
    deck_hashes: Vec<String>,
) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Failed to retrieve a pooled connection".into());
        }
    };

    let stmt = client
        .prepare(
            "
        SELECT human_hash
        FROM decks
        WHERE human_hash = ANY($1)
    ",
        )
        .await?;

    let rows = client.query(&stmt, &[&deck_hashes]).await?;
    let existing_hashes: HashSet<String> = rows.iter().map(|row| row.get("human_hash")).collect();
    let missing_hashes: Vec<String> = deck_hashes
        .into_iter()
        .filter(|hash| !existing_hashes.contains(hash))
        .collect();

    Ok(missing_hashes)
}
