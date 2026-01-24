use std::collections::{HashMap, HashSet};

use crate::cleanser;
use crate::database;
use crate::media_reference_manager::update_media_references_for_note;
use crate::notetypes::does_notetype_exist;
use crate::push;
use crate::structs::{AnkiDeck, Note};
use tracing::{error, warn};

use async_recursion::async_recursion;
use once_cell::sync::Lazy;
use regex::Regex;
use serde_json::json;
use std::sync::Arc; // for constructing history payloads

// --- Embedded minimal history module (mirrors main backend note_history) ---
pub mod history {
    use serde_json::Value as JsonValue;
    use tokio_postgres::Transaction;

    #[derive(Clone, Copy)]
    pub enum EventType {
        NoteCreated,
        FieldAdded,
        FieldUpdated,
        FieldRemoved,
        TagAdded,
        TagRemoved,
        NoteMoved,
    }

    impl EventType {
        pub fn as_str(&self) -> &'static str {
            match self {
                EventType::NoteCreated => "note_created",
                EventType::FieldAdded => "field_added",
                EventType::FieldUpdated => "field_updated",
                EventType::FieldRemoved => "field_removed",
                EventType::TagAdded => "tag_added",
                EventType::TagRemoved => "tag_removed",
                EventType::NoteMoved => "note_moved",
            }
        }
    }

    pub async fn log_event(
        tx: &Transaction<'_>,
        note_id: i64,
        event_type: EventType,
        old_value: Option<&JsonValue>,
        new_value: Option<&JsonValue>,
        commit_id: Option<i32>,
        actor_user_id: Option<i32>,
    ) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
        let row = tx
            .query_one(
                "UPDATE notes SET version = version + 1 WHERE id = $1 RETURNING version",
                &[&note_id],
            )
            .await?;
        let version: i64 = row.get(0);

        let inserted = tx
            .query_one(
                "INSERT INTO note_events (note_id, version, event_type, actor_user_id, commit_id, approved, old_value, new_value) \
                 VALUES ($1,$2,$3,$4,$5,$6,$7,$8) RETURNING id",
                &[
                    &note_id,
                    &version,
                    &event_type.as_str(),
                    &actor_user_id,            // maintainer / deck owner id when available
                    &commit_id,
                    &Some(true),               // overwrite implies approved
                    &old_value,
                    &new_value,
                ],
            )
            .await?;
        Ok(inserted.get(0))
    }
}

use bb8_postgres::bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
type SharedConn = PooledConnection<'static, PostgresConnectionManager<tokio_postgres::NoTls>>;

/// Validates that a note has at least one field (any position).
/// Returns true if the note has at least one field, false otherwise.
pub async fn validate_note_has_any_field(
    tx: &tokio_postgres::Transaction<'_>,
    note_id: i64,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let row = tx
        .query_opt(
            "SELECT 1 FROM fields WHERE note = $1 LIMIT 1",
            &[&note_id],
        )
        .await?;
    Ok(row.is_some())
}

/*
type DeckTreeCache = Arc<Mutex<Vec<i32>>>;

async fn precompute_deck_tree(client: &mut SharedConn, deck_id: i64, cache: DeckTreeCache) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut deck_tree = Vec::new();
    let mut to_visit = vec![deck_id];

    while let Some(current_deck_id) = to_visit.pop() {
        deck_tree.push(current_deck_id);

        // Find parent
        if let Some(row) = client.query_opt("SELECT parent FROM decks WHERE id = $1", &[&current_deck_id]).await? {
            if let Some(parent_id) = row.get::<_, Option<i64>>(0) {
                if !deck_tree.contains(&parent_id) {
                    to_visit.push(parent_id);
                }
            }
        }

        // Find children
        let rows = client.query("SELECT id FROM decks WHERE parent = $1", &[&current_deck_id]).await?;
        for row in rows {
            let child_deck_id: i64 = row.get(0);
            if !deck_tree.contains(&child_deck_id) {
                to_visit.push(child_deck_id);
            }
        }
    }

    // Cache the deck tree
    let mut cache_lock = cache.lock().await;
    *cache_lock = deck_tree;

    Ok(())
}

*/

pub async fn update_note_timestamp(
    tx: &tokio_postgres::Transaction<'_>,
    note_id: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let query1 = "
    WITH RECURSIVE tree AS (
        SELECT id, last_update, parent FROM decks
        WHERE id = (SELECT deck FROM notes WHERE id = $1)
        UNION ALL
        SELECT d.id, d.last_update, d.parent FROM decks d
        JOIN tree t ON d.id = t.parent
    )
    UPDATE decks
    SET last_update = NOW()
    WHERE id IN (SELECT id FROM tree)";

    let query2 = "UPDATE notes SET last_update = NOW() WHERE id = $1";

    tx.execute(query1, &[&note_id]).await?;
    tx.execute(query2, &[&note_id]).await?;

    Ok(())
}

async fn bump_linked_subscribers(
    tx: &tokio_postgres::Transaction<'_>,
    base_note_id: i64,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Batch-update timestamps for all subscriber notes and their ancestor decks in one go
    let rows = tx
        .query(
            "SELECT subscriber_note_id FROM anki.note_inheritance WHERE base_note_id = $1",
            &[&base_note_id],
        )
        .await?;
    if rows.is_empty() {
        return Ok(());
    }

    let sub_ids: Vec<i64> = rows.into_iter().map(|r| r.get(0)).collect();

    // 1) Update all subscriber notes' timestamps at once
    tx.execute(
        "UPDATE notes SET last_update = NOW() WHERE id = ANY($1::bigint[])",
        &[&sub_ids],
    )
    .await?;

    // 2) Bump all involved decks (and their ancestors) once via a single recursive CTE
    let update_decks_ts = "
        WITH RECURSIVE roots AS (
            SELECT DISTINCT n.deck AS id FROM notes n WHERE n.id = ANY($1::bigint[])
        ),
        up AS (
            SELECT d.id, d.parent FROM decks d JOIN roots r ON d.id = r.id
            UNION ALL
            SELECT d.id, d.parent FROM decks d JOIN up ON d.id = up.parent
        )
        UPDATE decks SET last_update = NOW() WHERE id IN (SELECT id FROM up)
    ";
    tx.execute(update_decks_ts, &[&sub_ids]).await?;

    Ok(())
}

pub async fn is_valid_optional_tag(
    tx: &tokio_postgres::Transaction<'_>,
    deck: &i64,
    tag: &str,
) -> std::result::Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    static OPTIONAL_TAG_RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"AnkiCollab_Optional::(?P<tag_group>[^:]+)(::(?P<subtag>[^:]+))?").unwrap()
    });

    if let Some(caps) = OPTIONAL_TAG_RE.captures(tag) {
        let tag_group = match caps.name("tag_group") {
            Some(t) => t.as_str(),
            None => return Ok(false),
        };

        let rows = tx
            .query(
                "SELECT id FROM optional_tags WHERE deck = $1 AND tag_group = $2",
                &[&deck, &tag_group],
            )
            .await?;

        Ok(!rows.is_empty())
    } else {
        Ok(false)
    }
}

pub async fn get_topmost_deck_by_note_id(
    tx: &tokio_postgres::Transaction<'_>,
    note_id: i64,
) -> std::result::Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let rows = tx
        .query(
            "
        WITH RECURSIVE deck_hierarchy AS (
            SELECT decks.id, parent
            FROM decks
            JOIN notes ON decks.id = notes.deck
            WHERE notes.id = $1
            UNION
            SELECT decks.id, decks.parent
            FROM decks
            JOIN deck_hierarchy ON deck_hierarchy.parent = decks.id
        )
        SELECT id
        FROM deck_hierarchy
        WHERE parent IS NULL      
    ",
            &[&note_id],
        )
        .await?;

    if rows.is_empty() {
        return Err("No deck found for note".into());
    }

    Ok(rows[0].get(0))
}

async fn force_overwrite_tag(
    client: &mut SharedConn,
    note_id: i64,
    new_content: &[String],
    req_ip: &str,
    commit: i32,
    reviewed: bool,
    actor_user_id: Option<i32>,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tx = client.transaction().await?;

    let query = "SELECT content from tags where note = $1 and reviewed = $2";
    let old_tags = tx
        .query(query, &[&note_id, &reviewed])
        .await?
        .into_iter()
        .map(|row| row.get::<_, String>("content"))
        .collect::<Vec<String>>();

    // HashSet could be faster in theory, but since our vecs are expected to be very small (< 100 tags on average) this is probably more performant
    let new_tags: Vec<_> = new_content
        .iter()
        .filter(|item| !old_tags.contains(item))
        .collect();
    let removed_tags: Vec<_> = old_tags
        .into_iter()
        .filter(|item| !new_content.contains(item))
        .collect();

    let insert_new_tag = tx.prepare("
        INSERT INTO tags (note, content, reviewed, creator_ip, action, commit) VALUES ($1, $2, $3, $4, true, $5)
    ").await?;

    let remove_new_tag = tx
        .prepare(
            "
        DELETE FROM tags WHERE note = $1 and content = $2
    ",
        )
        .await?;

    let deck_id = get_topmost_deck_by_note_id(&tx, note_id).await?;

    let mut added_tags: Vec<String> = Vec::new();
    for tag in new_tags {
        if tag.starts_with("AnkiCollab_Optional::")
            && !is_valid_optional_tag(&tx, &deck_id, tag).await?
        {
            continue; // Invalid Optional tag!
        }
        let con = cleanser::clean(tag);
        tx.execute(
            &insert_new_tag,
            &[&note_id, &con, &reviewed, &req_ip, &commit],
        )
        .await?;
        added_tags.push(con);
    }

    let mut removed_tags_clean: Vec<String> = Vec::new();
    for tag in removed_tags {
        let con = cleanser::clean(&tag);
        tx.execute(&remove_new_tag, &[&note_id, &con]).await?;
        removed_tags_clean.push(con);
    }

    // Emit tag events
    for t in added_tags {
        let _ = history::log_event(
            &tx,
            note_id,
            history::EventType::TagAdded,
            None,
            Some(&json!({"content": t})),
            Some(commit),
            actor_user_id,
        )
        .await?;
    }
    for t in removed_tags_clean {
        let _ = history::log_event(
            &tx,
            note_id,
            history::EventType::TagRemoved,
            Some(&json!({"content": t})),
            None,
            Some(commit),
            actor_user_id,
        )
        .await?;
    }

    tx.commit().await?;

    Ok(())
}

async fn check_tag(
    client: &mut SharedConn,
    note_id: i64,
    new_content: &[String],
    req_ip: &str,
    commit: i32,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let tx = client.transaction().await?;

    let query = "SELECT content from tags where note = $1 and reviewed = true";
    let old_tags = tx
        .query(query, &[&note_id])
        .await?
        .into_iter()
        .map(|row| row.get::<_, String>("content"))
        .collect::<Vec<String>>();

    // HashSet could be faster in theory, but since our vecs are expected to be very small (< 100 tags on average) this is probably more performant
    let new_tags: Vec<_> = new_content
        .iter()
        .filter(|item| !old_tags.contains(item))
        .collect();
    let removed_tags: Vec<_> = old_tags
        .into_iter()
        .filter(|item| !new_content.contains(item))
        .collect();

    let insert_new_tag = tx.prepare("
        INSERT INTO tags (note, content, reviewed, creator_ip, action, commit) VALUES ($1, $2, false, $3, true, $4)
    ").await?;

    let remove_new_tag = tx.prepare("
        INSERT INTO tags (note, content, reviewed, creator_ip, action, commit) VALUES ($1, $2, false, $3, false, $4)
    ").await?;

    let deck_id = get_topmost_deck_by_note_id(&tx, note_id).await?;

    for tag in new_tags {
        if tag.starts_with("AnkiCollab_Optional::")
            && !is_valid_optional_tag(&tx, &deck_id, tag).await?
        {
            continue; // Invalid Optional tag!
        }
        let con = cleanser::clean(tag);
        tx.execute(&insert_new_tag, &[&note_id, &con, &req_ip, &commit])
            .await?;
    }

    for tag in removed_tags {
        let con = cleanser::clean(&tag);
        tx.execute(&remove_new_tag, &[&note_id, &con, &req_ip, &commit])
            .await?;
    }

    tx.commit().await?;

    Ok(())
}

async fn get_inheritance_info(
    client: &SharedConn,
    subscriber_note_id: i64,
) -> Result<Option<(i64, Option<Vec<i32>>)>, Box<dyn std::error::Error + Send + Sync>> {
    let row = client.query_opt(
        "SELECT base_note_id, subscribed_fields FROM anki.note_inheritance WHERE subscriber_note_id = $1",
        &[&subscriber_note_id],
    ).await?;
    if let Some(r) = row {
        Ok(Some((r.get(0), r.get::<_, Option<Vec<i32>>>(1))))
    } else {
        Ok(None)
    }
}

async fn ensure_base_commit(
    client: &mut SharedConn,
    base_note_id: i64,
    ip: &str,
    committing_user: Option<i32>,
    source_deck_name: Option<String>,
    original_rationale: Option<i32>,
    original_comment: Option<String>,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    // Create a commit and attach it to the base note's deck using the current connection
    let mut description = match source_deck_name {
        Some(name) if !name.is_empty() => format!("Forwarded from subscriber deck: {}", name),
        _ => "Forwarded from subscriber deck".to_string(),
    };
    if let Some(orig) = original_comment {
        let trimmed = orig.trim();
        if !trimmed.is_empty() {
            // Append original comment; keep it short if excessively long to avoid bloating the forwarded commit.
            // Use character-based truncation to avoid panics on multi-byte UTF-8 boundaries.
            const MAX_CHARS: usize = 800; // safeguard; adjust if schema enforces a different limit
            let snippet: String = trimmed.chars().take(MAX_CHARS).collect();
            description.push_str(" | Original comment: ");
            description.push_str(&snippet);
            if trimmed.chars().count() > MAX_CHARS {
                description.push_str(" …");
            }
        }
    }
    let content = cleanser::clean(&description);
    let rationale_code = original_rationale.unwrap_or(13);
    let row = client.query_one(
        "INSERT INTO commits (rationale, info, ip, timestamp, user_id) VALUES ($1, $2, $3, NOW(), $4) RETURNING commit_id",
        &[&rationale_code, &content, &ip, &committing_user],
    ).await?;
    let commit_id: i32 = row.get(0);
    // Attach commit to the topmost deck for this note (root of the deck tree), not the immediate deck
    let deck_id: i64 = client
        .query_one(
            "WITH RECURSIVE deck_hierarchy AS (
            SELECT decks.id, parent
            FROM decks
            JOIN notes ON decks.id = notes.deck
            WHERE notes.id = $1
            UNION
            SELECT d2.id, d2.parent
            FROM decks d2
            JOIN deck_hierarchy dh ON dh.parent = d2.id
        )
        SELECT id
        FROM deck_hierarchy
        WHERE parent IS NULL",
            &[&base_note_id],
        )
        .await?
        .get(0);
    client
        .execute(
            "UPDATE commits SET deck = $1 WHERE commit_id = $2",
            &[&deck_id, &commit_id],
        )
        .await?;
    Ok(commit_id)
}

async fn route_inherited_field_suggestions(
    client: &mut SharedConn,
    note: &Note,
    subscriber_note_id: i64,
    base_note_id: i64,
    subscribed_fields: &Option<Vec<i32>>,
    req_ip: &str,
    local_commit: i32,
    base_commit: Option<i32>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Batch inserts with validation against notetype_field (protected=false and valid position)
    let mut base_pos: Vec<u32> = Vec::new();
    let mut base_val: Vec<String> = Vec::new();
    let mut local_pos: Vec<u32> = Vec::new();
    let mut local_val: Vec<String> = Vec::new();

    // Load existing reviewed contents for comparison to avoid suggesting unchanged fields
    let base_rows = client
        .query(
            "SELECT position, content FROM fields WHERE note = $1 AND reviewed = true",
            &[&base_note_id],
        )
        .await?;
    let local_rows = client
        .query(
            "SELECT position, content FROM fields WHERE note = $1 AND reviewed = true",
            &[&subscriber_note_id],
        )
        .await?;
    let base_map: HashMap<u32, String> = base_rows
        .into_iter()
        .map(|r| (r.get::<_, u32>(0), r.get::<_, String>(1)))
        .collect();
    let local_map: HashMap<u32, String> = local_rows
        .into_iter()
        .map(|r| (r.get::<_, u32>(0), r.get::<_, String>(1)))
        .collect();

    let is_subscribed = |idx: i32| match subscribed_fields {
        None => true,
        Some(v) => v.contains(&idx),
    };
    for (idx, raw) in note.fields.iter().enumerate() {
        let pos = idx as i32; // How bad am I at coding
        let upos = idx as u32;
        let cleaned = cleanser::clean(raw);
        if cleaned.is_empty() {
            continue;
        }
        if is_subscribed(pos) && base_commit.is_some() {
            // Only forward if changed from base reviewed content
            if base_map.get(&upos).map(|v| v != &cleaned).unwrap_or(true) {
                base_pos.push(upos);
                base_val.push(cleaned);
            }
        } else {
            // either not subscribed or base commit unavailable → keep local
            if local_map.get(&upos).map(|v| v != &cleaned).unwrap_or(true) {
                local_pos.push(upos);
                local_val.push(cleanser::clean(raw));
            }
        }
    }

    if !base_pos.is_empty() {
        let bc = base_commit.unwrap();
        client.execute(
            "WITH data AS (
                SELECT unnest($1::oid[]) AS position, unnest($2::text[]) AS content
            )
            INSERT INTO fields (note, position, content, creator_ip, commit, reviewed)
            SELECT $3::bigint, d.position, d.content, $4::text, $5::int, false
            FROM data d
            JOIN notes n ON n.id = $3::bigint
            JOIN notetype_field nf ON nf.notetype = n.notetype AND nf.position = d.position AND nf.protected = false
            LEFT JOIN fields f_rev ON f_rev.note = $3::bigint AND f_rev.position = d.position AND f_rev.reviewed = true
            LEFT JOIN fields f_unr ON f_unr.note = $3::bigint AND f_unr.position = d.position AND f_unr.reviewed = false AND f_unr.content = d.content
            WHERE d.content <> ''
              AND COALESCE(f_rev.content, '') <> d.content
              AND f_unr.note IS NULL",
            &[&base_pos, &base_val, &base_note_id, &req_ip, &bc],
        ).await?;
    }
    if !local_pos.is_empty() {
        client.execute(
            "WITH data AS (
                SELECT unnest($1::oid[]) AS position, unnest($2::text[]) AS content
            )
            INSERT INTO fields (note, position, content, creator_ip, commit, reviewed)
            SELECT $3::bigint, d.position, d.content, $4::text, $5::int, false
            FROM data d
            JOIN notes n ON n.id = $3::bigint
            JOIN notetype_field nf ON nf.notetype = n.notetype AND nf.position = d.position AND nf.protected = false
            LEFT JOIN fields f_rev ON f_rev.note = $3::bigint AND f_rev.position = d.position AND f_rev.reviewed = true
            LEFT JOIN fields f_unr ON f_unr.note = $3::bigint AND f_unr.position = d.position AND f_unr.reviewed = false AND f_unr.content = d.content
            WHERE d.content <> ''
              AND COALESCE(f_rev.content, '') <> d.content
              AND f_unr.note IS NULL",
            &[&local_pos, &local_val, &subscriber_note_id, &req_ip, &local_commit],
        ).await?;
    }
    Ok(())
}

async fn suggest_inherited_tags(
    client: &mut SharedConn,
    note: &Note,
    subscriber_note_id: i64,
    base_note_id: i64,
    req_ip: &str,
    local_commit: i32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Compute additions/removals against both base tags and current subscriber local tags.
    let tx = client.transaction().await?;
    // Base reviewed tags (inherited)
    let base_tags_rows = tx
        .query(
            "SELECT content FROM tags WHERE note = $1 AND reviewed = true",
            &[&base_note_id],
        )
        .await?;
    let base_set: HashSet<String> = base_tags_rows
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    // Subscriber reviewed tags (local)
    let local_tags_rows = tx
        .query(
            "SELECT content FROM tags WHERE note = $1 AND reviewed = true",
            &[&subscriber_note_id],
        )
        .await?;
    let local_set: HashSet<String> = local_tags_rows
        .into_iter()
        .map(|r| r.get::<_, String>(0))
        .collect();
    // Desired set from client payload (cleaned)
    let desired_set: HashSet<String> = note.tags.iter().map(|t| cleanser::clean(t)).collect();

    // No-overlap rule: if a tag exists on base and locally, prefer base; if the user still wants the tag,
    // schedule removal of the local duplicate.
    let local_overlaps_to_remove: HashSet<String> = local_set
        .intersection(&base_set)
        .filter(|t| desired_set.contains(*t))
        .cloned()
        .collect();

    // Additions: tags user wants that are not provided by base and not already present locally
    let mut additions: Vec<String> = desired_set
        .difference(&base_set)
        .filter(|t| !local_set.contains(*t))
        .cloned()
        .collect();

    // Optional tag validation for additions
    if !additions.is_empty() {
        let deck_id = get_topmost_deck_by_note_id(&tx, subscriber_note_id).await?;
        let mut validated: Vec<String> = Vec::with_capacity(additions.len());
        for t in additions.into_iter() {
            if t.starts_with("AnkiCollab_Optional::") {
                if is_valid_optional_tag(&tx, &deck_id, &t)
                    .await
                    .unwrap_or(false)
                {
                    validated.push(t);
                }
            } else {
                validated.push(t);
            }
        }
        additions = validated;
    }

    // Local removals: local-only tags that the user no longer wants
    let removals_local: HashSet<String> = local_set.difference(&desired_set).cloned().collect();

    // Force remove local duplicates where base already provides the tag
    let local_overlaps_vec: Vec<String> = local_overlaps_to_remove.into_iter().collect();
    tx.execute(
        "DELETE FROM tags WHERE note = $1 AND content = ANY($2::text[]) AND reviewed = true",
        &[&subscriber_note_id, &local_overlaps_vec],
    ).await?;

    // Base suppressions: base tags the user no longer wants effective
    let removals_base: HashSet<String> = base_set.difference(&desired_set).cloned().collect();

    // Insert additions with guards to avoid duplicates
    if !additions.is_empty() {
        tx.execute(
            "INSERT INTO tags (note, content, reviewed, creator_ip, action, commit)
             SELECT $1::bigint, t, false, $2::text, true, $3::int
             FROM unnest($4::text[]) AS t
             WHERE NOT EXISTS (SELECT 1 FROM tags WHERE note = $1 AND content = t AND reviewed = true)
               AND NOT EXISTS (SELECT 1 FROM tags WHERE note = $1 AND content = t AND reviewed = false AND action = true)",
            &[&subscriber_note_id, &req_ip, &local_commit, &additions],
        ).await?;
    }

    // Combine all removals (local deletions + base suppressions), dedup
    let removals_all: Vec<String> = removals_local.union(&removals_base).cloned().collect();
    if !removals_all.is_empty() {
        tx.execute(
            "INSERT INTO tags (note, content, reviewed, creator_ip, action, commit)
             SELECT $1::bigint, t, false, $2::text, false, $3::int
             FROM unnest($4::text[]) AS t
             WHERE NOT EXISTS (SELECT 1 FROM tags WHERE note = $1 AND content = t AND reviewed = false AND action = false)",
            &[&subscriber_note_id, &req_ip, &local_commit, &removals_all],
        ).await?;
    }

    tx.commit().await?;
    Ok(())
}

pub async fn overwrite_note(
    client: &mut SharedConn,
    note: &Note,
    n_id: i64,
    req_ip: &str,
    commit: i32,
    new_deck_id: i64,
    old_deck_id: i64,
    actor_user_id: Option<i32>,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Validation: prevent wiping a note's content entirely and enforce non-empty field 0
    if note.fields.is_empty() {
        return Err("At least one field is required".into());
    }

    let all_empty = note
        .fields
        .iter()
        .all(|f| cleanser::clean(f).trim().is_empty());
    if all_empty {
        return Err("All fields are empty; refusing to overwrite and wipe the note".into());
    }

    let n_r_q = client
        .query(
            "SELECT 1 FROM notes where id = $1 and deleted = false",
            &[&n_id],
        )
        .await?;
    if n_r_q.is_empty() {
        return Ok("note not found or deleted".into()); // Because its been marked as deleted
    }

    let tx = client.transaction().await?;

    // Capture existing reviewed fields BEFORE overwrite (position -> content)
    use std::collections::HashMap; // local scope to avoid conflict with outer use
    let existing_rows = tx
        .query(
            "SELECT position, content FROM fields WHERE note = $1 AND reviewed = true",
            &[&n_id],
        )
        .await?;
    let mut old_fields: HashMap<u32, String> = HashMap::with_capacity(existing_rows.len());
    for r in existing_rows {
        // Content in DB is already cleaned - no need to re-clean
        old_fields.insert(r.get(0), r.get::<_, String>(1));
    }

    // I added the order, to make sure to only overwrite the reviewed field and not the unreviewed one (if there is one)
    let upsert_field_q = tx.prepare("
        INSERT INTO fields (note, position, content, creator_ip, commit, reviewed)
        SELECT $1, $2, $3, $4, $5, true
        FROM notetype_field
        WHERE notetype = (SELECT notetype FROM notes WHERE id = $1) AND position = $2 AND protected = false
        AND NOT EXISTS (
            SELECT 1
            FROM fields
            WHERE note = $1 AND position = $2 AND content = $3
        )
        ON CONFLICT (note, position) WHERE reviewed=true DO UPDATE
        SET content = $3, creator_ip = $4
        WHERE fields.note = $1
        AND fields.position = $2
        AND fields.content <> $3
        AND fields.commit <> $5
    ").await?;

    let delete_field_q = tx
        .prepare(
            "
        DELETE FROM fields WHERE note = $1 AND position = $2 AND reviewed = true
    ",
        )
        .await?;

    // Apply field overwrites
    for (i, field) in note
        .fields
        .iter()
        .enumerate()
        .map(|(i, field)| (i as u32, field))
    {
        let content = cleanser::clean(field);
        if content.trim().is_empty() {
            // do not allow wiping field 0; ignore instead
            if i == 0 {
                continue;
            }
            tx.execute(&delete_field_q, &[&n_id, &i]).await?;
        } else {
            tx.execute(&upsert_field_q, &[&n_id, &i, &content, &req_ip, &commit])
                .await?;
        }
    }

    // Update note location if necessary
    if new_deck_id != old_deck_id {
        tx.execute(
            "UPDATE notes SET deck = $1 WHERE id = $2",
            &[&new_deck_id, &n_id],
        )
        .await?;
    }

    update_note_timestamp(&tx, n_id).await?;
    // Fan out timestamp bump to subscribers linked to this note (if this is a base note)
    bump_linked_subscribers(&tx, n_id).await?;

    // Snapshot reviewed fields AFTER overwrite
    let new_rows = tx
        .query(
            "SELECT position, content FROM fields WHERE note = $1 AND reviewed = true",
            &[&n_id],
        )
        .await?;
    let mut new_fields: HashMap<u32, String> = HashMap::with_capacity(new_rows.len());
    for r in new_rows {
        // Content in DB is already cleaned - no need to re-clean
        new_fields.insert(r.get(0), r.get::<_, String>(1));
    }

    // Emit field diff events
    for (pos, old_content) in &old_fields {
        match new_fields.get(pos) {
            None => {
                // removed
                let _ = history::log_event(
                    &tx,
                    n_id,
                    history::EventType::FieldRemoved,
                    Some(&json!({"position": pos, "content": old_content, "reviewed": true})),
                    None,
                    Some(commit),
                    actor_user_id,
                )
                .await?;
            }
            Some(new_content) if new_content != old_content => {
                // updated
                let _ = history::log_event(
                    &tx,
                    n_id,
                    history::EventType::FieldUpdated,
                    Some(&json!({"position": pos, "content": old_content, "reviewed": true})),
                    Some(&json!({"position": pos, "content": new_content, "reviewed": true})),
                    Some(commit),
                    actor_user_id,
                )
                .await?;
            }
            _ => {}
        }
    }
    for (pos, new_content) in &new_fields {
        // added
        if !old_fields.contains_key(pos) {
            let _ = history::log_event(
                &tx,
                n_id,
                history::EventType::FieldAdded,
                None,
                Some(&json!({"position": pos, "content": new_content, "reviewed": true})),
                Some(commit),
                actor_user_id,
            )
            .await?;
        }
    }

    // Deck move event
    if new_deck_id != old_deck_id {
        let _ = history::log_event(
            &tx,
            n_id,
            history::EventType::NoteMoved,
            Some(&json!({"deck": old_deck_id})),
            Some(&json!({"deck": new_deck_id})),
            Some(commit),
            actor_user_id,
        )
        .await?;
    }

    // Critical validation: ensure note still has at least one field before committing
    // This prevents data corruption where a note ends up with no fields
    if !validate_note_has_any_field(&tx, n_id).await? {
        error!(note_id = n_id, "CRITICAL: overwrite_note would leave note with no fields - aborting");
        sentry::capture_message(
            &format!("overwrite_note would leave note {} with no fields", n_id),
            sentry::Level::Error,
        );
        return Err(format!("Cannot overwrite note {}: operation would leave it with no fields", n_id).into());
    }

    tx.commit().await?;

    // update media references after commit to ensure note exists
    if let Err(e) = update_media_references_for_note(client, n_id).await {
        error!(note_id = n_id, error = %e, "Failed to update media references after overwrite");
        sentry::with_scope(
            |scope| {
                scope.set_fingerprint(Some(&["media_ref_update_failed"]));
                scope.set_extra("note_id", n_id.into());
                scope.set_tag("operation", "overwrite_note");
            },
            || {
                sentry::capture_message(
                    &format!("[BUG] Failed to update media references for note {}: {}", n_id, e),
                    sentry::Level::Error,
                );
            },
        );
        // Don't fail the whole operation - media refs can be fixed later
    }

    // force tag changes
    force_overwrite_tag(
        client,
        n_id,
        &note.tags,
        req_ip,
        commit,
        true,
        actor_user_id,
    )
    .await?;

    Ok(format!(
        "Updating the existing card {n_id:?} with the new information!"
    ))
}

pub async fn update_note(
    client: &mut SharedConn,
    note: &Note,
    n_id: i64,
    req_ip: &str,
    commit: i32,
    new_deck_id: i64,
    old_deck_id: i64,
    base_commit_cache: &mut std::collections::HashMap<i64, i32>,
    base_deck_subtrees: &mut std::collections::HashMap<i64, std::collections::HashSet<i64>>,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Validation: reject updates that would result in empty first field or all-empty payload
    if note.fields.is_empty() {
        return Err("At least one field is required".into());
    }

    let all_empty = note
        .fields
        .iter()
        .all(|f| cleanser::clean(f).trim().is_empty());
    if all_empty {
        return Err("All fields are empty; refusing to apply an empty update".into());
    }

    let n_r_q = client
        .query(
            "SELECT reviewed FROM notes where id = $1 and deleted = false",
            &[&n_id],
        )
        .await?;
    let note_reviewed = if n_r_q.is_empty() {
        return Ok("note not found or deleted".into()); // Because its been marked as deleted
    } else {
        n_r_q[0].get(0)
    };

    // Inheritance-aware routing
    let inh = get_inheritance_info(client, n_id).await;
    let inh = match inh {
        Ok(v) => v,
        Err(e) => return Err(e),
    };
    if let Some((base_note_id, subscribed_fields)) = inh {
        // Lazily create base commit if any field routes to base
        let needs_base_commit = match &subscribed_fields {
            None => true,
            Some(v) => !v.is_empty(),
        };
        // Propagate original author and subscriber deck name to forwarded commit
        let committing_user: Option<i32> = match client
            .query_opt(
                "SELECT user_id FROM commits WHERE commit_id = $1",
                &[&commit],
            )
            .await?
        {
            Some(r) => r.get::<_, Option<i32>>(0),
            None => None,
        };
        let source_deck_name: Option<String> = client
            .query_opt(
                "SELECT d.full_path FROM notes n JOIN decks d ON d.id = n.deck WHERE n.id = $1",
                &[&n_id],
            )
            .await?
            .map(|r| r.get::<_, String>(0));
        let original_rationale: Option<i32> = client
            .query_opt(
                "SELECT rationale, info FROM commits WHERE commit_id = $1",
                &[&commit],
            )
            .await?
            .map(|r| r.get::<_, i32>(0));
        let original_comment: Option<String> = client
            .query_opt("SELECT info FROM commits WHERE commit_id = $1", &[&commit])
            .await?
            .map(|r| r.get::<_, String>(0));
        // Reuse a single base commit per topmost base deck across the bulk
        let base_commit_id = if needs_base_commit {
            // First, resolve the base note's immediate deck (cheap)
            let base_deck_id: i64 = client
                .query_one("SELECT deck FROM notes WHERE id = $1", &[&base_note_id])
                .await?
                .get(0);

            // Try to find an existing subtree that contains this deck
            let mut matched_root: Option<i64> = None;
            for (root_id, subtree) in base_deck_subtrees.iter() {
                if subtree.contains(&base_deck_id) {
                    matched_root = Some(*root_id);
                    break;
                }
            }

            let base_root_deck_id = if let Some(r) = matched_root {
                r
            } else {
                // Compute the topmost root for this base note (one-time per unseen root)
                let root_id: i64 = client
                    .query_one(
                        "WITH RECURSIVE deck_hierarchy AS (
                            SELECT decks.id, parent
                            FROM decks
                            JOIN notes ON decks.id = notes.deck
                            WHERE notes.id = $1
                            UNION
                            SELECT d2.id, d2.parent
                            FROM decks d2
                            JOIN deck_hierarchy dh ON dh.parent = d2.id
                        )
                        SELECT id FROM deck_hierarchy WHERE parent IS NULL",
                        &[&base_note_id],
                    )
                    .await?
                    .get(0);

                // Build the full subtree under the root (downward) and cache it
                let rows = client
                    .query(
                        "WITH RECURSIVE sub AS (
                            SELECT id FROM decks WHERE id = $1
                            UNION ALL
                            SELECT d.id FROM decks d JOIN sub s ON d.parent = s.id
                        )
                        SELECT id FROM sub",
                        &[&root_id],
                    )
                    .await?;
                let mut set: std::collections::HashSet<i64> =
                    std::collections::HashSet::with_capacity(rows.len());
                for r in rows {
                    set.insert(r.get::<_, i64>(0));
                }
                base_deck_subtrees.insert(root_id, set);
                root_id
            };

            if let Some(existing) = base_commit_cache.get(&base_root_deck_id) {
                Some(*existing)
            } else {
                let created = ensure_base_commit(
                    client,
                    base_note_id,
                    req_ip,
                    committing_user,
                    source_deck_name,
                    original_rationale,
                    original_comment,
                )
                .await
                .unwrap_or(commit);
                base_commit_cache.insert(base_root_deck_id, created);
                Some(created)
            }
        } else {
            None // Fallback: if base commit creation failed, route all to local
        };

        route_inherited_field_suggestions(
            client,
            note,
            n_id,
            base_note_id,
            &subscribed_fields,
            req_ip,
            commit,
            base_commit_id,
        )
        .await?;
        suggest_inherited_tags(client, note, n_id, base_note_id, req_ip, commit).await?;
    } else {
        let tx = client.transaction().await?;

        let add_field_q = tx.prepare("
            INSERT INTO fields (note, position, content, creator_ip, commit)
            SELECT $1, $2, $3, $4, $5
            FROM notetype_field
            WHERE notetype = (SELECT notetype FROM notes WHERE id = $1) AND position = $2 AND protected = false
            AND NOT EXISTS (
                SELECT 1
                FROM fields
                WHERE note = $1 AND position = $2 AND content = $3
            )
            LIMIT 1
        ").await?;

        if note_reviewed {
            for (i, field) in note
                .fields
                .iter()
                .enumerate()
                .map(|(i, field)| (i as u32, field))
            {
                let content = cleanser::clean(field);
                // Ignore suggestions that attempt to set the first field (0) to empty
                if i == 0 && content.trim().is_empty() {
                    continue;
                }
                if !field.is_empty()
                    || !tx
                        .query(
                            "SELECT 1 FROM fields WHERE note = $1 AND position = $2",
                            &[&n_id, &i],
                        )
                        .await?
                        .is_empty()
                {
                    tx.execute(&add_field_q, &[&n_id, &i, &content, &req_ip, &commit])
                        .await?;
                }
            }
        } else {
            let does_field_exist = tx
                .prepare("SELECT 1 FROM fields WHERE note = $1 AND position = $2")
                .await?;

            let update_field_q = tx.prepare("
                UPDATE fields SET content = $3, creator_ip = $4 WHERE note = $1 AND position = $2 AND content <> $3 and commit <> $5
            ").await?;

            let delete_field_q = tx
                .prepare(
                    "DELETE FROM fields WHERE note = $1 AND position = $2 AND reviewed = false",
                )
                .await?;

            // Check if the field exists, if it does, update it, else insert it. If the new content is empty, don't insert but delete it instead
            // Field 0 is protected and cannot be deleted - it's the note's primary identifier
            for (i, field) in note
                .fields
                .iter()
                .enumerate()
                .map(|(i, field)| (i as u32, field))
            {
                if field.is_empty() {
                    // Never delete field 0 - skip silently instead
                    if i == 0 {
                        continue;
                    }
                    tx.execute(&delete_field_q, &[&n_id, &i]).await?;
                } else {
                    let rows = tx.query(&does_field_exist, &[&n_id, &i]).await?;
                    let content = cleanser::clean(field);
                    if rows.is_empty() {
                        tx.execute(&add_field_q, &[&n_id, &i, &content, &req_ip, &commit])
                            .await?;
                    } else {
                        tx.execute(&update_field_q, &[&n_id, &i, &content, &req_ip, &commit])
                            .await?;
                    }
                }
            }
        }

        // Critical validation: ensure note still has at least one field before committing
        // This prevents data corruption where a note ends up with no fields
        if !validate_note_has_any_field(&tx, n_id).await? {
            error!(note_id = n_id, "CRITICAL: update_note would leave note with no fields - aborting");
            sentry::capture_message(
                &format!("update_note would leave note {} with no fields", n_id),
                sentry::Level::Error,
            );
            return Err(format!("Cannot update note {}: operation would leave it with no fields", n_id).into());
        }

        tx.commit().await?;

        if note_reviewed {
            // Check tags for changes
            check_tag(client, n_id, &note.tags, req_ip, commit).await?;
        } else {
            // force tag changes, if the note is unreviewed but keep the old commit id so all changes are kept in the same commit
            // Gamble that there is only one commit per unreviewed note. should be the case beecause who other than the creator would change it?
            let get_old_commit = client.query("SELECT commit FROM tags WHERE note = $1 AND reviewed = false ORDER BY commit ASC LIMIT 1", &[&n_id],).await?;
            let old_commit_id = if get_old_commit.is_empty() {
                commit // No idea how this could happen, so we just use the current one as a fallback
            } else {
                get_old_commit[0].get(0)
            };
            // No actor_user_id available in this context (update_note), so pass None
            force_overwrite_tag(client, n_id, &note.tags, req_ip, old_commit_id, false, None)
                .await?;
        }
    }

    // Deck move suggestion/force remains the same behavior
    if new_deck_id != old_deck_id {
        if note_reviewed {
            // Suggest move
            let tx = client.transaction().await?;
            let insert_q = tx.prepare(
                "INSERT INTO note_move_suggestions (original_deck, target_deck, note, creator_ip, commit) VALUES ($1, $2, $3, $4, $5)"
            ).await?;
            tx.execute(
                &insert_q,
                &[&old_deck_id, &new_deck_id, &n_id, &req_ip, &commit],
            )
            .await?;
            tx.commit().await?;
        } else {
            // Force move
            let tx = client.transaction().await?;
            let update_q = tx
                .prepare("UPDATE notes SET deck = $1 WHERE id = $2")
                .await?;
            tx.execute(&update_q, &[&new_deck_id, &n_id]).await?;
            tx.commit().await?;
        }
    }

    Ok(format!(
        "Suggested the new information to the existing card {n_id:?}"
    ))
}

async fn get_original_name(
    db_state: &Arc<database::AppState>,
    input_hash: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            error!(error = %err, "Failed to get database pool connection in get_original_name");
            return Err("Failed to retrieve a pooled connection".into());
        }
    };

    let res = client
        .query(
            "SELECT name FROM decks WHERE human_hash = $1 LIMIT 1",
            &[&input_hash],
        )
        .await?;
    if res.is_empty() {
        Err("Deck not found".into())
    } else {
        Ok(res[0].get(0))
    }
}

async fn get_id_from_path(
    client: &SharedConn,
    input_hash: &str,
    deck_path: &str,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let query = "WITH RECURSIVE subdecks AS (
            SELECT id
            FROM decks
            WHERE human_hash = $1
            UNION ALL
            SELECT p.id
            FROM decks p
            JOIN subdecks s ON s.id = p.parent
        )
        SELECT d.id from decks d JOIN subdecks p on p.id = d.id
        WHERE d.full_path = $2 LIMIT 1";
    let result = client.query(query, &[&input_hash, &deck_path]).await?;

    if result.is_empty() {
        Err("not found".into())
    } else {
        Ok(result[0].get(0))
    }
}

#[async_recursion]
async fn try_suggest_note(
    client: &mut SharedConn,
    deck_hash: &String,
    notetype_cache: &mut HashMap<String, String>,
    deck_path: &String,
    deck_id: Option<i64>,
    deck: &AnkiDeck,
    req_ip: &String,
    commit: i32,
    force_overwrite: bool,
    deck_tree: &Vec<i64>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if deck_id.is_none() {
        return Err("Not found".into());
    }

    push::handle_notes_and_media_update(
        client,
        deck,
        notetype_cache,
        req_ip,
        deck_id,
        force_overwrite,
        commit,
        deck_tree,
    )
    .await?;

    for child in &deck.children {
        let child_path = format!("{}::{}", deck_path, child.name);
        let (san_deck_id, san_owner) = sanity_check(client, deck_hash, &child_path, commit).await?;
        make(
            client,
            deck_hash,
            notetype_cache,
            &child_path,
            child,
            req_ip,
            commit,
            force_overwrite,
            san_deck_id,
            san_owner,
            deck_tree,
        )
        .await?;
    }

    Ok("Success".to_string())
}
pub fn remove_ankicollab_suffix(
    raw_deck_path: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    // Remove AnkiCollab suffix
    static ANKICOLLAB_SUFFIX_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"\s?\(AnkiCollab\)(_\d+)?").unwrap());

    let res = ANKICOLLAB_SUFFIX_RE.replace(raw_deck_path, "").into_owned();
    if res.is_empty() {
        return Err("Error occurred: Deckname is ill-formed.".into());
    }
    Ok(res)
}

#[async_recursion]
pub async fn update_deck_names(deck: &mut AnkiDeck) {
    deck.name = match remove_ankicollab_suffix(&deck.name) {
        Ok(val) => val,
        Err(error) => {
            warn!(
                deck_name = %deck.name,
                error = %error,
                "Failed to remove AnkiCollab suffix from deck name"
            );
            deck.name.clone()
        }
    };

    for child in &mut deck.children {
        update_deck_names(child).await;
    }
}

pub async fn fix_deck_name(
    db_state: &Arc<database::AppState>,
    raw_deck_path: &str,
    alternate_name: &str,
    deck_hash: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let res = remove_ankicollab_suffix(raw_deck_path)?;
    // Replace the alternate name with the expected name
    let original_name = get_original_name(db_state, deck_hash).await?;
    Ok(res.replacen(alternate_name, &original_name, 1))
}

pub async fn create_new_commit(
    db_state: &Arc<database::AppState>,
    rationale: i32,
    commit_text: &str,
    ip: &String,
    user_id: Option<i32>,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            error!(error = %err, "Failed to get database pool connection in create_new_commit");
            return Err("Failed to retrieve a pooled connection".into());
        }
    };
    let content = cleanser::clean(commit_text);
    let rows = client.query("INSERT INTO commits (rationale, info, ip, timestamp, user_id) VALUES ($1, $2, $3, NOW(), $4) RETURNING commit_id", &[&rationale, &content, &ip, &user_id]).await?;
    Ok(rows[0].get(0))
}

#[async_recursion]
pub async fn sanity_check_notetypes(
    client: &SharedConn,
    cache: &mut HashMap<String, String>,
    deck_hash: &String,
    deck: &AnkiDeck,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let deck_query = client
        .query(
            "SELECT owner, restrict_notetypes from decks where human_hash = $1",
            &[&deck_hash],
        )
        .await?;
    if deck_query.is_empty() {
        return Err("Deck does not exist".into());
    }
    let owner: i32 = deck_query[0].get(0);
    let restrict_notetypes: bool = deck_query[0].get(1);
    if !restrict_notetypes {
        return Ok(());
    }

    if let Some(nt) = &deck.note_models {
        for n in nt {
            if cache.contains_key(&n.crowdanki_uuid) {
                continue;
            }
            let guid = does_notetype_exist(client, n, owner).await?;
            if guid.is_empty() {
                return Err(n.crowdanki_uuid.clone().into());
            }
            cache.insert(n.crowdanki_uuid.clone(), guid.clone());
        }
    }

    for child in &deck.children {
        sanity_check_notetypes(client, cache, deck_hash, child).await?;
    }

    Ok(())
}

pub async fn sanity_check(
    client: &SharedConn,
    deck_hash: &String,
    deck_path: &str,
    commit: i32,
) -> Result<(Option<i64>, i32), Box<dyn std::error::Error + Send + Sync>> {
    let mut deck_id = match get_id_from_path(client, deck_hash, deck_path).await {
        Ok(id) => Some(id),
        Err(_error) => None,
    };

    // Check if the parent exists and insert child if it does, else abort
    if deck_id.is_none() {
        let input_layers: Vec<&str> = deck_path.split("::").collect();
        let parent_path = input_layers[0..input_layers.len() - 1].join("::");

        let owner = client
            .query(
                "SELECT owner from decks where human_hash = $1",
                &[&deck_hash],
            )
            .await?;
        if parent_path.is_empty() || owner.is_empty() {
            warn!(
                parent_path = %parent_path,
                deck_hash = %deck_hash,
                "Deck does not exist - empty parent_path or owner"
            );
            return Err("Deck does not exist".into());
        }

        match get_id_from_path(client, deck_hash, &parent_path).await {
            Ok(id) => deck_id = Some(id),
            Err(_error) => return Err("Parent Deck does not exist".into()),
        };

        client
            .query(
                "UPDATE commits SET deck = $1 WHERE commit_id = $2 AND deck is NULL",
                &[&deck_id, &commit],
            )
            .await?;
        Ok((deck_id, owner[0].get(0)))
    } else {
        client
            .query(
                "UPDATE commits SET deck = $1 WHERE commit_id = $2 AND deck is NULL",
                &[&deck_id, &commit],
            )
            .await?;
        Ok((deck_id, 0))
    }
}

#[async_recursion]
pub async fn make(
    client: &mut SharedConn,
    deck_hash: &String,
    notetype_cache: &mut HashMap<String, String>,
    deck_path: &String,
    deck: &AnkiDeck,
    req_ip: &String,
    commit: i32,
    force_overwrite: bool,
    deck_id: Option<i64>,
    owner: i32,
    deck_tree: &Vec<i64>,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if deck_id.is_none() {
        return Err("Deck not found. Illegal code.".into());
    }
    // Check if the maintainer allows new subdecks to be created
    if owner != 0 {
        let deck_query = client
            .query(
                "SELECT restrict_subdecks from decks where human_hash = $1",
                &[&deck_hash],
            )
            .await?;
        if deck_query.is_empty() {
            return Err("Deck does not exist".into());
        }
        let restrict_subdecks: bool = deck_query[0].get(0);
        if restrict_subdecks {
            return Err("Subdecks are not allowed".into());
        }
    }
    if owner != 0 {
        push::unpack_deck_json(
            client,
            deck,
            notetype_cache,
            owner,
            req_ip,
            deck_id,
            force_overwrite,
            commit,
            deck_tree,
        )
        .await?;
    } else {
        // match try_suggest_note(client, deck_hash, notetype_cache, deck_path, deck_id, deck, req_ip, commit, force_overwrite, deck_tree).await {
        //     Ok(_res) => { },
        //     Err(error) => { println!("Error Submit Note: {error}") },
        // }; // Big Problem: Issues go unnoticed by the user.
                
        try_suggest_note(
            client,
            deck_hash,
            notetype_cache,
            deck_path,
            deck_id,
            deck,
            req_ip,
            commit,
            force_overwrite,
            deck_tree,
        )
        .await?;
    }

    Ok("Success".into())
}
