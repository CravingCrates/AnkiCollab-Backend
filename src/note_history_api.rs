use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::Value as JsonValue;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use crate::cleanser;
use crate::database::AppState;
use crate::error;
use crate::structs::{
    NoteHistoryEventResponse, NoteHistoryGroupResponse, NoteHistoryRequest, NoteHistoryResponse,
};
use crate::validate_deck_hash;

fn rationale_code_to_string(input: i32) -> &'static str {
    match input {
        0 => "None",
        1 => "Deck Creation",
        2 => "Updated content",
        3 => "New content",
        4 => "Content error",
        5 => "Spelling/Grammar",
        6 => "New card",
        7 => "Updated Tags",
        8 => "New Tags",
        9 => "Bulk Suggestion",
        10 => "Other",
        11 => "Card Deletion",
        12 => "Changed Deck",
        _ => "Suggestion",
    }
}

pub async fn get_note_history(
    State(db_state): State<Arc<AppState>>,
    Json(req): Json<NoteHistoryRequest>,
) -> impl IntoResponse {
    if !validate_deck_hash(&req.deck_hash) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "Invalid deck hash format"})),
        )
            .into_response();
    }

    if req.guid.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"error": "GUID cannot be empty"})),
        )
            .into_response();
    }

    let client = match db_state.db_pool.get_owned().await {
        Ok(pool) => pool,
        Err(err) => {
            error::AppError::db_connection("get_note_history", &err)
                .with_context("deck_hash", req.deck_hash.clone())
                .with_context("guid", req.guid.clone())
                .report_to_sentry();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Database connection error"})),
            )
                .into_response();
        }
    };

    let note_row = match client
        .query_opt(
            "WITH RECURSIVE deck_tree AS (
                SELECT id, parent, name
                FROM decks
                WHERE human_hash = $1
                UNION ALL
                SELECT d.id, d.parent, d.name
                FROM decks d
                INNER JOIN deck_tree dt ON dt.id = d.parent
            )
            SELECT n.id, n.notetype, dt.name
            FROM notes n
            INNER JOIN deck_tree dt ON n.deck = dt.id
            INNER JOIN decks root_d ON root_d.human_hash = $1
            WHERE n.deleted = false AND n.guid = $2 AND (root_d.private = false OR root_d.private IS NULL)
            LIMIT 1",
            &[&req.deck_hash, &req.guid],
        )
        .await
    {
        Ok(row) => row,
        Err(err) => {
            error::AppError::db_query("get_note_history", &err)
                .with_context("deck_hash", req.deck_hash.clone())
                .with_context("guid", req.guid.clone())
                .report_to_sentry();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Database query error"})),
            )
                .into_response();
        }
    };

    let Some(row) = note_row else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"error": "Note not found in this deck"})),
        )
            .into_response();
    };

    let note_id: i64 = row.get(0);
    let notetype_id: Option<i64> = row.get(1);
    let deck_name: String = row.get(2);

    // Fetch field names
    let mut field_map: HashMap<u32, String> = HashMap::new();
    if let Some(nt) = notetype_id {
        if let Ok(fields) = client
            .query(
                "SELECT position, name FROM notetype_field WHERE notetype = $1",
                &[&nt],
            )
            .await
        {
            for f in fields {
                let pos_u32: u32 = f.get(0);
                let name: String = f.get(1);
                field_map.insert(pos_u32, name);
            }
        }
    }

    let limit = req.limit.unwrap_or(100).clamp(1, 200);

    let event_rows = match client
        .query(
            "SELECT e.id, e.version, e.event_type, e.actor_user_id, u.username, e.commit_id, e.approved, e.old_value, e.new_value, to_char(e.created_at, 'YYYY-MM-DD HH24:MI:SS')
             FROM note_events e
             LEFT JOIN users u ON e.actor_user_id = u.id
             WHERE e.note_id = $1
             ORDER BY e.version DESC, e.id DESC
             LIMIT $2",
            &[&note_id, &limit],
        )
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            error::AppError::db_query("get_note_history::events", &err)
                .with_context("note_id", note_id.to_string())
                .report_to_sentry();
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"error": "Failed to query note events"})),
            )
                .into_response();
        }
    };

    // Collect commit IDs
    let mut commit_ids = HashSet::new();
    for row in &event_rows {
        if let Some(cid) = row.get::<_, Option<i32>>(5) {
            commit_ids.insert(cid);
        }
    }

    struct CommitMeta {
        rationale: Option<String>,
        author: Option<String>,
        timestamp: Option<String>,
    }

    let mut commit_meta_map: HashMap<i32, CommitMeta> = HashMap::new();
    if !commit_ids.is_empty() {
        let cids: Vec<i32> = commit_ids.into_iter().collect();
        if let Ok(c_rows) = client
            .query(
                "SELECT c.commit_id, c.rationale, c.info, u.username, to_char(c.timestamp, 'YYYY-MM-DD HH24:MI:SS')
                 FROM commits c
                 LEFT JOIN users u ON c.user_id = u.id
                 WHERE c.commit_id = ANY($1)",
                &[&cids],
            )
            .await
        {
            for cr in c_rows {
                let cid: i32 = cr.get(0);
                let rationale_code: Option<i32> = cr.get(1);
                let commit_info: Option<String> = cr.get(2);
                let author: Option<String> = cr.get(3);
                let timestamp: Option<String> = cr.get(4);

                let rationale: Option<String> = match (rationale_code, commit_info) {
                    (Some(code), Some(ref info)) if !info.trim().is_empty() => {
                        let label = rationale_code_to_string(code);
                        if label != "None" && label != "Other" {
                            Some(format!("{label}: {}", info.trim()))
                        } else {
                            Some(info.trim().to_string())
                        }
                    }
                    (Some(code), _) => {
                        let label = rationale_code_to_string(code);
                        if label != "None" {
                            Some(label.to_string())
                        } else {
                            None
                        }
                    }
                    (None, Some(ref info)) if !info.trim().is_empty() => {
                        Some(info.trim().to_string())
                    }
                    _ => None,
                };

                commit_meta_map.insert(
                    cid,
                    CommitMeta {
                        rationale,
                        author,
                        timestamp,
                    },
                );
            }
        }
    }

    let mut raw_events: Vec<(Option<i32>, NoteHistoryEventResponse)> = Vec::new();

    for row in event_rows {
        let event_type: String = row.get(2);
        let old_value: Option<JsonValue> = row.get(7);
        let new_value: Option<JsonValue> = row.get(8);

        let mut field_name = None;
        if event_type.contains("field") {
            let pos_val = new_value
                .as_ref()
                .and_then(|v| v.get("position"))
                .or_else(|| old_value.as_ref().and_then(|v| v.get("position")));

            if let Some(pos_v) = pos_val {
                if let Some(pos_u32) = pos_v.as_i64().and_then(|v| u32::try_from(v).ok()) {
                    field_name = field_map.get(&pos_u32).cloned();
                }
            }
        }

        let old_human = summarize_event(&event_type, old_value.as_ref(), "old");
        let new_side_value = if new_value.is_none()
            && (event_type == "field_change_denied" || event_type == "tag_change_denied")
        {
            &old_value
        } else {
            &new_value
        };
        let new_human = summarize_event(&event_type, new_side_value.as_ref(), "new");
        let diff_html = compute_diff_html(&event_type, old_value.as_ref(), new_value.as_ref());

        let commit_id: Option<i32> = row.get(5);

        let event = NoteHistoryEventResponse {
            id: row.get(0),
            version: row.get(1),
            event_type: event_type.clone(),
            actor_username: row.get(4),
            field_name,
            old_human,
            new_human,
            diff_html,
            created_at: row.get(9),
        };

        raw_events.push((commit_id, event));
    }

    // Group events by commit
    let mut groups: Vec<NoteHistoryGroupResponse> = Vec::new();
    let mut by_commit: BTreeMap<i32, usize> = BTreeMap::new();

    for (commit_id, event) in raw_events {
        if let Some(cid) = commit_id {
            if let Some(&idx) = by_commit.get(&cid) {
                if event.event_type == "commit_approved_effect" {
                    groups[idx].approved = true;
                    continue;
                }
                if event.event_type == "commit_denied_effect" {
                    groups[idx].denied = true;
                    continue;
                }
                groups[idx].events.push(event);
            } else {
                let meta = commit_meta_map.get(&cid);
                let mut group = NoteHistoryGroupResponse {
                    commit_id: Some(cid),
                    commit_author: meta.and_then(|m| m.author.clone()),
                    commit_rationale: meta.and_then(|m| m.rationale.clone()),
                    commit_timestamp: meta.and_then(|m| m.timestamp.clone()),
                    approved: false,
                    denied: false,
                    events: Vec::new(),
                };
                if event.event_type == "commit_approved_effect" {
                    group.approved = true;
                } else if event.event_type == "commit_denied_effect" {
                    group.denied = true;
                } else {
                    group.events.push(event);
                }
                by_commit.insert(cid, groups.len());
                groups.push(group);
            }
        } else {
            let group = NoteHistoryGroupResponse {
                commit_id: None,
                commit_author: None,
                commit_rationale: None,
                commit_timestamp: None,
                approved: false,
                denied: false,
                events: vec![event],
            };
            groups.push(group);
        }
    }

    // Auto-approve created-only groups
    for group in &mut groups {
        if group.commit_id.is_some() && !group.approved && !group.denied {
            let all_note_created = group
                .events
                .iter()
                .all(|event| event.event_type == "note_created");
            if all_note_created && !group.events.is_empty() {
                group.approved = true;
            }
        }
    }

    let resp = NoteHistoryResponse {
        note_id,
        guid: req.guid,
        deck_name,
        groups,
    };

    (StatusCode::OK, Json(resp)).into_response()
}

fn compute_diff_html(
    event_type: &str,
    old_value: Option<&JsonValue>,
    new_value: Option<&JsonValue>,
) -> Option<String> {
    if event_type != "field_updated" {
        return None;
    }
    let old_content = old_value
        .and_then(|v| v.get("content"))
        .and_then(|c| c.as_str())
        .unwrap_or("");
    let new_content = new_value
        .and_then(|v| v.get("content"))
        .and_then(|c| c.as_str())
        .unwrap_or("");
    if old_content.is_empty() && new_content.is_empty() {
        return None;
    }
    let clean_old = cleanser::clean(old_content);
    let clean_new = cleanser::clean(new_content);
    Some(htmldiff::htmldiff(&clean_old, &clean_new))
}

fn summarize_event(event_type: &str, json: Option<&JsonValue>, side: &str) -> Option<String> {
    let v = json?;
    match event_type {
        "field_added" | "field_removed" | "field_updated" => v
            .get("content")
            .and_then(|c| c.as_str())
            .map(std::string::ToString::to_string)
            .or_else(|| {
                v.get("value")
                    .and_then(|c| c.as_str())
                    .map(std::string::ToString::to_string)
            }),
        "tag_added" | "tag_removed" => v
            .get("content")
            .and_then(|c| c.as_str())
            .map(|s| format!("#{s}")),
        "note_moved" => {
            let to = v.get("to").and_then(|x| x.as_str()).unwrap_or("");
            let from = v.get("from").and_then(|x| x.as_str()).unwrap_or("");
            if side == "new" && !to.is_empty() {
                Some(format!("to deck {to}"))
            } else if side == "old" && !from.is_empty() {
                Some(format!("from deck {from}"))
            } else {
                Some("moved".to_string())
            }
        }
        "note_deleted" => Some("note deleted".to_string()),
        "note_denied" => Some("note denied".to_string()),
        "commit_approved_effect" => Some("commit approved".to_string()),
        "commit_denied_effect" => Some("commit denied".to_string()),
        "suggestion_denied" => Some("suggestion denied".to_string()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_diff_html_field_updated() {
        let old_val = serde_json::json!({"content": "Hello world"});
        let new_val = serde_json::json!({"content": "Hello beautiful world"});
        let diff = compute_diff_html("field_updated", Some(&old_val), Some(&new_val));
        assert!(diff.is_some());
        let html = diff.unwrap();
        assert!(html.contains("beautiful"));
    }

    #[test]
    fn test_compute_diff_html_ignored_for_other_events() {
        let old_val = serde_json::json!({"content": "Hello"});
        let new_val = serde_json::json!({"content": "World"});
        let diff = compute_diff_html("tag_added", Some(&old_val), Some(&new_val));
        assert!(diff.is_none());
    }

    #[test]
    fn test_summarize_event() {
        let field_val = serde_json::json!({"content": "My text"});
        assert_eq!(
            summarize_event("field_updated", Some(&field_val), "new"),
            Some("My text".to_string())
        );

        let tag_val = serde_json::json!({"content": "cardio::ic"});
        assert_eq!(
            summarize_event("tag_added", Some(&tag_val), "new"),
            Some("#cardio::ic".to_string())
        );
    }
}
