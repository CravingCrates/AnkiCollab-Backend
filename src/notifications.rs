use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use crate::database::AppState;
use crate::cleanser;
use crate::structs::{
    CommitFieldChange, CommitMoveChange, CommitSnapshotEvent, CommitSnapshotResponse,
    CommitTagChange,
    NotificationDeckGroup, NotificationHistoryResponse, NotificationItem, NotificationUnreadResponse,
};
use once_cell::sync::Lazy;
use serde_json::Value as JsonValue;

/// Strict sanitizer for diff HTML output. Only allows safe formatting and diff
/// markup tags -- no scripts, iframes, event handlers, or dangerous elements.
static DIFF_SANITIZER: Lazy<ammonia::Builder<'static>> = Lazy::new(|| {
    let mut builder = ammonia::Builder::empty();
    builder.add_tags(&[
        "ins", "del", "span", "b", "i", "u", "em", "strong", "sub", "sup", "br",
        "p", "div", "img", "ruby", "rt",
    ]);
    builder.add_generic_attributes(&["class"]);
    builder.add_tag_attributes("img", &["src", "alt"]);
    builder.add_tag_attributes("span", &["class"]);
    builder
});

fn sanitize_diff_html(html: &str) -> String {
    DIFF_SANITIZER.clean(html).to_string()
}

fn deck_display_name(full_path: &str) -> String {
    let name = full_path
        .rsplit("::")
        .next()
        .unwrap_or(full_path)
        .trim()
        .to_string();

    if name.is_empty() {
        "[Unnamed Deck]".to_string()
    } else {
        name
    }
}

pub async fn get_unread_grouped(
    state: &Arc<AppState>,
    user_id: i32,
) -> Result<NotificationUnreadResponse, String> {
    let client = state
        .db_pool
        .get_owned()
        .await
        .map_err(|_| "Internal Error".to_string())?;

    let rows = client
        .query(
            "SELECT n.id,
                    n.commit_id,
                    n.deck_id,
                    d.full_path,
                    n.status,
                    n.reason,
                        TO_CHAR(n.created_at, 'YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM')
             FROM notifications n
             JOIN decks d ON d.id = n.deck_id
             WHERE n.user_id = $1 AND n.is_read = false
             ORDER BY n.created_at DESC
             LIMIT 500",
            &[&user_id],
        )
        .await
        .map_err(|_| "Failed to fetch notifications".to_string())?;

    let mut groups: BTreeMap<i64, NotificationDeckGroup> = BTreeMap::new();

    for row in rows {
        let deck_id: i64 = row.get(2);
        let full_path: String = row.get(3);
        let status: String = row.get(4);

        let item = NotificationItem {
            id: row.get(0),
            commit_id: row.get(1),
            deck_id,
            deck_name: deck_display_name(&full_path),
            status: status.clone(),
            reason: row.get(5),
            created_at: row.get(6),
            is_read: false,
        };

        let entry = groups.entry(deck_id).or_insert_with(|| NotificationDeckGroup {
            deck_id,
            deck_name: deck_display_name(&full_path),
            approved_count: 0,
            denied_count: 0,
            notifications: Vec::new(),
        });

        if status == "approved" {
            entry.approved_count += 1;
        } else if status == "denied" {
            entry.denied_count += 1;
        }

        entry.notifications.push(item);
    }

    let group_list: Vec<NotificationDeckGroup> = groups.into_values().collect();
    let unread_count = group_list
        .iter()
        .map(|g| g.notifications.len() as i64)
        .sum::<i64>();

    Ok(NotificationUnreadResponse {
        unread_count,
        groups: group_list,
    })
}

pub async fn get_history(
    state: &Arc<AppState>,
    user_id: i32,
    offset: i64,
    limit: i64,
) -> Result<NotificationHistoryResponse, String> {
    let client = state
        .db_pool
        .get_owned()
        .await
        .map_err(|_| "Internal Error".to_string())?;

    let rows = client
        .query(
            "SELECT n.id,
                    n.commit_id,
                    n.deck_id,
                    d.full_path,
                    n.status,
                    n.reason,
                          TO_CHAR(n.created_at, 'YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM'),
                    n.is_read
             FROM notifications n
             JOIN decks d ON d.id = n.deck_id
             WHERE n.user_id = $1
             ORDER BY n.created_at DESC
             OFFSET $2
             LIMIT $3",
            &[&user_id, &offset, &limit],
        )
        .await
        .map_err(|_| "Failed to fetch notification history".to_string())?;

    let total_row = client
        .query_one(
            "SELECT COUNT(*) FROM notifications WHERE user_id = $1",
            &[&user_id],
        )
        .await
        .map_err(|_| "Failed to count notifications".to_string())?;

    let items: Vec<NotificationItem> = rows
        .into_iter()
        .map(|row| {
            let full_path: String = row.get(3);
            NotificationItem {
                id: row.get(0),
                commit_id: row.get(1),
                deck_id: row.get(2),
                deck_name: deck_display_name(&full_path),
                status: row.get(4),
                reason: row.get(5),
                created_at: row.get(6),
                is_read: row.get(7),
            }
        })
        .collect();

    Ok(NotificationHistoryResponse {
        total: total_row.get(0),
        offset,
        limit,
        items,
    })
}

/// Maximum number of notification IDs accepted in a single mark-read request.
/// Validated in the handler to return 400; enforced here as defence-in-depth.
pub const MAX_MARK_READ_IDS: usize = 1_000;

pub async fn mark_read(
    state: &Arc<AppState>,
    user_id: i32,
    ids: &[i32],
) -> Result<u64, String> {
    if ids.is_empty() {
        return Ok(0);
    }

    if ids.len() > MAX_MARK_READ_IDS {
        return Err(format!("Too many IDs (max {})", MAX_MARK_READ_IDS));
    }

    let client = state
        .db_pool
        .get_owned()
        .await
        .map_err(|_| "Internal Error".to_string())?;

    let updated = client
        .execute(
            "UPDATE notifications
             SET is_read = true
             WHERE user_id = $1
               AND id = ANY($2)",
            &[&user_id, &ids],
        )
        .await
        .map_err(|_| "Failed to update notifications".to_string())?;

    Ok(updated)
}

pub async fn get_commit_snapshot(
    state: &Arc<AppState>,
    commit_id: i32,
    user_id: i32,
) -> Result<CommitSnapshotResponse, String> {
    let client = state
        .db_pool
        .get_owned()
        .await
        .map_err(|_| "Internal Error".to_string())?;

    let commit_row = client
        .query_opt(
            "SELECT c.commit_id,
                    c.rationale,
                    COALESCE(c.info, ''),
                    TO_CHAR(c.timestamp, 'YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM'),
                    d.full_path,
                    COALESCE(u.username, 'Unknown')
             FROM commits c
             JOIN decks d ON d.id = c.deck
             LEFT JOIN users u ON u.id = c.user_id
             WHERE c.commit_id = $1
                             AND EXISTS (
                                     SELECT 1
                                     FROM notifications n
                                     WHERE n.commit_id = c.commit_id
                                         AND n.user_id = $2
                             )",
            &[&commit_id, &user_id],
        )
        .await
        .map_err(|_| "Failed to load commit snapshot".to_string())?;

    let Some(row) = commit_row else {
        return Err("Commit not found".to_string());
    };

    let event_rows = client
        .query(
            "SELECT e.note_id,
                    e.version,
                    e.event_type,
                    e.old_value,
                    e.new_value,
                    TO_CHAR(e.created_at, 'YYYY-MM-DD\"T\"HH24:MI:SSTZH:TZM'),
                    e.approved,
                    n.notetype,
                    n.guid
             FROM note_events e
             LEFT JOIN notes n ON n.id = e.note_id
             WHERE e.commit_id = $1
             ORDER BY e.note_id, e.version, e.id",
            &[&commit_id],
        )
        .await
        .map_err(|_| "Failed to load commit events".to_string())?;

    let mut field_name_map = BTreeMap::<(i64, i32), String>::new();
    let mut notetype_ids: HashSet<i64> = HashSet::new();
    for row in &event_rows {
        if let Some(notetype_id) = row.get::<_, Option<i64>>(7) {
            notetype_ids.insert(notetype_id);
        }
    }

    if !notetype_ids.is_empty() {
        let notetype_ids: Vec<i64> = notetype_ids.into_iter().collect();
        let field_rows = client
            .query(
                "SELECT notetype, position::bigint, name
                 FROM notetype_field
                 WHERE notetype = ANY($1)",
                &[&notetype_ids],
            )
            .await
            .map_err(|_| "Failed to load notetype field names".to_string())?;

        for row in field_rows {
            let notetype_id: i64 = row.get(0);
            let position_i64: i64 = row.get(1);
            let Ok(position) = i32::try_from(position_i64) else {
                continue;
            };
            let name: String = row.get(2);
            field_name_map.insert((notetype_id, position), name);
        }
    }

    let mut events: Vec<CommitSnapshotEvent> = Vec::new();
    let mut field_changes: Vec<CommitFieldChange> = Vec::new();
    let mut tag_changes: Vec<CommitTagChange> = Vec::new();
    let mut deleted_note_ids: HashSet<i64> = HashSet::new();
    let mut move_changes: Vec<CommitMoveChange> = Vec::new();

    for row in event_rows {
        let event_type: String = row.get(2);
        let old_value: Option<JsonValue> = row.get(3);
        let new_value: Option<JsonValue> = row.get(4);
        let reviewed: bool = row.get::<_, Option<bool>>(6).unwrap_or(false);
        let notetype_id: Option<i64> = row.get(7);

        let old_text = summarize_event_text(&event_type, &old_value, true);
        let new_side_value = if new_value.is_none()
            && (event_type == "field_change_denied" || event_type == "tag_change_denied")
        {
            &old_value
        } else {
            &new_value
        };
        let new_text = summarize_event_text(&event_type, new_side_value, false);

        let diff_html = if event_type == "field_updated" {
            let old_content = old_value
                .as_ref()
                .and_then(|v| extract_field_content(v))
                .unwrap_or("");
            let new_content = new_value
                .as_ref()
                .and_then(|v| extract_field_content(v))
                .unwrap_or("");

            if old_content.is_empty() && new_content.is_empty() {
                None
            } else {
                let clean_old = cleanser::clean(old_content);
                let clean_new = cleanser::clean(new_content);
                let raw_diff = htmldiff::htmldiff(&clean_old, &clean_new);
                Some(sanitize_diff_html(&raw_diff))
            }
        } else {
            None
        };

        let field_name = if event_type.contains("field") {
            let position = new_value
                .as_ref()
                .and_then(extract_position)
                .or_else(|| old_value.as_ref().and_then(extract_position));
            if let (Some(nt), Some(pos)) = (notetype_id, position) {
                field_name_map.get(&(nt, pos)).cloned()
            } else {
                None
            }
        } else {
            None
        };

        if event_type == "field_added" || event_type == "field_updated" || event_type == "field_removed" {
            let position = new_value
                .as_ref()
                .and_then(extract_position)
                .or_else(|| old_value.as_ref().and_then(extract_position))
                .unwrap_or(0);
            field_changes.push(CommitFieldChange {
                note_id: row.get(0),
                position,
                previous_content: old_text.clone(),
                suggested_content: new_text.clone().unwrap_or_default(),
                reviewed,
            });
        }

        if event_type == "tag_added" || event_type == "tag_removed" || event_type == "tag_change_denied" {
            let action = if event_type == "tag_removed" {
                false
            } else if event_type == "tag_change_denied" {
                new_value
                    .as_ref()
                    .and_then(|v| v.get("action"))
                    .and_then(|v| v.as_bool())
                    .or_else(|| {
                        old_value
                            .as_ref()
                            .and_then(|v| v.get("action"))
                            .and_then(|v| v.as_bool())
                    })
                    .unwrap_or(true)
            } else {
                true
            };
            let content = new_value
                .as_ref()
                .and_then(|v| v.get("content"))
                .and_then(|v| v.as_str())
                .or_else(|| {
                    old_value
                        .as_ref()
                        .and_then(|v| v.get("content"))
                        .and_then(|v| v.as_str())
                })
                .unwrap_or("")
                .to_string();
            tag_changes.push(CommitTagChange {
                note_id: row.get(0),
                content,
                action,
                reviewed,
            });
        }

        if event_type == "note_deleted" {
            let note_id: i64 = row.get(0);
            deleted_note_ids.insert(note_id);
        }

        if event_type == "note_moved" {
            move_changes.push(CommitMoveChange {
                note_id: row.get(0),
                from_deck: old_text.clone(),
                to_deck: new_text.clone(),
            });
        }

        events.push(CommitSnapshotEvent {
            note_id: row.get(0),
            note_guid: row.get(8),
            version: row.get(1),
            event_type,
            field_name,
            created_at: row.get(5),
            old_text,
            new_text,
            diff_html,
        });
    }

    let decision_row = client
        .query(
            "SELECT status, reason
             FROM notifications
             WHERE user_id = $1 AND commit_id = $2
             ORDER BY created_at DESC
             LIMIT 1",
            &[&user_id, &commit_id],
        )
        .await
        .map_err(|_| "Failed to load notification decision".to_string())?;

    let (decision_status, decision_reason) = if let Some(row) = decision_row.first() {
        (Some(row.get(0)), row.get(1))
    } else {
        (None, None)
    };

    Ok(CommitSnapshotResponse {
        commit_id: row.get(0),
        rationale: row.get(1),
        info: row.get(2),
        timestamp: row.get(3),
        deck_name: deck_display_name(&row.get::<_, String>(4)),
        author: row.get(5),
        field_changes,
        tag_changes,
        deleted_note_ids: deleted_note_ids.into_iter().collect(),
        move_changes,
        events,
        decision_status,
        decision_reason,
    })
}

fn extract_position(value: &JsonValue) -> Option<i32> {
    value
        .get("position")
        .and_then(|v| v.as_i64())
        .and_then(|v| i32::try_from(v).ok())
}

fn extract_field_content(value: &JsonValue) -> Option<&str> {
    value
        .get("content")
        .and_then(|v| v.as_str())
        .or_else(|| value.get("value").and_then(|v| v.as_str()))
}

fn summarize_event_text(event_type: &str, value: &Option<JsonValue>, old_side: bool) -> Option<String> {
    let json = value.as_ref()?;
    match event_type {
        "field_added" | "field_removed" | "field_updated" => extract_field_content(json)
            .map(|s| cleanser::clean(s)),
        "tag_added" | "tag_removed" => json
            .get("content")
            .and_then(|v| v.as_str())
            .map(|s| format!("#{}", s)),
        "note_moved" => {
            let key = if old_side { "from" } else { "to" };
            json.get(key)
                .and_then(|v| v.as_str())
                .map(|s| cleanser::clean(s))
                .filter(|s| !s.trim().is_empty())
                .or_else(|| Some("deck updated".to_string()))
        }
        "note_deleted" => Some("note deleted".to_string()),
        "commit_approved_effect" => Some("commit approved".to_string()),
        "commit_denied_effect" => Some("commit denied".to_string()),
        "suggestion_denied" => Some("suggestion denied".to_string()),
        "field_change_denied" => {
            let key = if old_side {
                "current_content"
            } else {
                "denied_content"
            };
            json.get(key)
                .and_then(|v| v.as_str())
                .map(|s| cleanser::clean(s))
        }
        "tag_change_denied" => {
            let content = json
                .get("content")
                .and_then(|v| v.as_str())
                .map(|s| cleanser::clean(s))
                .unwrap_or_default();
            let action = json.get("action").and_then(|v| v.as_bool()).unwrap_or(true);
            if content.trim().is_empty() {
                Some("tag change denied".to_string())
            } else if action {
                Some(format!("Denied adding tag #{}", content))
            } else {
                Some(format!("Denied removing tag #{}", content))
            }
        }
        _ => None,
    }
}
