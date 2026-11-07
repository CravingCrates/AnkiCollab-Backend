use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::AuthenticatedUser;
use crate::database::{AppState, SharedConn};
use crate::validate_deck_hash;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DemocracySettings {
    pub deck_id: i64,
    pub enabled: bool,
    pub mode: String,
    pub approval_threshold: i32,
    pub require_unanimity: bool,
    pub weekly_vote_limit: i32,
    pub min_days_subscribed: i32,
    pub min_cards_reviewed: i32,
    pub notifications_enabled: bool,
}

impl Default for DemocracySettings {
    fn default() -> Self {
        Self {
            deck_id: 0,
            enabled: false,
            mode: "consensus_strict".to_string(),
            approval_threshold: 5,
            require_unanimity: true,
            weekly_vote_limit: 20,
            min_days_subscribed: 14,
            min_cards_reviewed: 200,
            notifications_enabled: true,
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct VoteRequest {
    pub deck_hash: String,
    pub suggestion_type: String, // "field", "tag", "card_deletion", "note_move"
    pub suggestion_id: i64,
    pub vote: String, // "approve", "reject", "flag"
    #[serde(default)]
    pub early_adopted: bool,
}

#[derive(Debug, Serialize)]
pub struct VoteResponse {
    pub success: bool,
    pub status: String, // "voting", "approved", "rejected", "flagged_for_maintainer"
    pub remaining_weekly_votes: i32,
    pub approve_count: i32,
    pub reject_count: i32,
    pub message: String,
}

#[derive(Debug, Serialize)]
pub struct ModeratorStatusResponse {
    pub is_eligible: bool,
    pub reason: String,
    pub weekly_votes_limit: i32,
    pub remaining_weekly_votes: i32,
    pub mode: String,
    pub approval_threshold: i32,
    pub require_unanimity: bool,
    pub notifications_enabled: bool,
    pub allowed_tags: Option<Vec<String>>,
}

#[derive(Debug, Serialize)]
pub struct QueueItem {
    pub suggestion_type: String,
    pub suggestion_id: i64,
    pub note_id: i64,
    pub note_guid: String,
    pub field_position: Option<u32>,
    pub field_name: Option<String>,
    pub old_content: Option<String>,
    pub new_content: Option<String>,
    pub recto: Option<String>,
    pub verso: Option<String>,
    pub tag: Option<String>,
    pub tag_action: Option<bool>,
    pub approve_count: i32,
    pub reject_count: i32,
    pub threshold: i32,
    pub mode: String,
}

#[derive(Debug, Deserialize)]
pub struct SelfSpecializationRequest {
    pub deck_hash: String,
    pub tags: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct SelfSpecializationResponse {
    pub success: bool,
    pub tags: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct SyncEligibilityRequest {
    pub deck_hash: String,
    pub cards_reviewed_count: i32,
}

#[derive(Debug, Serialize)]
pub struct SyncEligibilityResponse {
    pub success: bool,
    pub is_eligible: bool,
    pub reason: String,
    pub notifications_enabled: bool,
}

#[derive(Debug, Serialize)]
pub struct AuditVoteDto {
    pub user_id: i32,
    pub username: String,
    pub vote: String,
    pub weight: f64,
    pub early_adopted: bool,
    pub voted_at: String,
}

#[derive(Debug, Serialize)]
pub struct AuditEntryDto {
    pub suggestion_type: String,
    pub suggestion_id: i64,
    pub deck_id: i64,
    pub status: String,
    pub approve_count: i32,
    pub reject_count: i32,
    pub note_id: Option<i64>,
    pub note_guid: Option<String>,
    pub field_position: Option<u32>,
    pub field_name: Option<String>,
    pub original_content: Option<String>,
    pub new_content: Option<String>,
    pub tag_name: Option<String>,
    pub tag_action: Option<bool>,
    pub resolved_at: Option<String>,
    pub reverted_at: Option<String>,
    pub reverted_by_username: Option<String>,
    pub override_reason: Option<String>,
    pub votes: Vec<AuditVoteDto>,
}

#[derive(Debug, Deserialize)]
pub struct OverrideRequest {
    pub deck_hash: String,
    pub suggestion_type: String,
    pub suggestion_id: i64,
    pub action: String, // "revert", "confirm_approval", "admin_reject"
    pub reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct OverrideResponse {
    pub success: bool,
    pub message: String,
    pub new_status: String,
}

#[derive(Debug, Deserialize)]
pub struct RosterActionRequest {
    pub deck_hash: String,
    pub username: String,
    pub status: String, // "blacklist", "whitelist"
    pub allowed_tags: Option<Vec<String>>,
    pub reason: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RosterActionResponse {
    pub success: bool,
    pub message: String,
}

pub async fn bump_note_subscribers(
    tx: &tokio_postgres::Transaction<'_>,
    note_id: i64,
) {
    if let Ok(subs) = tx
        .query(
            "SELECT subscriber_note_id FROM anki.note_inheritance WHERE base_note_id = $1",
            &[&note_id],
        )
        .await
    {
        if !subs.is_empty() {
            let sub_ids: Vec<i64> = subs.into_iter().map(|r| r.get(0)).collect();
            let _ = tx
                .execute(
                    "UPDATE anki.notes SET last_update = NOW() WHERE id = ANY($1::bigint[])",
                    &[&sub_ids],
                )
                .await;
            let _ = tx
                .execute(
                    "WITH RECURSIVE roots AS ( \
                         SELECT DISTINCT n.deck AS id FROM anki.notes n WHERE n.id = ANY($1::bigint[]) \
                     ), up AS ( \
                         SELECT d.id, d.parent FROM anki.decks d JOIN roots r ON d.id = r.id \
                         UNION ALL \
                         SELECT d.id, d.parent FROM anki.decks d JOIN up ON d.id = up.parent \
                     ) UPDATE anki.decks SET last_update = NOW() WHERE id IN (SELECT id FROM up)",
                    &[&sub_ids],
                )
                .await;
        }
    }
}

pub async fn check_deck_admin(
    client: &SharedConn,
    deck_id: i64,
    user_id: i32,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let row = client
        .query_opt(
            "SELECT 1 FROM anki.decks WHERE id = $1 AND owner = $2 \
             UNION \
             SELECT 1 FROM anki.maintainers WHERE deck = $1 AND user_id = $2",
            &[&deck_id, &user_id],
        )
        .await?;
    Ok(row.is_some())
}

pub async fn get_deck_id_by_hash(
    client: &SharedConn,
    deck_hash: &str,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let row = client
        .query_opt(
            "SELECT id FROM anki.decks WHERE human_hash = $1",
            &[&deck_hash],
        )
        .await?;
    match row {
        Some(r) => Ok(r.get(0)),
        None => Err(format!("Deck hash {deck_hash} not found").into()),
    }
}

pub async fn get_democracy_settings(
    client: &SharedConn,
    deck_id: i64,
) -> Result<DemocracySettings, Box<dyn std::error::Error + Send + Sync>> {
    let row = client
        .query_opt(
            "SELECT deck_id, enabled, mode, approval_threshold, require_unanimity, \
             weekly_vote_limit, min_days_subscribed, min_cards_reviewed, notifications_enabled \
             FROM anki.deck_democracy_settings WHERE deck_id = $1",
            &[&deck_id],
        )
        .await?;

    if let Some(r) = row {
        let raw_threshold: i32 = r.get(3);
        let raw_limit: i32 = r.get(5);
        Ok(DemocracySettings {
            deck_id: r.get(0),
            enabled: r.get(1),
            mode: r.get(2),
            approval_threshold: raw_threshold.max(1),
            require_unanimity: r.get(4),
            weekly_vote_limit: raw_limit.max(1),
            min_days_subscribed: r.get(6),
            min_cards_reviewed: r.get(7),
            notifications_enabled: r.get(8),
        })
    } else {
        let mut def = DemocracySettings::default();
        def.deck_id = deck_id;
        Ok(def)
    }
}

pub async fn get_remaining_weekly_votes(
    client: &SharedConn,
    deck_id: i64,
    user_id: i32,
    weekly_limit: i32,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    let row = client
        .query_one(
            "SELECT count(1)::int FROM anki.deck_moderator_votes \
             WHERE deck_id = $1 AND user_id = $2 AND voted_at > (NOW() - INTERVAL '7 days')",
            &[&deck_id, &user_id],
        )
        .await?;
    let used: i32 = row.get(0);
    Ok((weekly_limit - used).max(0))
}

pub async fn check_user_eligibility(
    client: &SharedConn,
    deck_id: i64,
    user_id: i32,
    reported_reviews: i32,
) -> Result<(bool, String, Option<Vec<String>>), Box<dyn std::error::Error + Send + Sync>> {
    let roster_row = client
        .query_opt(
            "SELECT status, allowed_tags, COALESCE(reviews_count, 0) FROM anki.deck_moderator_roster \
             WHERE deck_id = $1 AND user_id = $2",
            &[&deck_id, &user_id],
        )
        .await?;

    let settings = get_democracy_settings(client, deck_id).await?;
    if !settings.enabled {
        return Ok((
            false,
            "Community moderation disabled for this deck".to_string(),
            None,
        ));
    }

    let mut is_auto_qualified = false;
    let mut roster_allowed_tags: Option<Vec<String>> = None;
    let mut saved_reviews: i32 = 0;

    if let Some(row) = roster_row {
        let status: String = row.get(0);
        let allowed_tags: Option<Vec<String>> = row.get(1);
        saved_reviews = row.get(2);
        if status == "blacklist" {
            return Ok((false, "User is on deck blacklist".to_string(), None));
        }
        if status == "whitelist" || status == "active" {
            return Ok((
                true,
                "Eligible community moderator".to_string(),
                allowed_tags,
            ));
        }
        if status == "auto_qualified" {
            is_auto_qualified = true;
            roster_allowed_tags = allowed_tags;
        }
    }

    let effective_reviews = if reported_reviews > 0 {
        reported_reviews
    } else {
        saved_reviews
    };

    if effective_reviews < settings.min_cards_reviewed {
        return Ok((
            false,
            format!(
                "Minimum {} cards reviewed required (current: {})",
                settings.min_cards_reviewed, effective_reviews
            ),
            None,
        ));
    }

    let user_info = client
        .query_opt(
            "SELECT username, uuid::text FROM public.users WHERE id = $1",
            &[&user_id],
        )
        .await?;

    let (username, user_uuid) = match user_info {
        Some(u) => (u.get::<_, String>(0), u.get::<_, String>(1)),
        None => return Ok((false, "User not found".to_string(), None)),
    };
    let hash_sha256 = crate::auth::user_hash_from_username(&username);

    let sub_row = client
        .query_opt(
            "SELECT created_at FROM anki.subscriptions \
             WHERE deck_id = $1 AND (user_hash = $2 OR user_hash = $3) \
             ORDER BY created_at ASC LIMIT 1",
            &[&deck_id, &hash_sha256, &user_uuid],
        )
        .await?;

    if settings.min_days_subscribed > 0 {
        if let Some(r) = sub_row {
            let created_at: Option<chrono::DateTime<chrono::Utc>> = r.get(0);
            if let Some(ca) = created_at {
                let duration = chrono::Utc::now() - ca;
                if duration.num_days() < i64::from(settings.min_days_subscribed) {
                    return Ok((
                        false,
                        format!(
                            "Minimum {} days subscription required (current: {})",
                            settings.min_days_subscribed,
                            duration.num_days()
                        ),
                        None,
                    ));
                }
            }
        } else {
            return Ok((
                false,
                "Active subscription required to moderate this deck".to_string(),
                None,
            ));
        }
    }

    if is_auto_qualified {
        return Ok((
            true,
            "Eligible community moderator".to_string(),
            roster_allowed_tags,
        ));
    }

    // Semi-automatic roster qualification: user automatically promoted to auto_qualified
    let _ = client
        .execute(
            "INSERT INTO anki.deck_moderator_roster (deck_id, user_id, status, reviews_count) \
             VALUES ($1, $2, 'auto_qualified', $3) \
             ON CONFLICT (deck_id, user_id) DO UPDATE SET \
                reviews_count = GREATEST(COALESCE(anki.deck_moderator_roster.reviews_count, 0), EXCLUDED.reviews_count), \
                updated_at = NOW()",
            &[&deck_id, &user_id, &effective_reviews],
        )
        .await;

    let tag_row = client
        .query_opt(
            "SELECT allowed_tags FROM anki.deck_moderator_roster WHERE deck_id = $1 AND user_id = $2",
            &[&deck_id, &user_id],
        )
        .await?;
    let allowed_tags: Option<Vec<String>> = tag_row.and_then(|r| r.get(0));

    Ok((
        true,
        "Semi-automatic qualification: criteria met".to_string(),
        allowed_tags,
    ))
}

pub async fn get_status(
    auth_user: AuthenticatedUser,
    State(db_state): State<Arc<AppState>>,
    Path(deck_hash): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !validate_deck_hash(&deck_hash) {
        return Err((StatusCode::BAD_REQUEST, "Invalid deck hash format".to_string()));
    }

    let client = db_state
        .db_pool
        .get_owned()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let deck_id = get_deck_id_by_hash(&client, &deck_hash)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    let settings = get_democracy_settings(&client, deck_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let (is_eligible, reason, allowed_tags) =
        check_user_eligibility(&client, deck_id, auth_user.user_id, 0)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let remaining_votes = get_remaining_weekly_votes(
        &client,
        deck_id,
        auth_user.user_id,
        settings.weekly_vote_limit,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(ModeratorStatusResponse {
        is_eligible,
        reason,
        weekly_votes_limit: settings.weekly_vote_limit,
        remaining_weekly_votes: remaining_votes,
        mode: settings.mode,
        approval_threshold: settings.approval_threshold,
        require_unanimity: settings.require_unanimity,
        notifications_enabled: settings.notifications_enabled,
        allowed_tags,
    }))
}

pub async fn sync_eligibility(
    auth_user: AuthenticatedUser,
    State(db_state): State<Arc<AppState>>,
    Json(payload): Json<SyncEligibilityRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !validate_deck_hash(&payload.deck_hash) {
        return Err((StatusCode::BAD_REQUEST, "Invalid deck hash format".to_string()));
    }

    let client = db_state
        .db_pool
        .get_owned()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let deck_id = get_deck_id_by_hash(&client, &payload.deck_hash)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    let settings = get_democracy_settings(&client, deck_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if payload.cards_reviewed_count > 0 {
        let _ = client
            .execute(
                "INSERT INTO anki.deck_moderator_roster (deck_id, user_id, status, reviews_count) \
                 VALUES ($1, $2, 'auto_qualified', $3) \
                 ON CONFLICT (deck_id, user_id) DO UPDATE SET \
                    reviews_count = GREATEST(COALESCE(anki.deck_moderator_roster.reviews_count, 0), EXCLUDED.reviews_count), \
                    updated_at = NOW()",
                &[&deck_id, &auth_user.user_id, &payload.cards_reviewed_count],
            )
            .await;
    }

    let (is_eligible, reason, _) = check_user_eligibility(
        &client,
        deck_id,
        auth_user.user_id,
        payload.cards_reviewed_count,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(SyncEligibilityResponse {
        success: true,
        is_eligible,
        reason,
        notifications_enabled: settings.notifications_enabled,
    }))
}

pub async fn get_queue(
    auth_user: AuthenticatedUser,
    State(db_state): State<Arc<AppState>>,
    Path(deck_hash): Path<String>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !validate_deck_hash(&deck_hash) {
        return Err((StatusCode::BAD_REQUEST, "Invalid deck hash format".to_string()));
    }

    let client = db_state
        .db_pool
        .get_owned()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let deck_id = get_deck_id_by_hash(&client, &deck_hash)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    let settings = get_democracy_settings(&client, deck_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let (is_eligible, _, allowed_tags) =
        check_user_eligibility(&client, deck_id, auth_user.user_id, 0)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if !is_eligible {
        return Ok(Json(Vec::<QueueItem>::new()));
    }

    let field_rows = if let Some(ref tags) = allowed_tags {
        if !tags.is_empty() {
            client
                .query(
                    "SELECT f.id, f.note, n.guid, f.position, f.content, \
                            COALESCE(rf.content, '') AS old_content, \
                            COALESCE(sms.approve_count, 0) AS approves, \
                            COALESCE(sms.reject_count, 0) AS rejects, \
                            COALESCE(r0.content, '') AS recto_content, \
                            COALESCE(r1.content, '') AS verso_content \
                     FROM anki.fields f \
                     JOIN anki.notes n ON f.note = n.id \
                     LEFT JOIN anki.commits c ON f.commit = c.commit_id \
                     LEFT JOIN anki.fields rf ON rf.note = f.note AND rf.position = f.position AND rf.reviewed = true \
                     LEFT JOIN anki.fields r0 ON r0.note = f.note AND r0.position = 0 AND r0.reviewed = true \
                     LEFT JOIN anki.fields r1 ON r1.note = f.note AND r1.position = 1 AND r1.reviewed = true \
                     LEFT JOIN anki.suggestion_moderation_status sms ON sms.suggestion_type = 'field' AND sms.suggestion_id = f.id \
                     WHERE n.deck IN (WITH RECURSIVE dt AS (SELECT id FROM anki.decks WHERE id = $1 UNION ALL SELECT d.id FROM anki.decks d INNER JOIN dt ON dt.id = d.parent) SELECT id FROM dt) \
                       AND f.reviewed = false AND n.reviewed = true AND n.deleted = false \
                       AND (c.user_id IS NULL OR c.user_id != $2) \
                       AND COALESCE(sms.status, 'voting') = 'voting' \
                       AND f.id NOT IN ( \
                           SELECT suggestion_id FROM anki.deck_moderator_votes \
                           WHERE user_id = $2 AND suggestion_type = 'field' \
                       ) \
                       AND EXISTS ( \
                           SELECT 1 FROM anki.tags nt WHERE nt.note = f.note AND nt.reviewed = true AND nt.content = ANY($3) \
                       ) \
                     ORDER BY f.id ASC LIMIT 50",
                    &[&deck_id, &auth_user.user_id, tags],
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        } else {
            client
                .query(
                    "SELECT f.id, f.note, n.guid, f.position, f.content, \
                            COALESCE(rf.content, '') AS old_content, \
                            COALESCE(sms.approve_count, 0) AS approves, \
                            COALESCE(sms.reject_count, 0) AS rejects, \
                            COALESCE(r0.content, '') AS recto_content, \
                            COALESCE(r1.content, '') AS verso_content \
                     FROM anki.fields f \
                     JOIN anki.notes n ON f.note = n.id \
                     LEFT JOIN anki.commits c ON f.commit = c.commit_id \
                     LEFT JOIN anki.fields rf ON rf.note = f.note AND rf.position = f.position AND rf.reviewed = true \
                     LEFT JOIN anki.fields r0 ON r0.note = f.note AND r0.position = 0 AND r0.reviewed = true \
                     LEFT JOIN anki.fields r1 ON r1.note = f.note AND r1.position = 1 AND r1.reviewed = true \
                     LEFT JOIN anki.suggestion_moderation_status sms ON sms.suggestion_type = 'field' AND sms.suggestion_id = f.id \
                     WHERE n.deck IN (WITH RECURSIVE dt AS (SELECT id FROM anki.decks WHERE id = $1 UNION ALL SELECT d.id FROM anki.decks d INNER JOIN dt ON dt.id = d.parent) SELECT id FROM dt) \
                       AND f.reviewed = false AND n.reviewed = true AND n.deleted = false \
                       AND (c.user_id IS NULL OR c.user_id != $2) \
                       AND COALESCE(sms.status, 'voting') = 'voting' \
                       AND f.id NOT IN ( \
                           SELECT suggestion_id FROM anki.deck_moderator_votes \
                           WHERE user_id = $2 AND suggestion_type = 'field' \
                       ) \
                     ORDER BY f.id ASC LIMIT 50",
                    &[&deck_id, &auth_user.user_id],
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        }
    } else {
        client
            .query(
                "SELECT f.id, f.note, n.guid, f.position, f.content, \
                        COALESCE(rf.content, '') AS old_content, \
                        COALESCE(sms.approve_count, 0) AS approves, \
                        COALESCE(sms.reject_count, 0) AS rejects, \
                        COALESCE(r0.content, '') AS recto_content, \
                        COALESCE(r1.content, '') AS verso_content \
                 FROM anki.fields f \
                 JOIN anki.notes n ON f.note = n.id \
                 LEFT JOIN anki.commits c ON f.commit = c.commit_id \
                 LEFT JOIN anki.fields rf ON rf.note = f.note AND rf.position = f.position AND rf.reviewed = true \
                 LEFT JOIN anki.fields r0 ON r0.note = f.note AND r0.position = 0 AND r0.reviewed = true \
                 LEFT JOIN anki.fields r1 ON r1.note = f.note AND r1.position = 1 AND r1.reviewed = true \
                 LEFT JOIN anki.suggestion_moderation_status sms ON sms.suggestion_type = 'field' AND sms.suggestion_id = f.id \
                 WHERE n.deck IN (WITH RECURSIVE dt AS (SELECT id FROM anki.decks WHERE id = $1 UNION ALL SELECT d.id FROM anki.decks d INNER JOIN dt ON dt.id = d.parent) SELECT id FROM dt) \
                   AND f.reviewed = false AND n.reviewed = true AND n.deleted = false \
                   AND (c.user_id IS NULL OR c.user_id != $2) \
                   AND COALESCE(sms.status, 'voting') = 'voting' \
                   AND f.id NOT IN ( \
                       SELECT suggestion_id FROM anki.deck_moderator_votes \
                       WHERE user_id = $2 AND suggestion_type = 'field' \
                   ) \
                 ORDER BY f.id ASC LIMIT 50",
                &[&deck_id, &auth_user.user_id],
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    let mut queue = Vec::new();
    for row in field_rows {
        let field_id: i64 = row.get(0);
        let note_id: i64 = row.get(1);
        let guid: String = row.get(2);
        let pos: u32 = row.get(3);
        let new_content: String = row.get(4);
        let old_content: String = row.get(5);
        let approves: i32 = row.get(6);
        let rejects: i32 = row.get(7);
        let recto: String = row.get(8);
        let verso: String = row.get(9);

        queue.push(QueueItem {
            suggestion_type: "field".to_string(),
            suggestion_id: field_id,
            note_id,
            note_guid: guid,
            field_position: Some(pos),
            field_name: None,
            old_content: Some(old_content),
            new_content: Some(new_content),
            recto: if recto.is_empty() { None } else { Some(recto) },
            verso: if verso.is_empty() { None } else { Some(verso) },
            tag: None,
            tag_action: None,
            approve_count: approves,
            reject_count: rejects,
            threshold: settings.approval_threshold,
            mode: settings.mode.clone(),
        });
    }

    let tag_rows = if let Some(ref tags) = allowed_tags {
        if !tags.is_empty() {
            client
                .query(
                    "SELECT t.id, t.note, n.guid, t.content, t.action, \
                            COALESCE(sms.approve_count, 0) AS approves, \
                            COALESCE(sms.reject_count, 0) AS rejects, \
                            COALESCE(r0.content, '') AS recto_content, \
                            COALESCE(r1.content, '') AS verso_content \
                     FROM anki.tags t \
                     JOIN anki.notes n ON t.note = n.id \
                     LEFT JOIN anki.commits c ON t.commit = c.commit_id \
                     LEFT JOIN anki.fields r0 ON r0.note = t.note AND r0.position = 0 AND r0.reviewed = true \
                     LEFT JOIN anki.fields r1 ON r1.note = t.note AND r1.position = 1 AND r1.reviewed = true \
                     LEFT JOIN anki.suggestion_moderation_status sms ON sms.suggestion_type = 'tag' AND sms.suggestion_id = t.id \
                     WHERE n.deck IN (WITH RECURSIVE dt AS (SELECT id FROM anki.decks WHERE id = $1 UNION ALL SELECT d.id FROM anki.decks d INNER JOIN dt ON dt.id = d.parent) SELECT id FROM dt) \
                       AND t.reviewed = false AND n.reviewed = true AND n.deleted = false \
                       AND (c.user_id IS NULL OR c.user_id != $2) \
                       AND COALESCE(sms.status, 'voting') = 'voting' \
                       AND t.id NOT IN ( \
                           SELECT suggestion_id FROM anki.deck_moderator_votes \
                           WHERE user_id = $2 AND suggestion_type = 'tag' \
                       ) \
                       AND (t.content = ANY($3) OR EXISTS ( \
                           SELECT 1 FROM anki.tags nt WHERE nt.note = t.note AND nt.reviewed = true AND nt.content = ANY($3) \
                       )) \
                     ORDER BY t.id ASC LIMIT 50",
                    &[&deck_id, &auth_user.user_id, tags],
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        } else {
            client
                .query(
                    "SELECT t.id, t.note, n.guid, t.content, t.action, \
                            COALESCE(sms.approve_count, 0) AS approves, \
                            COALESCE(sms.reject_count, 0) AS rejects, \
                            COALESCE(r0.content, '') AS recto_content, \
                            COALESCE(r1.content, '') AS verso_content \
                     FROM anki.tags t \
                     JOIN anki.notes n ON t.note = n.id \
                     LEFT JOIN anki.commits c ON t.commit = c.commit_id \
                     LEFT JOIN anki.fields r0 ON r0.note = t.note AND r0.position = 0 AND r0.reviewed = true \
                     LEFT JOIN anki.fields r1 ON r1.note = t.note AND r1.position = 1 AND r1.reviewed = true \
                     LEFT JOIN anki.suggestion_moderation_status sms ON sms.suggestion_type = 'tag' AND sms.suggestion_id = t.id \
                     WHERE n.deck IN (WITH RECURSIVE dt AS (SELECT id FROM anki.decks WHERE id = $1 UNION ALL SELECT d.id FROM anki.decks d INNER JOIN dt ON dt.id = d.parent) SELECT id FROM dt) \
                       AND t.reviewed = false AND n.reviewed = true AND n.deleted = false \
                       AND (c.user_id IS NULL OR c.user_id != $2) \
                       AND COALESCE(sms.status, 'voting') = 'voting' \
                       AND t.id NOT IN ( \
                           SELECT suggestion_id FROM anki.deck_moderator_votes \
                           WHERE user_id = $2 AND suggestion_type = 'tag' \
                       ) \
                     ORDER BY t.id ASC LIMIT 50",
                    &[&deck_id, &auth_user.user_id],
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
        }
    } else {
        client
            .query(
                "SELECT t.id, t.note, n.guid, t.content, t.action, \
                        COALESCE(sms.approve_count, 0) AS approves, \
                        COALESCE(sms.reject_count, 0) AS rejects, \
                        COALESCE(r0.content, '') AS recto_content, \
                        COALESCE(r1.content, '') AS verso_content \
                 FROM anki.tags t \
                 JOIN anki.notes n ON t.note = n.id \
                 LEFT JOIN anki.commits c ON t.commit = c.commit_id \
                 LEFT JOIN anki.fields r0 ON r0.note = t.note AND r0.position = 0 AND r0.reviewed = true \
                 LEFT JOIN anki.fields r1 ON r1.note = t.note AND r1.position = 1 AND r1.reviewed = true \
                 LEFT JOIN anki.suggestion_moderation_status sms ON sms.suggestion_type = 'tag' AND sms.suggestion_id = t.id \
                 WHERE n.deck IN (WITH RECURSIVE dt AS (SELECT id FROM anki.decks WHERE id = $1 UNION ALL SELECT d.id FROM anki.decks d INNER JOIN dt ON dt.id = d.parent) SELECT id FROM dt) \
                   AND t.reviewed = false AND n.reviewed = true AND n.deleted = false \
                   AND (c.user_id IS NULL OR c.user_id != $2) \
                   AND COALESCE(sms.status, 'voting') = 'voting' \
                   AND t.id NOT IN ( \
                       SELECT suggestion_id FROM anki.deck_moderator_votes \
                       WHERE user_id = $2 AND suggestion_type = 'tag' \
                   ) \
                 ORDER BY t.id ASC LIMIT 50",
                &[&deck_id, &auth_user.user_id],
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?
    };

    for row in tag_rows {
        let tag_id: i64 = row.get(0);
        let note_id: i64 = row.get(1);
        let guid: String = row.get(2);
        let tag_content: String = row.get(3);
        let action: Option<bool> = row.get(4);
        let approves: i32 = row.get(5);
        let rejects: i32 = row.get(6);
        let recto: String = row.get(7);
        let verso: String = row.get(8);

        queue.push(QueueItem {
            suggestion_type: "tag".to_string(),
            suggestion_id: tag_id,
            note_id,
            note_guid: guid,
            field_position: None,
            field_name: None,
            old_content: None,
            new_content: None,
            recto: if recto.is_empty() { None } else { Some(recto) },
            verso: if verso.is_empty() { None } else { Some(verso) },
            tag: Some(tag_content),
            tag_action: action,
            approve_count: approves,
            reject_count: rejects,
            threshold: settings.approval_threshold,
            mode: settings.mode.clone(),
        });
    }

    Ok(Json(queue))
}

pub async fn self_specialization(
    auth_user: AuthenticatedUser,
    State(db_state): State<Arc<AppState>>,
    Json(payload): Json<SelfSpecializationRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !validate_deck_hash(&payload.deck_hash) {
        return Err((StatusCode::BAD_REQUEST, "Invalid deck hash format".to_string()));
    }

    let client = db_state
        .db_pool
        .get_owned()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let deck_id = get_deck_id_by_hash(&client, &payload.deck_hash)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    let (is_eligible, reason, _) = check_user_eligibility(&client, deck_id, auth_user.user_id, 0)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if !is_eligible {
        return Err((StatusCode::FORBIDDEN, reason));
    }

    let existing_roster = client
        .query_opt(
            "SELECT status, allowed_tags FROM anki.deck_moderator_roster \
             WHERE deck_id = $1 AND user_id = $2",
            &[&deck_id, &auth_user.user_id],
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if let Some(r) = existing_roster {
        let status: String = r.get(0);
        let current_tags: Option<Vec<String>> = r.get(1);
        if (status == "whitelist" || status == "active")
            && current_tags.as_ref().is_some_and(|t| !t.is_empty())
        {
            return Err((
                StatusCode::FORBIDDEN,
                "Tag restrictions were assigned by a deck maintainer and cannot be overridden via self-specialization."
                    .to_string(),
            ));
        }
    }

    let sanitized_tags: Vec<String> = payload
        .tags
        .into_iter()
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty() && t.len() <= 256)
        .take(50)
        .collect();

    client
        .execute(
            "INSERT INTO anki.deck_moderator_roster (deck_id, user_id, status, allowed_tags) \
             VALUES ($1, $2, 'auto_qualified', $3) \
             ON CONFLICT (deck_id, user_id) DO UPDATE SET allowed_tags = EXCLUDED.allowed_tags",
            &[&deck_id, &auth_user.user_id, &sanitized_tags],
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(SelfSpecializationResponse {
        success: true,
        tags: sanitized_tags,
    }))
}

pub async fn vote(
    auth_user: AuthenticatedUser,
    State(db_state): State<Arc<AppState>>,
    Json(payload): Json<VoteRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !validate_deck_hash(&payload.deck_hash) {
        return Err((StatusCode::BAD_REQUEST, "Invalid deck hash format".to_string()));
    }

    if payload.suggestion_type != "field" && payload.suggestion_type != "tag" {
        return Err((
            StatusCode::BAD_REQUEST,
            "Invalid suggestion_type (must be 'field' or 'tag')".to_string(),
        ));
    }

    let mut client = db_state
        .db_pool
        .get_owned()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let deck_id = get_deck_id_by_hash(&client, &payload.deck_hash)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    let settings = get_democracy_settings(&client, deck_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if !settings.enabled {
        return Err((
            StatusCode::FORBIDDEN,
            "Community moderation is disabled on this deck".to_string(),
        ));
    }

    let (is_eligible, reason, allowed_tags) =
        check_user_eligibility(&client, deck_id, auth_user.user_id, 0)
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if !is_eligible {
        return Err((StatusCode::FORBIDDEN, reason));
    }

    let remaining_votes = get_remaining_weekly_votes(
        &client,
        deck_id,
        auth_user.user_id,
        settings.weekly_vote_limit,
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if remaining_votes <= 0 {
        return Err((
            StatusCode::TOO_MANY_REQUESTS,
            format!(
                "Weekly voting quota reached ({} votes per 7-day sliding window).",
                settings.weekly_vote_limit
            ),
        ));
    }

    let vote_val = match payload.vote.as_str() {
        "approve" => "approve",
        "reject" => "reject",
        "flag" => "flag",
        _ => return Err((StatusCode::BAD_REQUEST, "Invalid vote action".to_string())),
    };

    let tx = client
        .transaction()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut orig_content: Option<String> = None;
    let mut new_content_val: Option<String> = None;
    let mut field_pos_val: Option<u32> = None;
    let mut field_name_val: Option<String> = None;
    let note_id_val: Option<i64>;
    let mut tag_name_val: Option<String> = None;
    let mut tag_action_val: Option<bool> = None;
    let commit_id_val: Option<i32>;

    if payload.suggestion_type == "field" {
        let field_info = tx
            .query_opt(
                "SELECT f.note, f.position, f.content, f.commit, c.user_id, nf.name \
                 FROM anki.fields f \
                 INNER JOIN anki.notes n ON f.note = n.id \
                 LEFT JOIN anki.commits c ON f.commit = c.commit_id \
                 LEFT JOIN anki.notetype_field nf ON n.notetype = nf.notetype AND f.position = nf.position \
                 WHERE f.id = $1 AND f.reviewed = false AND n.reviewed = true AND n.deleted = false \
                   AND n.deck IN ( \
                       WITH RECURSIVE dt AS ( \
                           SELECT id FROM anki.decks WHERE id = $2 \
                           UNION ALL \
                           SELECT d.id FROM anki.decks d INNER JOIN dt ON dt.id = d.parent \
                       ) SELECT id FROM dt \
                   ) \
                 FOR UPDATE OF f",
                &[&payload.suggestion_id, &deck_id],
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let fi = match field_info {
            Some(row) => row,
            None => {
                return Err((
                    StatusCode::NOT_FOUND,
                    "Field suggestion not found, already reviewed, or does not belong to this deck"
                        .to_string(),
                ));
            }
        };

        let note_id: i64 = fi.get(0);
        let pos: u32 = fi.get(1);
        let new_content: Option<String> = fi.get(2);
        let commit_id: Option<i32> = fi.get(3);
        let author_user_id: Option<i32> = fi.get(4);
        let field_name: Option<String> = fi.get(5);

        if author_user_id == Some(auth_user.user_id) {
            return Err((
                StatusCode::FORBIDDEN,
                "You cannot vote on your own suggestion".to_string(),
            ));
        }

        if let Some(ref tags) = allowed_tags {
            if !tags.is_empty() {
                let tag_match = tx
                    .query_opt(
                        "SELECT 1 FROM anki.tags WHERE note = $1 AND reviewed = true AND content = ANY($2) LIMIT 1",
                        &[&note_id, tags],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                if tag_match.is_none() {
                    return Err((
                        StatusCode::FORBIDDEN,
                        "This note does not match your allowed moderation tag scope".to_string(),
                    ));
                }
            }
        }

        let old_rev = tx
            .query_opt(
                "SELECT content FROM anki.fields WHERE note = $1 AND position = $2 AND reviewed = true AND id != $3",
                &[&note_id, &pos, &payload.suggestion_id],
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        orig_content = old_rev.and_then(|r| r.get(0));
        note_id_val = Some(note_id);
        field_pos_val = Some(pos);
        field_name_val = field_name;
        new_content_val = new_content;
        commit_id_val = commit_id;
    } else {
        let tag_info = tx
            .query_opt(
                "SELECT t.note, t.content, t.action, t.commit, c.user_id \
                 FROM anki.tags t \
                 INNER JOIN anki.notes n ON t.note = n.id \
                 LEFT JOIN anki.commits c ON t.commit = c.commit_id \
                 WHERE t.id = $1 AND t.reviewed = false AND n.reviewed = true AND n.deleted = false \
                   AND n.deck IN ( \
                       WITH RECURSIVE dt AS ( \
                           SELECT id FROM anki.decks WHERE id = $2 \
                           UNION ALL \
                           SELECT d.id FROM anki.decks d INNER JOIN dt ON dt.id = d.parent \
                       ) SELECT id FROM dt \
                   ) \
                 FOR UPDATE OF t",
                &[&payload.suggestion_id, &deck_id],
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        let ti = match tag_info {
            Some(row) => row,
            None => {
                return Err((
                    StatusCode::NOT_FOUND,
                    "Tag suggestion not found, already reviewed, or does not belong to this deck"
                        .to_string(),
                ));
            }
        };

        let note_id: i64 = ti.get(0);
        let tag_name: String = ti.get(1);
        let action: Option<bool> = ti.get(2);
        let commit_id: Option<i32> = ti.get(3);
        let author_user_id: Option<i32> = ti.get(4);

        if author_user_id == Some(auth_user.user_id) {
            return Err((
                StatusCode::FORBIDDEN,
                "You cannot vote on your own suggestion".to_string(),
            ));
        }

        if let Some(ref tags) = allowed_tags {
            if !tags.is_empty() && !tags.contains(&tag_name) {
                let tag_match = tx
                    .query_opt(
                        "SELECT 1 FROM anki.tags WHERE note = $1 AND reviewed = true AND content = ANY($2) LIMIT 1",
                        &[&note_id, tags],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                if tag_match.is_none() {
                    return Err((
                        StatusCode::FORBIDDEN,
                        "This tag suggestion does not match your allowed moderation tag scope"
                            .to_string(),
                    ));
                }
            }
        }

        note_id_val = Some(note_id);
        tag_name_val = Some(tag_name.clone());
        tag_action_val = action;
        commit_id_val = commit_id;
        if action == Some(false) {
            orig_content = Some(tag_name);
        } else {
            new_content_val = Some(tag_name);
        }
    }

    let status_row = tx
        .query_opt(
            "SELECT status FROM anki.suggestion_moderation_status \
             WHERE suggestion_type = $1 AND suggestion_id = $2 \
             FOR UPDATE",
            &[&payload.suggestion_type, &payload.suggestion_id],
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if let Some(sr) = status_row {
        let cur_status: String = sr.get(0);
        if cur_status == "approved"
            || cur_status == "rejected"
            || cur_status == "approved_by_admin"
            || cur_status == "reverted_by_admin"
            || cur_status == "rejected_by_admin"
        {
            return Err((
                StatusCode::CONFLICT,
                format!("Suggestion has already been finalized with status: {cur_status}"),
            ));
        }
    }

    let rows_inserted = tx
        .execute(
            "INSERT INTO anki.deck_moderator_votes \
         (deck_id, suggestion_type, suggestion_id, user_id, vote, weight, early_adopted, voted_at) \
         VALUES ($1, $2, $3, $4, $5, 1.0, $6, NOW()) \
         ON CONFLICT (suggestion_type, suggestion_id, user_id) DO NOTHING",
            &[
                &deck_id,
                &payload.suggestion_type,
                &payload.suggestion_id,
                &auth_user.user_id,
                &vote_val,
                &payload.early_adopted,
            ],
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if rows_inserted == 0 {
        return Err((
            StatusCode::CONFLICT,
            "You have already voted on this suggestion".to_string(),
        ));
    }

    let tally_row = tx
        .query_one(
            "SELECT \
                COUNT(*) FILTER (WHERE vote = 'approve')::int AS approves, \
                COUNT(*) FILTER (WHERE vote = 'reject')::int AS rejects, \
                COUNT(*) FILTER (WHERE vote = 'flag')::int AS flags \
             FROM anki.deck_moderator_votes \
             WHERE suggestion_type = $1 AND suggestion_id = $2",
            &[&payload.suggestion_type, &payload.suggestion_id],
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let approves: i32 = tally_row.get(0);
    let rejects: i32 = tally_row.get(1);
    let flags: i32 = tally_row.get(2);

    let mut new_status = "voting".to_string();

    if flags > 0 || vote_val == "flag" {
        new_status = "flagged_for_maintainer".to_string();
    } else {
        match settings.mode.as_str() {
            "consensus_strict" => {
                if settings.require_unanimity && rejects > 0 {
                    new_status = "flagged_for_maintainer".to_string();
                } else if approves >= settings.approval_threshold && rejects == 0 {
                    new_status = "approved".to_string();
                }
            }
            "qualified_majority" => {
                let total = approves + rejects;
                if total >= settings.approval_threshold {
                    let ratio = f64::from(approves) / f64::from(total);
                    if ratio >= 0.80 {
                        new_status = "approved".to_string();
                    } else if ratio < 0.50 {
                        new_status = "rejected".to_string();
                    } else {
                        new_status = "flagged_for_maintainer".to_string();
                    }
                }
            }
            "lazy_consensus" => {
                if rejects > 0 {
                    new_status = "flagged_for_maintainer".to_string();
                } else if approves >= settings.approval_threshold {
                    new_status = "approved".to_string();
                }
            }
            _ => {
                if approves >= settings.approval_threshold {
                    new_status = "approved".to_string();
                }
            }
        }
    }

    if payload.suggestion_type == "field" {
        if let (Some(note_id), Some(pos)) = (note_id_val, field_pos_val) {
            let proposed_text = new_content_val.as_deref().unwrap_or("");
            if new_status == "approved" && pos == 0 && proposed_text.trim().is_empty() {
                new_status = "flagged_for_maintainer".to_string();
            }

            if new_status == "approved" {
                tx.execute(
                    "DELETE FROM anki.fields WHERE note = $1 AND position = $2 AND reviewed = true AND id != $3",
                    &[&note_id, &pos, &payload.suggestion_id],
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                tx.execute(
                    "UPDATE anki.fields SET reviewed = true WHERE id = $1",
                    &[&payload.suggestion_id],
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                let updated_note = tx
                    .query_opt(
                        "UPDATE anki.notes SET last_update = NOW(), version = version + 1 WHERE id = $1 RETURNING version",
                        &[&note_id],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                if let Some(nr) = updated_note {
                    let new_ver: i64 = nr.get(0);
                    let event_type = if orig_content.is_some() {
                        "field_updated"
                    } else {
                        "field_added"
                    };
                    let old_val = orig_content
                        .as_ref()
                        .map(|c| serde_json::json!({ "position": pos, "content": c }));
                    let new_val = new_content_val
                        .as_ref()
                        .map(|c| serde_json::json!({ "position": pos, "content": c }));
                    let _ = tx
                        .execute(
                            "INSERT INTO anki.note_events (note_id, version, event_type, commit_id, actor_user_id, approved, old_value, new_value) \
                             VALUES ($1, $2, $3, $4, $5, true, $6, $7)",
                            &[&note_id, &new_ver, &event_type, &commit_id_val, &auth_user.user_id, &old_val, &new_val],
                        )
                        .await;
                    let _ = tx
                        .execute(
                            "WITH RECURSIVE dt AS ( \
                                 SELECT id, parent FROM anki.decks WHERE id = (SELECT deck FROM anki.notes WHERE id = $1) \
                                 UNION ALL \
                                 SELECT d.id, d.parent FROM anki.decks d INNER JOIN dt ON dt.parent = d.id \
                             ) UPDATE anki.decks SET last_update = NOW() WHERE id IN (SELECT id FROM dt)",
                            &[&note_id],
                        )
                        .await;
                    bump_note_subscribers(&tx, note_id).await;
                }
            } else if new_status == "rejected" {
                let updated_note = tx
                    .query_opt(
                        "UPDATE anki.notes SET version = version + 1 WHERE id = $1 RETURNING version",
                        &[&note_id],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                if let Some(nr) = updated_note {
                    let new_ver: i64 = nr.get(0);
                    let old_val = orig_content
                        .as_ref()
                        .map(|c| serde_json::json!({ "position": pos, "content": c }));
                    let new_val = new_content_val.as_ref().map(|c| {
                        serde_json::json!({
                            "suggestion_id": payload.suggestion_id,
                            "position": pos,
                            "proposed_content": c
                        })
                    });
                    let _ = tx
                        .execute(
                            "INSERT INTO anki.note_events (note_id, version, event_type, commit_id, actor_user_id, approved, old_value, new_value) \
                             VALUES ($1, $2, 'field_change_denied', $3, $4, false, $5, $6)",
                            &[&note_id, &new_ver, &commit_id_val, &auth_user.user_id, &old_val, &new_val],
                        )
                        .await;
                }
                tx.execute(
                    "DELETE FROM anki.fields WHERE id = $1 AND reviewed = false",
                    &[&payload.suggestion_id],
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            }
        }
    } else if payload.suggestion_type == "tag" {
        if let (Some(note_id), Some(ref tag_name)) = (note_id_val, &tag_name_val) {
            if new_status == "approved" {
                if tag_action_val == Some(false) {
                    tx.execute(
                        "DELETE FROM anki.tags WHERE note = $1 AND content = $2",
                        &[&note_id, tag_name],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                } else {
                    let already_exists = tx
                        .query_opt(
                            "SELECT 1 FROM anki.tags WHERE note = $1 AND content = $2 AND reviewed = true AND id != $3 LIMIT 1",
                            &[&note_id, tag_name, &payload.suggestion_id],
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                    if already_exists.is_some() {
                        tx.execute(
                            "DELETE FROM anki.tags WHERE id = $1 AND reviewed = false",
                            &[&payload.suggestion_id],
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                    } else {
                        tx.execute(
                            "UPDATE anki.tags SET reviewed = true WHERE id = $1",
                            &[&payload.suggestion_id],
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                    }
                }

                let updated_note = tx
                    .query_opt(
                        "UPDATE anki.notes SET last_update = NOW(), version = version + 1 WHERE id = $1 RETURNING version",
                        &[&note_id],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                if let Some(nr) = updated_note {
                    let new_ver: i64 = nr.get(0);
                    let (event_type, old_val, new_val) = if tag_action_val == Some(false) {
                        (
                            "tag_removed",
                            Some(serde_json::json!({ "content": tag_name })),
                            None,
                        )
                    } else {
                        (
                            "tag_added",
                            None,
                            Some(serde_json::json!({ "content": tag_name })),
                        )
                    };
                    let _ = tx
                        .execute(
                            "INSERT INTO anki.note_events (note_id, version, event_type, commit_id, actor_user_id, approved, old_value, new_value) \
                             VALUES ($1, $2, $3, $4, $5, true, $6, $7)",
                            &[&note_id, &new_ver, &event_type, &commit_id_val, &auth_user.user_id, &old_val, &new_val],
                        )
                        .await;
                    let _ = tx
                        .execute(
                            "WITH RECURSIVE dt AS ( \
                                 SELECT id, parent FROM anki.decks WHERE id = (SELECT deck FROM anki.notes WHERE id = $1) \
                                 UNION ALL \
                                 SELECT d.id, d.parent FROM anki.decks d INNER JOIN dt ON dt.parent = d.id \
                             ) UPDATE anki.decks SET last_update = NOW() WHERE id IN (SELECT id FROM dt)",
                            &[&note_id],
                        )
                        .await;
                    bump_note_subscribers(&tx, note_id).await;
                }
            } else if new_status == "rejected" {
                let updated_note = tx
                    .query_opt(
                        "UPDATE anki.notes SET version = version + 1 WHERE id = $1 RETURNING version",
                        &[&note_id],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                if let Some(nr) = updated_note {
                    let new_ver: i64 = nr.get(0);
                    let denied_val = serde_json::json!({
                        "suggestion_id": payload.suggestion_id,
                        "content": tag_name,
                        "action": tag_action_val.unwrap_or(true)
                    });
                    let _ = tx
                        .execute(
                            "INSERT INTO anki.note_events (note_id, version, event_type, commit_id, actor_user_id, approved, new_value) \
                             VALUES ($1, $2, 'tag_change_denied', $3, $4, false, $5)",
                            &[&note_id, &new_ver, &commit_id_val, &auth_user.user_id, &denied_val],
                        )
                        .await;
                }
                tx.execute(
                    "DELETE FROM anki.tags WHERE id = $1 AND reviewed = false",
                    &[&payload.suggestion_id],
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            }
        }
    }

    if new_status == "approved" {
        if let Some(note_id) = note_id_val {
            if let Ok(rules) = crate::deck_proposal_rules::get_rules_with_client(&tx, deck_id).await
            {
                let _ = crate::deck_proposal_rules::apply_tagging_rules_on_approval(
                    &tx,
                    note_id,
                    false,
                    &rules,
                    commit_id_val,
                )
                .await;
            }
        }
    }

    let field_pos_i32 = field_pos_val.map(|p| p as i32);
    tx.execute(
        "INSERT INTO anki.suggestion_moderation_status \
         (suggestion_type, suggestion_id, deck_id, status, approve_count, reject_count, resolved_at, \
          note_id, field_position, field_name, original_content, new_content, tag_name, tag_action) \
         VALUES ($1, $2, $3, $4, $5, $6, CASE WHEN $4 != 'voting' THEN NOW() ELSE NULL END, \
                 $7, $8, $9, $10, $11, $12, $13) \
         ON CONFLICT (suggestion_type, suggestion_id) DO UPDATE SET \
            status = EXCLUDED.status, \
            approve_count = EXCLUDED.approve_count, \
            reject_count = EXCLUDED.reject_count, \
            resolved_at = CASE WHEN EXCLUDED.status != 'voting' THEN NOW() ELSE suggestion_moderation_status.resolved_at END, \
            note_id = COALESCE(EXCLUDED.note_id, suggestion_moderation_status.note_id), \
            field_position = COALESCE(EXCLUDED.field_position, suggestion_moderation_status.field_position), \
            field_name = COALESCE(EXCLUDED.field_name, suggestion_moderation_status.field_name), \
            original_content = COALESCE(EXCLUDED.original_content, suggestion_moderation_status.original_content), \
            new_content = COALESCE(EXCLUDED.new_content, suggestion_moderation_status.new_content), \
            tag_name = COALESCE(EXCLUDED.tag_name, suggestion_moderation_status.tag_name), \
            tag_action = COALESCE(EXCLUDED.tag_action, suggestion_moderation_status.tag_action)",
        &[
            &payload.suggestion_type,
            &payload.suggestion_id,
            &deck_id,
            &new_status,
            &approves,
            &rejects,
            &note_id_val,
            &field_pos_i32,
            &field_name_val,
            &orig_content,
            &new_content_val,
            &tag_name_val,
            &tag_action_val,
        ],
    )
    .await
    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    tx.commit()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let remaining_after = (remaining_votes - 1).max(0);

    let msg = match new_status.as_str() {
        "approved" => "Vote recorded. Suggestion has reached consensus and is officially approved.",
        "flagged_for_maintainer" => {
            "Vote recorded. Non-unanimity detected; suggestion placed on hold for maintainer verification."
        }
        "rejected" => "Vote recorded. Suggestion rejected by community majority.",
        _ => "Vote recorded successfully.",
    };

    Ok(Json(VoteResponse {
        success: true,
        status: new_status,
        remaining_weekly_votes: remaining_after,
        approve_count: approves,
        reject_count: rejects,
        message: msg.to_string(),
    }))
}

pub async fn get_audit_log(
    State(app_state): State<Arc<AppState>>,
    Path(deck_hash): Path<String>,
    auth_user: AuthenticatedUser,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !validate_deck_hash(&deck_hash) {
        return Err((StatusCode::BAD_REQUEST, "Invalid deck hash format".to_string()));
    }

    let client = app_state
        .db_pool
        .get_owned()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let deck_id = get_deck_id_by_hash(&client, &deck_hash)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    let is_admin = check_deck_admin(&client, deck_id, auth_user.user_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if !is_admin {
        return Err((
            StatusCode::FORBIDDEN,
            "Access denied: deck administrator rights required".to_string(),
        ));
    }

    let rows = client
        .query(
            "SELECT s.suggestion_type, s.suggestion_id, s.deck_id, s.status, s.approve_count, s.reject_count, \
                    s.note_id, n.guid, s.field_position, s.field_name, s.original_content, s.new_content, \
                    s.tag_name, s.tag_action, \
                    to_char(s.resolved_at, 'YYYY-MM-DD HH24:MI:SS'), \
                    to_char(s.reverted_at, 'YYYY-MM-DD HH24:MI:SS'), \
                    u.username, s.override_reason \
             FROM anki.suggestion_moderation_status s \
             LEFT JOIN anki.notes n ON s.note_id = n.id \
             LEFT JOIN public.users u ON s.reverted_by = u.id \
             WHERE s.deck_id = $1 \
             ORDER BY COALESCE(s.reverted_at, s.resolved_at, NOW()) DESC \
             LIMIT 150",
            &[&deck_id],
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let mut field_ids = Vec::new();
    let mut tag_ids = Vec::new();
    for row in &rows {
        let st: String = row.get(0);
        let sid: i64 = row.get(1);
        if st == "field" {
            field_ids.push(sid);
        } else {
            tag_ids.push(sid);
        }
    }

    let mut votes_map: std::collections::HashMap<(String, i64), Vec<AuditVoteDto>> =
        std::collections::HashMap::new();

    if !field_ids.is_empty() || !tag_ids.is_empty() {
        let all_votes = client
            .query(
                "SELECT v.suggestion_type, v.suggestion_id, v.user_id, u.username, v.vote, v.weight::float8, v.early_adopted, \
                        to_char(v.voted_at, 'YYYY-MM-DD HH24:MI:SS') \
                 FROM anki.deck_moderator_votes v \
                 JOIN public.users u ON v.user_id = u.id \
                 WHERE (v.suggestion_type = 'field' AND v.suggestion_id = ANY($1)) \
                    OR (v.suggestion_type = 'tag' AND v.suggestion_id = ANY($2)) \
                 ORDER BY v.voted_at ASC",
                &[&field_ids, &tag_ids],
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

        for vr in all_votes {
            let st: String = vr.get(0);
            let sid: i64 = vr.get(1);
            let dto = AuditVoteDto {
                user_id: vr.get(2),
                username: vr.get(3),
                vote: vr.get(4),
                weight: vr.get(5),
                early_adopted: vr.get(6),
                voted_at: vr.get(7),
            };
            votes_map.entry((st, sid)).or_default().push(dto);
        }
    }

    let mut entries = Vec::new();
    for row in rows {
        let suggestion_type: String = row.get(0);
        let suggestion_id: i64 = row.get(1);
        let votes = votes_map
            .remove(&(suggestion_type.clone(), suggestion_id))
            .unwrap_or_default();

        let field_pos_i32: Option<i32> = row.get(8);
        entries.push(AuditEntryDto {
            suggestion_type,
            suggestion_id,
            deck_id: row.get(2),
            status: row.get(3),
            approve_count: row.get(4),
            reject_count: row.get(5),
            note_id: row.get(6),
            note_guid: row.get(7),
            field_position: field_pos_i32.map(|p| p as u32),
            field_name: row.get(9),
            original_content: row.get(10),
            new_content: row.get(11),
            tag_name: row.get(12),
            tag_action: row.get(13),
            resolved_at: row.get(14),
            reverted_at: row.get(15),
            reverted_by_username: row.get(16),
            override_reason: row.get(17),
            votes,
        });
    }

    Ok(Json(entries))
}

pub async fn post_moderation_override(
    State(app_state): State<Arc<AppState>>,
    auth_user: AuthenticatedUser,
    Json(payload): Json<OverrideRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !validate_deck_hash(&payload.deck_hash) {
        return Err((StatusCode::BAD_REQUEST, "Invalid deck hash format".to_string()));
    }

    let mut client = app_state
        .db_pool
        .get_owned()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let deck_id = get_deck_id_by_hash(&client, &payload.deck_hash)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    let is_admin = check_deck_admin(&client, deck_id, auth_user.user_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if !is_admin {
        return Err((
            StatusCode::FORBIDDEN,
            "Only deck owner or maintainers can override moderation decisions".to_string(),
        ));
    }

    let tx = client
        .transaction()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let status_row = tx
        .query_opt(
            "SELECT status, note_id, field_position, original_content, new_content, tag_name, tag_action \
             FROM anki.suggestion_moderation_status \
             WHERE suggestion_type = $1 AND suggestion_id = $2 AND deck_id = $3",
            &[&payload.suggestion_type, &payload.suggestion_id, &deck_id],
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let (
        current_status,
        note_id_opt,
        field_pos_opt,
        original_content,
        _new_content,
        tag_name_opt,
        tag_action_opt,
    ) = match status_row {
        Some(r) => (
            r.get::<_, String>(0),
            r.get::<_, Option<i64>>(1),
            r.get::<_, Option<i32>>(2),
            r.get::<_, Option<String>>(3),
            r.get::<_, Option<String>>(4),
            r.get::<_, Option<String>>(5),
            r.get::<_, Option<bool>>(6),
        ),
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                "Suggestion moderation entry not found".to_string(),
            ))
        }
    };

    let new_status: String;
    let message: String;

    match payload.action.as_str() {
        "revert" => {
            if current_status != "approved" && current_status != "approved_by_admin" {
                return Err((
                    StatusCode::BAD_REQUEST,
                    format!(
                        "Cannot revert suggestion in status '{current_status}'. Only approved suggestions can be reverted."
                    ),
                ));
            }

            if payload.suggestion_type == "field" {
                if let (Some(note_id), Some(pos)) = (note_id_opt, field_pos_opt) {
                    let pos_u32 = pos as u32;
                    tx.execute(
                        "DELETE FROM anki.fields WHERE id = $1 OR (note = $2 AND position = $3 AND reviewed = true)",
                        &[&payload.suggestion_id, &note_id, &pos_u32],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                    if let Some(ref orig) = original_content {
                        tx.execute(
                            "INSERT INTO anki.fields (note, position, content, reviewed, creator_ip) \
                             VALUES ($1, $2, $3, true, 'retro_revert')",
                            &[&note_id, &pos_u32, orig],
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                    }

                    tx.execute(
                        "UPDATE anki.notes SET last_update = NOW() WHERE id = $1",
                        &[&note_id],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                }
            } else if payload.suggestion_type == "tag" {
                if let (Some(note_id), Some(tag_name)) = (note_id_opt, tag_name_opt) {
                    if tag_action_opt == Some(true) {
                        tx.execute(
                            "DELETE FROM anki.tags WHERE note = $1 AND content = $2",
                            &[&note_id, &tag_name],
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                    } else if tag_action_opt == Some(false) {
                        tx.execute(
                            "INSERT INTO anki.tags (note, content, reviewed, action, creator_ip) \
                             SELECT $1, $2, true, true, 'retro_revert' \
                             WHERE NOT EXISTS ( \
                                 SELECT 1 FROM anki.tags WHERE note = $1 AND content = $2 AND reviewed = true \
                             )",
                            &[&note_id, &tag_name],
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                    }

                    tx.execute(
                        "UPDATE anki.notes SET last_update = NOW() WHERE id = $1",
                        &[&note_id],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                }
            }

            new_status = "reverted_by_admin".to_string();
            message = "Suggestion adoption retroactively reverted. Note state has been restored."
                .to_string();

            tx.execute(
                "UPDATE anki.suggestion_moderation_status \
                 SET status = $1, reverted_at = NOW(), reverted_by = $2, override_reason = $3 \
                 WHERE suggestion_type = $4 AND suggestion_id = $5",
                &[
                    &new_status,
                    &auth_user.user_id,
                    &payload.reason,
                    &payload.suggestion_type,
                    &payload.suggestion_id,
                ],
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
        "confirm_approval" => {
            if payload.suggestion_type == "field" {
                if let (Some(note_id), Some(pos)) = (note_id_opt, field_pos_opt) {
                    let pos_u32 = pos as u32;
                    if pos == 0 {
                        let field_row = tx
                            .query_opt("SELECT content FROM anki.fields WHERE id = $1", &[&payload.suggestion_id])
                            .await
                            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                        let content = field_row.and_then(|r| r.get::<_, Option<String>>(0)).unwrap_or_default();
                        if content.trim().is_empty() {
                            return Err((
                                StatusCode::BAD_REQUEST,
                                "Cannot approve empty content for field 0 (sort field / primary field).".to_string(),
                            ));
                        }
                    }
                    tx.execute(
                        "DELETE FROM anki.fields WHERE note = $1 AND position = $2 AND reviewed = true AND id != $3",
                        &[&note_id, &pos_u32, &payload.suggestion_id],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                    tx.execute(
                        "UPDATE anki.fields SET reviewed = true WHERE id = $1",
                        &[&payload.suggestion_id],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

                    tx.execute(
                        "UPDATE anki.notes SET last_update = NOW() WHERE id = $1",
                        &[&note_id],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                }
            } else if payload.suggestion_type == "tag" {
                if let (Some(note_id), Some(tag_name)) = (note_id_opt, tag_name_opt) {
                    if tag_action_opt == Some(false) {
                        tx.execute(
                            "DELETE FROM anki.tags WHERE note = $1 AND content = $2",
                            &[&note_id, &tag_name],
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                    } else {
                        tx.execute(
                            "UPDATE anki.tags SET reviewed = true WHERE id = $1",
                            &[&payload.suggestion_id],
                        )
                        .await
                        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                    }

                    tx.execute(
                        "UPDATE anki.notes SET last_update = NOW() WHERE id = $1",
                        &[&note_id],
                    )
                    .await
                    .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
                }
            }

            new_status = "approved_by_admin".to_string();
            message = "Suggestion manually confirmed and approved by administrator.".to_string();

            tx.execute(
                "UPDATE anki.suggestion_moderation_status \
                 SET status = $1, resolved_at = NOW(), override_reason = $2 \
                 WHERE suggestion_type = $3 AND suggestion_id = $4",
                &[
                    &new_status,
                    &payload.reason,
                    &payload.suggestion_type,
                    &payload.suggestion_id,
                ],
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
        "admin_reject" => {
            if payload.suggestion_type == "field" {
                tx.execute(
                    "DELETE FROM anki.fields WHERE id = $1 AND reviewed = false",
                    &[&payload.suggestion_id],
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            } else if payload.suggestion_type == "tag" {
                tx.execute(
                    "DELETE FROM anki.tags WHERE id = $1 AND reviewed = false",
                    &[&payload.suggestion_id],
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
            }

            new_status = "rejected_by_admin".to_string();
            message = "Suggestion rejected by administrator.".to_string();

            tx.execute(
                "UPDATE anki.suggestion_moderation_status \
                 SET status = $1, resolved_at = NOW(), override_reason = $2 \
                 WHERE suggestion_type = $3 AND suggestion_id = $4",
                &[
                    &new_status,
                    &payload.reason,
                    &payload.suggestion_type,
                    &payload.suggestion_id,
                ],
            )
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
        }
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                "Invalid action. Supported actions: revert, confirm_approval, admin_reject"
                    .to_string(),
            ))
        }
    }

    if payload.action == "revert" || payload.action == "confirm_approval" {
        if let Some(note_id) = note_id_opt {
            let updated_note = tx
                .query_opt(
                    "UPDATE anki.notes SET last_update = NOW(), version = version + 1 WHERE id = $1 RETURNING version",
                    &[&note_id],
                )
                .await
                .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

            if let Some(nr) = updated_note {
                let new_ver: i64 = nr.get(0);
                let event_type = if payload.action == "revert" {
                    "maintainer_revert"
                } else {
                    "maintainer_confirm_approval"
                };
                let meta = serde_json::json!({
                    "suggestion_type": payload.suggestion_type,
                    "suggestion_id": payload.suggestion_id,
                    "reason": payload.reason
                });
                let _ = tx
                    .execute(
                        "INSERT INTO anki.note_events (note_id, version, event_type, actor_user_id, approved, new_value) \
                         VALUES ($1, $2, $3, $4, true, $5)",
                        &[&note_id, &new_ver, &event_type, &auth_user.user_id, &meta],
                    )
                    .await;
            }

            let _ = tx
                .execute(
                    "WITH RECURSIVE dt AS ( \
                         SELECT id, parent FROM anki.decks WHERE id = (SELECT deck FROM anki.notes WHERE id = $1) \
                         UNION ALL \
                         SELECT d.id, d.parent FROM anki.decks d INNER JOIN dt ON dt.parent = d.id \
                     ) UPDATE anki.decks SET last_update = NOW() WHERE id IN (SELECT id FROM dt)",
                    &[&note_id],
                )
                .await;

            bump_note_subscribers(&tx, note_id).await;

            if payload.action == "confirm_approval" {
                if let Ok(rules) =
                    crate::deck_proposal_rules::get_rules_with_client(&tx, deck_id).await
                {
                    let _ = crate::deck_proposal_rules::apply_tagging_rules_on_approval(
                        &tx, note_id, false, &rules, None,
                    )
                    .await;
                }
            }
        }
    }

    tx.commit()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok(Json(OverrideResponse {
        success: true,
        message,
        new_status,
    }))
}

pub async fn post_roster_action(
    State(app_state): State<Arc<AppState>>,
    auth_user: AuthenticatedUser,
    Json(payload): Json<RosterActionRequest>,
) -> Result<impl IntoResponse, (StatusCode, String)> {
    if !validate_deck_hash(&payload.deck_hash) {
        return Err((StatusCode::BAD_REQUEST, "Invalid deck hash format".to_string()));
    }

    let client = app_state
        .db_pool
        .get_owned()
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let deck_id = get_deck_id_by_hash(&client, &payload.deck_hash)
        .await
        .map_err(|e| (StatusCode::NOT_FOUND, e.to_string()))?;

    let is_admin = check_deck_admin(&client, deck_id, auth_user.user_id)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    if !is_admin {
        return Err((
            StatusCode::FORBIDDEN,
            "Access denied: deck administrator rights required".to_string(),
        ));
    }

    let user_row = client
        .query_opt(
            "SELECT id FROM public.users WHERE LOWER(username) = LOWER($1)",
            &[&payload.username],
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let target_user_id: i32 = match user_row {
        Some(u) => u.get(0),
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                format!("User '{}' not found", payload.username),
            ))
        }
    };

    let valid_status = match payload.status.as_str() {
        "whitelist" => "whitelist",
        "blacklist" => "blacklist",
        "active" => "active",
        "auto_qualified" => "auto_qualified",
        _ => {
            return Err((
                StatusCode::BAD_REQUEST,
                "Invalid roster status (must be whitelist, blacklist, active, or auto_qualified)"
                    .to_string(),
            ));
        }
    };

    client
        .execute(
            "INSERT INTO anki.deck_moderator_roster \
             (deck_id, user_id, status, allowed_tags, reason, updated_at) \
             VALUES ($1, $2, $3, $4, $5, NOW()) \
             ON CONFLICT (deck_id, user_id) DO UPDATE SET \
                status = EXCLUDED.status, \
                allowed_tags = EXCLUDED.allowed_tags, \
                reason = EXCLUDED.reason, \
                updated_at = NOW()",
            &[
                &deck_id,
                &target_user_id,
                &valid_status,
                &payload.allowed_tags,
                &payload.reason,
            ],
        )
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    let msg = format!(
        "User '{}' successfully added to {}",
        payload.username, payload.status
    );
    Ok(Json(RosterActionResponse {
        success: true,
        message: msg,
    }))
}
