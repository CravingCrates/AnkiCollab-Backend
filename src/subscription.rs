use std::sync::Arc;

use crate::database;

pub async fn add(
    db_state: &Arc<database::AppState>,
    deck_hash: String,
    user_hash: String,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            sentry::capture_message(
                &format!("subscription::add: Failed to get pool: {}", err),
                sentry::Level::Error,
            );
            return Err("Failed to retrieve a pooled connection".into());
        }
    };

    let deck_id = client
        .query("SELECT id from decks where human_hash = $1", &[&deck_hash])
        .await?;
    if deck_id.is_empty() {
        return Err("Deck not found".into());
    }
    let deck_id: i64 = deck_id[0].get(0);
    let result = client
        .query(
            "INSERT INTO subscriptions (deck_id, user_hash) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            &[&deck_id, &user_hash],
        )
        .await;
    if let Err(err) = result {
        sentry::capture_message(
            &format!("subscription::add: Failed to insert: {}", err),
            sentry::Level::Error,
        );
        return Err("Cannot add the sub".into());
    }
    Ok(())
}

pub async fn remove(
    db_state: &Arc<database::AppState>,
    deck_hash: String,
    user_hash: String,
) -> std::result::Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            sentry::capture_message(
                &format!("subscription::remove: Failed to get pool: {}", err),
                sentry::Level::Error,
            );
            return Err("Failed to retrieve a pooled connection".into());
        }
    };

    let deck_id = client
        .query("SELECT id from decks where human_hash = $1", &[&deck_hash])
        .await?;
    if deck_id.is_empty() {
        return Err("Deck not found".into());
    }
    let deck_id: i64 = deck_id[0].get(0);
    let result = client
        .query(
            "DELETE FROM subscriptions WHERE deck_id = $1 AND user_hash = $2",
            &[&deck_id, &user_hash],
        )
        .await;
    if let Err(err) = result {
        sentry::capture_message(
            &format!("subscription::remove: Failed to delete: {}", err),
            sentry::Level::Error,
        );
        return Err("Cannot remove the sub".into());
    }
    Ok(())
}
