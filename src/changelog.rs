use std::sync::Arc;

use crate::{cleanser, database};

pub async fn insert_new_changelog(
    db_state: &Arc<database::AppState>,
    deck_hash: &String,
    message: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Failed to retrieve a pooled connection".into());
        }
    };

    let query = r"
        INSERT INTO changelogs (deck, message, timestamp)
        VALUES ((SELECT id FROM decks WHERE human_hash = $1), $2, NOW())
    ";

    let msg = cleanser::clean(message);

    client.execute(query, &[&deck_hash, &msg]).await?;
    Ok(())
}
