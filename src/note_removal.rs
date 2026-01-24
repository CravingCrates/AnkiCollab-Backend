use std::sync::Arc;

use crate::cleanser;
use crate::{database, suggestion};

pub async fn new(
    db_state: &Arc<database::AppState>,
    guids: Vec<String>,
    commit_text: String,
    deck_hash: String,
    ip: String,
    _force_overwrite: bool,
    user_id: Option<i32>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let strg = cleanser::clean(&commit_text);
    let commit_id = suggestion::create_new_commit(db_state, 11, &strg, &ip, user_id).await?; // 11 = Card Deletion

    let mut client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Failed to retrieve a pooled connection".into());
        }
    };

    let deck_id_q = client
        .query("SELECT id FROM decks WHERE human_hash = $1", &[&deck_hash])
        .await?;

    if deck_id_q.is_empty() {
        println!("Deck doesnt exist");
        return Err("Deck doesnt exist".to_string().into());
    }

    let deck_id: i64 = deck_id_q[0].get(0);

    client
        .query(
            "UPDATE commits SET deck = $1 WHERE commit_id = $2 AND deck is NULL",
            &[&deck_id, &commit_id],
        )
        .await?;

    let subdecks: Vec<i64> = client
        .query(
            "
                WITH RECURSIVE input_deck_cte AS (
                    SELECT id, name, parent
                    FROM decks
                    WHERE id = $1
                    UNION ALL
                    SELECT d.id, d.name, d.parent
                    FROM decks d
                    JOIN input_deck_cte s ON d.parent = s.id
                )
                SELECT id FROM input_deck_cte
            ",
            &[&deck_id],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();

    let tx = client.transaction().await?;

    let note_id_req = tx
        .prepare("SELECT id FROM notes WHERE guid = $1 AND deck = ANY($2) LIMIT 1")
        .await?;
    let removal_req = tx
        .prepare("INSERT INTO card_deletion_suggestions (note, creator_ip, commit) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING")
        .await?;

    for guid in guids {
        let rows = tx.query(&note_id_req, &[&guid, &subdecks]).await?;

        if rows.is_empty() {
            continue;
        }

        let note_id: Option<i64> = rows.first().map(|row| row.get(0));

        if note_id.is_none() {
            continue;
        }

        let result = tx.execute(&removal_req, &[&note_id, &ip, &commit_id]).await;

        if let Err(err) = result {
            tx.rollback().await?;
            println!("Error executing removal request: {err}");
            return Err("Cannot make the removal request".into());
        }
    }

    // Commit the transaction if all removal requests were successful
    tx.commit().await?;

    // TODO Add force_overwrite logic by making the request to the website microservice with token
    Ok(())
}
