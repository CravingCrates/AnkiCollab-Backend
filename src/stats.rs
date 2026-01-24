use crate::{database, structs::StatsInfo};

use std::sync::Arc;

pub async fn new(
    db_state: &Arc<database::AppState>,
    info: StatsInfo,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            sentry::capture_message(
                &format!("stats::new: Failed to get pool: {}", err),
                sentry::Level::Error,
            );
            return Err("Failed to retrieve a pooled connection".into());
        }
    };

    // Check if the deck exists
    let deck_id_rows = client
        .query(
            "SELECT id FROM decks WHERE human_hash = $1",
            &[&info.deck_hash],
        )
        .await?;
    if deck_id_rows.len() != 1 {
        // Expected case - user provided invalid deck hash, not a bug
        return Err(format!(
            "Expected one deck with hash {}, found {}",
            info.deck_hash,
            deck_id_rows.len()
        )
        .into());
    }
    let deck_id: i64 = deck_id_rows[0].get(0);

    let tx = client.transaction().await?;

    let deck_ids: Vec<i64> = tx
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

    if deck_ids.is_empty() {
        sentry::capture_message(
            &format!("stats::new: No decks found for deck_id {} (should not happen)", deck_id),
            sentry::Level::Error,
        );
        return Err(format!("No decks found for deck_id {}", deck_id).into());
    }

    // Collect all GUIDs and build a batch lookup
    let mut guid_to_stats = Vec::new();
    for notes in info.review_history.values() {
        for (guid, note_stats) in notes {
            guid_to_stats.push((guid.clone(), note_stats));
        }
    }

    if guid_to_stats.is_empty() {
        tx.commit().await?;
        return Ok(());
    }

    // Batch lookup all note IDs in one query
    let all_guids: Vec<String> = guid_to_stats.iter().map(|(g, _)| g.clone()).collect();
    let note_rows = tx
        .query(
            "SELECT id, guid FROM notes WHERE guid = ANY($1) AND deleted = false AND deck = ANY($2)",
            &[&all_guids, &deck_ids],
        )
        .await?;

    // Build a map of guid -> note_id
    let mut guid_to_note_id = std::collections::HashMap::new();
    for row in note_rows {
        let note_id: i64 = row.get(0);
        let guid: String = row.get(1);
        guid_to_note_id.insert(guid, note_id);
    }

    // Prepare data for batch operations, sorted by (note_id, user_hash) for consistent lock ordering
    let mut batch_data: Vec<(i64, i32, i32, i32)> = Vec::new();
    for (guid, note_stats) in guid_to_stats {
        if let Some(&note_id) = guid_to_note_id.get(&guid) {
            batch_data.push((
                note_id,
                note_stats.retention,
                note_stats.lapses,
                note_stats.reps,
            ));
        }
    }

    if batch_data.is_empty() {
        tx.commit().await?;
        return Ok(());
    }

    // Sort by note_id to ensure consistent lock ordering and avoid deadlocks
    batch_data.sort_by_key(|k| k.0);
    
    // Deduplicate by note_id (keep last occurrence if duplicate GUIDs map to same note)
    batch_data.dedup_by_key(|k| k.0);

    let note_ids: Vec<i64> = batch_data.iter().map(|(id, _, _, _)| *id).collect();
    let retentions: Vec<i32> = batch_data.iter().map(|(_, r, _, _)| *r).collect();
    let lapses: Vec<i32> = batch_data.iter().map(|(_, _, l, _)| *l).collect();
    let reps: Vec<i32> = batch_data.iter().map(|(_, _, _, r)| *r).collect();

    // Step 1: UPDATE existing rows
    // Lock rows in a consistent order to prevent deadlocks
    let update_count = tx.execute(
        "UPDATE note_stats ns
         SET retention = tu.retention,
             lapses = tu.lapses,
             reps = tu.reps
         FROM (
             SELECT * FROM UNNEST($1::bigint[], $2::int[], $3::int[], $4::int[])
             AS t(note_id, retention, lapses, reps)
         ) tu
         WHERE ns.note_id = tu.note_id AND ns.user_hash = $5::varchar",
        &[&note_ids, &retentions, &lapses, &reps, &info.user_hash],
    )
    .await?;

    // Step 2: INSERT new rows (only those that weren't updated)
    // If all rows were updated, skip the insert
    if (update_count as usize) < note_ids.len() {
        tx.execute(
            "INSERT INTO note_stats (note_id, user_hash, retention, lapses, reps)
             SELECT tu.note_id, $5::varchar, tu.retention, tu.lapses, tu.reps
             FROM (
                 SELECT * FROM UNNEST($1::bigint[], $2::int[], $3::int[], $4::int[])
                 AS t(note_id, retention, lapses, reps)
             ) tu
             WHERE NOT EXISTS (
                 SELECT 1 FROM note_stats ns 
                 WHERE ns.note_id = tu.note_id AND ns.user_hash = $5::varchar
             )",
            &[&note_ids, &retentions, &lapses, &reps, &info.user_hash],
        )
        .await?;
    }

    tx.commit().await?;

    Ok(())
}
