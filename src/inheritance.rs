use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::auth;
use crate::database;
use crate::structs::{CreateDeckLinkRequest, CreateNewNoteLinkRequest};

type SharedConn = database::SharedConn;

async fn get_deck_id(
    client: &SharedConn,
    deck_hash: &str,
) -> Result<i64, Box<dyn std::error::Error + Send + Sync>> {
    let row = client
        .query_opt("SELECT id FROM decks WHERE human_hash = $1", &[&deck_hash])
        .await?;
    match row {
        Some(r) => Ok(r.get(0)),
        None => Err(format!("Deck hash {} not found", deck_hash).into()),
    }
}

pub async fn create_deck_link(
    db_state: &Arc<database::AppState>,
    req: CreateDeckLinkRequest,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let user_id = auth::get_user_from_token(db_state, &req.token)
        .await
        .unwrap_or_default();
    if user_id == 0 {
        return Err("Unauthorized".into());
    }
    if !auth::is_valid_user_token(&db_state, &req.token, &req.subscriber_deck_hash).await? {
        return Err("Forbidden".into());
    }

    if req.subscriber_deck_hash == req.base_deck_hash {
        return Err("Cannot subscribe a deck to itself. This feature is still in development. Please don't use it without instructions.".into());
    }

    let mut client = db_state.db_pool.get_owned().await?;

    // Validate both deck hashes exist before proceeding
    let subscriber_deck_id = get_deck_id(&client, &req.subscriber_deck_hash)
        .await
        .map_err(|_e| format!("Subscriber deck '{}' not found", req.subscriber_deck_hash))?;
    let base_deck_id = get_deck_id(&client, &req.base_deck_hash)
        .await
        .map_err(|_e| format!("Base deck '{}' not found", req.base_deck_hash))?;

    // Compute subscriber subtree before starting the transaction to avoid borrow conflicts
    let subscriber_tree = get_descendant_deck_ids(&client, subscriber_deck_id).await?;

    // Early bail if subscription already exists
    let sub_check_row = client
        .query_opt(
            "SELECT 1 FROM deck_subscriptions WHERE subscriber_deck_id = $1 AND base_deck_id = $2",
            &[&subscriber_deck_id, &base_deck_id],
        )
        .await?;

    if sub_check_row.is_some() {
        return Ok(());
    }

    // Check for circular deck subscription: if base_deck is already subscribing to subscriber_deck (directly or transitively), reject
    let circular_check = client
        .query_opt(
            "WITH RECURSIVE sub_chain AS (
                SELECT subscriber_deck_id, base_deck_id FROM deck_subscriptions WHERE subscriber_deck_id = $1
                UNION
                SELECT ds.subscriber_deck_id, ds.base_deck_id 
                FROM deck_subscriptions ds 
                JOIN sub_chain sc ON ds.subscriber_deck_id = sc.base_deck_id
            )
            SELECT 1 FROM sub_chain WHERE base_deck_id = $2",
            &[&base_deck_id, &subscriber_deck_id],
        )
        .await?;

    if circular_check.is_some() {
        return Err("Circular subscription detected: the base deck (or one of its subscribed decks) already subscribes to the subscriber deck. This would create a circular dependency.".into());
    }

    let tx = client.transaction().await?;
    tx.execute(
        "INSERT INTO anki.deck_subscriptions (subscriber_deck_id, base_deck_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        &[&subscriber_deck_id, &base_deck_id],
    ).await?;

    // Pre-populate default subscribe-all policies for all notetypes in the subscriber deck subtree.
    let nt_rows = if subscriber_tree.is_empty() {
        Vec::new()
    } else {
        tx.query(
            "SELECT DISTINCT nt.id
             FROM notes n JOIN notetype nt ON n.notetype = nt.id
             WHERE n.deck = ANY($1) AND n.deleted = false",
            &[&subscriber_tree],
        )
        .await?
    };
    if !nt_rows.is_empty() {
        let nt_ids: Vec<i64> = nt_rows.into_iter().map(|r| r.get::<_, i64>(0)).collect();
        // For each notetype determine if it has protected fields. If yes, default subscribed_fields = all unprotected positions; else NULL (subscribe all)
        let field_meta_rows = tx.query(
            "SELECT nf.notetype,
                    COUNT(*) AS total_fields,
                    COUNT(*) FILTER (WHERE nf.protected) AS protected_count,
                    array_agg(nf.position::int ORDER BY nf.position) FILTER (WHERE nf.protected = false) AS unprotected_positions
             FROM notetype_field nf
             WHERE nf.notetype = ANY($1)
             GROUP BY nf.notetype",
            &[&nt_ids]
        ).await?;

        let insert_stmt = tx.prepare(
            "INSERT INTO anki.subscription_field_policy (subscriber_deck_id, base_deck_id, notetype_id, subscribed_fields)
             VALUES ($1, $2, $3, $4) ON CONFLICT (subscriber_deck_id, base_deck_id, notetype_id) DO NOTHING"
        ).await?;

        for row in field_meta_rows {
            let nt_id: i64 = row.get(0);
            let protected_count: i64 = row.get(2);
            let unprotected: Option<Vec<i32>> = row.get(3);
            // Decide subscribed_fields value
            let subscribed_fields: Option<Vec<i32>> = if protected_count > 0 {
                // Some fields protected: subscribe only to unprotected ones (could be empty)
                unprotected
            } else {
                // No protected fields: NULL means subscribe-all
                None
            };
            tx.execute(
                &insert_stmt,
                &[
                    &subscriber_deck_id,
                    &base_deck_id,
                    &nt_id,
                    &subscribed_fields,
                ],
            )
            .await?;
        }
    }

    tx.commit().await?;
    Ok(())
}

fn chunk_vec<T: Clone>(v: &[T], size: usize) -> Vec<Vec<T>> {
    // fixed-bounds helper
    let mut res = Vec::new();
    let mut i = 0;
    while i < v.len() {
        let end = (i + size).min(v.len());
        res.push(v[i..end].to_vec());
        i = end;
    }
    res
}

async fn get_descendant_deck_ids(
    client: &SharedConn,
    root_id: i64,
) -> Result<Vec<i64>, Box<dyn std::error::Error + Send + Sync>> {
    let rows = client
        .query(
            "WITH RECURSIVE cte AS (
            SELECT $1::bigint as id
            UNION ALL
            SELECT d.id FROM decks d JOIN cte ON d.parent = cte.id
        ) SELECT id FROM cte",
            &[&root_id],
        )
        .await?;
    Ok(rows.into_iter().map(|r| r.get(0)).collect())
}

pub async fn create_note_links(
    db_state: &Arc<database::AppState>,
    req: CreateNewNoteLinkRequest,
) -> Result<(usize, Vec<String>), Box<dyn std::error::Error + Send + Sync>> {
    let user_id = auth::get_user_from_token(db_state, &req.token)
        .await
        .unwrap_or_default();
    if user_id == 0 {
        return Err("Unauthorized. If you're a maintainer please log-out and log-in again.".into());
    }
    if !auth::is_valid_user_token(&db_state, &req.token, &req.subscriber_deck_hash).await? {
        return Err("Forbidden".into());
    }
    let mut client = db_state.db_pool.get_owned().await?;
    let subscriber_deck_id = get_deck_id(&client, &req.subscriber_deck_hash).await?;
    let base_deck_id = get_deck_id(&client, &req.base_deck_hash).await?;

    let sub_tree = get_descendant_deck_ids(&client, subscriber_deck_id).await?;
    let base_tree = get_descendant_deck_ids(&client, base_deck_id).await?;

    // Check if there is a subscription in the table deck_subscriptions
    let sub_check_row = client
        .query_opt(
            "SELECT 1 FROM deck_subscriptions WHERE subscriber_deck_id = $1 AND base_deck_id = $2",
            &[&subscriber_deck_id, &base_deck_id],
        )
        .await?;
    if sub_check_row.is_none() {
        return Err("No subscription found between the specified decks. Please create a deck subscription first.".into());
    }

    // Ensure the note guids exist in the subscriber deck
    if !sub_tree.is_empty() {
        // Check if guids exist in the subscriber deck
        let sub_guid_rows = client.query(
            "SELECT n.guid
             FROM notes n
             WHERE n.deck = ANY($1) AND n.guid = ANY($2) AND n.deleted = false and n.reviewed = true",
            &[&sub_tree, &req.note_guids],
        ).await?;

        if sub_guid_rows.is_empty() || sub_guid_rows.len() != req.note_guids.len() {
            println!(
                "Found {} matching guids in subscriber deck, expected {}",
                sub_guid_rows.len(),
                req.note_guids.len()
            );
            println!("Requested GUIDs: {:?}", req.note_guids);
            println!("Subscriber deck subtree: {:?}", sub_tree);
            // Print missing guids for debugging
            let found_guids: HashSet<String> = sub_guid_rows
                .into_iter()
                .map(|r| r.get::<_, String>(0))
                .collect();
            let missing_guids: Vec<String> = req
                .note_guids
                .iter()
                .filter(|g| !found_guids.contains(*g))
                .cloned()
                .collect();
            println!("GUIDs not found: {:?}", missing_guids);
            
            return Err("Please upload the notes before linking them.".into());
        }
    }

    let mut linked = 0usize;
    let mut skipped: Vec<String> = Vec::new();
    let chunks = chunk_vec(&req.note_guids, 1000);

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

    let mut notes_to_update = Vec::with_capacity(req.note_guids.len());

    for guids in chunks {
        // Fetch both sides for these guids in one shot each
        let sub_rows = client.query(
            "SELECT n.id, n.guid, nt.id as nt_id, nt.guid as nt_guid
             FROM notes n JOIN notetype nt ON n.notetype = nt.id
             WHERE n.deck = ANY($1) AND n.guid = ANY($2) AND n.deleted = false AND n.reviewed = true",
            &[&sub_tree, &guids],
        ).await?;
        if sub_rows.is_empty() {
            println!("No subscriber notes found for guids chunk, skipping all");
            continue;
        }
        // map guid -> (note_id, sub_nt_id, sub_nt_guid)
        let mut sub_map: HashMap<String, (i64, i64, String)> = HashMap::new();
        for r in &sub_rows {
            sub_map.insert(r.get(1), (r.get(0), r.get(2), r.get(3)));
        }

        let base_rows = client.query(
            "SELECT n.id, n.guid, nt.id as nt_id
             FROM notes n JOIN notetype nt ON n.notetype = nt.id
             WHERE n.deck = ANY($1) AND n.guid = ANY($2) AND n.deleted = false AND n.reviewed = true",
            &[&base_tree, &guids],
        ).await?;
        if base_rows.is_empty() {
            println!("No base notes found for guids chunk, skipping all");
            println!("Guids: {:?}", guids);
            println!("Base deck subtree: {:?}", base_tree);
            println!("Subscriber deck subtree: {:?}", sub_tree);
            skipped.extend(guids.clone());
            continue;
        }

        // policy: defaults to subscribe-all (NULL). Look up overrides for involved subscriber notetype IDs once per chunk
        let nt_ids_vec: Vec<i64> = sub_rows.iter().map(|r| r.get::<_, i64>(2)).collect();
        let pol_rows = if nt_ids_vec.is_empty() {
            Vec::new()
        } else {
            client.query(
            "SELECT notetype_id, subscribed_fields FROM subscription_field_policy WHERE subscriber_deck_id = $1 AND base_deck_id = $2 AND notetype_id = ANY($3)",
            &[&subscriber_deck_id, &base_deck_id, &nt_ids_vec],
                ).await?
        };
        let mut policy: HashMap<i64, Option<Vec<i32>>> = HashMap::new();
        for r in pol_rows {
            policy.insert(r.get(0), r.get::<_, Option<Vec<i32>>>(1));
        }

        // make sure all notetypes exist in the policy
        let unique_nt_ids = nt_ids_vec.into_iter().collect::<HashSet<_>>();
        if policy.len() != unique_nt_ids.len() {
            return Err("Please check the notetypes on the Website and ensure they have the correct subscription fields set.".into());
        }

        // Build a cached map of notetype_id -> ordered field names (by ord) for all involved notetypes in this chunk
        let mut nt_id_set: HashSet<i64> = HashSet::new();
        for r in &sub_rows {
            nt_id_set.insert(r.get::<_, i64>(2));
        }
        for r in &base_rows {
            nt_id_set.insert(r.get::<_, i64>(2));
        }
        let nt_ids: Vec<i64> = nt_id_set.into_iter().collect();
        let field_rows = if nt_ids.is_empty() {
            Vec::new()
        } else {
            client.query(
                "SELECT notetype, position, name FROM notetype_field WHERE notetype = ANY($1) ORDER BY notetype, position",
                &[&nt_ids],
            ).await?
        };
        let mut fields_by_nt: HashMap<i64, Vec<String>> = HashMap::new();
        for row in field_rows {
            let nt_id: i64 = row.get(0);
            let name: String = row.get(2);
            fields_by_nt.entry(nt_id).or_default().push(name);
        }

        // Compatibility cache: (sub_nt_id, base_nt_id) -> bool
        let mut compat_cache: HashMap<(i64, i64), bool> = HashMap::new();

        let tx = client.transaction().await?;
        for br in base_rows {
            let base_guid: String = br.get(1);
            if let Some((sub_id, sub_nt_id, _sub_nt_guid)) = sub_map.get(&base_guid) {
                let base_nt_id: i64 = br.get(2);

                // Check compatibility between sub_nt_id and base_nt_id using cached field name lists
                let key = (*sub_nt_id, base_nt_id);
                let compatible = if let Some(cached) = compat_cache.get(&key) {
                    *cached
                } else {
                    let sub_fields = fields_by_nt.get(sub_nt_id);
                    let base_fields = fields_by_nt.get(&base_nt_id);
                    let eq = match (sub_fields, base_fields) {
                        (Some(a), Some(b)) => a == b,
                        _ => false,
                    };
                    compat_cache.insert(key, eq);
                    eq
                };
                if !compatible {
                    println!("Incompatible note types: sub_nt_id = {}, base_nt_id = {}, Skipping note guid {}", sub_nt_id, base_nt_id, base_guid);
                    skipped.push(base_guid);
                    continue;
                }

                let base_note_id: i64 = br.get(0);

                // Check for circular note inheritance: if base_note already inherits from subscriber_note, reject
                let circular_note_check = tx
                    .query_opt(
                        "SELECT 1 FROM note_inheritance WHERE subscriber_note_id = $1 AND base_note_id = $2",
                        &[&base_note_id, sub_id],
                    )
                    .await?;

                if circular_note_check.is_some() {
                    println!("Circular inheritance detected: base note {} already inherits from subscriber note {}, skipping", base_note_id, sub_id);
                    skipped.push(base_guid);
                    continue;
                }

                // default is NULL meaning "all fields"
                let subscribed_fields: Option<Vec<i32>> =
                    policy.get(sub_nt_id).cloned().unwrap_or(None);
                tx.execute(
                    "INSERT INTO anki.note_inheritance (subscriber_note_id, base_note_id, subscribed_fields, removed_base_tags) VALUES ($1, $2, $3, '{}') ON CONFLICT (subscriber_note_id) DO NOTHING",
                    &[sub_id, &base_note_id, &subscribed_fields],
                ).await?;
                
                // Remove duplicate locally-reviewed addition tags (action=true, reviewed=true) that also exist on the base note.
                // We deliberately keep unreviewed suggestions and removals so user intent isn't lost.
                tx.execute(
                    r#"
                    DELETE FROM tags ts
                    WHERE ts.note = $1
                       AND ts.reviewed = true AND ts.action = true
                       AND EXISTS (
                        SELECT 1 FROM tags tb 
                        WHERE tb.note = $2 AND 
                            tb.content = ts.content AND 
                            tb.reviewed = true AND 
                            tb.action = true
                       )"#,
                    &[sub_id, &base_note_id]
                ).await?;

                // Remove local field rows that will be inherited (subscribed fields), keeping at least one field row.
                match &subscribed_fields {
                    None => {
                        // subscribe all: keep a single preferred field row, delete the rest
                        tx.execute(
                            r#"
                            WITH keep AS (
                            SELECT id FROM fields WHERE note = $1
                            ORDER BY (position = 0) DESC, (content <> '') DESC, position, id
                            LIMIT 1
                            )
                            DELETE FROM fields WHERE note = $1 AND id NOT IN (SELECT id FROM keep)
                            "#,
                            &[sub_id]
                        ).await?;
                    }
                    Some(ords) if !ords.is_empty() => {
                        // delete only subscribed positions except always preserve position 0 for safety
                        tx.execute(
                            "DELETE FROM fields WHERE note = $1 AND position = ANY($2::int[]) AND position <> 0",
                            &[sub_id, ords]
                        ).await?;
                    }
                    _ => {}
                }
                notes_to_update.push(*sub_id);
                linked += 1;
            } else {
                println!(
                    "No matching subscriber note for base note guid {}, skipping",
                    base_guid
                );
                skipped.push(base_guid);
            }
        }
        tx.commit().await?;
    }

    // bump subscriber note and parent decks' timestamps so pull merges immediately based on notes_to_update
    client
        .execute(&update_notes_ts_stmt, &[&notes_to_update])
        .await?;
    client
        .execute(&update_decks_ts_stmt, &[&notes_to_update])
        .await?;

    Ok((linked, skipped))
}
