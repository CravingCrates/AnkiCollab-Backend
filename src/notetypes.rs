use crate::{
    cleanser, database,
    structs::{
        CardRequirement, Note, NoteModel, NoteModelFieldInfo, Notetype, NotetypeField,
        NotetypeTemplate,
    },
};
use std::collections::HashSet;

use database::SharedConn;

pub async fn get_notetype_fields(client: &SharedConn, notetype_id: i64) -> Vec<NotetypeField> {
    let stmt = client
                .prepare("SELECT description, font, name, ord, rtl, size, sticky, anki_id, tag FROM notetype_field WHERE notetype = $1 ORDER BY position")
                .await.expect("Error preparing statement");

    let rows = client
        .query(&stmt, &[&notetype_id])
        .await
        .expect("Error executing statement");

    let mut fields = Vec::with_capacity(rows.len());

    for row in rows {
        let description: String = row.get(0);
        let font: String = row.get(1);
        let name: String = row.get(2);
        let ord: i32 = row.get(3);
        let rtl: bool = row.get(4);
        let size: i32 = row.get(5);
        let sticky: bool = row.get(6);
        let id: Option<i64> = row.get(7);
        let tag: Option<i32> = row.get(8);

        fields.push(NotetypeField {
            description,
            font,
            id,
            name,
            ord,
            rtl,
            size,
            sticky,
            tag,
        });
    }
    fields
}

pub async fn get_notetype_templates(
    client: &SharedConn,
    notetype_id: i64,
) -> Vec<NotetypeTemplate> {
    let stmt = client.prepare("SELECT qfmt, afmt, bqfmt, bafmt, bfont, bsize, name, anki_id, ord FROM notetype_template WHERE notetype = $1 ORDER BY position").await
                     .expect("Error preparing statement");
    let rows = client
        .query(&stmt, &[&notetype_id])
        .await
        .expect("Error executing statement");

    let mut templates = Vec::with_capacity(rows.len());

    for row in rows {
        let qfmt: String = row.get(0);
        let afmt: String = row.get(1);
        let bqfmt: String = row.get(2);
        let bafmt: String = row.get(3);
        let bfont: String = row.get(4);
        let bsize: i32 = row.get(5);
        let name: String = row.get(6);
        let id: Option<i64> = row.get(7);
        let ord: i32 = row.get(8);

        templates.push(NotetypeTemplate {
            afmt,
            bafmt,
            bfont,
            bqfmt,
            bsize,
            id,
            name,
            ord,
            qfmt,
        });
    }
    templates
}

use serde_json::from_str;

pub async fn get_notetypes(client: &SharedConn, notes: &Vec<Note>, owner: i32) -> Vec<Notetype> {
    let mut notetype_guids = HashSet::new();

    for note in notes {
        notetype_guids.insert(note.note_model_uuid.clone());
    }

    let notetype_guids_vec: Vec<&str> = notetype_guids
        .iter()
        .map(std::string::String::as_str)
        .collect();

    let stmt = client
        .prepare("
        SELECT id, guid, css, latex_post, latex_pre, latex_svg, name, type, original_stock_kind, sortf, req 
        FROM notetype n1
        WHERE guid = any($1) AND (
            owner = $2 OR 
            NOT EXISTS (
                SELECT 1 FROM notetype n2 WHERE n2.guid = n1.guid AND n2.owner = $2
            )
        )
        ")
        .await
        .expect("Error preparing statement");

    let rows = client
        .query(&stmt, &[&notetype_guids_vec, &owner])
        .await
        .expect("Error executing query");

    let mut notetypes = Vec::with_capacity(rows.len());

    for row in rows {
        let id: i64 = row.get(0);
        let guid: String = row.get(1);
        let css: String = row.get(2);
        let latex_post: String = row.get(3);
        let latex_pre: String = row.get(4);
        let latex_svg: bool = row.get(5);
        let name: String = row.get(6);
        let type_: i32 = row.get(7);
        let original_stock_kind: Option<i32> = row.get(8);
        let sortf: i32 = row.get(9);
        let req_str: String = row.get(10);

        let req: Vec<CardRequirement> = from_str(&req_str).unwrap_or_default();

        notetypes.push(Notetype {
            crowdanki_uuid: guid,
            css,
            flds: get_notetype_fields(client, id).await,
            latex_post,
            latex_pre,
            latex_svg,
            name,
            tmpls: get_notetype_templates(client, id).await,
            type_,
            original_stock_kind,
            req,
            sortf,
        });
    }

    notetypes
}

pub async fn pull_protected_fields(
    client: &SharedConn,
    human_hash: &String,
) -> std::result::Result<Vec<NoteModel>, Box<dyn std::error::Error + Send + Sync>> {
    let query = "
    WITH RECURSIVE subdecks AS (
        SELECT id, human_hash
        FROM decks
        WHERE human_hash = $1
        UNION ALL
        SELECT d.id, d.human_hash 
        FROM decks d
        JOIN subdecks s ON s.id = d.parent
    )
    SELECT DISTINCT nt.id, nt.name, ntf.id, ntf.name, ntf.protected
    FROM subdecks d
    JOIN notes n ON n.deck = d.id 
    JOIN notetype_field ntf ON ntf.protected = true
    JOIN notetype nt ON nt.id = ntf.notetype AND nt.id = n.notetype    
    ";
    let rows = client.query(query, &[&human_hash]).await?;

    let mut note_models = Vec::new();
    let mut current_note_model = NoteModel {
        id: 0,
        fields: Vec::new(),
        name: String::new(),
    };
    for row in rows {
        let notetype_id: i64 = row.get(0);
        let notetype_name: String = row.get(1);
        let notetype_field_id: i64 = row.get(2);
        let notetype_field_name: String = row.get(3);
        let notetype_field_protected: bool = row.get(4);

        let current_note_model_id = current_note_model.id;
        if current_note_model_id == 0 || current_note_model_id != notetype_id {
            // We've encountered a new notetype, so create a new NoteModel struct
            if !current_note_model.fields.is_empty() {
                note_models.push(current_note_model);
            }
            current_note_model = NoteModel {
                id: notetype_id,
                fields: Vec::new(),
                name: notetype_name.clone(),
            };
        }
        current_note_model.fields.push(NoteModelFieldInfo {
            id: notetype_field_id,
            name: notetype_field_name,
            protected: notetype_field_protected,
        });
    }
    if !current_note_model.fields.is_empty() {
        note_models.push(current_note_model);
    }
    Ok(note_models)
}

pub async fn does_notetype_exist(
    client: &SharedConn,
    notetype: &Notetype,
    owner: i32,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let check_existing_notetype = client
        .query(
            "SELECT id from notetype where guid = $1 AND owner = $2",
            &[&notetype.crowdanki_uuid, &owner],
        )
        .await?;

    if !check_existing_notetype.is_empty() {
        return Ok(notetype.crowdanki_uuid.clone());
    }

    // Before inserting a new notetype, I would like to check if a notetype that is semantically equal, already exists. So query all notetypes that are owned by owner
    // and then check if the notetype.fields and notetype.templates (as well as basic metrics) match a notetype that already exists. If so, return the id of that notetype.

    let existing_notetype_fields_stmt = client
        .prepare("SELECT name, anki_id FROM notetype_field where notetype = $1 ORDER BY position")
        .await?;

    let existing_notetype_templates_stmt = client.prepare("SELECT qfmt, afmt, bqfmt, bafmt, anki_id, name FROM notetype_template where notetype = $1 ORDER BY position").await?;

    let req_json = serde_json::to_string(&notetype.req)?;
    let check_existing_notetype = client.query("SELECT id, guid, original_stock_kind from notetype where owner = $1 and type = $2 and req = $3", &[&owner, &notetype.type_, &req_json]).await?;

    for row in check_existing_notetype {
        if !notetype
            .original_stock_kind
            .zip(row.get::<_, Option<i32>>(2))
            .map_or(true, |(a, b)| a == b)
        {
            continue;
        }

        let existing_notetype_id: i64 = row.get(0);
        let existing_notetype_guid: String = row.get(1);

        let existing_notetype_fields = client
            .query(&existing_notetype_fields_stmt, &[&existing_notetype_id])
            .await?;
        let existing_notetype_templates = client
            .query(&existing_notetype_templates_stmt, &[&existing_notetype_id])
            .await?;

        if notetype.flds.len() != existing_notetype_fields.len()
            || notetype.tmpls.len() != existing_notetype_templates.len()
        {
            continue;
        }

        let matching_fields = notetype.flds.iter().enumerate().all(|(i, field)| {
            existing_notetype_fields.get(i).map_or(false, |existing| {
                let id_match = field
                    .id
                    .zip(existing.get::<_, Option<i64>>(1))
                    .map_or(false, |(a, b)| a != 0 && b != 0 && a == b);
                let name_match = field.name == existing.get::<_, String>(0);
                id_match || name_match
            })
        });

        let matching_templates = notetype.tmpls.iter().enumerate().all(|(i, template)| {
            existing_notetype_templates
                .get(i)
                .map_or(false, |existing| {
                    let id_match = template
                        .id
                        .zip(existing.get::<_, Option<i64>>(4))
                        .map_or(false, |(a, b)| a != 0 && b != 0 && a == b);
                    let qfmt_match = template.qfmt == existing.get::<_, String>(0);
                    let afmt_match = template.afmt == existing.get::<_, String>(1);
                    let bqfmt_match = template.bqfmt == existing.get::<_, String>(2);
                    let bafmt_match = template.bafmt == existing.get::<_, String>(3);
                    id_match || (qfmt_match && afmt_match && bqfmt_match && bafmt_match)
                })
        });

        // Try to find a notetype that is exactly the same, (Best case)
        if matching_fields && matching_templates {
            return Ok(existing_notetype_guid);
        }
        
    }
    // Give up
    Ok(String::new())
}

pub async fn unpack_notetype(
    client: &mut SharedConn,
    notetype: &Notetype,
    deck: Option<i64>,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if deck.is_none() {
        return Err("Attempted insertion on no deck. Abort.".into());
    }

    let deck_owner = client
        .query("SELECT owner from decks where id = $1", &[&deck])
        .await?;
    if deck_owner.is_empty() {
        return Err("Deck does not exist".into());
    }
    let owner: i32 = deck_owner[0].get(0);

    let existing_notetype_guid = does_notetype_exist(client, notetype, owner).await?;
    if !existing_notetype_guid.is_empty() {
        return Ok(existing_notetype_guid);
    }

    // Notetype doesn't exist yet, so insert a new one

    let tx = client.transaction().await?;

    let insert_notetype_stmt = tx.prepare("
        INSERT INTO notetype (guid, owner, css, latex_post, latex_pre, latex_svg, name, type, original_stock_kind, sortf, req)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        RETURNING id
    ").await?;

    let insert_notetype_field_stmt = tx.prepare("
        INSERT INTO notetype_field (notetype, description, font, name, ord, rtl, size, sticky, position, anki_id, tag)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ").await?;

    let insert_notetype_template_stmt = tx.prepare("
        INSERT INTO notetype_template (notetype, qfmt, afmt, bqfmt, bafmt, bfont, bsize, name, position, anki_id, ord)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
    ").await?;

    let original_stock_kind = notetype.original_stock_kind.unwrap_or(0);

    let cleaned_notetype_name = cleanser::clean(&notetype.name);

    let req_json = serde_json::to_string(&notetype.req)?;
    let rows = tx
        .query(
            &insert_notetype_stmt,
            &[
                &notetype.crowdanki_uuid,
                &owner,
                &notetype.css,
                &notetype.latex_post,
                &notetype.latex_pre,
                &notetype.latex_svg,
                &cleaned_notetype_name,
                &notetype.type_,
                &original_stock_kind,
                &notetype.sortf,
                &req_json,
            ],
        )
        .await?;

    let id: i64 = rows[0].get(0);

    for (i, field) in notetype
        .flds
        .iter()
        .enumerate()
        .map(|(i, field)| (i as u32, field))
    {
        let anki_id = field.id.unwrap_or(0);
        let tag = field.tag.unwrap_or(0);
        let cleaned_field_description = ammonia::clean(&field.description);
        let cleaned_field_name = cleanser::clean(&field.name);

        tx.execute(
            &insert_notetype_field_stmt,
            &[
                &id,
                &cleaned_field_description,
                &field.font,
                &cleaned_field_name,
                &field.ord,
                &field.rtl,
                &field.size,
                &field.sticky,
                &i,
                &anki_id,
                &tag,
            ],
        )
        .await?;
    }

    for (i, template) in notetype
        .tmpls
        .iter()
        .enumerate()
        .map(|(i, template)| (i as u32, template))
    {
        let anki_id = template.id.unwrap_or(0);
        let cleaned_tmpl_name = cleanser::clean(&template.name);

        tx.execute(
            &insert_notetype_template_stmt,
            &[
                &id,
                &template.qfmt,
                &template.afmt,
                &template.bqfmt,
                &template.bafmt,
                &template.bfont,
                &template.bsize,
                &cleaned_tmpl_name,
                &i,
                &anki_id,
                &template.ord,
            ],
        )
        .await?;
    }

    tx.commit().await?;

    Ok(notetype.crowdanki_uuid.clone())
}

pub async fn delete_unused_notetypes(
    client: &SharedConn,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    client
        .query(
            "
        DELETE FROM notetype WHERE id NOT IN (
                SELECT DISTINCT notetype FROM notes
        )
    ",
            &[],
        )
        .await?;

    Ok("Success".into())
}
