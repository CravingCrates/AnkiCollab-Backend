// Retrieve all media files that belong to a given deck
// pub async fn get_media_files(client: &SharedConn, deck_id: i64) -> std::result::Result<Vec<String>, Box<dyn std::error::Error>> {
//     let query = "Select fileName from media where deck = $1";
//     let media_files = client.query(query, &[&deck_id])
//     .await?
//     .into_iter()
//     .map(|row| row.get::<_, String>("fileName"))
//     .collect::<Vec<String>>();

//     Ok(media_files)
// }

// Unpack media files and store them in database
// pub async fn unpack_media(client: &mut SharedConn, files: &Vec<String>, deck: Option<i64>)-> std::result::Result<String, Box<dyn std::error::Error>> {
//     let deck_id = deck.ok_or("Attempted insertion on no deck. Abort. (media)")?;

//     let tx = client.transaction().await?;

//     let insert_media_stmt = tx.prepare("
//         INSERT INTO media (fileName, deck)
//         VALUES ($1, $2) ON CONFLICT DO NOTHING
//     ").await?;

//     for file in files {
//         let f = cleanser::clean(file);
//         tx.execute(&insert_media_stmt, &[
//             &f,
//             &deck_id,
//         ]).await?;
//     }

//     tx.commit().await?;
//     Ok("Success".into())
// }
