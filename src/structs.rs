use std::collections::HashMap;

use serde::{Deserialize, Serialize};

// Anki Structs from the json export
/* Notetypes */
#[derive(Deserialize, Serialize)]
pub struct NotetypeField {
    pub description: String,
    pub font: String,
    pub id: Option<i64>,
    pub name: String,
    pub ord: i32,
    //pub preventDeletion: bool,
    pub rtl: bool,
    pub size: i32,
    pub sticky: bool,
    pub tag: Option<i32>,
}

#[derive(Deserialize, Serialize)]
pub struct NotetypeTemplate {
    pub afmt: String,
    pub bafmt: String,
    pub bfont: String,
    pub bqfmt: String,
    pub bsize: i32,
    pub id: Option<i64>,
    pub name: String,
    pub ord: i32,
    pub qfmt: String,
}

#[derive(Deserialize, Serialize)]
pub struct CardRequirement {
    pub card_ord: i32,
    pub kind: String,
    pub field_ords: Vec<u32>,
}

#[derive(Deserialize, Serialize)]
pub struct Notetype {
    pub crowdanki_uuid: String,
    pub css: String,
    pub flds: Vec<NotetypeField>,
    #[serde(rename = "latexPost")]
    pub latex_post: String,
    #[serde(rename = "latexPre")]
    pub latex_pre: String,
    #[serde(rename = "latexsvg")]
    pub latex_svg: bool,
    pub name: String,
    #[serde(rename = "originalStockKind")]
    pub original_stock_kind: Option<i32>,
    pub req: Vec<CardRequirement>,
    pub sortf: i32,
    pub tmpls: Vec<NotetypeTemplate>,
    #[serde(rename = "type")]
    pub type_: i32,
}

/* Notes */
#[derive(Deserialize, Serialize)]
pub struct Note {
    pub id: i64,
    pub fields: Vec<String>,
    pub guid: String,
    pub note_model_uuid: String, // this string equals Notetype::crowdanki_uuid
    pub tags: Vec<String>,
}

/* Decks */
#[derive(Deserialize, Serialize)]
pub struct AnkiDeck {
    pub crowdanki_uuid: String,
    pub children: Vec<AnkiDeck>,
    pub desc: String,
    pub name: String,
    pub note_models: Option<Vec<Notetype>>,
    pub notes: Vec<Note>,
}

impl AnkiDeck {
    pub fn from_json_string(json_string: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json_string)
    }
}

#[derive(Deserialize, Serialize)]
pub struct UpdateInfo {
    pub timestamp: String,
}

pub struct DeckUpdateInfo {
    pub deck_id: u64,
    pub deck_guid: String,
    pub deck_update: chrono::NaiveDateTime,
    pub parent: Option<u64>,
}

#[derive(Serialize, Deserialize)]
pub struct NoteModelFieldInfo {
    pub id: i64,
    pub name: String,
    pub protected: bool,
}

#[derive(Serialize, Deserialize)]
pub struct NoteModel {
    pub id: i64,
    pub fields: Vec<NoteModelFieldInfo>,
    pub name: String,
}

#[derive(Serialize, Deserialize)]
pub struct UpdateInfoResponse {
    pub protected_fields: Vec<NoteModel>,
    pub deck: AnkiDeck,
    pub changelog: String,
    pub deck_hash: String,
    pub optional_tags: Vec<String>,
    pub deleted_notes: Vec<String>,
    pub stats_enabled: bool,
}

#[derive(Deserialize, Serialize)]
pub struct CreateDeckReq {
    pub deck: String,
    pub username: String,
}

#[derive(Deserialize, Serialize)]
pub struct SubmitCardReq {
    pub remote_deck: String, // Hash of the deck
    pub deck_path: String,   // Path to the deck in Anki Format
    pub new_name: String, // Users can rename the top most deck. This helps us to keep track of the original name
    pub deck: String,     // JSON String of the actual deck content
    pub rationale: i32,   // Enum. See elsewhere
    pub commit_text: String, // (optional) additional info
    pub token: String,
    pub force_overwrite: bool,
}

#[derive(Deserialize, Serialize)]
pub struct NoteRemovalReq {
    pub remote_deck: String, // Topmost deck that contains all the notes with the guids. This is required because multiple decks might use a card
    pub note_guids: Vec<String>,
    pub commit_text: String,
    pub token: String,
    pub force_overwrite: bool,
}

#[derive(Deserialize, Serialize)]
pub struct SubmitChangelog {
    pub deck_hash: String,
    pub changelog: String,
    pub token: String,
}

#[derive(Deserialize)]
pub struct CheckDeckAliveRequest {
    pub deck_hashes: Vec<String>,
}

#[derive(Deserialize)]
pub struct SubscriptionRequest {
    pub deck_hash: String,
    pub user_hash: String,
}

#[derive(Deserialize)]
pub struct NoteStatsInfo {
    pub retention: i32,
    pub reps: i32,
    pub lapses: i32,
}

type NoteReview = HashMap<String, NoteStatsInfo>; // HashMap<NoteGuid, NoteStatsInfo
type DeckReview = HashMap<String, NoteReview>; // HashMap<DeckName, NoteReview>

#[derive(Deserialize)]
pub struct StatsInfo {
    pub user_hash: String,
    pub deck_hash: String,
    pub review_history: DeckReview,
}

#[derive(Deserialize)]
pub struct TokenInfo {
    pub token: String,
    pub deck_hash: String,
}

#[derive(Deserialize)]
pub struct TokenOnly {
    pub token: String,
}

// Media management

#[derive(Debug, Serialize)]
pub struct MediaDownloadItem {
    pub filename: String,
    pub download_url: String,
}

#[derive(Debug, Deserialize)]
pub struct MediaManifestRequest {
    pub user_token: String,
    pub deck_hash: String,
    pub filenames: Vec<String>,
}

#[derive(Debug, Serialize)]
pub struct MediaManifestResponse {
    pub files: Vec<MediaDownloadItem>,
    pub file_count: i32,
    pub expires_at: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MediaMissingFile {
    pub hash: String,
    pub note_id: i64,
    pub filename: String,
    pub file_size: i64,
    #[serde(alias = "presigned_url")]
    pub upload_url: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MediaBulkCheckResponse {
    pub existing_files: Vec<MediaExistingFile>,
    pub missing_files: Vec<MediaMissingFile>,
    pub failed_files: Vec<MediaFileInfo>,
    pub batch_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MediaBulkConfirmRequest {
    pub batch_id: String,
    pub confirmed_files: Vec<String>, // List of file hashes that were uploaded
    pub bulk_operation_id: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct MediaBulkConfirmResponse {
    pub processed_files: Vec<MediaProcessedFile>,
}

#[derive(Debug, Serialize)]
pub struct MediaProcessedFile {
    pub hash: String,
    pub media_id: i64,
    pub success: bool,
    pub error: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct MediaBulkCheckRequest {
    pub token: String,
    pub deck_hash: String,
    pub files: Vec<MediaFileInfo>,
    pub bulk_operation_id: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct MediaFileInfo {
    pub hash: String,
    pub filename: String,
    pub note_guid: String,
    pub file_size: i64,
}

#[derive(Debug, Serialize)]
pub struct MediaExistingFile {
    pub hash: String,
    pub media_id: i64,
}

// SVG sanitization structures
#[derive(Debug, Serialize, Deserialize)]
pub struct SvgFileItem {
    pub filename: String,
    pub content: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SvgSanitizeRequest {
    pub svg_files: Vec<SvgFileItem>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SanitizedSvgItem {
    pub filename: String,
    pub content: String,
    pub hash: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SvgSanitizeResponse {
    pub sanitized_files: Vec<SanitizedSvgItem>,
}

#[derive(Debug, Deserialize)]
pub struct CreateDeckLinkRequest {
    pub subscriber_deck_hash: String,
    pub base_deck_hash: String,
    pub token: String,
}

#[derive(Debug, Deserialize)]
pub struct CreateNewNoteLinkRequest {
    pub subscriber_deck_hash: String,
    pub base_deck_hash: String,
    pub note_guids: Vec<String>,
    pub token: String,
}

#[derive(Deserialize)]
pub struct LatestPointer {
    pub version_ts: String,
    pub manifest_key: String,
}
