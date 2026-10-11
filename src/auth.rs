use axum::extract::{FromRequestParts, OptionalFromRequestParts};
use axum::http::request::Parts;
use axum::http::StatusCode;
use chrono::serde::ts_seconds;
use chrono::{DateTime, Duration, Utc};
use rand::distr::{Alphanumeric, SampleString};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;

use crate::database;

// Token configuration constants
const ACCESS_TOKEN_LENGTH: usize = 48;
const REFRESH_TOKEN_LENGTH: usize = 64;
const ACCESS_TOKEN_EXPIRY_DAYS: i64 = 30;
const REFRESH_TOKEN_EXPIRY_DAYS: i64 = 90;

// Response struct for authentication
#[derive(Serialize, Debug)]
pub struct AuthResponse {
    pub token: String,
    pub refresh_token: String,
    #[serde(with = "ts_seconds")]
    pub expires_at: DateTime<Utc>,
    pub user_id: i32,
}

#[derive(Deserialize)]
pub struct Login {
    pub username: String,
    pub password: String,
    /// Stable per Anki profile. A client that predates device tokens sends nothing
    /// and lands on the legacy sentinel, keeping the previous behaviour exactly.
    #[serde(default = "legacy_device_id")]
    pub device_id: String,
}

fn legacy_device_id() -> String {
    LEGACY_DEVICE_ID.to_string()
}

/// What a request with no usable `device_id` is treated as.
///
/// Also the default for the `device_id` column, so pre-existing rows and
/// un-updated clients agree without a backfill.
pub const LEGACY_DEVICE_ID: &str = "legacy";

/// Client-supplied, so bounded and validated: it is a database key and it ends up
/// in logs. Anything unacceptable collapses to the legacy sentinel rather than
/// failing the login, so a bad value degrades to the old behaviour instead of
/// locking a user out.
pub(crate) fn normalise_device_id(raw: &str) -> String {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || trimmed.len() > 64
        || !trimmed
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        return legacy_device_id();
    }
    trimmed.to_string()
}

#[derive(Deserialize)]
pub struct TokenRefresh {
    pub refresh_token: String,
}

use database::SharedConn;

// ── Authenticated user extractor ─────────────────────────────────────
// Reads `Authorization: Bearer <token>` from request headers.
// If present and valid, yields `AuthenticatedUser`.
// Use `Option<AuthenticatedUser>` for endpoints where auth is optional.

pub struct AuthenticatedUser {
    pub user_id: i32,
    /// The hash of the token this request arrived with, so a handler can act on the
    /// caller's own row. That is what logging out one device needs. Deliberately not
    /// the raw token, and this struct is deliberately not `Debug`.
    pub token_hash: Vec<u8>,
}

impl FromRequestParts<Arc<database::AppState>> for AuthenticatedUser {
    type Rejection = (StatusCode, String);

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<database::AppState>,
    ) -> Result<Self, Self::Rejection> {
        let token = parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
            .ok_or((
                StatusCode::UNAUTHORIZED,
                "Missing or invalid Authorization header".to_string(),
            ))?;

        if token.is_empty() || token.len() > 1024 {
            return Err((StatusCode::UNAUTHORIZED, "Malformed token".to_string()));
        }

        let user_id = get_user_from_token(state, token).await.map_err(|_| {
            (
                StatusCode::UNAUTHORIZED,
                "Invalid or expired token".to_string(),
            )
        })?;

        if user_id == 0 {
            return Err((StatusCode::UNAUTHORIZED, "Invalid token".to_string()));
        }

        Ok(Self {
            user_id,
            token_hash: hash_token(token),
        })
    }
}

/// Optional variant: returns `Ok(None)` when the header is missing or the
/// token is invalid, instead of rejecting the request outright.
impl OptionalFromRequestParts<Arc<database::AppState>> for AuthenticatedUser {
    type Rejection = std::convert::Infallible;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<database::AppState>,
    ) -> Result<Option<Self>, Self::Rejection> {
        let token = match parts
            .headers
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("Bearer "))
        {
            Some(t) => t.to_owned(),
            None => return Ok(None),
        };

        if token.is_empty() || token.len() > 1024 {
            return Ok(None); // reject safely
        }

        match get_user_from_token(state, &token).await {
            Ok(user_id) if user_id > 0 => Ok(Some(Self {
                user_id,
                token_hash: hash_token(&token),
            })),
            _ => Ok(None),
        }
    }
}

// Generate a secure random token of specified length
// Shared with the pairing flow, which issues a poll token and stores it the same way.
pub(crate) fn generate_secure_token(length: usize) -> String {
    Alphanumeric.sample_string(&mut rand::rng(), length)
}

// Hash a token for secure storage
pub(crate) fn hash_token(token: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hasher.finalize().to_vec()
}

pub async fn login(
    db_state: &Arc<database::AppState>,
    form: &Login,
) -> Result<AuthResponse, String> {
    let client = match db_state.db_pool.get_owned().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Internal Error".into());
        }
    };
    let normalized_username = form.username.to_lowercase();

    let row = client
        .query_one(
            "SELECT id, username, password FROM users WHERE username = $1",
            &[&normalized_username],
        )
        .await
        .map_err(|_| "Invalid username or password".to_string())?;

    let user_id: i32 = row.get(0);
    let password_hash: String = row.get(2);

    if verify_password(&form.password, &password_hash)? {
        // Generate new tokens
        let device_id = normalise_device_id(&form.device_id);
        let auth_response = generate_tokens(&client, user_id, &device_id)
            .await
            .map_err(|e| format!("Error generating tokens: {e}"))?;

        Ok(auth_response)
    } else {
        Err("Invalid username or password".to_string())
    }
}

fn verify_password(password: &str, hash: &str) -> Result<bool, String> {
    argon2::verify_encoded(hash, password.as_bytes())
        .map_err(|e| format!("Password verification error: {e}"))
}

// Generate new tokens for a user, replacing that device's previous row.
pub async fn generate_tokens(
    client: &SharedConn,
    user_id: i32,
    device_id: &str,
) -> Result<AuthResponse, Box<dyn std::error::Error + Send + Sync>> {
    let access_token = generate_secure_token(ACCESS_TOKEN_LENGTH);
    let refresh_token = generate_secure_token(REFRESH_TOKEN_LENGTH);

    let access_token_hash = hash_token(&access_token);
    let refresh_token_hash = hash_token(&refresh_token);

    let now = Utc::now();
    let access_expires = now + Duration::days(ACCESS_TOKEN_EXPIRY_DAYS);
    let refresh_expires = now + Duration::days(REFRESH_TOKEN_EXPIRY_DAYS);

    client
        .execute(
            // Conflict on (user_id, device_id), not on user_id: a login or a refresh
            // on one device used to overwrite every other device's row, which silently
            // signed those devices out.
            "INSERT INTO auth_tokens 
            (user_id, device_id, token_hash, refresh_token_hash, expires_at, refresh_expires_at) 
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (user_id, device_id) 
         DO UPDATE SET 
            token_hash = EXCLUDED.token_hash, 
            refresh_token_hash = EXCLUDED.refresh_token_hash,
            expires_at = EXCLUDED.expires_at,
            refresh_expires_at = EXCLUDED.refresh_expires_at,
            last_used_at = NOW()",
            &[
                &user_id,
                &device_id,
                &access_token_hash,
                &refresh_token_hash,
                &access_expires,
                &refresh_expires,
            ],
        )
        .await?;

    Ok(AuthResponse {
        token: access_token,
        refresh_token,
        expires_at: access_expires,
        user_id,
    })
}

// Refresh an access token using a refresh token
pub async fn refresh_token(
    db_state: &Arc<database::AppState>,
    refresh_req: &TokenRefresh,
) -> Result<AuthResponse, String> {
    let client = match db_state.db_pool.get_owned().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Internal Error".into());
        }
    };

    // Hash the provided refresh token
    let refresh_token_hash = hash_token(&refresh_req.refresh_token);

    // Look up the user and the device by refresh token hash
    let row = client
        .query_opt(
            "SELECT user_id, device_id FROM auth_tokens 
         WHERE refresh_token_hash = $1 
         AND refresh_expires_at > NOW()",
            &[&refresh_token_hash],
        )
        .await
        .map_err(|e| format!("Database error: {e}"))?;

    // If found, generate new tokens for the same device, so refreshing here cannot
    // disturb any other device's row.
    match row {
        Some(row) => {
            let user_id: i32 = row.get(0);
            let device_id: String = row.get(1);

            // Generate new tokens
            let auth_response = generate_tokens(&client, user_id, &device_id)
                .await
                .map_err(|e| format!("Error generating tokens: {e}"))?;

            Ok(auth_response)
        }
        None => Err("Invalid or expired refresh token".to_string()),
    }
}

/// Remove exactly one token row, identified by its hash.
///
/// Logging out on one device must not sign the account out everywhere, which is
/// what deleting by `user_id` would now do.
pub async fn remove_token_by_hash(
    db_state: &Arc<database::AppState>,
    token_hash: &[u8],
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Internal Error".into());
        }
    };

    let result = client
        .execute(
            "DELETE FROM auth_tokens WHERE token_hash = $1",
            &[&token_hash],
        )
        .await?;

    if result == 0 {
        return Ok("Token not found".into());
    }

    Ok("Token successfully removed".into())
}

// Get user ID from an access token
pub async fn get_user_from_token(
    db_state: &Arc<database::AppState>,
    token: &str,
) -> Result<i32, Box<dyn std::error::Error + Send + Sync>> {
    if token.is_empty() {
        return Err("Token not provided".into());
    }

    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Internal Error".into());
        }
    };

    // Create a hash of the token
    let token_hash = hash_token(token);

    // Look up the token and update the last_used_at timestamp
    let rows = client
        .query(
            "UPDATE auth_tokens 
         SET last_used_at = NOW() 
         WHERE token_hash = $1 
         AND expires_at > NOW() 
         RETURNING user_id",
            &[&token_hash],
        )
        .await?;

    if rows.is_empty() {
        Err("Token not found or expired".into())
    } else {
        Ok(rows[0].get(0))
    }
}

// Get username from an access token
pub async fn get_username_from_token(
    db_state: &Arc<database::AppState>,
    token: &str,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    if token.is_empty() {
        return Err("Token not provided".into());
    }

    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Internal Error".into());
        }
    };

    let token_hash = hash_token(token);

    let rows = client
        .query(
            r"
            WITH updated AS (
                UPDATE auth_tokens
                SET last_used_at = NOW()
                WHERE token_hash = $1
                  AND expires_at > NOW()
                RETURNING user_id
            )
            SELECT users.username
            FROM users
            JOIN updated ON users.id = updated.user_id
            ",
            &[&token_hash],
        )
        .await?;

    if rows.is_empty() {
        Err("Token not found or expired".into())
    } else {
        Ok(rows[0].get(0))
    }
}

// Check if user has permission for a specific deck
pub async fn is_valid_user_token(
    db_state: &Arc<database::AppState>,
    token: &str,
    deck: &String,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Internal Error".into());
        }
    };

    let token_hash = hash_token(token);

    let rows = client.query("    
        WITH RECURSIVE deck_ancestors AS (
            SELECT id, parent, owner
            FROM decks
            WHERE human_hash = $2
            UNION
            SELECT d.id, d.parent, d.owner
            FROM decks d
            INNER JOIN deck_ancestors da ON da.parent = d.id
        ), user_from_token AS (
            SELECT user_id
            FROM auth_tokens
            WHERE token_hash = $1 
            AND expires_at > NOW()
        )
        SELECT access
        FROM (
            SELECT 1 AS access
            FROM decks d
            INNER JOIN deck_ancestors da ON da.id = d.id
            INNER JOIN user_from_token uft ON (d.owner = uft.user_id OR EXISTS (SELECT 1 FROM maintainers m WHERE m.deck = d.id AND m.user_id = uft.user_id))
            UNION
            SELECT 1 AS access
            FROM maintainers m
            INNER JOIN deck_ancestors da ON da.id = m.deck
            INNER JOIN user_from_token uft ON m.user_id = uft.user_id
        ) AS access_subquery      
    ", &[&token_hash, &deck]).await?;

    Ok(!rows.is_empty())
}

// Simple check if a token is valid
pub async fn is_authenticated(db_state: &Arc<database::AppState>, token: &str) -> bool {
    match get_user_from_token(db_state, token).await {
        Ok(user_id) => user_id > 0,
        Err(_) => false,
    }
}

// Cleanup expired tokens
pub async fn cleanup_expired_tokens(
    db_state: &Arc<database::AppState>,
) -> Result<u64, Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Internal Error".into());
        }
    };

    // Delete tokens that have expired
    let result = client
        .execute(
            "DELETE FROM auth_tokens WHERE expires_at < NOW() AND refresh_expires_at < NOW()",
            &[],
        )
        .await?;

    Ok(result)
}

// ── Helper functions for the new extractor-based auth ─────────────────

/// Get username by user ID (used after `AuthenticatedUser` extraction)
pub async fn get_username_by_user_id(
    db_state: &Arc<database::AppState>,
    user_id: i32,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = db_state.db_pool.get().await?;
    let row = client
        .query_one("SELECT username FROM users WHERE id = $1", &[&user_id])
        .await?;
    Ok(row.get(0))
}

#[must_use]
pub fn user_hash_from_username(username: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(username.as_bytes());
    hex::encode(hasher.finalize())
}

pub async fn get_user_hash_by_user_id(
    db_state: &Arc<database::AppState>,
    user_id: i32,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let username = get_username_by_user_id(db_state, user_id).await?;
    Ok(user_hash_from_username(&username))
}

/// Check if a user (by ID) is an owner or maintainer of a deck (by `human_hash`).
/// Equivalent to `is_valid_user_token` but works with `user_id` directly,
/// avoiding a redundant token lookup.
pub async fn is_deck_owner_or_maintainer(
    db_state: &Arc<database::AppState>,
    user_id: i32,
    deck_hash: &str,
) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
    let client = db_state.db_pool.get().await?;

    let rows = client
        .query(
            "
        WITH RECURSIVE deck_ancestors AS (
            SELECT id, parent, owner
            FROM decks
            WHERE human_hash = $2
            UNION
            SELECT d.id, d.parent, d.owner
            FROM decks d
            INNER JOIN deck_ancestors da ON da.parent = d.id
        )
        SELECT 1 AS access
        FROM (
            SELECT 1 AS access
            FROM decks d
            INNER JOIN deck_ancestors da ON da.id = d.id
            WHERE d.owner = $1 OR EXISTS (
                SELECT 1 FROM maintainers m WHERE m.deck = d.id AND m.user_id = $1
            )
            UNION
            SELECT 1 AS access
            FROM maintainers m
            INNER JOIN deck_ancestors da ON da.id = m.deck
            WHERE m.user_id = $1
        ) AS access_subquery
        ",
            &[&user_id, &deck_hash],
        )
        .await?;

    Ok(!rows.is_empty())
}

/// Remove all auth tokens for a user.
///
/// Unused by any route since logout became device-scoped. Kept for an explicit
/// "sign out everywhere" action; do not wire it to the ordinary logout button, as
/// that would sign the user out of every device they are logged in on.
pub async fn remove_all_tokens_for_user(
    db_state: &Arc<database::AppState>,
    user_id: i32,
) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = db_state.db_pool.get().await?;

    let result = client
        .execute("DELETE FROM auth_tokens WHERE user_id = $1", &[&user_id])
        .await?;

    if result == 0 {
        return Ok("Token not found".into());
    }

    Ok("Token successfully removed".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_device_id_is_bounded_and_sanitised() {
        // The value is client-supplied and becomes a database key, so anything a
        // client could use to smuggle in separators, control characters or unbounded
        // length collapses to the legacy sentinel.
        assert_eq!(normalise_device_id("abc-123_X"), "abc-123_X");
        assert_eq!(normalise_device_id("  abc  "), "abc");
        assert_eq!(normalise_device_id(""), LEGACY_DEVICE_ID);
        assert_eq!(normalise_device_id("   "), LEGACY_DEVICE_ID);
        assert_eq!(normalise_device_id("has space"), LEGACY_DEVICE_ID);
        assert_eq!(normalise_device_id("has/slash"), LEGACY_DEVICE_ID);
        assert_eq!(normalise_device_id("has\u{0}null"), LEGACY_DEVICE_ID);
        assert_eq!(normalise_device_id(&"a".repeat(65)), LEGACY_DEVICE_ID);
        assert_eq!(normalise_device_id(&"a".repeat(64)), "a".repeat(64));
    }

    #[test]
    fn a_login_without_a_device_id_is_treated_as_legacy() {
        // This is what keeps already-shipped addon builds working exactly as before:
        // they send no device id and land on the same sentinel the column default
        // gives the rows that already exist.
        let login: Login = serde_json::from_str(r#"{"username":"a","password":"b"}"#)
            .expect("an absent device id must be accepted");
        assert_eq!(login.device_id, LEGACY_DEVICE_ID);
        assert_eq!(normalise_device_id(&login.device_id), LEGACY_DEVICE_ID);
    }

    #[test]
    fn the_subscription_user_hash_is_sha256_of_the_username() {
        assert_eq!(
            user_hash_from_username("alice"),
            "2bd806c97f0e00af1a1fc3328fa763a9269723c8db8fac4f93af71db186d6e90"
        );
    }
}
