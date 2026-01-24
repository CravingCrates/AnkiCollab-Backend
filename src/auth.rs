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
}

#[derive(Deserialize)]
pub struct TokenRefresh {
    pub refresh_token: String,
}

use database::SharedConn;

// Generate a secure random token of specified length
fn generate_secure_token(length: usize) -> String {
    Alphanumeric.sample_string(&mut rand::rng(), length)
}

// Hash a token for secure storage
fn hash_token(token: &str) -> Vec<u8> {
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
        let auth_response = generate_tokens(&client, user_id)
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

// Generate new tokens for a user
pub async fn generate_tokens(
    client: &SharedConn,
    user_id: i32,
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
            "INSERT INTO auth_tokens 
            (user_id, token_hash, refresh_token_hash, expires_at, refresh_expires_at) 
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (user_id) 
         DO UPDATE SET 
            token_hash = EXCLUDED.token_hash, 
            refresh_token_hash = EXCLUDED.refresh_token_hash,
            expires_at = EXCLUDED.expires_at,
            refresh_expires_at = EXCLUDED.refresh_expires_at,
            last_used_at = NOW()",
            &[
                &user_id,
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

    // Look up the user by refresh token hash
    let row = client
        .query_opt(
            "SELECT user_id FROM auth_tokens 
         WHERE refresh_token_hash = $1 
         AND refresh_expires_at > NOW()",
            &[&refresh_token_hash],
        )
        .await
        .map_err(|e| format!("Database error: {e}"))?;

    // If found, generate new tokens
    match row {
        Some(row) => {
            let user_id: i32 = row.get(0);

            // Generate new tokens
            let auth_response = generate_tokens(&client, user_id)
                .await
                .map_err(|e| format!("Error generating tokens: {e}"))?;

            Ok(auth_response)
        }
        None => Err("Invalid or expired refresh token".to_string()),
    }
}

pub async fn remove_token(
    db_state: &Arc<database::AppState>,
    token: &str,
) -> std::result::Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let client = match db_state.db_pool.get().await {
        Ok(pool) => pool,
        Err(err) => {
            println!("Error getting pool: {err}");
            return Err("Internal Error".into());
        }
    };

    // Create a hash of the token
    let token_hash = hash_token(token);

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
