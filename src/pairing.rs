//! Device pairing: link an Anki profile to an account by approving a short code in
//! the browser, so no password is ever typed into the addon.
//!
//! The two halves never call each other. The client asks for a code and polls; the
//! website marks that same row approved. Both reach `public.device_pair_sessions`
//! directly, which is why there is no service-to-service secret and why the website
//! never mints a token - minting stays in the one place that already does it.
//!
//! A pairing ends in an ordinary device token on the ordinary `(user_id, device_id)`
//! row, so nothing downstream needs to know pairing exists.

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use chrono::{Duration, Utc};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth;
use crate::database::AppState;

/// How long a code stays worth typing. Also what the client is told, so its own
/// countdown cannot disagree with the server.
const PAIRING_TTL_MINUTES: i64 = 5;

/// Only ever travels to the device that asked for it.
const POLL_TOKEN_LENGTH: usize = 48;

/// Crockford-style: no `I`, `L`, `O` or `U`, so nothing is ambiguous to read off a
/// screen or mistype. 32^6 is about a billion, which puts guessing a live code out
/// of reach even before the rate limit.
const CODE_ALPHABET: &[u8] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";
const CODE_LENGTH: usize = 6;

/// A collision means two unrelated users both asked at the same moment; retrying is
/// cheaper than reasoning about the birthday bound.
const CODE_ATTEMPTS: usize = 5;

/// Upper bound on a client-supplied token before it reaches a hash, matching what
/// the bearer extractor allows a bearer token.
const MAX_TOKEN_LENGTH: usize = 1024;

#[derive(Deserialize)]
pub struct PairInitRequest {
    /// The same stable per-profile id the password flow sends, so approving replaces
    /// that profile's existing token instead of adding a second row for the device.
    #[serde(default)]
    pub device_id: String,
}

#[derive(Serialize)]
pub struct PairInitResponse {
    pub code: String,
    /// Returned once and never stored in the clear, so a dump of the table is not a
    /// set of live pairing tokens. The client holds this and polls with it.
    pub poll_token: String,
    pub expires_in: i64,
}

#[derive(Deserialize)]
pub struct PairPollRequest {
    /// In the body rather than the query string: this is a credential, and a URL
    /// travels through access logs, caches and history.
    pub token: String,
}

/// Deliberately shaped like `POST /login`, so the addon hands the approved case to
/// the same code that stores a password login and pairing needs no second parser.
#[derive(Serialize)]
struct PairPollResponse {
    status: &'static str,
    /// Absent until approved; `flatten` plus `None` contributes no fields at all.
    #[serde(flatten)]
    auth: Option<auth::AuthResponse>,
}

enum PollOutcome {
    Pending,
    Approved(auth::AuthResponse),
    /// Approved, but an earlier poll already collected the token. Reported honestly
    /// rather than re-minted: the poll token is single-use.
    Consumed,
    /// Denied in the browser, so the client can say so immediately instead of
    /// waiting out the expiry.
    Rejected,
    Expired,
}

fn generate_code() -> String {
    let mut rng = rand::rng();
    let mut code = String::with_capacity(CODE_LENGTH + 1);
    for index in 0..CODE_LENGTH {
        // Grouped for the person reading it aloud or typing it.
        if index == CODE_LENGTH / 2 {
            code.push('-');
        }
        let pick = rng.random_range(0..CODE_ALPHABET.len());
        code.push(char::from(CODE_ALPHABET[pick]));
    }
    code
}

fn respond(outcome: PollOutcome) -> (StatusCode, String) {
    let (status_code, body) = match outcome {
        PollOutcome::Pending => (
            StatusCode::OK,
            PairPollResponse {
                status: "pending",
                auth: None,
            },
        ),
        PollOutcome::Approved(auth) => (
            StatusCode::OK,
            PairPollResponse {
                status: "approved",
                auth: Some(auth),
            },
        ),
        // Distinct from "expired" so the client does not blame a timeout for a
        // deliberate denial.
        PollOutcome::Rejected => (
            StatusCode::FORBIDDEN,
            PairPollResponse {
                status: "rejected",
                auth: None,
            },
        ),
        PollOutcome::Consumed => (
            StatusCode::GONE,
            PairPollResponse {
                status: "consumed",
                auth: None,
            },
        ),
        PollOutcome::Expired => (
            StatusCode::NOT_FOUND,
            PairPollResponse {
                status: "expired",
                auth: None,
            },
        ),
    };
    (status_code, serde_json::to_string(&body).unwrap())
}

async fn begin_pairing(
    db_state: &Arc<AppState>,
    raw_device_id: &str,
) -> Result<PairInitResponse, String> {
    let client = db_state
        .db_pool
        .get_owned()
        .await
        .map_err(|e| format!("Error getting pool: {e}"))?;

    // Sessions that were never approved, or were approved long ago, are garbage.
    // Starting a pairing is rare and user-initiated, so sweeping here beats giving
    // this table a background job of its own.
    let _ = client
        .execute(
            "DELETE FROM device_pair_sessions
              WHERE expires_at < NOW() - INTERVAL '1 hour'
                 OR (status = 'consumed' AND created_at < NOW() - INTERVAL '1 day')",
            &[],
        )
        .await;

    let device_id = auth::normalise_device_id(raw_device_id);
    let poll_token = auth::generate_secure_token(POLL_TOKEN_LENGTH);
    let poll_token_hash = auth::hash_token(&poll_token);
    let expires_at = Utc::now() + Duration::minutes(PAIRING_TTL_MINUTES);

    for _ in 0..CODE_ATTEMPTS {
        let code = generate_code();
        let inserted = client
            .execute(
                "INSERT INTO device_pair_sessions (code, poll_token_hash, device_id, expires_at)
                 VALUES ($1, $2, $3, $4)
                 ON CONFLICT (code) DO NOTHING",
                &[&code, &poll_token_hash, &device_id, &expires_at],
            )
            .await
            .map_err(|e| format!("Error starting pairing: {e}"))?;

        if inserted == 1 {
            return Ok(PairInitResponse {
                code,
                poll_token,
                expires_in: PAIRING_TTL_MINUTES * 60,
            });
        }
    }

    Err("Could not allocate an unused pairing code".to_string())
}

async fn collect_pairing(
    db_state: &Arc<AppState>,
    poll_token: &str,
) -> Result<PollOutcome, String> {
    let client = db_state
        .db_pool
        .get_owned()
        .await
        .map_err(|e| format!("Error getting pool: {e}"))?;

    let poll_token_hash = auth::hash_token(poll_token);

    // Claiming and reading in one statement, so two polls arriving together cannot
    // both walk away with a token for the same approval.
    let claimed = client
        .query_opt(
            "UPDATE device_pair_sessions
                SET status = 'consumed'
              WHERE poll_token_hash = $1
                AND status = 'approved'
                AND user_id IS NOT NULL
                AND expires_at > NOW()
          RETURNING user_id, device_id",
            &[&poll_token_hash],
        )
        .await
        .map_err(|e| format!("Error polling pairing: {e}"))?;

    if let Some(row) = claimed {
        let user_id: i32 = row.get(0);
        let device_id: String = row.get(1);

        let auth_response = auth::generate_tokens(&client, user_id, &device_id)
            .await
            .map_err(|e| format!("Error generating tokens: {e}"))?;

        return Ok(PollOutcome::Approved(auth_response));
    }

    // Not approved, so say why: still waiting, denied, already collected, or gone.
    let row = client
        .query_opt(
            "SELECT status FROM device_pair_sessions
              WHERE poll_token_hash = $1 AND expires_at > NOW()",
            &[&poll_token_hash],
        )
        .await
        .map_err(|e| format!("Error polling pairing: {e}"))?;

    Ok(match row.map(|row| row.get::<_, String>(0)).as_deref() {
        Some("pending" | "approved") => PollOutcome::Pending,
        Some("rejected") => PollOutcome::Rejected,
        Some("consumed") => PollOutcome::Consumed,
        Some(_) | None => PollOutcome::Expired,
    })
}

/// Start a pairing. Unauthenticated by design - the client has no token yet, which
/// is the whole point.
pub async fn post_pair_init(
    State(db_state): State<Arc<AppState>>,
    Json(body): Json<PairInitRequest>,
) -> impl IntoResponse {
    match begin_pairing(&db_state, &body.device_id).await {
        Ok(response) => (StatusCode::OK, serde_json::to_string(&response).unwrap()),
        Err(error) => {
            sentry::capture_message(&error, sentry::Level::Error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Internal Error".to_string(),
            )
        }
    }
}

/// Collect the result of a pairing. Also unauthenticated: the poll token *is* the
/// credential, and it is only handed to the device that started the pairing. It
/// travels in the body, not the query string, so it stays out of access logs.
pub async fn post_pair_poll(
    State(db_state): State<Arc<AppState>>,
    Json(body): Json<PairPollRequest>,
) -> impl IntoResponse {
    if body.token.is_empty() || body.token.len() > MAX_TOKEN_LENGTH {
        return respond(PollOutcome::Expired);
    }

    match collect_pairing(&db_state, &body.token).await {
        Ok(outcome) => respond(outcome),
        Err(error) => {
            sentry::capture_message(&error, sentry::Level::Error);
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"status":"error"}"#.to_string(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_code_is_grouped_and_free_of_ambiguous_characters() {
        let code = generate_code();
        assert_eq!(code.len(), CODE_LENGTH + 1);
        assert_eq!(code.chars().nth(CODE_LENGTH / 2), Some('-'));
        for character in code.chars().filter(|c| *c != '-') {
            assert!(
                "0123456789ABCDEFGHJKMNPQRSTVWXYZ".contains(character),
                "unexpected character {character} in {code}"
            );
        }
    }

    #[test]
    fn codes_do_not_collide_in_a_small_sample() {
        // Not a uniqueness proof - begin_pairing retries on conflict - but it would
        // catch a generator that ignored its alphabet and returned a constant.
        let codes: std::collections::HashSet<String> = (0..200).map(|_| generate_code()).collect();
        assert!(codes.len() > 190, "only {} distinct codes", codes.len());
    }

    #[test]
    fn a_pending_pairing_carries_no_credentials() {
        let body = serde_json::to_string(&PairPollResponse {
            status: "pending",
            auth: None,
        })
        .unwrap();
        assert_eq!(body, r#"{"status":"pending"}"#);
        assert!(!body.contains("token"));
    }

    #[test]
    fn an_approved_pairing_looks_exactly_like_a_login() {
        let body = serde_json::to_string(&PairPollResponse {
            status: "approved",
            auth: Some(auth::AuthResponse {
                token: "access".to_string(),
                refresh_token: "refresh".to_string(),
                expires_at: Utc::now(),
                user_id: 7,
            }),
        })
        .unwrap();

        // Flattened, so the addon's existing login handler parses this unchanged.
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["status"], "approved");
        assert_eq!(parsed["token"], "access");
        assert_eq!(parsed["refresh_token"], "refresh");
        assert_eq!(parsed["user_id"], 7);
        assert!(parsed["expires_at"].is_i64());
    }
}
