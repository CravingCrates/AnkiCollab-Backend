use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use chrono::{Duration as ChronoDuration, Utc};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const TOKEN_VERSION: u8 = 1;

#[derive(Clone)]
pub struct CacheTokenService {
    secret: Arc<Vec<u8>>,
    ttl: Duration,
}

impl CacheTokenService {
    pub fn new(secret: Vec<u8>, ttl: Duration) -> Result<Self, CacheTokenError> {
        if secret.len() < 32 {
            return Err(CacheTokenError::InvalidSecret);
        }

        Ok(Self {
            secret: Arc::new(secret),
            ttl,
        })
    }

    pub fn generate_token(&self, params: CacheTokenParams) -> Result<String, CacheTokenError> {
        let exp = Self::expiry_from_duration(self.ttl)?;
        let claims = CacheTokenClaims {
            deck_hash: params.deck_hash,
            s3_key: params.s3_key,
            content_type: params.content_type,
            exp,
        };

        self.encode(claims)
    }

    pub fn verify_token(&self, token: &str) -> Result<CacheTokenClaims, CacheTokenError> {
        let envelope = self.decode(token)?;
        if envelope.version != TOKEN_VERSION {
            return Err(CacheTokenError::UnsupportedVersion(envelope.version));
        }

        Self::ensure_not_expired(envelope.claims.exp)?;
        Ok(envelope.claims)
    }

    fn encode(&self, claims: CacheTokenClaims) -> Result<String, CacheTokenError> {
        let envelope = TokenEnvelope {
            version: TOKEN_VERSION,
            claims,
        };

        let payload_bytes =
            serde_json::to_vec(&envelope).map_err(CacheTokenError::Serialization)?;

        let mut mac =
            HmacSha256::new_from_slice(&self.secret).map_err(|_| CacheTokenError::InvalidSecret)?;
        mac.update(&payload_bytes);
        let signature = mac.finalize().into_bytes();

        let payload_b64 = URL_SAFE_NO_PAD.encode(&payload_bytes);
        let signature_b64 = URL_SAFE_NO_PAD.encode(signature);

        Ok(format!("{payload_b64}.{signature_b64}"))
    }

    fn decode(&self, token: &str) -> Result<TokenEnvelope, CacheTokenError> {
        let mut parts = token.split('.');
        let payload_part = parts.next().ok_or(CacheTokenError::InvalidFormat)?;
        let signature_part = parts.next().ok_or(CacheTokenError::InvalidFormat)?;

        if parts.next().is_some() {
            return Err(CacheTokenError::InvalidFormat);
        }

        let payload_bytes = URL_SAFE_NO_PAD
            .decode(payload_part)
            .map_err(CacheTokenError::Decode)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature_part)
            .map_err(CacheTokenError::Decode)?;

        let mut mac =
            HmacSha256::new_from_slice(&self.secret).map_err(|_| CacheTokenError::InvalidSecret)?;
        mac.update(&payload_bytes);
        mac.verify_slice(&signature)
            .map_err(|_| CacheTokenError::InvalidSignature)?;

        let envelope: TokenEnvelope =
            serde_json::from_slice(&payload_bytes).map_err(CacheTokenError::Serialization)?;

        Ok(envelope)
    }

    fn expiry_from_duration(duration: Duration) -> Result<i64, CacheTokenError> {
        let chrono_duration =
            ChronoDuration::from_std(duration).map_err(|_| CacheTokenError::InvalidTtl)?;
        Ok(Utc::now()
            .checked_add_signed(chrono_duration)
            .ok_or(CacheTokenError::InvalidTtl)?
            .timestamp())
    }

    fn ensure_not_expired(exp: i64) -> Result<(), CacheTokenError> {
        if Utc::now().timestamp() > exp {
            return Err(CacheTokenError::Expired);
        }
        Ok(())
    }
}

#[derive(Debug)]
pub enum CacheTokenError {
    InvalidSecret,
    InvalidTtl,
    InvalidFormat,
    InvalidSignature,
    Expired,
    UnsupportedVersion(u8),
    Decode(base64::DecodeError),
    Serialization(serde_json::Error),
}

impl std::fmt::Display for CacheTokenError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CacheTokenError::InvalidSecret => {
                write!(f, "Cache token secret must be at least 32 bytes")
            }
            CacheTokenError::InvalidTtl => write!(f, "Invalid token TTL"),
            CacheTokenError::InvalidFormat => write!(f, "Invalid token format"),
            CacheTokenError::InvalidSignature => write!(f, "Invalid token signature"),
            CacheTokenError::Expired => write!(f, "Token expired"),
            CacheTokenError::UnsupportedVersion(v) => write!(f, "Unsupported token version: {v}"),
            CacheTokenError::Decode(err) => write!(f, "Token decode error: {err}"),
            CacheTokenError::Serialization(err) => write!(f, "Token serialization error: {err}"),
        }
    }
}

impl std::error::Error for CacheTokenError {}

#[derive(Debug, Clone)]
pub struct CacheTokenParams {
    pub deck_hash: String,
    pub s3_key: String,
    pub content_type: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CacheTokenClaims {
    pub deck_hash: String,
    pub s3_key: String,
    pub content_type: Option<String>,
    pub exp: i64,
}

#[derive(Serialize, Deserialize)]
struct TokenEnvelope {
    version: u8,
    claims: CacheTokenClaims,
}
