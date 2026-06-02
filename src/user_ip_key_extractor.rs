use axum::http::Request;
use hex::encode;
use sha2::{Digest, Sha256};
use tower_governor::{
    key_extractor::{KeyExtractor, SmartIpKeyExtractor},
    GovernorError,
};

#[derive(Clone)]
pub struct UserOrIpKeyExtractor;

impl KeyExtractor for UserOrIpKeyExtractor {
    type Key = String;

    fn extract<B>(&self, req: &Request<B>) -> Result<Self::Key, GovernorError> {
        // Authenticated path: key on a hash of the bearer token
        if let Some(auth) = req.headers().get(axum::http::header::AUTHORIZATION) {
            if let Ok(val) = auth.to_str() {
                if let Some(token) = val.strip_prefix("Bearer ") {
                    if !token.trim().is_empty() {
                        let mut hasher = Sha256::new();
                        hasher.update(token.trim().as_bytes());
                        return Ok(format!("u:{}", encode(hasher.finalize())));
                    }
                }
            }
        }

        // Anonymous path: delegate to SmartIpKeyExtractor
        let ip = SmartIpKeyExtractor.extract(req)?;
        Ok(format!("ip:{ip}"))
    }

}