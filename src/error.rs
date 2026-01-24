//! Centralized error handling for better Sentry reporting and debugging.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};

pub fn hash_pii(data: &str) -> String {
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

/// Error category determines whether an error should be reported to Sentry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCategory {
    /// Actual bugs that need investigation - ALWAYS report to Sentry
    Bug,
    /// Expected failures (auth, validation, not found) - never report to Sentry
    Expected,
    /// Operational issues (rate limits, timeouts) - report as warnings
    Operational,
}

/// A structured application error with full context for debugging.
#[derive(Debug)]
pub struct AppError {
    /// The HTTP status code to return
    pub status: StatusCode,
    /// User-facing error message (sanitized)
    pub message: String,
    /// Internal error details for logging (not exposed to users)
    pub internal_message: Option<String>,
    /// The operation/endpoint that failed
    pub operation: &'static str,
    /// Error category for Sentry filtering
    pub category: ErrorCategory,
    /// Additional context as key-value pairs
    pub context: Vec<(&'static str, String)>,
}

impl AppError {
    /// Create a new bug error (will be reported to Sentry as Error)
    pub fn bug(operation: &'static str, internal_msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: "Internal server error. Please try again or contact support.".to_string(),
            internal_message: Some(internal_msg.into()),
            operation,
            category: ErrorCategory::Bug,
            context: Vec::new(),
        }
    }

    /// Create an expected error (will NOT be reported to Sentry)
    pub fn expected(
        status: StatusCode,
        operation: &'static str,
        user_msg: impl Into<String>,
    ) -> Self {
        Self {
            status,
            message: user_msg.into(),
            internal_message: None,
            operation,
            category: ErrorCategory::Expected,
            context: Vec::new(),
        }
    }

    /// Create an operational error (will be reported to Sentry as Warning)
    pub fn operational(
        status: StatusCode,
        operation: &'static str,
        user_msg: impl Into<String>,
        internal_msg: impl Into<String>,
    ) -> Self {
        Self {
            status,
            message: user_msg.into(),
            internal_message: Some(internal_msg.into()),
            operation,
            category: ErrorCategory::Operational,
            context: Vec::new(),
        }
    }

    /// Add context to the error
    #[must_use]
    pub fn with_context(mut self, key: &'static str, value: impl Into<String>) -> Self {
        self.context.push((key, value.into()));
        self
    }

    /// Override the status code
    #[must_use]
    pub fn with_status(mut self, status: StatusCode) -> Self {
        self.status = status;
        self
    }

    /// Override the user-facing message
    #[must_use]
    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message = message.into();
        self
    }

    /// Report this error to Sentry if appropriate
    pub fn report_to_sentry(&self) {
        match self.category {
            ErrorCategory::Bug => self.capture_sentry_error(),
            ErrorCategory::Operational => self.capture_sentry_warning(),
            ErrorCategory::Expected => {
                // Only add breadcrumb for expected errors, don't capture
                self.add_sentry_breadcrumb();
            }
        }
    }

    fn capture_sentry_error(&self) {
        sentry::with_scope(
            |scope| {
                // Set fingerprint for grouping similar errors
                scope.set_fingerprint(Some(&[self.operation, &self.status.as_str()]));

                // Add context as extras
                for (key, value) in &self.context {
                    scope.set_extra(*key, value.clone().into());
                }

                scope.set_tag("operation", self.operation);
                scope.set_tag("status_code", self.status.as_str());
                scope.set_tag("error_category", "bug");
            },
            || {
                let msg = format!(
                    "[{}] {} - {}",
                    self.operation,
                    self.status,
                    self.internal_message.as_deref().unwrap_or(&self.message)
                );
                sentry::capture_message(&msg, sentry::Level::Error);
            },
        );
    }

    fn capture_sentry_warning(&self) {
        sentry::with_scope(
            |scope| {
                scope.set_fingerprint(Some(&[self.operation, "operational"]));

                for (key, value) in &self.context {
                    scope.set_extra(*key, value.clone().into());
                }

                scope.set_tag("operation", self.operation);
                scope.set_tag("error_category", "operational");
            },
            || {
                let msg = format!(
                    "[{}] Operational issue: {}",
                    self.operation,
                    self.internal_message.as_deref().unwrap_or(&self.message)
                );
                sentry::capture_message(&msg, sentry::Level::Warning);
            },
        );
    }

    fn add_sentry_breadcrumb(&self) {
        sentry::add_breadcrumb(sentry::Breadcrumb {
            category: Some("expected_error".into()),
            message: Some(format!(
                "[{}] {} - {}",
                self.operation, self.status, self.message
            )),
            level: sentry::Level::Info,
            data: self
                .context
                .iter()
                .map(|(k, v)| (k.to_string(), v.clone().into()))
                .collect(),
            ..Default::default()
        });
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}: {}", self.operation, self.status, self.message)
    }
}

impl std::error::Error for AppError {}

#[derive(Serialize)]
struct ErrorResponse {
    error: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        // Report to Sentry based on category
        self.report_to_sentry();

        // Return appropriate response to user
        let body = Json(ErrorResponse {
            error: self.message.clone(),
            code: Some(self.operation.to_string()),
        });

        (self.status, body).into_response()
    }
}

// Convenience constructors for common error types
impl AppError {
    /// Bad request - invalid input from client (expected, not a bug)
    pub fn bad_request(operation: &'static str, message: impl Into<String>) -> Self {
        Self::expected(StatusCode::BAD_REQUEST, operation, message)
    }

    /// Not found (expected, not a bug)
    pub fn not_found(operation: &'static str, message: impl Into<String>) -> Self {
        Self::expected(StatusCode::NOT_FOUND, operation, message)
    }

    /// Unauthorized - auth failure (expected, not a bug)
    pub fn unauthorized(operation: &'static str, message: impl Into<String>) -> Self {
        Self::expected(StatusCode::UNAUTHORIZED, operation, message)
    }

    /// Forbidden - permission denied (expected, not a bug)
    pub fn forbidden(operation: &'static str, message: impl Into<String>) -> Self {
        Self::expected(StatusCode::FORBIDDEN, operation, message)
    }

    /// Unprocessable entity - semantic validation failure (expected)
    pub fn unprocessable(operation: &'static str, message: impl Into<String>) -> Self {
        Self::expected(StatusCode::UNPROCESSABLE_ENTITY, operation, message)
    }

    /// Database connection failure (bug - infrastructure issue)
    pub fn db_connection(operation: &'static str, err: impl fmt::Display) -> Self {
        Self::bug(operation, format!("Database connection failed: {}", err))
            .with_message("Database temporarily unavailable. Please try again.")
    }

    /// Database query failure (bug - likely a code issue)
    pub fn db_query(operation: &'static str, err: impl fmt::Display) -> Self {
        Self::bug(operation, format!("Database query failed: {}", err))
    }

    /// S3/storage failure (operational - external service issue)
    pub fn storage(
        operation: &'static str,
        err: impl fmt::Display,
        user_message: impl Into<String>,
    ) -> Self {
        Self::operational(
            StatusCode::SERVICE_UNAVAILABLE,
            operation,
            user_message,
            format!("Storage error: {}", err),
        )
    }

    /// Rate limited (expected, not a bug)
    pub fn rate_limited(operation: &'static str) -> Self {
        Self::expected(
            StatusCode::TOO_MANY_REQUESTS,
            operation,
            "Rate limit exceeded. Please try again later.",
        )
    }

    /// Internal processing error (bug)
    pub fn internal(operation: &'static str, err: impl fmt::Display) -> Self {
        Self::bug(operation, format!("Internal error: {}", err))
    }
}

/// Helper macro to quickly capture a bug with context
#[macro_export]
macro_rules! capture_bug {
    ($operation:expr, $msg:expr) => {
        $crate::error::AppError::bug($operation, $msg).report_to_sentry()
    };
    ($operation:expr, $msg:expr, $($key:expr => $value:expr),+) => {{
        let mut err = $crate::error::AppError::bug($operation, $msg);
        $(
            err = err.with_context($key, $value);
        )+
        err.report_to_sentry()
    }};
}

/// Helper macro to capture operational issues
#[macro_export]
macro_rules! capture_operational {
    ($operation:expr, $msg:expr) => {
        $crate::error::AppError::operational(
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            $operation,
            "Service temporarily unavailable",
            $msg,
        ).report_to_sentry()
    };
}

/// Log an expected error (only as breadcrumb, not captured to Sentry)
pub fn log_expected(operation: &'static str, status: StatusCode, message: &str) {
    AppError::expected(status, operation, message).add_sentry_breadcrumb();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bug_error_has_correct_status() {
        let err = AppError::bug("test_op", "something broke");
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.category, ErrorCategory::Bug);
    }

    #[test]
    fn test_expected_error_has_correct_category() {
        let err = AppError::bad_request("test_op", "invalid input");
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.category, ErrorCategory::Expected);
    }

    #[test]
    fn test_context_chaining() {
        let err = AppError::bug("test_op", "error")
            .with_context("deck_hash", "abc-def-ghi")
            .with_context("user_id", "123");
        assert_eq!(err.context.len(), 2);
    }
}
