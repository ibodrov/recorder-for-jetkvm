//! Typed errors with stable protocol codes.
//!
//! Internal code keeps using `anyhow` for diagnostics; errors that are
//! meaningful to protocol callers are constructed as [`CodedError`] so the
//! stdio protocol can map them to stable codes instead of string matching.

use std::fmt;

/// Externally meaningful protocol error codes.
pub mod codes {
    pub const APPROVAL_REQUIRED: &str = "approval_required";
    pub const INVALID_PARAMS: &str = "invalid_params";
    pub const NOT_CONNECTED: &str = "not_connected";
    pub const STALE_GENERATION: &str = "stale_generation";
    pub const NOT_CANCELLABLE: &str = "not_cancellable";
    pub const CANCELLED: &str = "cancelled";
    pub const TIMEOUT: &str = "timeout";
    pub const UNSUPPORTED: &str = "unsupported";
    pub const SERVER_BUSY: &str = "server_busy";
    pub const OPERATION_FAILED: &str = "operation_failed";
}

/// An error carrying a stable protocol code.
#[derive(Debug)]
pub struct CodedError {
    code: &'static str,
    message: String,
}

impl CodedError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    pub fn code(&self) -> &'static str {
        self.code
    }
}

impl fmt::Display for CodedError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for CodedError {}

/// Extracts the protocol code from an error chain, falling back to
/// `operation_failed` for uncoded errors.
pub fn error_code(error: &anyhow::Error) -> &'static str {
    error
        .chain()
        .find_map(|cause| cause.downcast_ref::<CodedError>())
        .map_or(codes::OPERATION_FAILED, CodedError::code)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn code_survives_anyhow_context_wrapping() {
        let error = anyhow::Error::new(CodedError::new(
            codes::STALE_GENERATION,
            "frame cursor is stale",
        ))
        .context("snapshot failed");
        assert_eq!(error_code(&error), codes::STALE_GENERATION);
    }

    #[test]
    fn uncoded_errors_fall_back_to_operation_failed() {
        let error = anyhow::anyhow!("disk full");
        assert_eq!(error_code(&error), codes::OPERATION_FAILED);
    }
}
