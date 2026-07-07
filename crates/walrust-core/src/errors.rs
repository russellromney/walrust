//! Structured error types and exit codes for walrust
//!
//! Exit codes:
//! - 0: Success
//! - 1: General/unknown error
//! - 2: Configuration error (invalid config file, missing CLI args)
//! - 3: Database error (file not found, WAL corruption, SQLite issues)
//! - 4: S3 error (network, authentication, bucket access)
//! - 5: Integrity error (checksum mismatch, LTX verification failed)
//! - 6: Restore error (no snapshot found, PITR unavailable)

use std::fmt;
use std::process::ExitCode;

/// Exit codes for different error categories
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ExitStatus {
    Success = 0,
    General = 1,
    Config = 2,
    Database = 3,
    S3 = 4,
    Integrity = 5,
    Restore = 6,
}

impl From<ExitStatus> for ExitCode {
    fn from(status: ExitStatus) -> ExitCode {
        ExitCode::from(status as u8)
    }
}

/// Structured error type for walrust operations
#[derive(Debug)]
pub enum WalrustError {
    /// Configuration errors (invalid config file, missing CLI args)
    Config(String),
    /// Database errors (file not found, WAL corruption, SQLite issues)
    Database(String),
    /// S3 errors (network, authentication, bucket access)
    S3(String),
    /// Integrity errors (checksum mismatch, LTX verification failed)
    Integrity(String),
    /// Restore errors (no snapshot found, PITR unavailable)
    Restore(String),
    /// General/unknown errors
    General(String),
}

impl WalrustError {
    /// Get the exit code for this error type
    pub fn exit_status(&self) -> ExitStatus {
        match self {
            WalrustError::Config(_) => ExitStatus::Config,
            WalrustError::Database(_) => ExitStatus::Database,
            WalrustError::S3(_) => ExitStatus::S3,
            WalrustError::Integrity(_) => ExitStatus::Integrity,
            WalrustError::Restore(_) => ExitStatus::Restore,
            WalrustError::General(_) => ExitStatus::General,
        }
    }

    /// Create a config error
    pub fn config(msg: impl Into<String>) -> Self {
        WalrustError::Config(msg.into())
    }

    /// Create a database error
    pub fn database(msg: impl Into<String>) -> Self {
        WalrustError::Database(msg.into())
    }

    /// Create an S3 error
    pub fn s3(msg: impl Into<String>) -> Self {
        WalrustError::S3(msg.into())
    }

    /// Create an integrity error
    pub fn integrity(msg: impl Into<String>) -> Self {
        WalrustError::Integrity(msg.into())
    }

    /// Create a restore error
    pub fn restore(msg: impl Into<String>) -> Self {
        WalrustError::Restore(msg.into())
    }

    /// Create a general error
    pub fn general(msg: impl Into<String>) -> Self {
        WalrustError::General(msg.into())
    }
}

impl fmt::Display for WalrustError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalrustError::Config(msg) => write!(f, "configuration error: {}", msg),
            WalrustError::Database(msg) => write!(f, "database error: {}", msg),
            WalrustError::S3(msg) => write!(f, "S3 error: {}", msg),
            WalrustError::Integrity(msg) => write!(f, "integrity error: {}", msg),
            WalrustError::Restore(msg) => write!(f, "restore error: {}", msg),
            WalrustError::General(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for WalrustError {}

/// Classify an anyhow error into an exit status from typed walrust errors.
pub fn classify_error(err: &anyhow::Error) -> ExitStatus {
    for cause in err.chain() {
        if let Some(err) = cause.downcast_ref::<WalrustError>() {
            return err.exit_status();
        }
    }

    ExitStatus::General
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::anyhow;

    #[test]
    fn test_exit_status_values() {
        assert_eq!(ExitStatus::Success as u8, 0);
        assert_eq!(ExitStatus::General as u8, 1);
        assert_eq!(ExitStatus::Config as u8, 2);
        assert_eq!(ExitStatus::Database as u8, 3);
        assert_eq!(ExitStatus::S3 as u8, 4);
        assert_eq!(ExitStatus::Integrity as u8, 5);
        assert_eq!(ExitStatus::Restore as u8, 6);
    }

    #[test]
    fn test_walrust_error_exit_status() {
        assert_eq!(
            WalrustError::config("test").exit_status(),
            ExitStatus::Config
        );
        assert_eq!(
            WalrustError::database("test").exit_status(),
            ExitStatus::Database
        );
        assert_eq!(WalrustError::s3("test").exit_status(), ExitStatus::S3);
        assert_eq!(
            WalrustError::integrity("test").exit_status(),
            ExitStatus::Integrity
        );
        assert_eq!(
            WalrustError::restore("test").exit_status(),
            ExitStatus::Restore
        );
        assert_eq!(
            WalrustError::general("test").exit_status(),
            ExitStatus::General
        );
    }

    #[test]
    fn test_classify_config_errors() {
        let err = anyhow::Error::new(WalrustError::config("--bucket is required"));
        assert_eq!(classify_error(&err), ExitStatus::Config);
    }

    #[test]
    fn test_classify_database_errors() {
        let err = anyhow::Error::new(WalrustError::database("database unavailable"));
        assert_eq!(classify_error(&err), ExitStatus::Database);
    }

    #[test]
    fn test_classify_s3_errors() {
        let err = anyhow::Error::new(WalrustError::s3("upload failed"));
        assert_eq!(classify_error(&err), ExitStatus::S3);
    }

    #[test]
    fn test_classify_integrity_errors() {
        let err = anyhow::Error::new(WalrustError::integrity("chain broken"))
            .context("while verifying backup");
        assert_eq!(classify_error(&err), ExitStatus::Integrity);
    }

    #[test]
    fn test_classify_restore_errors() {
        let err = anyhow::Error::new(WalrustError::restore("snapshot unavailable"));
        assert_eq!(classify_error(&err), ExitStatus::Restore);
    }

    #[test]
    fn test_classify_general_errors() {
        let err = anyhow!("Something unexpected happened");
        assert_eq!(classify_error(&err), ExitStatus::General);
    }

    #[test]
    fn test_untyped_messages_are_not_classified_by_substring() {
        let err = anyhow!("Checksum mismatch: expected 0x123, got 0x456");
        assert_eq!(classify_error(&err), ExitStatus::General);
    }

    #[test]
    fn test_walrust_error_display() {
        assert_eq!(
            WalrustError::config("missing bucket").to_string(),
            "configuration error: missing bucket"
        );
        assert_eq!(
            WalrustError::s3("connection failed").to_string(),
            "S3 error: connection failed"
        );
        assert_eq!(WalrustError::general("oops").to_string(), "oops");
    }
}
