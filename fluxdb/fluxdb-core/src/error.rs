//! Error types for FluxDB

use thiserror::Error;

/// Result type alias for FluxDB operations
pub type Result<T> = std::result::Result<T, FluxError>;

/// FluxDB error types
#[derive(Error, Debug)]
pub enum FluxError {
    /// IO operation failed
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Data corruption detected
    #[error("Data corruption: {0}")]
    Corruption(String),

    /// Checksum mismatch
    #[error("Checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    /// Invalid data format
    #[error("Invalid format: {0}")]
    InvalidFormat(String),

    /// Compression/decompression error
    #[error("Compression error: {0}")]
    Compression(String),

    /// Query error
    #[error("Query error: {0}")]
    Query(String),

    /// SQL parsing error
    #[error("SQL parse error: {0}")]
    SqlParse(String),

    /// Configuration error
    #[error("Configuration error: {0}")]
    Config(String),

    /// Database not found
    #[error("Database not found: {0}")]
    DatabaseNotFound(String),

    /// Measurement not found
    #[error("Measurement not found: {0}")]
    MeasurementNotFound(String),

    /// WAL recovery error
    #[error("WAL recovery error: {0}")]
    WalRecovery(String),

    /// Compaction error
    #[error("Compaction error: {0}")]
    Compaction(String),

    /// Internal error
    #[error("Internal error: {0}")]
    Internal(String),
}

impl FluxError {
    /// Check if error is retryable
    pub fn is_retryable(&self) -> bool {
        matches!(self, FluxError::Io(_))
    }

    /// Check if error indicates corruption
    pub fn is_corruption(&self) -> bool {
        matches!(
            self,
            FluxError::Corruption(_) | FluxError::ChecksumMismatch { .. }
        )
    }
}
