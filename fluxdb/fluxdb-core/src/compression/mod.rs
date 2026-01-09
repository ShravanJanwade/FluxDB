//! Gorilla compression for time-series data
//!
//! Implements the compression algorithm from Facebook's paper:
//! "Gorilla: A Fast, Scalable, In-Memory Time Series Database"
//!
//! Achieves ~1.37 bytes per data point (vs 16 bytes raw).

mod encoder;
mod decoder;
mod bitstream;

pub use encoder::GorillaEncoder;
pub use decoder::GorillaDecoder;
pub use bitstream::{BitReader, BitWriter};

/// Compressed block of time-series data
#[derive(Debug, Clone)]
pub struct CompressedBlock {
    /// Compressed data
    pub data: Vec<u8>,
    /// Number of data points
    pub count: usize,
    /// First timestamp in block
    pub first_timestamp: i64,
    /// Last timestamp in block
    pub last_timestamp: i64,
}

impl CompressedBlock {
    /// Get compression ratio
    pub fn compression_ratio(&self, raw_size: usize) -> f64 {
        raw_size as f64 / self.data.len() as f64
    }

    /// Get bytes per point
    pub fn bytes_per_point(&self) -> f64 {
        self.data.len() as f64 / self.count as f64
    }
}

/// Compression configuration
#[derive(Debug, Clone, Copy)]
pub struct CompressionConfig {
    /// Maximum points per block
    pub block_size: usize,
    /// Whether to use LZ4 for additional compression
    pub use_lz4: bool,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            block_size: 1000,
            use_lz4: true,
        }
    }
}
