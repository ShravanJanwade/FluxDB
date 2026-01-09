//! SSTable (Sorted String Table) implementation
//!
//! Immutable on-disk storage for time-series data with:
//! - Block-based format with compression
//! - Sparse index for fast lookups
//! - Bloom filters for existence checks

mod block;
mod builder;
mod reader;
mod bloom;

pub use block::{DataBlock, BlockHeader};
pub use builder::SSTableBuilder;
pub use reader::SSTableReader;
pub use bloom::BloomFilter;

use crate::{SeriesKey, Timestamp};
use std::path::PathBuf;

/// SSTable file format version
pub const FORMAT_VERSION: u32 = 1;

/// SSTable metadata
#[derive(Debug, Clone)]
pub struct SSTableMeta {
    /// File path
    pub path: PathBuf,
    /// Unique ID
    pub id: u64,
    /// Level in LSM tree
    pub level: u32,
    /// Number of entries
    pub entry_count: usize,
    /// File size in bytes
    pub file_size: u64,
    /// Minimum timestamp
    pub min_timestamp: Timestamp,
    /// Maximum timestamp
    pub max_timestamp: Timestamp,
    /// Minimum key
    pub min_key: SeriesKey,
    /// Maximum key
    pub max_key: SeriesKey,
}

impl SSTableMeta {
    /// Check if the SSTable may contain data in time range
    pub fn overlaps_time(&self, start: Timestamp, end: Timestamp) -> bool {
        self.min_timestamp <= end && self.max_timestamp >= start
    }

    /// Check if the SSTable may contain data for a series
    pub fn may_contain_series(&self, key: &SeriesKey) -> bool {
        key >= &self.min_key && key <= &self.max_key
    }
}

/// SSTable configuration
#[derive(Debug, Clone)]
pub struct SSTableConfig {
    /// Block size in bytes
    pub block_size: usize,
    /// Enable compression
    pub compression: bool,
    /// Bloom filter bits per key
    pub bloom_bits_per_key: usize,
}

impl Default for SSTableConfig {
    fn default() -> Self {
        Self {
            block_size: 4096,
            compression: true,
            bloom_bits_per_key: 10,
        }
    }
}
