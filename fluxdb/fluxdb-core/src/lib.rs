//! FluxDB Core - High-Performance Time-Series Database Engine
//!
//! A Rust-based time-series database optimized for:
//! - High write throughput (500K+ writes/second)
//! - Efficient compression (8-10x using Gorilla algorithm)
//! - Fast range queries (sub-50ms for million-point ranges)
//!
//! # Architecture
//!
//! FluxDB uses an LSM-tree based storage engine with the following components:
//!
//! - **WAL (Write-Ahead Log)**: Durability guarantee through sequential writes
//! - **MemTable**: In-memory skip-list for fast writes
//! - **SSTable**: Immutable sorted files on disk with compression
//! - **Compaction**: Background merging to reduce read amplification

pub mod compression;
pub mod memtable;
pub mod query;
pub mod sstable;
pub mod storage;
pub mod wal;
pub mod compaction;

mod error;
mod types;

pub use error::{FluxError, Result};
pub use types::*;

/// FluxDB version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// Default configuration values
pub mod config {
    /// Maximum MemTable size before flush (64MB)
    pub const MEMTABLE_SIZE_LIMIT: usize = 64 * 1024 * 1024;
    
    /// SSTable block size (4KB)
    pub const BLOCK_SIZE: usize = 4 * 1024;
    
    /// Maximum SSTables in L0 before compaction
    pub const L0_COMPACTION_TRIGGER: usize = 4;
    
    /// Size ratio between levels
    pub const LEVEL_SIZE_RATIO: usize = 10;
    
    /// WAL segment size (16MB)
    pub const WAL_SEGMENT_SIZE: usize = 16 * 1024 * 1024;
    
    /// Bloom filter false positive rate
    pub const BLOOM_FP_RATE: f64 = 0.01;
}
