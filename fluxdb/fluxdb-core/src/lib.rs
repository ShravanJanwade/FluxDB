//! FluxDB single-node time-series database engine.
//!
//! WAL-backed writes, a synchronized skip-list memtable, typed LZ4 SSTables,
//! SQL scans/aggregations, retention, and manifest-based full compaction.
//! Performance is workload-dependent; see the benchmark script and architecture docs.

pub mod compaction;
pub mod compression;
pub mod memtable;
pub mod query;
pub mod sstable;
pub mod storage;
pub mod wal;

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
