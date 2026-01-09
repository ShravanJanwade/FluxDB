//! Storage engine - coordinates all storage components

mod engine;
mod database;

pub use engine::StorageEngine;
pub use database::Database;

use crate::sstable::SSTableConfig;
use crate::wal::WalConfig;
use std::path::PathBuf;

/// Storage engine configuration
#[derive(Debug, Clone)]
pub struct StorageConfig {
    /// Data directory
    pub data_dir: PathBuf,
    /// WAL configuration
    pub wal: WalConfig,
    /// SSTable configuration
    pub sstable: SSTableConfig,
    /// MemTable size limit in bytes
    pub memtable_size_limit: usize,
    /// L0 compaction trigger (number of files)
    pub l0_compaction_trigger: usize,
    /// Level size multiplier
    pub level_size_multiplier: usize,
    /// Maximum number of levels
    pub max_levels: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            wal: WalConfig::default(),
            sstable: SSTableConfig::default(),
            memtable_size_limit: crate::config::MEMTABLE_SIZE_LIMIT,
            l0_compaction_trigger: crate::config::L0_COMPACTION_TRIGGER,
            level_size_multiplier: crate::config::LEVEL_SIZE_RATIO,
            max_levels: 7,
        }
    }
}
