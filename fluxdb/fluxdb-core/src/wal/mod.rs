//! Write-Ahead Log (WAL) implementation
//!
//! The WAL provides durability by writing all changes to disk before
//! committing them to memory. In case of crashes, the WAL can be
//! replayed to recover the database state.

mod entry;
mod reader;
mod writer;

pub use entry::{WalEntry, WalEntryType};
pub use reader::WalReader;
pub use writer::WalWriter;

use std::path::PathBuf;

/// WAL sync policy
#[derive(Debug, Clone, Copy)]
pub enum SyncPolicy {
    /// Sync after every write (safest, slowest)
    Immediate,
    /// Sync after N writes
    EveryN(usize),
    /// Sync on interval (trades durability for performance)
    Interval { millis: u64 },
    /// Never sync (OS decides, fastest, least safe)
    None,
}

impl Default for SyncPolicy {
    fn default() -> Self {
        SyncPolicy::Immediate
    }
}

/// WAL configuration
#[derive(Debug, Clone)]
pub struct WalConfig {
    /// Directory for WAL files
    pub dir: PathBuf,
    /// Sync policy
    pub sync_policy: SyncPolicy,
    /// Maximum segment size in bytes
    pub segment_size: usize,
}

impl Default for WalConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("data/wal"),
            sync_policy: SyncPolicy::default(),
            segment_size: crate::config::WAL_SEGMENT_SIZE,
        }
    }
}
