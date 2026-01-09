//! Background compaction for LSM tree

use crate::sstable::{SSTableBuilder, SSTableConfig, SSTableMeta, SSTableReader};
use crate::{Result, FluxError, DataPoint, SeriesKey};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{info, warn};

/// Compaction task type
#[derive(Debug)]
pub enum CompactionTask {
    /// Compact L0 files to L1
    L0ToL1 {
        l0_files: Vec<SSTableMeta>,
        l1_files: Vec<SSTableMeta>,
    },
    /// Compact files from one level to the next
    LevelToLevel {
        source_level: u32,
        source_files: Vec<SSTableMeta>,
        target_level: u32,
        target_files: Vec<SSTableMeta>,
    },
}

/// Compaction scheduler
pub struct CompactionScheduler {
    data_dir: PathBuf,
    config: CompactionConfig,
    levels: RwLock<Vec<Level>>,
    task_tx: Option<mpsc::Sender<CompactionTask>>,
}

/// Level in LSM tree
#[derive(Debug)]
pub struct Level {
    pub level: u32,
    pub files: Vec<SSTableMeta>,
    pub size_bytes: u64,
}

/// Compaction configuration
#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Maximum files in L0 before triggering compaction
    pub l0_file_trigger: usize,
    /// Size multiplier between levels
    pub level_size_multiplier: u64,
    /// Base level size (L1) in bytes
    pub base_level_size: u64,
    /// Maximum levels
    pub max_levels: usize,
    /// SSTable configuration
    pub sstable_config: SSTableConfig,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            l0_file_trigger: 4,
            level_size_multiplier: 10,
            base_level_size: 64 * 1024 * 1024, // 64MB
            max_levels: 7,
            sstable_config: SSTableConfig::default(),
        }
    }
}

impl CompactionScheduler {
    /// Create a new compaction scheduler
    pub fn new(data_dir: PathBuf, config: CompactionConfig) -> Self {
        let mut levels = Vec::with_capacity(config.max_levels);
        for i in 0..config.max_levels {
            levels.push(Level {
                level: i as u32,
                files: Vec::new(),
                size_bytes: 0,
            });
        }

        Self {
            data_dir,
            config,
            levels: RwLock::new(levels),
            task_tx: None,
        }
    }

    /// Add a new SSTable to L0
    pub fn add_l0_file(&self, meta: SSTableMeta) {
        let mut levels = self.levels.write();
        let size = meta.file_size;
        levels[0].files.push(meta);
        levels[0].size_bytes += size;
    }

    /// Check if compaction is needed and return task
    pub fn select_compaction(&self) -> Option<CompactionTask> {
        let levels = self.levels.read();

        // Check L0 file count
        if levels[0].files.len() >= self.config.l0_file_trigger {
            return Some(CompactionTask::L0ToL1 {
                l0_files: levels[0].files.clone(),
                l1_files: levels[1].files.clone(),
            });
        }

        // Check level sizes
        for (i, level) in levels.iter().enumerate().skip(1) {
            let target_size = self.target_size_for_level(i);
            if level.size_bytes > target_size && i + 1 < self.config.max_levels {
                // Pick file with most overlap to next level
                if let Some(file) = self.pick_file_to_compact(level) {
                    let overlapping = self.find_overlapping(&levels[i + 1], &file);
                    return Some(CompactionTask::LevelToLevel {
                        source_level: i as u32,
                        source_files: vec![file],
                        target_level: (i + 1) as u32,
                        target_files: overlapping,
                    });
                }
            }
        }

        None
    }

    /// Execute a compaction task
    pub async fn execute(&self, task: CompactionTask) -> Result<Vec<SSTableMeta>> {
        match task {
            CompactionTask::L0ToL1 { l0_files, l1_files } => {
                self.compact_l0_to_l1(l0_files, l1_files).await
            }
            CompactionTask::LevelToLevel {
                source_level,
                source_files,
                target_level,
                target_files,
            } => {
                self.compact_level_to_level(
                    source_level,
                    source_files,
                    target_level,
                    target_files,
                ).await
            }
        }
    }

    async fn compact_l0_to_l1(
        &self,
        l0_files: Vec<SSTableMeta>,
        l1_files: Vec<SSTableMeta>,
    ) -> Result<Vec<SSTableMeta>> {
        info!(
            "Compacting {} L0 files with {} L1 files",
            l0_files.len(),
            l1_files.len()
        );

        // Merge all files
        let mut all_files = l0_files.clone();
        all_files.extend(l1_files.clone());

        // Read all data
        let merged_data = self.merge_files(&all_files)?;

        // Write new L1 files
        let new_files = self.write_level_files(1, merged_data)?;

        // Update levels
        {
            let mut levels = self.levels.write();
            
            // Remove old L0 files
            levels[0].files.clear();
            levels[0].size_bytes = 0;
            
            // Remove overlapping L1 files and add new ones
            levels[1].files.retain(|f| {
                !l1_files.iter().any(|old| old.id == f.id)
            });
            for meta in &new_files {
                levels[1].files.push(meta.clone());
                levels[1].size_bytes += meta.file_size;
            }
        }

        // Delete old files
        for meta in l0_files.iter().chain(l1_files.iter()) {
            if let Err(e) = std::fs::remove_file(&meta.path) {
                warn!("Failed to delete old SSTable {:?}: {}", meta.path, e);
            }
        }

        Ok(new_files)
    }

    async fn compact_level_to_level(
        &self,
        source_level: u32,
        source_files: Vec<SSTableMeta>,
        target_level: u32,
        target_files: Vec<SSTableMeta>,
    ) -> Result<Vec<SSTableMeta>> {
        info!(
            "Compacting {} L{} files with {} L{} files",
            source_files.len(),
            source_level,
            target_files.len(),
            target_level
        );

        // Merge files
        let mut all_files = source_files.clone();
        all_files.extend(target_files.clone());
        let merged_data = self.merge_files(&all_files)?;

        // Write new files
        let new_files = self.write_level_files(target_level, merged_data)?;

        // Update levels
        {
            let mut levels = self.levels.write();
            
            // Remove source files
            levels[source_level as usize].files.retain(|f| {
                !source_files.iter().any(|old| old.id == f.id)
            });
            
            // Remove overlapping target files and add new ones
            levels[target_level as usize].files.retain(|f| {
                !target_files.iter().any(|old| old.id == f.id)
            });
            
            for meta in &new_files {
                levels[target_level as usize].files.push(meta.clone());
                levels[target_level as usize].size_bytes += meta.file_size;
            }
        }

        // Delete old files
        for meta in source_files.iter().chain(target_files.iter()) {
            let _ = std::fs::remove_file(&meta.path);
        }

        Ok(new_files)
    }

    fn merge_files(
        &self,
        files: &[SSTableMeta],
    ) -> Result<BTreeMap<(SeriesKey, i64), DataPoint>> {
        let mut merged: BTreeMap<(SeriesKey, i64), DataPoint> = BTreeMap::new();

        for meta in files {
            let reader = SSTableReader::open(meta.path.clone())?;
            // In a real implementation, we'd iterate through all data
            // For now, this is simplified
        }

        Ok(merged)
    }

    fn write_level_files(
        &self,
        level: u32,
        data: BTreeMap<(SeriesKey, i64), DataPoint>,
    ) -> Result<Vec<SSTableMeta>> {
        // This is a simplified implementation
        // In production, we'd split into multiple files of target size
        Ok(vec![])
    }

    fn target_size_for_level(&self, level: usize) -> u64 {
        self.config.base_level_size * self.config.level_size_multiplier.pow(level as u32 - 1)
    }

    fn pick_file_to_compact(&self, level: &Level) -> Option<SSTableMeta> {
        // Simple strategy: pick oldest file
        level.files.first().cloned()
    }

    fn find_overlapping(&self, level: &Level, file: &SSTableMeta) -> Vec<SSTableMeta> {
        level
            .files
            .iter()
            .filter(|f| {
                f.max_key >= file.min_key && f.min_key <= file.max_key
            })
            .cloned()
            .collect()
    }
}
