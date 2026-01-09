//! Database - manages a single database instance

use crate::memtable::{ImmutableMemTable, MemTable};
use crate::query::{QueryExecutor, QueryParser, QueryPlan, QueryPlanner, QueryResult};
use crate::sstable::{SSTableBuilder, SSTableConfig, SSTableMeta, SSTableReader};
use crate::wal::{WalConfig, WalEntry, WalReader, WalWriter};
use crate::{DataPoint, Point, Result, FluxError, SeriesKey, TimeRange};
use parking_lot::{RwLock, Mutex};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{info, warn};

/// A single FluxDB database
pub struct Database {
    name: String,
    data_dir: PathBuf,
    
    // Write path
    wal: Arc<WalWriter>,
    memtable: Arc<RwLock<MemTable>>,
    immutable_memtables: Arc<Mutex<Vec<ImmutableMemTable>>>,
    
    // Read path
    sstables: Arc<RwLock<Vec<SSTableReader>>>,
    
    // Configuration
    memtable_size_limit: usize,
    sstable_config: SSTableConfig,
    
    // Counters
    next_memtable_id: AtomicU64,
    next_sstable_id: AtomicU64,
}

impl Database {
    /// Create or open a database
    pub fn open(
        name: &str,
        data_dir: PathBuf,
        wal_config: WalConfig,
        sstable_config: SSTableConfig,
        memtable_size_limit: usize,
    ) -> Result<Self> {
        let db_dir = data_dir.join(name);
        std::fs::create_dir_all(&db_dir)?;
        
        let wal_dir = db_dir.join("wal");
        let wal_config = WalConfig {
            dir: wal_dir,
            ..wal_config
        };
        
        // Open WAL
        let wal = Arc::new(WalWriter::new(wal_config.clone())?);
        
        // Create initial memtable
        let memtable = Arc::new(RwLock::new(MemTable::new(0)));
        
        // Load existing SSTables
        let sstables = Self::load_sstables(&db_dir)?;
        let next_sstable_id = sstables.iter()
            .map(|s| s.meta().id)
            .max()
            .unwrap_or(0) + 1;
        
        let db = Self {
            name: name.to_string(),
            data_dir: db_dir,
            wal,
            memtable,
            immutable_memtables: Arc::new(Mutex::new(Vec::new())),
            sstables: Arc::new(RwLock::new(sstables)),
            memtable_size_limit,
            sstable_config,
            next_memtable_id: AtomicU64::new(1),
            next_sstable_id: AtomicU64::new(next_sstable_id),
        };
        
        // Recover from WAL
        db.recover(wal_config)?;
        
        Ok(db)
    }

    /// Get database name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Write data points
    pub fn write(&self, points: &[Point]) -> Result<()> {
        // Write to WAL first
        let entry = WalEntry::write(&self.name, points)?;
        self.wal.append(&entry)?;
        
        // Then write to memtable
        {
            let memtable = self.memtable.read();
            memtable.insert_batch(points);
        }
        
        // Check if memtable needs flushing
        if self.memtable.read().should_flush(self.memtable_size_limit) {
            self.maybe_flush()?;
        }
        
        Ok(())
    }

    /// Query data
    pub fn query(&self, sql: &str) -> Result<QueryResult> {
        // Parse SQL
        let query = QueryParser::parse(sql)?;
        
        // Create plan
        let plan = QueryPlanner::plan(&query)?;
        
        // Collect data from all sources
        let data = self.collect_data(&plan)?;
        
        // Execute query
        QueryExecutor::execute(&plan, data)
    }

    /// Query a specific series
    pub fn query_series(
        &self,
        series_key: &SeriesKey,
        time_range: &TimeRange,
    ) -> Result<Vec<DataPoint>> {
        let mut results = Vec::new();
        
        // Query memtable
        {
            let memtable = self.memtable.read();
            results.extend(memtable.query(series_key, time_range));
        }
        
        // Query immutable memtables
        {
            let immutables = self.immutable_memtables.lock();
            for imm in immutables.iter() {
                results.extend(imm.query(series_key, time_range));
            }
        }
        
        // Query SSTables
        {
            let sstables = self.sstables.read();
            for sstable in sstables.iter() {
                if sstable.meta().overlaps_time(time_range.start, time_range.end) {
                    results.extend(sstable.query(series_key, time_range)?);
                }
            }
        }
        
        // Sort by timestamp
        results.sort_by_key(|p| p.timestamp);
        
        // Remove duplicates (keep latest)
        results.dedup_by(|a, b| a.timestamp == b.timestamp);
        
        Ok(results)
    }

    /// Get latest value for a series
    pub fn get_latest(&self, series_key: &SeriesKey) -> Result<Option<DataPoint>> {
        // Check memtable first (most recent)
        let memtable = self.memtable.read();
        if let Some(point) = memtable.get_latest(series_key) {
            return Ok(Some(point));
        }
        
        // Check immutable memtables
        let immutables = self.immutable_memtables.lock();
        for imm in immutables.iter().rev() {
            let points = imm.query(series_key, &TimeRange::new(i64::MIN, i64::MAX));
            if let Some(point) = points.last() {
                return Ok(Some(point.clone()));
            }
        }
        
        // Check SSTables from newest to oldest
        let sstables = self.sstables.read();
        for sstable in sstables.iter().rev() {
            let points = sstable.query(series_key, &TimeRange::new(i64::MIN, i64::MAX))?;
            if let Some(point) = points.last() {
                return Ok(Some(point.clone()));
            }
        }
        
        Ok(None)
    }

    /// Force flush memtable to disk
    pub fn flush(&self) -> Result<()> {
        self.maybe_flush()
    }

    /// Get database statistics
    pub fn stats(&self) -> DatabaseStats {
        let memtable_size = self.memtable.read().size();
        let immutable_count = self.immutable_memtables.lock().len();
        let sstable_count = self.sstables.read().len();
        let total_entries: usize = self.sstables.read()
            .iter()
            .map(|s| s.meta().entry_count)
            .sum();
        let total_size: u64 = self.sstables.read()
            .iter()
            .map(|s| s.meta().file_size)
            .sum();
        
        DatabaseStats {
            name: self.name.clone(),
            memtable_size,
            immutable_memtables: immutable_count,
            sstables: sstable_count,
            total_entries,
            total_size_bytes: total_size,
        }
    }

    fn collect_data(&self, plan: &QueryPlan) -> Result<Vec<(SeriesKey, DataPoint)>> {
        let mut data = Vec::new();
        let measurement = &plan.measurement;
        
        // Collect from memtable
        {
            let memtable = self.memtable.read();
            for (key, point) in memtable.iter() {
                if key.series_key.measurement == *measurement {
                    data.push((key.series_key.clone(), point.clone()));
                }
            }
        }
        
        // Collect from immutable memtables
        {
            let immutables = self.immutable_memtables.lock();
            for imm in immutables.iter() {
                for (key, point) in imm.iter() {
                    if key.series_key.measurement == *measurement {
                        data.push((key.series_key.clone(), point.clone()));
                    }
                }
            }
        }
        
        // Collect from SSTables
        {
            let sstables = self.sstables.read();
            for sstable in sstables.iter() {
                if !sstable.meta().overlaps_time(plan.time_range.start, plan.time_range.end) {
                    continue;
                }
                
                // This is a simplified implementation - in production,
                // we would use the bloom filter and index more efficiently
                let series_key = SeriesKey::new(measurement);
                let points = sstable.query(&series_key, &plan.time_range)?;
                for point in points {
                    data.push((series_key.clone(), point));
                }
            }
        }
        
        Ok(data)
    }

    fn maybe_flush(&self) -> Result<()> {
        let old_memtable;
        let new_id;
        
        {
            let mut memtable = self.memtable.write();
            if !memtable.should_flush(self.memtable_size_limit) {
                return Ok(());
            }
            
            new_id = self.next_memtable_id.fetch_add(1, Ordering::SeqCst);
            old_memtable = std::mem::replace(&mut *memtable, MemTable::new(new_id));
        }
        
        // Move to immutable
        let immutable = ImmutableMemTable::from(old_memtable);
        
        {
            let mut immutables = self.immutable_memtables.lock();
            immutables.push(immutable);
        }
        
        // Flush to SSTable (in production, this would be async)
        self.flush_immutable()?;
        
        Ok(())
    }

    fn flush_immutable(&self) -> Result<()> {
        let imm = {
            let mut immutables = self.immutable_memtables.lock();
            if immutables.is_empty() {
                return Ok(());
            }
            immutables.remove(0)
        };
        
        let sstable_id = self.next_sstable_id.fetch_add(1, Ordering::SeqCst);
        let sstable_path = self.data_dir.join(format!("sst_{:020}.flux", sstable_id));
        
        let meta = SSTableBuilder::build_from_memtable(
            sstable_path.clone(),
            sstable_id,
            0, // L0
            &imm,
            self.sstable_config.clone(),
        )?;
        
        info!("Flushed memtable {} to SSTable {}", imm.id(), sstable_id);
        
        // Open the new SSTable
        let reader = SSTableReader::open(sstable_path)?;
        
        {
            let mut sstables = self.sstables.write();
            sstables.push(reader);
        }
        
        // Truncate WAL
        let _ = self.wal.truncate_before(sstable_id);
        
        Ok(())
    }

    fn recover(&self, wal_config: WalConfig) -> Result<()> {
        let reader = WalReader::new(wal_config);
        let entries = reader.recover()?;
        
        if entries.is_empty() {
            return Ok(());
        }
        
        info!("Recovering {} WAL entries", entries.len());
        
        for entry in entries {
            if entry.database != self.name {
                continue;
            }
            
            let points = entry.get_points()?;
            let memtable = self.memtable.read();
            memtable.insert_batch(&points);
        }
        
        Ok(())
    }

    fn load_sstables(db_dir: &PathBuf) -> Result<Vec<SSTableReader>> {
        let mut sstables = Vec::new();
        
        if !db_dir.exists() {
            return Ok(sstables);
        }
        
        for entry in std::fs::read_dir(db_dir)? {
            let entry = entry?;
            let path = entry.path();
            
            if let Some(ext) = path.extension() {
                if ext == "flux" {
                    match SSTableReader::open(path.clone()) {
                        Ok(reader) => sstables.push(reader),
                        Err(e) => warn!("Failed to open SSTable {:?}: {}", path, e),
                    }
                }
            }
        }
        
        // Sort by ID (oldest first)
        sstables.sort_by_key(|s| s.meta().id);
        
        Ok(sstables)
    }
}

/// Database statistics
#[derive(Debug, Clone)]
pub struct DatabaseStats {
    pub name: String,
    pub memtable_size: usize,
    pub immutable_memtables: usize,
    pub sstables: usize,
    pub total_entries: usize,
    pub total_size_bytes: u64,
}
