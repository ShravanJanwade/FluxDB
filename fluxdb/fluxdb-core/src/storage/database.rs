//! Database - manages a single database instance

use crate::memtable::{ImmutableMemTable, MemTable};
use crate::query::{QueryExecutor, QueryParser, QueryPlan, QueryPlanner, QueryResult};
use crate::sstable::{SSTableBuilder, SSTableConfig, SSTableReader};
use crate::wal::{WalConfig, WalEntry, WalReader, WalWriter};
use crate::{DataPoint, FluxError, Point, Result, SeriesKey, TimeRange};
use parking_lot::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::info;

/// A single FluxDB database
pub struct Database {
    _directory_lock: std::fs::File,
    operation: RwLock<()>,
    retention_seconds: AtomicU64,
    name: String,
    data_dir: PathBuf,

    // Write path
    wal: Arc<WalWriter>,
    memtable: Arc<RwLock<MemTable>>,
    immutable_memtables: Arc<Mutex<Vec<Arc<ImmutableMemTable>>>>,

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
        super::StorageEngine::validate_name(name)?;
        let db_dir = data_dir.join(name);
        std::fs::create_dir_all(&db_dir)?;

        let directory_lock = std::fs::File::options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(db_dir.join(".lock"))?;
        directory_lock
            .try_lock()
            .map_err(|e| FluxError::Config(format!("Database is already open: {e}")))?;
        let wal_dir = db_dir.join("wal");
        let wal_config = WalConfig {
            dir: wal_dir,
            ..wal_config
        };

        let manifest = Self::manifest(&db_dir)?;
        let retention = match std::fs::read_to_string(db_dir.join("retention.json")) {
            Ok(s) => serde_json::from_str(&s)
                .map_err(|e| FluxError::Config(format!("Invalid retention policy: {e}")))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e.into()),
        };
        // Validate and repair a torn tail before opening the append handle.
        WalReader::new(wal_config.clone()).recover_from(manifest.1)?;
        // Open WAL
        let wal = Arc::new(WalWriter::new(wal_config.clone())?);

        // Create initial memtable
        let memtable = Arc::new(RwLock::new(MemTable::new(0)));

        // Load existing SSTables
        let sstables = Self::load_sstables(&db_dir, manifest.0)?;
        let next_sstable_id = sstables.iter().map(|s| s.meta().id).max().unwrap_or(0) + 1;

        let db = Self {
            _directory_lock: directory_lock,
            operation: RwLock::new(()),
            retention_seconds: AtomicU64::new(retention),
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
        db.recover(wal_config, manifest.1)?;

        Ok(db)
    }

    /// Get database name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Write data points
    pub fn write(&self, points: &[Point]) -> Result<()> {
        let _guard = self.operation.write();
        if points.is_empty() {
            return Err(FluxError::Config("No points supplied".into()));
        }
        let mut existing = BTreeMap::new();
        for point in points {
            let key = (point.key.clone(), point.data.timestamp);
            if let std::collections::btree_map::Entry::Vacant(entry) = existing.entry(key) {
                if let Some(previous) = self.lookup(&point.key, point.data.timestamp)? {
                    entry.insert(Point::new(point.key.clone(), previous));
                }
            }
        }
        let mut merged = Vec::new();
        for point in points {
            if point
                .data
                .fields
                .iter()
                .any(|(_, v)| matches!(v, crate::FieldValue::Float(f) if !f.is_finite()))
            {
                return Err(FluxError::Config(
                    "Nonfinite fields are not supported".into(),
                ));
            }
            if point.key.measurement.is_empty() || point.data.fields.0.is_empty() {
                return Err(FluxError::Config(
                    "Measurement and fields are required".into(),
                ));
            }
            let key = (point.key.clone(), point.data.timestamp);
            let mut next = existing.get(&key).cloned().unwrap_or_else(|| point.clone());
            next.data.fields.0.extend(point.data.fields.0.clone());
            existing.insert(key, next.clone());
            merged.push(next);
        }
        self.persist(&merged)?;
        if self.memtable.read().should_flush(self.memtable_size_limit) {
            self.maybe_flush()?;
        }
        if self.sstables.read().len() >= 8 {
            self.compact_locked()?;
        }
        Ok(())
    }

    fn lookup(&self, key: &SeriesKey, timestamp: i64) -> Result<Option<DataPoint>> {
        let range = TimeRange::new(timestamp, timestamp);
        if let Some(p) = self.memtable.read().query(key, &range).pop() {
            return Ok(if p.fields.0.is_empty() { None } else { Some(p) });
        }
        for table in self.immutable_memtables.lock().iter().rev() {
            if let Some(p) = table.query(key, &range).pop() {
                return Ok(if p.fields.0.is_empty() { None } else { Some(p) });
            }
        }
        for table in self.sstables.read().iter().rev() {
            if let Some(p) = table.query(key, &range)?.pop() {
                return Ok(if p.fields.0.is_empty() { None } else { Some(p) });
            }
        }
        Ok(None)
    }

    fn persist(&self, points: &[Point]) -> Result<()> {
        self.wal.append(&WalEntry::write(&self.name, points)?)?;
        self.memtable.read().insert_batch(points);
        Ok(())
    }

    /// Inclusive time-range deletion. Exact tombstones are WAL-durable before visibility.
    pub fn delete(
        &self,
        measurement: &str,
        tags: &BTreeMap<String, String>,
        range: TimeRange,
    ) -> Result<usize> {
        self.delete_matching(measurement, tags, range, false)
    }

    pub fn delete_matching(
        &self,
        measurement: &str,
        tags: &BTreeMap<String, String>,
        range: TimeRange,
        exact: bool,
    ) -> Result<usize> {
        let _guard = self.operation.write();
        let tombstones: Vec<_> = self
            .all_data()?
            .into_iter()
            .filter(|p| {
                p.key.measurement == measurement
                    && range.contains(p.data.timestamp)
                    && (!exact || &p.key.tags == tags)
                    && tags.iter().all(|(k, v)| p.key.tags.get(k) == Some(v))
            })
            .map(|mut p| {
                p.data.fields.0.clear();
                p
            })
            .collect();
        if !tombstones.is_empty() {
            self.persist(&tombstones)?;
        }
        Ok(tombstones.len())
    }

    pub fn points(&self) -> Result<Vec<Point>> {
        let _guard = self.operation.read();
        self.all_data()
    }

    fn all_data(&self) -> Result<Vec<Point>> {
        let mut latest = BTreeMap::new();
        let mut apply = |point: Point| {
            latest.insert((point.key.clone(), point.data.timestamp), point);
        };
        for table in self.sstables.read().iter() {
            for point in table.all_points()? {
                apply(point);
            }
        }
        for table in self.immutable_memtables.lock().iter() {
            for (k, p) in table.iter() {
                apply(Point::new(k.series_key, p));
            }
        }
        for (k, p) in self.memtable.read().iter() {
            apply(Point::new(k.series_key, p));
        }
        Ok(latest
            .into_values()
            .filter(|p| !p.data.fields.0.is_empty())
            .collect())
    }

    pub fn retention_seconds(&self) -> u64 {
        self.retention_seconds.load(Ordering::Relaxed)
    }
    pub fn set_retention(&self, seconds: u64) -> Result<()> {
        if seconds > 315_360_000 {
            return Err(FluxError::Config(
                "Retention must be at most ten years; use 0 for unlimited".into(),
            ));
        }
        let _guard = self.operation.write();
        self.atomic_metadata("retention.json", &serde_json::to_vec(&seconds).unwrap())?;
        self.retention_seconds.store(seconds, Ordering::Relaxed);
        Ok(())
    }
    pub fn enforce_retention(&self, now_ns: i64) -> Result<usize> {
        let _guard = self.operation.write();
        let seconds = self.retention_seconds();
        if seconds == 0 {
            return Ok(0);
        }
        let cutoff = now_ns.saturating_sub((seconds as i64).saturating_mul(1_000_000_000));
        let expired: Vec<_> = self
            .all_data()?
            .into_iter()
            .filter(|p| p.data.timestamp < cutoff)
            .map(|mut p| {
                p.data.fields.0.clear();
                p
            })
            .collect();
        if !expired.is_empty() {
            self.persist(&expired)?;
        }
        Ok(expired.len())
    }
    /// Full compaction commits a snapshot plus the first required WAL segment atomically.
    pub fn compact(&self) -> Result<()> {
        let _guard = self.operation.write();
        self.compact_locked()
    }
    fn compact_locked(&self) -> Result<()> {
        let points = self.all_data()?;
        let floor = self.wal.checkpoint_segment()?;
        let id = self.next_sstable_id.fetch_add(1, Ordering::SeqCst);
        let path = self.data_dir.join(format!("sst_{id:020}.flux"));
        let mut builder = SSTableBuilder::new(path.clone(), id, 0, self.sstable_config.clone());
        for point in points {
            builder.add(&point.key, &point.data)?;
        }
        builder.finish()?;
        let reader = SSTableReader::open(path)?;
        self.atomic_metadata("manifest.json", &serde_json::to_vec(&(id, floor)).unwrap())?;
        *self.sstables.write() = vec![reader];
        *self.memtable.write() =
            MemTable::new(self.next_memtable_id.fetch_add(1, Ordering::SeqCst));
        self.immutable_memtables.lock().clear();
        // Obsolete files are safe to reclaim after the manifest commit. Cleanup is retryable.
        if let Err(e) = self.wal.truncate_before(floor) {
            tracing::warn!("WAL cleanup: {e}");
        }
        for entry in std::fs::read_dir(&self.data_dir)? {
            let path = entry?.path();
            let old = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.strip_prefix("sst_"))
                .and_then(|s| s.parse::<u64>().ok());
            if path.extension().and_then(|s| s.to_str()) == Some("flux")
                && old.map(|old| old < id).unwrap_or(false)
            {
                if let Err(e) = std::fs::remove_file(path) {
                    tracing::warn!("SSTable cleanup: {e}");
                }
            }
        }
        Ok(())
    }
    fn atomic_metadata(&self, name: &str, bytes: &[u8]) -> Result<()> {
        use std::io::Write;
        let pending = self.data_dir.join(format!("{name}.pending"));
        let mut file = std::fs::File::create(&pending)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(pending, self.data_dir.join(name))?;
        #[cfg(unix)]
        std::fs::File::open(&self.data_dir)?.sync_all()?;
        Ok(())
    }
    fn manifest(dir: &std::path::Path) -> Result<(u64, u64)> {
        match std::fs::read(dir.join("manifest.json")) {
            Ok(data) => serde_json::from_slice(&data)
                .map_err(|e| FluxError::Corruption(format!("Invalid manifest: {e}"))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok((0, 0)),
            Err(e) => Err(e.into()),
        }
    }

    /// Query data
    pub fn query(&self, sql: &str) -> Result<QueryResult> {
        let _guard = self.operation.read();
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
        let _guard = self.operation.read();
        Ok(self
            .all_data()?
            .into_iter()
            .filter(|p| &p.key == series_key && time_range.contains(p.data.timestamp))
            .map(|p| p.data)
            .collect())
    }

    pub fn get_latest(&self, series_key: &SeriesKey) -> Result<Option<DataPoint>> {
        Ok(self
            .query_series(series_key, &TimeRange::new(i64::MIN, i64::MAX))?
            .into_iter()
            .max_by_key(|p| p.timestamp))
    }

    /// Force flush memtable to disk
    pub fn flush(&self) -> Result<()> {
        let _guard = self.operation.write();
        self.wal.sync()?;
        self.maybe_flush()
    }

    /// Get database statistics
    pub fn stats(&self) -> DatabaseStats {
        let _guard = self.operation.read();
        let memtable_size = self.memtable.read().size();
        let immutable_count = self.immutable_memtables.lock().len();
        let sstable_count = self.sstables.read().len();
        let total_entries = self.all_data().map(|p| p.len()).unwrap_or(0);
        let total_size: u64 = self
            .sstables
            .read()
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
            retention_seconds: self.retention_seconds(),
        }
    }

    fn collect_data(&self, plan: &QueryPlan) -> Result<Vec<(SeriesKey, DataPoint)>> {
        Ok(self
            .all_data()?
            .into_iter()
            .filter(|p| {
                p.key.measurement == plan.measurement && plan.time_range.contains(p.data.timestamp)
            })
            .map(|p| (p.key, p.data))
            .collect())
    }

    fn maybe_flush(&self) -> Result<()> {
        let old_memtable;
        let new_id;

        {
            let mut memtable = self.memtable.write();
            if memtable.iter().is_empty() {
                return Ok(());
            }

            new_id = self.next_memtable_id.fetch_add(1, Ordering::SeqCst);
            old_memtable = std::mem::replace(&mut *memtable, MemTable::new(new_id));
        }

        // Move to immutable
        let immutable = ImmutableMemTable::from(old_memtable);

        {
            let mut immutables = self.immutable_memtables.lock();
            immutables.push(Arc::new(immutable));
        }

        // Flush to SSTable (in production, this would be async)
        self.flush_immutable()?;

        Ok(())
    }

    fn flush_immutable(&self) -> Result<()> {
        let imm = {
            let immutables = self.immutable_memtables.lock();
            if immutables.is_empty() {
                return Ok(());
            }
            immutables[0].clone()
        };

        let sstable_id = self.next_sstable_id.fetch_add(1, Ordering::SeqCst);
        let sstable_path = self.data_dir.join(format!("sst_{:020}.flux", sstable_id));

        let _meta = SSTableBuilder::build_from_memtable(
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

        self.immutable_memtables.lock().remove(0);
        // Retain WAL until a durable checkpoint protocol exists. SSTable IDs are not WAL IDs.

        Ok(())
    }

    fn recover(&self, wal_config: WalConfig, floor: u64) -> Result<()> {
        let reader = WalReader::new(wal_config);
        let entries = reader.recover_from(floor)?;

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

    fn load_sstables(db_dir: &PathBuf, minimum: u64) -> Result<Vec<SSTableReader>> {
        let mut sstables = Vec::new();

        if !db_dir.exists() {
            return Ok(sstables);
        }

        for entry in std::fs::read_dir(db_dir)? {
            let entry = entry?;
            let path = entry.path();

            if let Some(ext) = path.extension() {
                if ext == "flux" {
                    let id = path
                        .file_stem()
                        .and_then(|s| s.to_str())
                        .and_then(|s| s.strip_prefix("sst_"))
                        .and_then(|s| s.parse::<u64>().ok())
                        .ok_or_else(|| {
                            FluxError::InvalidFormat("Invalid SSTable filename".into())
                        })?;
                    if id < minimum {
                        continue;
                    }
                    match SSTableReader::open(path.clone()) {
                        Ok(reader) => sstables.push(reader),
                        Err(e) => return Err(e),
                    }
                }
            }
        }

        if minimum > 0 && !sstables.iter().any(|s| s.meta().id == minimum) {
            return Err(FluxError::Corruption("Manifest snapshot is missing".into()));
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
    pub retention_seconds: u64,
}
