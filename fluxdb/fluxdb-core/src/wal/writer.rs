//! WAL writer implementation

use super::{SyncPolicy, WalConfig, WalEntry};
use crate::{FluxError, Result};
use parking_lot::Mutex;
use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// WAL writer for appending entries to disk
pub struct WalWriter {
    config: WalConfig,
    inner: Mutex<WalWriterInner>,
    current_offset: AtomicU64,
}

struct WalWriterInner {
    file: BufWriter<File>,
    segment_id: u64,
    bytes_written: usize,
    writes_since_sync: usize,
    last_sync: Instant,
}

impl WalWriter {
    /// Create a new WAL writer
    pub fn new(config: WalConfig) -> Result<Self> {
        // Create directory if it doesn't exist
        fs::create_dir_all(&config.dir)?;

        // Find the latest segment or create a new one
        let segment_id = Self::find_latest_segment(&config.dir)?;
        let file = Self::open_segment(&config.dir, segment_id)?;

        let inner = WalWriterInner {
            file: BufWriter::new(file),
            segment_id,
            bytes_written: 0,
            writes_since_sync: 0,
            last_sync: Instant::now(),
        };

        Ok(Self {
            config,
            inner: Mutex::new(inner),
            current_offset: AtomicU64::new(0),
        })
    }

    /// Append an entry to the WAL
    pub fn append(&self, entry: &WalEntry) -> Result<u64> {
        let serialized = entry.serialize_with_checksum();
        let mut inner = self.inner.lock();

        // Check if we need to rotate to a new segment
        if inner.bytes_written + serialized.len() > self.config.segment_size {
            self.rotate_segment(&mut inner)?;
        }

        // Write to buffer
        inner.file.write_all(&serialized)?;
        inner.bytes_written += serialized.len();
        inner.writes_since_sync += 1;

        // Sync based on policy
        if self.should_sync(&inner) {
            inner.file.flush()?;
            inner.file.get_ref().sync_all()?;
            inner.writes_since_sync = 0;
            inner.last_sync = Instant::now();
        }

        let offset = self.current_offset.fetch_add(serialized.len() as u64, Ordering::Relaxed);
        Ok(offset)
    }

    /// Force sync to disk
    pub fn sync(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.file.flush()?;
        inner.file.get_ref().sync_all()?;
        inner.writes_since_sync = 0;
        inner.last_sync = Instant::now();
        Ok(())
    }

    /// Get current segment ID
    pub fn current_segment(&self) -> u64 {
        self.inner.lock().segment_id
    }

    /// Truncate WAL up to the given segment (used after memtable flush)
    pub fn truncate_before(&self, segment_id: u64) -> Result<usize> {
        let mut truncated = 0;
        for entry in fs::read_dir(&self.config.dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if let Some(id) = name.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log")) {
                    if let Ok(id) = id.parse::<u64>() {
                        if id < segment_id {
                            fs::remove_file(&path)?;
                            truncated += 1;
                        }
                    }
                }
            }
        }
        Ok(truncated)
    }

    fn should_sync(&self, inner: &WalWriterInner) -> bool {
        match self.config.sync_policy {
            SyncPolicy::Immediate => true,
            SyncPolicy::EveryN(n) => inner.writes_since_sync >= n,
            SyncPolicy::Interval { millis } => {
                inner.last_sync.elapsed().as_millis() >= millis as u128
            }
            SyncPolicy::None => false,
        }
    }

    fn rotate_segment(&self, inner: &mut WalWriterInner) -> Result<()> {
        // Sync current segment
        inner.file.flush()?;
        inner.file.get_ref().sync_all()?;

        // Create new segment
        inner.segment_id += 1;
        let file = Self::open_segment(&self.config.dir, inner.segment_id)?;
        inner.file = BufWriter::new(file);
        inner.bytes_written = 0;
        inner.writes_since_sync = 0;

        Ok(())
    }

    fn find_latest_segment(dir: &PathBuf) -> Result<u64> {
        let mut max_id = 0u64;
        if dir.exists() {
            for entry in fs::read_dir(dir)? {
                let entry = entry?;
                let path = entry.path();
                if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                    if let Some(id) = name.strip_prefix("wal_").and_then(|s| s.strip_suffix(".log"))
                    {
                        if let Ok(id) = id.parse::<u64>() {
                            max_id = max_id.max(id);
                        }
                    }
                }
            }
        }
        Ok(max_id)
    }

    fn open_segment(dir: &PathBuf, segment_id: u64) -> Result<File> {
        let path = dir.join(format!("wal_{:020}.log", segment_id));
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|e| FluxError::Io(e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataPoint, FieldValue, Point, SeriesKey};
    use tempfile::TempDir;

    #[test]
    fn test_wal_writer() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            dir: temp_dir.path().to_path_buf(),
            sync_policy: SyncPolicy::Immediate,
            segment_size: 1024,
        };

        let writer = WalWriter::new(config).unwrap();

        let key = SeriesKey::new("temp").with_tag("id", "1");
        let data = DataPoint::new(1000, "value", FieldValue::Float(23.5));
        let points = vec![Point::new(key, data)];

        let entry = WalEntry::write("testdb", &points).unwrap();
        let offset = writer.append(&entry).unwrap();
        assert_eq!(offset, 0);

        writer.sync().unwrap();
    }
}
