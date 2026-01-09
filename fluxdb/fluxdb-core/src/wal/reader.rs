//! WAL reader for recovery

use super::{WalConfig, WalEntry};
use crate::{FluxError, Result};
use std::fs::{self, File};
use std::io::Read;
use std::path::PathBuf;
use tracing::{info, warn};

/// WAL reader for recovering entries after crash
pub struct WalReader {
    config: WalConfig,
}

impl WalReader {
    /// Create a new WAL reader
    pub fn new(config: WalConfig) -> Self {
        Self { config }
    }

    /// Recover all entries from WAL segments
    pub fn recover(&self) -> Result<Vec<WalEntry>> {
        let segments = self.find_segments()?;
        let mut entries = Vec::new();

        for segment_path in segments {
            match self.read_segment(&segment_path) {
                Ok(segment_entries) => {
                    info!(
                        "Recovered {} entries from {:?}",
                        segment_entries.len(),
                        segment_path
                    );
                    entries.extend(segment_entries);
                }
                Err(e) => {
                    warn!("Error reading segment {:?}: {}", segment_path, e);
                    // Continue with other segments
                }
            }
        }

        Ok(entries)
    }

    /// Recover entries from a specific segment onwards
    pub fn recover_from(&self, start_segment: u64) -> Result<Vec<WalEntry>> {
        let segments = self.find_segments()?;
        let mut entries = Vec::new();

        for segment_path in segments {
            if let Some(segment_id) = Self::parse_segment_id(&segment_path) {
                if segment_id >= start_segment {
                    match self.read_segment(&segment_path) {
                        Ok(segment_entries) => entries.extend(segment_entries),
                        Err(e) => warn!("Error reading segment {:?}: {}", segment_path, e),
                    }
                }
            }
        }

        Ok(entries)
    }

    fn find_segments(&self) -> Result<Vec<PathBuf>> {
        let mut segments = Vec::new();

        if !self.config.dir.exists() {
            return Ok(segments);
        }

        for entry in fs::read_dir(&self.config.dir)? {
            let entry = entry?;
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("wal_") && name.ends_with(".log") {
                    segments.push(path);
                }
            }
        }

        // Sort by segment ID
        segments.sort_by(|a, b| {
            let id_a = Self::parse_segment_id(a).unwrap_or(0);
            let id_b = Self::parse_segment_id(b).unwrap_or(0);
            id_a.cmp(&id_b)
        });

        Ok(segments)
    }

    fn read_segment(&self, path: &PathBuf) -> Result<Vec<WalEntry>> {
        let mut file = File::open(path)?;
        let mut data = Vec::new();
        file.read_to_end(&mut data)?;

        let mut entries = Vec::new();
        let mut offset = 0;

        while offset < data.len() {
            match WalEntry::deserialize_with_checksum(&data[offset..]) {
                Ok((entry, bytes_read)) => {
                    entries.push(entry);
                    offset += bytes_read;
                }
                Err(FluxError::ChecksumMismatch { .. }) => {
                    // Corrupted entry, skip rest of segment
                    warn!(
                        "Checksum mismatch at offset {} in {:?}, truncating",
                        offset, path
                    );
                    break;
                }
                Err(FluxError::InvalidFormat(msg)) if msg == "Entry too short" => {
                    // Incomplete entry at end (crash during write)
                    break;
                }
                Err(FluxError::InvalidFormat(msg)) if msg == "Incomplete entry" => {
                    // Incomplete entry at end (crash during write)
                    break;
                }
                Err(e) => {
                    return Err(e);
                }
            }
        }

        Ok(entries)
    }

    fn parse_segment_id(path: &PathBuf) -> Option<u64> {
        path.file_name()
            .and_then(|n| n.to_str())
            .and_then(|s| s.strip_prefix("wal_"))
            .and_then(|s| s.strip_suffix(".log"))
            .and_then(|s| s.parse().ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalWriter;
    use crate::{DataPoint, FieldValue, Point, SeriesKey};
    use tempfile::TempDir;

    #[test]
    fn test_wal_recovery() {
        let temp_dir = TempDir::new().unwrap();
        let config = WalConfig {
            dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };

        // Write some entries
        {
            let writer = WalWriter::new(config.clone()).unwrap();
            for i in 0..10 {
                let key = SeriesKey::new("temp").with_tag("id", &i.to_string());
                let data = DataPoint::new(i * 1000, "value", FieldValue::Float(23.5 + i as f64));
                let points = vec![Point::new(key, data)];
                let entry = WalEntry::write("testdb", &points).unwrap();
                writer.append(&entry).unwrap();
            }
            writer.sync().unwrap();
        }

        // Recover entries
        let reader = WalReader::new(config);
        let entries = reader.recover().unwrap();
        assert_eq!(entries.len(), 10);
    }
}
