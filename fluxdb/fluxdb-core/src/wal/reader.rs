//! WAL reader for recovery

use super::{WalConfig, WalEntry};
use crate::{FluxError, Result};
use std::fs::{self, File};
use std::io::Read;
use std::path::PathBuf;

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
            entries.extend(self.read_segment(&segment_path)?);
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
                    entries.extend(self.read_segment(&segment_path)?);
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
                Err(FluxError::InvalidFormat(msg)) if msg == "Entry too short" => {
                    // Discard only an incomplete tail, never a checksum failure.
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(path)?
                        .set_len(offset as u64)?;
                    break;
                }
                Err(FluxError::InvalidFormat(msg)) if msg == "Incomplete entry" => {
                    // Discard only an incomplete tail, never a checksum failure.
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(path)?
                        .set_len(offset as u64)?;
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
