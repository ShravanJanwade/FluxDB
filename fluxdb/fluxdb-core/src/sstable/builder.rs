//! Typed SSTable writer. FLX2 stores a checksummed LZ4 block of exact field values.
use super::{SSTableConfig, SSTableMeta};
use crate::memtable::ImmutableMemTable;
use crate::{DataPoint, FluxError, Point, Result, SeriesKey};
use std::{fs::File, io::Write, path::PathBuf};
pub struct SSTableBuilder {
    path: PathBuf,
    id: u64,
    level: u32,
    points: Vec<Point>,
}
impl SSTableBuilder {
    pub fn new(path: PathBuf, id: u64, level: u32, _config: SSTableConfig) -> Self {
        Self {
            path,
            id,
            level,
            points: vec![],
        }
    }
    pub fn add(&mut self, key: &SeriesKey, point: &DataPoint) -> Result<()> {
        self.points.push(Point::new(key.clone(), point.clone()));
        Ok(())
    }
    pub fn build_from_memtable(
        path: PathBuf,
        id: u64,
        level: u32,
        memtable: &ImmutableMemTable,
        config: SSTableConfig,
    ) -> Result<SSTableMeta> {
        let mut builder = Self::new(path, id, level, config);
        for (key, data) in memtable.iter() {
            builder.add(&key.series_key, &data)?;
        }
        builder.finish()
    }
    pub fn finish(mut self) -> Result<SSTableMeta> {
        self.points.sort_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then(a.data.timestamp.cmp(&b.data.timestamp))
        });
        let payload = bincode::serialize(&self.points)
            .map_err(|e| FluxError::InvalidFormat(e.to_string()))?;
        let compressed = lz4_flex::compress_prepend_size(&payload);
        let temporary = self.path.with_extension("pending");
        let mut file = File::create(&temporary)?;
        file.write_all(b"FLX2")?;
        file.write_all(&crc32fast::hash(&compressed).to_le_bytes())?;
        file.write_all(&compressed)?;
        file.sync_all()?;
        drop(file);
        std::fs::rename(&temporary, &self.path)?;
        #[cfg(unix)]
        File::open(self.path.parent().unwrap())?.sync_all()?;
        Ok(SSTableMeta {
            path: self.path,
            id: self.id,
            level: self.level,
            entry_count: self.points.len(),
            file_size: compressed.len() as u64 + 8,
            min_timestamp: self
                .points
                .iter()
                .map(|p| p.data.timestamp)
                .min()
                .unwrap_or(0),
            max_timestamp: self
                .points
                .iter()
                .map(|p| p.data.timestamp)
                .max()
                .unwrap_or(0),
            min_key: self
                .points
                .first()
                .map(|p| p.key.clone())
                .unwrap_or_else(|| SeriesKey::new("")),
            max_key: self
                .points
                .last()
                .map(|p| p.key.clone())
                .unwrap_or_else(|| SeriesKey::new("")),
        })
    }
}
