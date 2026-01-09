//! SSTable reader for querying data

use super::{BloomFilter, DataBlock, SSTableMeta, FORMAT_VERSION};
use crate::{DataPoint, FieldValue, Fields, Result, FluxError, SeriesKey, TimeRange, Timestamp};
use bytes::Buf;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use parking_lot::RwLock;

/// SSTable reader
pub struct SSTableReader {
    path: PathBuf,
    meta: SSTableMeta,
    index: Vec<IndexEntry>,
    bloom_filter: BloomFilter,
    cache: Arc<RwLock<BlockCache>>,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    series_key: String,
    field_name: String,
    offset: u64,
    size: u32,
    min_time: Timestamp,
    max_time: Timestamp,
}

struct BlockCache {
    blocks: BTreeMap<u64, DataBlock>,
    max_size: usize,
    current_size: usize,
}

impl BlockCache {
    fn new(max_size: usize) -> Self {
        Self {
            blocks: BTreeMap::new(),
            max_size,
            current_size: 0,
        }
    }

    fn get(&self, offset: u64) -> Option<&DataBlock> {
        self.blocks.get(&offset)
    }

    fn insert(&mut self, offset: u64, block: DataBlock) {
        let size = block.data.len();
        if self.current_size + size > self.max_size {
            // Simple eviction: remove oldest
            if let Some((&key, _)) = self.blocks.iter().next() {
                if let Some(removed) = self.blocks.remove(&key) {
                    self.current_size -= removed.data.len();
                }
            }
        }
        self.current_size += size;
        self.blocks.insert(offset, block);
    }
}

impl SSTableReader {
    /// Open an SSTable file
    pub fn open(path: PathBuf) -> Result<Self> {
        let mut file = File::open(&path)?;
        let file_size = file.metadata()?.len();

        // Read footer
        file.seek(SeekFrom::End(-36))?;
        let mut footer = [0u8; 36];
        file.read_exact(&mut footer)?;

        let mut cursor = std::io::Cursor::new(&footer);
        let index_offset = cursor.get_u64_le();
        let index_size = cursor.get_u64_le();
        let bloom_offset = cursor.get_u64_le();
        let bloom_size = cursor.get_u64_le();
        
        // Verify magic
        let mut magic = [0u8; 4];
        cursor.read_exact(&mut magic).map_err(|e| FluxError::Io(e))?;
        if &magic != b"FLUX" {
            return Err(FluxError::InvalidFormat("Invalid SSTable magic".into()));
        }

        // Read header
        file.seek(SeekFrom::Start(0))?;
        let mut header = [0u8; 32];
        file.read_exact(&mut header)?;
        
        let mut cursor = std::io::Cursor::new(&header);
        let mut magic = [0u8; 4];
        cursor.read_exact(&mut magic).map_err(|e| FluxError::Io(e))?;
        if &magic != b"FLUX" {
            return Err(FluxError::InvalidFormat("Invalid SSTable header".into()));
        }
        
        let version = cursor.get_u32_le();
        if version != FORMAT_VERSION {
            return Err(FluxError::InvalidFormat(format!(
                "Unsupported version: {}",
                version
            )));
        }
        
        let entry_count = cursor.get_u64_le() as usize;
        let min_timestamp = cursor.get_i64_le();
        let max_timestamp = cursor.get_i64_le();

        // Read index
        file.seek(SeekFrom::Start(index_offset))?;
        let mut index_data = vec![0u8; index_size as usize];
        file.read_exact(&mut index_data)?;
        let index = Self::parse_index(&index_data)?;

        // Read bloom filter
        file.seek(SeekFrom::Start(bloom_offset))?;
        let mut bloom_data = vec![0u8; bloom_size as usize];
        file.read_exact(&mut bloom_data)?;
        let bloom_filter = Self::parse_bloom(&bloom_data)?;

        // Extract key range from index
        let (min_key, max_key) = if index.is_empty() {
            (SeriesKey::new(""), SeriesKey::new(""))
        } else {
            let min = Self::parse_series_key(&index.first().unwrap().series_key);
            let max = Self::parse_series_key(&index.last().unwrap().series_key);
            (min, max)
        };

        let meta = SSTableMeta {
            path: path.clone(),
            id: 0, // Will be set by caller
            level: 0,
            entry_count,
            file_size,
            min_timestamp,
            max_timestamp,
            min_key,
            max_key,
        };

        Ok(Self {
            path,
            meta,
            index,
            bloom_filter,
            cache: Arc::new(RwLock::new(BlockCache::new(64 * 1024 * 1024))), // 64MB cache
        })
    }

    /// Get SSTable metadata
    pub fn meta(&self) -> &SSTableMeta {
        &self.meta
    }

    /// Check if SSTable may contain a series (bloom filter check)
    pub fn may_contain(&self, series_key: &SeriesKey) -> bool {
        self.bloom_filter.may_contain(&series_key.canonical())
    }

    /// Query data points for a series in a time range
    pub fn query(
        &self,
        series_key: &SeriesKey,
        time_range: &TimeRange,
    ) -> Result<Vec<DataPoint>> {
        // Quick checks
        if !self.meta.overlaps_time(time_range.start, time_range.end) {
            return Ok(vec![]);
        }
        if !self.may_contain(series_key) {
            return Ok(vec![]);
        }

        let key_str = series_key.canonical();
        let mut field_data: BTreeMap<i64, Fields> = BTreeMap::new();

        // Find matching index entries
        for entry in &self.index {
            if entry.series_key != key_str {
                continue;
            }
            if entry.max_time < time_range.start || entry.min_time > time_range.end {
                continue;
            }

            // Read block
            let block = self.read_block(entry.offset, entry.size)?;
            let points = block.decompress()?;

            for (ts, val) in points {
                if ts >= time_range.start && ts <= time_range.end {
                    let fields = field_data.entry(ts).or_insert_with(Fields::new);
                    fields.insert(entry.field_name.clone(), FieldValue::Float(val));
                }
            }
        }

        // Convert to DataPoints
        let results: Vec<DataPoint> = field_data
            .into_iter()
            .map(|(ts, fields)| DataPoint {
                timestamp: ts,
                fields,
            })
            .collect();

        Ok(results)
    }

    /// Query a specific field
    pub fn query_field(
        &self,
        series_key: &SeriesKey,
        field_name: &str,
        time_range: &TimeRange,
    ) -> Result<Vec<(Timestamp, f64)>> {
        if !self.meta.overlaps_time(time_range.start, time_range.end) {
            return Ok(vec![]);
        }
        if !self.may_contain(series_key) {
            return Ok(vec![]);
        }

        let key_str = series_key.canonical();
        let mut results = Vec::new();

        for entry in &self.index {
            if entry.series_key != key_str || entry.field_name != field_name {
                continue;
            }
            if entry.max_time < time_range.start || entry.min_time > time_range.end {
                continue;
            }

            let block = self.read_block(entry.offset, entry.size)?;
            let points = block.decompress()?;

            for (ts, val) in points {
                if ts >= time_range.start && ts <= time_range.end {
                    results.push((ts, val));
                }
            }
        }

        Ok(results)
    }

    fn read_block(&self, offset: u64, size: u32) -> Result<DataBlock> {
        // Check cache first
        {
            let cache = self.cache.read();
            if let Some(block) = cache.get(offset) {
                return Ok(DataBlock {
                    field_name: block.field_name.clone(),
                    data: block.data.clone(),
                    count: block.count,
                    first_timestamp: block.first_timestamp,
                    last_timestamp: block.last_timestamp,
                });
            }
        }

        // Read from file
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        let mut data = vec![0u8; size as usize];
        file.read_exact(&mut data)?;

        let block = DataBlock::from_bytes(&data)?;

        // Cache the block
        {
            let mut cache = self.cache.write();
            cache.insert(offset, DataBlock {
                field_name: block.field_name.clone(),
                data: block.data.clone(),
                count: block.count,
                first_timestamp: block.first_timestamp,
                last_timestamp: block.last_timestamp,
            });
        }

        Ok(block)
    }

    fn parse_index(data: &[u8]) -> Result<Vec<IndexEntry>> {
        let mut cursor = std::io::Cursor::new(data);
        let count = cursor.get_u32_le() as usize;
        let mut entries = Vec::with_capacity(count);

        for _ in 0..count {
            let key_len = cursor.get_u16_le() as usize;
            let pos = cursor.position() as usize;
            let series_key = String::from_utf8(data[pos..pos + key_len].to_vec())
                .map_err(|e| FluxError::InvalidFormat(e.to_string()))?;
            cursor.set_position((pos + key_len) as u64);

            let field_len = cursor.get_u16_le() as usize;
            let pos = cursor.position() as usize;
            let field_name = String::from_utf8(data[pos..pos + field_len].to_vec())
                .map_err(|e| FluxError::InvalidFormat(e.to_string()))?;
            cursor.set_position((pos + field_len) as u64);

            let offset = cursor.get_u64_le();
            let size = cursor.get_u32_le();
            let min_time = cursor.get_i64_le();
            let max_time = cursor.get_i64_le();

            entries.push(IndexEntry {
                series_key,
                field_name,
                offset,
                size,
                min_time,
                max_time,
            });
        }

        Ok(entries)
    }

    fn parse_bloom(data: &[u8]) -> Result<BloomFilter> {
        if data.len() < 5 {
            return Err(FluxError::InvalidFormat("Bloom filter data too short".into()));
        }

        let mut cursor = std::io::Cursor::new(data);
        let size = cursor.get_u32_le() as usize;
        let num_hashes = cursor.get_u8() as usize;
        
        let pos = cursor.position() as usize;
        let bloom_data = data[pos..pos + size].to_vec();

        Ok(BloomFilter::from_bytes(bloom_data, num_hashes))
    }

    fn parse_series_key(canonical: &str) -> SeriesKey {
        let parts: Vec<&str> = canonical.splitn(2, ',').collect();
        let measurement = parts[0];
        let mut key = SeriesKey::new(measurement);

        if parts.len() > 1 {
            for tag in parts[1].split(',') {
                if let Some((k, v)) = tag.split_once('=') {
                    key = key.with_tag(k, v);
                }
            }
        }

        key
    }
}
