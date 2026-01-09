//! SSTable builder for writing sorted data to disk

use super::{BloomFilter, DataBlock, SSTableConfig, SSTableMeta, FORMAT_VERSION};
use super::block::BlockBuilder;
use crate::{DataPoint, FieldValue, Point, Result, FluxError, SeriesKey, Timestamp};
use crate::memtable::{ImmutableMemTable, MemTableKey};
use bytes::{BufMut, BytesMut};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

/// SSTable builder
pub struct SSTableBuilder {
    config: SSTableConfig,
    path: PathBuf,
    id: u64,
    level: u32,
    
    // Current state
    blocks: Vec<BlockData>,
    current_blocks: BTreeMap<String, BlockBuilder>,
    current_series: Option<SeriesKey>,
    
    // Index data
    index_entries: Vec<IndexEntry>,
    bloom_filter: BloomFilter,
    
    // Stats
    entry_count: usize,
    min_timestamp: Timestamp,
    max_timestamp: Timestamp,
    min_key: Option<SeriesKey>,
    max_key: Option<SeriesKey>,
}

struct BlockData {
    series_key: SeriesKey,
    blocks: Vec<DataBlock>,
    offset: u64,
}

#[derive(Debug, Clone)]
struct IndexEntry {
    series_key: SeriesKey,
    field_name: String,
    offset: u64,
    size: u32,
    min_time: Timestamp,
    max_time: Timestamp,
}

impl SSTableBuilder {
    /// Create a new SSTable builder
    pub fn new(path: PathBuf, id: u64, level: u32, config: SSTableConfig) -> Self {
        Self {
            config,
            path,
            id,
            level,
            blocks: Vec::new(),
            current_blocks: BTreeMap::new(),
            current_series: None,
            index_entries: Vec::new(),
            bloom_filter: BloomFilter::new(1000, 10),
            entry_count: 0,
            min_timestamp: i64::MAX,
            max_timestamp: i64::MIN,
            min_key: None,
            max_key: None,
        }
    }

    /// Add a point to the SSTable
    pub fn add(&mut self, key: &SeriesKey, point: &DataPoint) -> Result<()> {
        // Check if we're starting a new series
        if self.current_series.as_ref() != Some(key) {
            self.flush_current_series()?;
            self.current_series = Some(key.clone());
            self.bloom_filter.add(&key.canonical());
        }

        // Update stats
        self.entry_count += 1;
        self.min_timestamp = self.min_timestamp.min(point.timestamp);
        self.max_timestamp = self.max_timestamp.max(point.timestamp);
        
        if self.min_key.is_none() {
            self.min_key = Some(key.clone());
        }
        self.max_key = Some(key.clone());

        // Add each field to its block builder
        for (field_name, field_value) in point.fields.iter() {
            if let Some(value) = field_value.as_f64() {
                let builder = self.current_blocks
                    .entry(field_name.clone())
                    .or_insert_with(|| BlockBuilder::new(field_name.clone()));
                builder.add(point.timestamp, value);
            }
        }

        Ok(())
    }

    /// Build from an immutable memtable
    pub fn build_from_memtable(
        path: PathBuf,
        id: u64,
        level: u32,
        memtable: &ImmutableMemTable,
        config: SSTableConfig,
    ) -> Result<SSTableMeta> {
        let mut builder = Self::new(path, id, level, config);
        
        for (key, data) in memtable.iter() {
            let point = Point::new(key.series_key.clone(), data);
            builder.add(&point.key, &point.data)?;
        }
        
        builder.finish()
    }

    fn flush_current_series(&mut self) -> Result<()> {
        if self.current_blocks.is_empty() {
            return Ok(());
        }

        let series_key = self.current_series.take().unwrap();
        let mut blocks = Vec::new();
        
        let keys: Vec<_> = self.current_blocks.keys().cloned().collect();
        for key in keys {
            if let Some(builder) = self.current_blocks.remove(&key) {
                if !builder.is_empty() {
                    blocks.push(builder.finish());
                }
            }
        }

        if !blocks.is_empty() {
            self.blocks.push(BlockData {
                series_key,
                blocks,
                offset: 0,
            });
        }

        Ok(())
    }

    /// Finish building and write to disk
    pub fn finish(mut self) -> Result<SSTableMeta> {
        self.flush_current_series()?;

        let mut file = BufWriter::new(File::create(&self.path)?);
        let mut offset = 0u64;

        // Write header
        let header = self.write_header(&mut file)?;
        offset += header as u64;

        // Write data blocks
        for block_data in &mut self.blocks {
            block_data.offset = offset;
            
            for block in &block_data.blocks {
                let bytes = block.to_bytes(self.config.compression);
                
                self.index_entries.push(IndexEntry {
                    series_key: block_data.series_key.clone(),
                    field_name: block.field_name.clone(),
                    offset,
                    size: bytes.len() as u32,
                    min_time: block.first_timestamp,
                    max_time: block.last_timestamp,
                });
                
                file.write_all(&bytes)?;
                offset += bytes.len() as u64;
            }
        }

        // Write index
        let index_offset = offset;
        let index_size = self.write_index(&mut file)?;
        offset += index_size as u64;

        // Write bloom filter
        let bloom_offset = offset;
        let bloom_size = self.write_bloom(&mut file)?;
        offset += bloom_size as u64;

        // Write footer
        self.write_footer(&mut file, index_offset, index_size as u64, bloom_offset, bloom_size as u64)?;

        file.flush()?;

        let file_size = offset + 32; // footer size

        Ok(SSTableMeta {
            path: self.path,
            id: self.id,
            level: self.level,
            entry_count: self.entry_count,
            file_size,
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
            min_key: self.min_key.unwrap_or_else(|| SeriesKey::new("")),
            max_key: self.max_key.unwrap_or_else(|| SeriesKey::new("")),
        })
    }

    fn write_header(&self, file: &mut BufWriter<File>) -> Result<usize> {
        let mut buf = BytesMut::new();
        
        // Magic number
        buf.put_slice(b"FLUX");
        // Version
        buf.put_u32_le(FORMAT_VERSION);
        // Entry count
        buf.put_u64_le(self.entry_count as u64);
        // Timestamp range
        buf.put_i64_le(self.min_timestamp);
        buf.put_i64_le(self.max_timestamp);
        
        file.write_all(&buf)?;
        Ok(buf.len())
    }

    fn write_index(&self, file: &mut BufWriter<File>) -> Result<usize> {
        let mut buf = BytesMut::new();
        
        buf.put_u32_le(self.index_entries.len() as u32);
        
        for entry in &self.index_entries {
            let key_bytes = entry.series_key.canonical();
            buf.put_u16_le(key_bytes.len() as u16);
            buf.put_slice(key_bytes.as_bytes());
            
            buf.put_u16_le(entry.field_name.len() as u16);
            buf.put_slice(entry.field_name.as_bytes());
            
            buf.put_u64_le(entry.offset);
            buf.put_u32_le(entry.size);
            buf.put_i64_le(entry.min_time);
            buf.put_i64_le(entry.max_time);
        }
        
        file.write_all(&buf)?;
        Ok(buf.len())
    }

    fn write_bloom(&self, file: &mut BufWriter<File>) -> Result<usize> {
        let mut buf = BytesMut::new();
        let bloom_data = self.bloom_filter.as_bytes();
        
        buf.put_u32_le(bloom_data.len() as u32);
        buf.put_u8(self.bloom_filter.num_hashes() as u8);
        buf.put_slice(bloom_data);
        
        file.write_all(&buf)?;
        Ok(buf.len())
    }

    fn write_footer(
        &self,
        file: &mut BufWriter<File>,
        index_offset: u64,
        index_size: u64,
        bloom_offset: u64,
        bloom_size: u64,
    ) -> Result<usize> {
        let mut buf = BytesMut::new();
        
        buf.put_u64_le(index_offset);
        buf.put_u64_le(index_size);
        buf.put_u64_le(bloom_offset);
        buf.put_u64_le(bloom_size);
        
        // Magic number at end for validation
        buf.put_slice(b"FLUX");
        
        file.write_all(&buf)?;
        Ok(buf.len())
    }
}
