//! SSTable data block implementation

use crate::{DataPoint, FieldValue, Fields, Result, FluxError};
use crate::compression::{GorillaEncoder, GorillaDecoder};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::collections::BTreeMap;

/// Block header
#[derive(Debug, Clone)]
pub struct BlockHeader {
    /// Block format version
    pub version: u8,
    /// Number of entries
    pub entry_count: u32,
    /// Compressed data size
    pub compressed_size: u32,
    /// Uncompressed data size
    pub uncompressed_size: u32,
    /// CRC32 checksum
    pub checksum: u32,
}

impl BlockHeader {
    /// Header size in bytes
    pub const SIZE: usize = 17;

    /// Serialize to bytes
    pub fn to_bytes(&self) -> Bytes {
        let mut buf = BytesMut::with_capacity(Self::SIZE);
        buf.put_u8(self.version);
        buf.put_u32_le(self.entry_count);
        buf.put_u32_le(self.compressed_size);
        buf.put_u32_le(self.uncompressed_size);
        buf.put_u32_le(self.checksum);
        buf.freeze()
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < Self::SIZE {
            return Err(FluxError::InvalidFormat("Block header too short".into()));
        }
        
        let mut cursor = std::io::Cursor::new(data);
        Ok(Self {
            version: cursor.get_u8(),
            entry_count: cursor.get_u32_le(),
            compressed_size: cursor.get_u32_le(),
            uncompressed_size: cursor.get_u32_le(),
            checksum: cursor.get_u32_le(),
        })
    }
}

/// A data block containing compressed time-series data
#[derive(Debug)]
pub struct DataBlock {
    /// Field name this block contains
    pub field_name: String,
    /// Compressed data
    pub data: Vec<u8>,
    /// Number of points
    pub count: usize,
    /// First timestamp
    pub first_timestamp: i64,
    /// Last timestamp
    pub last_timestamp: i64,
}

/// Block builder for writing data
pub struct BlockBuilder {
    field_name: String,
    encoder: GorillaEncoder,
    count: usize,
}

impl BlockBuilder {
    /// Create a new block builder
    pub fn new(field_name: impl Into<String>) -> Self {
        Self {
            field_name: field_name.into(),
            encoder: GorillaEncoder::new(),
            count: 0,
        }
    }

    /// Add a data point
    pub fn add(&mut self, timestamp: i64, value: f64) {
        self.encoder.encode(timestamp, value);
        self.count += 1;
    }

    /// Check if block has data
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Get entry count
    pub fn len(&self) -> usize {
        self.count
    }

    /// Finish building and return the data block
    pub fn finish(self) -> DataBlock {
        let compressed = self.encoder.finish();
        DataBlock {
            field_name: self.field_name,
            data: compressed.data,
            count: compressed.count,
            first_timestamp: compressed.first_timestamp,
            last_timestamp: compressed.last_timestamp,
        }
    }
}

impl DataBlock {
    /// Decompress and return all data points
    pub fn decompress(&self) -> Result<Vec<(i64, f64)>> {
        let mut decoder = GorillaDecoder::new(&self.data, self.count);
        decoder.decode_all()
    }

    /// Decompress with LZ4 if needed, then Gorilla decode
    pub fn decompress_lz4(&self, data: &[u8], count: usize) -> Result<Vec<(i64, f64)>> {
        // Decompress with LZ4 first
        let decompressed = lz4_flex::decompress_size_prepended(data)
            .map_err(|e| FluxError::Compression(e.to_string()))?;
        
        let mut decoder = GorillaDecoder::new(&decompressed, count);
        decoder.decode_all()
    }

    /// Serialize to bytes with optional LZ4 compression
    pub fn to_bytes(&self, use_lz4: bool) -> Bytes {
        let mut buf = BytesMut::new();
        
        // Field name
        buf.put_u16_le(self.field_name.len() as u16);
        buf.put_slice(self.field_name.as_bytes());
        
        // Metadata
        buf.put_u32_le(self.count as u32);
        buf.put_i64_le(self.first_timestamp);
        buf.put_i64_le(self.last_timestamp);
        
        // Data (with optional LZ4)
        if use_lz4 {
            let compressed = lz4_flex::compress_prepend_size(&self.data);
            buf.put_u8(1); // LZ4 flag
            buf.put_u32_le(compressed.len() as u32);
            buf.put_slice(&compressed);
        } else {
            buf.put_u8(0); // No LZ4
            buf.put_u32_le(self.data.len() as u32);
            buf.put_slice(&self.data);
        }
        
        // Checksum
        let checksum = crc32fast::hash(&buf);
        buf.put_u32_le(checksum);
        
        buf.freeze()
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        if data.len() < 10 {
            return Err(FluxError::InvalidFormat("Block too short".into()));
        }
        
        let mut cursor = std::io::Cursor::new(data);
        
        // Field name
        let field_len = cursor.get_u16_le() as usize;
        let pos = cursor.position() as usize;
        let field_name = String::from_utf8(data[pos..pos + field_len].to_vec())
            .map_err(|e| FluxError::InvalidFormat(e.to_string()))?;
        cursor.set_position((pos + field_len) as u64);
        
        // Metadata
        let count = cursor.get_u32_le() as usize;
        let first_timestamp = cursor.get_i64_le();
        let last_timestamp = cursor.get_i64_le();
        
        // Data
        let lz4_flag = cursor.get_u8();
        let data_len = cursor.get_u32_le() as usize;
        let pos = cursor.position() as usize;
        let raw_data = data[pos..pos + data_len].to_vec();
        
        // Decompress LZ4 if needed
        let block_data = if lz4_flag == 1 {
            lz4_flex::decompress_size_prepended(&raw_data)
                .map_err(|e| FluxError::Compression(e.to_string()))?
        } else {
            raw_data
        };
        
        // Verify checksum
        let checksum_pos = pos + data_len;
        if checksum_pos + 4 > data.len() {
            return Err(FluxError::InvalidFormat("Missing checksum".into()));
        }
        let expected_checksum = {
            let mut c = std::io::Cursor::new(&data[checksum_pos..]);
            c.get_u32_le()
        };
        let actual_checksum = crc32fast::hash(&data[..checksum_pos]);
        
        if expected_checksum != actual_checksum {
            return Err(FluxError::ChecksumMismatch {
                expected: expected_checksum,
                actual: actual_checksum,
            });
        }
        
        Ok(Self {
            field_name,
            data: block_data,
            count,
            first_timestamp,
            last_timestamp,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_builder() {
        let mut builder = BlockBuilder::new("temperature");
        
        for i in 0..100 {
            builder.add(1000000 + i * 10000, 20.0 + i as f64 * 0.1);
        }
        
        let block = builder.finish();
        assert_eq!(block.count, 100);
        assert_eq!(block.field_name, "temperature");
        
        let points = block.decompress().unwrap();
        assert_eq!(points.len(), 100);
        assert_eq!(points[0].0, 1000000);
    }

    #[test]
    fn test_block_serialization() {
        let mut builder = BlockBuilder::new("value");
        
        for i in 0..50 {
            builder.add(i * 1000, i as f64);
        }
        
        let block = builder.finish();
        let bytes = block.to_bytes(true);
        
        let restored = DataBlock::from_bytes(&bytes).unwrap();
        assert_eq!(restored.count, 50);
        assert_eq!(restored.field_name, "value");
        
        let points = restored.decompress().unwrap();
        assert_eq!(points.len(), 50);
    }
}
