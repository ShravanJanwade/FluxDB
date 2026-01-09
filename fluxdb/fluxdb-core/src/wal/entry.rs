//! WAL entry types and serialization

use crate::{Point, Result, FluxError};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};

/// WAL entry type
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum WalEntryType {
    /// Write data points
    Write = 1,
    /// Delete data points
    Delete = 2,
    /// Create database
    CreateDatabase = 3,
    /// Drop database
    DropDatabase = 4,
    /// Checkpoint marker
    Checkpoint = 5,
}

impl TryFrom<u8> for WalEntryType {
    type Error = FluxError;

    fn try_from(value: u8) -> Result<Self> {
        match value {
            1 => Ok(WalEntryType::Write),
            2 => Ok(WalEntryType::Delete),
            3 => Ok(WalEntryType::CreateDatabase),
            4 => Ok(WalEntryType::DropDatabase),
            5 => Ok(WalEntryType::Checkpoint),
            _ => Err(FluxError::InvalidFormat(format!(
                "Invalid WAL entry type: {}",
                value
            ))),
        }
    }
}

/// A single WAL entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    /// Entry type
    pub entry_type: WalEntryType,
    /// Database name
    pub database: String,
    /// Entry payload (serialized)
    pub payload: Vec<u8>,
}

impl WalEntry {
    /// Create a write entry for data points
    pub fn write(database: &str, points: &[Point]) -> Result<Self> {
        let payload = bincode::serialize(points)
            .map_err(|e| FluxError::InvalidFormat(e.to_string()))?;
        Ok(Self {
            entry_type: WalEntryType::Write,
            database: database.to_string(),
            payload,
        })
    }

    /// Create a checkpoint entry
    pub fn checkpoint(database: &str) -> Self {
        Self {
            entry_type: WalEntryType::Checkpoint,
            database: database.to_string(),
            payload: vec![],
        }
    }

    /// Serialize the entry with length prefix and CRC checksum
    ///
    /// Format:
    /// - 4 bytes: entry length (excluding this field)
    /// - 1 byte: entry type
    /// - 4 bytes: database name length
    /// - N bytes: database name
    /// - 4 bytes: payload length
    /// - N bytes: payload
    /// - 4 bytes: CRC32 checksum
    pub fn serialize_with_checksum(&self) -> Bytes {
        let mut buf = BytesMut::new();

        // Reserve space for length prefix
        buf.put_u32_le(0);

        // Entry type
        buf.put_u8(self.entry_type as u8);

        // Database name
        buf.put_u32_le(self.database.len() as u32);
        buf.put_slice(self.database.as_bytes());

        // Payload
        buf.put_u32_le(self.payload.len() as u32);
        buf.put_slice(&self.payload);

        // Calculate and write checksum (excluding length prefix)
        let checksum = crc32fast::hash(&buf[4..]);
        buf.put_u32_le(checksum);

        // Write actual length
        let len = (buf.len() - 4) as u32;
        buf[0..4].copy_from_slice(&len.to_le_bytes());

        buf.freeze()
    }

    /// Deserialize entry from bytes, validating checksum
    pub fn deserialize_with_checksum(data: &[u8]) -> Result<(Self, usize)> {
        if data.len() < 4 {
            return Err(FluxError::InvalidFormat("Entry too short".into()));
        }

        let mut cursor = std::io::Cursor::new(data);

        // Read length
        let len = cursor.get_u32_le() as usize;
        if data.len() < 4 + len {
            return Err(FluxError::InvalidFormat("Incomplete entry".into()));
        }

        let entry_data = &data[4..4 + len];

        // Validate checksum
        let expected_checksum = {
            let mut c = std::io::Cursor::new(&entry_data[entry_data.len() - 4..]);
            c.get_u32_le()
        };
        let actual_checksum = crc32fast::hash(&entry_data[..entry_data.len() - 4]);

        if expected_checksum != actual_checksum {
            return Err(FluxError::ChecksumMismatch {
                expected: expected_checksum,
                actual: actual_checksum,
            });
        }

        let mut cursor = std::io::Cursor::new(entry_data);

        // Entry type
        let entry_type = WalEntryType::try_from(cursor.get_u8())?;

        // Database name
        let db_len = cursor.get_u32_le() as usize;
        let pos = cursor.position() as usize;
        let database = String::from_utf8(entry_data[pos..pos + db_len].to_vec())
            .map_err(|e| FluxError::InvalidFormat(e.to_string()))?;
        cursor.set_position((pos + db_len) as u64);

        // Payload
        let payload_len = cursor.get_u32_le() as usize;
        let pos = cursor.position() as usize;
        let payload = entry_data[pos..pos + payload_len].to_vec();

        let entry = WalEntry {
            entry_type,
            database,
            payload,
        };

        Ok((entry, 4 + len))
    }

    /// Get the points from a write entry
    pub fn get_points(&self) -> Result<Vec<Point>> {
        if self.entry_type != WalEntryType::Write {
            return Err(FluxError::InvalidFormat("Not a write entry".into()));
        }
        bincode::deserialize(&self.payload)
            .map_err(|e| FluxError::InvalidFormat(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataPoint, FieldValue, SeriesKey};

    #[test]
    fn test_entry_serialization() {
        let key = SeriesKey::new("temperature").with_tag("sensor", "s1");
        let data = DataPoint::new(1000000, "value", FieldValue::Float(23.5));
        let points = vec![Point::new(key, data)];

        let entry = WalEntry::write("testdb", &points).unwrap();
        let serialized = entry.serialize_with_checksum();

        let (deserialized, len) = WalEntry::deserialize_with_checksum(&serialized).unwrap();
        assert_eq!(len, serialized.len());
        assert_eq!(deserialized.entry_type, WalEntryType::Write);
        assert_eq!(deserialized.database, "testdb");

        let recovered_points = deserialized.get_points().unwrap();
        assert_eq!(recovered_points.len(), 1);
    }

    #[test]
    fn test_checksum_validation() {
        let entry = WalEntry::checkpoint("testdb");
        let mut serialized = entry.serialize_with_checksum().to_vec();

        // Corrupt the data
        serialized[10] ^= 0xFF;

        let result = WalEntry::deserialize_with_checksum(&serialized);
        assert!(matches!(result, Err(FluxError::ChecksumMismatch { .. })));
    }
}
