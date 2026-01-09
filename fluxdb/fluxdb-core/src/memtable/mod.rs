//! MemTable implementation using skip list
//!
//! The MemTable is an in-memory data structure that stores recent writes
//! in sorted order, allowing for fast writes and efficient range scans.

mod skiplist;

use crate::{DataPoint, Point, SeriesKey, Timestamp, TimeRange, Result};
use parking_lot::RwLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

pub use skiplist::SkipList;

/// MemTable for in-memory writes
pub struct MemTable {
    /// Skip list storing data points indexed by (series_key, timestamp)
    data: RwLock<SkipList<MemTableKey, DataPoint>>,
    /// Approximate size in bytes
    size_bytes: AtomicUsize,
    /// Creation time for age-based flushing
    created_at: Instant,
    /// Unique ID for this memtable
    id: u64,
}

/// Key for MemTable entries (series key + timestamp)
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct MemTableKey {
    /// Series key
    pub series_key: SeriesKey,
    /// Timestamp
    pub timestamp: Timestamp,
}

impl MemTableKey {
    /// Create a new MemTable key
    pub fn new(series_key: SeriesKey, timestamp: Timestamp) -> Self {
        Self {
            series_key,
            timestamp,
        }
    }

    /// Get the size in bytes
    pub fn size(&self) -> usize {
        self.series_key.size() + 8
    }
}

impl MemTable {
    /// Create a new MemTable
    pub fn new(id: u64) -> Self {
        Self {
            data: RwLock::new(SkipList::new()),
            size_bytes: AtomicUsize::new(0),
            created_at: Instant::now(),
            id,
        }
    }

    /// Get the MemTable ID
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Insert a point into the MemTable
    pub fn insert(&self, point: &Point) {
        let key = MemTableKey::new(point.key.clone(), point.data.timestamp);
        let entry_size = key.size() + point.data.size();

        let mut data = self.data.write();
        data.insert(key, point.data.clone());
        self.size_bytes.fetch_add(entry_size, Ordering::Relaxed);
    }

    /// Insert multiple points
    pub fn insert_batch(&self, points: &[Point]) {
        let mut data = self.data.write();
        let mut total_size = 0;

        for point in points {
            let key = MemTableKey::new(point.key.clone(), point.data.timestamp);
            let entry_size = key.size() + point.data.size();
            data.insert(key, point.data.clone());
            total_size += entry_size;
        }

        self.size_bytes.fetch_add(total_size, Ordering::Relaxed);
    }

    /// Check if the MemTable should be flushed
    pub fn should_flush(&self, size_limit: usize) -> bool {
        self.size_bytes.load(Ordering::Relaxed) >= size_limit
    }

    /// Get the current size in bytes
    pub fn size(&self) -> usize {
        self.size_bytes.load(Ordering::Relaxed)
    }

    /// Get the age since creation
    pub fn age(&self) -> std::time::Duration {
        self.created_at.elapsed()
    }

    /// Query a range of data points for a series
    pub fn query(
        &self,
        series_key: &SeriesKey,
        time_range: &TimeRange,
    ) -> Vec<DataPoint> {
        let data = self.data.read();
        let start_key = MemTableKey::new(series_key.clone(), time_range.start);
        let end_key = MemTableKey::new(series_key.clone(), time_range.end);

        data.range(&start_key, &end_key)
            .filter(|(k, _)| k.series_key == *series_key)
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Get the latest data point for a series
    pub fn get_latest(&self, series_key: &SeriesKey) -> Option<DataPoint> {
        let data = self.data.read();
        // Create a key with max timestamp to find the last entry
        let end_key = MemTableKey::new(series_key.clone(), i64::MAX);
        let start_key = MemTableKey::new(series_key.clone(), i64::MIN);

        data.range(&start_key, &end_key)
            .filter(|(k, _)| k.series_key == *series_key)
            .last()
            .map(|(_, v)| v.clone())
    }

    /// Iterate over all entries in sorted order
    pub fn iter(&self) -> Vec<(MemTableKey, DataPoint)> {
        let data = self.data.read();
        data.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Get all unique series keys
    pub fn series_keys(&self) -> Vec<SeriesKey> {
        let data = self.data.read();
        let mut keys: Vec<SeriesKey> = data
            .iter()
            .map(|(k, _)| k.series_key.clone())
            .collect();
        keys.sort();
        keys.dedup();
        keys
    }

    /// Get the time range covered by this MemTable
    pub fn time_range(&self) -> Option<TimeRange> {
        let data = self.data.read();
        let first = data.iter().next();
        let last = data.iter().last();

        match (first, last) {
            (Some((first_key, _)), Some((last_key, _))) => {
                Some(TimeRange::new(first_key.timestamp, last_key.timestamp))
            }
            _ => None,
        }
    }

    /// Check if MemTable contains data for a series
    pub fn contains_series(&self, series_key: &SeriesKey) -> bool {
        let data = self.data.read();
        let start_key = MemTableKey::new(series_key.clone(), i64::MIN);
        let result = data.range(&start_key, &start_key)
            .any(|(k, _)| k.series_key == *series_key);
        result
    }

    /// Get entry count
    pub fn len(&self) -> usize {
        self.data.read().len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Immutable MemTable snapshot for flushing
pub struct ImmutableMemTable {
    inner: MemTable,
}

impl ImmutableMemTable {
    /// Create an immutable snapshot from a MemTable
    pub fn from(memtable: MemTable) -> Self {
        Self { inner: memtable }
    }

    /// Get the MemTable ID
    pub fn id(&self) -> u64 {
        self.inner.id()
    }

    /// Get the size
    pub fn size(&self) -> usize {
        self.inner.size()
    }

    /// Iterate over all entries
    pub fn iter(&self) -> Vec<(MemTableKey, DataPoint)> {
        self.inner.iter()
    }

    /// Query a range
    pub fn query(&self, series_key: &SeriesKey, time_range: &TimeRange) -> Vec<DataPoint> {
        self.inner.query(series_key, time_range)
    }

    /// Get time range
    pub fn time_range(&self) -> Option<TimeRange> {
        self.inner.time_range()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FieldValue;

    #[test]
    fn test_memtable_insert_query() {
        let memtable = MemTable::new(1);

        let key = SeriesKey::new("temperature").with_tag("sensor", "s1");
        for i in 0..100 {
            let data = DataPoint::new(i * 1000, "value", FieldValue::Float(20.0 + i as f64));
            let point = Point::new(key.clone(), data);
            memtable.insert(&point);
        }

        assert_eq!(memtable.len(), 100);

        let range = TimeRange::new(50_000, 60_000);
        let results = memtable.query(&key, &range);
        assert_eq!(results.len(), 11); // 50, 51, ..., 60
    }

    #[test]
    fn test_memtable_latest() {
        let memtable = MemTable::new(1);

        let key = SeriesKey::new("temperature");
        for i in 0..10 {
            let data = DataPoint::new(i * 1000, "value", FieldValue::Float(i as f64));
            let point = Point::new(key.clone(), data);
            memtable.insert(&point);
        }

        let latest = memtable.get_latest(&key).unwrap();
        assert_eq!(latest.timestamp, 9000);
    }
}
