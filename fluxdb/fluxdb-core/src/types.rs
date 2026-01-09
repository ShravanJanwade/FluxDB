//! Core types for FluxDB

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt;

/// Timestamp in nanoseconds since Unix epoch
pub type Timestamp = i64;

/// Series key combining measurement and tags
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct SeriesKey {
    /// Measurement name (e.g., "temperature", "cpu_usage")
    pub measurement: String,
    /// Sorted tags for consistent ordering
    pub tags: BTreeMap<String, String>,
}

impl SeriesKey {
    /// Create a new series key
    pub fn new(measurement: impl Into<String>) -> Self {
        Self {
            measurement: measurement.into(),
            tags: BTreeMap::new(),
        }
    }

    /// Add a tag to the series key
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Get the size in bytes (approximate)
    pub fn size(&self) -> usize {
        self.measurement.len()
            + self
                .tags
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
    }

    /// Create a canonical string representation for hashing
    pub fn canonical(&self) -> String {
        let mut s = self.measurement.clone();
        for (k, v) in &self.tags {
            s.push(',');
            s.push_str(k);
            s.push('=');
            s.push_str(v);
        }
        s
    }
}

impl fmt::Display for SeriesKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.canonical())
    }
}

/// A single data point with timestamp and value
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DataPoint {
    /// Timestamp in nanoseconds
    pub timestamp: Timestamp,
    /// Field values
    pub fields: Fields,
}

impl DataPoint {
    /// Create a new data point with a single field
    pub fn new(timestamp: Timestamp, field_name: impl Into<String>, value: FieldValue) -> Self {
        let mut fields = BTreeMap::new();
        fields.insert(field_name.into(), value);
        Self {
            timestamp,
            fields: Fields(fields),
        }
    }

    /// Get the size in bytes (approximate)
    pub fn size(&self) -> usize {
        8 + self.fields.size() // timestamp + fields
    }
}

/// Field values container
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Fields(pub BTreeMap<String, FieldValue>);

impl Fields {
    /// Create empty fields
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    /// Add a field
    pub fn insert(&mut self, key: impl Into<String>, value: FieldValue) {
        self.0.insert(key.into(), value);
    }

    /// Get a field value
    pub fn get(&self, key: &str) -> Option<&FieldValue> {
        self.0.get(key)
    }

    /// Get the size in bytes (approximate)
    pub fn size(&self) -> usize {
        self.0.iter()
            .map(|(k, v)| k.len() + v.size())
            .sum()
    }

    /// Iterate over fields
    pub fn iter(&self) -> impl Iterator<Item = (&String, &FieldValue)> {
        self.0.iter()
    }
}

impl Default for Fields {
    fn default() -> Self {
        Self::new()
    }
}

/// Possible field value types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FieldValue {
    /// 64-bit float
    Float(f64),
    /// 64-bit signed integer
    Integer(i64),
    /// Boolean
    Boolean(bool),
    /// String
    String(String),
}

impl FieldValue {
    /// Get the size in bytes
    pub fn size(&self) -> usize {
        match self {
            FieldValue::Float(_) => 8,
            FieldValue::Integer(_) => 8,
            FieldValue::Boolean(_) => 1,
            FieldValue::String(s) => s.len(),
        }
    }

    /// Get as f64 if possible
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            FieldValue::Float(v) => Some(*v),
            FieldValue::Integer(v) => Some(*v as f64),
            _ => None,
        }
    }

    /// Get as i64 if possible
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            FieldValue::Integer(v) => Some(*v),
            FieldValue::Float(v) => Some(*v as i64),
            _ => None,
        }
    }
}

impl From<f64> for FieldValue {
    fn from(v: f64) -> Self {
        FieldValue::Float(v)
    }
}

impl From<i64> for FieldValue {
    fn from(v: i64) -> Self {
        FieldValue::Integer(v)
    }
}

impl From<bool> for FieldValue {
    fn from(v: bool) -> Self {
        FieldValue::Boolean(v)
    }
}

impl From<String> for FieldValue {
    fn from(v: String) -> Self {
        FieldValue::String(v)
    }
}

impl From<&str> for FieldValue {
    fn from(v: &str) -> Self {
        FieldValue::String(v.to_string())
    }
}

/// A write request containing multiple data points
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WriteRequest {
    /// Database name
    pub database: String,
    /// Points to write
    pub points: Vec<Point>,
}

/// A complete point with series key and data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Point {
    /// Series key (measurement + tags)
    pub key: SeriesKey,
    /// Data point (timestamp + fields)
    pub data: DataPoint,
}

impl Point {
    /// Create a new point
    pub fn new(key: SeriesKey, data: DataPoint) -> Self {
        Self { key, data }
    }

    /// Get the size in bytes (approximate)
    pub fn size(&self) -> usize {
        self.key.size() + self.data.size()
    }
}

/// Time range for queries
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeRange {
    /// Start timestamp (inclusive)
    pub start: Timestamp,
    /// End timestamp (inclusive)
    pub end: Timestamp,
}

impl TimeRange {
    /// Create a new time range
    pub fn new(start: Timestamp, end: Timestamp) -> Self {
        Self { start, end }
    }

    /// Check if a timestamp is within the range
    pub fn contains(&self, ts: Timestamp) -> bool {
        ts >= self.start && ts <= self.end
    }

    /// Check if two ranges overlap
    pub fn overlaps(&self, other: &TimeRange) -> bool {
        self.start <= other.end && self.end >= other.start
    }

    /// Duration in nanoseconds
    pub fn duration(&self) -> i64 {
        self.end - self.start
    }
}

/// Aggregation functions
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateFunction {
    Count,
    Sum,
    Mean,
    Min,
    Max,
    First,
    Last,
    Stddev,
}

impl AggregateFunction {
    /// Parse from string
    pub fn from_str(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "count" => Some(AggregateFunction::Count),
            "sum" => Some(AggregateFunction::Sum),
            "mean" | "avg" | "average" => Some(AggregateFunction::Mean),
            "min" => Some(AggregateFunction::Min),
            "max" => Some(AggregateFunction::Max),
            "first" => Some(AggregateFunction::First),
            "last" => Some(AggregateFunction::Last),
            "stddev" => Some(AggregateFunction::Stddev),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_series_key() {
        let key = SeriesKey::new("temperature")
            .with_tag("sensor", "sensor-001")
            .with_tag("location", "building-a");

        assert_eq!(
            key.canonical(),
            "temperature,location=building-a,sensor=sensor-001"
        );
    }

    #[test]
    fn test_time_range() {
        let range1 = TimeRange::new(100, 200);
        let range2 = TimeRange::new(150, 250);
        let range3 = TimeRange::new(300, 400);

        assert!(range1.overlaps(&range2));
        assert!(!range1.overlaps(&range3));
        assert!(range1.contains(150));
        assert!(!range1.contains(250));
    }

    #[test]
    fn test_field_value() {
        let f = FieldValue::Float(3.14);
        assert_eq!(f.as_f64(), Some(3.14));
        assert_eq!(f.as_i64(), Some(3));

        let i = FieldValue::Integer(42);
        assert_eq!(i.as_f64(), Some(42.0));
        assert_eq!(i.as_i64(), Some(42));
    }
}
