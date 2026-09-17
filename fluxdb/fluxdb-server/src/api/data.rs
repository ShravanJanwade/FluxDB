//! Data-plane operations shared by the token-authenticated API (`/api/v1`,
//! used by self-hosted servers and the CLI/SDKs) and the tenant-scoped cloud
//! API (`/api/cloud`, used by signed-in accounts and project API keys).
//!
//! Everything here is synchronous and takes an already-resolved [`Database`],
//! so authorization and tenancy stay in the routing layers and the actual
//! storage work happens once, in one place, inside `spawn_blocking`.

use fluxdb_core::storage::Database;
use fluxdb_core::{DataPoint, FieldValue, Fields, Point, SeriesKey, TimeRange};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

/// Separates caller mistakes from storage failures so each routing layer can
/// map them onto its own status codes and error envelopes.
#[derive(Debug)]
pub enum DataError {
    Invalid(String),
    Engine(String),
}

impl DataError {
    pub fn message(&self) -> &str {
        match self {
            DataError::Invalid(message) | DataError::Engine(message) => message,
        }
    }
}

impl std::fmt::Display for DataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message())
    }
}

fn invalid(message: impl ToString) -> DataError {
    DataError::Invalid(message.to_string())
}

fn engine(message: impl ToString) -> DataError {
    DataError::Engine(message.to_string())
}

pub type Result<T> = std::result::Result<T, DataError>;

/// Maximum points accepted in one batch. Matches the line-protocol limit.
pub const MAX_BATCH: usize = 10_000;
/// Maximum accepted SQL length.
pub const MAX_SQL_BYTES: usize = 32 * 1024;

/// One point as the JSON API accepts it. Timestamps are decimal nanosecond
/// strings and exact integers are `{"integer": "…"}`, because IEEE-754 doubles
/// cannot round-trip the full `i64` range through JSON.
#[derive(Debug, Clone, Deserialize)]
pub struct PointInput {
    pub measurement: String,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    pub timestamp: String,
    pub fields: BTreeMap<String, Value>,
}

impl PointInput {
    pub fn convert(self) -> Result<Point> {
        if self.measurement.is_empty() || self.measurement.len() > 256 {
            return Err(invalid("A measurement of 1–256 characters is required"));
        }
        if self.fields.is_empty() {
            return Err(invalid("At least one field is required"));
        }
        let mut fields = Fields::new();
        for (key, value) in self.fields {
            if key.is_empty() {
                return Err(invalid("Field names cannot be empty"));
            }
            let value = match value {
                Value::Bool(v) => FieldValue::Boolean(v),
                Value::String(v) => FieldValue::String(v),
                Value::Number(v) => FieldValue::Float(
                    v.as_f64()
                        .filter(|f| f.is_finite())
                        .ok_or_else(|| invalid(format!("Field {key} is not a finite number")))?,
                ),
                Value::Object(v) if v.len() == 1 && v.contains_key("integer") => {
                    FieldValue::Integer(
                        v["integer"]
                            .as_str()
                            .ok_or_else(|| {
                                invalid(format!("Field {key} integer must be a decimal string"))
                            })?
                            .parse()
                            .map_err(|_| invalid(format!("Field {key} is not a 64-bit integer")))?,
                    )
                }
                _ => {
                    return Err(invalid(format!(
                    "Field {key} must be a number, string, boolean, or {{\"integer\": \"decimal\"}}"
                )))
                }
            };
            fields.insert(key, value);
        }
        Ok(Point::new(
            SeriesKey {
                measurement: self.measurement,
                tags: self.tags,
            },
            DataPoint {
                timestamp: self
                    .timestamp
                    .parse()
                    .map_err(|_| invalid("Timestamps are decimal nanosecond strings"))?,
                fields,
            },
        ))
    }
}

pub fn convert_batch(points: Vec<PointInput>) -> Result<Vec<Point>> {
    if points.is_empty() {
        return Err(invalid("A batch must contain at least one point"));
    }
    if points.len() > MAX_BATCH {
        return Err(invalid(format!(
            "A batch accepts at most {MAX_BATCH} points"
        )));
    }
    points.into_iter().map(PointInput::convert).collect()
}

pub fn point_json(point: Point) -> Value {
    let fields: BTreeMap<_, _> = point
        .data
        .fields
        .0
        .into_iter()
        .map(|(key, value)| {
            (
                key,
                match value {
                    FieldValue::Float(v) => json!(v),
                    FieldValue::Integer(v) => json!({"integer": v.to_string()}),
                    FieldValue::String(v) => json!(v),
                    FieldValue::Boolean(v) => json!(v),
                },
            )
        })
        .collect();
    json!({
        "measurement": point.key.measurement,
        "tags": point.key.tags,
        "timestamp": point.data.timestamp.to_string(),
        "fields": fields,
    })
}

pub fn write_batch(db: &Database, points: &[Point]) -> Result<usize> {
    db.write(points).map_err(engine)?;
    Ok(points.len())
}

#[derive(Debug, Default, Clone, Deserialize)]
pub struct ReadParams {
    pub measurement: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

pub fn read_points(db: &Database, params: &ReadParams) -> Result<Value> {
    let start: i64 = match &params.start {
        Some(value) => value
            .parse()
            .map_err(|_| invalid("start must be a decimal nanosecond string"))?,
        None => i64::MIN,
    };
    let end: i64 = match &params.end {
        Some(value) => value
            .parse()
            .map_err(|_| invalid("end must be a decimal nanosecond string"))?,
        None => i64::MAX,
    };
    if start > end {
        return Err(invalid("start must be less than or equal to end"));
    }
    let mut points = db.points().map_err(engine)?;
    points.retain(|point| {
        params
            .measurement
            .as_ref()
            .map(|m| m == &point.key.measurement)
            .unwrap_or(true)
            && point.data.timestamp >= start
            && point.data.timestamp <= end
    });
    points.sort_by(|a, b| {
        b.data
            .timestamp
            .cmp(&a.data.timestamp)
            .then(a.key.cmp(&b.key))
    });
    let total = points.len();
    let offset = params.offset.unwrap_or(0);
    let limit = params.limit.unwrap_or(100).clamp(1, 1000);
    Ok(json!({
        "total": total,
        "offset": offset,
        "limit": limit,
        "points": points
            .into_iter()
            .skip(offset)
            .take(limit)
            .map(point_json)
            .collect::<Vec<_>>(),
    }))
}

#[derive(Debug, Clone, Deserialize)]
pub struct DeleteRequest {
    pub measurement: String,
    #[serde(default)]
    pub tags: BTreeMap<String, String>,
    pub start: String,
    pub end: String,
    /// When true the supplied tag set must match a series exactly, instead of
    /// being treated as a filter.
    #[serde(default)]
    pub exact: bool,
}

pub fn delete_points(db: &Database, request: &DeleteRequest) -> Result<usize> {
    let range = TimeRange::new(
        request
            .start
            .parse()
            .map_err(|_| invalid("start must be a decimal nanosecond string"))?,
        request
            .end
            .parse()
            .map_err(|_| invalid("end must be a decimal nanosecond string"))?,
    );
    if request.measurement.is_empty() {
        return Err(invalid("A measurement is required"));
    }
    if range.start > range.end {
        return Err(invalid("start must be less than or equal to end"));
    }
    db.delete_matching(&request.measurement, &request.tags, range, request.exact)
        .map_err(engine)
}

/// Run SQL and shape the result for the browser: timestamps and 64-bit
/// integers become decimal strings so no precision is lost in JSON.
pub fn run_sql(db: &Database, sql: &str) -> Result<Value> {
    if sql.len() > MAX_SQL_BYTES {
        return Err(invalid("Query exceeds 32 KiB"));
    }
    if sql.trim().is_empty() {
        return Err(invalid("Enter a query"));
    }
    let result = db.query(sql).map_err(invalid)?;
    let rows: Vec<Vec<Value>> = result
        .rows
        .into_iter()
        .map(|row| {
            let mut values = Vec::new();
            if let Some(time) = row.time {
                values.push(json!(time.to_string()));
            }
            if let Some(series) = row.series {
                values.push(json!(series));
            }
            values.extend(row.values.into_iter().map(|value| match value {
                fluxdb_core::query::QueryValue::Integer(v) => json!(v.to_string()),
                other => json!(other),
            }));
            values
        })
        .collect();
    Ok(json!({
        "columns": result.columns,
        "rows": rows,
        "execution_time_ms": result.execution_time_ms,
    }))
}

/// Numeric value of the first column of the first row, used by monitors.
pub fn scalar(db: &Database, sql: &str) -> Result<Option<f64>> {
    let result = db.query(sql).map_err(invalid)?;
    let Some(row) = result.rows.into_iter().next() else {
        return Ok(None);
    };
    let from_values = row.values.into_iter().find_map(|value| match value {
        fluxdb_core::query::QueryValue::Float(v) => Some(v),
        fluxdb_core::query::QueryValue::Integer(v) => Some(v as f64),
        _ => None,
    });
    Ok(from_values)
}

pub fn schema_json(db: &Database) -> Result<Value> {
    let points = db.points().map_err(engine)?;
    let mut measurements: BTreeMap<String, Value> = BTreeMap::new();
    for point in points {
        let entry = measurements
            .entry(point.key.measurement)
            .or_insert_with(|| json!({"points": 0, "fields": {}, "tags": {}}));
        entry["points"] = json!(entry["points"].as_u64().unwrap_or(0) + 1);
        for (key, field) in point.data.fields.iter() {
            let kind = json!(match field {
                FieldValue::Float(_) => "float",
                FieldValue::Integer(_) => "integer",
                FieldValue::String(_) => "string",
                FieldValue::Boolean(_) => "boolean",
            });
            let types = entry["fields"]
                .as_object_mut()
                .expect("fields is an object")
                .entry(key.clone())
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .expect("field types is an array");
            if !types.contains(&kind) {
                types.push(kind);
            }
        }
        for (key, tag) in point.key.tags {
            let values = entry["tags"]
                .as_object_mut()
                .expect("tags is an object")
                .entry(key)
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .expect("tag values is an array");
            if !values.contains(&json!(tag)) {
                values.push(json!(tag));
            }
        }
    }
    Ok(json!({"measurements": measurements}))
}

pub fn export_json(db: &Database, display_name: &str) -> Result<Value> {
    let retention = db.retention_seconds();
    let points = db.points().map_err(engine)?;
    Ok(json!({
        "format": "fluxdb-json-v1",
        "database": display_name,
        "retention_seconds": retention,
        "points": points.into_iter().map(point_json).collect::<Vec<_>>(),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluxdb_core::storage::{StorageConfig, StorageEngine};
    use std::sync::Arc;

    fn database() -> (tempfile::TempDir, Arc<Database>) {
        let dir = tempfile::tempdir().unwrap();
        let engine = StorageEngine::new(StorageConfig {
            data_dir: dir.path().into(),
            ..Default::default()
        })
        .unwrap();
        let db = engine.create_database("test").unwrap();
        (dir, db)
    }

    fn input(timestamp: &str, usage: Value) -> PointInput {
        PointInput {
            measurement: "cpu".into(),
            tags: BTreeMap::from([("host".to_string(), "api-01".to_string())]),
            timestamp: timestamp.into(),
            fields: BTreeMap::from([("usage".to_string(), usage)]),
        }
    }

    #[test]
    fn rejects_field_shapes_json_cannot_represent_exactly() {
        assert!(input("1", json!({"integer": "9223372036854775807"}))
            .convert()
            .is_ok());
        for bad in [
            json!({"integer": 12}),
            json!({"integer": "not-a-number"}),
            json!([1, 2]),
            json!(null),
        ] {
            let error = input("1", bad.clone()).convert().unwrap_err();
            assert!(matches!(error, DataError::Invalid(_)), "{bad} -> {error:?}");
        }
        assert!(matches!(
            input("not-a-timestamp", json!(1.0)).convert().unwrap_err(),
            DataError::Invalid(_)
        ));
        assert!(convert_batch(vec![]).is_err());
    }

    #[test]
    fn reads_writes_queries_and_exports_round_trip_through_the_engine() {
        let (_dir, db) = database();
        let points = convert_batch(vec![
            input("1700000000000000001", json!(41.5)),
            input("1700000000000000002", json!(88.25)),
        ])
        .unwrap();
        assert_eq!(write_batch(&db, &points).unwrap(), 2);

        let page = read_points(&db, &ReadParams::default()).unwrap();
        assert_eq!(page["total"], 2);
        // Newest first.
        assert_eq!(page["points"][0]["timestamp"], "1700000000000000002");

        let filtered = read_points(
            &db,
            &ReadParams {
                start: Some("1700000000000000002".into()),
                ..Default::default()
            },
        )
        .unwrap();
        assert_eq!(filtered["total"], 1);
        assert!(read_points(
            &db,
            &ReadParams {
                start: Some("5".into()),
                end: Some("1".into()),
                ..Default::default()
            }
        )
        .is_err());

        let result = run_sql(&db, "SELECT COUNT(*) FROM cpu").unwrap();
        assert_eq!(result["rows"][0][0], "2");
        assert!(matches!(
            run_sql(&db, "DROP TABLE cpu").unwrap_err(),
            DataError::Invalid(_)
        ));
        assert!(run_sql(&db, "   ").is_err());

        let schema = schema_json(&db).unwrap();
        assert_eq!(schema["measurements"]["cpu"]["points"], 2);
        assert_eq!(schema["measurements"]["cpu"]["fields"]["usage"][0], "float");
        assert_eq!(schema["measurements"]["cpu"]["tags"]["host"][0], "api-01");

        let snapshot = export_json(&db, "friendly-name").unwrap();
        assert_eq!(snapshot["database"], "friendly-name");
        assert_eq!(snapshot["points"].as_array().unwrap().len(), 2);

        let deleted = delete_points(
            &db,
            &DeleteRequest {
                measurement: "cpu".into(),
                tags: BTreeMap::new(),
                start: "1700000000000000001".into(),
                end: "1700000000000000001".into(),
                exact: false,
            },
        )
        .unwrap();
        assert_eq!(deleted, 1);
        assert_eq!(
            read_points(&db, &ReadParams::default()).unwrap()["total"],
            1
        );
    }

    #[test]
    fn scalar_extracts_the_first_numeric_cell_for_monitor_evaluation() {
        let (_dir, db) = database();
        let points = convert_batch(vec![
            input("1700000000000000001", json!(40.0)),
            input("1700000000000000002", json!(60.0)),
        ])
        .unwrap();
        write_batch(&db, &points).unwrap();
        assert_eq!(
            scalar(&db, "SELECT MEAN(usage) FROM cpu").unwrap(),
            Some(50.0)
        );
        assert_eq!(
            scalar(&db, "SELECT MEAN(usage) FROM absent_measurement").unwrap(),
            None
        );
        assert!(scalar(&db, "SELECT nonsense FROM").is_err());
    }
}
