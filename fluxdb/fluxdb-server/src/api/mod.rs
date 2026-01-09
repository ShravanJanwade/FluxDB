//! HTTP API endpoints

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Json, Response},
    routing::{get, post},
    Router,
};
use fluxdb_core::storage::StorageEngine;
use fluxdb_core::{DataPoint, FieldValue, Fields, Point, SeriesKey};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use tower_http::trace::TraceLayer;

/// Application state
pub type AppState = Arc<StorageEngine>;

/// Create the API router
pub fn create_router(engine: Arc<StorageEngine>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        // Health check
        .route("/health", get(health))
        .route("/ping", get(ping))
        
        // Write endpoint (InfluxDB compatible)
        .route("/write", post(write))
        .route("/api/v2/write", post(write_v2))
        
        // Query endpoint
        .route("/query", get(query).post(query))
        .route("/api/v2/query", post(query_v2))
        
        // Database management
        .route("/databases", get(list_databases))
        .route("/databases/:name", post(create_database).delete(drop_database))
        
        // Stats
        .route("/stats", get(stats))
        .route("/metrics", get(metrics))
        
        .layer(cors)
        .layer(TraceLayer::new_for_http())
        .with_state(engine)
}

// ============================================================================
// Request/Response types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct WriteParams {
    db: Option<String>,
    database: Option<String>,
    precision: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct QueryParams {
    db: Option<String>,
    q: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct HealthResponse {
    pub status: String,
    pub version: String,
}

#[derive(Debug, Serialize)]
pub struct StatsResponse {
    pub database_count: usize,
    pub total_entries: usize,
    pub total_size_bytes: u64,
    pub databases: Vec<DatabaseStats>,
}

#[derive(Debug, Serialize)]
pub struct DatabaseStats {
    pub name: String,
    pub memtable_size: usize,
    pub sstables: usize,
    pub total_entries: usize,
}

#[derive(Debug, Serialize)]
pub struct QueryResponse {
    pub results: Vec<QueryResult>,
}

#[derive(Debug, Serialize)]
pub struct QueryResult {
    pub statement_id: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub series: Option<Vec<SeriesResult>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct SeriesResult {
    pub name: String,
    pub columns: Vec<String>,
    pub values: Vec<Vec<serde_json::Value>>,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub error: String,
}

// ============================================================================
// Handlers
// ============================================================================

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        version: fluxdb_core::VERSION.to_string(),
    })
}

async fn ping() -> &'static str {
    "pong"
}

async fn write(
    State(engine): State<AppState>,
    Query(params): Query<WriteParams>,
    body: String,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    let db = params.db.or(params.database).unwrap_or_else(|| "default".to_string());
    let precision = params.precision.unwrap_or_else(|| "ns".to_string());

    let points = parse_line_protocol(&body, &precision)
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })))?;

    engine
        .write(&db, &points)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() })))?;

    Ok(StatusCode::NO_CONTENT)
}

async fn write_v2(
    State(engine): State<AppState>,
    Query(params): Query<WriteParams>,
    body: String,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    write(State(engine), Query(params), body).await
}

async fn query(
    State(engine): State<AppState>,
    Query(params): Query<QueryParams>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let db = params.db.unwrap_or_else(|| "default".to_string());
    let sql = params.q.ok_or_else(|| {
        (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "Missing query parameter 'q'".into() }))
    })?;

    match engine.query(&db, &sql) {
        Ok(result) => {
            let series = if result.rows.is_empty() {
                None
            } else {
                Some(vec![SeriesResult {
                    name: "result".to_string(),
                    columns: result.columns,
                    values: result.rows.into_iter().map(|row| {
                        let mut vals = Vec::new();
                        if let Some(ts) = row.time {
                            vals.push(serde_json::json!(ts));
                        }
                        if let Some(series) = row.series {
                            vals.push(serde_json::json!(series));
                        }
                        for v in row.values {
                            vals.push(match v {
                                fluxdb_core::query::QueryValue::Null => serde_json::Value::Null,
                                fluxdb_core::query::QueryValue::Float(f) => serde_json::json!(f),
                                fluxdb_core::query::QueryValue::Integer(i) => serde_json::json!(i),
                                fluxdb_core::query::QueryValue::String(s) => serde_json::json!(s),
                                fluxdb_core::query::QueryValue::Boolean(b) => serde_json::json!(b),
                            });
                        }
                        vals
                    }).collect(),
                }])
            };

            Ok(Json(QueryResponse {
                results: vec![QueryResult {
                    statement_id: 0,
                    series,
                    error: None,
                }],
            }))
        }
        Err(e) => {
            Ok(Json(QueryResponse {
                results: vec![QueryResult {
                    statement_id: 0,
                    series: None,
                    error: Some(e.to_string()),
                }],
            }))
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct QueryV2Request {
    pub query: String,
    pub database: Option<String>,
}

async fn query_v2(
    State(engine): State<AppState>,
    Json(req): Json<QueryV2Request>,
) -> Result<Json<QueryResponse>, (StatusCode, Json<ErrorResponse>)> {
    let params = QueryParams {
        db: req.database,
        q: Some(req.query),
    };
    query(State(engine), Query(params)).await
}

async fn list_databases(
    State(engine): State<AppState>,
) -> Json<Vec<String>> {
    Json(engine.list_databases())
}

async fn create_database(
    State(engine): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    engine
        .create_database(&name)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, Json(ErrorResponse { error: e.to_string() })))?;
    
    Ok(StatusCode::CREATED)
}

async fn drop_database(
    State(engine): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    engine
        .drop_database(&name)
        .map_err(|e| (StatusCode::NOT_FOUND, Json(ErrorResponse { error: e.to_string() })))?;
    
    Ok(StatusCode::NO_CONTENT)
}

async fn stats(State(engine): State<AppState>) -> Json<StatsResponse> {
    let stats = engine.stats();
    Json(StatsResponse {
        database_count: stats.database_count,
        total_entries: stats.total_entries,
        total_size_bytes: stats.total_size_bytes,
        databases: stats.databases.into_iter().map(|d| DatabaseStats {
            name: d.name,
            memtable_size: d.memtable_size,
            sstables: d.sstables,
            total_entries: d.total_entries,
        }).collect(),
    })
}

async fn metrics(State(engine): State<AppState>) -> String {
    let stats = engine.stats();
    
    // Prometheus format
    let mut output = String::new();
    output.push_str("# HELP fluxdb_databases_total Total number of databases\n");
    output.push_str("# TYPE fluxdb_databases_total gauge\n");
    output.push_str(&format!("fluxdb_databases_total {}\n", stats.database_count));
    
    output.push_str("# HELP fluxdb_entries_total Total number of data points\n");
    output.push_str("# TYPE fluxdb_entries_total gauge\n");
    output.push_str(&format!("fluxdb_entries_total {}\n", stats.total_entries));
    
    output.push_str("# HELP fluxdb_storage_bytes_total Total storage size in bytes\n");
    output.push_str("# TYPE fluxdb_storage_bytes_total gauge\n");
    output.push_str(&format!("fluxdb_storage_bytes_total {}\n", stats.total_size_bytes));
    
    for db in stats.databases {
        output.push_str(&format!(
            "fluxdb_database_entries{{database=\"{}\"}} {}\n",
            db.name, db.total_entries
        ));
    }
    
    output
}

// ============================================================================
// Line Protocol Parser
// ============================================================================

fn parse_line_protocol(data: &str, precision: &str) -> Result<Vec<Point>, String> {
    let mut points = Vec::new();
    let precision_multiplier = match precision {
        "ns" => 1,
        "us" | "u" => 1_000,
        "ms" => 1_000_000,
        "s" => 1_000_000_000,
        _ => return Err(format!("Unknown precision: {}", precision)),
    };

    for line in data.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        let point = parse_line(line, precision_multiplier)?;
        points.push(point);
    }

    Ok(points)
}

fn parse_line(line: &str, precision_multiplier: i64) -> Result<Point, String> {
    // Format: measurement,tag1=val1,tag2=val2 field1=val1,field2=val2 timestamp
    // Example: temperature,sensor=s1,location=room1 value=23.5 1609459200000000000

    let parts: Vec<&str> = line.splitn(3, ' ').collect();
    if parts.len() < 2 {
        return Err("Invalid line format".to_string());
    }

    // Parse measurement and tags
    let measurement_tags: Vec<&str> = parts[0].split(',').collect();
    let measurement = measurement_tags[0];
    
    let mut series_key = SeriesKey::new(measurement);
    for tag in measurement_tags.iter().skip(1) {
        if let Some((k, v)) = tag.split_once('=') {
            series_key = series_key.with_tag(k, v);
        }
    }

    // Parse fields
    let mut fields = Fields::new();
    for field in parts[1].split(',') {
        if let Some((k, v)) = field.split_once('=') {
            let value = parse_field_value(v)?;
            fields.insert(k, value);
        }
    }

    // Parse timestamp
    let timestamp = if parts.len() > 2 {
        parts[2]
            .parse::<i64>()
            .map_err(|_| "Invalid timestamp")?
            * precision_multiplier
    } else {
        chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
    };

    Ok(Point::new(
        series_key,
        DataPoint {
            timestamp,
            fields,
        },
    ))
}

fn parse_field_value(s: &str) -> Result<FieldValue, String> {
    // String (quoted)
    if s.starts_with('"') && s.ends_with('"') {
        return Ok(FieldValue::String(s[1..s.len()-1].to_string()));
    }
    
    // Boolean
    if s == "true" || s == "t" || s == "T" || s == "TRUE" {
        return Ok(FieldValue::Boolean(true));
    }
    if s == "false" || s == "f" || s == "F" || s == "FALSE" {
        return Ok(FieldValue::Boolean(false));
    }
    
    // Integer (ends with 'i')
    if s.ends_with('i') {
        let n = s[..s.len()-1]
            .parse::<i64>()
            .map_err(|_| "Invalid integer")?;
        return Ok(FieldValue::Integer(n));
    }
    
    // Float (default)
    let n = s
        .parse::<f64>()
        .map_err(|_| format!("Invalid field value: {}", s))?;
    Ok(FieldValue::Float(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_line_protocol() {
        let line = "temperature,sensor=s1,location=room1 value=23.5,humidity=45.2 1609459200000000000";
        let point = parse_line(line, 1).unwrap();
        
        assert_eq!(point.key.measurement, "temperature");
        assert_eq!(point.key.tags.get("sensor"), Some(&"s1".to_string()));
        assert_eq!(point.data.timestamp, 1609459200000000000);
    }

    #[test]
    fn test_parse_field_values() {
        assert!(matches!(parse_field_value("23.5"), Ok(FieldValue::Float(_))));
        assert!(matches!(parse_field_value("42i"), Ok(FieldValue::Integer(42))));
        assert!(matches!(parse_field_value("\"hello\""), Ok(FieldValue::String(_))));
        assert!(matches!(parse_field_value("true"), Ok(FieldValue::Boolean(true))));
    }
}
