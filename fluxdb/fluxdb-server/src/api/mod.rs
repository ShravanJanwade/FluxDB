mod assistant;
pub mod console;
pub mod data;
// HTTP API endpoints

use axum::http::{header, HeaderName, HeaderValue, Method};
use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json,
    routing::{get, post},
    Router,
};
use fluxdb_core::storage::StorageEngine;
use fluxdb_core::{DataPoint, FieldValue, Fields, Point, SeriesKey};
use serde::{Deserialize, Serialize};
use std::path::Path as FsPath;
use std::sync::Arc;
use tower::ServiceBuilder;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::services::{ServeDir, ServeFile};
use tower_http::set_header::SetResponseHeaderLayer;
use tower_http::trace::TraceLayer;

/// Application state
pub type AppState = Arc<StorageEngine>;

type ApiError = (StatusCode, Json<ErrorResponse>);
fn internal_error(error: impl ToString) -> ApiError {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: error.to_string(),
        }),
    )
}

/// Browser origins permitted to call this server. The development console is
/// served by Vite on another port, so the defaults cover it; a hosted console
/// on a separate domain must be listed explicitly.
pub fn allowed_origins() -> Vec<String> {
    std::env::var("FLUXDB_CORS_ORIGINS")
        .unwrap_or_else(|_| {
            "http://localhost:5173,http://127.0.0.1:5173,http://localhost:4173,http://127.0.0.1:4173"
                .into()
        })
        .split(',')
        .map(|origin| origin.trim().to_string())
        .filter(|origin| !origin.is_empty())
        .collect()
}

/// Create the API router. `cloud` carries the multi-tenant control plane when
/// it is enabled; without it the server is a plain single-tenant, token-
/// authenticated FluxDB, which is what a self-hosted deployment usually wants.
pub fn create_router(
    engine: Arc<StorageEngine>,
    cloud: Option<crate::cloud::CloudState>,
    telemetry: console::Monitor,
    static_dir: Option<&FsPath>,
) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::AllowOrigin::list(
            allowed_origins()
                .iter()
                .filter_map(|origin| origin.parse().ok())
                .collect::<Vec<axum::http::HeaderValue>>(),
        ))
        .allow_methods([
            Method::GET,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
            Method::HEAD,
        ])
        // Enumerated rather than `*`: a wildcard cannot be combined with
        // credentialed requests, and sessions travel in a cookie.
        .allow_headers([
            header::CONTENT_TYPE,
            header::AUTHORIZATION,
            HeaderName::from_static("x-flux-target-url"),
            HeaderName::from_static("x-flux-target-token"),
        ])
        .allow_credentials(true);

    let router = Router::new()
        .merge(assistant::routes())
        .merge(console::routes())
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
        .route(
            "/databases/:name",
            post(create_database).delete(drop_database),
        )
        // Stats
        .route("/stats", get(stats))
        .route("/metrics", get(metrics))
        // Anything unmatched under /api is a client mistake, not a page: answer
        // in the shape an API caller can parse instead of returning the
        // console's HTML shell.
        .route("/api/*rest", axum::routing::any(unknown_api_path))
        .layer(axum::extract::DefaultBodyLimit::max(2 * 1024 * 1024))
        .layer(TraceLayer::new_for_http())
        .with_state(engine);
    let router = match cloud {
        Some(cloud) => router.merge(crate::cloud::routes(cloud)),
        None => router,
    };
    let api = console::instrument(router, telemetry);
    // The static console is merged into a fresh router so it sits outside the
    // administration-token middleware: the browser has to be able to fetch the
    // application shell before anyone has signed in.
    let router = match static_dir {
        Some(dir) => Router::new()
            .merge(api)
            .fallback_service(console_files(dir)),
        None => api,
    };
    router
        .layer(cors)
        // Outermost, so the request-observing middleware still sees
        // uncompressed bodies when it rewrites a non-JSON error.
        .layer(CompressionLayer::new())
        .layer(SetResponseHeaderLayer::overriding(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::X_FRAME_OPTIONS,
            HeaderValue::from_static("DENY"),
        ))
        .layer(SetResponseHeaderLayer::overriding(
            header::REFERRER_POLICY,
            HeaderValue::from_static("no-referrer"),
        ))
}

async fn unknown_api_path(uri: axum::http::Uri) -> ApiError {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: format!(
                "No API endpoint at {}. See /api/v1/openapi.json for the token API, or /docs for the full reference.",
                uri.path()
            ),
        }),
    )
}

/// Static file service for the console. Unknown paths fall back to
/// `index.html`, because the console is a single-page application: a deep link
/// such as `/app/p/abc/query` has to reach the browser router rather than a
/// 404. Build outputs carry content hashes in their names, so `/assets` is
/// immutable for a year while `index.html` itself is never cached.
fn console_files(dir: &FsPath) -> Router {
    let cache = |value: &'static str| {
        SetResponseHeaderLayer::overriding(header::CACHE_CONTROL, HeaderValue::from_static(value))
    };
    let assets = ServiceBuilder::new()
        .layer(cache("public, max-age=31536000, immutable"))
        .service(ServeDir::new(dir.join("assets")));
    let documents = ServiceBuilder::new().layer(cache("no-cache")).service(
        ServeDir::new(dir)
            .append_index_html_on_directories(true)
            // `fallback`, not `not_found_service`: the latter forces a 404
            // onto the response, and a deep link into the console has to
            // answer 200 with the application shell or the browser router
            // never gets a chance to resolve it.
            .fallback(ServeFile::new(dir.join("index.html"))),
    );
    Router::new()
        .nest_service("/assets", assets)
        .fallback_service(documents)
}

// ============================================================================
// Request/Response types
// ============================================================================

#[derive(Debug, Deserialize)]
pub struct WriteParams {
    db: Option<String>,
    database: Option<String>,
    precision: Option<String>,
    bucket: Option<String>,
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
    pub retention_seconds: u64,
    pub total_size_bytes: u64,
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
    let db = params
        .db
        .or(params.database)
        .or(params.bucket)
        .unwrap_or_else(|| "default".to_string());
    let precision = params.precision.unwrap_or_else(|| "ns".to_string());

    let points = parse_line_protocol(&body, &precision)
        .map_err(|e| (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })))?;

    StorageEngine::validate_name(&db).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    tokio::task::spawn_blocking(move || engine.write(&db, &points))
        .await
        .map_err(internal_error)?
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;

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
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Missing query parameter 'q'".into(),
            }),
        )
    })?;

    if sql.len() > 32768 {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Query exceeds 32 KiB".into(),
            }),
        ));
    }
    match tokio::task::spawn_blocking(move || engine.query(&db, &sql))
        .await
        .map_err(internal_error)?
    {
        Ok(result) => {
            let series = if result.rows.is_empty() {
                None
            } else {
                Some(vec![SeriesResult {
                    name: "result".to_string(),
                    columns: result.columns,
                    values: result
                        .rows
                        .into_iter()
                        .map(|row| {
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
                                    fluxdb_core::query::QueryValue::Float(f) => {
                                        serde_json::json!(f)
                                    }
                                    fluxdb_core::query::QueryValue::Integer(i) => {
                                        serde_json::json!(i)
                                    }
                                    fluxdb_core::query::QueryValue::String(s) => {
                                        serde_json::json!(s)
                                    }
                                    fluxdb_core::query::QueryValue::Boolean(b) => {
                                        serde_json::json!(b)
                                    }
                                });
                            }
                            vals
                        })
                        .collect(),
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
        Err(e) => Ok(Json(QueryResponse {
            results: vec![QueryResult {
                statement_id: 0,
                series: None,
                error: Some(e.to_string()),
            }],
        })),
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

async fn list_databases(State(engine): State<AppState>) -> Json<Vec<String>> {
    Json(engine.list_databases())
}

async fn create_database(
    State(engine): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    fluxdb_core::storage::StorageEngine::validate_name(&name).map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: e.to_string(),
            }),
        )
    })?;
    if engine.get_database(&name).is_some() {
        return Err((
            StatusCode::CONFLICT,
            Json(ErrorResponse {
                error: "Database already exists".into(),
            }),
        ));
    }
    tokio::task::spawn_blocking(move || engine.create_database(&name))
        .await
        .map_err(internal_error)?
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;

    Ok(StatusCode::CREATED)
}

async fn drop_database(
    State(engine): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, (StatusCode, Json<ErrorResponse>)> {
    tokio::task::spawn_blocking(move || engine.drop_database(&name))
        .await
        .map_err(internal_error)?
        .map_err(|e| {
            (
                match &e {
                    fluxdb_core::FluxError::DatabaseNotFound(_) => StatusCode::NOT_FOUND,
                    fluxdb_core::FluxError::Config(_) => StatusCode::CONFLICT,
                    _ => StatusCode::INTERNAL_SERVER_ERROR,
                },
                Json(ErrorResponse {
                    error: e.to_string(),
                }),
            )
        })?;

    Ok(StatusCode::NO_CONTENT)
}

async fn stats(State(engine): State<AppState>) -> Result<Json<StatsResponse>, ApiError> {
    let stats = tokio::task::spawn_blocking(move || engine.stats())
        .await
        .map_err(internal_error)?;
    Ok(Json(StatsResponse {
        database_count: stats.database_count,
        total_entries: stats.total_entries,
        total_size_bytes: stats.total_size_bytes,
        databases: stats
            .databases
            .into_iter()
            .map(|d| DatabaseStats {
                name: d.name,
                memtable_size: d.memtable_size,
                sstables: d.sstables,
                total_entries: d.total_entries,
                retention_seconds: d.retention_seconds,
                total_size_bytes: d.total_size_bytes,
            })
            .collect(),
    }))
}

async fn metrics(State(engine): State<AppState>) -> Result<String, ApiError> {
    let stats = tokio::task::spawn_blocking(move || engine.stats())
        .await
        .map_err(internal_error)?;

    // Prometheus format
    let mut output = String::new();
    output.push_str("# HELP fluxdb_databases_total Total number of databases\n");
    output.push_str("# TYPE fluxdb_databases_total gauge\n");
    output.push_str(&format!(
        "fluxdb_databases_total {}\n",
        stats.database_count
    ));

    output.push_str("# HELP fluxdb_entries_total Total number of data points\n");
    output.push_str("# TYPE fluxdb_entries_total gauge\n");
    output.push_str(&format!("fluxdb_entries_total {}\n", stats.total_entries));

    output.push_str("# HELP fluxdb_storage_bytes_total Total storage size in bytes\n");
    output.push_str("# TYPE fluxdb_storage_bytes_total gauge\n");
    output.push_str(&format!(
        "fluxdb_storage_bytes_total {}\n",
        stats.total_size_bytes
    ));

    for db in stats.databases {
        output.push_str(&format!(
            "fluxdb_database_entries{{database=\"{}\"}} {}\n",
            db.name, db.total_entries
        ));
    }

    Ok(output)
}

// ============================================================================
// Line Protocol Parser
// ============================================================================

pub(crate) fn parse_line_protocol(data: &str, precision: &str) -> Result<Vec<Point>, String> {
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

        if points.len() >= 10000 {
            return Err("A batch accepts at most 10,000 points".into());
        }
        let point = parse_line(line, precision_multiplier)?;
        points.push(point);
    }

    if points.is_empty() {
        return Err("No points supplied".into());
    }
    if points.len() > 10000 {
        return Err("Maximum 10,000 points per batch".into());
    }
    Ok(points)
}

fn split_protocol(input: &str, delimiter: char) -> Result<Vec<&str>, String> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    let mut escaped = false;
    for (i, c) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if c == '"' {
            quoted = !quoted;
        }
        if c == delimiter && !quoted {
            parts.push(&input[start..i]);
            start = i + c.len_utf8();
        }
    }
    if quoted || escaped {
        return Err("Unterminated quote or escape".into());
    }
    parts.push(&input[start..]);
    Ok(parts)
}
fn unescape(input: &str) -> String {
    let mut out = String::new();
    let mut chars = input.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            if let Some(next) = chars.next() {
                out.push(next);
            }
        } else {
            out.push(c);
        }
    }
    out
}
fn pair(input: &str) -> Result<(String, &str), String> {
    let mut escaped = false;
    for (i, c) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        if c == '\\' {
            escaped = true;
            continue;
        }
        if c == '=' {
            let key = unescape(&input[..i]);
            let value = &input[i + 1..];
            if key.is_empty() || value.is_empty() {
                break;
            }
            return Ok((key, value));
        }
    }
    Err("Expected a nonempty key=value pair".into())
}
fn parse_line(line: &str, precision_multiplier: i64) -> Result<Point, String> {
    let parts: Vec<_> = split_protocol(line, ' ')?
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect();
    if !(2..=3).contains(&parts.len()) {
        return Err("Expected measurement fields [timestamp]".into());
    }
    let keys = split_protocol(parts[0], ',')?;
    let measurement = unescape(keys[0]);
    if measurement.is_empty() {
        return Err("Measurement cannot be empty".into());
    }
    let mut series_key = SeriesKey::new(measurement);
    for tag in keys.iter().skip(1) {
        let (k, v) = pair(tag)?;
        if series_key.tags.insert(k, unescape(v)).is_some() {
            return Err("Duplicate tag key".into());
        }
    }
    let mut fields = Fields::new();
    for field in split_protocol(parts[1], ',')? {
        let (k, v) = pair(field)?;
        if fields.0.insert(k, parse_field_value(v)?).is_some() {
            return Err("Duplicate field key".into());
        }
    }
    let timestamp = if parts.len() == 3 {
        parts[2]
            .parse::<i64>()
            .map_err(|_| "Invalid timestamp")?
            .checked_mul(precision_multiplier)
            .ok_or("Timestamp overflow")?
    } else {
        chrono::Utc::now()
            .timestamp_nanos_opt()
            .ok_or("Current time out of range")?
    };
    Ok(Point::new(series_key, DataPoint { timestamp, fields }))
}

fn parse_field_value(s: &str) -> Result<FieldValue, String> {
    // String (quoted)
    if s.starts_with('"') && s.ends_with('"') {
        if s.len() < 2 {
            return Err("Invalid quoted string".into());
        }
        return Ok(FieldValue::String(unescape(&s[1..s.len() - 1])));
    }

    // Boolean
    if s == "true" || s == "t" || s == "T" || s == "TRUE" {
        return Ok(FieldValue::Boolean(true));
    }
    if s == "false" || s == "f" || s == "F" || s == "FALSE" {
        return Ok(FieldValue::Boolean(false));
    }

    // Integer (ends with 'i')
    if let Some(digits) = s.strip_suffix('i') {
        let n = digits.parse::<i64>().map_err(|_| "Invalid integer")?;
        return Ok(FieldValue::Integer(n));
    }

    // Float (default)
    let n = s
        .parse::<f64>()
        .map_err(|_| format!("Invalid field value: {}", s))?;
    if !n.is_finite() {
        return Err("Field values must be finite".into());
    }
    Ok(FieldValue::Float(n))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_line_protocol() {
        let line =
            "temperature,sensor=s1,location=room1 value=23.5,humidity=45.2 1609459200000000000";
        let point = parse_line(line, 1).unwrap();

        assert_eq!(point.key.measurement, "temperature");
        assert_eq!(point.key.tags.get("sensor"), Some(&"s1".to_string()));
        assert_eq!(point.data.timestamp, 1609459200000000000);
    }

    #[test]
    fn test_parse_field_values() {
        assert!(matches!(
            parse_field_value("23.5"),
            Ok(FieldValue::Float(_))
        ));
        assert!(matches!(
            parse_field_value("42i"),
            Ok(FieldValue::Integer(42))
        ));
        assert!(matches!(
            parse_field_value("\"hello\""),
            Ok(FieldValue::String(_))
        ));
        assert!(matches!(
            parse_field_value("true"),
            Ok(FieldValue::Boolean(true))
        ));
    }
}

#[cfg(test)]
mod protocol_regressions {
    use super::*;
    #[test]
    fn escapes_spaces_strings_and_overflow() {
        let points = parse_line_protocol(
            r#"room\ temp,host=api\,one message="hello, world",count=42i,ok=true 100"#,
            "ms",
        )
        .unwrap();
        assert_eq!(points[0].key.measurement, "room temp");
        assert_eq!(points[0].key.tags["host"], "api,one");
        assert_eq!(
            points[0].data.fields.get("message"),
            Some(&FieldValue::String("hello, world".into()))
        );
        assert_eq!(points[0].data.timestamp, 100_000_000);
        for line in [
            "x v=NaN 0",
            "x v=1 9223372036854775807",
            "x,broken v=1 0",
            "x v=\"unclosed 0",
            "x v=1i,v=2i 0",
        ] {
            assert!(parse_line_protocol(line, "s").is_err(), "{line}");
        }
    }
}
