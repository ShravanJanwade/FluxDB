//! Token-authenticated administration API (`/api/v1`).
//!
//! This is the surface a self-hosted FluxDB exposes: one shared bearer token
//! grants full access to every database on the instance. It is what the CLI,
//! the SDKs and the browser console's "connect your own server" path talk to.
//! Per-account, per-project authorization lives in the `cloud` module instead.

use super::data::{self, DataError};
use super::{AppState, ErrorResponse};

use axum::{
    extract::{Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Json, Router,
};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::Instant,
};

type ApiError = (StatusCode, Json<ErrorResponse>);

fn bad(e: impl ToString) -> ApiError {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: e.to_string(),
        }),
    )
}

fn internal(e: impl ToString) -> ApiError {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(ErrorResponse {
            error: e.to_string(),
        }),
    )
}

/// Caller mistakes become 400 and storage failures become 500, so a client can
/// tell "fix your request" apart from "retry later".
pub(crate) fn from_data(error: DataError) -> ApiError {
    match error {
        DataError::Invalid(message) => bad(message),
        DataError::Engine(message) => internal(message),
    }
}

fn database(
    engine: &AppState,
    name: &str,
) -> Result<Arc<fluxdb_core::storage::Database>, ApiError> {
    engine.get_database(name).ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!("Database {name} not found"),
            }),
        )
    })
}

/// Bounded in-memory request history plus the shared-token check and a
/// concurrency ceiling. The history is what the console's latency and error
/// charts read; it is reset on restart and describes server processing time
/// only.
#[derive(Clone)]
pub struct Monitor {
    started: Instant,
    samples: Arc<Mutex<VecDeque<Sample>>>,
    token: Option<String>,
    active: Arc<tokio::sync::Semaphore>,
}

#[derive(Clone, Serialize)]
struct Sample {
    time: i64,
    duration_ms: f64,
    status: u16,
    operation: String,
    database: Option<String>,
}

impl Monitor {
    pub fn new() -> Self {
        Self {
            active: Arc::new(tokio::sync::Semaphore::new(32)),
            started: Instant::now(),
            samples: Arc::new(Mutex::new(VecDeque::new())),
            token: std::env::var("FLUXDB_TOKEN").ok().filter(|s| !s.is_empty()),
        }
    }

    /// Record a request that was served outside this middleware, so cloud API
    /// traffic shows up in the same latency history as `/api/v1` traffic.
    pub fn record(
        &self,
        operation: String,
        database: Option<String>,
        status: u16,
        duration_ms: f64,
    ) {
        let mut samples = self.samples.lock().unwrap_or_else(|e| e.into_inner());
        if samples.len() == 2000 {
            samples.pop_front();
        }
        samples.push_back(Sample {
            time: chrono::Utc::now().timestamp_millis(),
            duration_ms,
            status,
            operation,
            database,
        });
    }

    pub fn telemetry(&self) -> Value {
        let samples = self
            .samples
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        json!({
            "uptime_seconds": self.started.elapsed().as_secs(),
            "samples": samples,
            "capacity": 2000,
            "authentication_enabled": self.token.is_some(),
        })
    }
}

impl Default for Monitor {
    fn default() -> Self {
        Self::new()
    }
}

pub fn routes() -> Router<AppState> {
    Router::new()
        .route(
            "/api/v1/openapi.json",
            get(|| async {
                Json(
                    serde_json::from_str::<Value>(include_str!("../../../../docs/openapi.json"))
                        .expect("valid bundled OpenAPI"),
                )
            }),
        )
        .route("/api/v1/databases", get(super::list_databases))
        .route(
            "/api/v1/databases/:name",
            post(super::create_database).delete(super::drop_database),
        )
        .route(
            "/api/v1/databases/:name/points",
            get(points).post(write_points).delete(delete_points),
        )
        .route("/api/v1/databases/:name/query", post(sql))
        .route("/api/v1/databases/:name/schema", get(schema))
        .route("/api/v1/databases/:name/flush", post(flush))
        .route("/api/v1/databases/:name/compact", post(compact))
        .route(
            "/api/v1/databases/:name/retention",
            get(retention).put(set_retention),
        )
        .route("/api/v1/databases/:name/export", get(export))
        .route("/api/v1/stats", get(super::stats))
        .route("/api/v1/health", get(super::health))
}

pub fn instrument(router: Router, monitor: Monitor) -> Router {
    router
        .route(
            "/api/v1/telemetry",
            get({
                let monitor = monitor.clone();
                move || async move { Json(monitor.telemetry()) }
            }),
        )
        .layer(middleware::from_fn_with_state(monitor, observe))
}

/// Paths served without the shared administration token. Health checks are
/// public so orchestrators can probe the instance, and the cloud module runs
/// its own account and API-key authorization for everything under its prefixes.
fn public_path(path: &str) -> bool {
    matches!(path, "/health" | "/ping" | "/api/v1/health")
        || path.starts_with("/api/cloud/")
        || path.starts_with("/api/ingest/")
}

async fn observe(State(m): State<Monitor>, request: Request, next: Next) -> Response {
    let path = request.uri().path().to_string();

    if request.method() != axum::http::Method::OPTIONS && !public_path(&path) {
        if let Some(token) = &m.token {
            let expected = format!("Bearer {token}");
            let supplied = request
                .headers()
                .get("authorization")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("");
            let diff = expected
                .bytes()
                .zip(supplied.bytes())
                .fold(expected.len() ^ supplied.len(), |a, (b, c)| {
                    a | (b ^ c) as usize
                });
            if diff != 0 {
                return axum::response::IntoResponse::into_response((
                    StatusCode::UNAUTHORIZED,
                    Json(json!({"error":"A valid bearer token is required"})),
                ));
            }
        }
    }

    let db = path
        .strip_prefix("/api/v1/databases/")
        .and_then(|s| s.split('/').next())
        .map(str::to_string);
    let operation = format!("{} {}", request.method(), path);
    let start = Instant::now();
    let response = match m.active.try_acquire() {
        Ok(_permit) => next.run(request).await,
        Err(_) => axum::response::IntoResponse::into_response((
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error":"Server is busy; retry with exponential backoff"})),
        )),
    };

    // Polling endpoints are excluded so the latency history describes real
    // data work rather than the console's own refresh loop.
    let excluded = path.ends_with("telemetry")
        || path.ends_with("health")
        || path.ends_with("stats")
        || path == "/metrics"
        || path.starts_with("/api/cloud/auth/")
        || path.starts_with("/api/cloud/overview");
    if !excluded {
        m.record(
            operation,
            db,
            response.status().as_u16(),
            start.elapsed().as_secs_f64() * 1000.,
        );
    }

    if response.status().is_client_error()
        && !response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .starts_with("application/json")
    {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 65536)
            .await
            .unwrap_or_default();
        return axum::response::IntoResponse::into_response((
            status,
            Json(json!({"error": String::from_utf8_lossy(&bytes)})),
        ));
    }
    response
}

#[derive(Deserialize)]
struct Batch {
    points: Vec<data::PointInput>,
}

async fn write_points(
    State(engine): State<AppState>,
    Path(name): Path<String>,
    Json(batch): Json<Batch>,
) -> Result<Json<Value>, ApiError> {
    let points = data::convert_batch(batch.points).map_err(from_data)?;
    let db = database(&engine, &name)?;
    let written = tokio::task::spawn_blocking(move || data::write_batch(&db, &points))
        .await
        .map_err(internal)?
        .map_err(from_data)?;
    Ok(Json(json!({"written": written})))
}

async fn points(
    State(engine): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<data::ReadParams>,
) -> Result<Json<Value>, ApiError> {
    let db = database(&engine, &name)?;
    let page = tokio::task::spawn_blocking(move || data::read_points(&db, &params))
        .await
        .map_err(internal)?
        .map_err(from_data)?;
    Ok(Json(page))
}

async fn delete_points(
    State(engine): State<AppState>,
    Path(name): Path<String>,
    Json(request): Json<data::DeleteRequest>,
) -> Result<Json<Value>, ApiError> {
    let db = database(&engine, &name)?;
    let deleted = tokio::task::spawn_blocking(move || data::delete_points(&db, &request))
        .await
        .map_err(internal)?
        .map_err(from_data)?;
    Ok(Json(json!({"deleted": deleted})))
}

#[derive(Deserialize)]
struct Sql {
    query: String,
}

async fn sql(
    State(engine): State<AppState>,
    Path(name): Path<String>,
    Json(input): Json<Sql>,
) -> Result<Json<Value>, ApiError> {
    let db = database(&engine, &name)?;
    let result = tokio::task::spawn_blocking(move || data::run_sql(&db, &input.query))
        .await
        .map_err(internal)?
        .map_err(from_data)?;
    Ok(Json(result))
}

async fn schema(
    State(engine): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = database(&engine, &name)?;
    let schema = tokio::task::spawn_blocking(move || data::schema_json(&db))
        .await
        .map_err(internal)?
        .map_err(from_data)?;
    Ok(Json(schema))
}

async fn flush(
    State(engine): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let db = database(&engine, &name)?;
    tokio::task::spawn_blocking(move || db.flush())
        .await
        .map_err(internal)?
        .map_err(internal)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn compact(
    State(engine): State<AppState>,
    Path(name): Path<String>,
) -> Result<StatusCode, ApiError> {
    let db = database(&engine, &name)?;
    tokio::task::spawn_blocking(move || db.compact())
        .await
        .map_err(internal)?
        .map_err(internal)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn retention(
    State(engine): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    Ok(Json(
        json!({"seconds": database(&engine, &name)?.retention_seconds()}),
    ))
}

#[derive(Deserialize)]
struct Policy {
    seconds: u64,
}

async fn set_retention(
    State(engine): State<AppState>,
    Path(name): Path<String>,
    Json(policy): Json<Policy>,
) -> Result<Json<Value>, ApiError> {
    let db = database(&engine, &name)?;
    tokio::task::spawn_blocking(move || db.set_retention(policy.seconds))
        .await
        .map_err(internal)?
        .map_err(bad)?;
    Ok(Json(json!({"seconds": policy.seconds})))
}

async fn export(
    State(engine): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = database(&engine, &name)?;
    let snapshot = tokio::task::spawn_blocking(move || data::export_json(&db, &name))
        .await
        .map_err(internal)?
        .map_err(from_data)?;
    Ok(Json(snapshot))
}

#[cfg(test)]
mod integration_tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::Request,
    };
    use fluxdb_core::storage::{StorageConfig, StorageEngine};
    use tower::ServiceExt;

    async fn call(
        app: &Router,
        method: &str,
        path: &str,
        body: Value,
        token: bool,
    ) -> (StatusCode, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json");
        if token {
            request = request.header("authorization", "Bearer test-secret");
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from(body.to_string())).unwrap())
            .await
            .unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 4 * 1024 * 1024)
            .await
            .unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn authenticated_crud_query_retention_export_and_telemetry() {
        let dir = tempfile::tempdir().unwrap();
        let engine = Arc::new(
            StorageEngine::new(StorageConfig {
                data_dir: dir.path().into(),
                ..Default::default()
            })
            .unwrap(),
        );
        let mut monitor = Monitor::new();
        monitor.token = Some("test-secret".into());
        let app = instrument(routes().with_state(engine), monitor);
        assert_eq!(
            call(&app, "GET", "/api/v1/health", json!(null), false)
                .await
                .0,
            StatusCode::OK
        );
        assert_eq!(
            call(&app, "GET", "/api/v1/databases", json!(null), false)
                .await
                .0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            call(&app, "POST", "/api/v1/databases/test", json!(null), true)
                .await
                .0,
            StatusCode::CREATED
        );
        assert_eq!(
            call(&app, "POST", "/api/v1/databases/test", json!(null), true)
                .await
                .0,
            StatusCode::CONFLICT
        );
        let point = json!({"measurement":"cpu","tags":{"host":"a"},"timestamp":"1789142400000000123","fields":{"value":3.5,"integer":{"integer":"9223372036854775807"},"label":"hello","ok":true}});
        assert_eq!(
            call(
                &app,
                "POST",
                "/api/v1/databases/test/points",
                json!({"points":[point.clone()]}),
                true
            )
            .await
            .0,
            StatusCode::OK
        );
        let (_, read) = call(
            &app,
            "GET",
            "/api/v1/databases/test/points",
            json!(null),
            true,
        )
        .await;
        assert_eq!(read["points"][0], point);
        let (_, result) = call(
            &app,
            "POST",
            "/api/v1/databases/test/query",
            json!({"query":"SELECT COUNT(*) FROM cpu"}),
            true,
        )
        .await;
        assert_eq!(result["rows"][0][0], "1");
        let mut edited = point.clone();
        edited["fields"] = json!({"value":8.5});
        call(
            &app,
            "POST",
            "/api/v1/databases/test/points",
            json!({"points":[edited]}),
            true,
        )
        .await;
        assert_eq!(
            call(
                &app,
                "POST",
                "/api/v1/databases/test/compact",
                json!(null),
                true
            )
            .await
            .0,
            StatusCode::NO_CONTENT
        );
        let (_, snapshot) = call(
            &app,
            "GET",
            "/api/v1/databases/test/export",
            json!(null),
            true,
        )
        .await;
        assert_eq!(snapshot["points"][0]["fields"]["value"], 8.5);
        assert_eq!(snapshot["points"][0]["fields"]["ok"], true);
        assert_eq!(
            call(
                &app,
                "PUT",
                "/api/v1/databases/test/retention",
                json!({"seconds":86400}),
                true
            )
            .await
            .0,
            StatusCode::OK
        );
        assert_eq!(
            call(
                &app,
                "GET",
                "/api/v1/databases/test/retention",
                json!(null),
                true
            )
            .await
            .1["seconds"],
            86400
        );
        assert_eq!(call(&app,"DELETE","/api/v1/databases/test/points",json!({"measurement":"cpu","start":"1789142400000000123","end":"1789142400000000123"}),true).await.1["deleted"],1);
        let (_, telemetry) = call(&app, "GET", "/api/v1/telemetry", json!(null), true).await;
        assert!(telemetry["samples"].as_array().unwrap().len() >= 10);
        assert_eq!(
            call(&app, "DELETE", "/api/v1/databases/test", json!(null), true)
                .await
                .0,
            StatusCode::NO_CONTENT
        );
    }
}
