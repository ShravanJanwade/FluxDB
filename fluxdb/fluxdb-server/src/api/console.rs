//! Browser-facing, versioned API. Timestamps and integer fields are decimal strings.

use super::{AppState, ErrorResponse};

use axum::{
    extract::{Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Json, Router,
};

use fluxdb_core::{DataPoint, FieldValue, Fields, Point, SeriesKey, TimeRange};

use serde::{Deserialize, Serialize};

use serde_json::{json, Value};

use std::{
    collections::{BTreeMap, VecDeque},
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
    router.route("/api/v1/telemetry", get({ let m = monitor.clone(); move || async move {

        let samples = m.samples.lock().unwrap_or_else(|e| e.into_inner()).clone();

        Json(json!({"uptime_seconds": m.started.elapsed().as_secs(), "samples": samples, "capacity":2000, "authentication_enabled":m.token.is_some()}))

    }})).layer(middleware::from_fn_with_state(monitor, observe))
}

async fn observe(State(m): State<Monitor>, request: Request, next: Next) -> Response {
    let path = request.uri().path().to_string();

    let public = path == "/health" || path == "/ping" || path == "/api/v1/health";

    if request.method() != axum::http::Method::OPTIONS && !public {
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

    if !path.ends_with("telemetry")
        && !path.ends_with("health")
        && !path.ends_with("stats")
        && path != "/metrics"
    {
        let mut samples = m.samples.lock().unwrap_or_else(|e| e.into_inner());

        if samples.len() == 2000 {
            samples.pop_front();
        }

        samples.push_back(Sample {
            time: chrono::Utc::now().timestamp_millis(),
            duration_ms: start.elapsed().as_secs_f64() * 1000.,
            status: response.status().as_u16(),
            operation,
            database: db,
        });
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
            Json(json!({"error":String::from_utf8_lossy(&bytes)})),
        ));
    }
    response
}

#[derive(Deserialize)]

pub(super) struct PointInput {
    measurement: String,
    #[serde(default)]
    tags: BTreeMap<String, String>,
    timestamp: String,
    fields: BTreeMap<String, Value>,
}

impl PointInput {
    pub(super) fn convert(self) -> Result<Point, ApiError> {
        if self.measurement.is_empty() || self.measurement.len() > 256 || self.fields.is_empty() {
            return Err(bad("Measurement and fields are required"));
        }

        let mut fields = Fields::new();

        for (key, value) in self.fields {
            if key.is_empty() {
                return Err(bad("Field names cannot be empty"));
            }

            let value =
                match value {
                    Value::Bool(v) => FieldValue::Boolean(v),
                    Value::String(v) => FieldValue::String(v),

                    Value::Number(v) => {
                        FieldValue::Float(v.as_f64().ok_or_else(|| bad("Invalid number"))?)
                    }

                    Value::Object(v) if v.len() == 1 && v.contains_key("integer") => {
                        FieldValue::Integer(
                            v["integer"]
                                .as_str()
                                .ok_or_else(|| bad("integer must be a decimal string"))?
                                .parse()
                                .map_err(bad)?,
                        )
                    }

                    _ => return Err(bad(
                        "Fields must be numbers, strings, booleans, or {integer: decimal string}",
                    )),
                };
            fields.insert(key, value);
        }

        Ok(Point::new(
            SeriesKey {
                measurement: self.measurement,
                tags: self.tags,
            },
            DataPoint {
                timestamp: self.timestamp.parse().map_err(bad)?,
                fields,
            },
        ))
    }
}

#[derive(Deserialize)]

struct Batch {
    points: Vec<PointInput>,
}

async fn write_points(
    State(engine): State<AppState>,
    Path(name): Path<String>,
    Json(batch): Json<Batch>,
) -> Result<Json<Value>, ApiError> {
    if batch.points.is_empty() || batch.points.len() > 10000 {
        return Err(bad("A batch must contain 1–10,000 points"));
    }

    let points: Vec<_> = batch
        .points
        .into_iter()
        .map(PointInput::convert)
        .collect::<Result<_, _>>()?;

    let count = points.len();
    let db = database(&engine, &name)?;

    tokio::task::spawn_blocking(move || db.write(&points))
        .await
        .map_err(internal)?
        .map_err(internal)?;

    Ok(Json(json!({"written":count})))
}

fn point_json(point: Point) -> Value {
    let fields: BTreeMap<_, _> = point
        .data
        .fields
        .0
        .into_iter()
        .map(|(k, v)| {
            (
                k,
                match v {
                    FieldValue::Float(v) => json!(v),
                    FieldValue::Integer(v) => json!({"integer":v.to_string()}),
                    FieldValue::String(v) => json!(v),
                    FieldValue::Boolean(v) => json!(v),
                },
            )
        })
        .collect();

    json!({"measurement":point.key.measurement,"tags":point.key.tags,"timestamp":point.data.timestamp.to_string(),"fields":fields})
}

#[derive(Deserialize)]

struct ReadParams {
    measurement: Option<String>,
    start: Option<String>,
    end: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
}

async fn points(
    State(engine): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<ReadParams>,
) -> Result<Json<Value>, ApiError> {
    let start: i64 = params
        .start
        .map(|s| s.parse())
        .transpose()
        .map_err(bad)?
        .unwrap_or(i64::MIN);

    let end: i64 = params
        .end
        .map(|s| s.parse())
        .transpose()
        .map_err(bad)?
        .unwrap_or(i64::MAX);

    if start > end {
        return Err(bad("start must be <= end"));
    }

    let db = database(&engine, &name)?;

    let mut points = tokio::task::spawn_blocking(move || db.points())
        .await
        .map_err(internal)?
        .map_err(internal)?;

    points.retain(|p| {
        params
            .measurement
            .as_ref()
            .map(|m| m == &p.key.measurement)
            .unwrap_or(true)
            && p.data.timestamp >= start
            && p.data.timestamp <= end
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

    Ok(Json(
        json!({"total":total,"offset":offset,"limit":limit,"points":points.into_iter().skip(offset).take(limit).map(point_json).collect::<Vec<_>>()}),
    ))
}

#[derive(Deserialize)]

struct Delete {
    measurement: String,
    #[serde(default)]
    tags: BTreeMap<String, String>,
    start: String,
    end: String,
    #[serde(default)]
    exact: bool,
}

async fn delete_points(
    State(engine): State<AppState>,
    Path(name): Path<String>,
    Json(input): Json<Delete>,
) -> Result<Json<Value>, ApiError> {
    let range = TimeRange::new(
        input.start.parse().map_err(bad)?,
        input.end.parse().map_err(bad)?,
    );

    if input.measurement.is_empty() || range.start > range.end {
        return Err(bad(
            "A measurement and valid inclusive time range are required",
        ));
    }

    let db = database(&engine, &name)?;

    let deleted = tokio::task::spawn_blocking(move || {
        db.delete_matching(&input.measurement, &input.tags, range, input.exact)
    })
    .await
    .map_err(internal)?
    .map_err(internal)?;

    Ok(Json(json!({"deleted":deleted})))
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
    if input.query.len() > 32768 {
        return Err(bad("Query exceeds 32 KiB"));
    }

    let db = database(&engine, &name)?;

    let result = tokio::task::spawn_blocking(move || db.query(&input.query))
        .await
        .map_err(internal)?
        .map_err(bad)?;

    let rows: Vec<_> = result
        .rows
        .into_iter()
        .map(|r| {
            let mut values = Vec::new();

            if let Some(t) = r.time {
                values.push(json!(t.to_string()));
            }

            if let Some(s) = r.series {
                values.push(json!(s));
            }

            values.extend(r.values.into_iter().map(|v| match v {
                fluxdb_core::query::QueryValue::Integer(i) => json!(i.to_string()),
                other => json!(other),
            }));
            values
        })
        .collect();

    Ok(Json(
        json!({"columns":result.columns,"rows":rows,"execution_time_ms":result.execution_time_ms}),
    ))
}

async fn schema(
    State(engine): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = database(&engine, &name)?;

    let points = tokio::task::spawn_blocking(move || db.points())
        .await
        .map_err(internal)?
        .map_err(internal)?;

    let mut measurements: BTreeMap<String, Value> = BTreeMap::new();

    for point in points {
        let value = measurements
            .entry(point.key.measurement)
            .or_insert_with(|| json!({"points":0,"fields":{},"tags":{}}));

        value["points"] = json!(value["points"].as_u64().unwrap() + 1);

        for (key, field) in point.data.fields.iter() {
            let kind = json!(match field {
                FieldValue::Float(_) => "float",
                FieldValue::Integer(_) => "integer",
                FieldValue::String(_) => "string",
                FieldValue::Boolean(_) => "boolean",
            });
            let types = value["fields"]
                .as_object_mut()
                .unwrap()
                .entry(key.clone())
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .unwrap();
            if !types.contains(&kind) {
                types.push(kind);
            }
        }

        for (key, tag) in point.key.tags {
            let tags = value["tags"]
                .as_object_mut()
                .unwrap()
                .entry(key)
                .or_insert_with(|| json!([]))
                .as_array_mut()
                .unwrap();
            if !tags.contains(&json!(tag)) {
                tags.push(json!(tag));
            }
        }
    }

    Ok(Json(json!({"measurements":measurements})))
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
        json!({"seconds":database(&engine,&name)?.retention_seconds()}),
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
    Ok(Json(json!({"seconds":policy.seconds})))
}
async fn export(
    State(engine): State<AppState>,
    Path(name): Path<String>,
) -> Result<Json<Value>, ApiError> {
    let db = database(&engine, &name)?;
    let retention = db.retention_seconds();
    let points = tokio::task::spawn_blocking(move || db.points())
        .await
        .map_err(internal)?
        .map_err(internal)?;
    Ok(Json(
        json!({"format":"fluxdb-json-v1","database":name,"retention_seconds":retention,"points":points.into_iter().map(point_json).collect::<Vec<_>>()}),
    ))
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
