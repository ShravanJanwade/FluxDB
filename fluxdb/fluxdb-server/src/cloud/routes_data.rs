//! Tenant-scoped data plane.
//!
//! Three ways in, one implementation underneath:
//!
//! * a signed-in account addressing `project/bucket` from the console,
//! * a project API key posting points or SQL from an agent or SDK,
//! * a bounded proxy to a FluxDB server the user runs themselves.
//!
//! No caller ever names an engine database. Bucket ids are resolved to physical
//! databases only after the caller's role in the owning organization has been
//! checked.

use super::model::{limits, ConnectionMode, Scope};
use super::{
    bad_request, forbidden, internal, not_found, now_ns, quota_exceeded, Actor, Cloud, CloudState,
    Result,
};
use crate::api::data;
use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::sync::Arc;

pub fn routes() -> Router<CloudState> {
    Router::new()
        .route(
            "/api/cloud/projects/:project/buckets/:bucket/points",
            get(read_points).post(write_points).delete(delete_points),
        )
        .route(
            "/api/cloud/projects/:project/buckets/:bucket/query",
            post(query),
        )
        .route(
            "/api/cloud/projects/:project/buckets/:bucket/schema",
            get(schema),
        )
        .route(
            "/api/cloud/projects/:project/buckets/:bucket/flush",
            post(flush),
        )
        .route(
            "/api/cloud/projects/:project/buckets/:bucket/compact",
            post(compact),
        )
        .route(
            "/api/cloud/projects/:project/buckets/:bucket/export",
            get(export),
        )
        .route("/api/cloud/telemetry", get(telemetry))
        .route("/api/cloud/proxy", post(proxy))
        .route("/api/ingest/v1/whoami", get(whoami))
        .route("/api/ingest/v1/write", post(ingest_line_protocol))
        .route("/api/ingest/v1/points", post(ingest_points))
        .route("/api/ingest/v1/query", post(ingest_query))
}

// ============================================================================
// Query macros
// ============================================================================

/// Bucket widths a dashboard or query may request. Restricted to a list so a
/// caller cannot ask for a width that would materialize millions of groups.
const INTERVALS: &[&str] = &[
    "10s", "30s", "1m", "2m", "5m", "10m", "15m", "30m", "1h", "3h", "6h", "12h", "1d",
];

/// Default query window when a request supplies no range.
const DEFAULT_WINDOW_NS: i64 = 6 * 60 * 60 * 1_000_000_000;

#[derive(Debug, Default, Deserialize)]
pub struct Window {
    /// Inclusive start, decimal nanoseconds.
    pub from: Option<String>,
    /// Inclusive end, decimal nanoseconds.
    pub to: Option<String>,
    pub interval: Option<String>,
}

impl Window {
    fn resolve(&self) -> Result<(i64, i64, String)> {
        let to = match &self.to {
            Some(value) => value
                .parse::<i64>()
                .map_err(|_| bad_request("to must be a decimal nanosecond string"))?,
            None => now_ns(),
        };
        let from = match &self.from {
            Some(value) => value
                .parse::<i64>()
                .map_err(|_| bad_request("from must be a decimal nanosecond string"))?,
            None => to.saturating_sub(DEFAULT_WINDOW_NS),
        };
        if from > to {
            return Err(bad_request("from must be less than or equal to to"));
        }
        let interval = match &self.interval {
            Some(value) => {
                if !INTERVALS.contains(&value.as_str()) {
                    return Err(bad_request(format!(
                        "interval must be one of: {}",
                        INTERVALS.join(", ")
                    )));
                }
                value.clone()
            }
            None => auto_interval(to - from),
        };
        Ok((from, to, interval))
    }
}

/// Pick a bucket width that yields roughly 60–360 points for the range, which
/// is the band where a line chart stays readable.
fn auto_interval(span_ns: i64) -> String {
    let target = (span_ns / 180).max(1);
    for candidate in INTERVALS {
        if interval_ns(candidate) >= target {
            return (*candidate).to_string();
        }
    }
    "1d".to_string()
}

fn interval_ns(interval: &str) -> i64 {
    let (value, unit) = interval.split_at(interval.len() - 1);
    let value: i64 = value.parse().unwrap_or(1);
    value
        * match unit {
            "s" => 1_000_000_000,
            "m" => 60 * 1_000_000_000,
            "h" => 3_600 * 1_000_000_000,
            _ => 86_400 * 1_000_000_000,
        }
}

/// Substitute the macros a saved panel or example query may contain. Keeping
/// time bounds out of stored SQL is what lets one panel serve every range the
/// user picks, and it means the engine only ever sees absolute timestamps.
pub fn substitute(query: &str, from: i64, to: i64, interval: &str) -> String {
    query
        .replace("$timeFilter", &format!("time >= {from} AND time <= {to}"))
        .replace("$interval", &format!("'{interval}'"))
        .replace("$from", &from.to_string())
        .replace("$to", &to.to_string())
}

// ============================================================================
// Console data access
// ============================================================================

/// Resolve `project/bucket` for a reader, returning the open database.
async fn readable(
    cloud: &Cloud,
    actor: &Actor,
    project_id: &str,
    bucket_id: &str,
) -> Result<Arc<fluxdb_core::storage::Database>> {
    let (project, _role) = cloud.project_role(project_id, actor).await?;
    let bucket = cloud.bucket(&project, bucket_id).await?;
    cloud.open_bucket(&bucket)
}

/// Resolve `project/bucket` for a writer, enforcing role, demo read-only status
/// and the project's point quota.
async fn writable(
    cloud: &Cloud,
    actor: &Actor,
    project_id: &str,
    bucket_id: &str,
) -> Result<(
    Arc<fluxdb_core::storage::Database>,
    super::model::Bucket,
    super::model::Project,
)> {
    let project = cloud.project_writable(project_id, actor).await?;
    let bucket = cloud.bucket(&project, bucket_id).await?;
    let db = cloud.open_bucket(&bucket)?;
    Ok((db, bucket, project))
}

fn enforce_point_quota(cloud: &Cloud, project_id: &str, incoming: usize) -> Result<()> {
    let stored = cloud.project_points(project_id);
    if stored + incoming > limits::POINTS_PER_PROJECT {
        return Err(quota_exceeded(format!(
            "This project holds {stored} of {} points allowed on the hosted plan. Delete data, shorten retention, or self-host FluxDB for unlimited storage.",
            limits::POINTS_PER_PROJECT
        )));
    }
    Ok(())
}

async fn read_points(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, bucket_id)): Path<(String, String)>,
    Query(params): Query<data::ReadParams>,
) -> Result<Json<Value>> {
    let db = readable(&cloud, &actor, &project_id, &bucket_id).await?;
    let page = tokio::task::spawn_blocking(move || data::read_points(&db, &params))
        .await
        .map_err(internal)??;
    Ok(Json(page))
}

#[derive(Deserialize)]
struct Batch {
    points: Vec<data::PointInput>,
}

async fn write_points(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, bucket_id)): Path<(String, String)>,
    Json(batch): Json<Batch>,
) -> Result<Json<Value>> {
    let points = data::convert_batch(batch.points)?;
    let (db, bucket, project) = writable(&cloud, &actor, &project_id, &bucket_id).await?;
    enforce_point_quota(&cloud, &project.id, points.len())?;
    let written = tokio::task::spawn_blocking(move || data::write_batch(&db, &points))
        .await
        .map_err(internal)??;
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "data.write",
            &bucket.name,
            &format!("{written} points"),
        )
        .await;
    Ok(Json(json!({"written": written})))
}

async fn delete_points(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, bucket_id)): Path<(String, String)>,
    Json(request): Json<data::DeleteRequest>,
) -> Result<Json<Value>> {
    let (db, bucket, project) = writable(&cloud, &actor, &project_id, &bucket_id).await?;
    let measurement = request.measurement.clone();
    let deleted = tokio::task::spawn_blocking(move || data::delete_points(&db, &request))
        .await
        .map_err(internal)??;
    cloud
        .audit(
            &actor,
            &project.org_id,
            Some(&project.id),
            "data.delete",
            &format!("{}/{measurement}", bucket.name),
            &format!("{deleted} points"),
        )
        .await;
    Ok(Json(json!({"deleted": deleted})))
}

#[derive(Deserialize)]
struct QueryRequest {
    query: String,
    #[serde(flatten)]
    window: Window,
}

async fn query(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, bucket_id)): Path<(String, String)>,
    Json(request): Json<QueryRequest>,
) -> Result<Json<Value>> {
    let db = readable(&cloud, &actor, &project_id, &bucket_id).await?;
    let (from, to, interval) = request.window.resolve()?;
    let sql = substitute(&request.query, from, to, &interval);
    let result = tokio::task::spawn_blocking(move || data::run_sql(&db, &sql))
        .await
        .map_err(internal)??;
    let mut result = result;
    // Echo the resolved window so the chart axis and the data always agree.
    result["window"] =
        json!({"from": from.to_string(), "to": to.to_string(), "interval": interval});
    Ok(Json(result))
}

async fn schema(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, bucket_id)): Path<(String, String)>,
) -> Result<Json<Value>> {
    let db = readable(&cloud, &actor, &project_id, &bucket_id).await?;
    let schema = tokio::task::spawn_blocking(move || data::schema_json(&db))
        .await
        .map_err(internal)??;
    Ok(Json(schema))
}

async fn flush(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, bucket_id)): Path<(String, String)>,
) -> Result<StatusCode> {
    let (db, _, _) = writable(&cloud, &actor, &project_id, &bucket_id).await?;
    tokio::task::spawn_blocking(move || db.flush())
        .await
        .map_err(internal)?
        .map_err(internal)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn compact(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, bucket_id)): Path<(String, String)>,
) -> Result<StatusCode> {
    let (db, _, _) = writable(&cloud, &actor, &project_id, &bucket_id).await?;
    tokio::task::spawn_blocking(move || db.compact())
        .await
        .map_err(internal)?
        .map_err(internal)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn export(
    State(cloud): State<CloudState>,
    actor: Actor,
    Path((project_id, bucket_id)): Path<(String, String)>,
) -> Result<Json<Value>> {
    let (project, _) = cloud.project_role(&project_id, &actor).await?;
    let bucket = cloud.bucket(&project, &bucket_id).await?;
    let db = cloud.open_bucket(&bucket)?;
    let name = bucket.name.clone();
    let snapshot = tokio::task::spawn_blocking(move || data::export_json(&db, &name))
        .await
        .map_err(internal)??;
    Ok(Json(snapshot))
}

/// Request latency and error history. Requires a session, because it describes
/// the whole instance rather than one tenant.
async fn telemetry(State(cloud): State<CloudState>, _actor: Actor) -> Json<Value> {
    Json(cloud.telemetry.telemetry())
}

// ============================================================================
// API key ingestion
// ============================================================================

fn bearer(headers: &HeaderMap) -> Result<String> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .map(str::to_string)
        .ok_or_else(|| {
            super::unauthorized("Send your project API key as `Authorization: Bearer fdbk_…`")
        })
}

#[derive(Deserialize)]
struct BucketParam {
    bucket: Option<String>,
    /// Accepted as an alias so InfluxDB-shaped agent configuration works
    /// unchanged.
    db: Option<String>,
    precision: Option<String>,
}

async fn whoami(State(cloud): State<CloudState>, headers: HeaderMap) -> Result<Json<Value>> {
    let (key, project) = cloud.resolve_key(&bearer(&headers)?).await?;
    let buckets = cloud
        .store
        .list_by_parent::<super::model::Bucket>(&project.id)
        .await?;
    Ok(Json(json!({
        "project": {"id": project.id, "name": project.name},
        "key": {"id": key.id, "name": key.name, "scopes": key.scopes},
        "buckets": buckets.iter().map(|bucket| json!({
            "name": bucket.name,
            "retention_seconds": bucket.retention_seconds,
        })).collect::<Vec<_>>(),
    })))
}

async fn ingest_line_protocol(
    State(cloud): State<CloudState>,
    headers: HeaderMap,
    Query(params): Query<BucketParam>,
    body: String,
) -> Result<StatusCode> {
    let (key, project) = cloud.resolve_key(&bearer(&headers)?).await?;
    cloud.require_scope(&key, Scope::Write)?;
    if project.demo {
        return Err(forbidden("The shared demo project is read-only"));
    }
    let bucket = cloud
        .key_bucket(&project, params.bucket.as_deref().or(params.db.as_deref()))
        .await?;
    let points =
        crate::api::parse_line_protocol(&body, params.precision.as_deref().unwrap_or("ns"))
            .map_err(bad_request)?;
    enforce_point_quota(&cloud, &project.id, points.len())?;
    let db = cloud.open_bucket(&bucket)?;
    tokio::task::spawn_blocking(move || data::write_batch(&db, &points))
        .await
        .map_err(internal)??;
    Ok(StatusCode::NO_CONTENT)
}

async fn ingest_points(
    State(cloud): State<CloudState>,
    headers: HeaderMap,
    Query(params): Query<BucketParam>,
    Json(batch): Json<Batch>,
) -> Result<Json<Value>> {
    let (key, project) = cloud.resolve_key(&bearer(&headers)?).await?;
    cloud.require_scope(&key, Scope::Write)?;
    if project.demo {
        return Err(forbidden("The shared demo project is read-only"));
    }
    let bucket = cloud
        .key_bucket(&project, params.bucket.as_deref().or(params.db.as_deref()))
        .await?;
    let points = data::convert_batch(batch.points)?;
    enforce_point_quota(&cloud, &project.id, points.len())?;
    let db = cloud.open_bucket(&bucket)?;
    let written = tokio::task::spawn_blocking(move || data::write_batch(&db, &points))
        .await
        .map_err(internal)??;
    Ok(Json(json!({"written": written, "bucket": bucket.name})))
}

#[derive(Deserialize)]
struct IngestQuery {
    query: String,
    bucket: Option<String>,
    #[serde(flatten)]
    window: Window,
}

async fn ingest_query(
    State(cloud): State<CloudState>,
    headers: HeaderMap,
    Json(request): Json<IngestQuery>,
) -> Result<Json<Value>> {
    let (key, project) = cloud.resolve_key(&bearer(&headers)?).await?;
    cloud.require_scope(&key, Scope::Read)?;
    let bucket = cloud
        .key_bucket(&project, request.bucket.as_deref())
        .await?;
    let (from, to, interval) = request.window.resolve()?;
    let sql = substitute(&request.query, from, to, &interval);
    let db = cloud.open_bucket(&bucket)?;
    let result = tokio::task::spawn_blocking(move || data::run_sql(&db, &sql))
        .await
        .map_err(internal)??;
    Ok(Json(result))
}

// ============================================================================
// Self-hosted servers
// ============================================================================

/// Validate the shape of a target URL. Browser-mode connections are checked by
/// the browser itself; proxy-mode connections are additionally resolved and
/// screened by [`ensure_public_target`] before any request is forwarded.
pub fn normalize_target(url: &str, mode: ConnectionMode) -> Result<String> {
    let trimmed = url.trim().trim_end_matches('/');
    if trimmed.len() > 200 {
        return Err(bad_request(
            "Server addresses are limited to 200 characters",
        ));
    }
    let parsed = reqwest::Url::parse(trimmed)
        .map_err(|_| bad_request("Enter a full address such as http://127.0.0.1:8086"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(bad_request("Use an http:// or https:// address"));
    }
    if parsed.host_str().is_none() {
        return Err(bad_request("Enter an address that includes a host"));
    }
    if !parsed.path().is_empty() && parsed.path() != "/" {
        return Err(bad_request(
            "Enter only the server's base address, without a path",
        ));
    }
    if mode == ConnectionMode::Proxy && parsed.scheme() == "http" && !is_loopback_host(&parsed) {
        return Err(bad_request(
            "A proxied connection must use https, so the token is not sent in the clear",
        ));
    }
    Ok(trimmed.to_string())
}

fn is_loopback_host(url: &reqwest::Url) -> bool {
    matches!(
        url.host_str(),
        Some("localhost") | Some("127.0.0.1") | Some("[::1]")
    )
}

/// Refuse to forward requests to addresses inside the deployment's own network.
/// Without this, a proxy endpoint is an SSRF primitive: a tenant could ask the
/// server to fetch its cloud metadata service or another tenant's sidecar.
pub async fn ensure_public_target(url: &str) -> Result<()> {
    let parsed = reqwest::Url::parse(url).map_err(|_| bad_request("That address is not valid"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| bad_request("That address has no host"))?
        .to_string();
    let port = parsed.port_or_known_default().unwrap_or(443);
    let addresses = tokio::task::spawn_blocking(move || {
        use std::net::ToSocketAddrs;
        (host.as_str(), port)
            .to_socket_addrs()
            .map(|iter| iter.collect::<Vec<_>>())
    })
    .await
    .map_err(internal)?
    .map_err(|_| bad_request("That address could not be resolved from this server"))?;
    if addresses.is_empty() {
        return Err(bad_request("That address could not be resolved"));
    }
    for address in addresses {
        let ip = address.ip();
        let private = match ip {
            std::net::IpAddr::V4(v4) => {
                v4.is_private()
                    || v4.is_loopback()
                    || v4.is_link_local()
                    || v4.is_broadcast()
                    || v4.is_documentation()
                    || v4.is_unspecified()
                    || v4.octets()[0] == 0
                    // Carrier-grade NAT and the cloud metadata range.
                    || (v4.octets()[0] == 100 && (64..128).contains(&v4.octets()[1]))
            }
            std::net::IpAddr::V6(v6) => {
                v6.is_loopback()
                    || v6.is_unspecified()
                    // Unique local and link-local.
                    || (v6.segments()[0] & 0xfe00) == 0xfc00
                    || (v6.segments()[0] & 0xffc0) == 0xfe80
            }
        };
        if private {
            return Err(bad_request(
                "That address resolves inside a private network. Use browser-direct mode for a server on your own machine.",
            ));
        }
    }
    Ok(())
}

#[derive(Deserialize)]
struct ProxyRequest {
    connection_id: String,
    project_id: String,
    method: String,
    path: String,
    #[serde(default)]
    body: Option<Value>,
}

/// Forward one bounded request to a user-registered FluxDB server. The target
/// token is supplied per request and never stored; only FluxDB's own API paths
/// are reachable, and only the documented methods.
async fn proxy(
    State(cloud): State<CloudState>,
    actor: Actor,
    headers: HeaderMap,
    Json(request): Json<ProxyRequest>,
) -> Result<(StatusCode, Json<Value>)> {
    let (project, _role) = cloud.project_role(&request.project_id, &actor).await?;
    let connection = cloud
        .store
        .get::<super::model::Connection>(&request.connection_id)
        .await?
        .filter(|connection| connection.project_id == project.id)
        .ok_or_else(|| not_found("Connection not found"))?;
    if connection.mode != ConnectionMode::Proxy {
        return Err(bad_request(
            "This connection is browser-direct; the console talks to it without this server",
        ));
    }
    let method = match request.method.to_ascii_uppercase().as_str() {
        "GET" => reqwest::Method::GET,
        "POST" => reqwest::Method::POST,
        "PUT" => reqwest::Method::PUT,
        "DELETE" => reqwest::Method::DELETE,
        other => return Err(bad_request(format!("{other} requests are not forwarded"))),
    };
    let path = request.path.trim();
    let allowed = path.starts_with("/api/v1/") || path == "/health" || path == "/api/v1/health";
    if !allowed || path.contains("..") || path.len() > 300 {
        return Err(bad_request(
            "Only FluxDB API paths under /api/v1/ can be forwarded",
        ));
    }
    ensure_public_target(&connection.url).await?;
    let mut outbound = cloud
        .http
        .request(method, format!("{}{path}", connection.url))
        .timeout(std::time::Duration::from_secs(20));
    if let Some(token) = headers
        .get("x-flux-target-token")
        .and_then(|value| value.to_str().ok())
        .filter(|token| !token.is_empty())
    {
        if token.len() > 512 {
            return Err(bad_request("That token is too long"));
        }
        outbound = outbound.bearer_auth(token);
    }
    if let Some(body) = request.body {
        outbound = outbound.json(&body);
    }
    let response = outbound
        .send()
        .await
        .map_err(|e| bad_request(format!("That server could not be reached: {e}")))?;
    let status =
        StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    let text = response.text().await.unwrap_or_default();
    if text.len() > 2 * 1024 * 1024 {
        return Err(bad_request(
            "That server returned more than 2 MiB; narrow the request",
        ));
    }
    let payload = serde_json::from_str::<Value>(&text).unwrap_or_else(|_| json!({"body": text}));
    Ok((status, Json(payload)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn macros_expand_to_absolute_bounds() {
        let sql = substitute(
            "SELECT MEAN(usage) FROM cpu WHERE $timeFilter GROUP BY time($interval)",
            100,
            200,
            "5m",
        );
        assert_eq!(
            sql,
            "SELECT MEAN(usage) FROM cpu WHERE time >= 100 AND time <= 200 GROUP BY time('5m')"
        );
        assert_eq!(substitute("$from..$to", 1, 2, "1m"), "1..2");
    }

    #[test]
    fn window_resolution_defaults_and_validates() {
        let window = Window {
            from: Some("1000".into()),
            to: Some("2000".into()),
            interval: Some("5m".into()),
        };
        assert_eq!(window.resolve().unwrap(), (1000, 2000, "5m".to_string()));

        let inverted = Window {
            from: Some("2000".into()),
            to: Some("1000".into()),
            interval: None,
        };
        assert!(inverted.resolve().is_err());

        let unknown = Window {
            from: None,
            to: None,
            interval: Some("7q".into()),
        };
        assert!(unknown.resolve().is_err());

        // With no bounds the window is the default span and the interval is
        // chosen for chart readability.
        let (from, to, interval) = Window::default().resolve().unwrap();
        assert_eq!(to - from, DEFAULT_WINDOW_NS);
        assert!(INTERVALS.contains(&interval.as_str()));
    }

    #[test]
    fn auto_interval_keeps_series_lengths_reasonable() {
        for span_minutes in [5i64, 30, 60, 360, 1440, 10080] {
            let span = span_minutes * 60 * 1_000_000_000;
            let interval = auto_interval(span);
            let buckets = span / interval_ns(&interval);
            assert!(
                (1..=400).contains(&buckets),
                "{span_minutes}m -> {interval} gives {buckets} buckets"
            );
        }
    }

    #[test]
    fn target_urls_must_be_bare_http_endpoints() {
        assert_eq!(
            normalize_target("http://127.0.0.1:8086/", ConnectionMode::Browser).unwrap(),
            "http://127.0.0.1:8086"
        );
        assert_eq!(
            normalize_target("https://flux.example.com", ConnectionMode::Proxy).unwrap(),
            "https://flux.example.com"
        );
        for bad in [
            "not-a-url",
            "ftp://example.com",
            "http://example.com/api/v1",
            &format!("https://{}.com", "x".repeat(250)),
        ] {
            assert!(
                normalize_target(bad, ConnectionMode::Browser).is_err(),
                "{bad} should be rejected"
            );
        }
        // Plain HTTP to a remote host would put the token on the wire.
        assert!(normalize_target("http://flux.example.com", ConnectionMode::Proxy).is_err());
        assert!(normalize_target("http://localhost:8086", ConnectionMode::Proxy).is_ok());
    }

    #[tokio::test]
    async fn proxy_targets_inside_private_networks_are_refused() {
        for blocked in [
            "http://127.0.0.1:8086",
            "http://10.0.0.5:8086",
            "http://192.168.1.10:8086",
            "http://169.254.169.254",
            "http://[::1]:8086",
        ] {
            assert!(
                ensure_public_target(blocked).await.is_err(),
                "{blocked} should be refused"
            );
        }
    }
}
