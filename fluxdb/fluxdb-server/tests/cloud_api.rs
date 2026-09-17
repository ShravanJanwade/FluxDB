//! End-to-end tests for the multi-tenant control plane.
//!
//! These drive the real router — session cookies, authorization, tenancy,
//! quotas, API keys and ingestion — against a temporary SQLite control plane
//! and a temporary data directory. The point of interest is the tenancy
//! boundary: one account must not be able to reach another's data by any route,
//! including by guessing project or bucket ids.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use axum::Router;
use serde_json::{json, Value};
use tower::ServiceExt;

/// A browser-like caller that remembers its session cookie.
struct Client {
    app: Router,
    cookie: Option<String>,
    bearer: Option<String>,
}

impl Client {
    fn new(app: &Router) -> Self {
        Self {
            app: app.clone(),
            cookie: None,
            bearer: None,
        }
    }

    async fn send(&mut self, method: &str, path: &str, body: Option<Value>) -> (StatusCode, Value) {
        let mut request = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json");
        if let Some(cookie) = &self.cookie {
            request = request.header("cookie", format!("flux_session={cookie}"));
        }
        if let Some(bearer) = &self.bearer {
            request = request.header("authorization", format!("Bearer {bearer}"));
        }
        let body = match body {
            Some(value) => Body::from(value.to_string()),
            None => Body::empty(),
        };
        let response = self
            .app
            .clone()
            .oneshot(request.body(body).unwrap())
            .await
            .expect("router responded");
        // Sessions arrive in Set-Cookie; keep them like a browser would.
        for header in response.headers().get_all("set-cookie") {
            let value = header.to_str().unwrap_or_default();
            if let Some(rest) = value.strip_prefix("flux_session=") {
                let token = rest.split(';').next().unwrap_or_default().to_string();
                self.cookie = if token.is_empty() { None } else { Some(token) };
            }
        }
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 8 * 1024 * 1024)
            .await
            .expect("body read");
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    /// Send text rather than JSON, for line-protocol ingestion.
    async fn send_text(&mut self, path: &str, body: &str) -> (StatusCode, Value) {
        let mut request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "text/plain");
        if let Some(bearer) = &self.bearer {
            request = request.header("authorization", format!("Bearer {bearer}"));
        }
        let response = self
            .app
            .clone()
            .oneshot(request.body(Body::from(body.to_string())).unwrap())
            .await
            .expect("router responded");
        let status = response.status();
        let bytes = to_bytes(response.into_body(), 65536).await.expect("body");
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn signup(&mut self, email: &str) -> Value {
        let (status, body) = self
            .send(
                "POST",
                "/api/cloud/auth/signup",
                Some(json!({"email": email, "password": "a-long-enough-passphrase", "name": "Test Person"})),
            )
            .await;
        assert_eq!(status, StatusCode::CREATED, "signup failed: {body}");
        body
    }
}

/// Build the application over throwaway directories. The `TempDir` is returned
/// so it outlives the test.
async fn app() -> (tempfile::TempDir, Router) {
    build_app(None, None).await
}

/// `console` is the directory a built console would live in. Passing it
/// explicitly matters: it used to come from the environment, and tests running
/// in parallel raced over that single variable.
async fn app_with_console(console: Option<std::path::PathBuf>) -> (tempfile::TempDir, Router) {
    build_app(console, None).await
}

/// Build the application with the agent pointed at `gemini`, a stand-in
/// provider. Passed through configuration rather than an environment variable
/// so tests can run in parallel.
async fn build_app(
    console: Option<std::path::PathBuf>,
    gemini: Option<String>,
) -> (tempfile::TempDir, Router) {
    // Sessions are signed, so a stable secret is required; the control plane
    // and data directory are per-test.
    std::env::set_var(
        "FLUXDB_SESSION_SECRET",
        "integration-test-session-secret-value",
    );
    std::env::remove_var("FLUXDB_TOKEN");
    std::env::remove_var("DATABASE_URL");
    std::env::remove_var("FLUXDB_CONTROL_PLANE_URL");
    std::env::remove_var("FLUXDB_CLOUD");
    std::env::remove_var("GITHUB_CLIENT_ID");
    std::env::remove_var("GITHUB_CLIENT_SECRET");
    let dir = tempfile::tempdir().expect("temp dir");
    let config = fluxdb_server::ServerConfig {
        http_addr: "127.0.0.1:0".parse().unwrap(),
        data_dir: dir.path().to_path_buf(),
        static_dir: console,
        gemini_endpoint: gemini,
    };
    let (_engine, _cloud, router) = fluxdb_server::build(&config).await.expect("app builds");
    (dir, router)
}

/// The visitor's own workspace out of a session payload.
fn own_org(session: &Value) -> Value {
    session["organizations"]
        .as_array()
        .expect("organizations")
        .iter()
        .find(|org| !org["is_demo"].as_bool().unwrap_or(false))
        .cloned()
        .expect("an own workspace")
}

fn demo_org(session: &Value) -> Value {
    session["organizations"]
        .as_array()
        .expect("organizations")
        .iter()
        .find(|org| org["is_demo"].as_bool().unwrap_or(false))
        .cloned()
        .expect("the shared demo workspace")
}

#[tokio::test]
async fn signup_provisions_a_workspace_and_grants_demo_access() {
    let (_dir, app) = app().await;
    let mut client = Client::new(&app);
    let session = client.signup("ada@example.com").await;

    assert_eq!(session["account"]["email"], "ada@example.com");
    assert_eq!(session["account"]["kind"], "standard");
    // A password hash must never appear in a response.
    assert!(session["account"].get("password_hash").is_none());
    assert_eq!(session["account"]["has_password"], true);

    let own = own_org(&session);
    assert_eq!(own["role"], "owner");
    assert_eq!(own["plan"], "free");
    assert_eq!(own["projects"].as_array().unwrap().len(), 1);

    // Every account can read the shared showcase, as a viewer.
    let demo = demo_org(&session);
    assert_eq!(demo["role"], "viewer");
    assert_eq!(demo["projects"][0]["demo"], true);

    // The session cookie alone is enough for subsequent calls.
    let (status, reloaded) = client.send("GET", "/api/cloud/auth/session", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(reloaded["account"]["id"], session["account"]["id"]);

    // Signing out invalidates it.
    let (status, _) = client.send("POST", "/api/cloud/auth/logout", None).await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = client.send("GET", "/api/cloud/auth/session", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn duplicate_email_and_weak_password_are_rejected() {
    let (_dir, app) = app().await;
    let mut first = Client::new(&app);
    first.signup("grace@example.com").await;

    let mut second = Client::new(&app);
    let (status, body) = second
        .send(
            "POST",
            "/api/cloud/auth/signup",
            Some(json!({"email": "GRACE@example.com", "password": "a-long-enough-passphrase"})),
        )
        .await;
    // Address comparison is case-insensitive, so this is the same account.
    assert_eq!(status, StatusCode::CONFLICT, "{body}");

    for (email, password) in [
        ("short@example.com", "abc"),
        ("nomix@example.com", "alllowercase1"),
    ] {
        let (status, _) = second
            .send(
                "POST",
                "/api/cloud/auth/signup",
                Some(json!({"email": email, "password": password})),
            )
            .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{password} was accepted");
    }
    let (status, _) = second
        .send(
            "POST",
            "/api/cloud/auth/signup",
            Some(json!({"email": "not-an-email", "password": "a-long-enough-passphrase"})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn sign_in_requires_the_correct_password() {
    let (_dir, app) = app().await;
    let mut client = Client::new(&app);
    client.signup("linus@example.com").await;
    client.cookie = None;

    let (status, _) = client
        .send(
            "POST",
            "/api/cloud/auth/login",
            Some(json!({"email": "linus@example.com", "password": "wrong-password-entirely"})),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert!(
        client.cookie.is_none(),
        "a failed sign-in must not set a session"
    );

    let (status, body) = client
        .send(
            "POST",
            "/api/cloud/auth/login",
            Some(json!({"email": "linus@example.com", "password": "a-long-enough-passphrase"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(client.cookie.is_some());

    // An address with no account is indistinguishable from a wrong password.
    let (status, _) = client
        .send(
            "POST",
            "/api/cloud/auth/login",
            Some(json!({"email": "nobody@example.com", "password": "a-long-enough-passphrase"})),
        )
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn buckets_accept_writes_and_answer_queries() {
    let (_dir, app) = app().await;
    let mut client = Client::new(&app);
    let session = client.signup("writer@example.com").await;
    let project_id = own_org(&session)["projects"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, created) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets"),
            Some(json!({"name": "telemetry", "retention_seconds": 0})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let bucket_id = created["id"].as_str().unwrap().to_string();

    // Two names cannot collide inside one project.
    let (status, _) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets"),
            Some(json!({"name": "telemetry"})),
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);

    // Names that would escape the engine's character set are refused.
    let (status, _) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets"),
            Some(json!({"name": "has spaces"})),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let points = json!({"points": [
        {"measurement": "cpu", "tags": {"host": "api-01"}, "timestamp": "1700000000000000000", "fields": {"usage": 41.5, "cores": {"integer": "8"}}},
        {"measurement": "cpu", "tags": {"host": "api-01"}, "timestamp": "1700000060000000000", "fields": {"usage": 88.25, "cores": {"integer": "8"}}}
    ]});
    let (status, written) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets/{bucket_id}/points"),
            Some(points),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{written}");
    assert_eq!(written["written"], 2);

    let (status, page) = client
        .send(
            "GET",
            &format!("/api/cloud/projects/{project_id}/buckets/{bucket_id}/points"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["total"], 2);
    // Exact 64-bit integers survive the JSON round trip as decimal strings.
    assert_eq!(page["points"][0]["fields"]["cores"]["integer"], "8");

    let (status, result) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets/{bucket_id}/query"),
            Some(json!({"query": "SELECT MAX(usage) AS peak FROM cpu WHERE $timeFilter", "from": "1699999999000000000", "to": "1700000100000000000"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["rows"][0][0], 88.25);
    // The resolved window is echoed so a chart axis matches the data.
    assert_eq!(result["window"]["from"], "1699999999000000000");

    let (status, schema) = client
        .send(
            "GET",
            &format!("/api/cloud/projects/{project_id}/buckets/{bucket_id}/schema"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(schema["measurements"]["cpu"]["points"], 2);
    assert_eq!(schema["measurements"]["cpu"]["tags"]["host"][0], "api-01");

    let (status, deleted) = client
        .send(
            "DELETE",
            &format!("/api/cloud/projects/{project_id}/buckets/{bucket_id}/points"),
            Some(json!({"measurement": "cpu", "start": "1700000000000000000", "end": "1700000000000000000"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{deleted}");
    assert_eq!(deleted["deleted"], 1);

    // Project detail reports live storage figures for the bucket.
    let (status, detail) = client
        .send("GET", &format!("/api/cloud/projects/{project_id}"), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(detail["project"]["writable"], true);
    // A new project is provisioned with an empty `metrics` bucket, so the one
    // created here is found by name rather than by position.
    let telemetry = detail["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["name"] == "telemetry")
        .expect("the created bucket is listed");
    assert_eq!(telemetry["points"], 1);
    // Freshly written points live in the memtable; `size_bytes` counts only
    // SSTable files, so it stays zero until a flush. The console reports the
    // two separately for the same reason.
    assert!(telemetry["memtable_bytes"].as_u64().unwrap() > 0);
    assert_eq!(telemetry["size_bytes"], 0);
    let (status, _) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets/{bucket_id}/flush"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, flushed) = client
        .send("GET", &format!("/api/cloud/projects/{project_id}"), None)
        .await;
    let telemetry = flushed["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|bucket| bucket["name"] == "telemetry")
        .expect("the created bucket is listed");
    assert!(telemetry["size_bytes"].as_u64().unwrap() > 0);
    assert!(telemetry["sstables"].as_u64().unwrap() >= 1);
}

#[tokio::test]
async fn one_account_cannot_reach_another_accounts_project() {
    let (_dir, app) = app().await;

    let mut owner = Client::new(&app);
    let owner_session = owner.signup("owner@example.com").await;
    let owner_org = own_org(&owner_session);
    let owner_project = owner_org["projects"][0]["id"].as_str().unwrap().to_string();
    let owner_org_id = owner_org["id"].as_str().unwrap().to_string();
    let (status, bucket) = owner
        .send(
            "POST",
            &format!("/api/cloud/projects/{owner_project}/buckets"),
            Some(json!({"name": "private"})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED);
    let owner_bucket = bucket["id"].as_str().unwrap().to_string();
    owner
        .send(
            "POST",
            &format!("/api/cloud/projects/{owner_project}/buckets/{owner_bucket}/points"),
            Some(json!({"points": [{"measurement": "secret", "tags": {}, "timestamp": "1700000000000000000", "fields": {"value": 1.0}}]})),
        )
        .await;

    let mut intruder = Client::new(&app);
    intruder.signup("intruder@example.com").await;

    // Knowing the ids is not enough: every route resolves the owning
    // organization and checks membership first. Unauthorized reads report 404
    // rather than 403, so ids cannot be probed for existence either.
    for (method, path, body) in [
        ("GET", format!("/api/cloud/projects/{owner_project}"), None),
        (
            "GET",
            format!("/api/cloud/projects/{owner_project}/buckets/{owner_bucket}/points"),
            None,
        ),
        (
            "GET",
            format!("/api/cloud/projects/{owner_project}/buckets/{owner_bucket}/schema"),
            None,
        ),
        (
            "GET",
            format!("/api/cloud/projects/{owner_project}/buckets/{owner_bucket}/export"),
            None,
        ),
        (
            "POST",
            format!("/api/cloud/projects/{owner_project}/buckets/{owner_bucket}/query"),
            Some(json!({"query": "SELECT * FROM secret"})),
        ),
        (
            "POST",
            format!("/api/cloud/projects/{owner_project}/buckets"),
            Some(json!({"name": "intrusion"})),
        ),
        (
            "DELETE",
            format!("/api/cloud/projects/{owner_project}/buckets/{owner_bucket}"),
            None,
        ),
        (
            "GET",
            format!("/api/cloud/projects/{owner_project}/keys"),
            None,
        ),
        (
            "POST",
            format!("/api/cloud/projects/{owner_project}/buckets/{owner_bucket}/points"),
            Some(
                json!({"points": [{"measurement": "x", "tags": {}, "timestamp": "1", "fields": {"v": 1.0}}]}),
            ),
        ),
    ] {
        let (status, response) = intruder.send(method, &path, body).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "{method} {path} leaked: {status} {response}"
        );
    }

    // The organization itself is equally invisible.
    let (status, _) = intruder
        .send("GET", &format!("/api/cloud/orgs/{owner_org_id}"), None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = intruder
        .send(
            "GET",
            &format!("/api/cloud/orgs/{owner_org_id}/members"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // And the owner's data is untouched.
    let (status, page) = owner
        .send(
            "GET",
            &format!("/api/cloud/projects/{owner_project}/buckets/{owner_bucket}/points"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(page["total"], 1);
}

#[tokio::test]
async fn unauthenticated_requests_are_refused() {
    let (_dir, app) = app().await;
    let mut anonymous = Client::new(&app);

    for (method, path) in [
        ("GET", "/api/cloud/auth/session"),
        ("GET", "/api/cloud/telemetry"),
        ("GET", "/api/cloud/projects/demofluxdb99"),
        ("GET", "/api/cloud/orgs/org_fluxdb_demo"),
    ] {
        let (status, _) = anonymous.send(method, path, None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{path} was readable");
    }

    // The public configuration document is intentionally open: the sign-in page
    // needs it before there is a session.
    let (status, config) = anonymous.send("GET", "/api/cloud/config", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(config["providers"]["github"], false);
    assert_eq!(config["control_plane"], "sqlite");
}

#[tokio::test]
async fn the_single_tenant_api_is_never_anonymous_when_accounts_exist() {
    // `/api/v1` names engine databases directly, so it sits underneath the
    // tenancy boundary: one open request there would expose every account's
    // data. No FLUXDB_TOKEN is configured in these tests, which is exactly the
    // case that used to leave it open.
    let (_dir, app) = app().await;
    let mut anonymous = Client::new(&app);

    for (method, path) in [
        ("GET", "/api/v1/databases"),
        ("GET", "/api/v1/stats"),
        ("GET", "/api/v1/telemetry"),
        ("GET", "/metrics"),
        ("POST", "/api/v1/databases/anything"),
    ] {
        let (status, _) = anonymous.send(method, path, None).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "{method} {path} was reachable without the administration token"
        );
    }

    // Liveness stays public so an orchestrator can probe the instance.
    for path in ["/health", "/api/v1/health"] {
        let (status, _) = anonymous.send("GET", path, None).await;
        assert_eq!(status, StatusCode::OK, "{path} should stay public");
    }

    // And a signed-in account still reaches its own data through the cloud API.
    let mut client = Client::new(&app);
    let session = client.signup("tenant@example.com").await;
    let org = own_org(&session);
    let project = org["projects"][0]["id"].as_str().unwrap();
    let (status, _) = client
        .send("GET", &format!("/api/cloud/projects/{project}"), None)
        .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn the_shared_demo_project_is_readable_but_never_writable() {
    let (_dir, app) = app().await;
    let mut client = Client::new(&app);
    let session = client.signup("reader@example.com").await;
    let demo_project = demo_org(&session)["projects"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, detail) = client
        .send("GET", &format!("/api/cloud/projects/{demo_project}"), None)
        .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["project"]["demo"], true);
    assert_eq!(detail["project"]["writable"], false);
    let bucket_id = detail["buckets"][0]["id"].as_str().unwrap().to_string();
    assert!(
        detail["buckets"][0]["points"].as_u64().unwrap() > 5_000,
        "the showcase should be seeded: {}",
        detail["buckets"][0]
    );
    // The seeded dashboard and monitors ship with it.
    assert!(!detail["monitors"].as_array().unwrap().is_empty());

    // Reading works.
    let (status, result) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{demo_project}/buckets/{bucket_id}/query"),
            Some(json!({"query": "SELECT MAX(latency_p99) AS p99 FROM http_requests WHERE $timeFilter GROUP BY service"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert!(!result["rows"].as_array().unwrap().is_empty());

    // Writing, deleting and administering do not.
    for (method, path, body) in [
        (
            "POST",
            format!("/api/cloud/projects/{demo_project}/buckets/{bucket_id}/points"),
            Some(
                json!({"points": [{"measurement": "x", "tags": {}, "timestamp": "1", "fields": {"v": 1.0}}]}),
            ),
        ),
        (
            "DELETE",
            format!("/api/cloud/projects/{demo_project}/buckets/{bucket_id}/points"),
            Some(json!({"measurement": "cpu", "start": "0", "end": "9"})),
        ),
        (
            "POST",
            format!("/api/cloud/projects/{demo_project}/buckets"),
            Some(json!({"name": "mine"})),
        ),
        (
            "DELETE",
            format!("/api/cloud/projects/{demo_project}"),
            None,
        ),
        (
            "POST",
            format!("/api/cloud/projects/{demo_project}/keys"),
            Some(json!({"name": "key"})),
        ),
    ] {
        let (status, response) = client.send(method, &path, body).await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "{method} {path} was allowed: {response}"
        );
    }
}

#[tokio::test]
async fn project_api_keys_ingest_and_query_within_their_project_only() {
    let (_dir, app) = app().await;
    let mut client = Client::new(&app);
    let session = client.signup("agent-owner@example.com").await;
    let project_id = own_org(&session)["projects"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (_, bucket) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets"),
            Some(json!({"name": "agents"})),
        )
        .await;
    let bucket_id = bucket["id"].as_str().unwrap().to_string();

    let (status, issued) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/keys"),
            Some(json!({"name": "telegraf", "scopes": ["read", "write"]})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{issued}");
    let token = issued["token"].as_str().unwrap().to_string();
    assert!(token.starts_with("fdbk_"));
    // Listing keys must never expose the secret again.
    let (_, listed) = client
        .send(
            "GET",
            &format!("/api/cloud/projects/{project_id}/keys"),
            None,
        )
        .await;
    let serialized = listed.to_string();
    assert!(
        !serialized.contains(&token[10..]),
        "a key secret was echoed"
    );
    assert!(listed["keys"][0]["masked_token"]
        .as_str()
        .unwrap()
        .contains('•'));

    let mut agent = Client::new(&app);
    agent.bearer = Some(token.clone());

    let (status, who) = agent.send("GET", "/api/ingest/v1/whoami", None).await;
    assert_eq!(status, StatusCode::OK, "{who}");
    assert_eq!(who["project"]["id"], project_id);
    assert_eq!(who["key"]["name"], "telegraf");

    // Line protocol, the format every metrics agent already speaks.
    let (status, response) = agent
        .send_text(
            "/api/ingest/v1/write?bucket=agents&precision=s",
            "cpu,host=edge-1 usage=73.5,cores=4i 1700000000\ncpu,host=edge-2 usage=12.25 1700000000",
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT, "{response}");

    // JSON batch, for code that would rather not build line protocol.
    let (status, written) = agent
        .send(
            "POST",
            "/api/ingest/v1/points?bucket=agents",
            Some(json!({"points": [{"measurement": "cpu", "tags": {"host": "edge-3"}, "timestamp": "1700000000000000000", "fields": {"usage": 55.0}}]})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{written}");
    assert_eq!(written["written"], 1);

    let (status, result) = agent
        .send(
            "POST",
            "/api/ingest/v1/query",
            Some(json!({"bucket": "agents", "query": "SELECT COUNT(usage) AS points FROM cpu"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{result}");
    assert_eq!(result["rows"][0][0], "3");

    // The console sees exactly the same data.
    let (_, page) = client
        .send(
            "GET",
            &format!("/api/cloud/projects/{project_id}/buckets/{bucket_id}/points"),
            None,
        )
        .await;
    assert_eq!(page["total"], 3);

    // A bucket outside the key's project is not addressable by name.
    let (status, _) = agent
        .send(
            "POST",
            "/api/ingest/v1/points?bucket=production",
            Some(json!({"points": [{"measurement": "x", "tags": {}, "timestamp": "1", "fields": {"v": 1.0}}]})),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Malformed and unknown tokens are refused.
    for bad in [
        "fdbk_deadbeefdeadbeef_wrongsecretwrongsecretwrong",
        "garbage",
    ] {
        let mut impostor = Client::new(&app);
        impostor.bearer = Some(bad.to_string());
        let (status, _) = impostor.send("GET", "/api/ingest/v1/whoami", None).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{bad} was accepted");
    }

    // Revocation takes effect immediately.
    let key_id = issued["key"]["id"].as_str().unwrap().to_string();
    let (status, _) = client
        .send(
            "DELETE",
            &format!("/api/cloud/projects/{project_id}/keys/{key_id}"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = agent.send("GET", "/api/ingest/v1/whoami", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn api_key_scopes_are_enforced() {
    let (_dir, app) = app().await;
    let mut client = Client::new(&app);
    let session = client.signup("scoped@example.com").await;
    let project_id = own_org(&session)["projects"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets"),
            Some(json!({"name": "readonly"})),
        )
        .await;
    let (_, issued) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/keys"),
            Some(json!({"name": "dashboards", "scopes": ["read"]})),
        )
        .await;

    let mut reader = Client::new(&app);
    reader.bearer = Some(issued["token"].as_str().unwrap().to_string());
    let (status, response) = reader
        .send(
            "POST",
            "/api/ingest/v1/points?bucket=readonly",
            Some(json!({"points": [{"measurement": "x", "tags": {}, "timestamp": "1", "fields": {"v": 1.0}}]})),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{response}");
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("cannot write data"));

    // Reading is still permitted.
    let (status, _) = reader
        .send(
            "POST",
            "/api/ingest/v1/query",
            Some(json!({"bucket": "readonly", "query": "SELECT COUNT(v) FROM x"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn guest_workspaces_are_writable_temporary_and_pre_filled() {
    let (_dir, app) = app().await;
    let mut guest = Client::new(&app);
    let (status, session) = guest.send("POST", "/api/cloud/auth/guest", None).await;
    assert_eq!(status, StatusCode::CREATED, "{session}");
    assert_eq!(session["account"]["kind"], "guest");
    assert!(session["account"]["expires_at"].as_i64().unwrap() > 0);

    let own = own_org(&session);
    assert_eq!(own["plan"], "demo");
    let project_id = own["projects"][0]["id"].as_str().unwrap().to_string();

    let (status, detail) = guest
        .send("GET", &format!("/api/cloud/projects/{project_id}"), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    // The sandbox arrives with data so the console is not an empty shell.
    let points = detail["buckets"][0]["points"].as_u64().unwrap();
    assert!(
        (200..1_000).contains(&points),
        "sandbox had {points} points"
    );
    let bucket_id = detail["buckets"][0]["id"].as_str().unwrap().to_string();

    // A guest can write in their own sandbox.
    let (status, written) = guest
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets/{bucket_id}/points"),
            Some(json!({"points": [{"measurement": "cpu", "tags": {"host": "mine"}, "timestamp": "1700000000000000000", "fields": {"usage": 1.0}}]})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{written}");

    // But not invite members, which belongs to a real account.
    let own_org_id = own["id"].as_str().unwrap().to_string();
    let (status, response) = guest
        .send(
            "POST",
            &format!("/api/cloud/orgs/{own_org_id}/members"),
            Some(json!({"email": "someone@example.com", "role": "member"})),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{response}");

    // And the shared showcase is visible read-only, as for everyone else.
    assert_eq!(demo_org(&session)["role"], "viewer");
}

#[tokio::test]
async fn members_roles_gate_writes_and_administration() {
    let (_dir, app) = app().await;
    let mut owner = Client::new(&app);
    let owner_session = owner.signup("lead@example.com").await;
    let org_id = own_org(&owner_session)["id"].as_str().unwrap().to_string();
    let project_id = own_org(&owner_session)["projects"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // The invitee signs up first so they can be added directly.
    let mut viewer = Client::new(&app);
    let viewer_session = viewer.signup("observer@example.com").await;
    let viewer_id = viewer_session["account"]["id"]
        .as_str()
        .unwrap()
        .to_string();

    let (status, added) = owner
        .send(
            "POST",
            &format!("/api/cloud/orgs/{org_id}/members"),
            Some(json!({"email": "observer@example.com", "role": "viewer"})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{added}");
    assert_eq!(added["status"], "added");

    // A viewer can read the project but not change it.
    let (status, detail) = viewer
        .send("GET", &format!("/api/cloud/projects/{project_id}"), None)
        .await;
    assert_eq!(status, StatusCode::OK, "{detail}");
    assert_eq!(detail["project"]["role"], "viewer");
    assert_eq!(detail["project"]["writable"], false);

    let (status, response) = viewer
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets"),
            Some(json!({"name": "attempt"})),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{response}");
    let (status, _) = viewer
        .send(
            "GET",
            &format!("/api/cloud/projects/{project_id}/keys"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let (status, _) = viewer
        .send("GET", &format!("/api/cloud/orgs/{org_id}/audit"), None)
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Promoted to member, the same account can write.
    let (status, _) = owner
        .send(
            "PATCH",
            &format!("/api/cloud/orgs/{org_id}/members/{viewer_id}"),
            Some(json!({"role": "member"})),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let (status, created) = viewer
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets"),
            Some(json!({"name": "attempt"})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");

    // A non-admin cannot change anyone else's role.
    let owner_id = owner_session["account"]["id"].as_str().unwrap().to_string();
    let (status, _) = viewer
        .send(
            "PATCH",
            &format!("/api/cloud/orgs/{org_id}/members/{owner_id}"),
            Some(json!({"role": "viewer"})),
        )
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);

    // Removal leaves the viewer without access again.
    let (status, _) = owner
        .send(
            "DELETE",
            &format!("/api/cloud/orgs/{org_id}/members/{viewer_id}"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = viewer
        .send("GET", &format!("/api/cloud/projects/{project_id}"), None)
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn invitations_are_redeemed_on_sign_up() {
    let (_dir, app) = app().await;
    let mut owner = Client::new(&app);
    let owner_session = owner.signup("host@example.com").await;
    let org_id = own_org(&owner_session)["id"].as_str().unwrap().to_string();

    let (status, invited) = owner
        .send(
            "POST",
            &format!("/api/cloud/orgs/{org_id}/members"),
            Some(json!({"email": "later@example.com", "role": "member"})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{invited}");
    assert_eq!(invited["status"], "invited");

    let mut joiner = Client::new(&app);
    let joined = joiner.signup("later@example.com").await;
    let orgs = joined["organizations"].as_array().unwrap();
    let invited_org = orgs
        .iter()
        .find(|org| org["id"] == org_id.as_str())
        .expect("the invitation was applied");
    assert_eq!(invited_org["role"], "member");
}

#[tokio::test]
async fn monitors_are_validated_and_dashboards_persist() {
    let (_dir, app) = app().await;
    let mut client = Client::new(&app);
    let session = client.signup("sre@example.com").await;
    let project_id = own_org(&session)["projects"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (_, bucket) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets"),
            Some(json!({"name": "slo"})),
        )
        .await;
    let bucket_id = bucket["id"].as_str().unwrap().to_string();
    client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets/{bucket_id}/points"),
            Some(json!({"points": [{"measurement": "cpu", "tags": {}, "timestamp": "1700000000000000000", "fields": {"usage": 95.0}}]})),
        )
        .await;

    let (status, monitor) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/monitors"),
            Some(json!({
                "name": "CPU saturation",
                "bucket_id": bucket_id,
                "query": "SELECT MAX(usage) AS cpu FROM cpu",
                "comparison": "above",
                "threshold": 90.0,
                "severity": "critical"
            })),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{monitor}");
    assert_eq!(monitor["state"], "unknown");

    // A monitor whose query cannot produce a number is rejected at creation,
    // not silently left un-evaluated.
    let (status, response) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/monitors"),
            Some(json!({
                "name": "Broken",
                "bucket_id": bucket_id,
                "query": "DROP TABLE cpu",
                "comparison": "above",
                "threshold": 1.0
            })),
        )
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");

    // A monitor cannot be attached to another project's bucket.
    let (status, _) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/monitors"),
            Some(json!({
                "name": "Cross tenant",
                "bucket_id": "bkt_does_not_exist",
                "query": "SELECT MAX(usage) FROM cpu",
                "comparison": "above",
                "threshold": 1.0
            })),
        )
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let monitor_id = monitor["id"].as_str().unwrap().to_string();
    let (status, updated) = client
        .send(
            "PATCH",
            &format!("/api/cloud/projects/{project_id}/monitors/{monitor_id}"),
            Some(json!({"enabled": false, "threshold": 99.0})),
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{updated}");
    assert_eq!(updated["enabled"], false);
    assert_eq!(updated["threshold"], 99.0);

    let (status, dashboard) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/dashboards"),
            Some(json!({
                "name": "Service health",
                "panels": [{
                    "title": "CPU",
                    "kind": "line",
                    "bucket_id": bucket_id,
                    "query": "SELECT MEAN(usage) AS cpu FROM cpu WHERE $timeFilter GROUP BY time($interval)",
                    "unit": "%",
                    "span": 12
                }]
            })),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{dashboard}");
    let dashboard_id = dashboard["id"].as_str().unwrap().to_string();
    assert_eq!(dashboard["panels"][0]["span"], 12);

    let (status, listed) = client
        .send(
            "GET",
            &format!("/api/cloud/projects/{project_id}/dashboards"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(listed["dashboards"].as_array().unwrap().len(), 1);

    let (status, _) = client
        .send(
            "DELETE",
            &format!("/api/cloud/projects/{project_id}/dashboards/{dashboard_id}"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn audit_trail_records_privileged_actions() {
    let (_dir, app) = app().await;
    let mut client = Client::new(&app);
    let session = client.signup("auditor@example.com").await;
    let org_id = own_org(&session)["id"].as_str().unwrap().to_string();
    let project_id = own_org(&session)["projects"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets"),
            Some(json!({"name": "tracked"})),
        )
        .await;
    let (status, trail) = client
        .send("GET", &format!("/api/cloud/orgs/{org_id}/audit"), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    let actions: Vec<String> = trail["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["action"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(
        actions.iter().any(|action| action == "bucket.create"),
        "expected bucket.create in {actions:?}"
    );
    assert_eq!(trail["entries"][0]["actor"], "auditor@example.com");
}

#[tokio::test]
async fn self_hosted_connections_reject_unusable_targets() {
    let (_dir, app) = app().await;
    let mut client = Client::new(&app);
    let session = client.signup("hybrid@example.com").await;
    let project_id = own_org(&session)["projects"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();

    // Browser-direct is how a server on the visitor's own machine is reached:
    // nothing transits this host, so a loopback address is fine.
    let (status, created) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/connections"),
            Some(json!({"name": "My laptop", "url": "http://127.0.0.1:8086/", "mode": "browser"})),
        )
        .await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    assert_eq!(created["url"], "http://127.0.0.1:8086");

    // A proxied connection would send the token through this server, so it must
    // be HTTPS and must not point back inside the deployment's own network.
    for (name, url) in [
        ("Plain HTTP", "http://flux.example.com"),
        ("Loopback proxy", "https://localhost:8086"),
        ("Link local", "https://169.254.169.254"),
        ("Not a URL", "definitely not a url"),
    ] {
        let (status, response) = client
            .send(
                "POST",
                &format!("/api/cloud/projects/{project_id}/connections"),
                Some(json!({"name": name, "url": url, "mode": "proxy"})),
            )
            .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{name} ({url}) was accepted: {response}"
        );
    }

    let (status, listed) = client
        .send(
            "GET",
            &format!("/api/cloud/projects/{project_id}/connections"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(listed["connections"].as_array().unwrap().len(), 1);

    let connection_id = created["id"].as_str().unwrap().to_string();
    let (status, response) = client
        .send(
            "POST",
            "/api/cloud/proxy",
            Some(json!({
                "connection_id": connection_id,
                "project_id": project_id,
                "method": "GET",
                "path": "/api/v1/databases"
            })),
        )
        .await;
    // Browser-direct connections are not forwarded by this server at all.
    assert_eq!(status, StatusCode::BAD_REQUEST, "{response}");
}

#[tokio::test]
async fn github_sign_in_reports_that_it_is_not_configured() {
    let (_dir, app) = app().await;
    let mut client = Client::new(&app);
    let (status, response) = client
        .send("GET", "/api/cloud/auth/github/start", None)
        .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert!(response["error"]
        .as_str()
        .unwrap()
        .contains("not configured"));
}

#[tokio::test]
async fn sample_data_can_be_loaded_once_into_an_empty_bucket() {
    let (_dir, app) = app().await;
    let mut client = Client::new(&app);
    let session = client.signup("starter@example.com").await;
    let project_id = own_org(&session)["projects"][0]["id"]
        .as_str()
        .unwrap()
        .to_string();
    let (status, detail) = client
        .send("GET", &format!("/api/cloud/projects/{project_id}"), None)
        .await;
    assert_eq!(status, StatusCode::OK);
    // A new account's bucket starts empty: inventing data in someone's own
    // workspace without asking would be the wrong default.
    assert_eq!(detail["buckets"][0]["points"], 0);
    let bucket_id = detail["buckets"][0]["id"].as_str().unwrap().to_string();

    let (status, loaded) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets/{bucket_id}/sample"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::OK, "{loaded}");
    assert!(loaded["written"].as_u64().unwrap() > 200);

    // Loading twice would mix two generated datasets, so it is refused.
    let (status, _) = client
        .send(
            "POST",
            &format!("/api/cloud/projects/{project_id}/buckets/{bucket_id}/sample"),
            None,
        )
        .await;
    assert_eq!(status, StatusCode::CONFLICT);
}

#[tokio::test]
async fn the_console_is_served_from_the_same_process_as_the_api() {
    // One process serves the API, the static console and the single-page
    // fallback, so a deployment has no reverse proxy to misconfigure. A deep
    // link must answer 200 with the application shell: a 404 would stop the
    // browser router before it ever ran.
    let web = tempfile::tempdir().expect("temp dir");
    std::fs::write(
        web.path().join("index.html"),
        "<!doctype html><title>FluxDB</title>",
    )
    .expect("write index.html");
    std::fs::create_dir(web.path().join("assets")).expect("assets dir");
    std::fs::write(web.path().join("assets/app-abc123.js"), "export default 1;")
        .expect("write asset");

    let (_dir, app) = app_with_console(Some(web.path().to_path_buf())).await;
    let mut visitor = Client::new(&app);

    for path in ["/", "/login", "/docs", "/app/p/demofluxdb99/query"] {
        let (status, body) = visitor.send("GET", path, None).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{path} did not serve the console: {body}"
        );
    }

    // The API is unaffected by the static fallback.
    let (status, config) = visitor.send("GET", "/api/cloud/config", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(config["demo_project_id"], "demofluxdb99");

    // An unknown API path stays an API error rather than becoming the shell,
    // so a broken client sees JSON it can parse instead of a page of HTML.
    let (status, body) = visitor.send("GET", "/api/cloud/nonexistent", None).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("No API endpoint"),
        "{body}"
    );
}

/// The GitHub callback sets the session cookie and clears the OAuth nonce in
/// one response. Axum's `IntoResponseParts` for an array of header pairs
/// *inserts* each pair, so that form keeps only the last — which silently
/// dropped the session and bounced a successful sign-in back to /login.
#[tokio::test]
async fn setting_two_cookies_in_one_response_emits_both() {
    use axum::http::header::SET_COOKIE;
    use axum::response::IntoResponse;

    // The shape the callback uses.
    let headers = fluxdb_server::cloud::auth::cookie_headers([
        "flux_session=abc; Path=/; HttpOnly".to_string(),
        "flux_oauth=; Path=/; Max-Age=0".to_string(),
    ])
    .expect("valid cookie values");
    let response = (headers, axum::response::Redirect::temporary("/app")).into_response();
    let sent: Vec<_> = response
        .headers()
        .get_all(SET_COOKIE)
        .iter()
        .map(|v| v.to_str().unwrap().to_string())
        .collect();
    assert_eq!(
        sent.len(),
        2,
        "both cookies must reach the browser: {sent:?}"
    );
    assert!(sent.iter().any(|c| c.starts_with("flux_session=abc")));
    assert!(sent.iter().any(|c| c.starts_with("flux_oauth=;")));

    // The shape that caused the bug, asserted so nobody reintroduces it.
    let overwritten = (
        [
            (SET_COOKIE, "first=1".to_string()),
            (SET_COOKIE, "second=2".to_string()),
        ],
        axum::response::Redirect::temporary("/app"),
    )
        .into_response();
    assert_eq!(
        overwritten.headers().get_all(SET_COOKIE).iter().count(),
        1,
        "an array of header pairs overwrites; use auth::cookie_headers instead"
    );
}

/// The console and landing page both link to the OpenAPI document. It
/// describes the API rather than exposing it, so the administration token gate
/// must not cover it — gating it answered 401 for every visitor.
#[tokio::test]
async fn the_openapi_document_is_readable_without_a_token() {
    let (_dir, app) = app().await;
    let mut anonymous = Client::new(&app);

    let (status, document) = anonymous.send("GET", "/api/v1/openapi.json", None).await;
    assert_eq!(status, StatusCode::OK, "OpenAPI must be public");
    assert!(
        document["openapi"]
            .as_str()
            .is_some_and(|v| v.starts_with('3')),
        "a real document: {:?}",
        document["openapi"]
    );
    assert!(
        document["paths"].is_object(),
        "a usable document, not a stub"
    );

    // The rest of /api/v1 stays behind the administration token.
    let (status, _) = anonymous.send("GET", "/api/v1/databases", None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}
