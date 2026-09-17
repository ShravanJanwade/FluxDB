//! FluxDB Cloud: the multi-tenant control plane.
//!
//! The data plane (`fluxdb-core`) knows only about databases. This module adds
//! the layer a hosted product needs on top of it: accounts, organizations,
//! projects, buckets, roles, project API keys, saved dashboards, threshold
//! monitors and an audit trail.
//!
//! Tenancy is enforced by name. Every project owns a private prefix of engine
//! database names (`t{project_id}_{bucket}`) and no request can name an engine
//! database directly: callers address a bucket by id, the control plane
//! resolves the project it belongs to, checks the caller's role in the owning
//! organization, and only then maps to a physical database. A signed-in
//! account therefore cannot read, write or drop another tenant's data even by
//! guessing names.

pub mod agent;
pub mod auth;
pub mod model;
mod routes_auth;
mod routes_data;
mod routes_workspace;
pub mod seed;
pub mod store;

use crate::api::data;
use axum::{
    extract::FromRequestParts,
    http::{request::Parts, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json, Router,
};
use fluxdb_core::storage::{Database, StorageEngine};
use model::{
    limits, Account, AccountKind, ApiKey, AuditEntry, Bucket, Member, Monitor, MonitorState, Org,
    Project, Role, Scope, Session,
};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use store::MetaStore;

/// Stable identifiers for the shared, read-only showcase workspace. They are
/// fixed rather than generated because the demo project id is part of its
/// engine database names, which must survive a restart.
pub const DEMO_ORG_ID: &str = "org_fluxdb_demo";
pub const DEMO_PROJECT_ID: &str = "demofluxdb99";
pub const DEMO_SLUG: &str = "fluxdb-demo";
/// Placeholder account id recorded as the creator of showcase records.
const SYSTEM_ACCOUNT: &str = "system";
/// Upper bound on concurrently live guest workspaces, so anonymous traffic
/// cannot exhaust the shared instance's memory.
const MAX_LIVE_GUESTS: i64 = 400;

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub fn now_ns() -> i64 {
    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
}

// ============================================================================
// Errors
// ============================================================================

/// A failed request. `code` is a stable machine-readable string the browser
/// switches on (for example to show the "verify your email" or "upgrade to
/// self-hosted" path), while `message` is written for a human.
#[derive(Debug)]
pub struct Fail {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
}

impl IntoResponse for Fail {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"error": self.message, "code": self.code})),
        )
            .into_response()
    }
}

impl std::fmt::Display for Fail {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl Fail {
    pub fn service_unavailable(message: impl Into<String>) -> Fail {
        Fail {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "unavailable",
            message: message.into(),
        }
    }

    /// Relay an upstream status without flattening it. A provider's 429 has to
    /// stay a 429 so the browser can tell "slow down" apart from "broken".
    pub fn from_status(status: u16, message: impl Into<String>) -> Fail {
        let status = StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY);
        Fail {
            status,
            code: match status {
                StatusCode::TOO_MANY_REQUESTS => "rate_limited",
                StatusCode::BAD_REQUEST => "invalid_request",
                StatusCode::SERVICE_UNAVAILABLE => "unavailable",
                _ => "upstream",
            },
            message: message.into(),
        }
    }
}

pub fn bad_request(message: impl Into<String>) -> Fail {
    Fail {
        status: StatusCode::BAD_REQUEST,
        code: "invalid_request",
        message: message.into(),
    }
}

pub fn unauthorized(message: impl Into<String>) -> Fail {
    Fail {
        status: StatusCode::UNAUTHORIZED,
        code: "unauthenticated",
        message: message.into(),
    }
}

pub fn forbidden(message: impl Into<String>) -> Fail {
    Fail {
        status: StatusCode::FORBIDDEN,
        code: "forbidden",
        message: message.into(),
    }
}

pub fn not_found(message: impl Into<String>) -> Fail {
    Fail {
        status: StatusCode::NOT_FOUND,
        code: "not_found",
        message: message.into(),
    }
}

pub fn conflict(message: impl Into<String>) -> Fail {
    Fail {
        status: StatusCode::CONFLICT,
        code: "conflict",
        message: message.into(),
    }
}

pub fn quota_exceeded(message: impl Into<String>) -> Fail {
    Fail {
        status: StatusCode::PAYMENT_REQUIRED,
        code: "quota_exceeded",
        message: message.into(),
    }
}

pub fn throttled(message: impl Into<String>) -> Fail {
    Fail {
        status: StatusCode::TOO_MANY_REQUESTS,
        code: "rate_limited",
        message: message.into(),
    }
}

pub fn internal(message: impl std::fmt::Display) -> Fail {
    // Storage and control-plane failures are logged with detail and reported to
    // the caller without leaking internals.
    tracing::error!("control plane failure: {message}");
    Fail {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        code: "internal",
        message: "The request could not be completed. Try again shortly.".into(),
    }
}

impl From<store::StoreError> for Fail {
    fn from(error: store::StoreError) -> Self {
        match error {
            store::StoreError::Conflict(_) => conflict("That name is already taken"),
            other => internal(other),
        }
    }
}

impl From<data::DataError> for Fail {
    fn from(error: data::DataError) -> Self {
        match error {
            data::DataError::Invalid(message) => bad_request(message),
            data::DataError::Engine(message) => internal(message),
        }
    }
}

pub type Result<T> = std::result::Result<T, Fail>;

// ============================================================================
// Rate limiting
// ============================================================================

/// Fixed-window counters used to slow down credential stuffing and to bound
/// anonymous workspace creation. Deliberately in-process: a single-instance
/// deployment needs nothing more, and a shared limiter would be the wrong
/// abstraction to guess at before there are several instances.
#[derive(Default)]
pub struct Throttle {
    windows: HashMap<String, (i64, u32)>,
}

impl Throttle {
    /// Returns false when `key` has already used its allowance for the current
    /// window.
    pub fn allow(&mut self, key: &str, limit: u32, window_ms: i64, now: i64) -> bool {
        if self.windows.len() > 20_000 {
            self.windows
                .retain(|_, (start, _)| now - *start < window_ms);
        }
        let entry = self.windows.entry(key.to_string()).or_insert((now, 0));
        if now - entry.0 >= window_ms {
            *entry = (now, 0);
        }
        entry.1 += 1;
        entry.1 <= limit
    }

    /// How much of `key`'s allowance the current window has consumed. Read-only:
    /// asking must not itself count against the limit.
    pub fn used(&mut self, key: &str, window_ms: i64, now: i64) -> u32 {
        match self.windows.get(key) {
            Some((start, count)) if now - *start < window_ms => *count,
            _ => 0,
        }
    }

    pub fn reset(&mut self, key: &str) {
        self.windows.remove(key);
    }
}

// ============================================================================
// State
// ============================================================================

pub struct Cloud {
    pub store: MetaStore,
    pub engine: Arc<StorageEngine>,
    pub secrets: auth::Secrets,
    pub http: reqwest::Client,
    /// One pooled client for the AI provider. Held here rather than built per
    /// round so connections are reused, and so a test can point the whole agent
    /// at a stand-in provider without touching a global.
    pub gemini: crate::gemini::Client,
    pub throttle: Mutex<Throttle>,
    /// Requests served by the cloud API are recorded in the same latency
    /// history the console charts.
    pub telemetry: crate::api::console::Monitor,
}

pub type CloudState = Arc<Cloud>;

impl Cloud {
    pub async fn open(
        engine: Arc<StorageEngine>,
        control_plane_url: &str,
        telemetry: crate::api::console::Monitor,
        gemini_endpoint: Option<&str>,
    ) -> anyhow::Result<Arc<Self>> {
        let store = MetaStore::connect(control_plane_url).await?;
        let (secrets, notes) = auth::Secrets::from_env();
        for note in notes {
            tracing::warn!("{note}");
        }
        let cloud = Arc::new(Self {
            store,
            engine,
            secrets,
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(15))
                .user_agent(concat!("fluxdb/", env!("CARGO_PKG_VERSION")))
                .build()?,
            throttle: Mutex::new(Throttle::default()),
            telemetry,
            gemini: match gemini_endpoint {
                Some(endpoint) => crate::gemini::Client::with_endpoint(endpoint),
                None => crate::gemini::Client::new(),
            },
        });
        tracing::info!("Control plane ready on {} storage", cloud.store.backend());
        cloud.bootstrap_demo().await?;
        Ok(cloud)
    }

    /// Whether `Secure` should be set on cookies for this request. A local HTTP
    /// run must not set it or the browser discards the session.
    pub fn secure_cookies(&self, headers: &HeaderMap) -> bool {
        if let Some(base) = &self.secrets.public_base_url {
            return base.starts_with("https://");
        }
        headers
            .get("x-forwarded-proto")
            .and_then(|v| v.to_str().ok())
            .map(|proto| proto.split(',').next().unwrap_or("").trim() == "https")
            .unwrap_or(false)
    }

    /// Absolute base URL used to build OAuth callbacks. Falls back to the
    /// forwarded host, which is validated to look like a hostname so a crafted
    /// `Host` header cannot redirect the flow elsewhere.
    pub fn base_url(&self, headers: &HeaderMap) -> Option<String> {
        if let Some(base) = &self.secrets.public_base_url {
            return Some(base.clone());
        }
        let host = headers
            .get("x-forwarded-host")
            .or_else(|| headers.get("host"))
            .and_then(|v| v.to_str().ok())?;
        let valid = !host.is_empty()
            && host.len() <= 255
            && host
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._:[]".contains(&b));
        if !valid {
            return None;
        }
        let scheme = if self.secure_cookies(headers) {
            "https"
        } else {
            "http"
        };
        Some(format!("{scheme}://{host}"))
    }

    // ---- authorization ----------------------------------------------------

    pub async fn account(&self, id: &str) -> Result<Account> {
        self.store
            .get::<Account>(id)
            .await?
            .ok_or_else(|| unauthorized("Sign in again"))
    }

    pub async fn membership(&self, org_id: &str, account_id: &str) -> Result<Member> {
        self.store
            .get::<Member>(&format!("{org_id}:{account_id}"))
            .await?
            .ok_or_else(|| not_found("Workspace not found"))
    }

    pub async fn org(&self, id: &str) -> Result<Org> {
        self.store
            .get::<Org>(id)
            .await?
            .ok_or_else(|| not_found("Workspace not found"))
    }

    /// Resolve a project and the caller's role in it. Returns 404 rather than
    /// 403 for projects the caller is not a member of, so project ids cannot be
    /// probed for existence.
    pub async fn project_role(&self, project_id: &str, actor: &Actor) -> Result<(Project, Role)> {
        let project = self
            .store
            .get::<Project>(project_id)
            .await?
            .ok_or_else(|| not_found("Project not found"))?;
        let role = self
            .membership(&project.org_id, &actor.account.id)
            .await
            .map_err(|_| not_found("Project not found"))?
            .role;
        Ok((project, role))
    }

    /// Resolve a project for a caller who must be able to change data in it.
    /// The shared showcase project is read-only for everyone.
    pub async fn project_writable(&self, project_id: &str, actor: &Actor) -> Result<Project> {
        let (project, role) = self.project_role(project_id, actor).await?;
        if project.demo {
            return Err(forbidden(
                "The shared demo workspace is read-only. Create a project in your own workspace to write data.",
            ));
        }
        if !role.can_write() {
            return Err(forbidden("Your role in this workspace allows reading only"));
        }
        Ok(project)
    }

    pub async fn project_admin(&self, project_id: &str, actor: &Actor) -> Result<Project> {
        let (project, role) = self.project_role(project_id, actor).await?;
        if project.demo {
            return Err(forbidden("The shared demo workspace cannot be modified"));
        }
        if !role.can_administer() {
            return Err(forbidden(
                "Only workspace admins and owners can change this setting",
            ));
        }
        Ok(project)
    }

    /// Resolve a bucket together with the project it belongs to, verifying the
    /// bucket really is a child of that project.
    pub async fn bucket(&self, project: &Project, bucket_id: &str) -> Result<Bucket> {
        let bucket = self
            .store
            .get::<Bucket>(bucket_id)
            .await?
            .ok_or_else(|| not_found("Bucket not found"))?;
        if bucket.project_id != project.id {
            return Err(not_found("Bucket not found"));
        }
        Ok(bucket)
    }

    /// Open the engine database backing a bucket, creating it on first use so a
    /// freshly created bucket is immediately writable.
    pub fn open_bucket(&self, bucket: &Bucket) -> Result<Arc<Database>> {
        if let Some(db) = self.engine.get_database(&bucket.namespace) {
            return Ok(db);
        }
        self.engine
            .get_or_create_database(&bucket.namespace)
            .map_err(internal)
    }

    // ---- quotas -----------------------------------------------------------

    /// Organization owning a project, for audit entries that only have a
    /// project id to hand.
    pub async fn project_org(&self, project_id: &str) -> Result<String> {
        Ok(self
            .store
            .get::<Project>(project_id)
            .await?
            .map(|project| project.org_id)
            .unwrap_or_default())
    }

    /// A compact digest of recent request history for the agent.
    ///
    /// The raw sample buffer holds two thousand entries, which is far more than
    /// a model context should carry, so this reduces it to percentiles, an error
    /// rate and the slowest operations — the shape a person actually reads when
    /// troubleshooting.
    pub fn telemetry_summary(&self) -> serde_json::Value {
        use serde_json::json;
        let raw = self.telemetry.telemetry();
        let samples: Vec<&serde_json::Value> = raw["samples"]
            .as_array()
            .map(|s| s.iter().collect())
            .unwrap_or_default();
        let mut durations: Vec<f64> = samples
            .iter()
            .filter_map(|sample| sample["duration_ms"].as_f64())
            .collect();
        durations.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let percentile = |fraction: f64| -> Option<f64> {
            if durations.is_empty() {
                return None;
            }
            let index = ((fraction * durations.len() as f64) as usize).min(durations.len() - 1);
            Some((durations[index] * 1000.0).round() / 1000.0)
        };
        let failures: Vec<&&serde_json::Value> = samples
            .iter()
            .filter(|sample| {
                sample["status"]
                    .as_u64()
                    .is_some_and(|status| status >= 400)
            })
            .collect();
        // Group failures by status and operation so a repeated fault reads as
        // one line with a count rather than hundreds of samples.
        let mut grouped: std::collections::BTreeMap<String, usize> =
            std::collections::BTreeMap::new();
        for failure in &failures {
            let key = format!(
                "{} {}",
                failure["status"].as_u64().unwrap_or(0),
                failure["operation"].as_str().unwrap_or("?")
            );
            *grouped.entry(key).or_default() += 1;
        }
        let mut slowest: Vec<&&serde_json::Value> = samples.iter().collect();
        slowest.sort_by(|a, b| {
            b["duration_ms"]
                .as_f64()
                .unwrap_or(0.0)
                .partial_cmp(&a["duration_ms"].as_f64().unwrap_or(0.0))
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        json!({
            "uptime_seconds": raw["uptime_seconds"],
            "requests_sampled": samples.len(),
            "sample_capacity": raw["capacity"],
            "note": "A bounded in-memory history of recent requests, reset on restart. Server processing time only, not network latency.",
            "latency_ms": {"p50": percentile(0.50), "p95": percentile(0.95), "p99": percentile(0.99),
                "max": durations.last().copied()},
            "errors": {
                "count": failures.len(),
                "rate": if samples.is_empty() { None } else {
                    Some(((failures.len() as f64 / samples.len() as f64) * 10_000.0).round() / 10_000.0)
                },
                "by_operation": grouped.into_iter().map(|(key, count)| json!({"operation": key, "count": count}))
                    .take(15).collect::<Vec<_>>(),
            },
            "slowest": slowest.into_iter().take(10).map(|sample| json!({
                "operation": sample["operation"], "duration_ms": sample["duration_ms"],
                "status": sample["status"],
            })).collect::<Vec<_>>(),
        })
    }

    /// Stored points across every bucket of a project.
    pub fn project_points(&self, project_id: &str) -> usize {
        let prefix = format!("t{project_id}_");
        self.engine
            .stats()
            .databases
            .into_iter()
            .filter(|db| db.name.starts_with(&prefix))
            .map(|db| db.total_entries)
            .sum()
    }

    pub async fn check_quota<T: store::Entity>(
        &self,
        parent: &str,
        limit: i64,
        noun: &str,
    ) -> Result<()> {
        if self.store.count::<T>(Some(parent)).await? >= limit {
            return Err(quota_exceeded(format!(
                "This workspace is limited to {limit} {noun} on the hosted plan. Self-host FluxDB for unlimited {noun}."
            )));
        }
        Ok(())
    }

    // ---- audit ------------------------------------------------------------

    /// Append an audit entry. Failures are logged and swallowed: losing an
    /// audit line must not fail the operation the user asked for, but it must
    /// not be silent either.
    pub async fn audit(
        &self,
        actor: &Actor,
        org_id: &str,
        project_id: Option<&str>,
        action: &str,
        target: &str,
        detail: &str,
    ) {
        let entry = AuditEntry {
            id: format!("{}_{}", now_ms(), auth::random_hex(6)),
            org_id: org_id.to_string(),
            project_id: project_id.map(str::to_string),
            account_id: actor.account.id.clone(),
            actor: actor.account.email.clone(),
            action: action.to_string(),
            target: target.to_string(),
            detail: detail.to_string(),
            at: now_ms(),
        };
        if let Err(error) = self.store.create(&entry).await {
            tracing::warn!("audit entry {action} on {target} was not recorded: {error}");
        }
    }

    // ---- sessions ---------------------------------------------------------

    pub async fn start_session(
        &self,
        account: &Account,
        user_agent: &str,
    ) -> Result<(String, i64)> {
        let token = auth::random_token(32);
        let lifetime = if account.is_guest() {
            limits::GUEST_SESSION_LIFETIME_SECONDS
        } else {
            limits::SESSION_LIFETIME_SECONDS
        };
        let session = Session {
            id: auth::sha256_hex(&token),
            account_id: account.id.clone(),
            created_at: now_ms(),
            expires_at: now_ms() + lifetime * 1000,
            user_agent: user_agent.chars().take(200).collect(),
        };
        self.store.create(&session).await?;
        Ok((token, lifetime))
    }

    pub async fn end_session(&self, token: &str) {
        let _ = self.store.delete::<Session>(&auth::sha256_hex(token)).await;
    }

    // ---- workspace creation ----------------------------------------------

    /// Create an organization owned by `account`, with `account` as Owner.
    pub async fn create_org(&self, account: &Account, name: &str, plan: &str) -> Result<Org> {
        let base = model::slugify(name);
        let mut slug = base.clone();
        // Slugs are globally unique because they appear in shareable URLs.
        for attempt in 0..6 {
            if self.store.find::<Org>(&slug).await?.is_none() {
                break;
            }
            slug = format!("{base}-{}", auth::random_id(4 + attempt));
        }
        let org = Org {
            id: format!("org_{}", auth::random_id(14)),
            name: name.to_string(),
            slug,
            owner_id: account.id.clone(),
            plan: plan.to_string(),
            created_at: now_ms(),
            expires_at: account.expires_at,
        };
        self.store.create(&org).await?;
        self.store
            .create(&Member {
                org_id: org.id.clone(),
                account_id: account.id.clone(),
                email: account.email.clone(),
                name: account.name.clone(),
                role: Role::Owner,
                created_at: now_ms(),
            })
            .await?;
        Ok(org)
    }

    pub async fn create_project(
        &self,
        account_id: &str,
        org_id: &str,
        name: &str,
        description: &str,
    ) -> Result<Project> {
        let base = model::slugify(name);
        let mut slug = base.clone();
        for attempt in 0..6 {
            if self
                .store
                .find::<Project>(&format!("{org_id}/{slug}"))
                .await?
                .is_none()
            {
                break;
            }
            slug = format!("{base}-{}", auth::random_id(4 + attempt));
        }
        let project = Project {
            id: auth::random_id(12),
            org_id: org_id.to_string(),
            name: name.to_string(),
            slug,
            description: description.to_string(),
            created_by: account_id.to_string(),
            created_at: now_ms(),
            demo: false,
        };
        self.store.create(&project).await?;
        Ok(project)
    }

    pub async fn create_bucket(
        &self,
        account_id: &str,
        project: &Project,
        name: &str,
        retention_seconds: u64,
    ) -> Result<Bucket> {
        let name = model::validate_bucket_name(name).map_err(bad_request)?;
        let namespace = model::namespace_for(&project.id, &name);
        StorageEngine::validate_name(&namespace).map_err(|e| bad_request(e.to_string()))?;
        let bucket = Bucket {
            id: format!("bkt_{}", auth::random_id(14)),
            project_id: project.id.clone(),
            name,
            namespace: namespace.clone(),
            retention_seconds,
            created_by: account_id.to_string(),
            created_at: now_ms(),
        };
        // The metadata record is written first: if database creation fails the
        // bucket is removed again, rather than leaving a database no tenant can
        // reach.
        self.store.create(&bucket).await?;
        let engine = self.engine.clone();
        let created =
            tokio::task::spawn_blocking(move || engine.get_or_create_database(&namespace))
                .await
                .map_err(internal)?;
        match created {
            Ok(db) => {
                if retention_seconds > 0 {
                    let _ = db.set_retention(retention_seconds);
                }
                Ok(bucket)
            }
            Err(error) => {
                let _ = self.store.delete::<Bucket>(&bucket.id).await;
                Err(internal(error))
            }
        }
    }

    /// Delete a bucket's metadata and its engine database.
    pub async fn delete_bucket(&self, bucket: &Bucket) -> Result<()> {
        self.store.delete::<Bucket>(&bucket.id).await?;
        let engine = self.engine.clone();
        let namespace = bucket.namespace.clone();
        let dropped = tokio::task::spawn_blocking(move || engine.drop_database(&namespace))
            .await
            .map_err(internal)?;
        if let Err(error) = dropped {
            // Metadata is already gone, so the bucket is unreachable either
            // way; a stranded database is logged for the operator.
            tracing::warn!(
                "bucket {} metadata deleted but database {} remains: {error}",
                bucket.id,
                bucket.namespace
            );
        }
        Ok(())
    }

    /// Delete a project, everything inside it, and all of its databases.
    pub async fn delete_project(&self, project: &Project) -> Result<()> {
        for bucket in self.store.list_by_parent::<Bucket>(&project.id).await? {
            self.delete_bucket(&bucket).await?;
        }
        for monitor in self.store.list_by_parent::<Monitor>(&project.id).await? {
            let _ = self
                .store
                .delete_by_owner::<model::AlertEvent>(&monitor.id)
                .await;
        }
        let _ = self.store.delete_by_parent::<Monitor>(&project.id).await;
        let _ = self
            .store
            .delete_by_parent::<model::AlertEvent>(&project.id)
            .await;
        let _ = self
            .store
            .delete_by_parent::<model::Dashboard>(&project.id)
            .await;
        let _ = self.store.delete_by_parent::<ApiKey>(&project.id).await;
        let _ = self
            .store
            .delete_by_parent::<model::Connection>(&project.id)
            .await;
        self.store.delete::<Project>(&project.id).await?;
        Ok(())
    }

    /// Delete an organization and everything below it.
    pub async fn delete_org(&self, org: &Org) -> Result<()> {
        for project in self.store.list_by_parent::<Project>(&org.id).await? {
            self.delete_project(&project).await?;
        }
        let _ = self.store.delete_by_parent::<Member>(&org.id).await;
        let _ = self.store.delete_by_parent::<model::Invite>(&org.id).await;
        let _ = self.store.delete_by_parent::<AuditEntry>(&org.id).await;
        self.store.delete::<Org>(&org.id).await?;
        Ok(())
    }

    // ---- shared demo workspace -------------------------------------------

    /// Create the read-only showcase workspace if it is missing, and seed it if
    /// its buckets are empty. Idempotent, so it can run on every start; on a
    /// host with an ephemeral filesystem this is what restores the demo after a
    /// redeploy.
    async fn bootstrap_demo(&self) -> anyhow::Result<()> {
        if self.store.get::<Org>(DEMO_ORG_ID).await?.is_none() {
            self.store
                .create(&Org {
                    id: DEMO_ORG_ID.to_string(),
                    name: "FluxDB Demo".into(),
                    slug: DEMO_SLUG.into(),
                    owner_id: SYSTEM_ACCOUNT.into(),
                    plan: "showcase".into(),
                    created_at: now_ms(),
                    expires_at: None,
                })
                .await?;
        }
        if self.store.get::<Project>(DEMO_PROJECT_ID).await?.is_none() {
            self.store
                .create(&Project {
                    id: DEMO_PROJECT_ID.to_string(),
                    org_id: DEMO_ORG_ID.to_string(),
                    name: "Production observability".into(),
                    slug: "production-observability".into(),
                    description:
                        "A live sample fleet: eight hosts, six services, an incident window, and the queries used to find it."
                            .into(),
                    created_by: SYSTEM_ACCOUNT.into(),
                    created_at: now_ms(),
                    demo: true,
                })
                .await?;
        }
        let project = self
            .store
            .get::<Project>(DEMO_PROJECT_ID)
            .await?
            .expect("demo project was just ensured");
        let buckets = self.store.list_by_parent::<Bucket>(&project.id).await?;
        let bucket = match buckets.into_iter().find(|b| b.name == seed::DEMO_BUCKET) {
            Some(bucket) => bucket,
            None => self
                .create_bucket(SYSTEM_ACCOUNT, &project, seed::DEMO_BUCKET, 0)
                .await
                .map_err(|e| anyhow::anyhow!("demo bucket: {e}"))?,
        };
        let db = self
            .open_bucket(&bucket)
            .map_err(|e| anyhow::anyhow!("demo database: {e}"))?;
        if db.stats().total_entries == 0 {
            let engine = self.engine.clone();
            let namespace = bucket.namespace.clone();
            tokio::task::spawn_blocking(move || {
                let db = engine
                    .get_or_create_database(&namespace)
                    .map_err(|e| anyhow::anyhow!("{e}"))?;
                seed::seed_fleet(&db)
            })
            .await??;
            tracing::info!("Seeded the shared demo workspace with sample fleet telemetry");
        }
        self.ensure_demo_dashboard(&project, &bucket).await?;
        self.ensure_demo_monitors(&project, &bucket).await?;
        Ok(())
    }

    async fn ensure_demo_dashboard(
        &self,
        project: &Project,
        bucket: &Bucket,
    ) -> anyhow::Result<()> {
        if !self
            .store
            .list_by_parent::<model::Dashboard>(&project.id)
            .await?
            .is_empty()
        {
            return Ok(());
        }
        self.store
            .create(&seed::demo_dashboard(&project.id, &bucket.id))
            .await?;
        Ok(())
    }

    async fn ensure_demo_monitors(&self, project: &Project, bucket: &Bucket) -> anyhow::Result<()> {
        if !self
            .store
            .list_by_parent::<Monitor>(&project.id)
            .await?
            .is_empty()
        {
            return Ok(());
        }
        for monitor in seed::demo_monitors(&project.id, &bucket.id) {
            self.store.create(&monitor).await?;
        }
        Ok(())
    }

    /// Give every account read access to the showcase workspace.
    pub async fn grant_demo_access(&self, account: &Account) -> Result<()> {
        let member = Member {
            org_id: DEMO_ORG_ID.to_string(),
            account_id: account.id.clone(),
            email: account.email.clone(),
            name: account.name.clone(),
            role: Role::Viewer,
            created_at: now_ms(),
        };
        if let Err(error) = self.store.create(&member).await {
            if !matches!(error, store::StoreError::Conflict(_)) {
                return Err(error.into());
            }
        }
        Ok(())
    }

    // ---- API keys ---------------------------------------------------------

    /// Resolve an `fdbk_…` token to its key and project. Records last use, so
    /// the console can show which keys are actually in service.
    pub async fn resolve_key(&self, token: &str) -> Result<(ApiKey, Project)> {
        let parsed = auth::parse_key_token(token).ok_or_else(|| {
            unauthorized("Supply a project API key as `Authorization: Bearer fdbk_…`")
        })?;
        let mut key = self
            .store
            .get::<ApiKey>(&parsed.id)
            .await?
            .ok_or_else(|| unauthorized("This API key is not valid"))?;
        if key.revoked {
            return Err(unauthorized("This API key has been revoked"));
        }
        if !auth::secrets_match(&auth::sha256_hex(&parsed.secret), &key.secret_hash) {
            return Err(unauthorized("This API key is not valid"));
        }
        let project = self
            .store
            .get::<Project>(&key.project_id)
            .await?
            .ok_or_else(|| unauthorized("This API key's project no longer exists"))?;
        // One write per minute at most, so ingestion is not slowed by metadata
        // updates on every request.
        if key.last_used_at.is_none_or(|at| now_ms() - at > 60_000) {
            key.last_used_at = Some(now_ms());
            let _ = self.store.save(&key).await;
        }
        Ok((key, project))
    }

    pub async fn key_bucket(&self, project: &Project, name: Option<&str>) -> Result<Bucket> {
        let buckets = self.store.list_by_parent::<Bucket>(&project.id).await?;
        match name {
            Some(name) => buckets
                .into_iter()
                .find(|bucket| bucket.name == name)
                .ok_or_else(|| not_found(format!("This project has no bucket named {name}"))),
            None if buckets.len() == 1 => Ok(buckets.into_iter().next().expect("length checked")),
            None => Err(bad_request(
                "Name the target bucket with the `bucket` query parameter",
            )),
        }
    }

    pub fn require_scope(&self, key: &ApiKey, scope: Scope) -> Result<()> {
        if key.allows(scope) {
            return Ok(());
        }
        Err(forbidden(match scope {
            Scope::Read => "This API key cannot read data",
            Scope::Write => "This API key cannot write data",
        }))
    }
}

// ============================================================================
// Actor extraction
// ============================================================================

/// The signed-in account behind a request, resolved from the session cookie.
#[derive(Clone, Debug)]
pub struct Actor {
    pub account: Account,
    /// SHA-256 of the presented cookie, so a handler can revoke exactly this
    /// session without ever holding the token.
    pub session_id: String,
}

impl Actor {
    pub fn requires_account(&self) -> Result<()> {
        if self.account.kind == AccountKind::Guest {
            return Err(forbidden(
                "Create a free account to use this. Guest workspaces are temporary.",
            ));
        }
        Ok(())
    }
}

#[axum::async_trait]
impl FromRequestParts<CloudState> for Actor {
    type Rejection = Fail;

    async fn from_request_parts(parts: &mut Parts, cloud: &CloudState) -> Result<Self> {
        let cookies = parts
            .headers
            .get(axum::http::header::COOKIE)
            .and_then(|value| value.to_str().ok());
        let token = auth::read_cookie(cookies, auth::SESSION_COOKIE)
            .ok_or_else(|| unauthorized("Sign in to continue"))?;
        let session_id = auth::sha256_hex(&token);
        let session = cloud
            .store
            .get::<Session>(&session_id)
            .await?
            .ok_or_else(|| unauthorized("Your session has expired. Sign in again."))?;
        if session.expires_at <= now_ms() {
            let _ = cloud.store.delete::<Session>(&session_id).await;
            return Err(unauthorized("Your session has expired. Sign in again."));
        }
        let mut account = cloud.account(&session.account_id).await?;
        if matches!(account.expires_at, Some(at) if at <= now_ms()) {
            return Err(unauthorized(
                "This guest workspace has expired. Start a new demo or create an account.",
            ));
        }
        // Coarse last-seen tracking, used to reap abandoned guest workspaces.
        if now_ms() - account.last_seen_at > 5 * 60_000 {
            account.last_seen_at = now_ms();
            let _ = cloud.store.save(&account).await;
        }
        Ok(Actor {
            account,
            session_id,
        })
    }
}

/// Client address as reported by the trusted reverse proxy, used only for rate
/// limiting. Falls back to a constant so a misconfigured proxy degrades to a
/// shared bucket rather than to no limit at all.
pub fn client_key(headers: &HeaderMap) -> String {
    headers
        .get("x-forwarded-for")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(',').next())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "local".to_string())
}

// ============================================================================
// Router
// ============================================================================

pub fn routes(cloud: CloudState) -> Router {
    Router::new()
        .merge(routes_auth::routes())
        .merge(routes_workspace::routes())
        .merge(routes_data::routes())
        .merge(agent::routes())
        .layer(axum::middleware::from_fn_with_state(
            cloud.clone(),
            routes_auth::guard_origin,
        ))
        .with_state(cloud)
}

/// Public description of the deployment, used by the marketing page and the
/// sign-in screen to show only the providers that are actually configured.
pub async fn public_config(cloud: &Cloud) -> Value {
    json!({
        "version": fluxdb_core::VERSION,
        "providers": {"github": cloud.secrets.github.is_some()},
        "guest_enabled": true,
        "demo_project_id": DEMO_PROJECT_ID,
        "control_plane": cloud.store.backend(),
        "limits": {
            "projects_per_org": limits::PROJECTS_PER_ORG,
            "buckets_per_project": limits::BUCKETS_PER_PROJECT,
            "points_per_project": limits::POINTS_PER_PROJECT,
            "guest_lifetime_hours": limits::GUEST_LIFETIME_SECONDS / 3600,
        },
    })
}

// ============================================================================
// Background maintenance
// ============================================================================

/// Expire sessions, reclaim abandoned guest workspaces and evaluate monitors.
/// Runs on one timer so the shared instance has exactly one periodic task.
pub fn spawn_maintenance(cloud: CloudState) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            if let Err(error) = sweep(&cloud).await {
                tracing::error!("control plane maintenance: {error}");
            }
            // Scheduled agents run outside `sweep` because an investigation
            // waits on a provider for tens of seconds; a failure there must not
            // stop session expiry or monitor evaluation from happening.
            agent::run_due(&cloud).await;
        }
    })
}

async fn sweep(cloud: &Cloud) -> anyhow::Result<()> {
    let now = now_ms();
    for session in cloud.store.expired::<Session>(now).await? {
        let _ = cloud.store.delete::<Session>(&session.id).await;
    }
    for account in cloud.store.expired::<Account>(now).await? {
        reclaim_guest(cloud, &account).await;
    }
    evaluate_monitors(cloud).await?;
    Ok(())
}

/// Remove a guest account and every resource it created.
async fn reclaim_guest(cloud: &Cloud, account: &Account) {
    if !account.is_guest() {
        return;
    }
    let orgs: Vec<Org> = cloud
        .store
        .list_by_owner::<Org>(&account.id)
        .await
        .unwrap_or_default();
    for org in orgs {
        if let Err(error) = cloud.delete_org(&org).await {
            tracing::warn!(
                "guest workspace {} was not fully reclaimed: {error}",
                org.id
            );
        }
    }
    let _ = cloud.store.delete_by_owner::<Session>(&account.id).await;
    let _ = cloud.store.delete_by_owner::<Member>(&account.id).await;
    let _ = cloud.store.delete::<Account>(&account.id).await;
    tracing::info!("Reclaimed expired guest workspace for {}", account.email);
}

/// Evaluate every enabled monitor and record state transitions. Only
/// transitions become alert events, so a monitor that stays in breach produces
/// one entry rather than one per sweep.
async fn evaluate_monitors(cloud: &Cloud) -> anyhow::Result<()> {
    for mut monitor in cloud.store.list_all::<Monitor>().await? {
        if !monitor.enabled {
            continue;
        }
        let Some(bucket) = cloud.store.get::<Bucket>(&monitor.bucket_id).await? else {
            continue;
        };
        let Some(db) = cloud.engine.get_database(&bucket.namespace) else {
            continue;
        };
        let query = monitor.query.clone();
        let outcome = tokio::task::spawn_blocking(move || data::scalar(&db, &query)).await?;
        let previous = monitor.state;
        monitor.last_checked_at = Some(now_ms());
        match outcome {
            Ok(Some(value)) => {
                monitor.last_value = Some(value);
                monitor.last_error = None;
                monitor.state = if monitor.breaches(value) {
                    MonitorState::Alerting
                } else {
                    MonitorState::Ok
                };
            }
            Ok(None) => {
                monitor.last_value = None;
                monitor.last_error = Some("The query returned no rows".into());
                monitor.state = MonitorState::Unknown;
            }
            Err(error) => {
                monitor.last_error = Some(error.message().to_string());
                monitor.state = MonitorState::Unknown;
            }
        }
        // Only transitions are recorded, and the first evaluation of a healthy
        // monitor is not announced as a recovery.
        let transitioned = monitor.state != previous
            && !(previous == MonitorState::Unknown && monitor.state == MonitorState::Ok);
        if transitioned {
            let message = match monitor.state {
                MonitorState::Alerting => format!(
                    "{} is {} the {} threshold ({:.2})",
                    monitor.name,
                    match monitor.comparison {
                        model::Comparison::Above => "above",
                        model::Comparison::Below => "below",
                    },
                    monitor.threshold,
                    monitor.last_value.unwrap_or_default()
                ),
                MonitorState::Ok => format!("{} recovered", monitor.name),
                MonitorState::Unknown => format!(
                    "{} could not be evaluated: {}",
                    monitor.name,
                    monitor.last_error.clone().unwrap_or_default()
                ),
            };
            let event = model::AlertEvent {
                id: format!("{}_{}", now_ms(), auth::random_hex(6)),
                monitor_id: monitor.id.clone(),
                project_id: monitor.project_id.clone(),
                monitor_name: monitor.name.clone(),
                state: monitor.state,
                severity: monitor.severity,
                value: monitor.last_value,
                message,
                at: now_ms(),
            };
            let _ = cloud.store.create(&event).await;
        }
        let _ = cloud.store.save(&monitor).await;
    }
    Ok(())
}

/// Number of guest workspaces currently alive, used to refuse new ones before
/// the shared instance runs out of room.
pub async fn live_guests(cloud: &Cloud) -> i64 {
    cloud
        .store
        .list_all::<Account>()
        .await
        .map(|accounts| {
            accounts
                .into_iter()
                .filter(|account| account.is_guest())
                .count() as i64
        })
        .unwrap_or(0)
}

pub fn guest_capacity_reached(live: i64) -> bool {
    live >= MAX_LIVE_GUESTS
}
