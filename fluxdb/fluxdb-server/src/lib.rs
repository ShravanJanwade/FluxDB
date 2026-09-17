//! FluxDB server.
//!
//! Two layers share one process:
//!
//! * `api` — the token-authenticated data plane (`/api/v1`, line protocol,
//!   Prometheus metrics, OpenAPI). This is the whole product for a self-hosted
//!   single-tenant server.
//! * `cloud` — the multi-tenant control plane (`/api/cloud`, `/api/ingest`):
//!   accounts, organizations, projects, buckets, roles, API keys, dashboards,
//!   monitors and audit. Enabled unless `FLUXDB_CLOUD=off`.
//!
//! Exposed as a library so the HTTP surface can be driven end to end from
//! integration tests without binding a socket.

pub mod api;
pub mod cloud;

use fluxdb_core::storage::{StorageConfig, StorageEngine};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// HTTP listen address.
    pub http_addr: SocketAddr,
    /// Persistent storage directory for time-series data.
    pub data_dir: PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_addr: "127.0.0.1:8086".parse().expect("valid default address"),
            data_dir: PathBuf::from("data"),
        }
    }
}

impl ServerConfig {
    /// Read configuration from the environment.
    pub fn from_env() -> anyhow::Result<Self> {
        let mut config = Self::default();
        if let Ok(addr) = std::env::var("FLUXDB_ADDR") {
            config.http_addr = addr.parse()?;
        }
        if let Ok(dir) = std::env::var("FLUXDB_DATA_DIR") {
            config.data_dir = dir.into();
        }
        // An instance reachable from outside this machine must have a real
        // administration token; the alternative is publishing an open database.
        if !config.http_addr.ip().is_loopback()
            && std::env::var("FLUXDB_TOKEN").unwrap_or_default().len() < 32
        {
            anyhow::bail!("Non-loopback binding requires FLUXDB_TOKEN with at least 32 characters");
        }
        Ok(config)
    }
}

/// Where control-plane metadata lives. A managed Postgres instance is used when
/// `DATABASE_URL` is set, which is what a hosted deployment needs because its
/// container filesystem does not survive a redeploy. Otherwise a SQLite file
/// sits beside the time-series data, so a local run needs no setup at all.
/// Returns `None` when the control plane is switched off.
pub fn control_plane_url(data_dir: &Path) -> Option<String> {
    if std::env::var("FLUXDB_CLOUD")
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "off" | "0" | "false"))
        .unwrap_or(false)
    {
        return None;
    }
    for variable in ["FLUXDB_CONTROL_PLANE_URL", "DATABASE_URL"] {
        if let Ok(url) = std::env::var(variable) {
            let url = url.trim().to_string();
            if !url.is_empty() {
                return Some(url);
            }
        }
    }
    Some(format!(
        "sqlite://{}",
        data_dir.join("control.sqlite").display()
    ))
}

/// Enforce retention across every database, then reclaim the space. Retention
/// is wall-clock based and applied approximately, which is why it runs on a
/// timer rather than on the write path.
pub fn spawn_retention(engine: Arc<StorageEngine>) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            interval.tick().await;
            let engine = engine.clone();
            let _ = tokio::task::spawn_blocking(move || {
                for name in engine.list_databases() {
                    if let Some(db) = engine.get_database(&name) {
                        match db.enforce_retention(
                            chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                        ) {
                            Ok(count) if count > 0 => {
                                if let Err(e) = db.compact() {
                                    tracing::error!("Retention compaction: {e}");
                                }
                            }
                            Err(e) => tracing::error!("Retention: {e}"),
                            _ => {}
                        }
                    }
                }
            })
            .await;
        }
    })
}

/// Build the complete HTTP application: storage engine, optional control plane,
/// and the router that serves both. Used by the binary and by integration
/// tests.
pub async fn build(
    config: &ServerConfig,
) -> anyhow::Result<(Arc<StorageEngine>, Option<cloud::CloudState>, axum::Router)> {
    let engine = Arc::new(StorageEngine::new(StorageConfig {
        data_dir: config.data_dir.clone(),
        ..Default::default()
    })?);
    let cloud = match control_plane_url(&config.data_dir) {
        Some(url) => Some(
            cloud::Cloud::open(engine.clone(), &url, api::console::Monitor::new())
                .await
                // A control plane that cannot start is a configuration problem
                // worth failing on: silently serving a signed-out product would
                // look like data loss to every account holder.
                .map_err(|error| {
                    anyhow::anyhow!("Control plane could not start ({error}). Fix DATABASE_URL, or set FLUXDB_CLOUD=off to run a single-tenant server.")
                })?,
        ),
        None => {
            info!("Control plane disabled; serving the token-authenticated API only");
            None
        }
    };
    let router = api::create_router(engine.clone(), cloud.clone());
    Ok((engine, cloud, router))
}

/// Run the server until the process is asked to stop.
pub async fn run(config: ServerConfig) -> anyhow::Result<()> {
    info!("Starting FluxDB server...");
    info!("Data directory: {:?}", config.data_dir);
    info!("HTTP server: http://{}", config.http_addr);

    let (engine, cloud, app) = build(&config).await?;
    let retention = spawn_retention(engine.clone());
    let maintenance = cloud.map(cloud::spawn_maintenance);

    let listener = tokio::net::TcpListener::bind(&config.http_addr).await?;
    info!("FluxDB server listening on {}", config.http_addr);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;

    retention.abort();
    if let Some(handle) = maintenance {
        handle.abort();
    }
    engine.flush_all()?;
    Ok(())
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = term.recv() => {} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
