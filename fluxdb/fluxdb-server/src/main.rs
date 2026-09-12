//! FluxDB Server - HTTP API for the time-series database

mod api;
mod protocol;

use fluxdb_core::storage::{StorageConfig, StorageEngine};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

/// Server configuration
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// HTTP listen address
    pub http_addr: SocketAddr,
    /// Data directory
    pub data_dir: PathBuf,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            http_addr: "127.0.0.1:8086".parse().unwrap(),
            data_dir: PathBuf::from("data"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load a local .env (including the repository parent); real environment wins.
    dotenvy::dotenv().ok();
    // Initialize logging
    FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .pretty()
        .init();

    let mut config = ServerConfig::default();
    if let Ok(addr) = std::env::var("FLUXDB_ADDR") {
        config.http_addr = addr.parse()?;
    }
    if let Ok(dir) = std::env::var("FLUXDB_DATA_DIR") {
        config.data_dir = dir.into();
    }

    if !config.http_addr.ip().is_loopback()
        && std::env::var("FLUXDB_TOKEN").unwrap_or_default().len() < 32
    {
        anyhow::bail!("Non-loopback binding requires FLUXDB_TOKEN with at least 32 characters");
    }
    info!("Starting FluxDB server...");
    info!("Data directory: {:?}", config.data_dir);
    info!("HTTP server: http://{}", config.http_addr);

    // Initialize storage engine
    let storage_config = StorageConfig {
        data_dir: config.data_dir.clone(),
        ..Default::default()
    };

    let engine = StorageEngine::new(storage_config)?;
    let engine = Arc::new(engine);

    let maintenance_engine = engine.clone();
    let maintenance = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            interval.tick().await;
            let engine = maintenance_engine.clone();
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
    });
    // Create router
    let app = api::create_router(engine.clone());

    // Start server
    let listener = tokio::net::TcpListener::bind(&config.http_addr).await?;
    info!("FluxDB server listening on {}", config.http_addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await?;
    maintenance.abort();
    engine.flush_all()?;

    Ok(())
}

async fn shutdown() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler");
        tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=term.recv()=>{} }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}
