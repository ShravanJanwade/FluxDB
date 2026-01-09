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
            http_addr: "0.0.0.0:8086".parse().unwrap(),
            data_dir: PathBuf::from("data"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize logging
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .pretty()
        .init();

    let config = ServerConfig::default();
    
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

    // Create router
    let app = api::create_router(engine.clone());

    // Start server
    let listener = tokio::net::TcpListener::bind(&config.http_addr).await?;
    info!("FluxDB server listening on {}", config.http_addr);
    
    axum::serve(listener, app).await?;

    Ok(())
}
