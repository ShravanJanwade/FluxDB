//! FluxDB server entry point. Configuration and the HTTP application live in
//! the library crate so both can be exercised by tests.

use tracing::Level;
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Load a local .env (including the repository parent); real environment wins.
    dotenvy::dotenv().ok();
    FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .with_thread_ids(false)
        .pretty()
        .init();

    fluxdb_server::run(fluxdb_server::ServerConfig::from_env()?).await
}
