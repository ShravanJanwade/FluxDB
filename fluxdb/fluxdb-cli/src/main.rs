//! Offline administration CLI. The server must be stopped before opening its data directory.
use anyhow::{bail, Context, Result};
use fluxdb_core::storage::{StorageConfig, StorageEngine};
fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|s| s == "--help") {
        println!("FluxDB offline CLI\nUsage: fluxdb-cli --data-dir PATH list|create DB|query DB SQL|compact DB|flush DB\nStop the server before using this CLI against its data directory.");
        return Ok(());
    }
    if args.len() < 3 || args[0] != "--data-dir" {
        bail!("Expected --data-dir PATH COMMAND; see --help");
    }
    let engine = StorageEngine::new(StorageConfig {
        data_dir: args[1].clone().into(),
        ..Default::default()
    })?;
    match args[2].as_str() {
        "list" => println!(
            "{}",
            serde_json::to_string_pretty(&engine.list_databases())?
        ),
        "create" => {
            let name = args.get(3).context("Missing database name")?;
            engine.create_database(name)?;
            println!("Created {name}");
        }
        "query" => {
            let db = args.get(3).context("Missing database")?;
            let sql = args.get(4).context("Missing quoted SQL")?;
            println!("{}", serde_json::to_string_pretty(&engine.query(db, sql)?)?);
        }
        "compact" | "flush" => {
            let name = args.get(3).context("Missing database")?;
            let db = engine.get_database(name).context("Database not found")?;
            if args[2] == "compact" {
                db.compact()?;
            } else {
                db.flush()?;
            }
            println!("Completed {} for {name}", args[2]);
        }
        _ => bail!("Unknown command; see --help"),
    }
    Ok(())
}
