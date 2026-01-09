//! Storage engine - top-level coordinator

use super::{Database, StorageConfig};
use crate::{Point, Result, FluxError};
use crate::query::QueryResult;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

/// FluxDB storage engine
pub struct StorageEngine {
    config: StorageConfig,
    databases: RwLock<HashMap<String, Arc<Database>>>,
}

impl StorageEngine {
    /// Create a new storage engine
    pub fn new(config: StorageConfig) -> Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;
        
        let engine = Self {
            config,
            databases: RwLock::new(HashMap::new()),
        };
        
        // Load existing databases
        engine.load_databases()?;
        
        Ok(engine)
    }

    /// Create a new database
    pub fn create_database(&self, name: &str) -> Result<Arc<Database>> {
        let mut databases = self.databases.write();
        
        if databases.contains_key(name) {
            return Err(FluxError::Config(format!("Database {} already exists", name)));
        }
        
        let db = Database::open(
            name,
            self.config.data_dir.clone(),
            self.config.wal.clone(),
            self.config.sstable.clone(),
            self.config.memtable_size_limit,
        )?;
        
        let db = Arc::new(db);
        databases.insert(name.to_string(), db.clone());
        
        info!("Created database: {}", name);
        
        Ok(db)
    }

    /// Get or create a database
    pub fn get_or_create_database(&self, name: &str) -> Result<Arc<Database>> {
        // Check if exists
        {
            let databases = self.databases.read();
            if let Some(db) = databases.get(name) {
                return Ok(db.clone());
            }
        }
        
        // Create new
        self.create_database(name)
    }

    /// Get a database by name
    pub fn get_database(&self, name: &str) -> Option<Arc<Database>> {
        self.databases.read().get(name).cloned()
    }

    /// Drop a database
    pub fn drop_database(&self, name: &str) -> Result<()> {
        let mut databases = self.databases.write();
        
        if databases.remove(name).is_none() {
            return Err(FluxError::DatabaseNotFound(name.to_string()));
        }
        
        // Remove data directory
        let db_path = self.config.data_dir.join(name);
        if db_path.exists() {
            std::fs::remove_dir_all(&db_path)?;
        }
        
        info!("Dropped database: {}", name);
        
        Ok(())
    }

    /// List all databases
    pub fn list_databases(&self) -> Vec<String> {
        self.databases.read().keys().cloned().collect()
    }

    /// Write points to a database
    pub fn write(&self, database: &str, points: &[Point]) -> Result<()> {
        let db = self.get_or_create_database(database)?;
        db.write(points)
    }

    /// Execute a query
    pub fn query(&self, database: &str, sql: &str) -> Result<QueryResult> {
        let db = self.get_database(database)
            .ok_or_else(|| FluxError::DatabaseNotFound(database.to_string()))?;
        db.query(sql)
    }

    /// Flush all databases
    pub fn flush_all(&self) -> Result<()> {
        let databases = self.databases.read();
        for db in databases.values() {
            db.flush()?;
        }
        Ok(())
    }

    /// Get engine statistics
    pub fn stats(&self) -> EngineStats {
        let databases = self.databases.read();
        let db_stats: Vec<_> = databases.values().map(|db| db.stats()).collect();
        
        EngineStats {
            database_count: databases.len(),
            total_entries: db_stats.iter().map(|s| s.total_entries).sum(),
            total_size_bytes: db_stats.iter().map(|s| s.total_size_bytes).sum(),
            databases: db_stats,
        }
    }

    fn load_databases(&self) -> Result<()> {
        if !self.config.data_dir.exists() {
            return Ok(());
        }
        
        for entry in std::fs::read_dir(&self.config.data_dir)? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                let name = entry.file_name().to_string_lossy().to_string();
                
                // Skip hidden directories
                if name.starts_with('.') {
                    continue;
                }
                
                match Database::open(
                    &name,
                    self.config.data_dir.clone(),
                    self.config.wal.clone(),
                    self.config.sstable.clone(),
                    self.config.memtable_size_limit,
                ) {
                    Ok(db) => {
                        let mut databases = self.databases.write();
                        databases.insert(name.clone(), Arc::new(db));
                        info!("Loaded database: {}", name);
                    }
                    Err(e) => {
                        tracing::warn!("Failed to load database {}: {}", name, e);
                    }
                }
            }
        }
        
        Ok(())
    }
}

/// Storage engine statistics
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub database_count: usize,
    pub total_entries: usize,
    pub total_size_bytes: u64,
    pub databases: Vec<super::database::DatabaseStats>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{DataPoint, FieldValue, SeriesKey};
    use tempfile::TempDir;

    #[test]
    fn test_storage_engine() {
        let temp_dir = TempDir::new().unwrap();
        let config = StorageConfig {
            data_dir: temp_dir.path().to_path_buf(),
            ..Default::default()
        };
        
        let engine = StorageEngine::new(config).unwrap();
        
        // Create database
        let db = engine.create_database("testdb").unwrap();
        assert_eq!(db.name(), "testdb");
        
        // Write data
        let key = SeriesKey::new("temperature").with_tag("sensor", "s1");
        let points: Vec<Point> = (0..100)
            .map(|i| {
                let data = DataPoint::new(i * 1000, "value", FieldValue::Float(20.0 + i as f64));
                Point::new(key.clone(), data)
            })
            .collect();
        
        engine.write("testdb", &points).unwrap();
        
        // Query
        let result = engine.query("testdb", "SELECT * FROM temperature").unwrap();
        assert!(!result.rows.is_empty());
    }
}
