//! Compaction entry point. Database owns the write lock and durable manifest protocol.
//! Keeping file reclamation in Database prevents an independent scheduler from
//! deleting inputs before the replacement snapshot has been committed.
use crate::{storage::Database, Result};
pub fn compact_database(database: &Database) -> Result<()> {
    database.compact()
}
