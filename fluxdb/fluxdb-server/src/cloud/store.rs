//! Portable control-plane metadata store.
//!
//! The control plane holds a small, low-volume set of records: accounts,
//! sessions, organizations, projects, buckets, API keys, monitors and audit
//! entries. It must run unchanged on a developer laptop (SQLite file) and on a
//! managed Postgres instance (hosted deployments, where the container
//! filesystem is ephemeral), so records are stored as typed JSON documents in
//! one table with explicit secondary index columns. All business rules and
//! validation live in typed Rust structs in `model.rs`; SQL is limited to the
//! subset both engines accept, which keeps exactly three functions
//! backend-specific instead of every query.

use serde::{de::DeserializeOwned, Serialize};
use sqlx::{
    postgres::{PgPool, PgPoolOptions},
    sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions},
    Row,
};
use std::str::FromStr;
use std::time::Duration;

/// A bind parameter. The control plane only needs text, integers and NULL.
#[derive(Debug, Clone)]
pub enum P {
    S(String),
    I(i64),
    Null,
}

impl From<&str> for P {
    fn from(v: &str) -> Self {
        P::S(v.to_string())
    }
}
impl From<String> for P {
    fn from(v: String) -> Self {
        P::S(v)
    }
}
impl From<i64> for P {
    fn from(v: i64) -> Self {
        P::I(v)
    }
}
impl From<Option<String>> for P {
    fn from(v: Option<String>) -> Self {
        v.map(P::S).unwrap_or(P::Null)
    }
}

/// One stored control-plane document.
#[derive(Debug, Clone)]
pub struct Record {
    pub kind: String,
    pub id: String,
    /// Owning container used for list queries (organization for a project,
    /// project for a bucket, and so on).
    pub parent: Option<String>,
    /// Acting account, used to list an account's memberships and audit trail.
    pub owner: Option<String>,
    /// Globally unique lookup value within a kind (email address, API key id,
    /// organization slug). `None` participates in no uniqueness constraint,
    /// because both SQLite and Postgres treat NULLs as distinct in a unique
    /// index.
    pub lookup: Option<String>,
    pub body: String,
    pub created_at: i64,
    pub updated_at: i64,
}

impl Record {
    pub fn decode<T: DeserializeOwned>(&self) -> std::result::Result<T, StoreError> {
        serde_json::from_str(&self.body).map_err(|e| {
            StoreError::Corrupt(format!(
                "stored {} record {} could not be decoded: {e}",
                self.kind, self.id
            ))
        })
    }
}

/// A typed control-plane entity that knows how to index itself.
pub trait Entity: Serialize + DeserializeOwned {
    const KIND: &'static str;
    fn id(&self) -> String;
    fn parent(&self) -> Option<String> {
        None
    }
    fn owner(&self) -> Option<String> {
        None
    }
    fn lookup(&self) -> Option<String> {
        None
    }
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("{0}")]
    Database(String),
    #[error("{0} already exists")]
    Conflict(String),
    #[error("{0}")]
    Corrupt(String),
    #[error("unsupported control-plane database URL: {0}")]
    UnsupportedUrl(String),
}

impl From<sqlx::Error> for StoreError {
    fn from(e: sqlx::Error) -> Self {
        StoreError::Database(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, StoreError>;

enum Pool {
    Sqlite(SqlitePool),
    Postgres(PgPool),
}

pub struct MetaStore {
    pool: Pool,
}

const SCHEMA: &[&str] = &[
    "CREATE TABLE IF NOT EXISTS records (
        kind TEXT NOT NULL,
        id TEXT NOT NULL,
        parent TEXT,
        owner TEXT,
        lookup TEXT,
        body TEXT NOT NULL,
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL,
        PRIMARY KEY (kind, id)
    )",
    "CREATE UNIQUE INDEX IF NOT EXISTS records_lookup ON records (kind, lookup)",
    "CREATE INDEX IF NOT EXISTS records_parent ON records (kind, parent)",
    "CREATE INDEX IF NOT EXISTS records_owner ON records (kind, owner)",
    "CREATE INDEX IF NOT EXISTS records_expiry ON records (kind, updated_at)",
];

const COLUMNS: &str = "kind, id, parent, owner, lookup, body, created_at, updated_at";

impl MetaStore {
    /// Open the control-plane store. A `postgres://` or `postgresql://` URL
    /// selects the managed backend; anything else is treated as a SQLite file
    /// path and created on demand.
    pub async fn connect(url: &str) -> Result<Self> {
        let pool = if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            Pool::Postgres(
                PgPoolOptions::new()
                    .max_connections(5)
                    .acquire_timeout(Duration::from_secs(20))
                    .connect(url)
                    .await?,
            )
        } else if url.starts_with("sqlite:") || url.ends_with(".sqlite") || url.ends_with(".db") {
            let path = url
                .trim_start_matches("sqlite://")
                .trim_start_matches("sqlite:");
            let options = SqliteConnectOptions::from_str(path)
                .map_err(|e| StoreError::UnsupportedUrl(e.to_string()))?
                .create_if_missing(true)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
                .busy_timeout(Duration::from_secs(10))
                .foreign_keys(true);
            Pool::Sqlite(
                SqlitePoolOptions::new()
                    .max_connections(4)
                    .connect_with(options)
                    .await?,
            )
        } else {
            return Err(StoreError::UnsupportedUrl(url.to_string()));
        };
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    pub fn backend(&self) -> &'static str {
        match self.pool {
            Pool::Sqlite(_) => "sqlite",
            Pool::Postgres(_) => "postgres",
        }
    }

    async fn migrate(&self) -> Result<()> {
        for statement in SCHEMA {
            self.execute(statement, vec![]).await?;
        }
        Ok(())
    }

    /// Rewrite `?` placeholders into the numbered form Postgres requires. No
    /// control-plane statement contains a literal question mark, so a plain
    /// sequential substitution is exact.
    fn dialect(&self, sql: &str) -> String {
        match self.pool {
            Pool::Sqlite(_) => sql.to_string(),
            Pool::Postgres(_) => {
                let mut out = String::with_capacity(sql.len() + 16);
                let mut n = 0;
                for c in sql.chars() {
                    if c == '?' {
                        n += 1;
                        out.push('$');
                        out.push_str(&n.to_string());
                    } else {
                        out.push(c);
                    }
                }
                out
            }
        }
    }

    async fn execute(&self, sql: &str, params: Vec<P>) -> Result<u64> {
        let sql = self.dialect(sql);
        match &self.pool {
            Pool::Sqlite(pool) => {
                let mut query = sqlx::query(&sql);
                for param in params {
                    query = match param {
                        P::S(v) => query.bind(v),
                        P::I(v) => query.bind(v),
                        P::Null => query.bind(Option::<String>::None),
                    };
                }
                Ok(query.execute(pool).await?.rows_affected())
            }
            Pool::Postgres(pool) => {
                let mut query = sqlx::query(&sql);
                for param in params {
                    query = match param {
                        P::S(v) => query.bind(v),
                        P::I(v) => query.bind(v),
                        P::Null => query.bind(Option::<String>::None),
                    };
                }
                Ok(query.execute(pool).await?.rows_affected())
            }
        }
    }

    async fn fetch(&self, sql: &str, params: Vec<P>) -> Result<Vec<Record>> {
        let sql = self.dialect(sql);
        let rows: Vec<Record> = match &self.pool {
            Pool::Sqlite(pool) => {
                let mut query = sqlx::query(&sql);
                for param in params {
                    query = match param {
                        P::S(v) => query.bind(v),
                        P::I(v) => query.bind(v),
                        P::Null => query.bind(Option::<String>::None),
                    };
                }
                query
                    .fetch_all(pool)
                    .await?
                    .into_iter()
                    .map(|row| Record {
                        kind: row.get("kind"),
                        id: row.get("id"),
                        parent: row.get("parent"),
                        owner: row.get("owner"),
                        lookup: row.get("lookup"),
                        body: row.get("body"),
                        created_at: row.get("created_at"),
                        updated_at: row.get("updated_at"),
                    })
                    .collect()
            }
            Pool::Postgres(pool) => {
                let mut query = sqlx::query(&sql);
                for param in params {
                    query = match param {
                        P::S(v) => query.bind(v),
                        P::I(v) => query.bind(v),
                        P::Null => query.bind(Option::<String>::None),
                    };
                }
                query
                    .fetch_all(pool)
                    .await?
                    .into_iter()
                    .map(|row| Record {
                        kind: row.get("kind"),
                        id: row.get("id"),
                        parent: row.get("parent"),
                        owner: row.get("owner"),
                        lookup: row.get("lookup"),
                        body: row.get("body"),
                        created_at: row.get("created_at"),
                        updated_at: row.get("updated_at"),
                    })
                    .collect()
            }
        };
        Ok(rows)
    }

    async fn insert_record(&self, record: &Record) -> Result<()> {
        let sql = format!("INSERT INTO records ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");
        let params = vec![
            P::S(record.kind.clone()),
            P::S(record.id.clone()),
            P::from(record.parent.clone()),
            P::from(record.owner.clone()),
            P::from(record.lookup.clone()),
            P::S(record.body.clone()),
            P::I(record.created_at),
            P::I(record.updated_at),
        ];
        let sql = self.dialect(&sql);
        let outcome = match &self.pool {
            Pool::Sqlite(pool) => {
                let mut query = sqlx::query(&sql);
                for param in params {
                    query = match param {
                        P::S(v) => query.bind(v),
                        P::I(v) => query.bind(v),
                        P::Null => query.bind(Option::<String>::None),
                    };
                }
                query.execute(pool).await.map(|_| ())
            }
            Pool::Postgres(pool) => {
                let mut query = sqlx::query(&sql);
                for param in params {
                    query = match param {
                        P::S(v) => query.bind(v),
                        P::I(v) => query.bind(v),
                        P::Null => query.bind(Option::<String>::None),
                    };
                }
                query.execute(pool).await.map(|_| ())
            }
        };
        match outcome {
            Ok(()) => Ok(()),
            Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                Err(StoreError::Conflict(record.kind.clone()))
            }
            Err(e) => Err(e.into()),
        }
    }

    fn record_of<T: Entity>(entity: &T, now: i64) -> Result<Record> {
        Ok(Record {
            kind: T::KIND.to_string(),
            id: entity.id(),
            parent: entity.parent(),
            owner: entity.owner(),
            lookup: entity.lookup(),
            body: serde_json::to_string(entity)
                .map_err(|e| StoreError::Corrupt(format!("could not encode {}: {e}", T::KIND)))?,
            created_at: now,
            updated_at: now,
        })
    }

    /// Insert a new entity. Returns [`StoreError::Conflict`] when the entity's
    /// id or lookup value is already taken, which is how uniqueness of email
    /// addresses, slugs and bucket names is enforced.
    pub async fn create<T: Entity>(&self, entity: &T) -> Result<()> {
        let now = super::now_ms();
        self.insert_record(&Self::record_of(entity, now)?).await
    }

    /// Insert or replace an entity, preserving the original creation time.
    pub async fn save<T: Entity>(&self, entity: &T) -> Result<()> {
        let record = Self::record_of(entity, super::now_ms())?;
        let sql = format!(
            "INSERT INTO records ({COLUMNS}) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (kind, id) DO UPDATE SET
                parent = excluded.parent,
                owner = excluded.owner,
                lookup = excluded.lookup,
                body = excluded.body,
                updated_at = excluded.updated_at"
        );
        let affected = self
            .execute(
                &sql,
                vec![
                    P::S(record.kind.clone()),
                    P::S(record.id.clone()),
                    P::from(record.parent.clone()),
                    P::from(record.owner.clone()),
                    P::from(record.lookup.clone()),
                    P::S(record.body.clone()),
                    P::I(record.created_at),
                    P::I(record.updated_at),
                ],
            )
            .await;
        match affected {
            Ok(_) => Ok(()),
            Err(StoreError::Database(message)) if message.contains("records_lookup") => {
                Err(StoreError::Conflict(T::KIND.to_string()))
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get<T: Entity>(&self, id: &str) -> Result<Option<T>> {
        let rows = self
            .fetch(
                &format!("SELECT {COLUMNS} FROM records WHERE kind = ? AND id = ?"),
                vec![P::from(T::KIND), P::from(id)],
            )
            .await?;
        rows.first().map(Record::decode).transpose()
    }

    pub async fn find<T: Entity>(&self, lookup: &str) -> Result<Option<T>> {
        let rows = self
            .fetch(
                &format!("SELECT {COLUMNS} FROM records WHERE kind = ? AND lookup = ?"),
                vec![P::from(T::KIND), P::from(lookup)],
            )
            .await?;
        rows.first().map(Record::decode).transpose()
    }

    pub async fn list_by_parent<T: Entity>(&self, parent: &str) -> Result<Vec<T>> {
        let rows = self
            .fetch(
                &format!(
                    "SELECT {COLUMNS} FROM records WHERE kind = ? AND parent = ? ORDER BY created_at ASC"
                ),
                vec![P::from(T::KIND), P::from(parent)],
            )
            .await?;
        rows.iter().map(Record::decode).collect()
    }

    pub async fn list_by_owner<T: Entity>(&self, owner: &str) -> Result<Vec<T>> {
        let rows = self
            .fetch(
                &format!(
                    "SELECT {COLUMNS} FROM records WHERE kind = ? AND owner = ? ORDER BY created_at ASC"
                ),
                vec![P::from(T::KIND), P::from(owner)],
            )
            .await?;
        rows.iter().map(Record::decode).collect()
    }

    /// Most recent records for a container, newest first.
    pub async fn recent_by_parent<T: Entity>(&self, parent: &str, limit: i64) -> Result<Vec<T>> {
        let rows = self
            .fetch(
                &format!(
                    "SELECT {COLUMNS} FROM records WHERE kind = ? AND parent = ? ORDER BY created_at DESC LIMIT ?"
                ),
                vec![P::from(T::KIND), P::from(parent), P::I(limit.clamp(1, 500))],
            )
            .await?;
        rows.iter().map(Record::decode).collect()
    }

    pub async fn list_all<T: Entity>(&self) -> Result<Vec<T>> {
        let rows = self
            .fetch(
                &format!("SELECT {COLUMNS} FROM records WHERE kind = ? ORDER BY created_at ASC"),
                vec![P::from(T::KIND)],
            )
            .await?;
        rows.iter().map(Record::decode).collect()
    }

    pub async fn delete<T: Entity>(&self, id: &str) -> Result<bool> {
        Ok(self
            .execute(
                "DELETE FROM records WHERE kind = ? AND id = ?",
                vec![P::from(T::KIND), P::from(id)],
            )
            .await?
            > 0)
    }

    pub async fn delete_by_parent<T: Entity>(&self, parent: &str) -> Result<u64> {
        self.execute(
            "DELETE FROM records WHERE kind = ? AND parent = ?",
            vec![P::from(T::KIND), P::from(parent)],
        )
        .await
    }

    pub async fn delete_by_owner<T: Entity>(&self, owner: &str) -> Result<u64> {
        self.execute(
            "DELETE FROM records WHERE kind = ? AND owner = ?",
            vec![P::from(T::KIND), P::from(owner)],
        )
        .await
    }

    pub async fn count<T: Entity>(&self, parent: Option<&str>) -> Result<i64> {
        let (sql, params) = match parent {
            Some(parent) => (
                "SELECT COUNT(*) AS total FROM records WHERE kind = ? AND parent = ?",
                vec![P::from(T::KIND), P::from(parent)],
            ),
            None => (
                "SELECT COUNT(*) AS total FROM records WHERE kind = ?",
                vec![P::from(T::KIND)],
            ),
        };
        let sql = self.dialect(sql);
        let total: i64 = match &self.pool {
            Pool::Sqlite(pool) => {
                let mut query = sqlx::query(&sql);
                for param in params {
                    query = match param {
                        P::S(v) => query.bind(v),
                        P::I(v) => query.bind(v),
                        P::Null => query.bind(Option::<String>::None),
                    };
                }
                query.fetch_one(pool).await?.get("total")
            }
            Pool::Postgres(pool) => {
                let mut query = sqlx::query(&sql);
                for param in params {
                    query = match param {
                        P::S(v) => query.bind(v),
                        P::I(v) => query.bind(v),
                        P::Null => query.bind(Option::<String>::None),
                    };
                }
                query.fetch_one(pool).await?.get("total")
            }
        };
        Ok(total)
    }

    /// Delete records of a kind whose JSON body carries an `expires_at` earlier
    /// than `before`. Used by the session and guest-workspace reapers.
    pub async fn expired<T: Entity>(&self, before: i64) -> Result<Vec<T>> {
        let rows = self
            .fetch(
                &format!("SELECT {COLUMNS} FROM records WHERE kind = ?"),
                vec![P::from(T::KIND)],
            )
            .await?;
        let mut expired = Vec::new();
        for record in &rows {
            let expiry = serde_json::from_str::<serde_json::Value>(&record.body)
                .ok()
                .and_then(|v| v.get("expires_at").and_then(|e| e.as_i64()));
            if matches!(expiry, Some(at) if at <= before) {
                expired.push(record.decode()?);
            }
        }
        Ok(expired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Serialize, Deserialize, Debug, PartialEq)]
    struct Widget {
        id: String,
        box_id: String,
        label: String,
        expires_at: Option<i64>,
    }

    impl Entity for Widget {
        const KIND: &'static str = "widget";
        fn id(&self) -> String {
            self.id.clone()
        }
        fn parent(&self) -> Option<String> {
            Some(self.box_id.clone())
        }
        fn lookup(&self) -> Option<String> {
            Some(self.label.clone())
        }
    }

    fn widget(id: &str, label: &str) -> Widget {
        Widget {
            id: id.into(),
            box_id: "box-1".into(),
            label: label.into(),
            expires_at: None,
        }
    }

    async fn store() -> MetaStore {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("control.sqlite");
        // The directory guard is intentionally leaked: the pool must outlive it
        // for the duration of the test.
        std::mem::forget(dir);
        MetaStore::connect(&format!("sqlite://{}", path.display()))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn enforces_lookup_uniqueness_and_round_trips_documents() {
        let store = store().await;
        store.create(&widget("w1", "alpha")).await.unwrap();
        // A second record claiming the same lookup value is a conflict, which is
        // how duplicate email addresses and bucket names are rejected.
        let clash = store.create(&widget("w2", "alpha")).await;
        assert!(matches!(clash, Err(StoreError::Conflict(_))), "{clash:?}");
        store.create(&widget("w2", "beta")).await.unwrap();

        let loaded: Widget = store.get("w1").await.unwrap().unwrap();
        assert_eq!(loaded, widget("w1", "alpha"));
        let by_lookup: Widget = store.find("beta").await.unwrap().unwrap();
        assert_eq!(by_lookup.id, "w2");
        assert_eq!(
            store.list_by_parent::<Widget>("box-1").await.unwrap().len(),
            2
        );
        assert_eq!(store.count::<Widget>(Some("box-1")).await.unwrap(), 2);
        assert!(store.get::<Widget>("missing").await.unwrap().is_none());
    }

    #[tokio::test]
    async fn saves_updates_and_reaps_expired_documents() {
        let store = store().await;
        store.create(&widget("w1", "alpha")).await.unwrap();
        let mut updated = widget("w1", "alpha");
        updated.expires_at = Some(500);
        store.save(&updated).await.unwrap();
        let loaded: Widget = store.get("w1").await.unwrap().unwrap();
        assert_eq!(loaded.expires_at, Some(500));

        assert!(store.expired::<Widget>(499).await.unwrap().is_empty());
        let due = store.expired::<Widget>(500).await.unwrap();
        assert_eq!(due.len(), 1);
        assert!(store.delete::<Widget>("w1").await.unwrap());
        assert!(!store.delete::<Widget>("w1").await.unwrap());
        assert_eq!(store.count::<Widget>(None).await.unwrap(), 0);
    }
}
