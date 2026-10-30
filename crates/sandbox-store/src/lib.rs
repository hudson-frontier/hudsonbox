//! PostgreSQL access and the object-storage interface.
//!
//! The schema this crate owns is specified in `docs/data-models.md`. Queries
//! live here rather than in the API or controller so that the constraints in
//! the migrations and the code that relies on them stay in one place.

pub mod admission;
pub mod allocation_permits;
pub mod allocation_retirement;
pub mod cancel;
pub mod claims;
pub mod compaction;
pub mod destroy;
pub mod dispatch;
pub mod execute;
pub mod files;
pub mod history;
mod host_capabilities;
pub mod host_maintenance;
pub mod host_registry;
pub mod leases;
pub mod lists;
pub mod output;
pub mod output_cleanup;
pub mod placement;
pub mod projects;
pub mod provision;
pub mod reads;
pub mod recovery;
pub mod retention;
pub mod storage_audit;
pub mod stream;
pub mod uploads;
pub mod work;

use std::time::Duration;

use sqlx::postgres::{PgPoolOptions, Postgres};
use sqlx::{Pool, migrate::Migrator};

/// Migrations, embedded at compile time from the workspace root.
pub static MIGRATOR: Migrator = sqlx::migrate!("../../migrations");

/// Why the store could not be reached or prepared.
#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    /// The connection pool could not be established.
    #[error("connecting to PostgreSQL: {0}")]
    Connect(#[source] sqlx::Error),
    /// Migrations could not be applied.
    #[error("applying migrations: {0}")]
    Migrate(#[source] sqlx::migrate::MigrateError),
    /// A query failed.
    #[error("querying PostgreSQL: {0}")]
    Query(#[source] sqlx::Error),
    /// A stored row could not be interpreted.
    ///
    /// Separate from a query failure on purpose: this means the data is wrong,
    /// not that the database is unreachable, and it must never be treated as
    /// an absent row.
    #[error("stored data is not usable: {0}")]
    Corrupt(String),
}

/// A connection pool to the durable store.
#[derive(Debug, Clone)]
pub struct Store {
    pool: Pool<Postgres>,
}

impl Store {
    /// Use an existing PostgreSQL pool, including an isolated integration-test database.
    #[must_use]
    pub fn from_pool(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    /// Connect, without touching the schema.
    ///
    /// Connections are established lazily, so this succeeding does not prove
    /// PostgreSQL is reachable. Call [`Store::migrate`] or issue a query to
    /// find that out.
    pub async fn connect(database_url: &str, max_connections: u32) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .acquire_timeout(Duration::from_secs(5))
            .connect_lazy(database_url)
            .map_err(StoreError::Connect)?;

        Ok(Self { pool })
    }

    /// Apply any migrations this binary carries that the database lacks.
    ///
    /// Idempotent: sqlx records what it has applied, so running it on every
    /// start is the intended use.
    pub async fn migrate(&self) -> Result<(), StoreError> {
        MIGRATOR.run(&self.pool).await.map_err(StoreError::Migrate)
    }

    /// Verify that a pooled connection can complete a query. The caller owns the deadline.
    /// This does not certify schema compatibility, write access, or runtime health.
    pub async fn check_connection(&self) -> Result<(), StoreError> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .map_err(StoreError::Query)?;
        Ok(())
    }

    /// The underlying pool, for queries elsewhere in this crate's callers.
    #[must_use]
    pub fn pool(&self) -> &Pool<Postgres> {
        &self.pool
    }
}
