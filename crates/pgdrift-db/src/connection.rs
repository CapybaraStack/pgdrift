use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;

#[derive(Debug, Clone)]
pub struct ConnectionPool {
    pool: PgPool,
}

impl ConnectionPool {
    /// Create a new connection pool from a database URL
    pub async fn new(database_url: &str) -> Result<Self, sqlx::Error> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .acquire_timeout(Duration::from_secs(30))
            .connect(database_url)
            .await?;

        Ok(Self { pool })
    }

    /// Get a reference to the underlying PgPool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Test the database connection by executing a simple query
    pub async fn test_connection(&self) -> Result<(), sqlx::Error> {
        sqlx::query("SELECT 1").fetch_one(&self.pool).await?;

        Ok(())
    }
}
