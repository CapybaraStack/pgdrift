use crate::output::{OutputFormat, print_columns};
use anyhow::{Context, Result};
use pgdrift_db::{ConnectionPool, discover_jsonb_columns};

/// runs the discover command to find JSONB columns in the database
pub async fn run(database_url: &str, format: OutputFormat) -> Result<()> {
    let conn = ConnectionPool::new(database_url)
        .await
        .context("Failed to connect to database")?;

    conn.test_connection()
        .await
        .context("Failed to test database connection")?;

    let columns = discover_jsonb_columns(conn.pool())
        .await
        .context("Failed to discover JSONB columns")?;

    print_columns(&columns, &format);

    Ok(())
}
