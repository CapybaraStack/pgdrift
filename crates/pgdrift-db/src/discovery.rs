use serde::Serialize;
use sqlx::PgPool;

/// Represents a JSOBN column in discovered in the DB
#[derive(Debug, Clone, Serialize)]
pub struct JsonbColumn {
    pub schema: String,
    pub table: String,
    pub column: String,
    pub estimated_rows: Option<i64>,
}

impl JsonbColumn {
    /// Get the fully qualified column name
    pub fn full_name(&self) -> String {
        format!("{}.{}.{}", self.schema, self.table, self.column)
    }
}

/// Discover all JSONB columns in the DB
///
/// Queries information_schema to find all columns with the type 'Jsonb',
/// excluding system schemas (pg_catalog, information_schema).
/// Also, fetch estimated row counts from pg_stat_user_tables
pub async fn discover_jsonb_columns(pool: &PgPool) -> Result<Vec<JsonbColumn>, sqlx::Error> {
    let columns = sqlx::query_as::<_, (String, String, String, Option<i64>)>(
        r#"
          SELECT
              c.table_schema,
              c.table_name,
              c.column_name,
              s.n_live_tup as estimated_rows
          FROM information_schema.columns c
          LEFT JOIN pg_stat_user_tables s
              ON s.schemaname = c.table_schema
              AND s.relname = c.table_name
          WHERE c.data_type = 'jsonb'
              AND c.table_schema NOT IN ('pg_catalog', 'information_schema')
          ORDER BY c.table_schema, c.table_name, c.column_name
          "#,
    )
    .fetch_all(pool)
    .await?
    .into_iter()
    .map(|(schema, table, column, estimated_rows)| JsonbColumn {
        schema,
        table,
        column,
        estimated_rows,
    })
    .collect();

    Ok(columns)
}

/// Get exact row count for a specific table
///
/// Executes COUNT(*) query on the specified table.
/// Note: This can be slow on large tables - use estimated_rows from
/// discover_jsonb_columns for quick estimates.
pub async fn get_row_count(pool: &PgPool, schema: &str, table: &str) -> Result<i64, sqlx::Error> {
    let count: i64 = sqlx::query_scalar(&format!(
        "SELECT COUNT(*) FROM {}.{}",
        quote_identifier(schema),
        quote_identifier(table)
    ))
    .fetch_one(pool)
    .await?;

    Ok(count)
}

/// Quote a postgresql identifier (schema/table/column name) to prevent sql injection
fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace("\"", "\"\""))
}
