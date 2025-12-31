use crate::output::{AnalysisResult, OutputFormat, print_analysis};
use anyhow::{Context, Result};
use pgdrift_core::analyzer::JsonAnalyzer;
use pgdrift_core::drift::{DriftConfig, detect_drift};
use pgdrift_db::{ConnectionPool, Sampler};

/// run performs analysis of a specified jsonb column in a PostgreSQL database
pub async fn run(
    database_url: &str,
    table: &str,
    column: &str,
    sample_size: usize,
    format: OutputFormat,
    production_mode: bool,
) -> Result<()> {
    let (schema, table) = parse_table_name(table);

    let conn = ConnectionPool::new(database_url)
        .await
        .context("Failed to create database connection pool")?;

    conn.test_connection()
        .await
        .context("Failed to connect to the database")?;

    let sampler = Sampler::new(conn.pool(), &schema, &table, None, sample_size)
        .await
        .context("Failed to create sampler")?
        .production_mode(production_mode)
        .show_progress(true);

    println!("\nSampling Strategy: {}", sampler.strategy_info());

    let samples = sampler
        .sample(conn.pool(), &schema, &table, column)
        .await
        .context("Failed to sample data")?;

    if samples.is_empty() {
        anyhow::bail!("No samples found. Column may be empty or NUILL.");
    }

    println!("Analyzing {} samples ...", samples.len());

    let mut analyzer = JsonAnalyzer::new();
    for sample in &samples {
        analyzer.analyze(sample)
    }
    let stats = analyzer.finalize();
    let mut field_stats: Vec<_> = stats.values().cloned().collect();
    field_stats.sort_by(|a, b| a.path.cmp(&b.path));

    let config = DriftConfig::default();
    let drift_issues = detect_drift(&stats, &config);

    let result = AnalysisResult {
        table: table.to_string(),
        column: column.to_string(),
        samples_analyzed: samples.len() as u64,
        field_stats,
        drift_issues,
    };

    print_analysis(&result, &format);
    Ok(())
}

/// Parse table name into schema and table components
fn parse_table_name(table: &str) -> (String, String) {
    match table.split_once('.') {
        Some((schema, table)) => (schema.to_string(), table.to_string()),
        None => ("public".to_string(), table.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_table_name() {
        let (schema, table) = parse_table_name("myschema.mytable");
        assert_eq!(schema, "myschema");
        assert_eq!(table, "mytable");

        let (schema, table) = parse_table_name("mytable");
        assert_eq!(schema, "public");
        assert_eq!(table, "mytable");

        assert_eq!(
            parse_table_name("users"),
            ("public".to_string(), "users".to_string())
        );

        assert_eq!(
            parse_table_name("myschema.users"),
            ("myschema".to_string(), "users".to_string())
        );

        assert_eq!(
            parse_table_name("public.orders"),
            ("public".to_string(), "orders".to_string())
        );
    }
}
