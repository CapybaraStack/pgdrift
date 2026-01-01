use crate::output::{IndexRecommendationResult, OutputFormat, print_index_recommendations};
use anyhow::{Context, Result};
use pgdrift_core::analyzer::JsonAnalyzer;
use pgdrift_core::index::{IndexConfig, recommend_index};
use pgdrift_db::{ConnectionPool, Sampler};

/// run performs index recommendation analysis on a JSONB column
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
        anyhow::bail!("No samples found. Column may be empty or NULL.");
    }

    println!(
        "Analyzing {} samples for index recommendations...",
        samples.len()
    );

    // Analyze the samples to get field statistics
    let mut analyzer = JsonAnalyzer::new();
    for sample in &samples {
        analyzer.analyze(sample);
    }
    let stats = analyzer.finalize();
    let mut field_stats: Vec<_> = stats.values().cloned().collect();
    field_stats.sort_by(|a, b| a.path.cmp(&b.path));

    // Generate index recommendations
    let config = IndexConfig::default();
    let recommendations = recommend_index(&table, column, &field_stats, &config);

    let result = IndexRecommendationResult {
        table: table.to_string(),
        column: column.to_string(),
        recommendations,
    };

    print_index_recommendations(&result, &format);
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
