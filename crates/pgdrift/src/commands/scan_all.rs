use crate::output::{ColumnScanResult, OutputFormat, ScanAllResult};
use anyhow::{Context, Result};
use pgdrift_core::analyzer::JsonAnalyzer;
use pgdrift_core::drift::{DriftConfig, DriftIssue, Severity, detect_drift};
use pgdrift_db::{ConnectionPool, Sampler, discover_jsonb_columns};

/// Run scan-all command to analyze all JSONB columns in the given DB
pub async fn run(database_url: &str, sample_size: usize, format: OutputFormat) -> Result<()> {
    let conn = ConnectionPool::new(database_url)
        .await
        .context("Failed to connect to the database")?;

    conn.test_connection()
        .await
        .context("Failed to test the database connection")?;

    let columns = discover_jsonb_columns(conn.pool())
        .await
        .context("Failed to discover JSONB columns")?;

    if columns.is_empty() {
        println!("No JSONB columns found in the database.");
        return Ok(());
    }

    println!(
        "Discovered {} JSONB columns. Starting analysis...\n",
        columns.len()
    );

    let mut column_results = Vec::new();
    let config = DriftConfig::default();

    for col in &columns {
        println!(
            "Analyzing column: {}.{} (table: {})",
            col.schema, col.column, col.table
        );

        match analyze_column(
            conn.pool(),
            &col.schema,
            &col.table,
            &col.column,
            sample_size,
            &config,
        )
        .await
        {
            Ok((samples_analyzed, drift_issues)) => {
                let critical = drift_issues
                    .iter()
                    .filter(|i| i.severity() == Severity::Critical)
                    .count();
                let warning = drift_issues
                    .iter()
                    .filter(|i| i.severity() == Severity::Warning)
                    .count();
                let info = drift_issues
                    .iter()
                    .filter(|i| i.severity() == Severity::Info)
                    .count();

                println!(
                    "Analysis complete for {}.{}.{} - Samples Analyzed: {}, Issues Found: {} (Critical: {}, Warning: {}, Info: {})\n",
                    col.schema,
                    col.table,
                    col.column,
                    samples_analyzed,
                    drift_issues.len(),
                    critical,
                    warning,
                    info
                );

                column_results.push(ColumnScanResult {
                    schema: col.schema.clone(),
                    table: col.table.clone(),
                    column: col.column.clone(),
                    samples_analyzed: samples_analyzed as u64,
                    drift_issues,
                });
            }
            Err(e) => {
                eprintln!(
                    "Error analyzing column {}.{}.{}: {}\n",
                    col.schema, col.table, col.column, e
                );
                // Continue with next column even if there's an error
                column_results.push(ColumnScanResult {
                    schema: col.schema.clone(),
                    table: col.table.clone(),
                    column: col.column.clone(),
                    samples_analyzed: 0,
                    drift_issues: vec![],
                });
            }
        }
    }

    let result = ScanAllResult {
        total_columns: columns.len(),
        column_results,
    };

    crate::output::print_scan_all_summary(&result, &format)?;

    Ok(())
}

async fn analyze_column(
    pool: &sqlx::PgPool,
    schema: &str,
    table: &str,
    column: &str,
    sample_size: usize,
    config: &DriftConfig,
) -> Result<(usize, Vec<DriftIssue>)> {
    let sampler = Sampler::new(pool, schema, table, None, sample_size)
        .await
        .context("Failed to create sampler")?
        .show_progress(false);

    let samples = sampler
        .sample(pool, schema, table, column)
        .await
        .context("Failed to sample data")?;

    if samples.is_empty() {
        anyhow::bail!("No samples found in the column");
    }

    let mut analyzer = JsonAnalyzer::new();
    for sample in &samples {
        analyzer.analyze(sample);
    }
    let stats = analyzer.finalize();
    let drift_issues = detect_drift(&stats, config);

    Ok((samples.len(), drift_issues))
}
