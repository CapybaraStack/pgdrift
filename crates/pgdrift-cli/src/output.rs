use clap::ValueEnum;
use colored::Colorize;
use pgdrift_core::drift::{DriftIssue, Severity};
use pgdrift_core::stats::FieldStats;
use pgdrift_db::discovery::JsonbColumn;
use serde_json::json;
use tabled::{
    Table, Tabled,
    settings::{
        Color, Modify, Style,
        object::{Columns, Object, Rows},
    },
};

#[derive(Debug, Clone, ValueEnum)]
pub enum OutputFormat {
    Table,
    Json,
    Markdown,
}

#[derive(Tabled)]
pub struct ColumnRow {
    #[tabled(rename = "Schema")]
    pub schema: String,
    #[tabled(rename = "Table")]
    pub table: String,
    #[tabled(rename = "Column")]
    pub column: String,
    #[tabled(rename = "Est. Rows")]
    pub row_count: String,
}

impl From<JsonbColumn> for ColumnRow {
    fn from(col: JsonbColumn) -> Self {
        Self {
            schema: col.schema,
            table: col.table,
            column: col.column,
            row_count: col
                .estimated_rows
                .map_or("N/A".to_string(), |c| c.to_string()),
        }
    }
}

pub fn print_columns(columns: &[JsonbColumn], format: &OutputFormat) {
    match format {
        OutputFormat::Table => {
            if columns.is_empty() {
                println!("{}", "No JSONB columns found.".yellow());
                return;
            }

            let rows: Vec<ColumnRow> = columns.iter().map(|c| c.clone().into()).collect();
            let mut table = Table::new(rows);
            table.with(Style::rounded());

            println!("\n{}", "JSONB Columns:".bold().green());
            println!("{}", table);
            println!("\nFound {} JSONB column(s)\n", columns.len());
        }
        OutputFormat::Json => {
            let output = json!({
                "columns": columns,
                "count": columns.len()
            });
            println!("{}", serde_json::to_string_pretty(&output).unwrap());
        }
        OutputFormat::Markdown => {
            println!("# JSONB Columns\n");
            println!("| Schema | Table | Column | Est. Rows |");
            println!("|--------|-------|--------|-----------|");
            for col in columns {
                println!(
                    "| {} | {} | {} | {} |",
                    col.schema,
                    col.table,
                    col.column,
                    col.estimated_rows
                        .map_or("N/A".to_string(), |c| c.to_string())
                );
            }
            println!("\nFound {} JSONB column(s)\n", columns.len());
        }
    }
}

#[derive(Tabled)]
pub struct DriftRow {
    #[tabled(rename = "Path")]
    pub path: String,
    #[tabled(rename = "Severity")]
    pub severity: String,
    #[tabled(rename = "Issue")]
    pub issue: String,
}

impl From<&DriftIssue> for DriftRow {
    fn from(issue: &DriftIssue) -> Self {
        let severity_str = match issue.severity() {
            Severity::Info => "Info",
            Severity::Warning => "Warning",
            Severity::Critical => "Critical",
        };

        Self {
            path: issue.path().to_string(),
            severity: severity_str.to_string(),
            issue: issue.description(),
        }
    }
}

pub struct AnalysisResult {
    pub table: String,
    pub column: String,
    pub samples_analyzed: u64,
    pub field_stats: Vec<FieldStats>,
    pub drift_issues: Vec<DriftIssue>,
}

pub fn print_analysis(result: &AnalysisResult, format: &OutputFormat) {
    match format {
        OutputFormat::Table => print_analysis_table(result),
        OutputFormat::Json => print_analysis_json(result),
        OutputFormat::Markdown => print_analysis_markdown(result),
    }
}

fn print_analysis_json(result: &AnalysisResult) {
    let output = json!({
        "table": result.table,
        "column": result.column,
        "samples_analyzed": result.samples_analyzed,
        "field_stats": result.field_stats,
        "drift_issues": result.drift_issues,
        "summary": {
            "total_paths": result.field_stats.len(),
            "max_depth": result.field_stats.iter().map(|fs| fs.depth).max().unwrap_or(0),
            "critical_issues": result.drift_issues.iter().filter(|di| di.severity() == Severity::Critical).count(),
            "warning_issues": result.drift_issues.iter().filter(|di| di.severity() == Severity::Warning).count(),
            "info_issues": result.drift_issues.iter().filter(|di| di.severity() == Severity::Info).count(),
        }
    });
    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn print_analysis_markdown(result: &AnalysisResult) {
    println!("# Schema Analysis: {}.{}\n", result.table, result.column);
    println!("**Samples analyzed:** {}\n", result.samples_analyzed);

    let max_depth = result
        .field_stats
        .iter()
        .map(|f| f.depth)
        .max()
        .unwrap_or(0);
    let critical_count = result
        .drift_issues
        .iter()
        .filter(|di| di.severity() == Severity::Critical)
        .count();
    let warning_count = result
        .drift_issues
        .iter()
        .filter(|di| di.severity() == Severity::Warning)
        .count();
    let info_count = result
        .drift_issues
        .iter()
        .filter(|di| di.severity() == Severity::Info)
        .count();

    println!("## Summary\n");
    println!("- Total unique paths: {}", result.field_stats.len());
    println!("- Max nesting depth: {}", max_depth);
    println!(
        "- Issues found: {} critical, {} warnings, {} info\n",
        critical_count, warning_count, info_count
    );

    if !result.drift_issues.is_empty() {
        println!("## Drift Issues\n");
        println!("| Path | Severity | Issue |");
        println!("|------|----------|-------|");
        for issue in &result.drift_issues {
            println!(
                "| {} | {:?} | {} |",
                issue.path(),
                issue.severity(),
                issue.description()
            );
        }
    } else {
        println!("**No drift issues found!**\n");
    }
}

fn print_analysis_table(result: &AnalysisResult) {
    println!(
        "\n{} {}.{} ({} samples)\n",
        "Analyzing".bold().green(),
        result.table,
        result.column,
        result.samples_analyzed
    );

    // Summary statistics
    let max_depth = result
        .field_stats
        .iter()
        .map(|f| f.depth)
        .max()
        .unwrap_or(0);
    let critical_count = result
        .drift_issues
        .iter()
        .filter(|i| i.severity() == Severity::Critical)
        .count();
    let warning_count = result
        .drift_issues
        .iter()
        .filter(|i| i.severity() == Severity::Warning)
        .count();
    let info_count = result
        .drift_issues
        .iter()
        .filter(|i| i.severity() == Severity::Info)
        .count();

    println!("{}", "Schema Summary:".bold());
    println!("  Total unique paths: {}", result.field_stats.len());
    println!("  Max nesting depth: {}", max_depth);

    if result.drift_issues.is_empty() {
        println!("  {}", "No drift issues found!".green().bold());
    } else {
        println!(
            "  Issues found: {} critical, {} warnings, {} info",
            critical_count.to_string().red(),
            warning_count.to_string().yellow(),
            info_count.to_string().cyan()
        );

        // Group by severity
        let critical_issues: Vec<&DriftIssue> = result
            .drift_issues
            .iter()
            .filter(|i| i.severity() == Severity::Critical)
            .collect();
        let warning_issues: Vec<&DriftIssue> = result
            .drift_issues
            .iter()
            .filter(|i| i.severity() == Severity::Warning)
            .collect();
        let info_issues: Vec<&DriftIssue> = result
            .drift_issues
            .iter()
            .filter(|i| i.severity() == Severity::Info)
            .collect();

        // Print critical issues first
        if !critical_issues.is_empty() {
            println!("\n{}", "Critical Issues:".red().bold());
            let rows: Vec<DriftRow> = critical_issues.iter().map(|i| (*i).into()).collect();
            let mut table = Table::new(rows);
            table.with(Style::rounded());
            table.with(
                Modify::new(Columns::new(1..=1).intersect(Rows::new(1..))).with(Color::FG_RED),
            );
            println!("{}", table);
        }

        // Then warnings
        if !warning_issues.is_empty() {
            println!("\n{}", "Warnings:".yellow().bold());
            let rows: Vec<DriftRow> = warning_issues.iter().map(|i| (*i).into()).collect();
            let mut table = Table::new(rows);
            table.with(Style::rounded());
            table.with(
                Modify::new(Columns::new(1..=1).intersect(Rows::new(1..))).with(Color::FG_YELLOW),
            );
            println!("{}", table);
        }

        // Then info
        if !info_issues.is_empty() {
            println!("\n{}", "Info:".cyan().bold());
            let rows: Vec<DriftRow> = info_issues.iter().map(|i| (*i).into()).collect();
            let mut table = Table::new(rows);
            table.with(Style::rounded());
            table.with(
                Modify::new(Columns::new(1..=1).intersect(Rows::new(1..))).with(Color::FG_CYAN),
            );
            println!("{}", table);
        }
    }

    println!();
}

#[derive(Tabled)]
pub struct IndexRow {
    #[tabled(rename = "Field Path")]
    pub field_path: String,
    #[tabled(rename = "Index Type")]
    pub index_type: String,
    #[tabled(rename = "Priority")]
    pub priority: String,
    #[tabled(rename = "Reason")]
    pub reason: String,
}

impl From<&pgdrift_core::index::IndexRecommendation> for IndexRow {
    fn from(rec: &pgdrift_core::index::IndexRecommendation) -> Self {
        Self {
            field_path: rec.field_path.clone(),
            index_type: rec.index_type.to_name().to_string(),
            priority: rec.priority.to_name().to_string(),
            reason: rec.reason.clone(),
        }
    }
}

pub struct IndexRecommendationResult {
    pub table: String,
    pub column: String,
    pub recommendations: Vec<pgdrift_core::index::IndexRecommendation>,
}

pub fn print_index_recommendations(result: &IndexRecommendationResult, format: &OutputFormat) {
    match format {
        OutputFormat::Table => print_index_recommendations_table(result),
        OutputFormat::Json => print_index_recommendations_json(result),
        OutputFormat::Markdown => print_index_recommendations_markdown(result),
    }
}

fn print_index_recommendations_json(result: &IndexRecommendationResult) {
    let output = json!({
        "table": result.table,
        "column": result.column,
        "recommendations": result.recommendations,
        "summary": {
            "total_recommendations": result.recommendations.len(),
            "high_priority": result.recommendations.iter().filter(|r| r.priority == pgdrift_core::index::IndexPriority::High).count(),
            "medium_priority": result.recommendations.iter().filter(|r| r.priority == pgdrift_core::index::IndexPriority::Medium).count(),
            "low_priority": result.recommendations.iter().filter(|r| r.priority == pgdrift_core::index::IndexPriority::Low).count(),
        }
    });
    println!("{}", serde_json::to_string_pretty(&output).unwrap());
}

fn print_index_recommendations_markdown(result: &IndexRecommendationResult) {
    println!(
        "# Index Recommendations: {}.{}\n",
        result.table, result.column
    );

    if result.recommendations.is_empty() {
        println!("**No index recommendations.**\n");
        println!("This could mean:\n");
        println!("- All fields have low occurrence counts (< 100 samples)");
        println!("- All fields are objects or arrays (not directly indexable)");
        println!("- Field densities are in the middle range without strong indexing needs\n");
        return;
    }

    println!("Found {} recommendation(s)\n", result.recommendations.len());

    println!("| Field Path | Index Type | Priority | Reason |");
    println!("|------------|------------|----------|--------|");
    for rec in &result.recommendations {
        println!(
            "| {} | {} | {} | {} |",
            rec.field_path,
            rec.index_type.to_name(),
            rec.priority.to_name(),
            rec.reason
        );
    }

    println!("\n## SQL Commands\n");
    for (i, rec) in result.recommendations.iter().enumerate() {
        println!("### {} - {}\n", i + 1, rec.field_path);
        println!("```sql\n{}\n```\n", rec.sql);
        println!("**Estimated Benefit:** {}\n", rec.estimated_benefit);
    }
}

fn print_index_recommendations_table(result: &IndexRecommendationResult) {
    println!(
        "\n{} {}.{}\n",
        "Index Recommendations for".bold().green(),
        result.table,
        result.column
    );

    if result.recommendations.is_empty() {
        println!("{}", "No index recommendations.".yellow());
        println!("\n{}", "This could mean:".bold());
        println!("  • All fields have low occurrence counts (< 100 samples)");
        println!("  • All fields are objects or arrays (not directly indexable)");
        println!("  • Field densities are in the middle range without strong indexing needs\n");
        return;
    }

    println!("{}", "Summary:".bold());
    println!("  Total recommendations: {}", result.recommendations.len());
    let high_count = result
        .recommendations
        .iter()
        .filter(|r| r.priority == pgdrift_core::index::IndexPriority::High)
        .count();
    let medium_count = result
        .recommendations
        .iter()
        .filter(|r| r.priority == pgdrift_core::index::IndexPriority::Medium)
        .count();
    let low_count = result
        .recommendations
        .iter()
        .filter(|r| r.priority == pgdrift_core::index::IndexPriority::Low)
        .count();

    if high_count > 0 {
        println!("  High priority: {}", high_count.to_string().red());
    }
    if medium_count > 0 {
        println!("  Medium priority: {}", medium_count.to_string().yellow());
    }
    if low_count > 0 {
        println!("  Low priority: {}", low_count.to_string().cyan());
    }

    // Print recommendations table
    println!("\n{}", "Recommendations:".bold());
    let rows: Vec<IndexRow> = result.recommendations.iter().map(|r| r.into()).collect();
    let mut table = Table::new(rows);
    table.with(Style::rounded());
    println!("{}", table);

    // Print SQL commands
    println!("\n{}", "SQL Commands:".bold().green());
    for (i, rec) in result.recommendations.iter().enumerate() {
        println!(
            "\n{} - {}",
            (i + 1).to_string().bold(),
            rec.field_path.bold()
        );
        println!("{}", rec.sql.dimmed());
        println!("{} {}", "Benefit:".bold(), rec.estimated_benefit);
    }

    println!();
}
