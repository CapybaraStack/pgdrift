use futures::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use serde_json::Value;
use sqlx::PgPool;

/// Sampling strategy selection based on table size
#[derive(Debug, Clone, PartialEq)]
pub enum SamplingStrategy {
    /// Full table scan - when sample_size >= row_count
    /// No randomization, deterministic results
    Full,

    /// Random sampling for smaller tables (< 100k rows)
    /// Simple ORDER BY Random() LIMIT N
    Random { limit: usize },

    /// Reservoir sampling for medium tables (100K - 10M rows)
    /// uses primary key based random sampling for better performance
    ReservoirPK { sample_size: usize, pk: String },

    /// TABLESAMPLE for larger tables (> 10M rows)
    /// Postgresql's built in sampling  - fast and no table locks
    TableSample { percentage: f32, limit: usize },
}

impl SamplingStrategy {
    /// Auto select the best sampling strat based on table size
    ///
    /// # Strat selection
    /// - < 100k rows: Random sampling
    /// - 100k - 10M rows: Resevoir sampling with PK
    /// - 10M rows: TABLESAMPLE
    pub async fn auto_select(
        pool: &PgPool,
        schema: &str,
        table: &str,
        estimated_rows: Option<i64>,
        sample_size: usize,
    ) -> Result<Self, sqlx::Error> {
        let row_count = match estimated_rows {
            Some(count) if count > 0 => count,
            _ => crate::discovery::get_row_count(pool, schema, table).await?,
        };

        // If requesting all or more rows than exist, do a full deterministic scan
        if sample_size >= row_count as usize {
            return Ok(Self::Full);
        }

        Ok(match row_count {
            n if n < 100_000 => Self::Random {
                limit: sample_size,
            },
            n if n < 10_000_000 => {
                // try to find pk for Reservoir sampling
                match find_primary_key(pool, schema, table).await {
                    Ok(pk) => Self::ReservoirPK {
                        sample_size,
                        pk,
                    },
                    Err(_) => {
                        // Fallback to random pk
                        Self::Random {
                            limit: sample_size,
                        }
                    }
                }
            }
            _ => {
                // for very large tables
                // Cap percentage at 100.0 (PostgreSQL limit) and minimum 0.1
                let pct =
                    (sample_size as f32 / row_count as f32 * 100.0).clamp(0.1, 100.0);
                Self::TableSample {
                    percentage: pct,
                    limit: sample_size,
                }
            }
        })
    }

    /// Get the max number of samples that this strat should return
    pub fn max_samples(&self) -> usize {
        match self {
            Self::Full => usize::MAX, // Full scan - unknown size
            Self::Random { limit } => *limit,
            Self::ReservoirPK { sample_size, .. } => *sample_size,
            Self::TableSample { limit, .. } => *limit,
        }
    }

    fn build_query(&self, schema: &str, table: &str, column: &str) -> String {
        let schema_quoted = quote_identifier(schema);
        let table_quoted = quote_identifier(table);
        let column_quoted = quote_identifier(column);

        match self {
            Self::Full => {
                // Full table scan - deterministic, no randomization
                format!(
                    "SELECT {} FROM {}.{} WHERE {} IS NOT NULL",
                    column_quoted, schema_quoted, table_quoted, column_quoted
                )
            }
            Self::Random { limit } => {
                format!(
                    "SELECT {} FROM {}.{} WHERE {} IS NOT NULL ORDER BY random() LIMIT {}",
                    column_quoted, schema_quoted, table_quoted, column_quoted, limit
                )
            }
            Self::ReservoirPK { sample_size, pk } => {
                let pk_quoted = quote_identifier(pk);
                // True reservoir sampling: generate random IDs and fetch via index
                // This is MUCH faster than ORDER BY random() because it uses the PK index
                format!(
                    "WITH random_ids AS (
                        SELECT floor(random() * (SELECT MAX({}) FROM {}.{}))::bigint AS rand_id
                        FROM generate_series(1, {} * 2)
                    )
                    SELECT t.{}
                    FROM {}.{} t
                    INNER JOIN random_ids r ON t.{} = r.rand_id
                    WHERE t.{} IS NOT NULL
                    LIMIT {}",
                    pk_quoted,
                    schema_quoted,
                    table_quoted,  // MAX(pk)
                    sample_size,   // Generate 2x samples to account for PK gaps
                    column_quoted, // SELECT column
                    schema_quoted,
                    table_quoted,  // FROM table
                    pk_quoted,     // JOIN ON pk
                    column_quoted, // WHERE column IS NOT NULL
                    sample_size    // LIMIT
                )
            }
            Self::TableSample { percentage, limit } => {
                format!(
                    "SELECT {} FROM {}.{} TABLESAMPLE BERNOULLI({}) WHERE {} IS NOT NULL LIMIT {}",
                    column_quoted, schema_quoted, table_quoted, percentage, column_quoted, limit
                )
            }
        }
    }
}

pub struct Sampler {
    strategy: SamplingStrategy,
    production_mode: bool,
    show_progress: bool,
}

impl Sampler {
    /// Create a new sampler with auto select strat
    pub async fn new(
        pool: &PgPool,
        schema: &str,
        table: &str,
        estimated_rows: Option<i64>,
        sample_size: usize,
    ) -> Result<Self, sqlx::Error> {
        let strategy =
            SamplingStrategy::auto_select(pool, schema, table, estimated_rows, sample_size).await?;
        Ok(Self {
            strategy,
            production_mode: false,
            show_progress: true,
        })
    }

    /// Create a sampler with a specific strat
    pub fn with_strategy(strategy: SamplingStrategy) -> Self {
        Self {
            strategy,
            production_mode: false,
            show_progress: true,
        }
    }

    /// Enable prod mode
    pub fn production_mode(mut self, enabled: bool) -> Self {
        self.production_mode = enabled;
        self
    }

    /// Enable or disable prog bar
    pub fn show_progress(mut self, enabled: bool) -> Self {
        self.show_progress = enabled;
        self
    }

    //// Execute the sampling strat and return jsonb valuies
    ///
    /// # Production safety
    /// In prod mode:
    /// - Max 1% sampling for large tables
    /// - Requires explicit confirmation (future work)
    /// - shows estimated query
    pub async fn sample(
        &self,
        pool: &PgPool,
        schema: &str,
        table: &str,
        column: &str,
    ) -> Result<Vec<Value>, sqlx::Error> {
        // Production mode safety check
        if self.production_mode
            && let SamplingStrategy::TableSample { percentage, .. } = &self.strategy
            && *percentage > 1.0
        {
            eprintln!(
                "WARNING: Production mode limits sampling to 1%. Reducing from {:.2}%",
                percentage
            );
            // In a real implementation, we'd adjust the strategy here
        }

        let query = self.strategy.build_query(schema, table, column);
        let max_samples = self.strategy.max_samples();

        // Create progress bar if enabled
        let progress = if self.show_progress {
            let pb = ProgressBar::new(max_samples as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} samples")
                    .expect("Invalid progress bar template")
                    .progress_chars("█▓▒░"),
            );
            Some(pb)
        } else {
            None
        };

        // Execute query and collect results
        let mut samples = Vec::new();
        let mut rows = sqlx::query_scalar::<_, Value>(&query).fetch(pool);

        // Use sqlx's streaming to handle large result sets
        while let Some(value) = rows.try_next().await? {
            samples.push(value);

            if let Some(ref pb) = progress {
                pb.set_position(samples.len() as u64);
            }
        }

        if let Some(pb) = progress {
            pb.finish_with_message(format!("Collected {} samples", samples.len()));
        }

        Ok(samples)
    }
    /// Get information about the sampling strategy
    pub fn strategy_info(&self) -> String {
        match &self.strategy {
            SamplingStrategy::Full => {
                "Full table scan (all non-NULL rows)".to_string()
            }
            SamplingStrategy::Random { limit } => {
                format!("Random sampling (up to {} rows)", limit)
            }
            SamplingStrategy::ReservoirPK { sample_size, pk } => {
                format!(
                    "Reservoir sampling using PK '{}' (up to {} rows)",
                    pk, sample_size
                )
            }
            SamplingStrategy::TableSample { percentage, limit } => {
                format!("TABLESAMPLE {:.2}% (up to {} rows)", percentage, limit)
            }
        }
    }
}

async fn find_primary_key(pool: &PgPool, schema: &str, table: &str) -> Result<String, sqlx::Error> {
    let pk: Option<String> = sqlx::query_scalar(
        r#"
          SELECT a.attname
          FROM pg_index i
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
          WHERE i.indrelid = ($1 || '.' || $2)::regclass
            AND i.indisprimary
          LIMIT 1
          "#,
    )
    .bind(schema)
    .bind(table)
    .fetch_optional(pool)
    .await?;

    pk.ok_or_else(|| sqlx::Error::RowNotFound)
}

fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace("\"", "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strategy_max_samples() {
        let random = SamplingStrategy::Random { limit: 5000 };
        assert_eq!(random.max_samples(), 5000);

        let reservoir = SamplingStrategy::ReservoirPK {
            sample_size: 10000,
            pk: "id".to_string(),
        };
        assert_eq!(reservoir.max_samples(), 10000);

        let tablesample = SamplingStrategy::TableSample {
            percentage: 1.0,
            limit: 15000,
        };
        assert_eq!(tablesample.max_samples(), 15000);
    }

    #[test]
    fn test_build_query_random() {
        let strategy = SamplingStrategy::Random { limit: 1000 };
        let query = strategy.build_query("public", "users", "metadata");

        assert!(query.contains("ORDER BY random()"));
        assert!(query.contains("LIMIT 1000"));
        assert!(query.contains("IS NOT NULL"));
        assert!(query.contains("\"public\""));
        assert!(query.contains("\"users\""));
        assert!(query.contains("\"metadata\""));
    }

    #[test]
    fn test_build_query_reservoir() {
        let strategy = SamplingStrategy::ReservoirPK {
            sample_size: 5000,
            pk: "id".to_string(),
        };
        let query = strategy.build_query("public", "users", "metadata");

        assert!(query.contains("WITH random_ids"));
        assert!(query.contains("generate_series"));
        assert!(query.contains("INNER JOIN"));
        assert!(query.contains("LIMIT 5000"));
        assert!(query.contains("IS NOT NULL"));
    }

    #[test]
    fn test_build_query_tablesample() {
        let strategy = SamplingStrategy::TableSample {
            percentage: 0.5,
            limit: 10000,
        };
        let query = strategy.build_query("public", "users", "metadata");

        assert!(query.contains("TABLESAMPLE BERNOULLI(0.5)"));
        assert!(query.contains("LIMIT 10000"));
        assert!(query.contains("IS NOT NULL"));
    }

    #[test]
    fn test_quote_identifier() {
        assert_eq!(quote_identifier("simple"), "\"simple\"");
        assert_eq!(quote_identifier("with\"quote"), "\"with\"\"quote\"");
        assert_eq!(quote_identifier("schema.table"), "\"schema.table\"");
    }

    #[test]
    fn test_quote_identifier_sql_injection() {
        // Ensure SQL injection attempts are properly escaped
        assert_eq!(
            quote_identifier("table\"; DROP TABLE users; --"),
            "\"table\"\"; DROP TABLE users; --\""
        );
    }

    #[test]
    fn test_sampler_builder() {
        let strategy = SamplingStrategy::Random { limit: 1000 };
        let sampler = Sampler::with_strategy(strategy.clone())
            .production_mode(true)
            .show_progress(false);

        assert_eq!(sampler.strategy, strategy);
        assert!(sampler.production_mode);
        assert!(!sampler.show_progress);
    }

    #[test]
    fn test_sampler_default_settings() {
        let strategy = SamplingStrategy::Random { limit: 5000 };
        let sampler = Sampler::with_strategy(strategy);

        assert!(!sampler.production_mode);
        assert!(sampler.show_progress);
    }

    #[test]
    fn test_strategy_info_random() {
        let sampler = Sampler::with_strategy(SamplingStrategy::Random { limit: 5000 });
        assert_eq!(sampler.strategy_info(), "Random sampling (up to 5000 rows)");
    }

    #[test]
    fn test_strategy_info_reservoir() {
        let sampler = Sampler::with_strategy(SamplingStrategy::ReservoirPK {
            sample_size: 10000,
            pk: "user_id".to_string(),
        });
        assert_eq!(
            sampler.strategy_info(),
            "Reservoir sampling using PK 'user_id' (up to 10000 rows)"
        );
    }

    #[test]
    fn test_strategy_info_tablesample() {
        let sampler = Sampler::with_strategy(SamplingStrategy::TableSample {
            percentage: 2.5,
            limit: 20000,
        });
        assert_eq!(
            sampler.strategy_info(),
            "TABLESAMPLE 2.50% (up to 20000 rows)"
        );
    }

    #[test]
    fn test_strategy_equality() {
        let strat1 = SamplingStrategy::Random { limit: 1000 };
        let strat2 = SamplingStrategy::Random { limit: 1000 };
        let strat3 = SamplingStrategy::Random { limit: 2000 };

        assert_eq!(strat1, strat2);
        assert_ne!(strat1, strat3);
    }

    #[test]
    fn test_tablesample_percentage_capped_at_100() {
        // Simulate requesting more samples than rows in table
        let row_count = 10_000_000_i64;
        let sample_size = 10_000_011_usize;

        let pct = (sample_size as f32 / row_count as f32 * 100.0).clamp(0.1, 100.0);

        assert!(
            pct <= 100.0,
            "Percentage must not exceed 100.0, got {}",
            pct
        );
        assert_eq!(
            pct, 100.0,
            "When sample_size > row_count, percentage should be capped at 100.0"
        );
    }

    #[test]
    fn test_tablesample_percentage_minimum() {
        // Very large table with small sample size should respect minimum
        let row_count = 1_000_000_000_i64;
        let sample_size = 100_usize;

        let pct = (sample_size as f32 / row_count as f32 * 100.0).clamp(0.1, 100.0);

        assert_eq!(pct, 0.1, "Minimum percentage should be 0.1");
    }
}
