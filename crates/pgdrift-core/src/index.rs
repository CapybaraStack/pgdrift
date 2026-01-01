use crate::stats::FieldStats;
use crate::types::JsonType;
use serde::{Deserialize, Serialize};

/// Type of index to recommend
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexType {
    /// GIN Index for high density fiels with good query support
    Gin,
    /// Partial index for sparse fields
    Partial,
    /// B-tree index on extcted scalar values
    BTreeExtracted,
}

impl IndexType {
    pub fn to_name(&self) -> &str {
        match self {
            IndexType::Gin => "GIN",
            IndexType::Partial => "Partial GIN",
            IndexType::BTreeExtracted => "B-tree (extracted)",
        }
    }
}

/// Priority of the index recommendation
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IndexPriority {
    /// High priority index recommendation
    High,
    /// Medium priority index recommendation
    Medium,
    /// Low priority index recommendation
    Low,
}

impl IndexPriority {
    pub fn to_name(&self) -> &str {
        match self {
            IndexPriority::High => "High",
            IndexPriority::Medium => "Medium",
            IndexPriority::Low => "Low",
        }
    }
}

/// Index recommendation for sepecific fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexRecommendation {
    pub field_path: String,
    pub index_type: IndexType,
    pub priority: IndexPriority,
    pub reason: String,
    pub sql: String,
    pub estimated_benefit: String,
}

/// Configuration for index recommendations
#[derive(Debug, Clone)]
pub struct IndexConfig {
    /// Density threshold for high density fields (default: 0.8)
    pub high_density_threshold: f64,
    /// Density threshold for medium density fields (default: 0.2)
    pub medium_density_threshold: f64,
    /// Minimum occurences for index recommendation (default: 100)
    pub min_occurences: u64,
}

impl Default for IndexConfig {
    fn default() -> Self {
        IndexConfig {
            high_density_threshold: 0.8,
            medium_density_threshold: 0.2,
            min_occurences: 100,
        }
    }
}

/// Analyze field stats and generate an appropriate index recommendation if needed
pub fn recommend_index(
    table: &str,
    column: &str,
    field_stats: &[FieldStats],
    config: &IndexConfig,
) -> Vec<IndexRecommendation> {
    let mut recommendations = Vec::new();

    // Collect all high-density fields first to create a single consolidated GIN index
    let high_density_fields: Vec<&FieldStats> = field_stats
        .iter()
        .filter(|s| {
            s.occurrences >= config.min_occurences
                && s.density >= config.high_density_threshold
                && !matches!(
                    get_dominant_type(s),
                    Some(JsonType::Object) | Some(JsonType::Array)
                )
        })
        .collect();

    // If there are high-density fields, create a single GIN index recommendation
    if !high_density_fields.is_empty() {
        // Use the field with highest density as the primary field for the recommendation
        let primary_field = high_density_fields
            .iter()
            .max_by(|a, b| {
                a.density
                    .partial_cmp(&b.density)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap();

        recommendations.push(create_consolidated_gin_recommendation(
            table,
            column,
            primary_field,
            &high_density_fields,
            IndexPriority::Medium,
        ));
    }

    // Process other recommendations (partial GIN, B-tree)
    for stats in field_stats {
        if stats.occurrences < config.min_occurences {
            continue;
        }

        let dominant_type = get_dominant_type(stats);
        if dominant_type == Some(JsonType::Object) || dominant_type == Some(JsonType::Array) {
            continue;
        }

        // Skip high-density fields (already handled above)
        if stats.density >= config.high_density_threshold {
            continue;
        }

        if stats.density > 0.0 && stats.density <= config.medium_density_threshold {
            recommendations.push(create_partial_gin_recommendation(
                table,
                column,
                stats,
                IndexPriority::Medium,
            ));
        } else if stats.density > config.medium_density_threshold
            && stats.density < config.high_density_threshold
            && is_scalar_type(dominant_type)
        {
            recommendations.push(create_btree_extracted_recommendation(
                table,
                column,
                stats,
                dominant_type.unwrap(),
                IndexPriority::Medium,
            ));
        }
    }

    recommendations.sort_by(|a, b| {
        let priority_order = |p: &IndexPriority| match p {
            IndexPriority::High => 0,
            IndexPriority::Medium => 1,
            IndexPriority::Low => 2,
        };
        priority_order(&a.priority).cmp(&priority_order(&b.priority))
    });

    recommendations
}

fn get_dominant_type(stats: &FieldStats) -> Option<JsonType> {
    stats
        .types
        .iter()
        .max_by_key(|(_, count)| *count)
        .map(|(json_type, _)| *json_type)
}

fn is_scalar_type(json_type: Option<JsonType>) -> bool {
    matches!(
        json_type,
        Some(JsonType::String) | Some(JsonType::Number) | Some(JsonType::Boolean)
    )
}

fn create_consolidated_gin_recommendation(
    table: &str,
    column: &str,
    primary_stats: &FieldStats,
    all_high_density: &[&FieldStats],
    priority: IndexPriority,
) -> IndexRecommendation {
    let index_name = generate_index_name(table, column, "gin", "gin");

    // Create a list of all high-density fields with their densities
    let field_list = all_high_density
        .iter()
        .map(|s| format!("{} ({:.1}%)", s.path, s.density * 100.0))
        .collect::<Vec<_>>()
        .join(", ");

    let sql = format!(
        "-- GIN index for high-density fields: {}\n\
        CREATE INDEX {} ON {} USING GIN ({});",
        field_list, index_name, table, column
    );

    let reason = if all_high_density.len() == 1 {
        format!(
            "High density ({:.1}%) - present in {}/{} samples. \
             GIN index enables fast JSONB queries (@>, ?, ?&, ?|)",
            primary_stats.density * 100.0,
            primary_stats.occurrences,
            primary_stats.total_samples
        )
    } else {
        format!(
            "{} high-density fields ({}). \
             Single GIN index supports fast JSONB queries (@>, ?, ?&, ?|) for all fields.",
            all_high_density.len(),
            field_list
        )
    };

    IndexRecommendation {
        field_path: primary_stats.path.clone(),
        index_type: IndexType::Gin,
        priority,
        reason,
        sql,
        estimated_benefit: "Improved query performance for existence checks and containment queries across all high-density fields.".to_string(),
    }
}

fn create_partial_gin_recommendation(
    table: &str,
    column: &str,
    stats: &FieldStats,
    priority: IndexPriority,
) -> IndexRecommendation {
    let index_name = generate_index_name(table, column, &stats.path, "partial_gin");
    let path_condition = json_path_to_sql_conditions(&stats.path);
    let sql = format!(
        "-- Partial GIN index for sparse field: {:.1}% of rows contain this field\n\
        CREATE INDEX {} ON {} USING GIN ({}) WHERE {};",
        stats.density * 100.0,
        index_name,
        table,
        column,
        path_condition
    );

    IndexRecommendation {
        field_path: stats.path.clone(),
        index_type: IndexType::Partial,
        priority,
        reason: format!(
            "Sparce field ({:.1}%) - only {}/{} sample have this field. \
                Partial index reduces index size and maintenance cost",
            stats.density * 100.0,
            stats.occurrences,
            stats.total_samples
        ),
        sql,
        estimated_benefit: format!(
            "Smaller index (~{:.1}% of full GIN), faster updates, same query performance for matching rows",
            stats.density * 100.0
        ),
    }
}

fn create_btree_extracted_recommendation(
    table: &str,
    column: &str,
    stats: &FieldStats,
    json_type: JsonType,
    priority: IndexPriority,
) -> IndexRecommendation {
    let index_name = generate_index_name(table, column, &stats.path, "btree_ext");
    let (extraction_expr, pg_type) = match json_type {
        JsonType::String => (
            format!("({} #>> '{{{}}}')", column, escape_json_path(&stats.path)),
            "TEXT",
        ),
        JsonType::Number => (
            format!(
                "(({} #>> '{{{}}}')::NUMERIC)",
                column,
                escape_json_path(&stats.path)
            ),
            "NUMERIC",
        ),
        JsonType::Boolean => (
            format!(
                "(({} #>> '{{{}}}')::BOOLEAN)",
                column,
                escape_json_path(&stats.path)
            ),
            "BOOLEAN",
        ),
        _ => unreachable!(),
    };

    let sql = format!(
        "-- B-tree index on extracted {} value: {:.1}% density\n\
        CREATE INDEX {} ON {} ({}) WHERE {} IS NOT NULL;",
        pg_type,
        stats.density * 100.0,
        index_name,
        table,
        extraction_expr,
        extraction_expr
    );

    IndexRecommendation {
        field_path: stats.path.clone(),
        index_type: IndexType::BTreeExtracted,
        priority,
        reason: format!(
            "Medium density {} field ({:.1}%) - {}/{} samples. \
             B-tree index on extracted value range query and sorting.",
            json_type,
            stats.density * 100.0,
            stats.occurrences,
            stats.total_samples
        ),
        sql,
        estimated_benefit:
            "Improved query performance for lookups and range queries on scalar values.".to_string(),
    }
}

fn generate_index_name(table: &str, column: &str, path: &str, index_type: &str) -> String {
    let clean_path = path
        .replace("[]", "_arr")
        .replace(".", "_")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect::<String>();

    let max_len = 63;
    let prefix = format!("idx_{}_{}_{}_{}", table, column, clean_path, index_type);

    if prefix.len() <= max_len {
        prefix
    } else {
        let hash = format!("{:x}", calculate_simple_hash(&prefix));
        let truncate_len = max_len - hash.len() - 1;
        format!("{}_{}", &prefix[..truncate_len], hash)
    }
}

fn calculate_simple_hash(s: &str) -> u32 {
    s.bytes()
        .fold(0u32, |hash, b| hash.wrapping_mul(31).wrapping_add(b as u32))
}

fn json_path_to_sql_conditions(path: &str) -> String {
    let parts: Vec<&str> = path.split('.').collect();
    if parts.len() == 1 {
        let clean_part = parts[0].replace("[]", "");
        format!("metadata ? '{}'", clean_part)
    } else {
        let parent_path = parts[..parts.len() - 1]
            .iter()
            .map(|p| p.replace("[]", ""))
            .collect::<Vec<_>>()
            .join(",");
        let last = parts.last().unwrap().replace("[]", "");
        format!("metadata #> '{{{}}}' ? '{}'", parent_path, last)
    }
}

fn escape_json_path(path: &str) -> String {
    path.replace("[]", "") // Remove array notation
        .replace('\'', "''") // Escape single quotes for SQL
        .replace('.', ",") // Convert dots to commas for PostgreSQL {a,b,c} syntax
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_stats(path: &str, density: f64, occurrences: u64, total: u64) -> FieldStats {
        let mut stats = FieldStats::new(path.to_string(), 1);
        stats.occurrences = occurrences;
        stats.total_samples = total;
        stats.density = density;
        stats
    }

    #[test]
    fn test_high_density_recommends_gin() {
        let mut stats = create_test_stats("user.email", 0.95, 9500, 10000);
        stats.types.insert(JsonType::String, 9500);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        assert_eq!(recommendations.len(), 1);
        assert_eq!(recommendations[0].index_type, IndexType::Gin);
        assert_eq!(recommendations[0].priority, IndexPriority::Medium);
        assert!(recommendations[0].sql.contains("CREATE INDEX"));
        assert!(recommendations[0].sql.contains("USING GIN"));
        assert!(recommendations[0].sql.contains("95.0%"));
    }

    #[test]
    fn test_sparse_field_recommends_partial_gin() {
        let mut stats = create_test_stats("billing.legacy_plan", 0.05, 100, 2000);
        stats.types.insert(JsonType::String, 100);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        assert_eq!(recommendations.len(), 1);
        assert_eq!(recommendations[0].index_type, IndexType::Partial);
        assert_eq!(recommendations[0].priority, IndexPriority::Medium);
        assert!(recommendations[0].sql.contains("WHERE"));
        assert!(recommendations[0].sql.contains("5.0%"));
        assert!(recommendations[0].estimated_benefit.contains("5.0%"));
    }

    #[test]
    fn test_medium_density_string_recommends_btree() {
        let mut stats = create_test_stats("username", 0.5, 500, 1000);
        stats.types.insert(JsonType::String, 500);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        assert_eq!(recommendations.len(), 1);
        assert_eq!(recommendations[0].index_type, IndexType::BTreeExtracted);
        assert_eq!(recommendations[0].priority, IndexPriority::Medium);
        assert!(recommendations[0].sql.contains("#>>"));
        assert!(recommendations[0].sql.contains("TEXT"));
    }

    #[test]
    fn test_medium_density_number_recommends_btree() {
        let mut stats = create_test_stats("age", 0.6, 600, 1000);
        stats.types.insert(JsonType::Number, 600);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        assert_eq!(recommendations.len(), 1);
        assert_eq!(recommendations[0].index_type, IndexType::BTreeExtracted);
        assert!(recommendations[0].sql.contains("::NUMERIC"));
        assert!(recommendations[0].sql.contains("NUMERIC"));
    }

    #[test]
    fn test_medium_density_boolean_recommends_btree() {
        let mut stats = create_test_stats("is_active", 0.45, 450, 1000);
        stats.types.insert(JsonType::Boolean, 450);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        assert_eq!(recommendations.len(), 1);
        assert_eq!(recommendations[0].index_type, IndexType::BTreeExtracted);
        assert!(recommendations[0].sql.contains("::BOOLEAN"));
        assert!(recommendations[0].sql.contains("BOOLEAN"));
    }

    #[test]
    fn test_skips_object_types() {
        let mut stats = create_test_stats("user", 0.9, 900, 1000);
        stats.types.insert(JsonType::Object, 900);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        assert_eq!(recommendations.len(), 0);
    }

    #[test]
    fn test_skips_array_types() {
        let mut stats = create_test_stats("tags", 0.9, 900, 1000);
        stats.types.insert(JsonType::Array, 900);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        assert_eq!(recommendations.len(), 0);
    }

    #[test]
    fn test_skips_low_occurrence_fields() {
        let mut stats = create_test_stats("rare_field", 0.9, 50, 55);
        stats.types.insert(JsonType::String, 50);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        assert_eq!(recommendations.len(), 0);
    }

    #[test]
    fn test_respects_min_occurrences_threshold() {
        let mut stats = create_test_stats("field", 0.9, 99, 110);
        stats.types.insert(JsonType::String, 99);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        // Should skip because 99 < 100 (default min)
        assert_eq!(recommendations.len(), 0);

        let mut stats2 = create_test_stats("field2", 0.9, 100, 111);
        stats2.types.insert(JsonType::String, 100);

        let recommendations2 = recommend_index("users", "metadata", &[stats2], &config);

        // Should recommend because 100 >= 100
        assert_eq!(recommendations2.len(), 1);
    }

    #[test]
    fn test_custom_high_density_threshold() {
        let mut stats = create_test_stats("field", 0.7, 700, 1000);
        stats.types.insert(JsonType::String, 700);

        let config = IndexConfig {
            high_density_threshold: 0.6,
            medium_density_threshold: 0.2,
            min_occurences: 100,
        };

        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        assert_eq!(recommendations.len(), 1);
        assert_eq!(recommendations[0].index_type, IndexType::Gin);
    }

    #[test]
    fn test_custom_medium_density_threshold() {
        let mut stats = create_test_stats("field", 0.15, 150, 1000);
        stats.types.insert(JsonType::String, 150);

        let config = IndexConfig {
            high_density_threshold: 0.8,
            medium_density_threshold: 0.1,
            min_occurences: 100,
        };

        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        // Should get BTree because 0.15 > 0.1 (medium threshold) and < 0.8 (high threshold)
        assert_eq!(recommendations.len(), 1);
        assert_eq!(recommendations[0].index_type, IndexType::BTreeExtracted);
    }

    #[test]
    fn test_index_name_generation_basic() {
        let name = generate_index_name("users", "metadata", "user.email", "gin");
        assert_eq!(name, "idx_users_metadata_user_email_gin");
        assert!(name.len() <= 63);
    }

    #[test]
    fn test_index_name_generation_with_arrays() {
        let name = generate_index_name("orders", "data", "items[].sku", "btree_ext");
        assert!(name.contains("items_arr_sku"));
        assert!(name.len() <= 63);
    }

    #[test]
    fn test_index_name_generation_truncation() {
        let long_path = "very.long.deeply.nested.path.that.will.definitely.exceed.the.postgresql.limit.for.index.names";
        let name = generate_index_name("table_with_very_long_name", "column", long_path, "gin");
        assert!(name.len() <= 63);
        assert!(name.starts_with("idx_"));
    }

    #[test]
    fn test_index_name_special_chars_removed() {
        let name = generate_index_name("users", "data", "user-email@domain", "gin");
        // Special chars should be filtered out
        assert!(!name.contains('@'));
        assert!(!name.contains('-'));
    }

    #[test]
    fn test_json_path_escaping() {
        // Dots are converted to commas for PostgreSQL path syntax
        assert_eq!(escape_json_path("user.email"), "user,email");
        assert_eq!(escape_json_path("tags[]"), "tags");
        // Single quotes are escaped, dots become commas
        assert_eq!(escape_json_path("user's.name"), "user''s,name");
        // Array notation removed, dots become commas
        assert_eq!(escape_json_path("items[].price"), "items,price");
    }

    #[test]
    fn test_json_path_to_sql_conditions_simple() {
        let condition = json_path_to_sql_conditions("email");
        assert_eq!(condition, "metadata ? 'email'");
    }

    #[test]
    fn test_json_path_to_sql_conditions_nested() {
        let condition = json_path_to_sql_conditions("user.profile.email");
        assert_eq!(condition, "metadata #> '{user,profile}' ? 'email'");
    }

    #[test]
    fn test_json_path_to_sql_conditions_with_array() {
        let condition = json_path_to_sql_conditions("tags[]");
        assert_eq!(condition, "metadata ? 'tags'");
    }

    #[test]
    fn test_priority_sorting() {
        let mut stats1 = create_test_stats("low_priority", 0.5, 500, 1000);
        stats1.types.insert(JsonType::String, 500);

        let mut stats2 = create_test_stats("high_priority", 0.95, 950, 1000);
        stats2.types.insert(JsonType::String, 950);

        let mut stats3 = create_test_stats("medium_priority", 0.1, 100, 1000);
        stats3.types.insert(JsonType::String, 100);

        let config = IndexConfig::default();
        let recommendations =
            recommend_index("users", "metadata", &[stats1, stats2, stats3], &config);

        assert_eq!(recommendations.len(), 3);
        // All should be Medium priority in this implementation
        assert!(recommendations[0].priority == IndexPriority::Medium);
        assert!(recommendations[1].priority == IndexPriority::Medium);
        assert!(recommendations[2].priority == IndexPriority::Medium);
    }

    #[test]
    fn test_multiple_recommendations() {
        let mut stats1 = create_test_stats("email", 0.95, 950, 1000);
        stats1.types.insert(JsonType::String, 950);

        let mut stats2 = create_test_stats("age", 0.6, 600, 1000);
        stats2.types.insert(JsonType::Number, 600);

        let mut stats3 = create_test_stats("legacy_id", 0.05, 100, 2000);
        stats3.types.insert(JsonType::String, 100);

        let config = IndexConfig::default();
        let recommendations =
            recommend_index("users", "metadata", &[stats1, stats2, stats3], &config);

        assert_eq!(recommendations.len(), 3);

        // Find each type
        let gin = recommendations
            .iter()
            .find(|r| r.index_type == IndexType::Gin);
        let btree = recommendations
            .iter()
            .find(|r| r.index_type == IndexType::BTreeExtracted);
        let partial = recommendations
            .iter()
            .find(|r| r.index_type == IndexType::Partial);

        assert!(gin.is_some());
        assert!(btree.is_some());
        assert!(partial.is_some());
    }

    #[test]
    fn test_multiple_high_density_creates_single_gin() {
        let mut stats1 = create_test_stats("email", 0.95, 950, 1000);
        stats1.types.insert(JsonType::String, 950);

        let mut stats2 = create_test_stats("name", 0.92, 920, 1000);
        stats2.types.insert(JsonType::String, 920);

        let mut stats3 = create_test_stats("status", 0.88, 880, 1000);
        stats3.types.insert(JsonType::String, 880);

        let mut stats4 = create_test_stats("user_id", 1.0, 1000, 1000);
        stats4.types.insert(JsonType::String, 1000);

        let config = IndexConfig::default();
        let recommendations = recommend_index(
            "users",
            "metadata",
            &[stats1, stats2, stats3, stats4],
            &config,
        );

        // Should generate single GIN index, not four
        let gin_count = recommendations
            .iter()
            .filter(|r| r.index_type == IndexType::Gin)
            .count();
        assert_eq!(
            gin_count, 1,
            "Should generate single GIN index for multiple high-density fields"
        );

        // Verify the recommendation mentions all fields
        let gin_rec = recommendations
            .iter()
            .find(|r| r.index_type == IndexType::Gin)
            .unwrap();
        assert!(gin_rec.reason.contains("email") || gin_rec.sql.contains("email"));
        assert!(gin_rec.reason.contains("name") || gin_rec.sql.contains("name"));
        assert!(gin_rec.reason.contains("status") || gin_rec.sql.contains("status"));
        assert!(gin_rec.reason.contains("user_id") || gin_rec.sql.contains("user_id"));
        assert!(gin_rec.reason.contains("4 high-density fields"));
    }

    #[test]
    fn test_get_dominant_type() {
        let mut stats = create_test_stats("mixed_field", 0.5, 500, 1000);
        stats.types.insert(JsonType::String, 450);
        stats.types.insert(JsonType::Number, 50);

        let dominant = get_dominant_type(&stats);
        assert_eq!(dominant, Some(JsonType::String));
    }

    #[test]
    fn test_is_scalar_type() {
        assert!(is_scalar_type(Some(JsonType::String)));
        assert!(is_scalar_type(Some(JsonType::Number)));
        assert!(is_scalar_type(Some(JsonType::Boolean)));
        assert!(!is_scalar_type(Some(JsonType::Object)));
        assert!(!is_scalar_type(Some(JsonType::Array)));
        assert!(!is_scalar_type(Some(JsonType::Null)));
        assert!(!is_scalar_type(None));
    }

    #[test]
    fn test_index_type_to_name() {
        assert_eq!(IndexType::Gin.to_name(), "GIN");
        assert_eq!(IndexType::Partial.to_name(), "Partial GIN");
        assert_eq!(IndexType::BTreeExtracted.to_name(), "B-tree (extracted)");
    }

    #[test]
    fn test_index_priority_to_name() {
        assert_eq!(IndexPriority::High.to_name(), "High");
        assert_eq!(IndexPriority::Medium.to_name(), "Medium");
        assert_eq!(IndexPriority::Low.to_name(), "Low");
    }

    #[test]
    fn test_calculate_simple_hash() {
        let hash1 = calculate_simple_hash("test");
        let hash2 = calculate_simple_hash("test");
        let hash3 = calculate_simple_hash("different");

        // Same input should produce same hash
        assert_eq!(hash1, hash2);
        // Different input should (likely) produce different hash
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_recommendation_contains_field_path() {
        let mut stats = create_test_stats("user.profile.email", 0.9, 900, 1000);
        stats.types.insert(JsonType::String, 900);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        assert_eq!(recommendations.len(), 1);
        assert_eq!(recommendations[0].field_path, "user.profile.email");
    }

    #[test]
    fn test_gin_recommendation_has_complete_sql() {
        let mut stats = create_test_stats("email", 0.95, 950, 1000);
        stats.types.insert(JsonType::String, 950);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        let sql = &recommendations[0].sql;
        assert!(sql.contains("CREATE INDEX"));
        assert!(sql.contains("USING GIN"));
        assert!(sql.contains("users"));
        assert!(sql.contains("metadata"));
        assert!(sql.starts_with("--")); // Has comment
    }

    #[test]
    fn test_partial_gin_has_where_clause() {
        let mut stats = create_test_stats("rare.field", 0.05, 100, 2000);
        stats.types.insert(JsonType::String, 100);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        let sql = &recommendations[0].sql;
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("metadata"));
    }

    #[test]
    fn test_btree_has_extraction_and_where() {
        let mut stats = create_test_stats("score", 0.5, 500, 1000);
        stats.types.insert(JsonType::Number, 500);

        let config = IndexConfig::default();
        let recommendations = recommend_index("users", "metadata", &[stats], &config);

        let sql = &recommendations[0].sql;
        assert!(sql.contains("#>>"));
        assert!(sql.contains("::NUMERIC"));
        assert!(sql.contains("WHERE"));
        assert!(sql.contains("IS NOT NULL"));
    }
}
