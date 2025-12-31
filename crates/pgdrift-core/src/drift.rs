use crate::stats::FieldStats;
use crate::types::JsonType;
use serde::Serialize;
use std::collections::HashMap;

/// Severity level for drift issues
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub enum Severity {
    Info,
    Warning,
    Critical, // Add more if we need to
}

impl std::fmt::Display for Severity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Severity::Info => write!(f, "Info"),
            Severity::Warning => write!(f, "Warning"),
            Severity::Critical => write!(f, "Critical"),
        }
    }
}

/// Types of drift issues that we can detect
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum DriftIssue {
    /// Field appears with multiple types, minority type excee=ds threshodl
    TypeInconsistency {
        path: String,
        types: HashMap<JsonType, TypeDistribution>,
        minority_percentage: f64,
    },
    /// Field appear in very few samples (< 10% threshold)
    GhostKey {
        path: String,
        density: f64,
        occurunces: u64,
        total_samples: u64,
    },
    /// Optional field with moderate presence (10-80%)
    SparseField {
        path: String,
        density: f64,
        occurrences: u64,
        total_samples: u64,
    },
    /// Expected high density key with unexpected gaps (80-95%)
    MissingKey {
        path: String,
        density: f64,
        expected_occurrences: u64,
        actual_occurrences: u64,
    },
    /// Schema changes detected - versions or naming inconsistency
    SchemaEvolution {
        path: String,
        pattern: EvolutionPattern,
    },
}

/// Type distribution
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TypeDistribution {
    pub json_type: JsonType,
    pub count: u64,
    pub percentage: f64,
}

/// Evolution pattern for existing schema
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum EvolutionPattern {
    /// Version markers
    VersionMarker { marker_path: String },
    /// Deprecated or legacy naming patters
    DeprecatedNaming { old_path: String, new_path: String },
    /// Mutually exclusive field
    MutuallyExclusive { paths: Vec<String> },
}

impl DriftIssue {
    /// Get the severity of the issues
    pub fn severity(&self) -> Severity {
        match self {
            DriftIssue::TypeInconsistency {
                minority_percentage,
                ..
            } => {
                if *minority_percentage >= 10.0 {
                    Severity::Critical
                } else if *minority_percentage >= 5.0 {
                    Severity::Warning
                } else {
                    Severity::Info
                }
            }
            DriftIssue::MissingKey { density, .. } => {
                if *density < 0.90 {
                    Severity::Critical
                } else if *density < 0.95 {
                    Severity::Warning
                } else {
                    Severity::Info
                }
            }
            DriftIssue::GhostKey { .. } => Severity::Info,
            DriftIssue::SparseField { .. } => Severity::Info,
            DriftIssue::SchemaEvolution { .. } => Severity::Warning,
        }
    }

    /// Get the path of the issue
    pub fn path(&self) -> &str {
        match self {
            DriftIssue::TypeInconsistency { path, .. } => path,
            DriftIssue::GhostKey { path, .. } => path,
            DriftIssue::SparseField { path, .. } => path,
            DriftIssue::MissingKey { path, .. } => path,
            DriftIssue::SchemaEvolution { path, .. } => path,
        }
    }

    /// Get description of the issue
    pub fn description(&self) -> String {
        match self {
            DriftIssue::TypeInconsistency {
                types,
                minority_percentage,
                ..
            } => {
                let mut type_list: Vec<_> = types.values().collect();
                type_list.sort_by(|a, b| b.percentage.partial_cmp(&a.percentage).unwrap());
                let type_sts: Vec<String> = type_list
                    .iter()
                    .map(|td| format!("{}:{:.1}", td.json_type, td.percentage))
                    .collect();
                format!(
                    "Type inconsistency (minority: {:.1}%: {}",
                    minority_percentage,
                    type_sts.join(", ")
                )
            }
            DriftIssue::GhostKey {
                density,
                occurunces,
                total_samples,
                ..
            } => {
                format!(
                    "Ghost key: {:.2}% present ({}/{} samples)",
                    density * 100.0,
                    occurunces,
                    total_samples
                )
            }
            DriftIssue::SparseField {
                density,
                occurrences,
                total_samples,
                ..
            } => {
                format!(
                    "Sparse field: {:.2}% present ({}/{} samples)",
                    density * 100.0,
                    occurrences,
                    total_samples
                )
            }
            DriftIssue::MissingKey {
                density,
                actual_occurrences,
                expected_occurrences,
                ..
            } => {
                let missing_count = expected_occurrences - actual_occurrences;
                let missing_percentage = (1.0 - density) * 100.0;
                format!(
                    "Missing key: {:.2}% missing ({}/{} samples missing field)",
                    missing_percentage, missing_count, expected_occurrences
                )
            }

            DriftIssue::SchemaEvolution { pattern, .. } => match pattern {
                EvolutionPattern::VersionMarker { marker_path } => {
                    format!("Schema evolution: version marker '{}'", marker_path)
                }
                EvolutionPattern::DeprecatedNaming { old_path, new_path } => {
                    format!(
                        "Schema evolution: deprecated field '{}' → '{}'",
                        old_path, new_path
                    )
                }
                EvolutionPattern::MutuallyExclusive { paths } => {
                    format!(
                        "Schema evolution: mutually exclusive fields: {}",
                        paths.join(", ")
                    )
                }
            },
        }
    }
}

/// Configuration for drift detection thresholds
///
/// TODO: Make these thresholds configurable via:
/// 1. Config file (~/.config/pgdrift/config.toml or .pgdrift.toml in project root)
/// 2. Command-line arguments (--ghost-key-threshold, --sparse-field-threshold, etc.)
///
/// Current hardcoded thresholds can cause boundary issues when field density
/// is exactly at a threshold (e.g., 80%). User-configurable thresholds would allow
/// tuning for specific use cases and avoid false positives/negatives.
#[derive(Debug, Clone)]
pub struct DriftConfig {
    /// Minimum percentage for minority type to trigger type inconsistency (default: 5.0%)
    pub type_inconsistency_threshold: f64,
    /// Maximum density for ghost key detection (default: 0.10 = 10%)
    pub ghost_key_threshold: f64,
    /// Maximum density for sparse field detection (default: 0.80 = 80%)
    pub sparse_field_threshold: f64,
    /// Minimum density for missing key detection (default: 0.95 = 95%)
    pub missing_key_threshold: f64,
    /// Whether to detect schema evolution patterns
    pub detect_schema_evolution: bool,
}

impl Default for DriftConfig {
    fn default() -> Self {
        Self {
            type_inconsistency_threshold: 5.0,
            ghost_key_threshold: 0.10,
            sparse_field_threshold: 0.80,
            missing_key_threshold: 0.95,
            detect_schema_evolution: true,
        }
    }
}

/// Analyze field statistics and detect drift
pub fn detect_drift(stats: &HashMap<String, FieldStats>, config: &DriftConfig) -> Vec<DriftIssue> {
    let mut issues = Vec::new();
    for field_stats in stats.values() {
        if let Some(issue) = detect_type_inconsistency(field_stats, config) {
            issues.push(issue);
        }
        if let Some(issue) = detect_ghost_key(field_stats, config) {
            issues.push(issue);
        }
        if let Some(issue) = detect_sparse_field(field_stats, config) {
            issues.push(issue);
        }
        if let Some(issue) = detect_missing_key(field_stats, config) {
            issues.push(issue);
        }
    }

    if config.detect_schema_evolution {
        issues.extend(detect_schema_evolution(stats));
    }

    issues.sort_by(|a, b| {
        b.severity()
            .cmp(&a.severity())
            .then_with(|| a.path().cmp(b.path()))
    });

    issues
}

/// Detect type inconsistency: field appears as multiple types
fn detect_type_inconsistency(stats: &FieldStats, config: &DriftConfig) -> Option<DriftIssue> {
    // Need at least 2 different types
    if stats.types.len() < 2 {
        return None;
    }

    let total_typed: u64 = stats.types.values().sum();
    if total_typed == 0 {
        return None;
    }

    // Calculate type distributions
    let mut type_distributions: HashMap<JsonType, TypeDistribution> = HashMap::new();
    for (json_type, count) in &stats.types {
        let percentage = (*count as f64 / total_typed as f64) * 100.0;
        type_distributions.insert(
            *json_type,
            TypeDistribution {
                json_type: *json_type,
                count: *count,
                percentage,
            },
        );
    }

    // Find minority types (not the most common)
    let max_count = stats.types.values().max().copied().unwrap_or(0);
    let minority_count: u64 = stats.types.values().filter(|&&c| c != max_count).sum();

    let minority_percentage = (minority_count as f64 / total_typed as f64) * 100.0;

    // Only report if minority exceeds threshold
    if minority_percentage >= config.type_inconsistency_threshold {
        Some(DriftIssue::TypeInconsistency {
            path: stats.path.clone(),
            types: type_distributions,
            minority_percentage,
        })
    } else {
        None
    }
}

/// Detect ghost keys: fields with very low density
fn detect_ghost_key(stats: &FieldStats, config: &DriftConfig) -> Option<DriftIssue> {
    if stats.density <= config.ghost_key_threshold && stats.density > 0.0 {
        Some(DriftIssue::GhostKey {
            path: stats.path.clone(),
            density: stats.density,
            occurunces: stats.occurrences,
            total_samples: stats.total_samples,
        })
    } else {
        None
    }
}

/// Detect sparse fields: optional fields with moderate presence (10-80%)
fn detect_sparse_field(stats: &FieldStats, config: &DriftConfig) -> Option<DriftIssue> {
    // Fields between ghost threshold and sparse threshold
    if stats.density > config.ghost_key_threshold && stats.density <= config.sparse_field_threshold
    {
        Some(DriftIssue::SparseField {
            path: stats.path.clone(),
            density: stats.density,
            occurrences: stats.occurrences,
            total_samples: stats.total_samples,
        })
    } else {
        None
    }
}

/// Detect missing keys: expected fields (high density) with gaps (80-95%)
fn detect_missing_key(stats: &FieldStats, config: &DriftConfig) -> Option<DriftIssue> {
    // Only check fields that should be present (density between sparse and missing thresholds)
    if stats.density > config.sparse_field_threshold && stats.density < config.missing_key_threshold
    {
        let expected_occurrences = stats.total_samples;
        Some(DriftIssue::MissingKey {
            path: stats.path.clone(),
            density: stats.density,
            expected_occurrences,
            actual_occurrences: stats.occurrences,
        })
    } else {
        None
    }
}

/// Detect schema evolution patterns
fn detect_schema_evolution(stats: &HashMap<String, FieldStats>) -> Vec<DriftIssue> {
    // TODO: probably need to rework this. Too many assumptions, maybe not even relevent
    let mut issues = Vec::new();

    // Check for version markers
    let version_markers = ["version", "schema_version", "v", "api_version"];
    for path in stats.keys() {
        let path_segments: Vec<&str> = path.split('.').collect();
        for marker in &version_markers {
            // Check if any path segment exactly matches the version marker
            if path_segments
                .iter()
                .any(|seg| seg.to_lowercase() == *marker)
            {
                issues.push(DriftIssue::SchemaEvolution {
                    path: path.clone(),
                    pattern: EvolutionPattern::VersionMarker {
                        marker_path: path.clone(),
                    },
                });
                break;
            }
        }
    }

    // Check for deprecated/legacy naming
    let deprecated_prefixes = ["old_", "legacy_", "deprecated_"];
    for path in stats.keys() {
        for prefix in &deprecated_prefixes {
            if path.to_lowercase().starts_with(prefix) {
                // Try to find the new field (without prefix)
                let potential_new = path.replacen(prefix, "", 1);
                if stats.contains_key(&potential_new) {
                    issues.push(DriftIssue::SchemaEvolution {
                        path: path.clone(),
                        pattern: EvolutionPattern::DeprecatedNaming {
                            old_path: path.clone(),
                            new_path: potential_new,
                        },
                    });
                }
                break;
            }
        }
    }

    // Check for mutually exclusive fields (same base path, different variants)
    // This is a more complex pattern - simplified version here
    // Making a lot of assuptions at this point
    let mut path_families: HashMap<String, Vec<String>> = HashMap::new();
    for path in stats.keys() {
        // Group by base path (e.g., "user.address" for "user.address_v1" and "user.address_v2")
        if let Some(base) = path.rsplit_once('_').map(|(base, _)| base) {
            path_families
                .entry(base.to_string())
                .or_default()
                .push(path.clone());
        }
    }

    for (base, paths) in path_families {
        if paths.len() >= 2 {
            // Check if they're mutually exclusive (sum of densities ~= max individual density)
            let densities: Vec<f64> = paths
                .iter()
                .filter_map(|p| stats.get(p).map(|s| s.density))
                .collect();
            if densities.len() >= 2 {
                let sum: f64 = densities.iter().sum();
                let max = densities.iter().copied().fold(0.0f64, f64::max);
                // If sum is close to max, they're likely mutually exclusive
                if (sum - max).abs() < 0.1 {
                    issues.push(DriftIssue::SchemaEvolution {
                        path: base,
                        pattern: EvolutionPattern::MutuallyExclusive { paths },
                    });
                }
            }
        }
    }

    issues
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_field_stats(
        path: &str,
        occurrences: u64,
        total_samples: u64,
        types: Vec<(JsonType, u64)>,
    ) -> FieldStats {
        let mut stats = FieldStats::new(path.to_string(), 1);
        stats.occurrences = occurrences;
        stats.total_samples = total_samples;
        stats.density = occurrences as f64 / total_samples as f64;
        for (json_type, count) in types {
            stats.types.insert(json_type, count);
        }
        stats
    }

    #[test]
    fn test_severity_ordering() {
        assert!(Severity::Critical > Severity::Warning);
        assert!(Severity::Warning > Severity::Info);
    }

    #[test]
    fn test_type_inconsistency_detection() {
        let config = DriftConfig::default();

        // 92% string, 8% number - should trigger (>5% minority)
        let stats = create_field_stats(
            "user.age",
            100,
            100,
            vec![(JsonType::String, 92), (JsonType::Number, 8)],
        );

        let issue = detect_type_inconsistency(&stats, &config);
        assert!(issue.is_some());

        let issue = issue.unwrap();
        assert_eq!(issue.severity(), Severity::Warning);
        assert!(matches!(issue, DriftIssue::TypeInconsistency { .. }));

        if let DriftIssue::TypeInconsistency {
            minority_percentage,
            types,
            ..
        } = issue
        {
            assert_eq!(minority_percentage, 8.0);
            assert_eq!(types.len(), 2);
        }
    }

    #[test]
    fn test_type_inconsistency_critical_threshold() {
        let config = DriftConfig::default();

        // 85% string, 15% number - should be Critical (≥10% minority)
        let stats = create_field_stats(
            "user.age",
            100,
            100,
            vec![(JsonType::String, 85), (JsonType::Number, 15)],
        );

        let issue = detect_type_inconsistency(&stats, &config).unwrap();
        assert_eq!(issue.severity(), Severity::Critical);
    }

    #[test]
    fn test_type_inconsistency_below_threshold() {
        let config = DriftConfig::default();

        // 98% string, 2% number - should NOT trigger (<5% minority)
        let stats = create_field_stats(
            "user.name",
            100,
            100,
            vec![(JsonType::String, 98), (JsonType::Number, 2)],
        );

        let issue = detect_type_inconsistency(&stats, &config);
        assert!(issue.is_none());
    }

    #[test]
    fn test_ghost_key_detection() {
        let config = DriftConfig::default();

        // 0.5% density - ghost key
        let stats = create_field_stats("billing.legacy_plan", 5, 1000, vec![(JsonType::String, 5)]);

        let issue = detect_ghost_key(&stats, &config);
        assert!(issue.is_some());

        let issue = issue.unwrap();
        assert!(matches!(issue, DriftIssue::GhostKey { .. }));
        assert_eq!(issue.severity(), Severity::Info);

        if let DriftIssue::GhostKey {
            density,
            occurunces,
            total_samples,
            ..
        } = issue
        {
            assert_eq!(density, 0.005);
            assert_eq!(occurunces, 5);
            assert_eq!(total_samples, 1000);
        }
    }

    #[test]
    fn test_ghost_key_below_threshold() {
        let config = DriftConfig::default();

        // 15% density - NOT a ghost key (>10% threshold)
        let stats =
            create_field_stats("user.middle_name", 150, 1000, vec![(JsonType::String, 150)]);

        let issue = detect_ghost_key(&stats, &config);
        assert!(issue.is_none());
    }

    #[test]
    fn test_missing_key_detection() {
        let config = DriftConfig::default();

        // 85% density - missing key (expected >95%)
        let stats = create_field_stats("user.email", 850, 1000, vec![(JsonType::String, 850)]);

        let issue = detect_missing_key(&stats, &config);
        assert!(issue.is_some());

        let issue = issue.unwrap();
        assert!(matches!(issue, DriftIssue::MissingKey { .. }));
        assert_eq!(issue.severity(), Severity::Critical);
    }

    #[test]
    fn test_missing_key_warning_threshold() {
        let config = DriftConfig::default();

        // 92% density - missing key warning (90-95%)
        let stats = create_field_stats("user.phone", 920, 1000, vec![(JsonType::String, 920)]);

        let issue = detect_missing_key(&stats, &config);
        assert!(issue.is_some());
        assert_eq!(issue.unwrap().severity(), Severity::Warning);
    }

    #[test]
    fn test_missing_key_above_threshold() {
        let config = DriftConfig::default();

        // 98% density - NOT a missing key (>95%)
        let stats = create_field_stats("user.id", 980, 1000, vec![(JsonType::Number, 980)]);

        let issue = detect_missing_key(&stats, &config);
        assert!(issue.is_none());
    }

    #[test]
    fn test_schema_evolution_version_marker() {
        let mut stats = HashMap::new();
        stats.insert(
            "schema_version".to_string(),
            create_field_stats("schema_version", 1000, 1000, vec![(JsonType::Number, 1000)]),
        );

        let issues = detect_schema_evolution(&stats);
        assert_eq!(issues.len(), 1);
        assert!(matches!(
            issues[0],
            DriftIssue::SchemaEvolution {
                pattern: EvolutionPattern::VersionMarker { .. },
                ..
            }
        ));
    }

    #[test]
    fn test_schema_evolution_deprecated_naming() {
        let mut stats = HashMap::new();
        stats.insert(
            "legacy_address".to_string(),
            create_field_stats("legacy_address", 100, 1000, vec![(JsonType::String, 100)]),
        );
        stats.insert(
            "address".to_string(),
            create_field_stats("address", 900, 1000, vec![(JsonType::String, 900)]),
        );

        let issues = detect_schema_evolution(&stats);
        assert!(!issues.is_empty());

        let deprecated = issues.iter().find(|i| {
            matches!(
                i,
                DriftIssue::SchemaEvolution {
                    pattern: EvolutionPattern::DeprecatedNaming { .. },
                    ..
                }
            )
        });
        assert!(deprecated.is_some());
    }

    #[test]
    fn test_detect_drift_comprehensive() {
        let mut stats = HashMap::new();

        // Type inconsistency
        stats.insert(
            "user.age".to_string(),
            create_field_stats(
                "user.age",
                100,
                100,
                vec![(JsonType::String, 92), (JsonType::Number, 8)],
            ),
        );

        // Ghost key
        stats.insert(
            "billing.legacy_plan".to_string(),
            create_field_stats("billing.legacy_plan", 5, 1000, vec![(JsonType::String, 5)]),
        );

        // Missing key
        stats.insert(
            "user.email".to_string(),
            create_field_stats("user.email", 850, 1000, vec![(JsonType::String, 850)]),
        );

        // Version marker
        stats.insert(
            "version".to_string(),
            create_field_stats("version", 1000, 1000, vec![(JsonType::Number, 1000)]),
        );

        let config = DriftConfig::default();
        let issues = detect_drift(&stats, &config);

        // Should find at least 4 issues (type inconsistency, ghost, missing, version)
        assert!(issues.len() >= 4);

        // Verify sorted by severity (Critical first)
        let severities: Vec<Severity> = issues.iter().map(|i| i.severity()).collect();
        let mut sorted_severities = severities.clone();
        sorted_severities.sort_by(|a, b| b.cmp(a));
        assert_eq!(severities, sorted_severities);
    }

    #[test]
    fn test_drift_issue_description() {
        let issue = DriftIssue::TypeInconsistency {
            path: "user.age".to_string(),
            types: {
                let mut map = HashMap::new();
                map.insert(
                    JsonType::String,
                    TypeDistribution {
                        json_type: JsonType::String,
                        count: 92,
                        percentage: 92.0,
                    },
                );
                map.insert(
                    JsonType::Number,
                    TypeDistribution {
                        json_type: JsonType::Number,
                        count: 8,
                        percentage: 8.0,
                    },
                );
                map
            },
            minority_percentage: 8.0,
        };

        let desc = issue.description();
        assert!(desc.contains("Type inconsistency"));
        assert!(desc.contains("8.0%"));
    }

    #[test]
    fn test_custom_config_thresholds() {
        let config = DriftConfig {
            type_inconsistency_threshold: 10.0,
            ghost_key_threshold: 0.005,
            sparse_field_threshold: 0.70,
            missing_key_threshold: 0.99,
            detect_schema_evolution: false,
        };

        // 8% minority - should NOT trigger with 10% threshold
        let stats = create_field_stats(
            "user.age",
            100,
            100,
            vec![(JsonType::String, 92), (JsonType::Number, 8)],
        );

        let issue = detect_type_inconsistency(&stats, &config);
        assert!(issue.is_none());
    }
}
