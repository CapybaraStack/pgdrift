use crate::stats::FieldStats;
use serde_json::Value;
use std::collections::HashMap;

pub struct JsonAnalyzer {
    stats: HashMap<String, FieldStats>,
    total_samples: u64,
}

impl Default for JsonAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonAnalyzer {
    pub fn new() -> Self {
        Self {
            stats: HashMap::new(),
            total_samples: 0,
        }
    }

    /// Analyze a sing json document
    pub fn analyze(&mut self, value: &Value) {
        self.total_samples += 1;
        self.walk("", value, 0);
    }

    /// Recursive walk
    fn walk(&mut self, path: &str, value: &Value, depth: usize) {
        match value {
            Value::Object(map) => {
                for (key, val) in map {
                    let field_path = if path.is_empty() {
                        key.clone()
                    } else {
                        format!("{}.{}", path, key)
                    };

                    self.record_field(&field_path, val, depth + 1);

                    self.walk(&field_path, val, depth + 1);
                }
            }
            Value::Array(arr) => {
                let array_path = format!("{}[]", path);

                for item in arr {
                    self.walk(&array_path, item, depth + 1);
                }
            }
            _ => {
                // Leaf node recorded by parent
            }
        }
    }

    fn record_field(&mut self, path: &str, value: &Value, depth: usize) {
        self.stats
            .entry(path.to_string())
            .or_insert_with(|| FieldStats::new(path.to_string(), depth))
            .record(value);
    }

    pub fn finalize(mut self) -> HashMap<String, FieldStats> {
        for stats in self.stats.values_mut() {
            stats.finalize(self.total_samples);
        }
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::JsonType;
    use serde_json::json;

    #[test]
    fn test_flat_object() {
        let mut analyzer = JsonAnalyzer::new();

        analyzer.analyze(&json!({
            "name": "Alice",
            "age": 30
        }));

        let stats = analyzer.finalize();

        assert_eq!(stats.len(), 2);
        assert!(stats.contains_key("name"));
        assert!(stats.contains_key("age"));

        let name_stats = &stats["name"];
        assert_eq!(name_stats.occurrences, 1);
        assert_eq!(name_stats.density, 1.0);
        assert_eq!(name_stats.depth, 1);
    }

    #[test]
    fn test_nested_object() {
        let mut analyzer = JsonAnalyzer::new();

        analyzer.analyze(&json!({
            "user": {
                "profile": {
                    "email": "test@example.com"
                }
            }
        }));

        let stats = analyzer.finalize();

        assert!(stats.contains_key("user"));
        assert!(stats.contains_key("user.profile"));
        assert!(stats.contains_key("user.profile.email"));

        assert_eq!(stats["user.profile.email"].depth, 3);
    }

    #[test]
    fn test_array_with_objects() {
        let mut analyzer = JsonAnalyzer::new();

        analyzer.analyze(&json!({
            "addresses": [
                {"city": "Brisbane", "zip": "4000"},
                {"city": "Sydney", "zip": "2000"}
            ]
        }));

        let stats = analyzer.finalize();

        // Should have: addresses, addresses[], addresses[].city, addresses[].zip
        assert!(stats.contains_key("addresses"));
        assert!(stats.contains_key("addresses[].city"));
        assert!(stats.contains_key("addresses[].zip"));

        // Each item in array was seen once, but we analyzed 1 document
        let city_stats = &stats["addresses[].city"];
        assert_eq!(city_stats.occurrences, 2); // Appears in both array items
    }

    #[test]
    fn test_multiple_documents_density() {
        let mut analyzer = JsonAnalyzer::new();

        // 3 documents, "nickname" only in 1
        analyzer.analyze(&json!({"name": "Alice", "nickname": "Al"}));
        analyzer.analyze(&json!({"name": "Bob"}));
        analyzer.analyze(&json!({"name": "Carol"}));

        let stats = analyzer.finalize();

        let name_stats = &stats["name"];
        assert_eq!(name_stats.occurrences, 3);
        assert_eq!(name_stats.density, 1.0); // 100%

        let nickname_stats = &stats["nickname"];
        assert_eq!(nickname_stats.occurrences, 1);
        assert!((nickname_stats.density - 0.333).abs() < 0.01); // ~33%
    }

    #[test]
    fn test_type_inconsistency() {
        let mut analyzer = JsonAnalyzer::new();

        analyzer.analyze(&json!({"age": "25"})); // String
        analyzer.analyze(&json!({"age": 30})); // Number
        analyzer.analyze(&json!({"age": "35"})); // String

        let stats = analyzer.finalize();
        let age_stats = &stats["age"];

        // Should track both types
        assert_eq!(age_stats.types.get(&JsonType::String), Some(&2));
        assert_eq!(age_stats.types.get(&JsonType::Number), Some(&1));
    }

    #[test]
    fn test_null_tracking() {
        let mut analyzer = JsonAnalyzer::new();

        analyzer.analyze(&json!({"optional": null}));
        analyzer.analyze(&json!({"optional": "value"}));
        analyzer.analyze(&json!({"optional": null}));

        let stats = analyzer.finalize();
        let optional_stats = &stats["optional"];

        assert_eq!(optional_stats.null_count, 2);
        assert_eq!(optional_stats.occurrences, 3);
    }

    #[test]
    fn test_empty_array() {
        let mut analyzer = JsonAnalyzer::new();

        analyzer.analyze(&json!({"items": []}));

        let stats = analyzer.finalize();

        // Should record the array field itself
        assert!(stats.contains_key("items"));

        // But no items in the array
        assert!(!stats.contains_key("items[]"));
    }

    #[test]
    fn test_deep_nesting() {
        let mut analyzer = JsonAnalyzer::new();

        analyzer.analyze(&json!({
            "level1": {
                "level2": {
                    "level3": {
                        "level4": {
                            "value": "deep"
                        }
                    }
                }
            }
        }));

        let stats = analyzer.finalize();

        assert_eq!(stats["level1"].depth, 1);
        assert_eq!(stats["level1.level2"].depth, 2);
        assert_eq!(stats["level1.level2.level3"].depth, 3);
        assert_eq!(stats["level1.level2.level3.level4"].depth, 4);
        assert_eq!(stats["level1.level2.level3.level4.value"].depth, 5);
    }

    #[test]
    fn test_examples_collection() {
        let mut analyzer = JsonAnalyzer::new();

        for i in 0..15 {
            analyzer.analyze(&json!({"value": i}));
        }

        let stats = analyzer.finalize();
        let value_stats = &stats["value"];

        // Should limit to 10 examples
        assert_eq!(value_stats.examples.len(), 10);
    }
}
