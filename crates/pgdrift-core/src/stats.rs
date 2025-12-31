use crate::types::JsonType;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize)]
pub struct FieldStats {
    pub path: String,
    pub occurrences: u64,
    pub total_samples: u64,
    pub density: f64,
    pub null_count: u64,
    pub types: HashMap<JsonType, u64>,
    pub examples: Vec<Value>,
    pub depth: usize,
}

impl FieldStats {
    pub fn new(path: String, depth: usize) -> Self {
        Self {
            path,
            occurrences: 0,
            total_samples: 0,
            density: 0.0,
            null_count: 0,
            types: HashMap::new(),
            examples: Vec::new(),
            depth,
        }
    }

    /// Record and occurence of this field with its value
    pub fn record(&mut self, value: &Value) {
        self.occurrences += 1;

        let json_type = JsonType::from_value(value);
        *self.types.entry(json_type).or_insert(0) += 1;

        if matches!(value, Value::Null) {
            self.null_count += 1;
        }

        // store examples  - max 10
        if self.examples.len() < 10 {
            self.examples.push(value.clone());
        }
    }

    pub fn finalize(&mut self, total_samples: u64) {
        self.total_samples = total_samples;

        if self.total_samples > 0 {
            self.density = self.occurrences as f64 / self.total_samples as f64;
        }
    }
}
