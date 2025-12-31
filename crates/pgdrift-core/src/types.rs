use serde::Serialize;
use serde_json::Value;
use std::fmt;

/// Represet the different json types we can encouter
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, Serialize)]
pub enum JsonType {
    Null,
    Boolean,
    Number,
    String,
    Array,
    Object,
}

impl JsonType {
    /// Determine the JsonTpe from a serde_json::Value
    pub fn from_value(value: &Value) -> Self {
        match value {
            Value::Null => JsonType::Null,
            Value::Bool(_) => JsonType::Boolean,
            Value::Number(_) => JsonType::Number,
            Value::String(_) => JsonType::String,
            Value::Array(_) => JsonType::Array,
            Value::Object(_) => JsonType::Object,
        }
    }
}

impl fmt::Display for JsonType {
    /// Display nicely
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JsonType::Null => write!(f, "null"),

            JsonType::Boolean => write!(f, "boolean"),
            JsonType::Number => write!(f, "number"),
            JsonType::String => write!(f, "string"),
            JsonType::Array => write!(f, "array"),
            JsonType::Object => write!(f, "object"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_jsoon_type_from_value() {
        assert_eq!(JsonType::from_value(&json!(null)), JsonType::Null);
        assert_eq!(JsonType::from_value(&json!(true)), JsonType::Boolean);
        assert_eq!(JsonType::from_value(&json!(32)), JsonType::Number);
        assert_eq!(JsonType::from_value(&json!(32.33)), JsonType::Number);
        assert_eq!(JsonType::from_value(&json!("hello")), JsonType::String);
        assert_eq!(JsonType::from_value(&json!([])), JsonType::Array);
        assert_eq!(JsonType::from_value(&json!({})), JsonType::Object);
    }

    #[test]
    fn test_json_type_display() {
        assert_eq!(JsonType::Null.to_string(), "null");
        assert_eq!(JsonType::Boolean.to_string(), "boolean");
        assert_eq!(JsonType::Number.to_string(), "number");
        assert_eq!(JsonType::String.to_string(), "string");
        assert_eq!(JsonType::Array.to_string(), "array");
        assert_eq!(JsonType::Object.to_string(), "object");
    }
}
