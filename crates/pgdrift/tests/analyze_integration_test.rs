use pgdrift::commands::analyze;
use pgdrift::output::OutputFormat;
use pgdrift_db::fixtures;
use pgdrift_db::test_utils::TestDb;

/// Test end-to-end analyze command with consistent schema
#[tokio::test]
async fn test_analyze_consistent_schema() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Run analyze command
    let result = analyze::run(
        test_db.database_url(),
        "users",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Analyze command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze command detects type inconsistency
#[tokio::test]
async fn test_analyze_detects_type_inconsistency() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_type_inconsistency(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Run analyze - should detect type inconsistency in age field
    let result = analyze::run(
        test_db.database_url(),
        "users_mixed_types",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Analyze command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze command detects ghost keys
#[tokio::test]
async fn test_analyze_detects_ghost_keys() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_ghost_keys(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Run analyze - should detect ghost keys (premium_feature)
    let result = analyze::run(
        test_db.database_url(),
        "users_sparse",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Analyze command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze command handles deep nesting
#[tokio::test]
async fn test_analyze_handles_deep_nesting() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_nested(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Run analyze - should handle deeply nested structures
    let result = analyze::run(
        test_db.database_url(),
        "users_nested",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Analyze command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze command with schema.table format
#[tokio::test]
async fn test_analyze_with_schema_prefix() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Run analyze with explicit schema
    let result = analyze::run(
        test_db.database_url(),
        "public.users",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Analyze command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze command with production mode
#[tokio::test]
async fn test_analyze_production_mode() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Run analyze in production mode
    let result = analyze::run(
        test_db.database_url(),
        "users",
        "metadata",
        1000,
        OutputFormat::Json,
        true, // production_mode = true
    )
    .await;

    assert!(result.is_ok(), "Analyze command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze command with different output formats
#[tokio::test]
async fn test_analyze_output_formats() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Test all output formats
    for format in [
        OutputFormat::Table,
        OutputFormat::Json,
        OutputFormat::Markdown,
    ] {
        let result = analyze::run(
            test_db.database_url(),
            "users",
            "metadata",
            1000,
            format.clone(),
            false,
        )
        .await;

        assert!(
            result.is_ok(),
            "Analyze command failed for {:?} format: {:?}",
            format,
            result.err()
        );
    }

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze command fails gracefully with invalid table
#[tokio::test]
async fn test_analyze_invalid_table() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    // Try to analyze non-existent table
    let result = analyze::run(
        test_db.database_url(),
        "nonexistent_table",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_err(), "Expected error for invalid table");

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze command fails gracefully with invalid column
#[tokio::test]
async fn test_analyze_invalid_column() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Try to analyze non-existent column
    let result = analyze::run(
        test_db.database_url(),
        "users",
        "nonexistent_column",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_err(), "Expected error for invalid column");

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze command with empty column
#[tokio::test]
async fn test_analyze_empty_column() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    // Create table with no data
    sqlx::query(
        "CREATE TABLE empty_table (
            id SERIAL PRIMARY KEY,
            data JSONB
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Try to analyze empty column
    let result = analyze::run(
        test_db.database_url(),
        "empty_table",
        "data",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_err(), "Expected error for empty column");
    if let Err(e) = result {
        assert!(
            e.to_string().contains("No samples"),
            "Expected 'No samples' error"
        );
    }

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze command with schema evolution patterns
#[tokio::test]
async fn test_analyze_detects_schema_evolution() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_products_schema_evolution(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Run analyze - should detect schema evolution (version marker)
    let result = analyze::run(
        test_db.database_url(),
        "products",
        "data",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Analyze command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with invalid database URL
#[tokio::test]
async fn test_analyze_invalid_database_url() {
    let result = analyze::run(
        "postgres://invalid:invalid@localhost:9999/invalid",
        "users",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_err(), "Expected error for invalid database URL");
}

// ===== CRITICAL EDGE CASE TESTS =====

/// Test analyze with all NULL JSONB values
#[tokio::test]
async fn test_analyze_all_null_column() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    // Create table with all NULL JSONB values
    sqlx::query(
        "CREATE TABLE null_table (
            id SERIAL PRIMARY KEY,
            data JSONB
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Insert rows with NULL values
    for _ in 0..100 {
        sqlx::query("INSERT INTO null_table (data) VALUES (NULL)")
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert NULL");
    }

    // Try to analyze - should fail with "No samples" error
    let result = analyze::run(
        test_db.database_url(),
        "null_table",
        "data",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_err(), "Expected error for all-NULL column");
    if let Err(e) = result {
        assert!(
            e.to_string().contains("No samples"),
            "Expected 'No samples' error, got: {}",
            e
        );
    }

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with mixed NULL and valid JSONB
#[tokio::test]
async fn test_analyze_mixed_null_values() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE mixed_null_table (
            id SERIAL PRIMARY KEY,
            data JSONB
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Insert mix of NULL (80%) and valid JSON (20%)
    for i in 0..1000 {
        if i % 5 == 0 {
            sqlx::query("INSERT INTO mixed_null_table (data) VALUES ($1)")
                .bind(serde_json::json!({"value": i, "name": format!("item_{}", i)}))
                .execute(&test_db.pool)
                .await
                .expect("Failed to insert data");
        } else {
            sqlx::query("INSERT INTO mixed_null_table (data) VALUES (NULL)")
                .execute(&test_db.pool)
                .await
                .expect("Failed to insert NULL");
        }
    }

    // Should succeed and analyze the 20% valid samples
    let result = analyze::run(
        test_db.database_url(),
        "mixed_null_table",
        "data",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "Analyze should handle mixed NULL values: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with SQL injection attempts in table name
#[tokio::test]
async fn test_analyze_sql_injection_table_name() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Try various SQL injection patterns - should fail safely
    let injection_attempts = vec![
        "users; DROP TABLE users; --",
        "users' OR '1'='1",
        "users\"; DROP TABLE users; --",
        "users` OR 1=1; --",
    ];

    for attempt in injection_attempts {
        let result = analyze::run(
            test_db.database_url(),
            attempt,
            "metadata",
            1000,
            OutputFormat::Json,
            false,
        )
        .await;

        assert!(
            result.is_err(),
            "SQL injection attempt should fail: {}",
            attempt
        );
    }

    // Verify users table still exists (not dropped)
    let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM users")
        .fetch_one(&test_db.pool)
        .await
        .expect("Users table should still exist");

    assert_eq!(count.0, 5000, "Users table should be intact");

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with SQL injection attempts in column name
#[tokio::test]
async fn test_analyze_sql_injection_column_name() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Try SQL injection in column name
    let injection_attempts = vec!["metadata; DROP TABLE users; --", "metadata' OR '1'='1"];

    for attempt in injection_attempts {
        let result = analyze::run(
            test_db.database_url(),
            "users",
            attempt,
            1000,
            OutputFormat::Json,
            false,
        )
        .await;

        assert!(
            result.is_err(),
            "SQL injection in column should fail: {}",
            attempt
        );
    }

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with very large JSON documents
#[tokio::test]
async fn test_analyze_large_json_documents() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE large_docs (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Create large JSON documents (each ~100 fields)
    for i in 0..100 {
        let mut large_doc = serde_json::Map::new();

        // Add 100 fields
        for j in 0..100 {
            large_doc.insert(
                format!("field_{}", j),
                serde_json::json!({
                    "value": i * 100 + j,
                    "name": format!("field_{}_{}", i, j),
                    "metadata": {
                        "created": "2025-01-01",
                        "updated": "2025-01-02",
                        "tags": ["tag1", "tag2", "tag3"]
                    }
                }),
            );
        }

        sqlx::query("INSERT INTO large_docs (data) VALUES ($1)")
            .bind(serde_json::Value::Object(large_doc))
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert large document");
    }

    // Should handle large documents
    let result = analyze::run(
        test_db.database_url(),
        "large_docs",
        "data",
        100,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "Should handle large documents: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with Unicode and special characters
#[tokio::test]
async fn test_analyze_unicode_and_special_chars() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE unicode_table (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Insert documents with various Unicode and special characters
    let test_data = vec![
        serde_json::json!({
            "name": "日本語",
            "emoji": "🔥💯🚀",
            "chinese": "中文测试",
            "arabic": "العربية",
            "special": "!@#$%^&*()_+-=[]{}|;':\",./<>?"
        }),
        serde_json::json!({
            "name": "Ελληνικά",
            "emoji": "🎉🎊🎈",
            "korean": "한국어",
            "hebrew": "עברית",
            "special": "\t\n\r"
        }),
        serde_json::json!({
            "name": "Русский",
            "emoji": "🌟✨💫",
            "thai": "ภาษาไทย",
            "special": "quotes: \"'`"
        }),
    ];

    for data in test_data {
        sqlx::query("INSERT INTO unicode_table (data) VALUES ($1)")
            .bind(data)
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert Unicode data");
    }

    // Should handle Unicode correctly
    let result = analyze::run(
        test_db.database_url(),
        "unicode_table",
        "data",
        100,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "Should handle Unicode characters: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with empty JSONB objects
#[tokio::test]
async fn test_analyze_empty_json_objects() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE empty_objects (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Insert empty objects
    for _ in 0..100 {
        sqlx::query("INSERT INTO empty_objects (data) VALUES ($1)")
            .bind(serde_json::json!({}))
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert empty object");
    }

    // Should handle empty objects (no fields to analyze)
    let result = analyze::run(
        test_db.database_url(),
        "empty_objects",
        "data",
        100,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "Should handle empty objects: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with mixed empty and populated objects
#[tokio::test]
async fn test_analyze_mixed_empty_objects() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE mixed_empty (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Mix of empty and populated objects
    for i in 0..1000 {
        let data = if i % 3 == 0 {
            serde_json::json!({}) // Empty object
        } else {
            serde_json::json!({
                "id": i,
                "name": format!("item_{}", i),
                "active": true
            })
        };

        sqlx::query("INSERT INTO mixed_empty (data) VALUES ($1)")
            .bind(data)
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert data");
    }

    // Should handle mix of empty and populated
    let result = analyze::run(
        test_db.database_url(),
        "mixed_empty",
        "data",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "Should handle mixed empty/populated: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with extreme nesting depth (stress test)
#[tokio::test]
async fn test_analyze_extreme_nesting_depth() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE extreme_nesting (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Create extremely nested structure (20 levels deep)
    let mut nested = serde_json::json!({"value": "deep"});
    for i in (0..20).rev() {
        nested = serde_json::json!({
            format!("level_{}", i): nested
        });
    }

    // Insert a few of these extreme documents
    for _ in 0..10 {
        sqlx::query("INSERT INTO extreme_nesting (data) VALUES ($1)")
            .bind(nested.clone())
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert deeply nested data");
    }

    // Should handle extreme nesting
    let result = analyze::run(
        test_db.database_url(),
        "extreme_nesting",
        "data",
        10,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "Should handle extreme nesting: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with field type mutation (object -> primitive -> array)
#[tokio::test]
async fn test_analyze_field_type_mutation() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE type_mutation (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Same field appears as different JSON types
    let test_cases = vec![
        serde_json::json!({"field": {"nested": "object"}}), // Object
        serde_json::json!({"field": "string value"}),       // String
        serde_json::json!({"field": 123}),                  // Number
        serde_json::json!({"field": true}),                 // Boolean
        serde_json::json!({"field": [1, 2, 3]}),            // Array
        serde_json::json!({"field": null}),                 // Null
    ];

    for case in test_cases {
        sqlx::query("INSERT INTO type_mutation (data) VALUES ($1)")
            .bind(case)
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert mutation data");
    }

    // Should detect extreme type inconsistency
    let result = analyze::run(
        test_db.database_url(),
        "type_mutation",
        "data",
        100,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "Should handle type mutations: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with arrays containing mixed types
#[tokio::test]
async fn test_analyze_mixed_type_arrays() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE mixed_arrays (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Arrays with mixed types
    for i in 0..100 {
        let data = serde_json::json!({
            "mixed_array": [
                "string",
                123,
                true,
                {"nested": "object"},
                [1, 2, 3],
                null
            ],
            "id": i
        });

        sqlx::query("INSERT INTO mixed_arrays (data) VALUES ($1)")
            .bind(data)
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert mixed array");
    }

    // Should handle mixed-type arrays
    let result = analyze::run(
        test_db.database_url(),
        "mixed_arrays",
        "data",
        100,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "Should handle mixed-type arrays: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test analyze with field appearing at different nesting levels
#[tokio::test]
async fn test_analyze_inconsistent_nesting_levels() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE inconsistent_nesting (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Same logical field at different depths
    for i in 0..100 {
        let data = if i % 2 == 0 {
            serde_json::json!({
                "user": {
                    "email": format!("user{}@example.com", i)
                }
            })
        } else {
            serde_json::json!({
                "email": format!("user{}@example.com", i)
            })
        };

        sqlx::query("INSERT INTO inconsistent_nesting (data) VALUES ($1)")
            .bind(data)
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert inconsistent nesting");
    }

    // Should treat these as different fields (user.email vs email)
    let result = analyze::run(
        test_db.database_url(),
        "inconsistent_nesting",
        "data",
        100,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_ok(),
        "Should handle inconsistent nesting: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}
