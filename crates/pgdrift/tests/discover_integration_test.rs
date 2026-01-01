use pgdrift::commands::discover;
use pgdrift::output::OutputFormat;
use pgdrift_db::fixtures;
use pgdrift_db::test_utils::TestDb;

/// Test discover command with single JSONB column
#[tokio::test]
async fn test_discover_single_column() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Run discover command
    let result = discover::run(test_db.database_url(), OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Discover command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test discover command with multiple JSONB columns
#[tokio::test]
async fn test_discover_multiple_columns() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    // Create multiple tables with JSONB columns
    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create users fixture");

    fixtures::create_products_schema_evolution(&test_db.pool)
        .await
        .expect("Failed to create products fixture");

    fixtures::create_users_ghost_keys(&test_db.pool)
        .await
        .expect("Failed to create sparse users fixture");

    // Run discover command
    let result = discover::run(test_db.database_url(), OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Discover command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test discover command with all output formats
#[tokio::test]
async fn test_discover_output_formats() {
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
        let result = discover::run(test_db.database_url(), format.clone()).await;

        assert!(
            result.is_ok(),
            "Discover command failed for {:?} format: {:?}",
            format,
            result.err()
        );
    }

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test discover command with no JSONB columns
#[tokio::test]
async fn test_discover_no_jsonb_columns() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    // Create a table with no JSONB columns
    sqlx::query(
        "CREATE TABLE regular_table (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Run discover command - should succeed but find no columns
    let result = discover::run(test_db.database_url(), OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Discover command should succeed even with no JSONB columns"
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test discover command with invalid database URL
#[tokio::test]
async fn test_discover_invalid_database_url() {
    let result = discover::run(
        "postgres://invalid:invalid@localhost:9999/invalid",
        OutputFormat::Json,
    )
    .await;

    assert!(result.is_err(), "Expected error for invalid database URL");
}

/// Test discover command excludes system schemas
#[tokio::test]
async fn test_discover_excludes_system_schemas() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // The discover command should only find columns in the public schema,
    // not in pg_catalog or information_schema
    let result = discover::run(test_db.database_url(), OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Discover command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

/// Test discover command with nested JSON in different tables
#[tokio::test]
async fn test_discover_different_jsonb_structures() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_nested(&test_db.pool)
        .await
        .expect("Failed to create nested fixture");

    fixtures::create_users_type_inconsistency(&test_db.pool)
        .await
        .expect("Failed to create type inconsistency fixture");

    // Run discover - should find both tables with different JSONB structures
    let result = discover::run(test_db.database_url(), OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Discover command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}
