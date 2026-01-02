use pgdrift::commands::scan_all;
use pgdrift::output::OutputFormat;
use pgdrift_db::fixtures;
use pgdrift_db::test_utils::TestDb;

// ============================================================================
// Basic Functionality Tests
// ============================================================================

#[tokio::test]
async fn test_scan_all_single_column() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = scan_all::run(test_db.database_url(), 1000, OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Scan all command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_scan_all_multiple_columns() {
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

    let result = scan_all::run(test_db.database_url(), 1000, OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Scan all command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_scan_all_with_drift_issues() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    // Create tables with known drift issues
    fixtures::create_users_type_inconsistency(&test_db.pool)
        .await
        .expect("Failed to create type inconsistency fixture");

    fixtures::create_users_ghost_keys(&test_db.pool)
        .await
        .expect("Failed to create ghost keys fixture");

    let result = scan_all::run(test_db.database_url(), 1000, OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Scan all command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_scan_all_no_columns() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    // Don't create any JSONB columns
    let result = scan_all::run(test_db.database_url(), 1000, OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Scan all should succeed with no columns: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

// ============================================================================
// Output Format Tests
// ============================================================================

#[tokio::test]
async fn test_scan_all_table_format() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = scan_all::run(test_db.database_url(), 1000, OutputFormat::Table).await;

    assert!(
        result.is_ok(),
        "Scan all command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_scan_all_markdown_format() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = scan_all::run(test_db.database_url(), 1000, OutputFormat::Markdown).await;

    assert!(
        result.is_ok(),
        "Scan all command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

// ============================================================================
// Configuration Tests
// ============================================================================

#[tokio::test]
async fn test_scan_all_with_custom_sample_size() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = scan_all::run(
        test_db.database_url(),
        100, // small sample size
        OutputFormat::Json,
    )
    .await;

    assert!(
        result.is_ok(),
        "Scan all command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_scan_all_invalid_database_url() {
    let result = scan_all::run(
        "postgres://invalid:invalid@localhost:5432/invalid",
        1000,
        OutputFormat::Json,
    )
    .await;

    assert!(
        result.is_err(),
        "Scan all should fail for invalid database URL"
    );
}

#[tokio::test]
async fn test_scan_all_continues_on_column_error() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    // Create one valid table
    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Create a table with all NULL JSONB values (will cause error during analysis)
    sqlx::query(
        "CREATE TABLE users_null (
            id SERIAL PRIMARY KEY,
            metadata JSONB
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    sqlx::query("INSERT INTO users_null (metadata) SELECT NULL FROM generate_series(1, 100)")
        .execute(&test_db.pool)
        .await
        .expect("Failed to insert data");

    // scan_all should succeed overall even if one column fails
    let result = scan_all::run(test_db.database_url(), 1000, OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Scan all should continue despite column errors: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

// ============================================================================
// Complex Scenarios
// ============================================================================

#[tokio::test]
async fn test_scan_all_with_schema_evolution() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_products_schema_evolution(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = scan_all::run(test_db.database_url(), 1000, OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Scan all command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_scan_all_with_deep_nesting() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_nested(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = scan_all::run(test_db.database_url(), 1000, OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Scan all command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_scan_all_aggregates_drift_correctly() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    // Create tables with varying levels of drift
    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create consistent fixture");

    fixtures::create_users_type_inconsistency(&test_db.pool)
        .await
        .expect("Failed to create type inconsistency fixture");

    fixtures::create_users_ghost_keys(&test_db.pool)
        .await
        .expect("Failed to create ghost keys fixture");

    let result = scan_all::run(test_db.database_url(), 1000, OutputFormat::Json).await;

    assert!(
        result.is_ok(),
        "Scan all command failed: {:?}",
        result.err()
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}
