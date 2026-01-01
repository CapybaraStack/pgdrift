use pgdrift_cli::commands::index;
use pgdrift_cli::output::OutputFormat;
use pgdrift_db::fixtures;
use pgdrift_db::test_utils::TestDb;
use serde_json::json;

// ============================================================================
// Basic Functionality Tests
// ============================================================================

#[tokio::test]
async fn test_index_recommendations_consistent_schema() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "users",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Index command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_recommendations_sparse_fields() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_ghost_keys(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "users_sparse",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Index command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_recommendations_type_inconsistency() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_type_inconsistency(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "users_mixed_types",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Index command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_with_table_format() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "users",
        "metadata",
        1000,
        OutputFormat::Table,
        false,
    )
    .await;

    assert!(result.is_ok(), "Index command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_with_markdown_format() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "users",
        "metadata",
        1000,
        OutputFormat::Markdown,
        false,
    )
    .await;

    assert!(result.is_ok(), "Index command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_with_schema_table_notation() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "public.users",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Index command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_with_production_mode() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "users",
        "metadata",
        1000,
        OutputFormat::Json,
        true, // production_mode
    )
    .await;

    assert!(result.is_ok(), "Index command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_with_custom_sample_size() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "users",
        "metadata",
        100, // small sample size
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Index command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_index_invalid_table() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "nonexistent_table",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_err(),
        "Index command should fail for invalid table"
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_invalid_column() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "users",
        "nonexistent_column",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_err(),
        "Index command should fail for invalid column"
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_invalid_database_url() {
    let result = index::run(
        "postgres://invalid:invalid@localhost:5432/invalid",
        "users",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_err(),
        "Index command should fail for invalid database URL"
    );
}

#[tokio::test]
async fn test_index_empty_column() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    // Create table with empty JSONB column
    sqlx::query(
        "CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            metadata JSONB
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Insert rows with NULL metadata
    sqlx::query("INSERT INTO users (metadata) SELECT NULL FROM generate_series(1, 100)")
        .execute(&test_db.pool)
        .await
        .expect("Failed to insert data");

    let result = index::run(
        test_db.database_url(),
        "users",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_err(),
        "Index command should fail for empty column"
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

// ============================================================================
// Data Type Tests
// ============================================================================

#[tokio::test]
async fn test_index_with_high_density_strings() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            metadata JSONB
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Insert 1000 rows with high-density string fields
    for i in 0..1000 {
        let metadata = json!({
            "email": format!("user{}@example.com", i),
            "name": format!("User {}", i)
        });

        sqlx::query("INSERT INTO users (metadata) VALUES ($1)")
            .bind(metadata)
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert data");
    }

    let result = index::run(
        test_db.database_url(),
        "users",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Index command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_with_number_fields() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            metadata JSONB
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Insert rows with medium-density number fields
    for i in 0..1000 {
        let metadata = if i % 2 == 0 {
            json!({
                "age": 25 + (i % 50),
                "score": 75 + (i % 25)
            })
        } else {
            json!({
                "name": format!("User {}", i)
            })
        };

        sqlx::query("INSERT INTO users (metadata) VALUES ($1)")
            .bind(metadata)
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert data");
    }

    let result = index::run(
        test_db.database_url(),
        "users",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Index command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_with_boolean_fields() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    sqlx::query(
        "CREATE TABLE users (
            id SERIAL PRIMARY KEY,
            metadata JSONB
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Insert rows with medium-density boolean fields
    for i in 0..1000 {
        let metadata = if i % 2 == 0 {
            json!({
                "is_active": true,
                "is_verified": i % 3 == 0
            })
        } else {
            json!({
                "name": format!("User {}", i)
            })
        };

        sqlx::query("INSERT INTO users (metadata) VALUES ($1)")
            .bind(metadata)
            .execute(&test_db.pool)
            .await
            .expect("Failed to insert data");
    }

    let result = index::run(
        test_db.database_url(),
        "users",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(result.is_ok(), "Index command failed: {:?}", result.err());

    test_db.cleanup().await.expect("Failed to cleanup");
}

// ============================================================================
// SQL Injection Protection Tests
// ============================================================================

#[tokio::test]
async fn test_index_sql_injection_in_table_name() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "users; DROP TABLE users; --",
        "metadata",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_err(),
        "Index command should reject SQL injection in table name"
    );

    // Verify table still exists
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&test_db.pool)
        .await
        .expect("Failed to query table");

    assert!(count > 0, "Table should not be dropped");

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_index_sql_injection_in_column_name() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let result = index::run(
        test_db.database_url(),
        "users",
        "metadata; DROP TABLE users; --",
        1000,
        OutputFormat::Json,
        false,
    )
    .await;

    assert!(
        result.is_err(),
        "Index command should reject SQL injection in column name"
    );

    // Verify table still exists
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM users")
        .fetch_one(&test_db.pool)
        .await
        .expect("Failed to query table");

    assert!(count > 0, "Table should not be dropped");

    test_db.cleanup().await.expect("Failed to cleanup");
}
