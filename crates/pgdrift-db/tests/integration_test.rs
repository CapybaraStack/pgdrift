use pgdrift_db::discover_jsonb_columns;
use pgdrift_db::test_utils::TestDb;
use pgdrift_db::{Sampler, SamplingStrategy};

#[tokio::test]
async fn test_discover_consistent_schema() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    pgdrift_db::fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create consistent fixture");

    let columns = discover_jsonb_columns(&test_db.pool)
        .await
        .expect("Failed to discover JSONB columns");

    // Should find the users.metadata column
    let users_metadata = columns
        .iter()
        .find(|col| col.table == "users" && col.column == "metadata");

    assert!(
        users_metadata.is_some(),
        "Failed to discover users.metadata column"
    );

    let col = users_metadata.unwrap();
    assert_eq!(col.schema, "public");
    // estimated_rows is from pg_stat_user_tables which isn't immediately updated
    // Just check it exists and is a reasonable number
    assert!(col.estimated_rows.is_some());
    assert!(col.estimated_rows.unwrap() > 0);

    test_db
        .cleanup()
        .await
        .expect("Failed to cleanup test database");
}

#[tokio::test]
async fn test_discover_multiple_tables() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    pgdrift_db::fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create users fixture");

    pgdrift_db::fixtures::create_products_schema_evolution(&test_db.pool)
        .await
        .expect("Failed to create products fixture");

    let columns = discover_jsonb_columns(&test_db.pool)
        .await
        .expect("Failed to discover JSONB columns");

    // Should find both users.metadata and products.data
    assert!(
        columns.iter().any(|col| col.table == "users"),
        "Failed to find users table"
    );
    assert!(
        columns.iter().any(|col| col.table == "products"),
        "Failed to find products table"
    );

    test_db
        .cleanup()
        .await
        .expect("Failed to cleanup test database");
}

#[tokio::test]
async fn test_discover_sparse_schema() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    pgdrift_db::fixtures::create_users_ghost_keys(&test_db.pool)
        .await
        .expect("Failed to create sparse fixture");

    let columns = discover_jsonb_columns(&test_db.pool)
        .await
        .expect("Failed to discover JSONB columns");

    let users_sparse = columns
        .iter()
        .find(|col| col.table == "users_sparse" && col.column == "metadata");

    assert!(
        users_sparse.is_some(),
        "Failed to discover users_sparse.metadata column"
    );

    test_db
        .cleanup()
        .await
        .expect("Failed to cleanup test database");
}

// ===== Sampler Integration Tests =====

#[tokio::test]
async fn test_sampler_auto_select_random_strategy() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    pgdrift_db::fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Small table (5000 rows) should use Random strategy
    // Request fewer samples than rows to trigger Random sampling
    let sampler = Sampler::new(&test_db.pool, "public", "users", Some(5000), 100)
        .await
        .expect("Failed to create sampler");

    let info = sampler.strategy_info();
    assert!(
        info.contains("Random sampling"),
        "Expected Random strategy, got: {}",
        info
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_sampler_random_sampling() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    pgdrift_db::fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Use Random strategy explicitly
    let sampler =
        Sampler::with_strategy(SamplingStrategy::Random { limit: 100 }).show_progress(false); // Disable progress bar for tests

    let samples = sampler
        .sample(&test_db.pool, "public", "users", "metadata")
        .await
        .expect("Failed to sample");

    // Should get samples (up to 100)
    assert!(!samples.is_empty(), "Expected samples but got none");
    assert!(samples.len() <= 100, "Got more samples than limit");

    // Each sample should be valid JSON
    for sample in &samples {
        assert!(sample.is_object(), "Expected JSON object in sample");
    }

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_sampler_with_type_inconsistency() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    pgdrift_db::fixtures::create_users_type_inconsistency(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    let sampler =
        Sampler::with_strategy(SamplingStrategy::Random { limit: 200 }).show_progress(false);

    let samples = sampler
        .sample(&test_db.pool, "public", "users_mixed_types", "metadata")
        .await
        .expect("Failed to sample");

    assert!(
        !samples.is_empty(),
        "Expected samples from mixed type table"
    );

    // Verify we got samples with the age field
    let has_age = samples.iter().any(|s| s.get("age").is_some());

    assert!(has_age, "Expected to find samples with 'age' field");

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_sampler_reservoir_strategy_selection() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    pgdrift_db::fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Medium table (500k rows estimated) should use Reservoir strategy
    let sampler = Sampler::new(&test_db.pool, "public", "users", Some(500_000), 10_000)
        .await
        .expect("Failed to create sampler");

    let info = sampler.strategy_info();
    // Should use Reservoir (if PK found) or Random (fallback)
    assert!(
        info.contains("Reservoir") || info.contains("Random"),
        "Expected Reservoir or Random strategy, got: {}",
        info
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_sampler_tablesample_strategy_selection() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    pgdrift_db::fixtures::create_users_consistent(&test_db.pool)
        .await
        .expect("Failed to create fixture");

    // Large table (20M rows estimated) should use TABLESAMPLE
    let sampler = Sampler::new(&test_db.pool, "public", "users", Some(20_000_000), 10_000)
        .await
        .expect("Failed to create sampler");

    let info = sampler.strategy_info();
    assert!(
        info.contains("TABLESAMPLE"),
        "Expected TABLESAMPLE strategy, got: {}",
        info
    );

    test_db.cleanup().await.expect("Failed to cleanup");
}

#[tokio::test]
async fn test_sampler_handles_null_values() {
    let test_db = TestDb::new().await.expect("Failed to create test database");

    // Create table with some NULL JSONB values
    sqlx::query(
        "CREATE TABLE test_nulls (
            id SERIAL PRIMARY KEY,
            data JSONB
        )",
    )
    .execute(&test_db.pool)
    .await
    .expect("Failed to create table");

    // Insert mix of NULL and valid JSON
    for i in 0..10 {
        if i % 3 == 0 {
            sqlx::query("INSERT INTO test_nulls (data) VALUES (NULL)")
                .execute(&test_db.pool)
                .await
                .expect("Failed to insert NULL");
        } else {
            sqlx::query("INSERT INTO test_nulls (data) VALUES ($1)")
                .bind(serde_json::json!({"value": i}))
                .execute(&test_db.pool)
                .await
                .expect("Failed to insert data");
        }
    }

    let sampler =
        Sampler::with_strategy(SamplingStrategy::Random { limit: 100 }).show_progress(false);

    let samples = sampler
        .sample(&test_db.pool, "public", "test_nulls", "data")
        .await
        .expect("Failed to sample");

    // Should only get non-NULL values (IS NOT NULL in query)
    assert!(!samples.is_empty(), "Expected non-null samples");
    assert!(samples.len() < 10, "Should filter out NULL values");

    test_db.cleanup().await.expect("Failed to cleanup");
}
