use pgdrift_core::analyzer::JsonAnalyzer;
use pgdrift_core::types::JsonType;
use pgdrift_db::fixtures;
use pgdrift_db::test_utils::TestDb;

#[tokio::test]
async fn test_analyzer_with_consistent_schema() {
    let db = TestDb::new().await.expect("Failed to create test database");

    // Create fixture with consistent schema
    fixtures::create_users_consistent(&db.pool)
        .await
        .expect("Failed to create fixture");

    // Query sample of JSONB data
    let rows: Vec<(serde_json::Value,)> = sqlx::query_as("SELECT metadata FROM users LIMIT 100")
        .fetch_all(&db.pool)
        .await
        .expect("Failed to query data");

    // Analyze with JsonAnalyzer
    let mut analyzer = JsonAnalyzer::new();
    for (metadata,) in rows {
        analyzer.analyze(&metadata);
    }

    let stats = analyzer.finalize();

    // Validate results
    assert!(stats.contains_key("email"));
    assert!(stats.contains_key("age"));
    assert!(stats.contains_key("preferences"));
    assert!(stats.contains_key("preferences.theme"));
    assert!(stats.contains_key("preferences.notifications"));
    assert!(stats.contains_key("tags"));

    // All fields should have 100% density (consistent schema)
    assert_eq!(stats["email"].density, 1.0);
    assert_eq!(stats["age"].density, 1.0);
    assert_eq!(stats["country"].density, 1.0);

    // Types should be consistent
    assert_eq!(stats["email"].types.len(), 1);
    assert_eq!(stats["email"].types.get(&JsonType::String), Some(&100));

    assert_eq!(stats["age"].types.len(), 1);
    assert_eq!(stats["age"].types.get(&JsonType::Number), Some(&100));
}

#[tokio::test]
async fn test_analyzer_detects_type_inconsistency() {
    let db = TestDb::new().await.expect("Failed to create test database");

    // Create fixture with type inconsistency
    fixtures::create_users_type_inconsistency(&db.pool)
        .await
        .expect("Failed to create fixture");

    // Query all data
    let rows: Vec<(serde_json::Value,)> = sqlx::query_as("SELECT metadata FROM users_mixed_types")
        .fetch_all(&db.pool)
        .await
        .expect("Failed to query data");

    let mut analyzer = JsonAnalyzer::new();
    for (metadata,) in rows {
        analyzer.analyze(&metadata);
    }

    let stats = analyzer.finalize();

    // Age field should have TWO types (string and number)
    let age_stats = &stats["age"];
    assert_eq!(age_stats.types.len(), 2);

    // 92% strings, 8% numbers (according to fixture)
    let string_count = age_stats.types.get(&JsonType::String).unwrap();
    let number_count = age_stats.types.get(&JsonType::Number).unwrap();

    let total = string_count + number_count;
    let string_pct = (*string_count as f64 / total as f64) * 100.0;
    let number_pct = (*number_count as f64 / total as f64) * 100.0;

    assert!((string_pct - 92.0).abs() < 2.0); // ~92%
    assert!((number_pct - 8.0).abs() < 2.0); // ~8%
}

#[tokio::test]
async fn test_analyzer_detects_ghost_keys() {
    let db = TestDb::new().await.expect("Failed to create test database");

    // Create fixture with ghost keys
    fixtures::create_users_ghost_keys(&db.pool)
        .await
        .expect("Failed to create fixture");

    let rows: Vec<(serde_json::Value,)> = sqlx::query_as("SELECT metadata FROM users_sparse")
        .fetch_all(&db.pool)
        .await
        .expect("Failed to query data");

    let mut analyzer = JsonAnalyzer::new();
    for (metadata,) in rows {
        analyzer.analyze(&metadata);
    }

    let stats = analyzer.finalize();

    // premium_feature should have very low density (<1%)
    // According to fixture: appears in first 20 out of 5000 = 0.4%
    let premium_stats = &stats["premium_feature"];
    assert!(premium_stats.density < 0.01); // Less than 1%

    // Common fields should have high density
    assert!(stats["email"].density > 0.99);
}

#[tokio::test]
async fn test_analyzer_handles_deep_nesting() {
    let db = TestDb::new().await.expect("Failed to create test database");

    // Create fixture with deep nesting
    fixtures::create_users_nested(&db.pool)
        .await
        .expect("Failed to create fixture");

    let rows: Vec<(serde_json::Value,)> = sqlx::query_as("SELECT metadata FROM users_nested")
        .fetch_all(&db.pool)
        .await
        .expect("Failed to query data");

    let mut analyzer = JsonAnalyzer::new();
    for (metadata,) in rows {
        analyzer.analyze(&metadata);
    }

    let stats = analyzer.finalize();

    // The fixture creates: user.profile.personal.name.first, etc.
    // Verify depth tracking through the nested structure
    assert_eq!(stats["user"].depth, 1);
    assert_eq!(stats["user.profile"].depth, 2);
    assert_eq!(stats["user.profile.personal"].depth, 3);
    assert_eq!(stats["user.profile.personal.name"].depth, 4);
    assert_eq!(stats["user.profile.personal.name.first"].depth, 5);
}
