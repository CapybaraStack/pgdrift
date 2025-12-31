use serde_json::json;
use sqlx::PgPool;

/// Create a test users table with consistent JSONB schema
pub async fn create_users_consistent(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            metadata JSONB NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Insert 5000 rows with consistent schema
    for i in 0..5000 {
        let metadata = json!({
            "email": format!("user{}@example.com", i),
            "age": 25 + (i % 50),
            "country": "USA",
            "preferences": {
                "theme": "dark",
                "notifications": true,
                "language": "en"
            },
            "tags": ["active", "verified"],
            "created_at": "2025-01-01",
            "status": "active"
        });

        sqlx::query("INSERT INTO users (metadata) VALUES ($1)")
            .bind(metadata)
            .execute(pool)
            .await?;
    }

    Ok(())
}

/// Create users table with type inconsistency (age: string vs number)
pub async fn create_users_type_inconsistency(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS users_mixed_types (
            id SERIAL PRIMARY KEY,
            metadata JSONB NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Insert 5000 rows: 92% with age as string, 8% as number
    for i in 0..5000 {
        let metadata = if i % 100 < 8 {
            // 8% - age as number
            json!({
                "email": format!("user{}@example.com", i),
                "age": 25 + (i % 50),
                "country": "USA",
                "preferences": {
                    "theme": "dark",
                    "notifications": true
                }
            })
        } else {
            // 92% - age as string
            json!({
                "email": format!("user{}@example.com", i),
                "age": format!("{}", 25 + (i % 50)),
                "country": "USA",
                "preferences": {
                    "theme": "dark",
                    "notifications": true
                }
            })
        };

        sqlx::query("INSERT INTO users_mixed_types (metadata) VALUES ($1)")
            .bind(metadata)
            .execute(pool)
            .await?;
    }

    Ok(())
}

/// Create users table with ghost keys and sparse fields
pub async fn create_users_ghost_keys(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS users_sparse (
            id SERIAL PRIMARY KEY,
            metadata JSONB NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Insert 5000 rows with sparse fields
    for i in 0..5000 {
        let mut metadata = json!({
            "email": format!("user{}@example.com", i),
            "name": format!("User {}", i),
            "country": "USA"
        });

        // Ghost key: premium_feature - appears in <1% of rows (20 out of 5000 = 0.4%)
        if i < 20 {
            metadata["premium_feature"] = json!({
                "active": true,
                "tier": "pro",
                "expiry": "2025-12-31"
            });
        }

        // Missing key: billing_address - present in 95% of rows
        if i >= 250 {
            // First 250 rows don't have it
            metadata["billing_address"] = json!({
                "street": "123 Main St",
                "city": "San Francisco",
                "state": "CA"
            });
        }

        // Occasional nested array
        if i % 100 == 0 {
            metadata["addresses"] = json!([
                {"type": "home", "city": "SF"},
                {"type": "work", "city": "Oakland"}
            ]);
        }

        sqlx::query("INSERT INTO users_sparse (metadata) VALUES ($1)")
            .bind(metadata)
            .execute(pool)
            .await?;
    }

    Ok(())
}

/// Create users table with deeply nested schema
pub async fn create_users_nested(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS users_nested (
            id SERIAL PRIMARY KEY,
            metadata JSONB NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Insert 5000 rows with deep nesting
    for i in 0..5000 {
        let metadata = json!({
            "user": {
                "profile": {
                    "personal": {
                        "name": {
                            "first": "John",
                            "last": "Doe"
                        },
                        "contact": {
                            "email": format!("user{}@example.com", i),
                            "phone": "+1-555-0100"
                        }
                    },
                    "settings": {
                        "privacy": {
                            "profile_public": true,
                            "email_visible": false
                        }
                    }
                },
                "subscriptions": [
                    {"type": "email", "enabled": true},
                    {"type": "sms", "enabled": false}
                ]
            }
        });

        sqlx::query("INSERT INTO users_nested (metadata) VALUES ($1)")
            .bind(metadata)
            .execute(pool)
            .await?;
    }

    Ok(())
}

/// Create a simple products table for additional test scenarios
pub async fn create_products_schema_evolution(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS products (
            id SERIAL PRIMARY KEY,
            data JSONB NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await?;

    // Insert 5000 rows with schema evolution (old vs new format)
    for i in 0..5000 {
        let data = if i < 2500 {
            // Old schema format
            json!({
                "product_id": i,
                "name": format!("Product {}", i),
                "price": 99.99,
                "inventory": 100
            })
        } else {
            // New schema format (with additional fields)
            json!({
                "product_id": i,
                "name": format!("Product {}", i),
                "price": 99.99,
                "inventory": 100,
                "sku": format!("SKU-{}", i),
                "category": "electronics",
                "tags": ["new", "featured"],
                "metadata": {
                    "version": "2.0",
                    "updated_at": "2025-01-01"
                }
            })
        };

        sqlx::query("INSERT INTO products (data) VALUES ($1)")
            .bind(data)
            .execute(pool)
            .await?;
    }

    Ok(())
}

/// Clean up all test tables
pub async fn cleanup(pool: &PgPool) -> Result<(), sqlx::Error> {
    sqlx::query("DROP TABLE IF EXISTS users")
        .execute(pool)
        .await?;
    sqlx::query("DROP TABLE IF EXISTS users_mixed_types")
        .execute(pool)
        .await?;
    sqlx::query("DROP TABLE IF EXISTS users_sparse")
        .execute(pool)
        .await?;
    sqlx::query("DROP TABLE IF EXISTS users_nested")
        .execute(pool)
        .await?;
    sqlx::query("DROP TABLE IF EXISTS products")
        .execute(pool)
        .await?;
    Ok(())
}
