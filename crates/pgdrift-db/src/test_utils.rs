use sqlx::PgPool;
use testcontainers::{
    GenericImage, ImageExt,
    core::{ContainerAsync, WaitFor},
    runners::AsyncRunner,
};

/// Test database container and pool wrapper
pub struct TestDb {
    pub pool: PgPool,
    database_url: String,
    _container: ContainerAsync<GenericImage>,
}

impl TestDb {
    /// Start a new postgres container and connection pool
    pub async fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let image = GenericImage::new("postgres", "16")
            .with_wait_for(WaitFor::message_on_stderr(
                "database system is ready to accept connections",
            ))
            .with_env_var("POSTGRES_USER", "pgdrift")
            .with_env_var("POSTGRES_PASSWORD", "pgdrift_test")
            .with_env_var("POSTGRES_DB", "pgdrift_test");

        let container = image.start().await?;
        let port = container.get_host_port_ipv4(5432).await?;

        let database_url = format!(
            "postgres://pgdrift:pgdrift_test@127.0.0.1:{}/pgdrift_test",
            port
        );

        // Wait for the connection
        let mut attempts = 0;
        let pool = loop {
            match PgPool::connect(&database_url).await {
                Ok(p) => break p,
                Err(_) if attempts < 30 => {
                    attempts += 1;
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                }
                Err(e) => return Err(Box::new(e)),
            }
        };

        Ok(TestDb {
            pool,
            database_url,
            _container: container,
        })
    }

    /// Get the database URL for this test database
    pub fn database_url(&self) -> &str {
        &self.database_url
    }

    /// Cleanup all test tables
    pub async fn cleanup(&self) -> Result<(), sqlx::Error> {
        crate::fixtures::cleanup(&self.pool).await
    }
}
