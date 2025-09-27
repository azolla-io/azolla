pub mod orchestrator;
pub mod proto;
pub mod shepherd;

#[cfg(any(test, feature = "test-harness"))]
pub mod test_harness;

// Event type constants
pub const EVENT_TASK_CREATED: i16 = 1;
pub const EVENT_TASK_STARTED: i16 = 2;
pub const EVENT_TASK_ENDED: i16 = 3;
pub const EVENT_TASK_ATTEMPT_STARTED: i16 = 4;
pub const EVENT_TASK_ATTEMPT_ENDED: i16 = 5;
pub const EVENT_SHEPHERD_REGISTERED: i16 = 6;

// Task status constants
pub const TASK_STATUS_CREATED: i16 = 0;
pub const TASK_STATUS_ATTEMPT_STARTED: i16 = 1;
pub const TASK_STATUS_ATTEMPT_SUCCEEDED: i16 = 2;
pub const TASK_STATUS_SUCCEEDED: i16 = 3;
pub const TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT: i16 = 4;
pub const TASK_STATUS_ATTEMPT_FAILED_WITHOUT_ATTEMPTS_LEFT: i16 = 5;
pub const TASK_STATUS_FAILED: i16 = 6;

// Task attempt status constants
pub const ATTEMPT_STATUS_STARTED: i16 = 0;
pub const ATTEMPT_STATUS_SUCCEEDED: i16 = 1;
pub const ATTEMPT_STATUS_FAILED: i16 = 2;

#[cfg(test)]
#[macro_export]
macro_rules! db_test {
    ($test_name:ident, $body:expr) => {
        #[tokio::test]
        async fn $test_name() {
            use testcontainers::{
                core::{IntoContainerPort, WaitFor},
                runners::AsyncRunner,
                GenericImage, ImageExt,
            };
            use $crate::orchestrator::db::{
                create_pool, run_migrations, Database, Server, Settings,
            };

            // 1. Start a fresh Postgres container using testcontainers
            let container = GenericImage::new("postgres", "16-alpine")
                .with_wait_for(WaitFor::message_on_stderr(
                    "database system is ready to accept connections",
                ))
                .with_exposed_port(5432u16.tcp())
                .with_env_var("POSTGRES_PASSWORD", "postgres")
                .with_env_var("POSTGRES_USER", "postgres")
                .with_env_var("POSTGRES_DB", "postgres")
                .start()
                .await
                .expect("Failed to start Postgres container");

            // Give the database a moment to fully initialize after the ready message
            tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

            let port = container.get_host_port_ipv4(5432).await.unwrap();

            // 2. Build a connection string for this instance
            let db_url = format!(
                "postgres://postgres:postgres@127.0.0.1:{}/postgres?sslmode=disable",
                port
            );

            // 3. Create your settings struct and pool
            let settings = Settings {
                database: Database {
                    url: db_url,
                    pool_size: 8,
                },
                server: Server { port: 0 }, // dummy
                event_stream: $crate::orchestrator::db::EventStream::default(),
                domains: $crate::orchestrator::db::DomainsConfig::default(),
                shutdown: $crate::orchestrator::db::ShutdownConfig::default(),
            };
            let pool = create_pool(&settings).unwrap();

            // 4. Run migrations
            run_migrations(&pool).await.unwrap();

            // 5. Run the test body, passing the pool with timeout
            let fut = $body(pool);
            tokio::time::timeout(tokio::time::Duration::from_secs(3), fut)
                .await
                .expect("Test timed out after 3 seconds");
            // 6. Container is dropped here, cleaning up DB
        }
    };
}
