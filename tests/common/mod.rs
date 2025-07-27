#[macro_export]
macro_rules! db_test {
    ($test_name:ident, $body:expr) => {
        #[tokio::test]
        async fn $test_name() {
            use azolla::orchestrator::db::{
                create_pool, run_migrations, Database, DomainsConfig, EventStream, Server,
                Settings, ShutdownConfig,
            };
            use testcontainers::{
                core::{IntoContainerPort, WaitFor},
                runners::AsyncRunner,
                GenericImage, ImageExt,
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
                event_stream: EventStream::default(),
                domains: DomainsConfig::default(),
                shutdown: ShutdownConfig::default(),
            };
            let pool = create_pool(&settings).unwrap();

            // 4. Run migrations
            run_migrations(&pool).await.unwrap();

            // 5. Run the test body, passing the pool
            let fut = $body(pool);
            fut.await;
            // 6. Container is dropped here, cleaning up DB
        }
    };
}
