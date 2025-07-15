use azolla::test_harness::{find_available_port, TaskTestData, TestHarness};

#[tokio::test]
async fn test_task_creation_and_shepherd_startup() {
    let _ = env_logger::try_init();

    let mut harness = TestHarness::new().await.unwrap();

    // Ensure worker binary is available
    harness.ensure_worker_binary().await.unwrap();

    let _shepherd = harness.start_shepherd().await.unwrap();

    // Create a simple successful task
    let request = TaskTestData::echo_task("hello world");
    let response = harness.client.create_task(request).await.unwrap();
    let task_id = response.into_inner().task_id;

    // Merge events to ensure task is persisted to database
    harness.engine.merge_events_to_db().await.unwrap();

    // Verify task was created in database
    let task_status = harness.get_task_status(&task_id).await.unwrap();
    assert!(task_status.is_some(), "Task should be created in database");

    // Verify shepherd is registered
    let shepherd_count = harness.get_shepherd_count().await.unwrap();
    assert_eq!(shepherd_count, 1, "Should have one registered shepherd");

    harness.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_shepherd_startup_and_registration() {
    let _ = env_logger::try_init();

    let mut harness = TestHarness::new().await.unwrap();

    // Test that shepherd starts correctly using the refactored code
    let shepherd_handle = harness.start_shepherd().await.unwrap();

    // Verify shepherd is configured correctly
    assert_eq!(shepherd_handle.config.max_concurrency, 4);
    assert!(shepherd_handle
        .config
        .worker_binary_path
        .ends_with("azolla-worker"));

    // Verify shepherd load tracking works
    let current_load = shepherd_handle.get_current_load();
    assert_eq!(current_load, 0, "Initial load should be 0");

    // Verify shepherd is registered with orchestrator
    let shepherd_count = harness.get_shepherd_count().await.unwrap();
    assert_eq!(shepherd_count, 1, "Should have one registered shepherd");

    harness.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_multiple_shepherds() {
    let _ = env_logger::try_init();

    let mut harness = TestHarness::new().await.unwrap();

    // Start first shepherd
    let _shepherd1 = harness.start_shepherd().await.unwrap();

    // Update config for second shepherd (different port)
    harness.shepherd_config.worker_grpc_port = find_available_port();

    // Start second shepherd
    let _shepherd2 = harness.start_shepherd().await.unwrap();

    // Verify both shepherds are registered
    let shepherd_count = harness.get_shepherd_count().await.unwrap();
    assert_eq!(shepherd_count, 2, "Should have two registered shepherds");

    harness.shutdown().await.unwrap();
}
