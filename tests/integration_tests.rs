#![cfg(feature = "test-harness")]

use azolla::test_harness::{find_available_port, IntegrationTestEnvironment, TaskTestData};

#[tokio::test]
async fn test_task_creation_execution_and_result_handling() {
    let _ = env_logger::try_init();

    let mut harness = IntegrationTestEnvironment::new().await.unwrap();

    // Ensure worker binary is available
    harness.ensure_worker_binary().await.unwrap();

    let _shepherd = harness.start_shepherd().await.unwrap();

    // Wait a bit for shepherd to fully register and be available for task dispatch
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Create a simple successful task
    let request = TaskTestData::echo_task("hello world");
    let response = harness.client.create_task(request).await.unwrap();
    let task_id = response.into_inner().task_id;

    // Verify task was created and processed by scheduler
    let task_status = harness.get_task_status(&task_id).await.unwrap();
    assert!(
        task_status.is_some(),
        "Task should be created and available"
    );

    // Verify shepherd is registered
    let shepherd_count = harness.get_shepherd_count().await.unwrap();
    assert_eq!(shepherd_count, 1, "Should have one registered shepherd");

    // Wait for task to complete - TaskSet updates are synchronous so we should see status changes
    let task_completed = harness
        .wait_for_task_completion(&task_id, std::time::Duration::from_secs(10))
        .await
        .unwrap();

    assert!(
        task_completed,
        "Task should complete successfully within timeout"
    );

    println!("✓ Task completed successfully - full end-to-end flow working!");

    // Verify task attempts were recorded in TaskSet
    let attempts = harness.get_task_attempts(&task_id).await.unwrap();
    assert!(
        !attempts.is_empty(),
        "Task should have at least one attempt recorded"
    );

    let attempt = &attempts[0];
    assert_eq!(
        attempt.attempt_number, 0,
        "First attempt should be attempt 0 (0-indexed)"
    );
    assert!(
        attempt.started_at <= chrono::Utc::now(),
        "Attempt should have started"
    );

    // Verify final task status is SUCCEEDED
    let final_status = harness.get_task_status(&task_id).await.unwrap();
    assert_eq!(
        final_status,
        Some(azolla::TASK_STATUS_SUCCEEDED),
        "Task should be in SUCCEEDED state"
    );

    // Verify the core integration is working (task creation, shepherd registration, and result flow setup)
    println!(
        "✓ Core integration test completed - task creation and shepherd registration verified"
    );

    harness.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_task_retry_handling() {
    let _ = env_logger::try_init();

    let mut harness = IntegrationTestEnvironment::new().await.unwrap();

    // Ensure worker binary is available
    harness.ensure_worker_binary().await.unwrap();

    let _shepherd = harness.start_shepherd().await.unwrap();

    // Wait a bit for shepherd to fully register and be available for task dispatch
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    // Create a task that always fails to test retry mechanism
    let request = TaskTestData::failing_task_with_retries();
    let response = harness.client.create_task(request).await.unwrap();
    let task_id = response.into_inner().task_id;

    // Verify task was created
    let task_status = harness.get_task_status(&task_id).await.unwrap();
    assert!(
        task_status.is_some(),
        "Task should be created and available"
    );

    // Verify shepherd is registered
    let shepherd_count = harness.get_shepherd_count().await.unwrap();
    assert_eq!(shepherd_count, 1, "Should have one registered shepherd");

    // Give the task some time to attempt execution
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Check the task status and attempts - this tests that failure handling works
    let attempts = harness.get_task_attempts(&task_id).await.unwrap();
    let current_status = harness.get_task_status(&task_id).await.unwrap();

    println!("Task status: {current_status:?}");
    println!("Total attempts: {}", attempts.len());

    // Verify that the task failed as expected
    assert!(
        !attempts.is_empty(),
        "Task should have at least one attempt"
    );
    assert_eq!(attempts.len(), 1, "Task should have one attempt recorded");

    // Verify the attempt details
    let attempt = &attempts[0];
    assert_eq!(
        attempt.attempt_number, 0,
        "First attempt should be attempt 0"
    );
    assert!(
        attempt.started_at <= chrono::Utc::now(),
        "Attempt should have started"
    );
    assert!(
        attempt.ended_at.is_some(),
        "Failed attempt should have ended"
    );

    // Verify task is in failed state (either with retries left or final failure)
    // Status 4 = TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT
    // Status 6 = TASK_STATUS_FAILED
    match current_status {
        Some(4) => {
            println!("✓ Task correctly failed with attempts remaining (retry policy working)");
        }
        Some(6) => {
            println!("✓ Task correctly failed after exhausting retries");
        }
        Some(status) => {
            panic!("Unexpected task status: {status} (expected failed status 4 or 6)");
        }
        None => {
            panic!("Task status should be available");
        }
    }

    println!("✓ Failure handling verified - task failed as expected");
    println!("✓ Retry policy configuration properly processed");

    // This test verifies current behavior:
    // 1. Tasks with retry policies can be created and executed
    // 2. Tasks can properly fail and be marked with appropriate failure status
    // 3. Attempt tracking works correctly for failed tasks
    // 4. The retry policy framework is properly integrated
    // 5. Tasks are correctly marked as ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT when retries are available
    //
    // TODO: Implement automatic retry triggering in scheduler to test end-to-end retry flow

    harness.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_shepherd_startup_and_registration() {
    let _ = env_logger::try_init();

    let mut harness = IntegrationTestEnvironment::new().await.unwrap();

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

    let mut harness = IntegrationTestEnvironment::new().await.unwrap();

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
