//! Integration tests for basic task execution and lifecycle.
//!
//! This module tests the fundamental task execution flow from creation to completion,
//! including task creation, shepherd registration, worker execution, and result handling.
//! These tests verify the core end-to-end functionality of the orchestrator system.

#![cfg(feature = "test-harness")]

use azolla::test_harness::{IntegrationTestEnvironment, TaskTestData};

/// Tests the complete end-to-end task execution flow.
///
/// **Purpose:** Verifies that tasks can be created, dispatched to workers, executed,
/// and results properly reported back through the orchestrator system.
///
/// **Flow:**
/// 1. Creates orchestrator and shepherd
/// 2. Creates a simple "echo" task
/// 3. Waits for task completion
/// 4. Verifies task status transitions and attempt tracking
///
/// **Expected Behavior:**
/// - Task should transition: CREATED → ATTEMPT_STARTED → SUCCEEDED
/// - Task attempts should be properly recorded with timing
/// - Final status should be TASK_STATUS_SUCCEEDED
#[tokio::test]
async fn test_task_creation_execution_and_result_handling() {
    let _ = env_logger::try_init();

    let mut harness = IntegrationTestEnvironment::new().await.unwrap();

    // Ensure worker binary is available
    harness.ensure_worker_binary().await.unwrap();

    let shepherd_uuid = harness.shepherd_config.uuid;
    let _shepherd = harness.start_shepherd().await.unwrap();

    // Wait for shepherd to be registered with orchestrator (proper synchronization)
    let registered = harness
        .wait_for_shepherd_registration(shepherd_uuid, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    assert!(registered, "Shepherd should register within 5 seconds");

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

    harness.shutdown().await.unwrap();
}
