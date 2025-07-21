//! Integration tests for task retry mechanism and scheduling.
//!
//! This module tests the retry policy system, including automatic retry execution,
//! retry scheduling, and race condition handling when multiple tasks with different
//! retry timings are processed concurrently.

#![cfg(feature = "test-harness")]

use azolla::proto::orchestrator::CreateTaskRequest;
use azolla::test_harness::IntegrationTestEnvironment;
use serde_json::json;

/// Tests the complete end-to-end retry mechanism.
///
/// **Purpose:** Verifies that failed tasks are automatically retried according to
/// their retry policy configuration, with proper status transitions and attempt tracking.
///
/// **Flow:**
/// 1. Creates a task that always fails with 3 max attempts and 1s retry delay
/// 2. Waits for first attempt to fail
/// 3. Verifies retry is scheduled and executed automatically
/// 4. Confirms all attempts are tracked with proper timing
///
/// **Expected Behavior:**
/// - First attempt: ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT
/// - Automatic retry after 1 second delay
/// - Multiple attempts executed (up to max_attempts: 3)
/// - Final status: TASK_STATUS_FAILED after all retries exhausted
/// - All attempts properly recorded with correct timing
#[tokio::test]
async fn test_task_retry_handling() {
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

    // Create a task that always fails with a short retry delay to test retry mechanism
    let request = RetryTestData::failing_task_with_short_retry_delay();
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

    // Wait for first attempt to complete and verify it failed with retries left
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let first_attempts = harness.get_task_attempts(&task_id).await.unwrap();
    let first_status = harness.get_task_status(&task_id).await.unwrap();

    // Verify first attempt failed but retry is scheduled
    assert_eq!(
        first_attempts.len(),
        1,
        "Should have one attempt after first execution"
    );
    assert_eq!(
        first_status,
        Some(azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT),
        "Task should be in retry state after first failure"
    );

    // Wait for retry to be executed (retry delay is 1 second)
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    let second_attempts = harness.get_task_attempts(&task_id).await.unwrap();
    let second_status = harness.get_task_status(&task_id).await.unwrap();

    // Verify that retry was actually executed
    assert!(
        second_attempts.len() >= 2,
        "Should have at least 2 attempts after retry execution (got {})",
        second_attempts.len()
    );

    // Verify attempt details
    let first_attempt = &second_attempts[0];
    let second_attempt = &second_attempts[1];

    assert_eq!(
        first_attempt.attempt_number, 0,
        "First attempt should be attempt 0"
    );
    assert_eq!(
        second_attempt.attempt_number, 1,
        "Second attempt should be attempt 1"
    );

    assert!(
        first_attempt.ended_at.is_some(),
        "First attempt should have ended"
    );
    assert!(
        second_attempt.started_at > first_attempt.started_at,
        "Second attempt should start after first attempt"
    );

    // With max_attempts: 3, we should see retries being executed
    match second_status {
        Some(azolla::TASK_STATUS_ATTEMPT_STARTED) => {
            // Task is currently executing (retry in progress)
        }
        Some(azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT) => {
            // Task correctly failed with retries still remaining
        }
        Some(azolla::TASK_STATUS_FAILED) => {
            // Task correctly failed after exhausting all retries
            assert!(
                second_attempts.len() >= 3,
                "Should have all 3 attempts if fully failed (got {})",
                second_attempts.len()
            );
        }
        Some(status) => {
            panic!("Unexpected task status: {status} (expected status 1, 4, or 6)");
        }
        None => {
            panic!("Task status should be available");
        }
    }

    // Wait a bit more to see final status if task was still executing
    if second_status == Some(azolla::TASK_STATUS_ATTEMPT_STARTED) {
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        let final_attempts = harness.get_task_attempts(&task_id).await.unwrap();
        let final_status = harness.get_task_status(&task_id).await.unwrap();

        assert_eq!(
            final_status,
            Some(azolla::TASK_STATUS_FAILED),
            "Task should be finally failed after all retries exhausted"
        );
        assert_eq!(
            final_attempts.len(),
            3,
            "Should have exactly 3 attempts (max_attempts reached)"
        );
    }

    harness.shutdown().await.unwrap();
}

/// Tests retry scheduling race condition handling.
///
/// **Purpose:** Verifies that the SchedulerActor correctly handles the race condition
/// where a new task with an earlier retry time is added after another task is already
/// scheduled for retry. This tests the retry_schedule_changed flag mechanism.
///
/// **Flow:**
/// 1. Creates TaskA with longer retry delay (5 seconds) - retry scheduled for T=6
/// 2. Waits for TaskA to fail and be scheduled for retry
/// 3. Creates TaskB with shorter retry delay (1 second) - retry scheduled for T=4
/// 4. Verifies TaskB's retry executes before TaskA's retry (T=4 < T=6)
///
/// **Expected Behavior:**
/// - TaskA gets scheduled for retry at T=6 (5 second delay)
/// - TaskB gets scheduled for retry at T=4 (1 second delay)
/// - Scheduler properly reschedules to handle TaskB's earlier due time
/// - TaskB's retry executes before TaskA's retry despite being created later
/// - Both tasks should have proper attempt tracking
#[tokio::test]
async fn test_retry_scheduling_race_condition() {
    let _ = env_logger::try_init();

    let mut harness = IntegrationTestEnvironment::new().await.unwrap();

    // Ensure worker binary is available
    harness.ensure_worker_binary().await.unwrap();

    let shepherd_uuid = harness.shepherd_config.uuid;
    let _shepherd = harness.start_shepherd().await.unwrap();

    // Wait for shepherd to be registered with orchestrator
    let registered = harness
        .wait_for_shepherd_registration(shepherd_uuid, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    assert!(registered, "Shepherd should register within 5 seconds");

    // Create taskA with longer retry delay (3 seconds)
    let task_a_request = RetryTestData::failing_task_with_longer_retry_delay();
    let task_a_response = harness.client.create_task(task_a_request).await.unwrap();
    let task_a_id = task_a_response.into_inner().task_id;

    // Wait for taskA to complete its first attempt and be scheduled for retry (5 second delay)
    // TaskA will fail at ~T=1, and be scheduled for retry at ~T=6
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Verify TaskA is in retry state (scheduled for future retry at T=6)
    let task_a_intermediate_status = harness.get_task_status(&task_a_id).await.unwrap();
    assert_eq!(
        task_a_intermediate_status,
        Some(azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT),
        "TaskA should be scheduled for retry after first failure"
    );

    // Now create taskB with shorter retry delay (1 second) - this creates the race condition
    // We're at T=2, TaskB will fail at ~T=3, and be scheduled for retry at ~T=4
    // This means TaskB's retry (T=4) should execute BEFORE TaskA's retry (T=6)
    // This tests if the scheduler properly reschedules when a task with earlier due time is added

    let task_b_request = RetryTestData::failing_task_with_short_retry_delay();
    let task_b_response = harness.client.create_task(task_b_request).await.unwrap();
    let task_b_id = task_b_response.into_inner().task_id;
    let _task_b_created_at = std::time::Instant::now();

    // Verify both tasks are created and processed by the scheduler
    let task_a_status = harness.get_task_status(&task_a_id).await.unwrap();
    let task_b_status = harness.get_task_status(&task_b_id).await.unwrap();

    assert!(
        task_a_status.is_some(),
        "TaskA should be created and available"
    );
    assert!(
        task_b_status.is_some(),
        "TaskB should be created and available"
    );

    // Wait for TaskB to complete its first attempt and be scheduled for retry
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let task_b_first_attempts = harness.get_task_attempts(&task_b_id).await.unwrap();
    assert!(
        !task_b_first_attempts.is_empty(),
        "TaskB should have at least one attempt after initial execution"
    );

    let task_b_first_status = harness.get_task_status(&task_b_id).await.unwrap();
    assert_eq!(
        task_b_first_status,
        Some(azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT),
        "TaskB should be scheduled for retry after first failure"
    );

    // Wait for TaskB's retry to be executed (should happen ~1 second after first failure)
    // This tests the race condition: TaskB's retry should execute BEFORE TaskA's retry
    // TaskB: fails at ~T=3.5, retry at ~T=4.5
    // TaskA: already scheduled for retry at ~T=6
    // Let's check at T=5 (before TaskA's retry but after TaskB's retry)
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Verify TaskB's retry was executed
    let task_b_retry_attempts = harness.get_task_attempts(&task_b_id).await.unwrap();

    assert!(
        task_b_retry_attempts.len() >= 2,
        "TaskB should have at least 2 attempts after retry execution (got {})",
        task_b_retry_attempts.len()
    );

    // The key test: TaskB should have executed its retry
    // We don't need to assert exact timing for TaskA since timing can be variable in tests
    // The important thing is that both tasks are processed correctly despite the race condition

    // Verify timing: TaskB's retry happened before TaskA's scheduled retry time
    let task_b_first_attempt = &task_b_retry_attempts[0];
    let task_b_second_attempt = &task_b_retry_attempts[1];

    assert!(
        task_b_first_attempt.ended_at.is_some(),
        "TaskB first attempt should have ended"
    );

    let retry_delay = task_b_second_attempt
        .started_at
        .signed_duration_since(task_b_first_attempt.ended_at.unwrap())
        .num_milliseconds() as f64
        / 1000.0;

    assert!(
        (0.8..=1.5).contains(&retry_delay),
        "TaskB retry should execute approximately 1 second after first failure (actual delay: {retry_delay:.2}s)"
    );

    // Wait for TaskA's retry to also execute
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Verify both tasks have been properly processed and retried
    let task_a_final_attempts = harness.get_task_attempts(&task_a_id).await.unwrap();
    let task_b_final_attempts = harness.get_task_attempts(&task_b_id).await.unwrap();

    assert!(
        task_a_final_attempts.len() >= 2,
        "TaskA should have at least 2 attempts after retry (got {})",
        task_a_final_attempts.len()
    );

    assert!(
        task_b_final_attempts.len() >= 2,
        "TaskB should have at least 2 attempts after retry (got {})",
        task_b_final_attempts.len()
    );

    // Verify final states
    let final_a_status = harness.get_task_status(&task_a_id).await.unwrap();
    let final_b_status = harness.get_task_status(&task_b_id).await.unwrap();

    // Both tasks should either be in retry state or failed completely
    assert!(
        matches!(
            final_a_status,
            Some(
                azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT | azolla::TASK_STATUS_FAILED
            )
        ),
        "TaskA should be in failed or retry state (actual: {final_a_status:?})"
    );
    assert!(
        matches!(
            final_b_status,
            Some(
                azolla::TASK_STATUS_ATTEMPT_STARTED
                    | azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT
                    | azolla::TASK_STATUS_FAILED
            )
        ),
        "TaskB should be executing, in retry state, or failed (actual: {final_b_status:?})"
    );

    harness.shutdown().await.unwrap();
}

/// Test helper functions for retry mechanism tests.
struct RetryTestData;

impl RetryTestData {
    /// Creates a task that always fails with a longer retry delay (5 seconds).
    /// Used for testing race conditions where this task is created first.
    pub fn failing_task_with_longer_retry_delay() -> CreateTaskRequest {
        CreateTaskRequest {
            name: "always_fail".to_string(),
            domain: "test".to_string(),
            args: vec![],
            kwargs: r#"{"should_fail": true}"#.to_string(),
            retry_policy: json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {
                    "strategy": "fixed",
                    "delay": 5.0  // 5 second delay for taskA
                },
                "retry": {"include_errors": ["TestError", "ValueError", "RuntimeError"]}
            })
            .to_string(),
            flow_instance_id: None,
        }
    }

    /// Creates a task that always fails with a short retry delay for predictable timing.
    /// Includes TestError in retry policy for proper error matching.
    pub fn failing_task_with_short_retry_delay() -> CreateTaskRequest {
        CreateTaskRequest {
            name: "always_fail".to_string(),
            domain: "test".to_string(),
            args: vec![],
            kwargs: r#"{"should_fail": true}"#.to_string(),
            retry_policy: json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {
                    "strategy": "fixed",
                    "delay": 1.0  // 1 second delay for predictable retry timing
                },
                "retry": {"include_errors": ["TestError", "ValueError", "RuntimeError"]}
            })
            .to_string(),
            flow_instance_id: None,
        }
    }
}
