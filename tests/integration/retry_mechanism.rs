//! Integration tests for task retry mechanism and scheduling.
//!
//! This module tests the retry policy system, including automatic retry execution,
//! retry scheduling, and race condition handling when multiple tasks with different
//! retry timings are processed concurrently.

#![cfg(feature = "test-harness")]

use azolla::orchestrator::retry_policy::RetryPolicy as InternalRetryPolicy;
use azolla::proto::common::RetryPolicy as ProtoRetryPolicy;
use azolla::proto::orchestrator::CreateTaskRequest;
use azolla::test_harness::IntegrationTestEnvironment;
use serde_json::json;

use crate::integration::poll_until;

fn build_retry_policy(json: serde_json::Value) -> Option<ProtoRetryPolicy> {
    let policy = InternalRetryPolicy::from_json(&json).expect("invalid retry policy json");
    Some(policy.to_proto())
}

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
    let poll_step = std::time::Duration::from_millis(200);
    let (first_attempts, first_status) =
        poll_until(std::time::Duration::from_secs(2), poll_step, || async {
            let attempts = harness.get_task_attempts(&task_id).await.unwrap();
            let status = harness.get_task_status(&task_id).await.unwrap();
            if !attempts.is_empty()
                && status == Some(azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT)
            {
                Some((attempts, status))
            } else {
                None
            }
        })
        .await
        .expect("Task should enter retry state after the first attempt");

    assert!(
        !first_attempts.is_empty(),
        "Should observe at least one attempt after first execution"
    );
    assert_eq!(
        first_status,
        Some(azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT),
        "Task should be in retry state after first failure"
    );

    let (second_attempts, second_status) =
        poll_until(std::time::Duration::from_secs(4), poll_step, || async {
            let attempts = harness.get_task_attempts(&task_id).await.unwrap();
            let status = harness.get_task_status(&task_id).await.unwrap();
            if attempts.len() >= 2 {
                Some((attempts, status))
            } else {
                None
            }
        })
        .await
        .expect("Task should execute a retry attempt within the expected delay");

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
        let (final_attempts, final_status) =
            poll_until(std::time::Duration::from_secs(3), poll_step, || async {
                let attempts = harness.get_task_attempts(&task_id).await.unwrap();
                let status = harness.get_task_status(&task_id).await.unwrap();
                if status == Some(azolla::TASK_STATUS_FAILED) {
                    Some((attempts, status))
                } else {
                    None
                }
            })
            .await
            .expect("Task should reach FAILED status after exhausting retries");

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

    // Verify final task result is properly stored after all retries exhausted
    let wait_result = {
        let mut elapsed = std::time::Duration::ZERO;
        let wait_timeout = std::time::Duration::from_secs(3);
        loop {
            let wait_request =
                tonic::Request::new(azolla::proto::orchestrator::WaitForTaskRequest {
                    task_id: task_id.clone(),
                    domain: harness.shepherd_config.domain.clone(),
                    timeout_ms: Some(1000),
                });

            let wait_response = harness.client.wait_for_task(wait_request).await.unwrap();
            let wait_result = wait_response.into_inner();
            if wait_result.status_code
                == azolla::proto::orchestrator::WaitForTaskStatus::Completed as i32
            {
                break wait_result;
            }

            if elapsed >= wait_timeout {
                panic!("WaitForTask did not report completion within the expected window");
            }

            tokio::time::sleep(poll_step).await;
            elapsed += poll_step;
        }
    };

    // Verify the response indicates completion with error result
    assert_eq!(
        wait_result.status_code,
        azolla::proto::orchestrator::WaitForTaskStatus::Completed as i32,
        "Failed task should be completed with stored error result"
    );

    // Verify the error result contains proper error information
    match &wait_result.result_type {
        Some(azolla::proto::orchestrator::wait_for_task_response::ResultType::Error(error)) => {
            assert_eq!(error.r#type, "TestError", "Error type should be TestError");
            assert_eq!(
                error.message, "This task always fails",
                "Error message should match expected failure message"
            );
            assert!(
                error.retriable,
                "Error should be marked as retriable since it's a TestError"
            );
        }
        Some(azolla::proto::orchestrator::wait_for_task_response::ResultType::Success(_)) => {
            panic!("Expected error result for failed task, got success");
        }
        None => {
            panic!("Expected result_type to be present for failed task with stored result");
        }
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
/// **Retry Policy Analysis:**
/// - **TaskA**: 5-second delay, max_attempts=3
/// - **TaskB**: 1-second delay, max_attempts=3
///
/// **TaskA Timeline (5s retry delay):**
/// - T=0-1: First attempt fails, retry scheduled for T=6 (1+5s)
/// - T=6-7: Second attempt fails, retry scheduled for T=12 (7+5s)
/// - T=12-13: Third attempt fails, status becomes FAILED (max attempts reached)
///
/// **TaskB Timeline (1s retry delay):**
/// - T=2-3: First attempt fails, retry scheduled for T=4 (3+1s)
/// - T=4-5: Second attempt fails, retry scheduled for T=6 (5+1s)
/// - T=6-7: Third attempt fails, status becomes FAILED (max attempts reached)
///
/// **Test Verification Points:**
/// - T=8: Race condition check (TaskB=FAILED, TaskA=still has attempts left)
/// - T=15: Final status check (both TaskA=FAILED and TaskB=FAILED)
///
/// **Expected Behavior:**
/// - Scheduler properly reschedules to handle TaskB's earlier due time
/// - TaskB's retry executes before TaskA's retry despite being created later  
/// - Both tasks should have proper attempt tracking
/// - TaskA's retry should execute properly (may still be running due to timing variance)
#[tokio::test]
async fn test_retry_scheduling_race_condition() {
    let _ = env_logger::try_init();

    // === TEST SETUP PHASE ===
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

    // === T=0: CREATE TASK A ===
    // TaskA configured with 5-second retry delay, will execute immediately
    let task_a_request = RetryTestData::failing_task_with_longer_retry_delay();
    let task_a_response = harness.client.create_task(task_a_request).await.unwrap();
    let task_a_id = task_a_response.into_inner().task_id;

    // === T=0-2: WAIT FOR TASK A FIRST ATTEMPT ===
    // Expected: TaskA executes at ~T=0-1, fails, schedules retry for T=6 (1+5 seconds)
    let wait_step = std::time::Duration::from_millis(200);
    let wait_limit = std::time::Duration::from_secs(2);
    let task_a_intermediate_status = poll_until(wait_limit, wait_step, || async {
        let status = harness.get_task_status(&task_a_id).await.unwrap();
        if status == Some(azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT) {
            Some(status)
        } else {
            None
        }
    })
    .await
    .expect("TaskA should enter retry state within the expected window");

    // === T=2: VERIFY TASK A IS IN RETRY STATE ===
    // TaskA should have failed and be scheduled for retry at T=6
    assert_eq!(
        task_a_intermediate_status,
        Some(azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT),
        "TaskA should be scheduled for retry after first failure"
    );

    // === T=2: CREATE TASK B (RACE CONDITION SETUP) ===
    // TaskB has 1-second retry delay vs TaskA's 5-second delay
    // Expected: TaskB will fail at ~T=2-3, retry at ~T=4-5, retry at ~T=6-7 (overlapping with TaskA's retry)
    // This tests if scheduler properly handles task with earlier due time
    let task_b_request = RetryTestData::failing_task_with_short_retry_delay();
    let task_b_response = harness.client.create_task(task_b_request).await.unwrap();
    let task_b_id = task_b_response.into_inner().task_id;
    let _task_b_created_at = std::time::Instant::now();

    // === T=2: VERIFY BOTH TASKS EXIST ===
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

    // === T=2-4: WAIT FOR TASK B FIRST ATTEMPT ===
    // Expected: TaskB executes at ~T=2-3, fails, schedules retry for T=4 (3+1 seconds)
    let (task_b_first_attempts, task_b_first_status) =
        poll_until(wait_limit, wait_step, || async {
            let attempts = harness.get_task_attempts(&task_b_id).await.unwrap();
            let status = harness.get_task_status(&task_b_id).await.unwrap();
            if !attempts.is_empty()
                && status == Some(azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT)
            {
                Some((attempts, status))
            } else {
                None
            }
        })
        .await
        .expect("TaskB should enter retry state within the expected window");

    // === T=4: VERIFY TASK B IS IN RETRY STATE ===
    assert!(
        !task_b_first_attempts.is_empty(),
        "TaskB should have at least one attempt after initial execution"
    );

    assert_eq!(
        task_b_first_status,
        Some(azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT),
        "TaskB should be scheduled for retry after first failure"
    );

    // === T=4-6: WAIT FOR TASK B RETRY EXECUTION ===
    // Expected: TaskB retry executes at ~T=4-5 (1 second after T=3 failure)
    // This is the KEY RACE CONDITION TEST: TaskB retry (T=4-5) overlaps with TaskA retry (T=6)
    let retry_wait_limit = std::time::Duration::from_secs(4);
    let task_b_retry_attempts = poll_until(retry_wait_limit, wait_step, || async {
        let attempts = harness.get_task_attempts(&task_b_id).await.unwrap();
        if attempts.len() >= 2 {
            Some(attempts)
        } else {
            None
        }
    })
    .await
    .expect("TaskB should execute a retry attempt within the expected window");

    // === T=6: VERIFY TASK B RETRY EXECUTED (RACE CONDITION SUCCESS) ===
    assert!(
        task_b_retry_attempts.len() >= 2,
        "TaskB should have at least 2 attempts after retry execution (got {})",
        task_b_retry_attempts.len()
    );

    // === VERIFY RACE CONDITION: TaskB retry timing ===
    // TaskB's retry should have executed ~1 second after first failure
    // This proves scheduler properly reordered retry queue when TaskB (earlier due time) was added
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

    // === T=6-8: WAIT FOR TASK B COMPLETION AND TASK A RETRY ===
    // Expected: TaskA retry executes at ~T=6-7 (5 seconds after T=1 failure)
    // TaskB should complete all 3 attempts by T=7-8 due to 1s retry delays + execution time
    // Wait to ensure TaskB's final attempt completes and TaskA's retry starts
    let overlap_wait_limit = std::time::Duration::from_secs(5);
    let (task_a_final_attempts, task_b_final_attempts) =
        poll_until(overlap_wait_limit, wait_step, || async {
            let attempts_a = harness.get_task_attempts(&task_a_id).await.unwrap();
            let attempts_b = harness.get_task_attempts(&task_b_id).await.unwrap();
            if attempts_a.len() >= 2 && attempts_b.len() >= 2 {
                Some((attempts_a, attempts_b))
            } else {
                None
            }
        })
        .await
        .expect("Both tasks should record at least two attempts within the expected window");

    // === T=8: VERIFY BOTH TASKS HAVE RETRY ATTEMPTS ===
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

    // === T=8: VERIFY FINAL TASK STATES (RACE CONDITION VERIFICATION) ===
    let checkpoint_a_status = harness.get_task_status(&task_a_id).await.unwrap();
    let checkpoint_b_status = harness.get_task_status(&task_b_id).await.unwrap();

    // DEBUG: Log current task states and attempt counts for investigation
    log::info!(
        "DEBUG T=8: TaskA status: {checkpoint_a_status:?}, attempts: {}",
        task_a_final_attempts.len()
    );
    log::info!(
        "DEBUG T=8: TaskB status: {checkpoint_b_status:?}, attempts: {}",
        task_b_final_attempts.len()
    );

    // DEBUG: Log attempt details for TaskB to understand what's happening
    for (i, attempt) in task_b_final_attempts.iter().enumerate() {
        log::info!(
            "DEBUG TaskB attempt {}: started={:?}, ended={:?}, status={:?}, error={:?}",
            i,
            attempt.started_at,
            attempt.ended_at,
            attempt.status,
            attempt.error_message
        );
    }

    // === CRITICAL RACE CONDITION ASSERTION ===
    // TaskB should be FAILED by T=8 because:
    // - TaskB: 1s retry delays → completes 3 attempts by T=7-8 (T=2-3, T=4-5, T=6-7)
    // - TaskA: 5s retry delay → should have completed 2nd attempt by T=7, but still have 1 attempt left

    // Wait for TaskB to reach FAILED status (allow for database update timing)
    let task_b_status_check = poll_until(std::time::Duration::from_secs(2), wait_step, || async {
        let status = harness.get_task_status(&task_b_id).await.unwrap();
        if status == Some(azolla::TASK_STATUS_FAILED) {
            Some(status)
        } else {
            None
        }
    })
    .await
    .expect("TaskB should reach FAILED state by the race-condition checkpoint");

    assert_eq!(
        task_b_status_check,
        Some(azolla::TASK_STATUS_FAILED),
        "TaskB should be FAILED (max attempts reached) due to faster 1s retry cycle. \
         This verifies the race condition works: TaskB retried faster than TaskA."
    );

    // TaskA should still have attempts left (completed fewer retries due to 5s delays)
    // Allow ATTEMPT_STARTED in case TaskA's retry is still executing at T=9
    assert!(
        matches!(
            checkpoint_a_status,
            Some(azolla::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT | azolla::TASK_STATUS_ATTEMPT_STARTED)
        ),
        "TaskA should have attempts left or be executing (slower 5s retry cycle) by T=8. (actual: {checkpoint_a_status:?})"
    );

    // === T=8-T=15: WAIT FOR TASK A FINAL ATTEMPT ===
    // TaskA's 3rd attempt: starts at T=12-13, needs time to complete execution
    // Wait up to 8 additional seconds, polling so we can finish early when both tasks fail
    let (final_a_status, final_b_status) =
        poll_until(std::time::Duration::from_secs(8), wait_step, || async {
            let status_a = harness.get_task_status(&task_a_id).await.unwrap();
            let status_b = harness.get_task_status(&task_b_id).await.unwrap();
            if status_a == Some(azolla::TASK_STATUS_FAILED)
                && status_b == Some(azolla::TASK_STATUS_FAILED)
            {
                Some((status_a, status_b))
            } else {
                None
            }
        })
        .await
        .expect("Both tasks should reach FAILED state within the allotted timeframe");

    // Both tasks should be FAILED after exhausting all 3 attempts
    assert_eq!(
        final_a_status,
        Some(azolla::TASK_STATUS_FAILED),
        "TaskA should be FAILED after 3 attempts (T=0-1, T=6-7, T=12-13) by T=15. (actual: {final_a_status:?})"
    );

    assert_eq!(
        final_b_status,
        Some(azolla::TASK_STATUS_FAILED),
        "TaskB should be FAILED after 3 attempts (T=2-3, T=4-5, T=6-7) by T=15. (actual: {final_b_status:?})"
    );

    // === TEST CLEANUP ===
    // Shutdown test environment and clean up resources
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
            args: serde_json::to_string(&Vec::<String>::new()).unwrap(),
            kwargs: r#"{"should_fail": true}"#.to_string(),
            retry_policy: build_retry_policy(json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {
                    "strategy": "fixed",
                    "delay": 5.0
                },
                "retry": {"include_errors": ["TestError", "ValueError", "RuntimeError"]}
            })),
            flow_instance_id: None,
            shepherd_group: None,
        }
    }

    /// Creates a task that always fails with a short retry delay for predictable timing.
    /// Includes TestError in retry policy for proper error matching.
    pub fn failing_task_with_short_retry_delay() -> CreateTaskRequest {
        CreateTaskRequest {
            name: "always_fail".to_string(),
            domain: "test".to_string(),
            args: serde_json::to_string(&Vec::<String>::new()).unwrap(),
            kwargs: r#"{"should_fail": true}"#.to_string(),
            retry_policy: build_retry_policy(json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {
                    "strategy": "fixed",
                    "delay": 1.0
                },
                "retry": {"include_errors": ["TestError", "ValueError", "RuntimeError"]}
            })),
            flow_instance_id: None,
            shepherd_group: None,
        }
    }
}
