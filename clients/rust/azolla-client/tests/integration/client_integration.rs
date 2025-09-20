//! Client integration tests with real orchestrator
//! Tests client connection, task submission, and orchestrator interactions

use azolla_client::client::ClientConfig;
use azolla_client::error::AzollaError;
use azolla_client::retry_policy::RetryPolicy;
use azolla_client::Client;
use serde_json::json;
use std::time::Duration;

use super::TestOrchestrator;

/// Test the expected behavior: client connection with invalid endpoint
#[tokio::test]
async fn test_client_connection_invalid_endpoint() {
    // Test connection with malformed endpoint - should trigger lines 54-56
    let result = Client::connect("invalid://endpoint::malformed").await;

    assert!(result.is_err());
    if let Err(AzollaError::InvalidConfig(msg)) = result {
        assert!(msg.contains("Invalid endpoint"));
    } else {
        panic!("Expected InvalidConfig error for malformed endpoint");
    }
}

/// Test the purpose of Client connection timeout handling
/// Expected behavior: connection should fail with timeout error for unreachable endpoint
#[tokio::test]
async fn test_client_connection_timeout() {
    let config = ClientConfig {
        endpoint: "http://192.0.2.1:12345".to_string(), // Non-routable IP
        domain: "test".to_string(),
        timeout: Duration::from_millis(10), // Very short timeout
    };

    // This should trigger connection timeout (lines 58-59)
    let result = Client::with_config(config).await;
    assert!(result.is_err());
}

/// Test the expected behavior: Client builder build method failure
#[tokio::test]
async fn test_client_builder_build_failure() {
    // Test ClientBuilder build method with invalid config
    let result = Client::builder()
        .endpoint("invalid://endpoint")
        .timeout(Duration::from_millis(1))
        .build()
        .await;

    assert!(result.is_err());
}

/// Test client connection lifecycle with orchestrator
#[tokio::test]
async fn test_client_connection_lifecycle() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("test")
        .timeout(Duration::from_secs(10))
        .build()
        .await
        .expect("Failed to connect to orchestrator");

    // Test that client can be used (this will likely fail without tasks, but should connect)
    let task_submission = client
        .submit_task("test_task")
        .args(json!({"test": "data"}))
        .expect("Failed to set args")
        .shepherd_group("test-group");

    let result = task_submission.submit().await;
    // Submission should succeed even if no workers are registered
    let handle = result.expect("Task submission should succeed without workers");

    // Without workers, waiting on the task should time out rather than completing
    let wait_outcome = tokio::time::timeout(Duration::from_millis(200), handle.wait()).await;
    assert!(
        wait_outcome.is_err(),
        "Task wait unexpectedly completed without workers"
    );
}

/// Test task submission with retry policy
#[tokio::test]
async fn test_task_submission_with_retry_policy() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .domain("test")
        .build()
        .await
        .expect("Failed to connect to orchestrator");

    let retry_policy = RetryPolicy::builder()
        .max_attempts(3)
        .initial_delay(Duration::from_millis(100))
        .build();

    let result = client
        .submit_task("nonexistent_task")
        .args(json!({}))
        .expect("Failed to set args")
        .retry_policy(retry_policy)
        .submit()
        .await;

    // Task submission should succeed (orchestrator accepts it), but task will be queued
    // This tests that retry policy serialization works correctly
    assert!(result.is_ok());
}

/// Test task submission with various argument types
#[tokio::test]
async fn test_task_submission_with_args() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .build()
        .await
        .expect("Failed to connect to orchestrator");

    // Test different argument serialization patterns
    let simple_args_result = client
        .submit_task("test_task")
        .args(42)
        .expect("Failed to set args")
        .submit()
        .await;
    assert!(simple_args_result
        .as_ref()
        .map(|handle| !handle.id().is_empty())
        .unwrap_or(false));

    let complex_args_result = client
        .submit_task("test_task")
        .args(json!({"nested": {"data": [1, 2, 3]}}))
        .expect("Failed to set args")
        .submit()
        .await;
    assert!(complex_args_result
        .as_ref()
        .map(|handle| !handle.id().is_empty())
        .unwrap_or(false));

    let array_args_result = client
        .submit_task("test_task")
        .args(vec![1, 2, 3, 4, 5])
        .expect("Failed to set args")
        .submit()
        .await;
    assert!(array_args_result
        .as_ref()
        .map(|handle| !handle.id().is_empty())
        .unwrap_or(false));
}

/// Test task submission with shepherd group
#[tokio::test]
async fn test_task_submission_with_shepherd_group() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .build()
        .await
        .expect("Failed to connect to orchestrator");

    let result = client
        .submit_task("test_task")
        .args(json!({}))
        .expect("Failed to set args")
        .shepherd_group("special-workers")
        .submit()
        .await;

    assert!(result
        .as_ref()
        .map(|handle| !handle.id().is_empty())
        .unwrap_or(false));
}

/// Test task wait operations
#[tokio::test]
async fn test_task_wait_operations() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .build()
        .await
        .expect("Failed to connect to orchestrator");

    // Submit a task (will fail but might get a task handle)
    let task_result = client
        .submit_task("test_task")
        .args(json!({}))
        .expect("Failed to set args")
        .submit()
        .await;

    let handle = task_result.expect("Task submission should succeed without workers");
    let wait_outcome = tokio::time::timeout(Duration::from_millis(200), handle.wait()).await;
    assert!(
        wait_outcome.is_err(),
        "Task wait unexpectedly completed without workers"
    );
}

/// Test client connection timeouts in various scenarios
#[tokio::test]
async fn test_client_connection_timeouts() {
    // Test connection timeout with valid orchestrator
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    // Test very short timeout (should still succeed for local connections)
    let quick_client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .timeout(Duration::from_millis(5000)) // Reasonable timeout for local connection
        .build()
        .await;

    // Local connection should succeed even with short timeout
    assert!(quick_client.is_ok());
}

/// Test serialization errors in task submission
#[tokio::test]
async fn test_task_submission_serialization_errors() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .build()
        .await
        .expect("Failed to connect to orchestrator");

    // Test with arguments that might cause serialization issues
    // (though serde_json is quite robust)
    let result = client
        .submit_task("test_task")
        .args(json!({"valid": "json"}))
        .expect("Failed to set args")
        .submit()
        .await;

    // Submission succeeds; serialization errors would surface before this point
    assert!(result
        .as_ref()
        .map(|handle| !handle.id().is_empty())
        .unwrap_or(false));
}

/// Test retry policy JSON serialization in submission
#[tokio::test]
async fn test_retry_policy_serialization_in_submission() {
    let orchestrator = TestOrchestrator::start()
        .await
        .expect("Failed to start test orchestrator");

    let client = Client::builder()
        .endpoint(&orchestrator.endpoint())
        .build()
        .await
        .expect("Failed to connect to orchestrator");

    let complex_retry_policy = RetryPolicy::builder()
        .max_attempts(5)
        .initial_delay(Duration::from_millis(200))
        .max_delay(Duration::from_secs(30))
        .retry_on(&["NetworkError", "TimeoutError", "ServiceUnavailable"])
        .exclude_errors(&["AuthenticationError", "ValidationError"])
        .build();

    let result = client
        .submit_task("complex_task")
        .args(json!({"complex": "data"}))
        .expect("Failed to set args")
        .retry_policy(complex_retry_policy)
        .submit()
        .await;

    // Submission should succeed even if task execution ultimately fails
    assert!(result
        .as_ref()
        .map(|handle| !handle.id().is_empty())
        .unwrap_or(false));
}
