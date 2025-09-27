//! Integration tests for shepherd lifecycle and cluster management.
//!
//! This module tests shepherd startup, registration, configuration, and multi-shepherd
//! coordination. These tests verify that shepherds can properly connect to the orchestrator,
//! register themselves, and work together in a cluster configuration.

#![cfg(feature = "test-harness")]

use azolla::test_harness::{find_available_port, IntegrationTestEnvironment};

/// Tests basic shepherd startup and registration with the orchestrator.
///
/// **Purpose:** Verifies that a shepherd can start correctly, configure itself properly,
/// and register with the orchestrator for task dispatch.
///
/// **Flow:**
/// 1. Starts a single shepherd instance
/// 2. Verifies shepherd configuration (concurrency, binary path, etc.)
/// 3. Confirms shepherd registration with orchestrator
/// 4. Validates load tracking functionality
///
/// **Expected Behavior:**
/// - Shepherd should configure with correct max_concurrency (4)
/// - Worker binary path should be properly resolved
/// - Initial load should be 0
/// - Shepherd should register within 5 seconds
/// - Orchestrator should report 1 connected shepherd
#[tokio::test]
async fn test_shepherd_startup_and_registration() {
    let _ = env_logger::try_init();

    let mut harness = IntegrationTestEnvironment::new().await.unwrap();

    // Test that shepherd starts correctly using the refactored code
    let shepherd_uuid = harness.shepherd_config.uuid;
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

    // Wait for shepherd to register with orchestrator
    let registered = harness
        .wait_for_shepherd_registration(shepherd_uuid, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    assert!(registered, "Shepherd should register within 5 seconds");

    // Verify shepherd is registered with orchestrator
    let shepherd_count = harness.get_shepherd_count().await.unwrap();
    assert_eq!(shepherd_count, 1, "Should have one registered shepherd");

    harness.shutdown().await.unwrap();
}

/// Tests multiple shepherds running concurrently in a cluster.
///
/// **Purpose:** Verifies that multiple shepherd instances can coexist and register
/// with the same orchestrator, enabling horizontal scaling of task execution capacity.
///
/// **Flow:**
/// 1. Starts first shepherd and waits for registration
/// 2. Configures and starts second shepherd with different ports/UUID
/// 3. Waits for second shepherd registration
/// 4. Verifies both shepherds are tracked by orchestrator
///
/// **Expected Behavior:**
/// - First shepherd should register successfully
/// - Second shepherd should use different ports to avoid conflicts
/// - Both shepherds should have unique UUIDs
/// - Orchestrator should report 2 connected shepherds
/// - No resource conflicts or registration failures
///
/// **Note:** This test ensures proper isolation and coordination in multi-shepherd deployments.
#[tokio::test]
async fn test_multiple_shepherds() {
    let _ = env_logger::try_init();

    let mut harness = IntegrationTestEnvironment::new().await.unwrap();

    // Start first shepherd
    let shepherd1_uuid = harness.shepherd_config.uuid;
    let _shepherd1 = harness.start_shepherd().await.unwrap();

    // Wait for first shepherd to register
    let registered1 = harness
        .wait_for_shepherd_registration(shepherd1_uuid, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        registered1,
        "First shepherd should register within 5 seconds"
    );

    // Update config for second shepherd (different port and UUID)
    harness.shepherd_config.worker_grpc_port = find_available_port();
    harness.shepherd_config.uuid = uuid::Uuid::new_v4();
    let shepherd2_uuid = harness.shepherd_config.uuid;

    // Start second shepherd
    let _shepherd2 = harness.start_shepherd().await.unwrap();

    // Wait for second shepherd to register
    let registered2 = harness
        .wait_for_shepherd_registration(shepherd2_uuid, std::time::Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        registered2,
        "Second shepherd should register within 5 seconds"
    );

    // Verify both shepherds are registered
    let shepherd_count = harness.get_shepherd_count().await.unwrap();
    assert_eq!(shepherd_count, 2, "Should have two registered shepherds");

    harness.shutdown().await.unwrap();
}
