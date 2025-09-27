//! Integration tests for shepherd group routing behavior.
//!
//! Verifies that tasks are routed to:
//! 1) the domain's default shepherd group when no group is specified;
//! 2) the designated shepherd group when provided on task creation.

#![cfg(feature = "test-harness")]

use azolla::orchestrator::retry_policy::RetryPolicy as InternalRetryPolicy;
use azolla::proto::common::RetryPolicy as ProtoRetryPolicy;
use azolla::proto::orchestrator::CreateTaskRequest;
use azolla::test_harness::{find_available_port, IntegrationTestEnvironment};
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

const DOMAIN: &str = "group_test";

fn build_retry_policy(json: serde_json::Value) -> Option<ProtoRetryPolicy> {
    let policy = InternalRetryPolicy::from_json(&json).expect("invalid retry policy json");
    Some(policy.to_proto())
}

// Helper removed in favor of test harness API

#[tokio::test]
async fn test_default_group_routing() {
    let _ = env_logger::try_init();

    // Start orchestrator and clients
    let mut harness = IntegrationTestEnvironment::new().await.unwrap();
    // Ensure worker binary exists to avoid any shepherd-side startup hiccups
    harness.ensure_worker_binary().await.unwrap();

    // Configure shepherd in domain with default group "default"
    harness.shepherd_config.domain = DOMAIN.to_string();
    harness.shepherd_config.shepherd_group = "default".to_string();
    let shepherd_uuid = harness.shepherd_config.uuid;
    harness.start_shepherd().await.unwrap();

    // Wait for registration
    harness
        .wait_for_shepherd_registration(shepherd_uuid, Duration::from_secs(5))
        .await
        .unwrap();

    // Submit a task without specifying shepherd_group
    let req = CreateTaskRequest {
        name: "echo".to_string(),
        domain: DOMAIN.to_string(),
        retry_policy: build_retry_policy(json!({
            "version": 1,
            "stop": {"max_attempts": 1},
            "wait": {"strategy": "fixed", "delay": 1}
        })),
        args: serde_json::to_string(&vec!["hello".to_string()]).unwrap(),
        kwargs: "{}".to_string(),
        flow_instance_id: None,
        shepherd_group: None,
    };
    let resp = harness.client.create_task(req).await.unwrap().into_inner();
    let task_id = Uuid::parse_str(&resp.task_id).unwrap();

    // Wait for attempt started event and verify routing metadata
    let metadata = harness
        .wait_for_attempt_started_metadata(&task_id, DOMAIN, Duration::from_secs(5))
        .await
        .expect("query events returned")
        .expect("attempt started event present");
    assert_eq!(
        metadata.get("shepherd_group").and_then(|v| v.as_str()),
        Some("default")
    );
    assert_eq!(
        metadata
            .get("shepherd_uuid")
            .and_then(|v| v.as_str())
            .map(|s| Uuid::parse_str(s).unwrap()),
        Some(shepherd_uuid)
    );

    harness.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_designated_group_routing() {
    let _ = env_logger::try_init();

    let mut harness = IntegrationTestEnvironment::new().await.unwrap();
    // Ensure worker binary exists to avoid any shepherd-side startup hiccups
    harness.ensure_worker_binary().await.unwrap();

    // Shepherd A in group "blue"
    harness.shepherd_config.domain = DOMAIN.to_string();
    harness.shepherd_config.shepherd_group = "blue".to_string();
    harness.shepherd_config.worker_grpc_port = find_available_port();
    harness.shepherd_config.uuid = Uuid::new_v4();
    let blue_uuid = harness.shepherd_config.uuid;
    harness.start_shepherd().await.unwrap();
    harness
        .wait_for_shepherd_registration(blue_uuid, Duration::from_secs(5))
        .await
        .unwrap();

    // Shepherd B in group "green"
    harness.shepherd_config.shepherd_group = "green".to_string();
    harness.shepherd_config.worker_grpc_port = find_available_port();
    harness.shepherd_config.uuid = Uuid::new_v4();
    let green_uuid = harness.shepherd_config.uuid;
    harness.start_shepherd().await.unwrap();
    harness
        .wait_for_shepherd_registration(green_uuid, Duration::from_secs(5))
        .await
        .unwrap();

    // Submit a task targeting group "green"
    let req = CreateTaskRequest {
        name: "echo".to_string(),
        domain: DOMAIN.to_string(),
        retry_policy: build_retry_policy(json!({
            "version": 1,
            "stop": {"max_attempts": 1},
            "wait": {"strategy": "fixed", "delay": 1}
        })),
        args: serde_json::to_string(&vec!["world".to_string()]).unwrap(),
        kwargs: "{}".to_string(),
        flow_instance_id: None,
        shepherd_group: Some("green".to_string()),
    };
    let resp = harness.client.create_task(req).await.unwrap().into_inner();
    let task_id = Uuid::parse_str(&resp.task_id).unwrap();

    // Verify attempt started indicates green group and matches green shepherd uuid
    let metadata = harness
        .wait_for_attempt_started_metadata(&task_id, DOMAIN, Duration::from_secs(5))
        .await
        .expect("query events returned")
        .expect("attempt started event present");
    assert_eq!(
        metadata.get("shepherd_group").and_then(|v| v.as_str()),
        Some("green")
    );
    assert_eq!(
        metadata
            .get("shepherd_uuid")
            .and_then(|v| v.as_str())
            .map(|s| Uuid::parse_str(s).unwrap()),
        Some(green_uuid)
    );

    harness.shutdown().await.unwrap();
}
