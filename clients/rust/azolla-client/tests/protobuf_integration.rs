/// Test the purpose of Protocol Buffer integration: ensure protobuf modules are available and basic functionality works
/// Test the expected behavior: protobuf modules should be accessible and provide basic protobuf functionality
use prost::Message;
use serde_json::json;

/// Test that protobuf modules are accessible
#[test]
fn test_protobuf_modules_available() {
    // Test that we can access the proto modules without panicking
    // This ensures the build process generated the protobuf code correctly

    // This test mainly verifies that the protobuf code generation worked
    // and that we can access the proto modules in the crate
    println!("Testing protobuf module availability");

    // Basic test that the proto module exists
    let _proto_module =
        std::any::type_name::<azolla_client::proto::orchestrator::CreateTaskRequest>();
    println!("Orchestrator proto module accessible: {_proto_module}");

    let _common_module = std::any::type_name::<azolla_client::proto::common::Task>();
    println!("Common proto module accessible: {_common_module}");

    assert_eq!(2 + 2, 4); // Basic compilation test
}

/// Test protobuf compilation and module structure
#[test]
fn test_protobuf_structure() {
    // Verify that the proto module structure exists
    // This is mainly a compilation test to ensure protobuf generation worked

    use azolla_client::proto::common;
    use azolla_client::proto::orchestrator;

    println!("Testing protobuf module structure");

    // Test that we can create instances of protobuf messages
    let task = common::Task::default();
    assert!(task.task_id.is_empty()); // Default values should be empty/default

    let create_task_req = orchestrator::CreateTaskRequest::default();
    assert!(create_task_req.name.is_empty());

    println!("Protobuf structures created successfully");
}

/// Test basic protobuf serialization
#[test]
fn test_basic_protobuf_serialization() {
    use azolla_client::proto::common;

    // Create a simple task
    let original_task = common::Task {
        task_id: "test-task-123".to_string(),
        name: "test_task".to_string(),
        ..Default::default()
    };

    // Test encoding
    let encoded = original_task.encode_to_vec();
    assert!(!encoded.is_empty());
    println!("Encoded task size: {} bytes", encoded.len());

    // Test decoding
    let decoded_task = common::Task::decode(&encoded[..]).unwrap();

    // Verify all fields match
    assert_eq!(decoded_task.task_id, original_task.task_id);
    assert_eq!(decoded_task.name, original_task.name);

    println!("Protobuf serialization/deserialization successful");
}

/// Test create task request protobuf message
#[test]
fn test_create_task_request_serialization() {
    use azolla_client::proto::orchestrator;

    let original_request = orchestrator::CreateTaskRequest {
        name: "data_processing_task".to_string(),
        domain: "production".to_string(),
        shepherd_group: Some("gpu-workers".to_string()),
        args: "test-args".to_string(),
        ..Default::default()
    };

    // Test encoding
    let encoded = original_request.encode_to_vec();
    assert!(!encoded.is_empty());

    // Test decoding
    let decoded_request = orchestrator::CreateTaskRequest::decode(&encoded[..]).unwrap();

    // Verify all fields match
    assert_eq!(decoded_request.name, original_request.name);
    assert_eq!(decoded_request.domain, original_request.domain);
    assert_eq!(
        decoded_request.shepherd_group,
        original_request.shepherd_group
    );
    assert_eq!(decoded_request.args, original_request.args);

    println!("CreateTaskRequest serialization successful");
}

/// Test create task response protobuf message
#[test]
fn test_create_task_response_serialization() {
    use azolla_client::proto::orchestrator;

    let original_response = orchestrator::CreateTaskResponse {
        task_id: "created-task-456".to_string(),
    };

    // Test encoding
    let encoded = original_response.encode_to_vec();
    assert!(!encoded.is_empty());

    // Test decoding
    let decoded_response = orchestrator::CreateTaskResponse::decode(&encoded[..]).unwrap();

    // Verify fields match
    assert_eq!(decoded_response.task_id, original_response.task_id);

    println!("CreateTaskResponse serialization successful");
}

/// Test protobuf message with empty fields
#[test]
fn test_protobuf_empty_fields() {
    use azolla_client::proto::common;

    let task_with_empty_fields = common::Task {
        task_id: "empty-fields-task".to_string(),
        name: "".to_string(), // Empty name
        ..Default::default()
    };

    // Should encode/decode without issues
    let encoded = task_with_empty_fields.encode_to_vec();
    let decoded = common::Task::decode(&encoded[..]).unwrap();

    assert_eq!(decoded.task_id, task_with_empty_fields.task_id);
    assert_eq!(decoded.name, task_with_empty_fields.name);

    println!("Empty fields handled correctly");
}

/// Test protobuf message size with large payloads
#[test]
fn test_protobuf_large_payloads() {
    use azolla_client::proto::common;

    // Create a task with large data
    let large_data = "x".repeat(5_000); // 5KB string
    let task_with_large_data = common::Task {
        task_id: "large-payload-task".to_string(),
        name: large_data.clone(),
        ..Default::default()
    };

    // Should handle large payloads correctly
    let encoded = task_with_large_data.encode_to_vec();
    assert!(encoded.len() > 5_000); // Encoded size should reflect large payload

    let decoded = common::Task::decode(&encoded[..]).unwrap();
    assert_eq!(decoded.name, large_data);

    println!(
        "Large payload handled correctly, size: {} bytes",
        encoded.len()
    );
}

/// Test error handling in protobuf decoding
#[test]
fn test_protobuf_decode_errors() {
    use azolla_client::proto::common;

    // Test with invalid/corrupted data
    let invalid_data = [0xFF, 0xFF, 0xFF, 0xFF];
    let result = common::Task::decode(&invalid_data[..]);
    assert!(result.is_err());

    // Test with empty data
    let empty_data = [];
    let result = common::Task::decode(&empty_data[..]);
    // This should succeed as protobuf can decode empty messages
    assert!(result.is_ok());
    let decoded = result.unwrap();
    assert_eq!(decoded.task_id, "");
    assert_eq!(decoded.name, "");

    println!("Protobuf error handling works correctly");
}

/// Test protobuf message field determinism
#[test]
fn test_protobuf_deterministic_encoding() {
    use azolla_client::proto::common;

    // Create two identical messages
    let task1 = common::Task {
        task_id: "determinism-test".to_string(),
        name: "test_task".to_string(),
        ..Default::default()
    };

    let task2 = common::Task {
        task_id: "determinism-test".to_string(),
        name: "test_task".to_string(),
        ..Default::default()
    };

    // Both should encode to the same bytes
    let encoded1 = task1.encode_to_vec();
    let encoded2 = task2.encode_to_vec();
    assert_eq!(encoded1, encoded2);

    println!("Protobuf encoding is deterministic");
}

/// Test protobuf integration with JSON serialization
#[test]
fn test_protobuf_json_integration() {
    use azolla_client::proto::common;

    // Create a task
    let task = common::Task {
        task_id: "json-integration-test".to_string(),
        name: "test_task".to_string(),
        ..Default::default()
    };

    // Convert task data to JSON (simulating what might happen in real usage)
    let task_as_json = json!({
        "task_id": task.task_id,
        "name": task.name
    });

    // Verify JSON contains expected data
    assert_eq!(task_as_json["task_id"], "json-integration-test");
    assert_eq!(task_as_json["name"], "test_task");

    // This demonstrates that protobuf data can be easily converted to JSON
    println!("Protobuf-JSON integration test passed");
}

/// Test protobuf with concurrent access
#[tokio::test]
async fn test_protobuf_concurrent_usage() {
    use azolla_client::proto::common;
    use std::sync::Arc;

    let base_task = Arc::new(common::Task {
        task_id: "concurrent-test".to_string(),
        name: "concurrent_task".to_string(),
        ..Default::default()
    });

    // Spawn multiple tasks that encode the same protobuf message
    let mut handles = Vec::new();
    for i in 0..5 {
        let task = base_task.clone();
        let handle = tokio::spawn(async move {
            let encoded = task.encode_to_vec();
            let decoded = common::Task::decode(&encoded[..]).unwrap();
            (i, decoded.task_id == task.task_id)
        });
        handles.push(handle);
    }

    // Wait for all tasks to complete
    let mut success_count = 0;
    for handle in handles {
        let (task_id, success) = handle.await.unwrap();
        if success {
            success_count += 1;
        }
        println!("Concurrent task {task_id} completed: {success}");
    }

    assert_eq!(success_count, 5);
    println!("Concurrent protobuf usage successful");
}
