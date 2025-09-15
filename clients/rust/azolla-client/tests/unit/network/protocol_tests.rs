//! Protocol and serialization tests
//! Tests gRPC protocol handling, message serialization, and communication

use azolla_client::error::{AzollaError, TaskError};
use serde_json::{json, Value};

/// Test gRPC status code handling
#[test]
fn test_grpc_status_conversion() {
    let grpc_statuses = vec![
        (tonic::Code::Ok, "OK"),
        (tonic::Code::InvalidArgument, "InvalidArgument"),
        (tonic::Code::DeadlineExceeded, "DeadlineExceeded"),
        (tonic::Code::NotFound, "NotFound"),
        (tonic::Code::Unavailable, "Unavailable"),
        (tonic::Code::Internal, "Internal"),
        (tonic::Code::Unauthenticated, "Unauthenticated"),
    ];

    for (code, description) in grpc_statuses {
        let status = tonic::Status::new(code, format!("Test {description} error"));
        let azolla_error: AzollaError = status.into();

        match azolla_error {
            AzollaError::Grpc(converted_status) => {
                assert_eq!(converted_status.code(), code);
                assert!(converted_status.message().contains(description));
            }
            _ => panic!("Expected Grpc error variant for {description}"),
        }
    }
}

/// Test message serialization for various data types
#[test]
fn test_message_serialization() {
    let test_cases = vec![
        (json!(null), "null value"),
        (json!(true), "boolean true"),
        (json!(false), "boolean false"),
        (json!(42), "integer"),
        (json!(std::f64::consts::PI), "float"),
        (json!("hello world"), "string"),
        (json!(""), "empty string"),
        (json!("unicode: 🦀 🔥 ⚡"), "unicode string"),
        (json!([]), "empty array"),
        (json!([1, 2, 3]), "number array"),
        (json!(["a", "b", "c"]), "string array"),
        (json!({}), "empty object"),
        (json!({"key": "value"}), "simple object"),
        (
            json!({
                "nested": {
                    "array": [1, 2, {"deep": true}],
                    "number": 123.456,
                    "null_field": null
                }
            }),
            "complex nested structure",
        ),
    ];

    for (value, description) in test_cases {
        println!("Testing serialization for: {description}");

        // Test JSON serialization round-trip
        let serialized = serde_json::to_string(&value).unwrap();
        assert!(
            !serialized.is_empty(),
            "Serialized {description} should not be empty"
        );

        let deserialized: Value = serde_json::from_str(&serialized).unwrap();
        assert_eq!(value, deserialized, "Round-trip failed for {description}");

        // Test compact serialization
        let compact = serde_json::to_string(&value).unwrap();
        let pretty = serde_json::to_string_pretty(&value).unwrap();

        // Pretty should be longer or equal (unless it's very simple)
        assert!(
            pretty.len() >= compact.len(),
            "Pretty format should be >= compact for {description}"
        );
    }
}

/// Test task error serialization for protocol communication
#[test]
fn test_task_error_protocol_serialization() {
    let errors = vec![
        TaskError::execution_failed("Task execution failed"),
        TaskError::invalid_args("Invalid arguments provided"),
        TaskError::new("Custom error message")
            .with_error_type("CustomError")
            .with_error_code("CUSTOM_001"),
        TaskError {
            error_type: "NetworkError".to_string(),
            message: "Connection to database failed".to_string(),
            code: Some("NET_DB_001".to_string()),
            data: Some(json!({
                "host": "db.example.com",
                "port": 5432,
                "database": "production",
                "retry_count": 3
            })),
            retryable: true,
        },
    ];

    for error in errors {
        println!("Testing error serialization: {}", error.error_type);

        // Test JSON serialization
        let serialized = serde_json::to_string(&error).unwrap();
        assert!(!serialized.is_empty());
        assert!(serialized.contains(&error.error_type));
        assert!(serialized.contains(&error.message));

        // Test deserialization
        let deserialized: TaskError = serde_json::from_str(&serialized).unwrap();
        assert_eq!(error.error_type, deserialized.error_type);
        assert_eq!(error.message, deserialized.message);
        assert_eq!(error.code, deserialized.code);
        assert_eq!(error.retryable, deserialized.retryable);
        assert_eq!(error.data, deserialized.data);

        // Test pretty-printing for debugging
        let pretty = serde_json::to_string_pretty(&error).unwrap();
        assert!(pretty.len() >= serialized.len());
        println!("Pretty error: {pretty}");
    }
}

/// Test protocol buffer compatibility structures
#[test]
fn test_protobuf_compatibility() {
    // Test structures that would be used in gRPC communication

    #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
    struct TaskRequest {
        task_name: String,
        args: Vec<Value>,
        domain: String,
        shepherd_group: Option<String>,
        retry_policy: Option<Value>,
    }

    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct TaskResponse {
        task_id: String,
        status: String,
        result: Option<Value>,
        error: Option<TaskError>,
    }

    impl PartialEq for TaskResponse {
        fn eq(&self, other: &Self) -> bool {
            self.task_id == other.task_id
                && self.status == other.status
                && self.result == other.result
                && match (&self.error, &other.error) {
                    (None, None) => true,
                    (Some(a), Some(b)) => {
                        a.error_type == b.error_type
                            && a.message == b.message
                            && a.code == b.code
                            && a.data == b.data
                    }
                    _ => false,
                }
        }
    }

    // Test task request serialization
    let request = TaskRequest {
        task_name: "test_task".to_string(),
        args: vec![json!(42), json!("hello"), json!(true)],
        domain: "production".to_string(),
        shepherd_group: Some("worker-pool-1".to_string()),
        retry_policy: Some(json!({
            "max_attempts": 3,
            "initial_delay_ms": 1000
        })),
    };

    let request_json = serde_json::to_string(&request).unwrap();
    let request_back: TaskRequest = serde_json::from_str(&request_json).unwrap();
    assert_eq!(request, request_back);

    // Test task response serialization (success case)
    let success_response = TaskResponse {
        task_id: "task-123".to_string(),
        status: "completed".to_string(),
        result: Some(json!({"output": "success", "value": 42})),
        error: None,
    };

    let success_json = serde_json::to_string(&success_response).unwrap();
    let success_back: TaskResponse = serde_json::from_str(&success_json).unwrap();
    assert_eq!(success_response, success_back);

    // Test task response serialization (error case)
    let error_response = TaskResponse {
        task_id: "task-456".to_string(),
        status: "failed".to_string(),
        result: None,
        error: Some(TaskError::execution_failed("Task processing failed")),
    };

    let error_json = serde_json::to_string(&error_response).unwrap();
    let error_back: TaskResponse = serde_json::from_str(&error_json).unwrap();
    assert_eq!(error_response, error_back);
}

/// Test binary data handling in protocol messages
#[test]
fn test_binary_data_handling() {
    // Test simple binary data representation using byte arrays
    let binary_data = vec![0u8, 1, 2, 3, 255, 254, 253];
    let _encoded: Vec<u8> = binary_data.clone();

    // Test binary data in JSON messages (using numbers array)
    let message_with_binary = json!({
        "data": binary_data,
        "length": binary_data.len(),
        "encoding": "byte_array"
    });

    let serialized = serde_json::to_string(&message_with_binary).unwrap();
    let deserialized: Value = serde_json::from_str(&serialized).unwrap();
    assert_eq!(message_with_binary, deserialized);

    // Verify binary data can be extracted
    let extracted_data = deserialized["data"].as_array().unwrap();
    let extracted_binary: Vec<u8> = extracted_data
        .iter()
        .map(|v| v.as_u64().unwrap() as u8)
        .collect();
    assert_eq!(binary_data, extracted_binary);
}

/// Test large message handling
#[test]
fn test_large_message_handling() {
    // Create a large JSON structure
    let large_array: Vec<i32> = (0..10000).collect();
    let large_object = json!({
        "array": large_array,
        "metadata": {
            "size": 10000,
            "type": "large_test",
            "description": "A".repeat(1000)  // 1000 character string
        }
    });

    // Test serialization performance and correctness
    let start_time = std::time::Instant::now();
    let serialized = serde_json::to_string(&large_object).unwrap();
    let serialize_duration = start_time.elapsed();

    assert!(serialized.len() > 10000); // Should be a reasonably large message
    println!("Large message size: {} bytes", serialized.len());
    println!("Large message serialization took: {serialize_duration:?}");

    // Test deserialization
    let start_time = std::time::Instant::now();
    let deserialized: Value = serde_json::from_str(&serialized).unwrap();
    let deserialize_duration = start_time.elapsed();

    println!("Large message deserialization took: {deserialize_duration:?}");

    // Verify correctness
    assert_eq!(large_object, deserialized);
    assert_eq!(deserialized["array"].as_array().unwrap().len(), 10000);
    assert_eq!(deserialized["metadata"]["size"], 10000);
}

/// Test protocol version compatibility
#[test]
fn test_protocol_version_compatibility() {
    #[derive(serde::Serialize, serde::Deserialize, Debug)]
    struct VersionedMessage {
        version: u32,
        #[serde(skip_serializing_if = "Option::is_none")]
        data_v1: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        data_v2: Option<Value>,
        #[serde(default)]
        features: Vec<String>,
    }

    // Test version 1 message
    let v1_message = VersionedMessage {
        version: 1,
        data_v1: Some("legacy data".to_string()),
        data_v2: None,
        features: vec!["basic".to_string()],
    };

    let v1_json = serde_json::to_string(&v1_message).unwrap();
    println!("V1 message: {v1_json}");

    // Test version 2 message
    let v2_message = VersionedMessage {
        version: 2,
        data_v1: None,
        data_v2: Some(json!({"advanced": "features", "count": 42})),
        features: vec!["advanced".to_string(), "new_feature".to_string()],
    };

    let v2_json = serde_json::to_string(&v2_message).unwrap();
    println!("V2 message: {v2_json}");

    // Test cross-version deserialization
    let v1_parsed: VersionedMessage = serde_json::from_str(&v1_json).unwrap();
    assert_eq!(v1_parsed.version, 1);
    assert!(v1_parsed.data_v1.is_some());
    assert!(v1_parsed.data_v2.is_none());

    let v2_parsed: VersionedMessage = serde_json::from_str(&v2_json).unwrap();
    assert_eq!(v2_parsed.version, 2);
    assert!(v2_parsed.data_v1.is_none());
    assert!(v2_parsed.data_v2.is_some());
}

/// Test UTF-8 and special character handling
#[test]
fn test_utf8_special_characters() {
    let special_chars = vec![
        "ASCII text",
        "Émojis: 🚀 🎉 ✨ 🦀",
        "Unicode: αβγδε 中文 русский עברית عربي",
        "Special: \n\t\r\"\\",
        "Math: ∑∫∂∇ ≤≥≠≈ ∞∅∈∋",
        "Arrows: ←→↑↓ ⟵⟶⟷",
        "Currency: $€£¥₹₽",
        "Mixed: Hello 世界 🌍 $100",
    ];

    for text in special_chars {
        println!("Testing UTF-8 text: {text}");

        // Test JSON serialization
        let json_value = json!({"text": text, "length": text.len()});
        let serialized = serde_json::to_string(&json_value).unwrap();
        let deserialized: Value = serde_json::from_str(&serialized).unwrap();

        assert_eq!(json_value, deserialized);
        assert_eq!(deserialized["text"].as_str().unwrap(), text);

        // Test in task error context
        let error = TaskError::new(text).with_error_type("UTF8Test");
        let error_json = serde_json::to_string(&error).unwrap();
        let error_back: TaskError = serde_json::from_str(&error_json).unwrap();
        assert_eq!(error.message, error_back.message);
    }
}

/// Test JSON schema validation concepts
#[test]
fn test_json_schema_validation() {
    // Test required fields validation
    let valid_message = json!({
        "task_name": "test_task",
        "args": [],
        "domain": "test"
    });

    let invalid_message = json!({
        "task_name": "test_task"
        // Missing required fields
    });

    // Test structure validation function
    fn validate_task_request(value: &Value) -> Result<(), String> {
        let obj = value.as_object().ok_or("Must be an object")?;

        if !obj.contains_key("task_name") {
            return Err("Missing required field: task_name".to_string());
        }

        if !obj.contains_key("args") {
            return Err("Missing required field: args".to_string());
        }

        if !obj.contains_key("domain") {
            return Err("Missing required field: domain".to_string());
        }

        Ok(())
    }

    assert!(validate_task_request(&valid_message).is_ok());
    assert!(validate_task_request(&invalid_message).is_err());

    // Test type validation
    let wrong_type_message = json!({
        "task_name": 123, // Should be string
        "args": "not_an_array", // Should be array
        "domain": true // Should be string
    });

    let type_error = validate_task_request(&wrong_type_message);
    // This simple validator doesn't check types, but shows the concept
    assert!(type_error.is_ok()); // Would fail with a more sophisticated validator
}
