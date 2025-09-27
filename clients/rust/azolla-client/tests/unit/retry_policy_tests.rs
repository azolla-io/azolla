//! Unit tests for retry policy functionality
//! Tests retry policy configuration, serialization, and behavior

use azolla_client::retry_policy::{RetryPolicy, WaitStrategy};
use serde_json::json;
use std::time::Duration;

/// Test RetryPolicy default implementation
#[test]
fn test_retry_policy_defaults() {
    let policy = RetryPolicy::default();

    // Test default values are reasonable
    assert!(policy.stop.max_attempts.is_some());
    // Note: max_delay might be None in default policy, which is valid

    // Test wait strategy is set
    match policy.wait {
        WaitStrategy::ExponentialJitter { .. } => { /* Expected */ }
        _ => panic!("Expected ExponentialJitter as default strategy"),
    }
}

/// Test RetryPolicy builder pattern
#[test]
fn test_retry_policy_builder() {
    let policy = RetryPolicy::builder()
        .max_attempts(5)
        .max_delay(Duration::from_secs(300))
        .initial_delay(Duration::from_millis(100))
        .build();

    assert_eq!(policy.stop.max_attempts, Some(5));
    assert_eq!(policy.stop.max_delay, Some(Duration::from_secs(300)));

    match policy.wait {
        WaitStrategy::ExponentialJitter { initial_delay, .. } => {
            assert_eq!(initial_delay, Duration::from_millis(100));
        }
        _ => panic!("Expected ExponentialJitter strategy"),
    }
}

/// Test RetryPolicy exponential backoff configuration
#[test]
fn test_exponential_backoff_configuration() {
    let policy = RetryPolicy::exponential()
        .max_attempts(3)
        .initial_delay(Duration::from_millis(500))
        .build();

    match policy.wait {
        WaitStrategy::ExponentialJitter {
            initial_delay,
            multiplier,
            max_delay,
        } => {
            assert_eq!(initial_delay, Duration::from_millis(500));
            assert_eq!(multiplier, 2.0);
            assert!(max_delay > Duration::from_secs(1));
        }
        _ => panic!("Expected ExponentialJitter strategy"),
    }
}

/// Test RetryPolicy with retry conditions
#[test]
fn test_retry_conditions() {
    let policy = RetryPolicy::builder()
        .retry_on(&["NetworkError", "TimeoutError"])
        .exclude_errors(&["AuthenticationError", "ValidationError"])
        .build();

    assert_eq!(
        policy.retry.include_errors,
        vec!["NetworkError", "TimeoutError"]
    );
    assert_eq!(
        policy.retry.exclude_errors,
        vec!["AuthenticationError", "ValidationError"]
    );
}

/// Test RetryPolicy serialization
#[test]
fn test_retry_policy_serialization() {
    let policy = RetryPolicy::builder()
        .max_attempts(3)
        .initial_delay(Duration::from_millis(200))
        .retry_on(&["NetworkError"])
        .build();

    let serialized = serde_json::to_string(&policy).unwrap();
    assert!(!serialized.is_empty());
    assert!(serialized.contains("max_attempts"));
    assert!(serialized.contains("NetworkError"));

    // Test deserialization round-trip
    let deserialized: RetryPolicy = serde_json::from_str(&serialized).unwrap();
    assert_eq!(policy.stop.max_attempts, deserialized.stop.max_attempts);
    assert_eq!(
        policy.retry.include_errors,
        deserialized.retry.include_errors
    );
}

/// Test JSON payload conversion for orchestrator submission
#[test]
fn test_retry_policy_submission_json_conversion() {
    let policy = RetryPolicy::builder()
        .max_attempts(3)
        .max_delay(Duration::from_secs(90))
        .initial_delay(Duration::from_millis(250))
        .retry_on(&["NetworkError", "TimeoutError"])
        .exclude_errors(&["FatalError"])
        .build();

    let submission_json = policy.to_submission_json();

    assert_eq!(submission_json["version"], json!(1));
    assert_eq!(submission_json["stop"]["max_attempts"], json!(3));
    assert_eq!(submission_json["stop"]["max_delay"], json!(90.0));
    assert_eq!(
        submission_json["wait"]["strategy"],
        json!("exponential_jitter")
    );
    assert_eq!(submission_json["wait"]["initial_delay"], json!(0.25));
    assert_eq!(submission_json["wait"]["multiplier"], json!(2.0));
    assert_eq!(submission_json["wait"]["max_delay"], json!(300.0));
    assert_eq!(
        submission_json["retry"]["include_errors"],
        json!(["NetworkError", "TimeoutError"])
    );
    assert_eq!(
        submission_json["retry"]["exclude_errors"],
        json!(["FatalError"])
    );
}

/// Test fixed wait strategy conversion to submission JSON
#[test]
fn test_fixed_wait_strategy_submission_json_conversion() {
    let policy = RetryPolicy::fixed(Duration::from_secs(5))
        .retry_on(&["TransientError"])
        .build();

    let submission_json = policy.to_submission_json();

    assert_eq!(submission_json["wait"]["strategy"], json!("fixed"));
    assert_eq!(submission_json["wait"]["delay"], json!(5.0));
}

/// Test RetryPolicy with no retry (disabled)
#[test]
fn test_no_retry_policy() {
    let policy = RetryPolicy::builder().max_attempts(1).build();

    assert_eq!(policy.stop.max_attempts, Some(1));
}

/// Test RetryPolicy edge cases
#[test]
fn test_retry_policy_edge_cases() {
    // Test with very large max attempts
    let policy = RetryPolicy::builder().max_attempts(u32::MAX).build();
    assert_eq!(policy.stop.max_attempts, Some(u32::MAX));

    // Test with zero delay
    let zero_delay_policy = RetryPolicy::builder()
        .initial_delay(Duration::from_secs(0))
        .build();

    match zero_delay_policy.wait {
        WaitStrategy::ExponentialJitter { initial_delay, .. } => {
            assert_eq!(initial_delay, Duration::from_secs(0));
        }
        _ => panic!("Expected ExponentialJitter strategy"),
    }

    // Test with very large delay
    let large_delay_policy = RetryPolicy::builder()
        .max_delay(Duration::from_secs(3600))
        .build();
    assert_eq!(
        large_delay_policy.stop.max_delay,
        Some(Duration::from_secs(3600))
    );
}

/// Test RetryPolicy method chaining
#[test]
fn test_builder_method_chaining() {
    let policy = RetryPolicy::exponential()
        .max_attempts(10)
        .initial_delay(Duration::from_millis(50))
        .max_delay(Duration::from_secs(60))
        .retry_on(&["Error1", "Error2"])
        .exclude_errors(&["FatalError"])
        .build();

    assert_eq!(policy.stop.max_attempts, Some(10));
    assert_eq!(policy.stop.max_delay, Some(Duration::from_secs(60)));
    assert_eq!(policy.retry.include_errors, vec!["Error1", "Error2"]);
    assert_eq!(policy.retry.exclude_errors, vec!["FatalError"]);
}

/// Test initial delay modification
#[test]
fn test_initial_delay_modification() {
    let policy1 = RetryPolicy::builder()
        .initial_delay(Duration::from_millis(100))
        .build();

    let policy2 = RetryPolicy::builder()
        .initial_delay(Duration::from_secs(1))
        .build();

    match (policy1.wait, policy2.wait) {
        (
            WaitStrategy::ExponentialJitter {
                initial_delay: delay1,
                ..
            },
            WaitStrategy::ExponentialJitter {
                initial_delay: delay2,
                ..
            },
        ) => {
            assert_eq!(delay1, Duration::from_millis(100));
            assert_eq!(delay2, Duration::from_secs(1));
            assert!(delay2 > delay1);
        }
        _ => panic!("Expected ExponentialJitter strategies"),
    }
}

/// Test wait strategy configurations
#[test]
fn test_wait_strategies() {
    let policy = RetryPolicy::exponential().build();

    match policy.wait {
        WaitStrategy::ExponentialJitter {
            initial_delay,
            multiplier,
            max_delay,
        } => {
            assert!(initial_delay > Duration::from_secs(0));
            assert_eq!(multiplier, 2.0);
            assert!(max_delay > initial_delay);
        }
        _ => panic!("Expected ExponentialJitter strategy"),
    }
}

/// Test stop condition configuration
#[test]
fn test_stop_condition() {
    let policy = RetryPolicy::builder()
        .max_attempts(5)
        .max_delay(Duration::from_secs(120))
        .build();

    assert_eq!(policy.stop.max_attempts, Some(5));
    assert_eq!(policy.stop.max_delay, Some(Duration::from_secs(120)));
}

/// Test policy versioning
#[test]
fn test_policy_versioning() {
    let policy = RetryPolicy::default();
    assert_eq!(policy.version, 1);
}

/// Test complex policy JSON serialization
#[test]
fn test_complex_policy_json() {
    let policy = RetryPolicy::builder()
        .max_attempts(7)
        .initial_delay(Duration::from_millis(250))
        .max_delay(Duration::from_secs(180))
        .retry_on(&["ConnectionTimeout", "ServiceUnavailable", "InternalError"])
        .exclude_errors(&["InvalidAuth", "QuotaExceeded"])
        .build();

    let json = serde_json::to_string_pretty(&policy).unwrap();
    assert!(json.contains("max_attempts"));
    assert!(json.contains("ConnectionTimeout"));
    assert!(json.contains("InvalidAuth"));

    let parsed: RetryPolicy = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.stop.max_attempts, Some(7));
    assert!(parsed
        .retry
        .include_errors
        .contains(&"ServiceUnavailable".to_string()));
    assert!(parsed
        .retry
        .exclude_errors
        .contains(&"QuotaExceeded".to_string()));
}

/// Test retry condition patterns
#[test]
fn test_retry_condition_patterns() {
    // Test empty conditions
    let empty_policy = RetryPolicy::builder()
        .retry_on(&[])
        .exclude_errors(&[])
        .build();
    assert!(empty_policy.retry.include_errors.is_empty());
    assert!(empty_policy.retry.exclude_errors.is_empty());

    // Test single condition
    let single_policy = RetryPolicy::builder().retry_on(&["SingleError"]).build();
    assert_eq!(single_policy.retry.include_errors, vec!["SingleError"]);

    // Test many conditions
    let many_errors = vec![
        "Error1", "Error2", "Error3", "Error4", "Error5", "Error6", "Error7", "Error8", "Error9",
        "Error10",
    ];
    let many_policy = RetryPolicy::builder().retry_on(&many_errors).build();
    assert_eq!(many_policy.retry.include_errors.len(), 10);
}

/// Test wait strategy serialization
#[test]
fn test_wait_strategy_serialization() {
    let policy = RetryPolicy::exponential()
        .initial_delay(Duration::from_millis(123))
        .max_delay(Duration::from_secs(456))
        .build();

    let serialized = serde_json::to_string(&policy).unwrap();
    let deserialized: RetryPolicy = serde_json::from_str(&serialized).unwrap();

    match (policy.wait, deserialized.wait) {
        (
            WaitStrategy::ExponentialJitter {
                initial_delay: orig_initial,
                multiplier: orig_mult,
                max_delay: orig_max,
            },
            WaitStrategy::ExponentialJitter {
                initial_delay: deser_initial,
                multiplier: deser_mult,
                max_delay: deser_max,
            },
        ) => {
            assert_eq!(orig_initial, deser_initial);
            assert_eq!(orig_mult, deser_mult);
            assert_eq!(orig_max, deser_max);
        }
        _ => panic!("Expected ExponentialJitter strategies in both"),
    }
}

/// Test wait strategy debug formatting
#[test]
fn test_wait_strategy_debug() {
    let policy = RetryPolicy::exponential().build();
    let debug_str = format!("{:?}", policy.wait);
    assert!(debug_str.contains("ExponentialJitter"));
    assert!(debug_str.contains("initial_delay"));
    assert!(debug_str.contains("multiplier"));
    assert!(debug_str.contains("max_delay"));
}
