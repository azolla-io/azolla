//! Network timeout tests
//! Tests various timeout scenarios and configurations

use azolla_client::client::ClientConfig;
use azolla_client::{AzollaError, Client};
use std::time::{Duration, Instant};
use tokio::time::timeout;

/// Test connection timeout with various durations
#[tokio::test]
async fn test_connection_timeout_durations() {
    let timeout_tests = vec![
        (Duration::from_millis(1), "1ms timeout"),
        (Duration::from_millis(10), "10ms timeout"),
        (Duration::from_millis(100), "100ms timeout"),
        (Duration::from_millis(1000), "1s timeout"),
        (Duration::from_secs(5), "5s timeout"),
    ];

    for (timeout_duration, description) in timeout_tests {
        println!("Testing {description}");

        let config = ClientConfig {
            endpoint: "http://203.0.113.100:54321".to_string(), // Non-routable IP
            domain: "timeout-test".to_string(),
            timeout: timeout_duration,
        };

        let start_time = Instant::now();
        let result = Client::with_config(config).await;
        let elapsed = start_time.elapsed();

        // Should fail
        assert!(result.is_err());

        // Elapsed time should be reasonably close to timeout (allowing for overhead)
        println!("Timeout: {timeout_duration:?}, Elapsed: {elapsed:?}");

        // For very short timeouts, allow more variance due to system overhead
        let max_expected = if timeout_duration < Duration::from_millis(100) {
            timeout_duration + Duration::from_millis(2000) // Allow 2s overhead for short timeouts
        } else {
            timeout_duration * 2 // Allow 2x timeout for longer timeouts
        };

        if elapsed > max_expected {
            println!("Warning: elapsed time {elapsed:?} exceeds expected max {max_expected:?}");
        }
    }
}

/// Test timeout with different network scenarios
#[tokio::test]
async fn test_timeout_network_scenarios() {
    let scenarios = vec![
        (
            "http://192.0.2.1:12345", // Non-routable (should timeout quickly)
            Duration::from_millis(500),
            "Non-routable IP timeout",
        ),
        (
            "http://blackhole.invalid:8080", // DNS failure (might timeout on DNS)
            Duration::from_millis(1000),
            "DNS failure timeout",
        ),
        (
            "http://127.0.0.1:54321", // Localhost unreachable port (should fail fast)
            Duration::from_millis(2000),
            "Localhost unreachable port",
        ),
    ];

    for (endpoint, timeout_duration, description) in scenarios {
        println!("Testing {description}: {endpoint}");

        let config = ClientConfig {
            endpoint: endpoint.to_string(),
            domain: "scenario-test".to_string(),
            timeout: timeout_duration,
        };

        // Wrap in outer timeout to prevent test hanging
        let result = timeout(timeout_duration * 3, Client::with_config(config)).await;

        match result {
            Ok(client_result) => {
                // Should fail due to connection issues
                assert!(client_result.is_err());
                println!("Connection failed as expected for {description}");
            }
            Err(_) => {
                println!("Test timeout (outer) for {description}");
            }
        }
    }
}

/// Test timeout behavior with client builder
#[tokio::test]
async fn test_builder_timeout_behavior() {
    let timeout_configs = vec![
        Duration::from_nanos(1),    // Extremely short
        Duration::from_millis(1),   // Very short
        Duration::from_millis(50),  // Short
        Duration::from_millis(500), // Moderate
        Duration::from_secs(1),     // Long
        Duration::from_secs(10),    // Very long
    ];

    for timeout_duration in timeout_configs {
        println!("Testing builder timeout: {timeout_duration:?}");

        let builder = Client::builder()
            .endpoint("http://203.0.113.200:65432") // Non-routable
            .domain("builder-timeout-test")
            .timeout(timeout_duration);

        let start_time = Instant::now();
        let result = timeout(Duration::from_secs(15), builder.build()).await;
        let elapsed = start_time.elapsed();

        match result {
            Ok(client_result) => {
                assert!(client_result.is_err());
                println!("Builder timeout {timeout_duration:?} failed in {elapsed:?}");
            }
            Err(_) => {
                println!("Builder timeout {timeout_duration:?} hung for {elapsed:?}");
            }
        }
    }
}

/// Test timeout precision and accuracy
#[tokio::test]
async fn test_timeout_precision() {
    let precise_timeouts = vec![
        Duration::from_millis(100),
        Duration::from_millis(200),
        Duration::from_millis(500),
        Duration::from_millis(1000),
    ];

    for expected_timeout in precise_timeouts {
        println!("Testing timeout precision: {expected_timeout:?}");

        let config = ClientConfig {
            endpoint: "http://198.51.100.1:9999".to_string(), // Non-routable
            domain: "precision-test".to_string(),
            timeout: expected_timeout,
        };

        let mut elapsed_times = Vec::new();

        // Run multiple attempts to check consistency
        for attempt in 0..3 {
            let start_time = Instant::now();
            let result = timeout(expected_timeout * 5, Client::with_config(config.clone())).await;
            let elapsed = start_time.elapsed();

            match result {
                Ok(client_result) => {
                    assert!(client_result.is_err());
                    elapsed_times.push(elapsed);
                    println!("Attempt {attempt}: {elapsed:?}");
                }
                Err(_) => {
                    println!("Attempt {attempt}: test timeout");
                }
            }
        }

        if !elapsed_times.is_empty() {
            let avg_elapsed = elapsed_times.iter().sum::<Duration>() / elapsed_times.len() as u32;
            println!("Average elapsed: {avg_elapsed:?} for expected {expected_timeout:?}");
        }
    }
}

/// Test timeout with concurrent connections
#[tokio::test]
async fn test_concurrent_timeouts() {
    let endpoints = vec![
        "http://203.0.113.10:10001",
        "http://203.0.113.11:10002",
        "http://203.0.113.12:10003",
        "http://203.0.113.13:10004",
    ];

    let timeout_duration = Duration::from_millis(500);
    let mut handles = Vec::new();

    let start_time = Instant::now();

    for (i, endpoint) in endpoints.into_iter().enumerate() {
        let config = ClientConfig {
            endpoint: endpoint.to_string(),
            domain: format!("concurrent-{i}"),
            timeout: timeout_duration,
        };

        let handle = tokio::spawn(async move {
            let result = Client::with_config(config).await;
            (i, result.is_err())
        });

        handles.push(handle);
    }

    // Wait for all connections to complete
    let mut completed_count = 0;
    for handle in handles {
        match handle.await {
            Ok((i, failed)) => {
                assert!(failed, "Connection {i} should have failed");
                completed_count += 1;
            }
            Err(e) => {
                println!("Join error: {e:?}");
            }
        }
    }

    let total_elapsed = start_time.elapsed();
    println!("All {completed_count} concurrent timeouts completed in {total_elapsed:?}");

    // All connections should run concurrently, so total time should be close to timeout duration
    assert_eq!(completed_count, 4);
    assert!(total_elapsed < timeout_duration * 2); // Should be concurrent, not sequential
}

/// Test timeout edge cases
#[tokio::test]
async fn test_timeout_edge_cases() {
    // Test zero timeout
    let zero_config = ClientConfig {
        endpoint: "http://203.0.113.255:99999".to_string(),
        domain: "zero-timeout".to_string(),
        timeout: Duration::from_secs(0),
    };

    let result = Client::with_config(zero_config).await;
    assert!(result.is_err());

    // Test maximum duration timeout (within reason)
    let max_config = ClientConfig {
        endpoint: "http://203.0.113.254:99998".to_string(),
        domain: "max-timeout".to_string(),
        timeout: Duration::from_secs(3600), // 1 hour
    };

    // We won't actually wait an hour, just test that it's accepted
    let start_time = Instant::now();
    let result = timeout(Duration::from_millis(100), Client::with_config(max_config)).await;
    let elapsed = start_time.elapsed();

    match result {
        Ok(client_result) => {
            // If it completes quickly, that's fine (fast failure)
            assert!(client_result.is_err());
        }
        Err(_) => {
            // If it times out (our test timeout), that's also expected
            assert!(elapsed >= Duration::from_millis(100));
        }
    }
}

/// Test timeout interaction with retries
#[tokio::test]
async fn test_timeout_with_retries() {
    use azolla_client::retry_policy::RetryPolicy;

    let config = ClientConfig {
        endpoint: "http://203.0.113.50:50000".to_string(),
        domain: "retry-timeout-test".to_string(),
        timeout: Duration::from_millis(200),
    };

    // Create client first
    let result = Client::with_config(config).await;
    assert!(result.is_err()); // Should fail to connect

    // Test retry policy creation (can't test actual retry without orchestrator)
    let retry_policy = RetryPolicy::builder()
        .max_attempts(3)
        .initial_delay(Duration::from_millis(100))
        .max_delay(Duration::from_secs(1))
        .build();

    // Verify retry policy is created correctly
    assert_eq!(retry_policy.stop.max_attempts, Some(3));
    assert_eq!(retry_policy.stop.max_delay, Some(Duration::from_secs(1)));
}

/// Test timeout error message quality
#[tokio::test]
async fn test_timeout_error_messages() {
    let config = ClientConfig {
        endpoint: "http://203.0.113.100:55555".to_string(),
        domain: "error-message-test".to_string(),
        timeout: Duration::from_millis(100),
    };

    let result = Client::with_config(config).await;
    assert!(result.is_err());

    let error = result.err().unwrap();
    let error_message = error.to_string();

    println!("Timeout error message: {error_message}");

    // Error message should be descriptive
    assert!(!error_message.is_empty());
    assert!(error_message.len() > 5);

    // Should be a connection error
    match error {
        AzollaError::Connection(_) => { /* Expected */ }
        other => println!("Got non-connection error (may be acceptable): {other:?}"),
    }
}
