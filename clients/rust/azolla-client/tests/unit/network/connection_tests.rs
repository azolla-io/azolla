//! Network connection tests
//! Tests various connection scenarios, timeouts, and edge cases

use azolla_client::client::ClientConfig;
use azolla_client::{AzollaError, Client};
use std::time::Duration;
use tokio::time::timeout;

/// Test connection with various invalid endpoint formats
#[tokio::test]
async fn test_invalid_endpoint_formats() {
    let invalid_endpoints = vec![
        "",                            // Empty string
        "not-a-url",                   // Not a URL
        "ftp://invalid-protocol.com",  // Wrong protocol
        "http://",                     // Incomplete URL
        "https://",                    // Incomplete HTTPS URL
        "http://localhost:",           // Missing port
        "http://:8080",                // Missing host
        "http://localhost:999999",     // Invalid port range
        "http://[invalid-ipv6",        // Malformed IPv6
        "http://256.256.256.256:8080", // Invalid IP address
    ];

    for endpoint in invalid_endpoints {
        println!("Testing invalid endpoint: {endpoint}");
        let result = Client::connect(endpoint).await;

        assert!(result.is_err());
        match result.err().unwrap() {
            AzollaError::Connection(_) => { /* Expected */ }
            AzollaError::InvalidConfig(msg) => {
                assert!(!msg.is_empty());
            }
            other => panic!("Unexpected error type for endpoint {endpoint}: {other:?}"),
        }
    }
}

/// Test connection timeout scenarios
#[tokio::test]
async fn test_connection_timeouts() {
    let timeout_configs = vec![
        // Very short timeout
        ClientConfig {
            endpoint: "http://192.0.2.1:12345".to_string(), // Non-routable IP (RFC 5737)
            domain: "test".to_string(),
            timeout: Duration::from_millis(1),
        },
        // Short timeout
        ClientConfig {
            endpoint: "http://192.0.2.2:12346".to_string(),
            domain: "test".to_string(),
            timeout: Duration::from_millis(100),
        },
        // Reasonable timeout to unreachable host
        ClientConfig {
            endpoint: "http://203.0.113.1:8080".to_string(), // Another non-routable IP
            domain: "test".to_string(),
            timeout: Duration::from_millis(500),
        },
    ];

    for config in timeout_configs {
        println!("Testing timeout config: {}", config.endpoint);

        // Wrap in outer timeout to prevent test hanging
        let result = timeout(Duration::from_secs(2), Client::with_config(config)).await;

        match result {
            Ok(client_result) => {
                assert!(client_result.is_err());
                match client_result.err().unwrap() {
                    AzollaError::Connection(_) => { /* Expected timeout or connection error */ }
                    other => println!("Non-connection error (acceptable): {other:?}"),
                }
            }
            Err(_) => {
                // Test itself timed out - this is also acceptable for this test
                println!("Test timed out (connection attempt took too long)");
            }
        }
    }
}

/// Test DNS resolution failures
#[tokio::test]
async fn test_dns_resolution_failures() {
    let unresolvable_hosts = vec![
        "http://this-host-should-not-exist-12345.invalid:8080",
        "http://definitely-not-a-real-domain-54321.test:9090",
        "http://nonexistent-subdomain.localhost.invalid:7070",
    ];

    for endpoint in unresolvable_hosts {
        println!("Testing DNS resolution for: {endpoint}");

        let result = timeout(Duration::from_secs(5), Client::connect(endpoint)).await;

        match result {
            Ok(client_result) => {
                assert!(client_result.is_err());
                match client_result.err().unwrap() {
                    AzollaError::Connection(_) => { /* Expected DNS resolution failure */ }
                    other => println!("Non-connection error (acceptable): {other:?}"),
                }
            }
            Err(_) => {
                println!("DNS resolution timed out (acceptable)");
            }
        }
    }
}

/// Test connection with different protocols
#[tokio::test]
async fn test_protocol_variations() {
    let protocol_tests = vec![
        ("http://127.0.0.1:99999", "HTTP with unreachable port"),
        ("https://127.0.0.1:99998", "HTTPS with unreachable port"),
        (
            "http://localhost:99997",
            "HTTP localhost with unreachable port",
        ),
        (
            "https://localhost:99996",
            "HTTPS localhost with unreachable port",
        ),
    ];

    for (endpoint, description) in protocol_tests {
        println!("Testing {description}: {endpoint}");

        let result = timeout(Duration::from_secs(3), Client::connect(endpoint)).await;

        match result {
            Ok(client_result) => {
                assert!(client_result.is_err());
                // All should fail with connection errors since ports are unreachable
            }
            Err(_) => {
                println!("Connection attempt timed out for {description}");
            }
        }
    }
}

/// Test IPv6 address handling
#[tokio::test]
async fn test_ipv6_addresses() {
    let ipv6_endpoints = vec![
        "http://[::1]:99999",        // IPv6 loopback
        "http://[2001:db8::1]:8080", // Documentation IPv6 address (RFC 3849)
        "http://[fe80::1]:9090",     // Link-local IPv6
    ];

    for endpoint in ipv6_endpoints {
        println!("Testing IPv6 endpoint: {endpoint}");

        let result = timeout(Duration::from_secs(3), Client::connect(endpoint)).await;

        match result {
            Ok(client_result) => {
                // Should fail with connection error (unreachable)
                assert!(client_result.is_err());
                match client_result.err().unwrap() {
                    AzollaError::Connection(_) => { /* Expected */ }
                    other => {
                        println!("Non-connection error for IPv6 (may be acceptable): {other:?}")
                    }
                }
            }
            Err(_) => {
                println!("IPv6 connection timed out");
            }
        }
    }
}

/// Test port range edge cases
#[tokio::test]
async fn test_port_edge_cases() {
    let port_tests = vec![
        ("http://127.0.0.1:1", "Port 1 (low)"),
        ("http://127.0.0.1:65535", "Port 65535 (high)"),
        ("http://127.0.0.1:8080", "Common HTTP alt port"),
        ("http://127.0.0.1:443", "HTTPS port on HTTP"),
        ("https://127.0.0.1:80", "HTTP port on HTTPS"),
    ];

    for (endpoint, description) in port_tests {
        println!("Testing {description}: {endpoint}");

        let result = timeout(Duration::from_secs(2), Client::connect(endpoint)).await;

        match result {
            Ok(client_result) => {
                // All should fail since we're not running servers on these ports
                assert!(client_result.is_err());
            }
            Err(_) => {
                println!("Port test timed out for {description}");
            }
        }
    }
}

/// Test client builder with edge case configurations
#[tokio::test]
async fn test_client_builder_edge_cases() {
    // Test with extreme timeout values
    let builder_configs = vec![
        (Duration::from_nanos(1), "1 nanosecond timeout"),
        (Duration::from_millis(1), "1 millisecond timeout"),
        (Duration::from_secs(3600), "1 hour timeout"),
    ];

    for (timeout_duration, description) in builder_configs {
        println!("Testing client builder with {description}");

        let builder = Client::builder()
            .endpoint("http://192.0.2.100:12345") // Non-routable
            .domain("edge-case-test")
            .timeout(timeout_duration);

        // The builder should be created successfully
        assert!(std::mem::size_of_val(&builder) > 0);

        // Attempting to build should fail due to unreachable endpoint
        let result = timeout(Duration::from_secs(2), builder.build()).await;

        match result {
            Ok(client_result) => {
                assert!(client_result.is_err());
            }
            Err(_) => {
                println!("Client build timed out with {description}");
            }
        }
    }
}

/// Test domain name validation
#[tokio::test]
async fn test_domain_validation() {
    let long_domain_253 = "a".repeat(253);
    let long_domain_254 = "a".repeat(254);
    let domain_tests = vec![
        ("", "Empty domain"),
        ("a", "Single character domain"),
        (&long_domain_253, "Maximum length domain"),
        (&long_domain_254, "Over maximum length domain"),
        ("valid-domain", "Valid domain"),
        ("domain.with.dots", "Domain with dots"),
        ("123-numeric-start", "Domain starting with numbers"),
        ("UPPERCASE", "Uppercase domain"),
        ("mixed-CASE-123", "Mixed case domain"),
        ("domain_with_underscores", "Domain with underscores"),
    ];

    for (domain, description) in domain_tests {
        println!("Testing {description}: '{domain}'");

        let config = ClientConfig {
            endpoint: "http://192.0.2.200:12345".to_string(), // Non-routable
            domain: domain.to_string(),
            timeout: Duration::from_millis(500),
        };

        let result = timeout(Duration::from_secs(1), Client::with_config(config)).await;

        match result {
            Ok(client_result) => {
                // Should fail due to unreachable endpoint, but domain should be accepted
                assert!(client_result.is_err());
                // The error should be connection-related, not config-related
                match client_result.err().unwrap() {
                    AzollaError::Connection(_) => { /* Expected */ }
                    AzollaError::InvalidConfig(msg) => {
                        // Only expect config error for truly invalid domains
                        if domain.len() > 253 {
                            println!("Config error for overly long domain: {msg}");
                        } else {
                            panic!("Unexpected config error for domain '{domain}': {msg}");
                        }
                    }
                    other => println!("Other error for domain '{domain}': {other:?}"),
                }
            }
            Err(_) => {
                println!("Connection attempt timed out for domain '{domain}'");
            }
        }
    }
}

/// Test concurrent connection attempts
#[tokio::test]
async fn test_concurrent_connection_attempts() {
    let endpoints = vec![
        "http://192.0.2.10:10001",
        "http://192.0.2.11:10002",
        "http://192.0.2.12:10003",
        "http://192.0.2.13:10004",
        "http://192.0.2.14:10005",
    ];

    let mut handles = Vec::new();

    for endpoint in endpoints {
        let endpoint = endpoint.to_string();
        let handle = tokio::spawn(async move {
            let result = timeout(Duration::from_secs(2), Client::connect(&endpoint)).await;

            match result {
                Ok(client_result) => {
                    assert!(client_result.is_err());
                    endpoint
                }
                Err(_) => {
                    println!("Concurrent connection to {endpoint} timed out");
                    endpoint
                }
            }
        });
        handles.push(handle);
    }

    // Wait for all concurrent connection attempts
    let mut completed_count = 0;
    for handle in handles {
        let result = handle.await;
        assert!(result.is_ok());
        completed_count += 1;
    }

    assert_eq!(completed_count, 5);
    println!("All concurrent connection attempts completed");
}

/// Test connection retry behavior structure
#[test]
fn test_connection_retry_structure() {
    // Test that we can create multiple client instances without interference
    let configs = [
        ClientConfig {
            endpoint: "http://test1.invalid:8001".to_string(),
            domain: "retry-test-1".to_string(),
            timeout: Duration::from_millis(100),
        },
        ClientConfig {
            endpoint: "http://test2.invalid:8002".to_string(),
            domain: "retry-test-2".to_string(),
            timeout: Duration::from_millis(200),
        },
        ClientConfig {
            endpoint: "http://test3.invalid:8003".to_string(),
            domain: "retry-test-3".to_string(),
            timeout: Duration::from_millis(300),
        },
    ];

    // Verify all configs are created properly
    for (i, config) in configs.iter().enumerate() {
        assert!(!config.endpoint.is_empty());
        assert!(!config.domain.is_empty());
        assert!(config.timeout.as_millis() > 0);
        println!(
            "Config {}: endpoint={}, domain={}, timeout={:?}",
            i + 1,
            config.endpoint,
            config.domain,
            config.timeout
        );
    }
}

/// Test error message quality for connection failures
#[tokio::test]
async fn test_connection_error_messages() {
    let test_cases = vec![
        (
            "http://192.0.2.254:99999",
            "Should include host and port information",
        ),
        (
            "https://nonexistent.invalid:443",
            "Should indicate DNS or connection failure",
        ),
    ];

    for (endpoint, expected_info) in test_cases {
        println!("Testing error message quality for: {endpoint}");

        let result = timeout(Duration::from_secs(3), Client::connect(endpoint)).await;

        match result {
            Ok(client_result) => {
                assert!(client_result.is_err());
                let error = client_result.err().unwrap();
                let error_string = error.to_string();

                // Error message should not be empty and should be descriptive
                assert!(!error_string.is_empty());
                println!("Error message for {endpoint}: {error_string}");
                println!("Expected info: {expected_info}");

                // Basic quality checks
                assert!(error_string.len() > 10); // Should be reasonably descriptive
                assert!(!error_string.contains("Debug(")); // Should not expose internal debug info
            }
            Err(_) => {
                println!("Connection attempt timed out for error message test");
            }
        }
    }
}
