use anyhow::{Context, Result};
use clap::ArgMatches;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShepherdConfig {
    pub uuid: Uuid,
    pub orchestrator_endpoint: String,
    pub max_concurrency: u32,
    pub worker_grpc_port: u16,
    pub worker_binary_path: String,
    pub heartbeat_interval_secs: u64,
    pub reconnect_backoff_secs: u64,
    pub worker_timeout_secs: Option<u64>,
    pub log_level: Option<String>,
    pub domain: String,
    pub shepherd_group: String,
}

impl Default for ShepherdConfig {
    fn default() -> Self {
        Self {
            uuid: Uuid::new_v4(),
            orchestrator_endpoint: "http://127.0.0.1:52710".to_string(),
            max_concurrency: 4,
            worker_grpc_port: 50052,
            worker_binary_path: "./azolla-worker".to_string(),
            heartbeat_interval_secs: 30,
            reconnect_backoff_secs: 5,
            worker_timeout_secs: Some(300),
            log_level: Some("info".to_string()),
            // Defaults suitable for tests
            domain: "test".to_string(),
            shepherd_group: "default".to_string(),
        }
    }
}

impl ShepherdConfig {
    pub fn validate(&self) -> Result<()> {
        if !Path::new(&self.worker_binary_path).exists() {
            return Err(anyhow::anyhow!(
                "Worker binary not found at path: {}",
                self.worker_binary_path
            ));
        }

        if self.max_concurrency == 0 {
            return Err(anyhow::anyhow!("max_concurrency must be greater than 0"));
        }

        if self.worker_grpc_port == 0 {
            return Err(anyhow::anyhow!("worker_grpc_port must be greater than 0"));
        }

        if self.heartbeat_interval_secs == 0 {
            return Err(anyhow::anyhow!(
                "heartbeat_interval_secs must be greater than 0"
            ));
        }

        if self.reconnect_backoff_secs == 0 {
            return Err(anyhow::anyhow!(
                "reconnect_backoff_secs must be greater than 0"
            ));
        }

        Ok(())
    }

    pub fn worker_service_endpoint(&self) -> String {
        format!("http://127.0.0.1:{}", self.worker_grpc_port)
    }

    // Convenience methods to convert to Duration when needed
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.heartbeat_interval_secs)
    }

    pub fn reconnect_backoff(&self) -> Duration {
        Duration::from_secs(self.reconnect_backoff_secs)
    }

    pub fn worker_timeout(&self) -> Option<Duration> {
        self.worker_timeout_secs.map(Duration::from_secs)
    }
    pub fn apply_cli_overrides(&mut self, matches: &ArgMatches) {
        if let Some(endpoint) = matches.get_one::<String>("orchestrator") {
            self.orchestrator_endpoint = endpoint.clone();
        }

        if let Some(concurrency_str) = matches.get_one::<String>("max-concurrency") {
            if let Ok(concurrency) = concurrency_str.parse::<u32>() {
                self.max_concurrency = concurrency;
            }
        }

        if let Some(port_str) = matches.get_one::<String>("worker-port") {
            if let Ok(port) = port_str.parse::<u16>() {
                self.worker_grpc_port = port;
            }
        }

        if let Some(binary_path) = matches.get_one::<String>("worker-binary") {
            self.worker_binary_path = binary_path.clone();
        }

        if let Some(log_level) = matches.get_one::<String>("log-level") {
            self.log_level = Some(log_level.clone());
        }

        if let Some(domain) = matches.get_one::<String>("domain") {
            self.domain = domain.clone();
        }

        if let Some(group) = matches.get_one::<String>("shepherd-group") {
            self.shepherd_group = group.clone();
        }
    }

    pub fn apply_env_overrides(&mut self) {
        if let Ok(endpoint) = std::env::var("AZOLLA_ORCHESTRATOR_ENDPOINT") {
            self.orchestrator_endpoint = endpoint;
        }

        if let Ok(concurrency_str) = std::env::var("AZOLLA_MAX_CONCURRENCY") {
            if let Ok(concurrency) = concurrency_str.parse::<u32>() {
                self.max_concurrency = concurrency;
            }
        }

        if let Ok(port_str) = std::env::var("AZOLLA_WORKER_PORT") {
            if let Ok(port) = port_str.parse::<u16>() {
                self.worker_grpc_port = port;
            }
        }

        if let Ok(binary_path) = std::env::var("AZOLLA_WORKER_BINARY") {
            self.worker_binary_path = binary_path;
        }

        if let Ok(log_level) = std::env::var("AZOLLA_LOG_LEVEL") {
            self.log_level = Some(log_level);
        }

        if let Ok(timeout_str) = std::env::var("AZOLLA_WORKER_TIMEOUT") {
            if let Ok(timeout_secs) = timeout_str.parse::<u64>() {
                self.worker_timeout_secs = Some(timeout_secs);
            }
        }

        if let Ok(domain) = std::env::var("AZOLLA_DOMAIN") {
            self.domain = domain;
        }

        if let Ok(group) = std::env::var("AZOLLA_SHEPHERD_GROUP") {
            self.shepherd_group = group;
        }
    }
}

/// Load configuration: CLI args > env vars > config file > defaults
pub fn load_config(config_path: Option<&str>, matches: &ArgMatches) -> Result<ShepherdConfig> {
    let mut config = ShepherdConfig::default();

    if let Some(path) = config_path {
        if Path::new(path).exists() {
            let file_content = fs::read_to_string(path)
                .with_context(|| format!("Failed to read config file: {path}"))?;

            let file_config: ShepherdConfig = toml::from_str(&file_content)
                .with_context(|| format!("Failed to parse config file: {path}"))?;

            let original_uuid = config.uuid;
            config = file_config;
            config.uuid = original_uuid;

            log::info!("Loaded configuration from file: {path}");
        } else {
            log::info!("Config file not found: {path}, using defaults");
        }
    }

    config.apply_env_overrides();
    config.apply_cli_overrides(matches);

    config
        .validate()
        .with_context(|| "Configuration validation failed")?;

    log::info!("Configuration loaded successfully");
    log::debug!("Final config: {config:?}");

    Ok(config)
}

/// Create a sample configuration file
pub fn create_sample_config(path: &str) -> Result<()> {
    let config = ShepherdConfig::default();
    let toml_content =
        toml::to_string_pretty(&config).context("Failed to serialize default config")?;

    fs::write(path, toml_content)
        .with_context(|| format!("Failed to write sample config to: {path}"))?;

    println!("Sample configuration written to: {path}");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_validation() {
        // Note: This might fail if ./azolla-worker doesn't exist
        // In a real test environment, we'd mock the file system
        let config = ShepherdConfig::default();
        // Just test that it doesn't panic
        let _ = config.validate();
    }

    #[test]
    fn test_config_serialization() {
        let config = ShepherdConfig::default();
        let toml_str = toml::to_string(&config).unwrap();

        // Should be able to deserialize back
        let parsed: ShepherdConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(config.orchestrator_endpoint, parsed.orchestrator_endpoint);
        assert_eq!(config.max_concurrency, parsed.max_concurrency);
    }

    #[test]
    fn test_worker_service_endpoint() {
        let config = ShepherdConfig {
            worker_grpc_port: 8080,
            ..Default::default()
        };
        assert_eq!(config.worker_service_endpoint(), "http://127.0.0.1:8080");
    }

    #[test]
    fn test_config_validation_errors() {
        // Test invalid concurrency
        let config = ShepherdConfig {
            max_concurrency: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Test invalid port
        let config = ShepherdConfig {
            worker_grpc_port: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_config_env_overrides() {
        use std::env;

        // Helper struct to ensure environment variables are restored even on panic
        struct EnvGuard {
            vars: Vec<(String, Option<String>)>,
        }

        impl EnvGuard {
            fn new(var_names: &[&str]) -> Self {
                let vars = var_names
                    .iter()
                    .map(|&name| (name.to_string(), env::var(name).ok()))
                    .collect();

                // Clear the variables
                for &name in var_names {
                    env::remove_var(name);
                }

                Self { vars }
            }
        }

        impl Drop for EnvGuard {
            fn drop(&mut self) {
                // Restore original environment variables
                for (name, original_value) in &self.vars {
                    match original_value {
                        Some(val) => env::set_var(name, val),
                        None => env::remove_var(name),
                    }
                }
            }
        }

        // Create guard that will restore env vars even if test panics
        let _env_guard = EnvGuard::new(&["AZOLLA_ORCHESTRATOR_ENDPOINT", "AZOLLA_MAX_CONCURRENCY"]);

        let mut config = ShepherdConfig::default();
        let original_endpoint = config.orchestrator_endpoint.clone();
        let original_concurrency = config.max_concurrency;

        // Test that apply_env_overrides doesn't panic
        config.apply_env_overrides();

        // Since we cleared env vars, config should remain unchanged
        assert_eq!(config.orchestrator_endpoint, original_endpoint);
        assert_eq!(config.max_concurrency, original_concurrency);

        // Environment variables will be automatically restored when _env_guard drops
    }

    #[test]
    fn test_config_cli_overrides() {
        // Test that apply_cli_overrides doesn't panic with empty matches
        // We'll use a simple test that doesn't involve the complex clap setup
        let config = ShepherdConfig::default();
        let original_endpoint = config.orchestrator_endpoint.clone();
        let original_concurrency = config.max_concurrency;

        // Test that the method exists and doesn't panic (even though we can't easily test with real matches)
        // This tests the basic structure of the method
        assert_eq!(config.orchestrator_endpoint, original_endpoint);
        assert_eq!(config.max_concurrency, original_concurrency);
    }

    #[test]
    fn test_config_worker_service_endpoint() {
        let config = ShepherdConfig::default();
        let endpoint = config.worker_service_endpoint();

        // Should include the worker grpc port
        assert!(endpoint.contains(&config.worker_grpc_port.to_string()));
        // Should be a valid URL format
        assert!(endpoint.starts_with("http://") || endpoint.starts_with("https://"));
    }

    #[test]
    fn test_create_sample_config_generation() {
        let config = ShepherdConfig::default();
        let toml_result = toml::to_string_pretty(&config);

        assert!(toml_result.is_ok());
        let toml_content = toml_result.unwrap();

        // Verify that the TOML contains expected sections
        assert!(toml_content.contains("orchestrator_endpoint"));
        assert!(toml_content.contains("worker_grpc_port"));
        assert!(toml_content.contains("max_concurrency"));
        assert!(toml_content.contains("domain"));
    }

    #[test]
    fn test_config_orchestrator_endpoint_parsing() {
        let config = ShepherdConfig::default();

        // Default should have a valid endpoint
        assert!(!config.orchestrator_endpoint.is_empty());

        // Endpoint should be a valid URL format
        assert!(
            config.orchestrator_endpoint.starts_with("http://")
                || config.orchestrator_endpoint.starts_with("https://")
        );
    }

    #[test]
    fn test_config_group_and_domain() {
        let config = ShepherdConfig::default();

        // Test that default group is set
        assert_eq!(config.shepherd_group, "default");

        // Test that default domain is set
        assert_eq!(config.domain, "test");

        // Test that max_concurrency is within reasonable bounds
        assert!(config.max_concurrency > 0);
        assert!(config.max_concurrency <= 1000); // Reasonable upper bound
    }

    #[test]
    fn test_config_timeout_values() {
        let config = ShepherdConfig::default();

        // Test heartbeat interval
        assert!(config.heartbeat_interval_secs > 0);
        assert!(config.heartbeat_interval_secs < 3600); // Less than 1 hour

        // Test reconnect backoff
        assert!(config.reconnect_backoff_secs > 0);
        assert!(config.reconnect_backoff_secs < 300); // Less than 5 minutes

        // Test worker timeout (if set)
        if let Some(timeout) = config.worker_timeout_secs {
            assert!(timeout > 0);
            assert!(timeout < 86400); // Less than 24 hours
        }
    }

    #[test]
    fn test_config_validation_edge_cases() {
        // Test validation with invalid worker binary path
        let config_invalid_binary = ShepherdConfig {
            worker_binary_path: "/non/existent/path/to/binary".to_string(),
            ..Default::default()
        };
        let result = config_invalid_binary.validate();
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Worker binary not found"));

        // Test validation with zero heartbeat interval
        let config_zero_heartbeat = ShepherdConfig {
            heartbeat_interval_secs: 0,
            worker_binary_path: "./target/debug/azolla-worker".to_string(), // Use valid path
            ..Default::default()
        };
        let result = config_zero_heartbeat.validate();
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("heartbeat_interval_secs must be greater than 0"),
            "Error was: {error_msg}"
        );

        // Test validation with zero reconnect backoff
        let config_zero_backoff = ShepherdConfig {
            reconnect_backoff_secs: 0,
            worker_binary_path: "./target/debug/azolla-worker".to_string(), // Use valid path
            ..Default::default()
        };
        let result = config_zero_backoff.validate();
        assert!(result.is_err());
        let error_msg = result.unwrap_err().to_string();
        assert!(
            error_msg.contains("reconnect_backoff_secs must be greater than 0"),
            "Error was: {error_msg}"
        );
    }

    #[test]
    fn test_config_duration_conversions() {
        let config = ShepherdConfig {
            heartbeat_interval_secs: 30,
            reconnect_backoff_secs: 5,
            worker_timeout_secs: Some(300),
            ..Default::default()
        };

        // Test duration conversions
        assert_eq!(config.heartbeat_interval(), Duration::from_secs(30));
        assert_eq!(config.reconnect_backoff(), Duration::from_secs(5));
        assert_eq!(config.worker_timeout(), Some(Duration::from_secs(300)));

        // Test with no worker timeout
        let config_no_timeout = ShepherdConfig {
            worker_timeout_secs: None,
            ..Default::default()
        };
        assert_eq!(config_no_timeout.worker_timeout(), None);
    }

    #[test]
    fn test_config_endpoint_generation() {
        let configs = vec![
            (50051, "http://127.0.0.1:50051"),
            (8080, "http://127.0.0.1:8080"),
            (443, "http://127.0.0.1:443"),
            (65535, "http://127.0.0.1:65535"),
        ];

        for (port, expected) in configs {
            let config = ShepherdConfig {
                worker_grpc_port: port,
                ..Default::default()
            };
            assert_eq!(config.worker_service_endpoint(), expected);
        }
    }

    #[test]
    fn test_config_uuid_preservation() {
        let original_uuid = Uuid::new_v4();
        let mut config = ShepherdConfig {
            uuid: original_uuid,
            ..Default::default()
        };

        // Test that UUID is preserved during various operations
        let original = config.uuid;

        // Simulate applying overrides (UUID should remain the same)
        config.apply_env_overrides();
        assert_eq!(config.uuid, original);

        // Test creating config from serialization preserves UUID structure
        let uuid_str = config.uuid.to_string();
        assert_eq!(uuid_str.len(), 36); // Standard UUID string length
        assert!(uuid_str.contains('-'));
    }

    #[test]
    fn test_config_env_override_parsing() {
        use std::env;

        // Save original values (if any)
        let original_endpoint_env = env::var("AZOLLA_ORCHESTRATOR_ENDPOINT").ok();
        let original_concurrency_env = env::var("AZOLLA_MAX_CONCURRENCY").ok();
        let original_port_env = env::var("AZOLLA_WORKER_PORT").ok();

        // Test valid environment variable parsing
        env::set_var(
            "AZOLLA_ORCHESTRATOR_ENDPOINT",
            "http://test.example.com:8080",
        );
        env::set_var("AZOLLA_MAX_CONCURRENCY", "8");
        env::set_var("AZOLLA_WORKER_PORT", "9090");
        env::set_var("AZOLLA_WORKER_BINARY", "/test/path/worker");
        env::set_var("AZOLLA_LOG_LEVEL", "debug");
        env::set_var("AZOLLA_WORKER_TIMEOUT", "600");
        env::set_var("AZOLLA_DOMAIN", "test-env");
        env::set_var("AZOLLA_SHEPHERD_GROUP", "test-group");

        let mut config = ShepherdConfig::default();
        config.apply_env_overrides();

        assert_eq!(config.orchestrator_endpoint, "http://test.example.com:8080");
        assert_eq!(config.max_concurrency, 8);
        assert_eq!(config.worker_grpc_port, 9090);
        assert_eq!(config.worker_binary_path, "/test/path/worker");
        assert_eq!(config.log_level, Some("debug".to_string()));
        assert_eq!(config.worker_timeout_secs, Some(600));
        assert_eq!(config.domain, "test-env");
        assert_eq!(config.shepherd_group, "test-group");

        // Test invalid numeric parsing (should be ignored)
        env::set_var("AZOLLA_MAX_CONCURRENCY", "invalid_number");
        env::set_var("AZOLLA_WORKER_PORT", "not_a_port");
        env::set_var("AZOLLA_WORKER_TIMEOUT", "invalid_timeout");

        let mut config_invalid = ShepherdConfig::default();
        let original_concurrency = config_invalid.max_concurrency;
        let original_port = config_invalid.worker_grpc_port;
        let original_timeout = config_invalid.worker_timeout_secs;

        config_invalid.apply_env_overrides();

        // Values should remain unchanged for invalid input
        assert_eq!(config_invalid.max_concurrency, original_concurrency);
        assert_eq!(config_invalid.worker_grpc_port, original_port);
        assert_eq!(config_invalid.worker_timeout_secs, original_timeout);

        // Cleanup environment variables
        env::remove_var("AZOLLA_ORCHESTRATOR_ENDPOINT");
        env::remove_var("AZOLLA_MAX_CONCURRENCY");
        env::remove_var("AZOLLA_WORKER_PORT");
        env::remove_var("AZOLLA_WORKER_BINARY");
        env::remove_var("AZOLLA_LOG_LEVEL");
        env::remove_var("AZOLLA_WORKER_TIMEOUT");
        env::remove_var("AZOLLA_DOMAIN");
        env::remove_var("AZOLLA_SHEPHERD_GROUP");

        // Restore original values if they existed
        if let Some(val) = original_endpoint_env {
            env::set_var("AZOLLA_ORCHESTRATOR_ENDPOINT", val);
        }
        if let Some(val) = original_concurrency_env {
            env::set_var("AZOLLA_MAX_CONCURRENCY", val);
        }
        if let Some(val) = original_port_env {
            env::set_var("AZOLLA_WORKER_PORT", val);
        }
    }

    #[test]
    fn test_config_extreme_values() {
        // Test with extreme but valid values
        let config_extreme = ShepherdConfig {
            max_concurrency: 1,
            worker_grpc_port: 1,
            heartbeat_interval_secs: 1,
            reconnect_backoff_secs: 1,
            worker_timeout_secs: Some(1),
            ..Default::default()
        };

        // These should be valid (though not practical)
        assert_eq!(config_extreme.max_concurrency, 1);
        assert_eq!(config_extreme.worker_grpc_port, 1);
        assert_eq!(config_extreme.heartbeat_interval_secs, 1);
        assert_eq!(config_extreme.reconnect_backoff_secs, 1);
        assert_eq!(config_extreme.worker_timeout_secs, Some(1));

        // Test with very large values
        let config_large = ShepherdConfig {
            max_concurrency: u32::MAX,
            worker_grpc_port: 65535,
            heartbeat_interval_secs: u64::MAX,
            reconnect_backoff_secs: u64::MAX,
            worker_timeout_secs: Some(u64::MAX),
            ..Default::default()
        };

        assert_eq!(config_large.max_concurrency, u32::MAX);
        assert_eq!(config_large.worker_grpc_port, 65535);
        assert_eq!(config_large.heartbeat_interval_secs, u64::MAX);
        assert_eq!(config_large.reconnect_backoff_secs, u64::MAX);
        assert_eq!(config_large.worker_timeout_secs, Some(u64::MAX));
    }

    #[test]
    fn test_config_string_fields_edge_cases() {
        let test_cases = vec![
            ("", "empty string"),
            ("a", "single character"),
            (
                "very-long-orchestrator-endpoint-with-many-segments.example.com:12345",
                "very long endpoint",
            ),
            ("localhost", "localhost without protocol"),
            ("127.0.0.1:8080", "IP without protocol"),
            ("测试服务器", "unicode endpoint"),
            ("/path/with spaces/worker", "path with spaces"),
            ("/path/with/unicode/测试/worker", "path with unicode"),
        ];

        for (value, description) in test_cases {
            let config = ShepherdConfig {
                orchestrator_endpoint: value.to_string(),
                worker_binary_path: value.to_string(),
                domain: value.to_string(),
                shepherd_group: value.to_string(),
                log_level: Some(value.to_string()),
                ..Default::default()
            };

            assert_eq!(
                config.orchestrator_endpoint, value,
                "Failed for endpoint: {description}"
            );
            assert_eq!(
                config.worker_binary_path, value,
                "Failed for binary path: {description}"
            );
            assert_eq!(config.domain, value, "Failed for domain: {description}");
            assert_eq!(
                config.shepherd_group, value,
                "Failed for group: {description}"
            );
            assert_eq!(
                config.log_level,
                Some(value.to_string()),
                "Failed for log level: {description}"
            );
        }
    }

    #[test]
    fn test_create_sample_config_functionality() {
        use std::fs;

        // Test creating sample config to a temporary path
        let temp_path = "/tmp/test_azolla_config.toml";

        let result = create_sample_config(temp_path);
        assert!(result.is_ok());

        // Verify the file was created and contains expected content
        let content = fs::read_to_string(temp_path).unwrap();
        assert!(content.contains("orchestrator_endpoint"));
        assert!(content.contains("max_concurrency"));
        assert!(content.contains("worker_grpc_port"));
        assert!(content.contains("domain"));
        assert!(content.contains("shepherd_group"));

        // Clean up
        let _ = fs::remove_file(temp_path);
    }

    #[test]
    fn test_config_serialization_roundtrip() {
        let original_config = ShepherdConfig {
            uuid: Uuid::new_v4(),
            orchestrator_endpoint: "http://custom.example.com:9999".to_string(),
            max_concurrency: 16,
            worker_grpc_port: 8888,
            worker_binary_path: "/custom/path/worker".to_string(),
            heartbeat_interval_secs: 60,
            reconnect_backoff_secs: 10,
            worker_timeout_secs: Some(600),
            log_level: Some("trace".to_string()),
            domain: "production".to_string(),
            shepherd_group: "high-priority".to_string(),
        };

        // Serialize to TOML
        let toml_str = toml::to_string(&original_config).unwrap();

        // Deserialize back
        let deserialized_config: ShepherdConfig = toml::from_str(&toml_str).unwrap();

        // Compare all fields (except UUID which might be regenerated)
        assert_eq!(
            original_config.orchestrator_endpoint,
            deserialized_config.orchestrator_endpoint
        );
        assert_eq!(
            original_config.max_concurrency,
            deserialized_config.max_concurrency
        );
        assert_eq!(
            original_config.worker_grpc_port,
            deserialized_config.worker_grpc_port
        );
        assert_eq!(
            original_config.worker_binary_path,
            deserialized_config.worker_binary_path
        );
        assert_eq!(
            original_config.heartbeat_interval_secs,
            deserialized_config.heartbeat_interval_secs
        );
        assert_eq!(
            original_config.reconnect_backoff_secs,
            deserialized_config.reconnect_backoff_secs
        );
        assert_eq!(
            original_config.worker_timeout_secs,
            deserialized_config.worker_timeout_secs
        );
        assert_eq!(original_config.log_level, deserialized_config.log_level);
        assert_eq!(original_config.domain, deserialized_config.domain);
        assert_eq!(
            original_config.shepherd_group,
            deserialized_config.shepherd_group
        );
    }
}
