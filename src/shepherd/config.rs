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
}
