use rand::Rng;
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    #[serde(default = "default_version")]
    pub version: u8,
    #[serde(default)]
    pub stop: StopConfig,
    #[serde(default)]
    pub wait: WaitConfig,
    #[serde(default)]
    pub retry: RetryConfig,
}

fn default_version() -> u8 {
    1
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StopConfig {
    pub max_attempts: Option<u32>, // None = infinite
    pub max_delay: Option<f64>,    // seconds, None = no time limit
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WaitConfig {
    #[serde(default = "default_exponential_jitter")]
    pub strategy: WaitStrategy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub delay: Option<f64>, // for fixed strategy
    #[serde(default = "default_initial_delay")]
    pub initial_delay: f64,
    #[serde(default = "default_multiplier")]
    pub multiplier: f64,
    #[serde(default = "default_max_delay")]
    pub max_delay: f64,
    #[serde(default = "default_full_jitter")]
    pub jitter: JitterType,
}

fn default_exponential_jitter() -> WaitStrategy {
    WaitStrategy::ExponentialJitter
}

fn default_initial_delay() -> f64 {
    1.0
}

fn default_multiplier() -> f64 {
    2.0
}

fn default_max_delay() -> f64 {
    300.0
}

fn default_full_jitter() -> JitterType {
    JitterType::Full
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitStrategy {
    Fixed,
    Exponential,
    ExponentialJitter,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JitterType {
    None,
    Full,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RetryConfig {
    #[serde(default = "default_include_errors")]
    pub include_errors: Vec<String>,
    #[serde(default)]
    pub exclude_errors: Vec<String>,
}

fn default_include_errors() -> Vec<String> {
    vec!["ValueError".to_string()]
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            version: 1,
            stop: StopConfig::default(),
            wait: WaitConfig::default(),
            retry: RetryConfig::default(),
        }
    }
}

impl Default for StopConfig {
    fn default() -> Self {
        Self {
            max_attempts: Some(5),
            max_delay: None,
        }
    }
}

impl Default for WaitConfig {
    fn default() -> Self {
        Self {
            strategy: WaitStrategy::ExponentialJitter,
            delay: None,
            initial_delay: 1.0,
            multiplier: 2.0,
            max_delay: 300.0,
            jitter: JitterType::Full,
        }
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            include_errors: vec!["ValueError".to_string()],
            exclude_errors: vec![],
        }
    }
}

impl WaitConfig {
    pub fn calculate_delay(&self, attempt_number: u32) -> Duration {
        let delay_secs = match self.strategy {
            WaitStrategy::Fixed => self.delay.unwrap_or(self.initial_delay),
            WaitStrategy::Exponential | WaitStrategy::ExponentialJitter => {
                let base_delay = self.initial_delay * self.multiplier.powi(attempt_number as i32);
                let capped_delay = base_delay.min(self.max_delay);

                if self.strategy == WaitStrategy::ExponentialJitter
                    && self.jitter == JitterType::Full
                {
                    let mut rng = rand::thread_rng();
                    rng.gen_range(0.0..=capped_delay)
                } else {
                    capped_delay
                }
            }
        };

        Duration::from_secs_f64(delay_secs)
    }
}

impl RetryPolicy {
    pub fn from_json(json: &serde_json::Value) -> Result<Self, String> {
        serde_json::from_value(json.clone()).map_err(|e| format!("Invalid retry policy: {e}"))
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.version != 1 {
            return Err("Unsupported retry policy version".to_string());
        }

        if let Some(max_attempts) = self.stop.max_attempts {
            if max_attempts == 0 {
                return Err("max_attempts must be >= 1".to_string());
            }
        }

        if self.wait.multiplier < 1.0 {
            return Err("multiplier must be >= 1".to_string());
        }

        if self.wait.initial_delay < 0.0 || self.wait.max_delay < 0.0 {
            return Err("delay values must be >= 0".to_string());
        }

        if let Some(delay) = self.wait.delay {
            if delay < 0.0 {
                return Err("delay must be >= 0".to_string());
            }
        }

        Ok(())
    }
}

pub fn should_retry_error(retry_config: &RetryConfig, error_type: &str) -> bool {
    // If error is in exclude list, don't retry
    if retry_config
        .exclude_errors
        .contains(&error_type.to_string())
    {
        return false;
    }

    // If include list is empty, retry nothing
    if retry_config.include_errors.is_empty() {
        return false;
    }

    // Check if error is in include list
    retry_config
        .include_errors
        .contains(&error_type.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_default_retry_policy() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.version, 1);
        assert_eq!(policy.stop.max_attempts, Some(5));
        assert_eq!(policy.wait.strategy, WaitStrategy::ExponentialJitter);
        assert_eq!(policy.wait.jitter, JitterType::Full);
        assert_eq!(policy.retry.include_errors, vec!["ValueError".to_string()]);
    }

    #[test]
    fn test_fixed_wait_strategy() {
        let config = WaitConfig {
            strategy: WaitStrategy::Fixed,
            delay: Some(10.0),
            ..Default::default()
        };

        let delay = config.calculate_delay(0);
        assert_eq!(delay, Duration::from_secs(10));

        let delay = config.calculate_delay(5);
        assert_eq!(delay, Duration::from_secs(10));
    }

    #[test]
    fn test_exponential_wait_strategy() {
        let config = WaitConfig {
            strategy: WaitStrategy::Exponential,
            initial_delay: 1.0,
            multiplier: 2.0,
            max_delay: 10.0,
            jitter: JitterType::None,
            ..Default::default()
        };

        assert_eq!(config.calculate_delay(0), Duration::from_secs(1));
        assert_eq!(config.calculate_delay(1), Duration::from_secs(2));
        assert_eq!(config.calculate_delay(2), Duration::from_secs(4));
        assert_eq!(config.calculate_delay(3), Duration::from_secs(8));
        assert_eq!(config.calculate_delay(4), Duration::from_secs(10)); // capped
    }

    #[test]
    fn test_exponential_jitter_wait_strategy() {
        let config = WaitConfig {
            strategy: WaitStrategy::ExponentialJitter,
            initial_delay: 1.0,
            multiplier: 2.0,
            max_delay: 10.0,
            jitter: JitterType::Full,
            ..Default::default()
        };

        // Test that jittered values are within expected range
        for attempt in 0..5 {
            let delay = config.calculate_delay(attempt);
            let expected_max =
                Duration::from_secs_f64((1.0 * 2.0_f64.powi(attempt as i32)).min(10.0));
            assert!(delay <= expected_max);
            assert!(delay >= Duration::from_secs(0));
        }
    }

    #[test]
    fn test_retry_condition_evaluation() {
        let config = RetryConfig {
            include_errors: vec!["ValueError".to_string(), "TimeoutError".to_string()],
            exclude_errors: vec!["ValueError".to_string()],
        };

        assert!(!should_retry_error(&config, "ValueError")); // excluded
        assert!(should_retry_error(&config, "TimeoutError")); // in include and not excluded
        assert!(!should_retry_error(&config, "RuntimeError")); // not in include
    }

    #[test]
    fn test_empty_include_list() {
        let config = RetryConfig {
            include_errors: vec![],
            exclude_errors: vec![],
        };

        assert!(!should_retry_error(&config, "ValueError"));
        assert!(!should_retry_error(&config, "TimeoutError"));
    }

    #[test]
    fn test_json_parsing() {
        let json = json!({
            "version": 1,
            "stop": {"max_attempts": 3},
            "wait": {"strategy": "fixed", "delay": 5},
            "retry": {"include_errors": ["ValueError"]}
        });

        let policy = RetryPolicy::from_json(&json).unwrap();
        assert_eq!(policy.stop.max_attempts, Some(3));
        assert_eq!(policy.wait.strategy, WaitStrategy::Fixed);
        assert_eq!(policy.wait.delay, Some(5.0));
        assert_eq!(policy.retry.include_errors, vec!["ValueError".to_string()]);
    }

    #[test]
    fn test_validation() {
        let mut policy = RetryPolicy::default();
        assert!(policy.validate().is_ok());

        // Test invalid version
        policy.version = 2;
        assert!(policy.validate().is_err());
        policy.version = 1;

        // Test zero max_attempts
        policy.stop.max_attempts = Some(0);
        assert!(policy.validate().is_err());
        policy.stop.max_attempts = Some(5);

        // Test invalid multiplier
        policy.wait.multiplier = 0.5;
        assert!(policy.validate().is_err());
        policy.wait.multiplier = 2.0;

        // Test negative delays
        policy.wait.initial_delay = -1.0;
        assert!(policy.validate().is_err());
        policy.wait.initial_delay = 1.0;

        policy.wait.max_delay = -1.0;
        assert!(policy.validate().is_err());
        policy.wait.max_delay = 300.0;

        policy.wait.delay = Some(-1.0);
        assert!(policy.validate().is_err());
        policy.wait.delay = Some(1.0);

        assert!(policy.validate().is_ok());
    }

    #[test]
    fn test_partial_json_with_defaults() {
        let json = json!({
            "stop": {"max_attempts": 10}
        });

        let policy = RetryPolicy::from_json(&json).unwrap();
        assert_eq!(policy.version, 1); // default
        assert_eq!(policy.stop.max_attempts, Some(10));
        assert_eq!(policy.wait.strategy, WaitStrategy::ExponentialJitter); // default
        assert_eq!(policy.retry.include_errors, vec!["ValueError".to_string()]);
        // default
    }
}
