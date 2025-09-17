use crate::proto::common::{
    retry_policy_wait::Kind as ProtoWaitKind, RetryPolicy as ProtoRetryPolicy,
    RetryPolicyExponentialJitterWait as ProtoExpJitterWait,
    RetryPolicyExponentialWait as ProtoExpWait, RetryPolicyFixedWait as ProtoFixedWait,
    RetryPolicyRetry as ProtoRetry, RetryPolicyStop as ProtoStop, RetryPolicyWait as ProtoWait,
};
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WaitStrategy {
    Fixed,
    Exponential,       // Always deterministic
    ExponentialJitter, // Always has full jitter
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
            WaitStrategy::Exponential => {
                let base_delay = self.initial_delay * self.multiplier.powi(attempt_number as i32);
                base_delay.min(self.max_delay)
            }
            WaitStrategy::ExponentialJitter => {
                let base_delay = self.initial_delay * self.multiplier.powi(attempt_number as i32);
                let capped_delay = base_delay.min(self.max_delay);

                let mut rng = rand::thread_rng();
                rng.gen_range(0.0..=capped_delay)
            }
        };

        Duration::from_secs_f64(delay_secs)
    }
}

impl RetryPolicy {
    pub fn from_json(json: &serde_json::Value) -> Result<Self, String> {
        serde_json::from_value(json.clone()).map_err(|e| format!("Invalid retry policy: {e}"))
    }

    pub fn from_proto(proto: &ProtoRetryPolicy) -> Result<Self, String> {
        let stop = proto
            .stop
            .as_ref()
            .map(|value| StopConfig {
                max_attempts: value.max_attempts,
                max_delay: value.max_delay,
            })
            .unwrap_or_default();

        let wait = match proto.wait.as_ref().and_then(|wait| wait.kind.as_ref()) {
            Some(ProtoWaitKind::Fixed(ProtoFixedWait { delay })) => WaitConfig {
                strategy: WaitStrategy::Fixed,
                delay: Some(*delay),
                initial_delay: *delay,
                multiplier: 1.0,
                max_delay: *delay,
            },
            Some(ProtoWaitKind::Exponential(ProtoExpWait {
                initial_delay,
                multiplier,
                max_delay,
            })) => WaitConfig {
                strategy: WaitStrategy::Exponential,
                delay: None,
                initial_delay: *initial_delay,
                multiplier: *multiplier,
                max_delay: *max_delay,
            },
            Some(ProtoWaitKind::ExponentialJitter(ProtoExpJitterWait {
                initial_delay,
                multiplier,
                max_delay,
            })) => WaitConfig {
                strategy: WaitStrategy::ExponentialJitter,
                delay: None,
                initial_delay: *initial_delay,
                multiplier: *multiplier,
                max_delay: *max_delay,
            },
            None => WaitConfig::default(),
        };

        let retry = proto
            .retry
            .as_ref()
            .map(|value| RetryConfig {
                include_errors: value.include_errors.clone(),
                exclude_errors: value.exclude_errors.clone(),
            })
            .unwrap_or_default();

        let policy = Self {
            version: proto.version as u8,
            stop,
            wait,
            retry,
        };

        policy.validate()?;
        Ok(policy)
    }

    pub fn to_proto(&self) -> ProtoRetryPolicy {
        let stop = ProtoStop {
            max_attempts: self.stop.max_attempts,
            max_delay: self.stop.max_delay,
        };

        let wait_kind = match self.wait.strategy {
            WaitStrategy::Fixed => ProtoWaitKind::Fixed(ProtoFixedWait {
                delay: self.wait.delay.unwrap_or(self.wait.initial_delay),
            }),
            WaitStrategy::Exponential => ProtoWaitKind::Exponential(ProtoExpWait {
                initial_delay: self.wait.initial_delay,
                multiplier: self.wait.multiplier,
                max_delay: self.wait.max_delay,
            }),
            WaitStrategy::ExponentialJitter => {
                ProtoWaitKind::ExponentialJitter(ProtoExpJitterWait {
                    initial_delay: self.wait.initial_delay,
                    multiplier: self.wait.multiplier,
                    max_delay: self.wait.max_delay,
                })
            }
        };

        let wait = ProtoWait {
            kind: Some(wait_kind),
        };

        let retry = ProtoRetry {
            include_errors: self.retry.include_errors.clone(),
            exclude_errors: self.retry.exclude_errors.clone(),
        };

        ProtoRetryPolicy {
            version: self.version as u32,
            stop: Some(stop),
            wait: Some(wait),
            retry: Some(retry),
        }
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

    // Special-case: Treat "TaskError" as a wildcard for any task error type
    // This allows client libraries to specify the base error to retry on,
    // while concrete error_type values (e.g., "TestError", "ValidationError")
    // still work as before. Exclude list takes precedence above.
    if retry_config.include_errors.iter().any(|e| e == "TaskError") {
        return true;
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

    // =============================================================================
    // Basic Structure and Defaults Tests
    // =============================================================================

    /// Tests that default retry policy has expected values matching specification.
    /// This is the foundation test ensuring our defaults align with documented behavior.
    #[test]
    fn test_default_retry_policy() {
        let policy = RetryPolicy::default();

        // Verify default values from specification
        assert_eq!(policy.version, 1);
        assert_eq!(policy.stop.max_attempts, Some(5));
        assert_eq!(policy.stop.max_delay, None);
        assert_eq!(policy.wait.strategy, WaitStrategy::ExponentialJitter);
        assert_eq!(policy.wait.initial_delay, 1.0);
        assert_eq!(policy.wait.multiplier, 2.0);
        assert_eq!(policy.wait.max_delay, 300.0);
        assert_eq!(policy.retry.include_errors, vec!["ValueError".to_string()]);
        assert_eq!(policy.retry.exclude_errors, Vec::<String>::new());

        // Ensure default policy is valid
        assert!(policy.validate().is_ok());
    }

    /// Tests that individual config structs have correct default values.
    #[test]
    fn test_config_defaults() {
        let stop = StopConfig::default();
        assert_eq!(stop.max_attempts, Some(5));
        assert_eq!(stop.max_delay, None);

        let wait = WaitConfig::default();
        assert_eq!(wait.strategy, WaitStrategy::ExponentialJitter);
        assert_eq!(wait.initial_delay, 1.0);
        assert_eq!(wait.multiplier, 2.0);
        assert_eq!(wait.max_delay, 300.0);

        let retry = RetryConfig::default();
        assert_eq!(retry.include_errors, vec!["ValueError".to_string()]);
        assert!(retry.exclude_errors.is_empty());
    }

    // =============================================================================
    // Wait Strategy Tests
    // =============================================================================

    /// Tests fixed delay strategy - should return same delay regardless of attempt number.
    #[test]
    fn test_wait_strategy_fixed() {
        let config = WaitConfig {
            strategy: WaitStrategy::Fixed,
            delay: Some(10.0),
            ..Default::default()
        };

        // Fixed strategy should return same delay for all attempts
        assert_eq!(config.calculate_delay(0), Duration::from_secs(10));
        assert_eq!(config.calculate_delay(5), Duration::from_secs(10));
        assert_eq!(config.calculate_delay(100), Duration::from_secs(10));

        // Test fallback to initial_delay when delay is None
        let config_no_delay = WaitConfig {
            strategy: WaitStrategy::Fixed,
            delay: None,
            initial_delay: 5.0,
            ..Default::default()
        };
        assert_eq!(config_no_delay.calculate_delay(0), Duration::from_secs(5));
    }

    /// Tests exponential backoff without jitter - deterministic exponential growth.
    #[test]
    fn test_wait_strategy_exponential() {
        let config = WaitConfig {
            strategy: WaitStrategy::Exponential,
            initial_delay: 1.0,
            multiplier: 2.0,
            max_delay: 10.0,
            ..Default::default()
        };

        // Test exponential growth: 1, 2, 4, 8, 10 (capped)
        assert_eq!(config.calculate_delay(0), Duration::from_secs(1));
        assert_eq!(config.calculate_delay(1), Duration::from_secs(2));
        assert_eq!(config.calculate_delay(2), Duration::from_secs(4));
        assert_eq!(config.calculate_delay(3), Duration::from_secs(8));
        assert_eq!(config.calculate_delay(4), Duration::from_secs(10)); // capped at max_delay
        assert_eq!(config.calculate_delay(10), Duration::from_secs(10)); // stays capped
    }

    /// Tests exponential backoff with full jitter - should be within bounds.
    #[test]
    fn test_wait_strategy_exponential_jitter() {
        let config = WaitConfig {
            strategy: WaitStrategy::ExponentialJitter,
            initial_delay: 1.0,
            multiplier: 2.0,
            max_delay: 10.0,
            ..Default::default()
        };

        // Test that jittered values are within expected range [0, expected_max]
        for attempt in 0..5 {
            let delay = config.calculate_delay(attempt);
            let expected_max =
                Duration::from_secs_f64((1.0 * 2.0_f64.powi(attempt as i32)).min(10.0));

            assert!(
                delay <= expected_max,
                "Attempt {attempt}: delay {delay:?} > max {expected_max:?}"
            );
            assert!(
                delay >= Duration::from_secs(0),
                "Attempt {attempt}: delay {delay:?} < 0"
            );
        }

        // Test that jitter actually produces different values (probabilistic test)
        let delays: Vec<_> = (0..10).map(|_| config.calculate_delay(2)).collect();
        // With jitter, we should get some variation (not all identical)
        let all_same = delays.windows(2).all(|w| w[0] == w[1]);
        assert!(!all_same, "Jitter should produce some variation in delays");
    }

    // =============================================================================
    // Retry Condition Logic Tests
    // =============================================================================

    /// Tests retry decision logic with include/exclude error lists.
    #[test]
    fn test_retry_error_filtering() {
        let config = RetryConfig {
            include_errors: vec!["ValueError".to_string(), "TimeoutError".to_string()],
            exclude_errors: vec!["ValueError".to_string()],
        };

        // ValueError is included but also excluded - exclude takes precedence
        assert!(!should_retry_error(&config, "ValueError"));

        // TimeoutError is included and not excluded - should retry
        assert!(should_retry_error(&config, "TimeoutError"));

        // RuntimeError is not in include list - should not retry
        assert!(!should_retry_error(&config, "RuntimeError"));

        // Test case sensitivity
        assert!(!should_retry_error(&config, "valueerror")); // different case
    }

    /// Tests that empty include list means no retries (fail-safe behavior).
    #[test]
    fn test_empty_include_list_blocks_all_retries() {
        let config = RetryConfig {
            include_errors: vec![],
            exclude_errors: vec![],
        };

        // Empty include list should block all retries
        assert!(!should_retry_error(&config, "ValueError"));
        assert!(!should_retry_error(&config, "TimeoutError"));
        assert!(!should_retry_error(&config, "AnyError"));
    }

    /// Tests include-only configuration (common case).
    #[test]
    fn test_include_only_configuration() {
        let config = RetryConfig {
            include_errors: vec!["NetworkError".to_string(), "TimeoutError".to_string()],
            exclude_errors: vec![],
        };

        assert!(should_retry_error(&config, "NetworkError"));
        assert!(should_retry_error(&config, "TimeoutError"));
        assert!(!should_retry_error(&config, "ValueError"));
    }

    // =============================================================================
    // JSON Serialization/Deserialization Tests
    // =============================================================================

    /// Tests parsing complete, valid JSON policy.
    #[test]
    fn test_complete_json_parsing() {
        let json = json!({
            "version": 1,
            "stop": {"max_attempts": 3, "max_delay": 1800},
            "wait": {
                "strategy": "fixed",
                "delay": 5,
                "initial_delay": 2,
                "multiplier": 1.5,
                "max_delay": 60,
            },
            "retry": {
                "include_errors": ["ValueError", "NetworkError"],
                "exclude_errors": ["FatalError"]
            }
        });

        let policy = RetryPolicy::from_json(&json).unwrap();

        assert_eq!(policy.version, 1);
        assert_eq!(policy.stop.max_attempts, Some(3));
        assert_eq!(policy.stop.max_delay, Some(1800.0));
        assert_eq!(policy.wait.strategy, WaitStrategy::Fixed);
        assert_eq!(policy.wait.delay, Some(5.0));
        assert_eq!(policy.wait.initial_delay, 2.0);
        assert_eq!(policy.wait.multiplier, 1.5);
        assert_eq!(policy.wait.max_delay, 60.0);
        assert_eq!(
            policy.retry.include_errors,
            vec!["ValueError".to_string(), "NetworkError".to_string()]
        );
        assert_eq!(policy.retry.exclude_errors, vec!["FatalError".to_string()]);

        assert!(policy.validate().is_ok());
    }

    /// Tests partial JSON with defaults applied correctly.
    #[test]
    fn test_partial_json_with_defaults() {
        let json = json!({
            "stop": {"max_attempts": 10}
        });

        let policy = RetryPolicy::from_json(&json).unwrap();

        // Explicitly provided value
        assert_eq!(policy.stop.max_attempts, Some(10));

        // Default values should be applied
        assert_eq!(policy.version, 1);
        assert_eq!(policy.stop.max_delay, None);
        assert_eq!(policy.wait.strategy, WaitStrategy::ExponentialJitter);
        assert_eq!(policy.retry.include_errors, vec!["ValueError".to_string()]);

        assert!(policy.validate().is_ok());
    }

    /// Tests minimal valid JSON (empty object should use all defaults).
    #[test]
    fn test_minimal_json() {
        let json = json!({});
        let policy = RetryPolicy::from_json(&json).unwrap();

        // Should be identical to default
        let default_policy = RetryPolicy::default();
        assert_eq!(policy.version, default_policy.version);
        assert_eq!(policy.stop.max_attempts, default_policy.stop.max_attempts);
        assert_eq!(policy.wait.strategy, default_policy.wait.strategy);
        assert_eq!(
            policy.retry.include_errors,
            default_policy.retry.include_errors
        );

        assert!(policy.validate().is_ok());
    }

    // =============================================================================
    // Invalid Input and Error Handling Tests
    // =============================================================================

    /// Tests various invalid JSON structures and malformed input.
    #[test]
    fn test_invalid_json_parsing() {
        // Non-object JSON
        assert!(RetryPolicy::from_json(&json!("string")).is_err());
        assert!(RetryPolicy::from_json(&json!(123)).is_err());
        assert!(RetryPolicy::from_json(&json!(null)).is_err());
        // Note: empty array [] actually deserializes successfully with defaults

        // Invalid enum values
        assert!(RetryPolicy::from_json(&json!({
            "wait": {"strategy": "invalid_strategy"}
        }))
        .is_err());

        // Invalid field types
        assert!(RetryPolicy::from_json(&json!({
            "version": "not_a_number"
        }))
        .is_err());

        assert!(RetryPolicy::from_json(&json!({
            "stop": {"max_attempts": "not_a_number"}
        }))
        .is_err());

        assert!(RetryPolicy::from_json(&json!({
            "wait": {"initial_delay": "not_a_number"}
        }))
        .is_err());
    }

    /// Tests that unknown fields are gracefully ignored.
    #[test]
    fn test_unknown_fields_ignored() {
        let json = json!({
            "version": 1,
            "unknown_field": "should_be_ignored",
            "stop": {
                "max_attempts": 5,
                "unknown_stop_field": true
            },
            "wait": {
                "strategy": "fixed",
                "delay": 10,
                "unknown_wait_field": [1, 2, 3]
            },
            "retry": {
                "include_errors": ["ValueError"],
                "unknown_retry_field": {"nested": "object"}
            }
        });

        let policy = RetryPolicy::from_json(&json).unwrap();
        assert_eq!(policy.version, 1);
        assert_eq!(policy.stop.max_attempts, Some(5));
        assert_eq!(policy.wait.strategy, WaitStrategy::Fixed);
        assert_eq!(policy.wait.delay, Some(10.0));
        assert_eq!(policy.retry.include_errors, vec!["ValueError".to_string()]);

        assert!(policy.validate().is_ok());
    }

    // =============================================================================
    // Validation Logic Tests
    // =============================================================================

    /// Tests policy validation rules and constraint enforcement.
    #[test]
    fn test_policy_validation() {
        // Valid policy should pass
        let mut policy = RetryPolicy::default();
        assert!(policy.validate().is_ok());

        // Invalid version
        policy.version = 0;
        assert!(policy.validate().is_err());
        policy.version = 2;
        assert!(policy.validate().is_err());
        policy.version = 1; // reset

        // Invalid max_attempts (zero)
        policy.stop.max_attempts = Some(0);
        assert!(policy.validate().is_err());
        policy.stop.max_attempts = Some(1); // minimum valid
        assert!(policy.validate().is_ok());
        policy.stop.max_attempts = Some(5); // reset

        // Invalid multiplier (< 1.0)
        policy.wait.multiplier = 0.0;
        assert!(policy.validate().is_err());
        policy.wait.multiplier = 0.99;
        assert!(policy.validate().is_err());
        policy.wait.multiplier = 1.0; // minimum valid
        assert!(policy.validate().is_ok());
        policy.wait.multiplier = 2.0; // reset

        // Invalid delays (negative values)
        policy.wait.initial_delay = -0.1;
        assert!(policy.validate().is_err());
        policy.wait.initial_delay = 0.0; // minimum valid
        assert!(policy.validate().is_ok());
        policy.wait.initial_delay = 1.0; // reset

        policy.wait.max_delay = -1.0;
        assert!(policy.validate().is_err());
        policy.wait.max_delay = 0.0; // minimum valid
        assert!(policy.validate().is_ok());
        policy.wait.max_delay = 300.0; // reset

        policy.wait.delay = Some(-1.0);
        assert!(policy.validate().is_err());
        policy.wait.delay = Some(0.0); // minimum valid
        assert!(policy.validate().is_ok());
        policy.wait.delay = None; // reset

        // Final check - should be valid again
        assert!(policy.validate().is_ok());
    }

    /// Tests edge cases for validation boundary conditions.
    #[test]
    fn test_validation_edge_cases() {
        let mut policy = RetryPolicy::default();

        // Test None max_attempts (infinite retries) - should be valid
        policy.stop.max_attempts = None;
        assert!(policy.validate().is_ok());

        // Test None max_delay (no time limit) - should be valid
        policy.stop.max_delay = None;
        assert!(policy.validate().is_ok());

        // Test multiplier exactly 1.0 (no exponential growth) - should be valid
        policy.wait.multiplier = 1.0;
        assert!(policy.validate().is_ok());

        // Test very large values - should be valid
        policy.stop.max_attempts = Some(u32::MAX);
        policy.stop.max_delay = Some(f64::MAX);
        policy.wait.initial_delay = f64::MAX;
        policy.wait.max_delay = f64::MAX;
        policy.wait.multiplier = f64::MAX;
        assert!(policy.validate().is_ok());
    }

    // =============================================================================
    // Max Delay Tests
    // =============================================================================

    /// Tests wait strategy max_delay capping behavior for exponential backoff.
    #[test]
    fn test_wait_max_delay_capping() {
        let config = WaitConfig {
            strategy: WaitStrategy::Exponential,
            initial_delay: 1.0,
            multiplier: 10.0, // Aggressive multiplier to test capping
            max_delay: 5.0,   // Low cap to test effectiveness
            ..Default::default()
        };

        // Verify delays are capped at max_delay
        assert_eq!(config.calculate_delay(0), Duration::from_secs(1)); // 1 * 10^0 = 1
        assert_eq!(config.calculate_delay(1), Duration::from_secs(5)); // 1 * 10^1 = 10, capped to 5
        assert_eq!(config.calculate_delay(2), Duration::from_secs(5)); // 1 * 10^2 = 100, capped to 5
        assert_eq!(config.calculate_delay(10), Duration::from_secs(5)); // Always capped
    }

    /// Tests wait strategy max_delay capping with exponential jitter.
    #[test]
    fn test_wait_max_delay_capping_with_jitter() {
        let config = WaitConfig {
            strategy: WaitStrategy::ExponentialJitter,
            initial_delay: 1.0,
            multiplier: 10.0,
            max_delay: 5.0,
            ..Default::default()
        };

        // Test that jittered values never exceed max_delay
        for attempt in 1..5 {
            for _ in 0..20 {
                // Test multiple times due to randomness
                let delay = config.calculate_delay(attempt);
                assert!(
                    delay <= Duration::from_secs(5),
                    "Jittered delay {delay:?} exceeded max_delay of 5s for attempt {attempt}"
                );
            }
        }
    }

    /// Tests max_delay validation with various values.
    #[test]
    fn test_max_delay_validation() {
        let mut policy = RetryPolicy::default();

        // Test valid max_delay values
        policy.stop.max_delay = Some(0.0);
        assert!(policy.validate().is_ok());

        policy.stop.max_delay = Some(1.0);
        assert!(policy.validate().is_ok());

        policy.stop.max_delay = Some(f64::MAX);
        assert!(policy.validate().is_ok());

        policy.stop.max_delay = None; // No time limit
        assert!(policy.validate().is_ok());

        // Test that negative max_delay in stop config would be invalid if supported
        // (Currently stop.max_delay doesn't have validation, but wait.max_delay does)
        policy.wait.max_delay = -1.0;
        assert!(policy.validate().is_err());
        policy.wait.max_delay = 0.0; // Reset to valid
        assert!(policy.validate().is_ok());
    }

    /// Tests JSON parsing and serialization of max_delay configurations.
    #[test]
    fn test_max_delay_json_handling() {
        // Test stop max_delay parsing
        let json_with_stop_max_delay = json!({
            "version": 1,
            "stop": {"max_delay": 3600},
            "wait": {"max_delay": 300}
        });

        let policy = RetryPolicy::from_json(&json_with_stop_max_delay).unwrap();
        assert_eq!(policy.stop.max_delay, Some(3600.0));
        assert_eq!(policy.wait.max_delay, 300.0);

        // Test null max_delay (infinite time)
        let json_with_null_max_delay = json!({
            "version": 1,
            "stop": {"max_delay": null}
        });

        let policy = RetryPolicy::from_json(&json_with_null_max_delay).unwrap();
        assert_eq!(policy.stop.max_delay, None);

        // Test fractional max_delay values
        let json_with_fractional = json!({
            "version": 1,
            "stop": {"max_delay": 1.5},
            "wait": {"max_delay": 2.75}
        });

        let policy = RetryPolicy::from_json(&json_with_fractional).unwrap();
        assert_eq!(policy.stop.max_delay, Some(1.5));
        assert_eq!(policy.wait.max_delay, 2.75);
    }

    /// Tests edge cases for wait strategy max_delay.
    #[test]
    fn test_wait_max_delay_edge_cases() {
        // Test with max_delay = 0 (immediate retry)
        let config_zero_delay = WaitConfig {
            strategy: WaitStrategy::Exponential,
            initial_delay: 10.0,
            multiplier: 2.0,
            max_delay: 0.0,
            ..Default::default()
        };
        assert_eq!(config_zero_delay.calculate_delay(0), Duration::from_secs(0));
        assert_eq!(config_zero_delay.calculate_delay(5), Duration::from_secs(0));

        // Test with very small max_delay
        let config_small_delay = WaitConfig {
            strategy: WaitStrategy::Exponential,
            initial_delay: 1.0,
            multiplier: 2.0,
            max_delay: 0.1,
            ..Default::default()
        };
        assert_eq!(
            config_small_delay.calculate_delay(0),
            Duration::from_secs_f64(0.1)
        );
        assert_eq!(
            config_small_delay.calculate_delay(10),
            Duration::from_secs_f64(0.1)
        );

        // Test with max_delay smaller than initial_delay
        let config_tiny_max = WaitConfig {
            strategy: WaitStrategy::Exponential,
            initial_delay: 5.0,
            multiplier: 2.0,
            max_delay: 1.0, // Smaller than initial_delay
            ..Default::default()
        };
        assert_eq!(config_tiny_max.calculate_delay(0), Duration::from_secs(1)); // Capped immediately
    }

    /// Tests max_delay behavior with fixed strategy.
    #[test]
    fn test_fixed_strategy_max_delay_interaction() {
        // When using fixed strategy, max_delay should not affect the result
        // since fixed delay should always be constant
        let config = WaitConfig {
            strategy: WaitStrategy::Fixed,
            delay: Some(10.0),
            max_delay: 5.0, // Smaller than fixed delay
            ..Default::default()
        };

        // Fixed strategy should ignore max_delay and use the specified delay
        assert_eq!(config.calculate_delay(0), Duration::from_secs(10));
        assert_eq!(config.calculate_delay(100), Duration::from_secs(10));

        // Test fixed strategy fallback to initial_delay when delay is None
        let config_fallback = WaitConfig {
            strategy: WaitStrategy::Fixed,
            delay: None,
            initial_delay: 8.0,
            max_delay: 5.0, // Smaller than initial_delay
            ..Default::default()
        };

        // Should use initial_delay, ignoring max_delay for fixed strategy
        assert_eq!(config_fallback.calculate_delay(0), Duration::from_secs(8));
    }
}
