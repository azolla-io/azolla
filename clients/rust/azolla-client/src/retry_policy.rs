use crate::proto::common::{
    retry_policy_wait::Kind as ProtoWaitKind, RetryPolicy as ProtoRetryPolicy,
    RetryPolicyExponentialJitterWait as ProtoExpJitterWait,
    RetryPolicyExponentialWait as ProtoExpWait, RetryPolicyFixedWait as ProtoFixedWait,
    RetryPolicyRetry as ProtoRetry, RetryPolicyStop as ProtoStop, RetryPolicyWait as ProtoWait,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;

/// Retry policy configuration  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub version: u32,
    pub stop: StopCondition,
    pub wait: WaitStrategy,
    pub retry: RetryCondition,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl RetryPolicy {
    pub fn builder() -> RetryPolicyBuilder {
        RetryPolicyBuilder::default()
    }

    pub fn exponential() -> RetryPolicyBuilder {
        RetryPolicyBuilder::new(WaitStrategy::ExponentialJitter {
            initial_delay: Duration::from_secs(1),
            multiplier: 2.0,
            max_delay: Duration::from_secs(300),
        })
    }

    pub fn fixed(delay: Duration) -> RetryPolicyBuilder {
        RetryPolicyBuilder::new(WaitStrategy::Fixed { delay })
    }

    pub fn none() -> RetryPolicyBuilder {
        RetryPolicyBuilder::new(WaitStrategy::Fixed {
            delay: Duration::from_secs(0),
        })
        .max_attempts(1)
    }

    /// Convert the retry policy into the JSON structure expected by the orchestrator API
    pub fn to_submission_json(&self) -> serde_json::Value {
        let stop_max_delay_seconds = self.stop.max_delay.map(|delay| delay.as_secs_f64());

        let stop = json!({
            "max_attempts": self.stop.max_attempts,
            "max_delay": stop_max_delay_seconds,
        });

        let wait = match &self.wait {
            WaitStrategy::Fixed { delay } => json!({
                "strategy": "fixed",
                "delay": delay.as_secs_f64(),
            }),
            WaitStrategy::Exponential {
                initial_delay,
                multiplier,
                max_delay,
            } => json!({
                "strategy": "exponential",
                "initial_delay": initial_delay.as_secs_f64(),
                "multiplier": *multiplier,
                "max_delay": max_delay.as_secs_f64(),
            }),
            WaitStrategy::ExponentialJitter {
                initial_delay,
                multiplier,
                max_delay,
            } => json!({
                "strategy": "exponential_jitter",
                "initial_delay": initial_delay.as_secs_f64(),
                "multiplier": *multiplier,
                "max_delay": max_delay.as_secs_f64(),
            }),
        };

        json!({
            "version": self.version,
            "stop": stop,
            "wait": wait,
            "retry": {
                "include_errors": self.retry.include_errors.clone(),
                "exclude_errors": self.retry.exclude_errors.clone(),
            }
        })
    }

    pub fn to_proto(&self) -> ProtoRetryPolicy {
        let stop = ProtoStop {
            max_attempts: self.stop.max_attempts,
            max_delay: self.stop.max_delay.map(|d| d.as_secs_f64()),
        };

        let wait_kind = match &self.wait {
            WaitStrategy::Fixed { delay } => ProtoWaitKind::Fixed(ProtoFixedWait {
                delay: delay.as_secs_f64(),
            }),
            WaitStrategy::Exponential {
                initial_delay,
                multiplier,
                max_delay,
            } => ProtoWaitKind::Exponential(ProtoExpWait {
                initial_delay: initial_delay.as_secs_f64(),
                multiplier: *multiplier,
                max_delay: max_delay.as_secs_f64(),
            }),
            WaitStrategy::ExponentialJitter {
                initial_delay,
                multiplier,
                max_delay,
            } => ProtoWaitKind::ExponentialJitter(ProtoExpJitterWait {
                initial_delay: initial_delay.as_secs_f64(),
                multiplier: *multiplier,
                max_delay: max_delay.as_secs_f64(),
            }),
        };

        let wait = ProtoWait {
            kind: Some(wait_kind),
        };

        let retry = ProtoRetry {
            include_errors: self.retry.include_errors.clone(),
            exclude_errors: self.retry.exclude_errors.clone(),
        };

        ProtoRetryPolicy {
            version: self.version,
            stop: Some(stop),
            wait: Some(wait),
            retry: Some(retry),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StopCondition {
    pub max_attempts: Option<u32>,
    pub max_delay: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WaitStrategy {
    Fixed {
        delay: Duration,
    },
    Exponential {
        initial_delay: Duration,
        multiplier: f64,
        max_delay: Duration,
    },
    ExponentialJitter {
        initial_delay: Duration,
        multiplier: f64,
        max_delay: Duration,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryCondition {
    pub include_errors: Vec<String>,
    pub exclude_errors: Vec<String>,
}

#[derive(Debug)]
pub struct RetryPolicyBuilder {
    version: u32,
    strategy: WaitStrategy,
    max_attempts: Option<u32>,
    max_delay: Option<Duration>,
    include_errors: Vec<String>,
    exclude_errors: Vec<String>,
}

impl Default for RetryPolicyBuilder {
    fn default() -> Self {
        Self {
            version: 1,
            strategy: WaitStrategy::ExponentialJitter {
                initial_delay: Duration::from_secs(1),
                multiplier: 2.0,
                max_delay: Duration::from_secs(300),
            },
            max_attempts: Some(5),
            max_delay: None,
            include_errors: vec!["ValueError".to_string()],
            exclude_errors: vec![],
        }
    }
}

impl RetryPolicyBuilder {
    pub fn new(strategy: WaitStrategy) -> Self {
        Self {
            strategy,
            ..Default::default()
        }
    }

    pub fn max_attempts(mut self, attempts: u32) -> Self {
        self.max_attempts = Some(attempts);
        self
    }

    pub fn max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = Some(delay);
        self
    }

    pub fn initial_delay(mut self, delay: Duration) -> Self {
        match &mut self.strategy {
            WaitStrategy::Exponential { initial_delay, .. }
            | WaitStrategy::ExponentialJitter { initial_delay, .. } => {
                *initial_delay = delay;
            }
            _ => {}
        }
        self
    }

    pub fn retry_on(mut self, errors: &[&str]) -> Self {
        self.include_errors = errors.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn exclude_errors(mut self, errors: &[&str]) -> Self {
        self.exclude_errors = errors.iter().map(|s| s.to_string()).collect();
        self
    }

    pub fn build(self) -> RetryPolicy {
        RetryPolicy {
            version: self.version,
            stop: StopCondition {
                max_attempts: self.max_attempts,
                max_delay: self.max_delay,
            },
            wait: self.strategy,
            retry: RetryCondition {
                include_errors: self.include_errors,
                exclude_errors: self.exclude_errors,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    /// Test the purpose of RetryPolicy: ensure default configuration is reasonable
    #[test]
    fn test_retry_policy_defaults() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.version, 1);
        assert_eq!(policy.stop.max_attempts, Some(5));
        assert!(policy.stop.max_delay.is_none());

        // Check that it includes ValueError by default
        assert!(policy
            .retry
            .include_errors
            .contains(&"ValueError".to_string()));
        assert!(policy.retry.exclude_errors.is_empty());

        // Check wait strategy is exponential jitter by default
        match policy.wait {
            WaitStrategy::ExponentialJitter {
                initial_delay,
                multiplier,
                max_delay,
            } => {
                assert_eq!(initial_delay, Duration::from_secs(1));
                assert_eq!(multiplier, 2.0);
                assert_eq!(max_delay, Duration::from_secs(300));
            }
            _ => panic!("Expected ExponentialJitter strategy by default"),
        }
    }

    /// Test the expected behavior: RetryPolicyBuilder should allow configuration
    #[test]
    fn test_retry_policy_builder() {
        let policy = RetryPolicy::builder()
            .max_attempts(10)
            .max_delay(Duration::from_secs(600))
            .retry_on(&["NetworkError", "TimeoutError"])
            .exclude_errors(&["AuthError"])
            .build();

        assert_eq!(policy.stop.max_attempts, Some(10));
        assert_eq!(policy.stop.max_delay, Some(Duration::from_secs(600)));
        assert_eq!(
            policy.retry.include_errors,
            vec!["NetworkError", "TimeoutError"]
        );
        assert_eq!(policy.retry.exclude_errors, vec!["AuthError"]);
    }

    /// Test different wait strategies
    #[test]
    fn test_wait_strategies() {
        // Test fixed strategy
        let fixed_policy = RetryPolicy::fixed(Duration::from_secs(5)).build();
        match fixed_policy.wait {
            WaitStrategy::Fixed { delay } => {
                assert_eq!(delay, Duration::from_secs(5));
            }
            _ => panic!("Expected Fixed strategy"),
        }

        // Test exponential strategy
        let exp_policy = RetryPolicy::exponential()
            .initial_delay(Duration::from_millis(500))
            .max_attempts(3)
            .build();
        match exp_policy.wait {
            WaitStrategy::ExponentialJitter {
                initial_delay,
                multiplier,
                max_delay,
            } => {
                assert_eq!(initial_delay, Duration::from_millis(500));
                assert_eq!(multiplier, 2.0);
                assert_eq!(max_delay, Duration::from_secs(300));
            }
            _ => panic!("Expected ExponentialJitter strategy"),
        }
        assert_eq!(exp_policy.stop.max_attempts, Some(3));
    }

    /// Test no retry policy
    #[test]
    fn test_no_retry_policy() {
        let no_retry = RetryPolicy::none().build();
        assert_eq!(no_retry.stop.max_attempts, Some(1));

        match no_retry.wait {
            WaitStrategy::Fixed { delay } => {
                assert_eq!(delay, Duration::from_secs(0));
            }
            _ => panic!("Expected Fixed strategy with zero delay"),
        }
    }

    /// Test retry policy serialization and deserialization
    #[test]
    fn test_retry_policy_serialization() {
        let policy = RetryPolicy::builder()
            .max_attempts(3)
            .max_delay(Duration::from_secs(120))
            .retry_on(&["NetworkError", "TimeoutError"])
            .exclude_errors(&["AuthenticationError"])
            .build();

        // Test serialization
        let serialized = serde_json::to_string(&policy).unwrap();
        assert!(!serialized.is_empty());
        assert!(serialized.contains("max_attempts"));
        assert!(serialized.contains("NetworkError"));

        // Test deserialization
        let deserialized: RetryPolicy = serde_json::from_str(&serialized).unwrap();
        assert_eq!(policy.version, deserialized.version);
        assert_eq!(policy.stop.max_attempts, deserialized.stop.max_attempts);
        assert_eq!(policy.stop.max_delay, deserialized.stop.max_delay);
        assert_eq!(
            policy.retry.include_errors,
            deserialized.retry.include_errors
        );
        assert_eq!(
            policy.retry.exclude_errors,
            deserialized.retry.exclude_errors
        );
    }

    /// Test wait strategy serialization
    #[test]
    fn test_wait_strategy_serialization() {
        let strategies = vec![
            WaitStrategy::Fixed {
                delay: Duration::from_secs(5),
            },
            WaitStrategy::Exponential {
                initial_delay: Duration::from_secs(1),
                multiplier: 2.0,
                max_delay: Duration::from_secs(60),
            },
            WaitStrategy::ExponentialJitter {
                initial_delay: Duration::from_millis(500),
                multiplier: 1.5,
                max_delay: Duration::from_secs(120),
            },
        ];

        for strategy in strategies {
            let serialized = serde_json::to_string(&strategy).unwrap();
            let deserialized: WaitStrategy = serde_json::from_str(&serialized).unwrap();

            // Compare strategy types and key values
            match (&strategy, &deserialized) {
                (WaitStrategy::Fixed { delay: d1 }, WaitStrategy::Fixed { delay: d2 }) => {
                    assert_eq!(d1, d2);
                }
                (
                    WaitStrategy::Exponential {
                        initial_delay: id1,
                        multiplier: m1,
                        max_delay: md1,
                    },
                    WaitStrategy::Exponential {
                        initial_delay: id2,
                        multiplier: m2,
                        max_delay: md2,
                    },
                ) => {
                    assert_eq!(id1, id2);
                    assert_eq!(m1, m2);
                    assert_eq!(md1, md2);
                }
                (
                    WaitStrategy::ExponentialJitter {
                        initial_delay: id1,
                        multiplier: m1,
                        max_delay: md1,
                    },
                    WaitStrategy::ExponentialJitter {
                        initial_delay: id2,
                        multiplier: m2,
                        max_delay: md2,
                    },
                ) => {
                    assert_eq!(id1, id2);
                    assert_eq!(m1, m2);
                    assert_eq!(md1, md2);
                }
                _ => panic!("Strategy types don't match after serialization"),
            }
        }
    }

    /// Test builder method chaining
    #[test]
    fn test_builder_method_chaining() {
        let policy = RetryPolicy::builder()
            .max_attempts(7)
            .max_delay(Duration::from_secs(240))
            .initial_delay(Duration::from_millis(200))
            .retry_on(&["IOError", "ConnectionError"])
            .exclude_errors(&["PermissionError"])
            .build();

        assert_eq!(policy.stop.max_attempts, Some(7));
        assert_eq!(policy.stop.max_delay, Some(Duration::from_secs(240)));
        assert_eq!(
            policy.retry.include_errors,
            vec!["IOError", "ConnectionError"]
        );
        assert_eq!(policy.retry.exclude_errors, vec!["PermissionError"]);

        match policy.wait {
            WaitStrategy::ExponentialJitter { initial_delay, .. } => {
                assert_eq!(initial_delay, Duration::from_millis(200));
            }
            _ => panic!("Expected ExponentialJitter with modified initial delay"),
        }
    }

    /// Test initial_delay modification with different strategies
    #[test]
    fn test_initial_delay_modification() {
        // Test with ExponentialJitter (should modify)
        let jitter_policy = RetryPolicy::exponential()
            .initial_delay(Duration::from_millis(750))
            .build();
        match jitter_policy.wait {
            WaitStrategy::ExponentialJitter { initial_delay, .. } => {
                assert_eq!(initial_delay, Duration::from_millis(750));
            }
            _ => panic!("Expected ExponentialJitter"),
        }

        // Test with Fixed strategy (should not modify)
        let fixed_policy = RetryPolicy::fixed(Duration::from_secs(2))
            .initial_delay(Duration::from_millis(750)) // This should be ignored
            .build();
        match fixed_policy.wait {
            WaitStrategy::Fixed { delay } => {
                assert_eq!(delay, Duration::from_secs(2)); // Original value preserved
            }
            _ => panic!("Expected Fixed"),
        }
    }

    /// Test edge cases and validation
    #[test]
    fn test_edge_cases() {
        // Test zero max attempts
        let zero_attempts = RetryPolicy::builder().max_attempts(0).build();
        assert_eq!(zero_attempts.stop.max_attempts, Some(0));

        // Test very large values
        let large_values = RetryPolicy::builder()
            .max_attempts(u32::MAX)
            .max_delay(Duration::from_secs(u64::MAX / 1000)) // Avoid overflow
            .build();
        assert_eq!(large_values.stop.max_attempts, Some(u32::MAX));

        // Test empty error lists
        let empty_errors = RetryPolicy::builder()
            .retry_on(&[])
            .exclude_errors(&[])
            .build();
        assert!(empty_errors.retry.include_errors.is_empty());
        assert!(empty_errors.retry.exclude_errors.is_empty());
    }

    /// Test WaitStrategy Debug formatting
    #[test]
    fn test_wait_strategy_debug() {
        let fixed = WaitStrategy::Fixed {
            delay: Duration::from_secs(1),
        };
        let debug_str = format!("{fixed:?}");
        assert!(debug_str.contains("Fixed"));
        assert!(debug_str.contains("1"));

        let exponential = WaitStrategy::ExponentialJitter {
            initial_delay: Duration::from_millis(500),
            multiplier: 2.0,
            max_delay: Duration::from_secs(60),
        };
        let debug_exp = format!("{exponential:?}");
        assert!(debug_exp.contains("ExponentialJitter"));
        assert!(debug_exp.contains("500"));
        assert!(debug_exp.contains("2"));
    }

    /// Test RetryCondition with complex error patterns
    #[test]
    fn test_retry_condition_patterns() {
        let condition = RetryCondition {
            include_errors: vec![
                "NetworkError".to_string(),
                "TimeoutError".to_string(),
                "ConnectionResetError".to_string(),
            ],
            exclude_errors: vec![
                "AuthenticationError".to_string(),
                "PermissionDeniedError".to_string(),
            ],
        };

        assert_eq!(condition.include_errors.len(), 3);
        assert_eq!(condition.exclude_errors.len(), 2);
        assert!(condition
            .include_errors
            .contains(&"NetworkError".to_string()));
        assert!(condition
            .exclude_errors
            .contains(&"AuthenticationError".to_string()));
    }

    /// Test StopCondition with optional fields
    #[test]
    fn test_stop_condition() {
        // Test with both fields set
        let full_stop = StopCondition {
            max_attempts: Some(5),
            max_delay: Some(Duration::from_secs(120)),
        };
        assert!(full_stop.max_attempts.is_some());
        assert!(full_stop.max_delay.is_some());

        // Test with only max_attempts
        let attempts_only = StopCondition {
            max_attempts: Some(3),
            max_delay: None,
        };
        assert_eq!(attempts_only.max_attempts, Some(3));
        assert!(attempts_only.max_delay.is_none());

        // Test with only max_delay
        let delay_only = StopCondition {
            max_attempts: None,
            max_delay: Some(Duration::from_secs(90)),
        };
        assert!(delay_only.max_attempts.is_none());
        assert_eq!(delay_only.max_delay, Some(Duration::from_secs(90)));
    }

    /// Test policy versioning
    #[test]
    fn test_policy_versioning() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.version, 1);

        let builder = RetryPolicyBuilder::default();
        assert_eq!(builder.version, 1);
    }

    /// Test complex policy JSON structure
    #[test]
    fn test_complex_policy_json() {
        let policy = RetryPolicy::builder()
            .max_attempts(5)
            .max_delay(Duration::from_secs(300))
            .retry_on(&["NetworkError", "TimeoutError", "ServiceUnavailable"])
            .exclude_errors(&["AuthenticationError", "AuthorizationError"])
            .build();

        let json = serde_json::to_string_pretty(&policy).unwrap();
        assert!(json.contains("version"));
        assert!(json.contains("stop"));
        assert!(json.contains("wait"));
        assert!(json.contains("retry"));
        assert!(json.contains("max_attempts"));
        assert!(json.contains("NetworkError"));
        assert!(json.contains("AuthenticationError"));

        // Ensure it can round-trip
        let deserialized: RetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(policy.version, deserialized.version);
    }
}
