use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Retry policy configuration  
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    pub version: u32,
    pub stop: StopCondition,
    pub wait: WaitStrategy,
    pub retry: RetryCondition,
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
