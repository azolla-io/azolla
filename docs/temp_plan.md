# Enhanced Retry Policy Implementation Plan

## Overview
Implement the complete retry policy specification from `retry_policy.md` to replace the current basic `max_attempts` retry logic with a comprehensive retry system including wait strategies, retry conditions, and time-based stopping.

## Current State Analysis
- ✅ Basic `max_attempts` retry logic in scheduler (scheduler.rs:527-541)
- ✅ Task attempt tracking and database persistence (TaskAttempt struct)
- ✅ JSON retry policy storage in Task.retry_policy field
- ❌ **Missing automatic retry triggering** (TODO comment at scheduler.rs:611)
- ❌ No wait/backoff strategy implementation
- ❌ No retry condition evaluation (include_errors/exclude_errors)
- ❌ No max_delay time-based stopping
- ❌ No enhanced wait strategies (fixed, exponential, exponential_jitter)

## Implementation Phases

### Phase 1: Core Type System and Wait Strategies
**Status: PENDING**

#### 1.1 Create Retry Policy Types (src/orchestrator/retry_policy.rs)
**Status: PENDING**
- Create new file `src/orchestrator/retry_policy.rs`
- Define types matching the spec:
  ```rust
  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct RetryPolicy {
      pub version: u8,
      pub stop: StopConfig,
      pub wait: WaitConfig,
      pub retry: RetryConfig,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct StopConfig {
      pub max_attempts: Option<u32>, // None = infinite
      pub max_delay: Option<f64>,    // seconds, None = no time limit
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct WaitConfig {
      pub strategy: WaitStrategy,
      pub delay: Option<f64>,         // for fixed strategy
      pub initial_delay: f64,
      pub multiplier: f64,
      pub max_delay: f64,
      pub jitter: JitterType,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub enum WaitStrategy {
      Fixed,
      Exponential,
      ExponentialJitter,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub enum JitterType {
      None,
      Full,
  }

  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  pub struct RetryConfig {
      pub include_errors: Vec<String>,
      pub exclude_errors: Vec<String>,
  }
  ```

#### 1.2 Implement Default Values
**Status: PENDING**
- Implement `Default` trait for all config structs matching spec defaults:
  ```rust
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
  ```

#### 1.3 Implement Wait Strategy Calculation
**Status: PENDING**
- Add wait calculation methods to `WaitConfig`:
  ```rust
  impl WaitConfig {
      pub fn calculate_delay(&self, attempt_number: u32) -> Duration {
          let delay_secs = match self.strategy {
              WaitStrategy::Fixed => self.delay.unwrap_or(self.initial_delay),
              WaitStrategy::Exponential | WaitStrategy::ExponentialJitter => {
                  let base_delay = self.initial_delay * self.multiplier.powi(attempt_number as i32 - 1);
                  let capped_delay = base_delay.min(self.max_delay);
                  
                  if self.strategy == WaitStrategy::ExponentialJitter && self.jitter == JitterType::Full {
                      use rand::Rng;
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
  ```

#### 1.4 Add JSON Parsing and Validation
**Status: PENDING**
- Implement conversion from JSON to typed structs:
  ```rust
  impl RetryPolicy {
      pub fn from_json(json: &serde_json::Value) -> Result<Self, String> {
          serde_json::from_value(json.clone())
              .map_err(|e| format!("Invalid retry policy: {}", e))
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
          
          Ok(())
      }
  }
  ```

### Phase 2: Task Scheduling Infrastructure
**Status: PENDING**

#### 2.1 Add Retry Scheduler to TaskSet
**Status: PENDING**
- Modify `src/orchestrator/taskset.rs`:
  ```rust
  use std::collections::BinaryHeap;
  use std::time::Instant;
  use std::cmp::Reverse;

  #[derive(Debug, PartialEq, Eq)]
  struct ScheduledRetry {
      when: Instant,
      task_id: Uuid,
  }

  impl Ord for ScheduledRetry {
      fn cmp(&self, other: &Self) -> std::cmp::Ordering {
          // Reverse order for min-heap (earliest first)
          other.when.cmp(&self.when)
      }
  }

  impl PartialOrd for ScheduledRetry {
      fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
          Some(self.cmp(other))
      }
  }

  pub struct TaskSet {
      // existing fields...
      pub retry_scheduler: BinaryHeap<ScheduledRetry>,
  }

  impl TaskSet {
      pub fn schedule_retry(&mut self, task_id: Uuid, when: Instant) {
          self.retry_scheduler.push(ScheduledRetry { when, task_id });
      }
      
      pub fn get_next_retry_time(&self) -> Option<Instant> {
          self.retry_scheduler.peek().map(|retry| retry.when)
      }
      
      pub fn pop_due_retries(&mut self, now: Instant) -> Vec<Uuid> {
          let mut due_tasks = Vec::new();
          
          while let Some(retry) = self.retry_scheduler.peek() {
              if retry.when <= now {
                  due_tasks.push(self.retry_scheduler.pop().unwrap().task_id);
              } else {
                  break;
              }
          }
          
          due_tasks
      }
  }
  ```

#### 2.2 Add Task Time Tracking
**Status: PENDING**
- Add method to `Task` in `taskset.rs` to check max_delay using existing attempts:
  ```rust
  impl Task {
      pub fn has_exceeded_max_delay(&self, max_delay_secs: Option<f64>) -> bool {
          if let Some(max_delay) = max_delay_secs {
              if let Some(first_attempt) = self.attempts.first() {
                  let elapsed = Utc::now().signed_duration_since(first_attempt.start_time);
                  elapsed.num_seconds() as f64 >= max_delay
              } else {
                  false
              }
          } else {
              false
          }
      }
  }
  ```

### Phase 3: Scheduler Actor Enhancement
**Status: PENDING**

#### 3.1 Add Dynamic Timeout to SchedulerActor
**Status: PENDING**
- Modify `src/orchestrator/scheduler.rs` SchedulerActor loop:
  ```rust
  impl SchedulerActor {
      async fn run(&mut self) -> Result<()> {
          loop {
              let next_retry_time = self.task_set.get_next_retry_time();
              
              let timeout_future = match next_retry_time {
                  Some(when) => {
                      let tokio_instant = tokio::time::Instant::from_std(when);
                      tokio::time::sleep_until(tokio_instant)
                  },
                  None => tokio::time::sleep(Duration::from_secs(3600)), // 1 hour default
              };
              
              tokio::select! {
                  command = self.receiver.recv() => {
                      match command {
                          Some(cmd) => self.handle_command(cmd).await?,
                          None => break,
                      }
                  }
                  _ = timeout_future => {
                      self.process_due_retries().await?;
                  }
              }
          }
          Ok(())
      }
      
      async fn process_due_retries(&mut self) -> Result<()> {
          let now = Instant::now();
          let due_task_ids = self.task_set.pop_due_retries(now);
          
          for task_id in due_task_ids {
              if let Some(task) = self.task_set.get_task_mut(&task_id) {
                  if task.status == TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT {
                      self.start_task_attempt(task_id).await?;
                  }
              }
          }
          
          Ok(())
      }
  }
  ```

#### 3.2 Implement Automatic Retry Triggering
**Status: PENDING**
- Replace TODO at scheduler.rs:611 with retry scheduling:
  ```rust
  // In execute_handle_task_result function
  } else {
      // Automatic retry triggering
      if !task_result_data.is_final_failure {
          // Calculate retry delay and schedule
          if let Some(task) = task_set.get_task(&task_id) {
              match RetryPolicy::from_json(&task.retry_policy) {
                  Ok(retry_policy) => {
                      let attempt_number = task.attempts.len() as u32;
                      let delay = retry_policy.wait.calculate_delay(attempt_number);
                      let retry_time = Instant::now() + delay;
                      
                      task_set.schedule_retry(task_id, retry_time);
                      
                      info!("Scheduled retry for task {} in {} seconds", 
                            task_id, delay.as_secs_f64());
                  },
                  Err(e) => {
                      warn!("Failed to parse retry policy for task {}: {}. Using default.", task_id, e);
                      let default_policy = RetryPolicy::default();
                      let attempt_number = task.attempts.len() as u32;
                      let delay = default_policy.wait.calculate_delay(attempt_number);
                      let retry_time = Instant::now() + delay;
                      
                      task_set.schedule_retry(task_id, retry_time);
                  }
              }
          } else {
              error!("Task {} not found when scheduling retry", task_id);
          }
      }
  }
  ```

### Phase 4: Enhanced Retry Logic
**Status: PENDING**

#### 4.1 Implement Stop Conditions
**Status: PENDING**
- Modify `decide_handle_task_result` in scheduler.rs:
  ```rust
  fn decide_handle_task_result(
      task: &mut Task,
      task_id: Uuid,
      result: &TaskResult,
  ) -> Option<TaskResultData> {
      match &result.result_type {
          Some(common::task_result::ResultType::Error(error)) => {
              // Parse retry policy
              let retry_policy = RetryPolicy::from_json(&task.retry_policy)
                  .unwrap_or_default();
              
              // Check if error type should trigger retry
              if !should_retry_error(&retry_policy.retry, &error.r#type) {
                  info!("Error type '{}' not in retry list, failing task", error.r#type);
                  task.status = TASK_STATUS_FAILED;
                  return Some(TaskResultData {
                      flow_instance_id: task.flow_instance_id,
                      attempt_number: task.attempts.len() as i32,
                      is_final_failure: true,
                  });
              }
              
              let attempt_number = task.attempts.len() as i32;
              
              // Check max_attempts
              if let Some(max_attempts) = retry_policy.stop.max_attempts {
                  if attempt_number >= max_attempts as i32 {
                      info!("Max attempts ({}) reached for task {}", max_attempts, task_id);
                      task.status = TASK_STATUS_FAILED;
                      return Some(TaskResultData {
                          flow_instance_id: task.flow_instance_id,
                          attempt_number,
                          is_final_failure: true,
                      });
                  }
              }
              
              // Check max_delay
              if task.has_exceeded_max_delay(retry_policy.stop.max_delay) {
                  info!("Max delay exceeded for task {}", task_id);
                  task.status = TASK_STATUS_FAILED;
                  return Some(TaskResultData {
                      flow_instance_id: task.flow_instance_id,
                      attempt_number,
                      is_final_failure: true,
                  });
              }
              
              // Task can be retried
              task.status = TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT;
              Some(TaskResultData {
                  flow_instance_id: task.flow_instance_id,
                  attempt_number,
                  is_final_failure: false,
              })
          }
          // ... existing success handling
      }
  }
  ```

#### 4.2 Implement Retry Condition Evaluation
**Status: PENDING**
- Add retry condition helper function:
  ```rust
  fn should_retry_error(retry_config: &RetryConfig, error_type: &str) -> bool {
      // If error is in exclude list, don't retry
      if retry_config.exclude_errors.contains(&error_type.to_string()) {
          return false;
      }
      
      // If include list is empty, retry nothing
      if retry_config.include_errors.is_empty() {
          return false;
      }
      
      // Check if error is in include list
      retry_config.include_errors.contains(&error_type.to_string())
  }
  ```

### Phase 5: Validation and Integration
**Status: PENDING**

#### 5.1 Add Policy Validation to ClientService
**Status: PENDING**
- Modify `src/orchestrator/client_service.rs` create_task method:
  ```rust
  async fn create_task(
      &self,
      request: Request<CreateTaskRequest>,
  ) -> Result<Response<CreateTaskResponse>, Status> {
      let req = request.into_inner();
      
      // Validate retry policy
      let retry_policy_json: serde_json::Value = serde_json::from_str(&req.retry_policy)
          .map_err(|e| Status::invalid_argument(format!("Invalid retry_policy JSON: {e}")))?;
      
      let retry_policy = RetryPolicy::from_json(&retry_policy_json)
          .map_err(|e| Status::invalid_argument(format!("Invalid retry policy: {e}")))?;
      
      retry_policy.validate()
          .map_err(|e| Status::invalid_argument(format!("Retry policy validation failed: {e}")))?;
      
      // ... rest of existing create_task logic
  }
  ```

#### 5.2 Verify First Attempt Time Tracking
**Status: PENDING**
- Ensure task attempts are properly created with start_time in existing start_task logic
- No additional fields needed since attempts[0].start_time provides first attempt time

### Phase 6: Testing and Migration
**Status: PENDING**

#### 6.1 Add Unit Tests
**Status: PENDING**
- Create `src/orchestrator/retry_policy.rs` tests:
  ```rust
  #[cfg(test)]
  mod tests {
      use super::*;
      
      #[test]
      fn test_default_retry_policy() {
          let policy = RetryPolicy::default();
          assert_eq!(policy.version, 1);
          assert_eq!(policy.stop.max_attempts, Some(5));
          assert_eq!(policy.wait.strategy, WaitStrategy::ExponentialJitter);
      }
      
      #[test]
      fn test_fixed_wait_strategy() {
          let config = WaitConfig {
              strategy: WaitStrategy::Fixed,
              delay: Some(10.0),
              ..Default::default()
          };
          
          let delay = config.calculate_delay(1);
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
              ..Default::default()
          };
          
          assert_eq!(config.calculate_delay(1), Duration::from_secs(1));
          assert_eq!(config.calculate_delay(2), Duration::from_secs(2));
          assert_eq!(config.calculate_delay(3), Duration::from_secs(4));
          assert_eq!(config.calculate_delay(4), Duration::from_secs(8));
          assert_eq!(config.calculate_delay(5), Duration::from_secs(10)); // capped
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
      }
  }
  ```

#### 6.2 Migrate Test Code Retry Policies
**Status: PENDING**
- Update test helper functions in `src/test_harness.rs`:
  ```rust
  pub fn failing_task_with_retries() -> CreateTaskRequest {
      CreateTaskRequest {
          name: "failing_task".to_string(),
          domain: "test_domain".to_string(),
          args: vec![],
          kwargs: r#"{"should_fail": true}"#.to_string(),
          retry_policy: json!({
              "version": 1,
              "stop": {"max_attempts": 3},
              "wait": {
                  "strategy": "exponential",
                  "initial_delay": 1,
                  "multiplier": 2,
                  "max_delay": 60
              },
              "retry": {"include_errors": ["ValueError", "RuntimeError"]}
          }).to_string(),
          flow_instance_id: None,
      }
  }
  ```

- Update scheduler tests in `src/orchestrator/scheduler.rs`:
  ```rust
  fn create_test_task(_domain: &str, retry_policy: serde_json::Value) -> Task {
      Task {
          id: Uuid::new_v4(),
          name: "test_task".to_string(),
          created_at: Utc::now(),
          flow_instance_id: None,
          retry_policy,
          args: vec![],
          kwargs: json!({}),
          status: TASK_STATUS_CREATED,
          attempts: vec![],
          // Note: first_attempt_time available via attempts[0].start_time
      }
  }
  ```

## Implementation Checklist

### Phase 1: Core Type System ❌
- [ ] Create `src/orchestrator/retry_policy.rs`
- [ ] Define RetryPolicy, StopConfig, WaitConfig, RetryConfig structs
- [ ] Implement Default traits with spec defaults
- [ ] Implement wait strategy calculation
- [ ] Add JSON parsing and validation
- [ ] Add unit tests for type system

### Phase 2: Task Scheduling Infrastructure ❌
- [ ] Add BinaryHeap retry scheduler to TaskSet
- [ ] Implement retry scheduling methods  
- [ ] Add max_delay checking using existing attempts

### Phase 3: Scheduler Actor Enhancement ❌
- [ ] Add dynamic timeout to SchedulerActor loop
- [ ] Implement process_due_retries method
- [ ] Replace TODO at scheduler.rs:611 with retry scheduling

### Phase 4: Enhanced Retry Logic ❌
- [ ] Implement comprehensive stop conditions
- [ ] Implement retry condition evaluation
- [ ] Update decide_handle_task_result with new logic

### Phase 5: Validation and Integration ❌
- [ ] Add policy validation to ClientService
- [ ] Verify first attempt time tracking in existing logic
- [ ] Test end-to-end retry flow

### Phase 6: Testing and Migration ❌
- [ ] Add comprehensive unit tests
- [ ] Migrate test code retry policies
- [ ] Update test helper functions

## Dependencies and Imports

Required new dependencies in `Cargo.toml`:
```toml
[dependencies]
rand = "0.8"  # For jitter calculation
```

## Files to Modify

1. **New Files:**
   - `src/orchestrator/retry_policy.rs` - Core retry policy types and logic

2. **Modified Files:**
   - `src/orchestrator/taskset.rs` - Add retry scheduler and time tracking
   - `src/orchestrator/scheduler.rs` - Dynamic timeout and automatic retry
   - `src/orchestrator/client_service.rs` - Policy validation
   - `src/test_harness.rs` - Migrate test policies
   - `Cargo.toml` - Add rand dependency

## Risk Assessment

**Low Risk:**
- Type system and validation (isolated, testable)
- Wait strategy calculations (pure functions)
- Unit tests

**Medium Risk:**
- BinaryHeap integration (affects TaskSet interface)
- Timer modifications (affects scheduler loop)

**High Risk:**
- Automatic retry triggering (core scheduler logic)
- Policy migration (breaking changes to test code)

## Success Criteria

1. All retry policy features from spec implemented
2. Automatic retry triggering works correctly
3. All unit tests pass
4. No regression in existing retry behavior
5. Integration tests demonstrate full retry flow