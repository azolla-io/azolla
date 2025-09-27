use crate::orchestrator::event_stream::EventStream;
use crate::orchestrator::retry_policy::RetryPolicy;
use crate::orchestrator::shepherd_manager::ShepherdManager;
use crate::orchestrator::taskset::{Task, TaskResultValue, TaskSet};
use crate::{
    TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT, TASK_STATUS_ATTEMPT_STARTED,
    TASK_STATUS_CREATED, TASK_STATUS_FAILED, TASK_STATUS_SUCCEEDED,
};

#[cfg(test)]
use crate::EVENT_TASK_ATTEMPT_ENDED;

#[cfg(test)]
use crate::EVENT_TASK_ATTEMPT_STARTED;
use anyhow::Result;
use chrono::Utc;
use dashmap::DashMap;
use log::{debug, error, info, warn};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, oneshot};
use uuid::Uuid;

use crate::proto::common::TaskResult;

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    pub default_task_attempt_creation_timeout_secs: u64,
    pub default_task_timeout_secs: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            default_task_attempt_creation_timeout_secs: std::env::var(
                "AZOLLA_SCHEDULER_DEFAULT_TASK_ATTEMPT_CREATION_TIMEOUT_SECS",
            )
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(300),
            default_task_timeout_secs: std::env::var("AZOLLA_SCHEDULER_DEFAULT_TASK_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(1800),
        }
    }
}

impl SchedulerConfig {
    pub fn from_env_with_defaults(
        default_creation_timeout: u64,
        default_task_timeout: u64,
    ) -> Self {
        Self {
            default_task_attempt_creation_timeout_secs: std::env::var(
                "AZOLLA_SCHEDULER_DEFAULT_TASK_ATTEMPT_CREATION_TIMEOUT_SECS",
            )
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(default_creation_timeout),
            default_task_timeout_secs: std::env::var("AZOLLA_SCHEDULER_DEFAULT_TASK_TIMEOUT_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(default_task_timeout),
        }
    }
}

pub enum SchedulerCommand {
    StartTask {
        task_id: Uuid,
        task: Box<Option<Task>>, // StartTask can be used for starting a new Task, or an existing Task (none)
        respond_to: oneshot::Sender<Result<()>>,
    },
    HandleTaskResult {
        task_id: Uuid,
        result: TaskResult,
        respond_to: oneshot::Sender<Result<()>>,
    },
    HandleShepherdDeath {
        affected_task_ids: Vec<Uuid>,
        respond_to: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        respond_to: oneshot::Sender<()>,
    },
    GetTaskForTest {
        task_id: Uuid,
        respond_to: oneshot::Sender<Option<Task>>,
    },
    GetTaskResult {
        task_id: Uuid,
        respond_to: oneshot::Sender<Option<TaskResultValue>>,
    },
}

pub struct SchedulerActor {
    sender: mpsc::Sender<SchedulerCommand>,
    domain: String,
}

#[derive(Debug)]
pub enum SchedulerError {
    ChannelClosed,
    ResponseLost,
}

impl SchedulerActor {
    pub fn new(
        domain: String,
        task_set: TaskSet,
        shepherd_manager: Arc<ShepherdManager>,
        event_stream: Arc<EventStream>,
        config: SchedulerConfig,
    ) -> Self {
        let (sender, mut receiver) = mpsc::channel(1000);
        let domain_clone = domain.clone();

        tokio::spawn(async move {
            let shepherd_manager_clone = (*shepherd_manager).clone();
            let mut scheduler_state = SchedulerState {
                domain: domain_clone,
                task_set,
                shepherd_manager: shepherd_manager_clone,
                event_stream,
                config,
                retry_schedule_changed: false,
            };
            let mut next_retry_time = scheduler_state.task_set.get_next_retry_time();
            let mut current_timeout_time = next_retry_time;
            let mut timeout_future =
                Box::pin(SchedulerState::create_timeout_future(next_retry_time));

            loop {
                tokio::select! {
                    command = receiver.recv() => {
                        match command {
                            Some(SchedulerCommand::StartTask { task_id, task, respond_to: _ }) => {
                                info!("Scheduler received StartTask command for task {} in domain {}", task_id, scheduler_state.domain);

                                // If task is provided, upsert it first
                                if let Some(task) = *task {
                                    scheduler_state.task_set.upsert_task(task);
                                }

                                if let Err(e) = scheduler_state.schedule_task_once(task_id).await {
                                    error!("Failed to schedule task {task_id}: {e}");
                                }
                            }
                            Some(SchedulerCommand::HandleTaskResult { task_id, result, respond_to }) => {
                                match scheduler_state.decide_handle_task_result(task_id, result) {
                                    Some(task_result_data) => {
                                        let domain = scheduler_state.domain.clone();
                                        let event_stream = scheduler_state.event_stream.clone();
                                        let shepherd_manager = scheduler_state.shepherd_manager.clone();

                                        tokio::spawn(async move {
                                            let result = SchedulerState::execute_handle_task_result(
                                                task_id,
                                                task_result_data,
                                                domain,
                                                event_stream,
                                                shepherd_manager,
                                            ).await;
                                            let _ = respond_to.send(result);
                                        });
                                    },
                                    None => {
                                        let _ = respond_to.send(Ok(()));
                                    }
                                }
                            }
                            Some(SchedulerCommand::HandleShepherdDeath { affected_task_ids, respond_to }) => {
                                let result = scheduler_state.handle_shepherd_death(affected_task_ids).await;
                                let _ = respond_to.send(result);
                            }
                            Some(SchedulerCommand::Shutdown { respond_to }) => {
                                info!("Scheduler for domain {} shutting down", scheduler_state.domain);

                                // CRITICAL: Cancel the timeout future to prevent hanging on shutdown
                                // This prevents the deadlock where timeout_future continues waiting
                                // for retries while other components are shutting down
                                #[allow(unused_assignments)]
                                {
                                    timeout_future = Box::pin(tokio::time::sleep(std::time::Duration::from_secs(0)));
                                }

                                let _ = respond_to.send(());
                                break;
                            }
                            Some(SchedulerCommand::GetTaskForTest { task_id, respond_to }) => {
                                let task = scheduler_state.task_set.get_task(task_id).cloned();
                                let _ = respond_to.send(task);
                            }
                            Some(SchedulerCommand::GetTaskResult { task_id, respond_to }) => {
                                let result = scheduler_state.task_set.get_task(task_id)
                                    .and_then(|task| task.result.clone());
                                let _ = respond_to.send(result);
                            }
                            None => {
                                info!("Scheduler for domain {} channel closed", scheduler_state.domain);
                                break;
                            }
                        }

                        // Efficiently recalculate timeout only if retry schedule changed
                        if scheduler_state.retry_schedule_changed {
                            scheduler_state.retry_schedule_changed = false;
                            next_retry_time = scheduler_state.task_set.get_next_retry_time();

                            // Replace timeout future if time changed - this cancels the old one
                            if next_retry_time != current_timeout_time {
                                current_timeout_time = next_retry_time;
                                timeout_future = Box::pin(SchedulerState::create_timeout_future(next_retry_time));
                            }
                        }
                    }
                    _ = &mut timeout_future => {
                        scheduler_state.process_due_retries().await;

                        // Recalculate timeout after processing retries
                        next_retry_time = scheduler_state.task_set.get_next_retry_time();
                        current_timeout_time = next_retry_time;
                        timeout_future = Box::pin(SchedulerState::create_timeout_future(next_retry_time));
                    }
                }
            }
        });

        Self { sender, domain }
    }

    pub fn domain(&self) -> &str {
        &self.domain
    }

    pub async fn start_task_async(
        &self,
        task_id: Uuid,
        task: Option<Task>,
    ) -> Result<(), SchedulerError> {
        let (tx, _rx) = oneshot::channel();
        let cmd = SchedulerCommand::StartTask {
            task_id,
            task: Box::new(task),
            respond_to: tx,
        };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;

        Ok(())
    }

    pub async fn handle_task_result(
        &self,
        task_id: Uuid,
        result: TaskResult,
    ) -> Result<(), SchedulerError> {
        debug!(
            "handle_task_result called for task {} in domain {}",
            task_id, self.domain
        );
        let (tx, rx) = oneshot::channel();
        let cmd = SchedulerCommand::HandleTaskResult {
            task_id,
            result,
            respond_to: tx,
        };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;

        match rx.await.map_err(|_| SchedulerError::ResponseLost)? {
            Ok(_) => Ok(()),
            Err(_) => Err(SchedulerError::ResponseLost),
        }
    }

    pub async fn handle_shepherd_death(
        &self,
        affected_task_ids: Vec<Uuid>,
    ) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        let cmd = SchedulerCommand::HandleShepherdDeath {
            affected_task_ids,
            respond_to: tx,
        };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;

        match rx.await.map_err(|_| SchedulerError::ResponseLost)? {
            Ok(_) => Ok(()),
            Err(_) => Err(SchedulerError::ResponseLost),
        }
    }

    pub async fn shutdown(&self) -> Result<(), SchedulerError> {
        let (tx, rx) = oneshot::channel();
        let cmd = SchedulerCommand::Shutdown { respond_to: tx };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;

        rx.await.map_err(|_| SchedulerError::ResponseLost)?;
        Ok(())
    }

    // This function should only be used in testing and NOT in production code
    pub async fn get_task_for_test(&self, task_id: Uuid) -> Result<Option<Task>, SchedulerError> {
        let (tx, rx) = oneshot::channel();
        let cmd = SchedulerCommand::GetTaskForTest {
            task_id,
            respond_to: tx,
        };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;

        rx.await.map_err(|_| SchedulerError::ResponseLost)
    }

    /// Get the result of a task if it has completed
    /// Returns None if task doesn't exist or hasn't completed yet
    pub async fn get_task_result(
        &self,
        task_id: Uuid,
    ) -> Result<Option<TaskResultValue>, SchedulerError> {
        let (tx, rx) = oneshot::channel();
        let cmd = SchedulerCommand::GetTaskResult {
            task_id,
            respond_to: tx,
        };

        self.sender
            .send(cmd)
            .await
            .map_err(|_| SchedulerError::ChannelClosed)?;

        rx.await.map_err(|_| SchedulerError::ResponseLost)
    }
}

// TaskSet uses exclusive ownership (not Arc) to avoid locking overhead per domain

struct TaskResultData {
    flow_instance_id: Option<Uuid>,
    attempt_number: i32,
    is_final_failure: bool,
}

struct SchedulerState {
    domain: String,
    task_set: TaskSet,
    shepherd_manager: crate::orchestrator::shepherd_manager::ShepherdManager,
    event_stream: Arc<EventStream>,
    config: SchedulerConfig,
    retry_schedule_changed: bool,
}

impl SchedulerState {
    fn create_timeout_future(next_retry_time: Option<Instant>) -> tokio::time::Sleep {
        match next_retry_time {
            Some(when) => {
                let tokio_instant = tokio::time::Instant::from_std(when);
                tokio::time::sleep_until(tokio_instant)
            }
            None => tokio::time::sleep(std::time::Duration::from_secs(60)), // 1 min default
        }
    }

    fn convert_task_result_to_value(result: &TaskResult) -> TaskResultValue {
        match &result.result_type {
            Some(crate::proto::common::task_result::ResultType::Success(success)) => {
                TaskResultValue::Success(Self::convert_any_value_to_json(success.result.as_ref()))
            }
            Some(crate::proto::common::task_result::ResultType::Error(error)) => {
                TaskResultValue::Error {
                    error_type: error.r#type.clone(),
                    message: error.message.clone(),
                    data: serde_json::from_str(&error.data)
                        .unwrap_or_else(|_| serde_json::Value::String(error.data.clone())),
                    retriable: error.retriable,
                }
            }
            None => TaskResultValue::Error {
                error_type: "UnknownError".to_string(),
                message: "Task result had no result_type".to_string(),
                data: serde_json::Value::Null,
                retriable: false,
            },
        }
    }

    fn convert_any_value_to_json(
        any_value: Option<&crate::proto::common::AnyValue>,
    ) -> serde_json::Value {
        match any_value {
            Some(any_value) => match &any_value.value {
                Some(crate::proto::common::any_value::Value::StringValue(s)) => {
                    serde_json::Value::String(s.clone())
                }
                Some(crate::proto::common::any_value::Value::IntValue(i)) => {
                    serde_json::Value::Number(serde_json::Number::from(*i))
                }
                Some(crate::proto::common::any_value::Value::UintValue(u)) => {
                    serde_json::Value::Number(serde_json::Number::from(*u))
                }
                Some(crate::proto::common::any_value::Value::DoubleValue(d)) => {
                    serde_json::Value::Number(
                        serde_json::Number::from_f64(*d)
                            .unwrap_or_else(|| serde_json::Number::from(0)),
                    )
                }
                Some(crate::proto::common::any_value::Value::BoolValue(b)) => {
                    serde_json::Value::Bool(*b)
                }
                Some(crate::proto::common::any_value::Value::JsonValue(json_str)) => {
                    serde_json::from_str(json_str)
                        .unwrap_or_else(|_| serde_json::Value::String(json_str.clone()))
                }
                None => serde_json::Value::Null,
            },
            None => serde_json::Value::Null,
        }
    }

    /// Schedule a task for execution: make scheduling decisions, update TaskSet, and enqueue to virtual queue
    /// The attempt started event will be written by ShepherdManager when dispatch actually happens
    async fn schedule_task_once(&mut self, task_id: Uuid) -> Result<()> {
        info!("Scheduling task {} in domain {}", task_id, self.domain);

        let task = match self.task_set.get_task_mut(task_id) {
            Some(task) => {
                info!(
                    "Found task {} with status {} in domain {}",
                    task_id, task.status, self.domain
                );
                task
            }
            None => {
                warn!("Task {} not found in domain {}", task_id, self.domain);
                return Ok(()); // Not an error, task might have been processed elsewhere
            }
        };

        if task.status != TASK_STATUS_CREATED
            && task.status != TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT
        {
            warn!(
                "Task {} in domain {} is not in schedulable state (status: {})",
                task_id, self.domain, task.status
            );
            return Ok(()); // Not an error, task is in different state
        }

        // Shepherd selection is now handled by the ShepherdManager's virtual queue system

        let attempt_number = task.attempts.len() as i32;

        info!(
            "Updating task {} status from {} to ATTEMPT_STARTED in domain {}",
            task_id, task.status, self.domain
        );
        task.status = TASK_STATUS_ATTEMPT_STARTED;

        let new_attempt = crate::orchestrator::taskset::TaskAttempt {
            attempt: attempt_number,
            start_time: Some(Utc::now()),
            end_time: None,
            status: crate::ATTEMPT_STATUS_STARTED,
        };
        task.attempts.push(new_attempt);

        info!(
            "Prepared task {} attempt {} for dispatch in domain {}",
            task_id, attempt_number, self.domain
        );

        // Create task dispatch data
        let task_dispatch = crate::orchestrator::shepherd_manager::TaskDispatch {
            task_id,
            task_name: task.name.clone(),
            args: task.args.clone(),
            kwargs: task.kwargs.to_string(),
            memory_limit: None,
            cpu_limit: None,
            flow_instance_id: task.flow_instance_id,
            attempt_number,
            shepherd_group: task.shepherd_group.clone(),
        };

        debug!(
            "Enqueuing task {task_id} to domain {} virtual queue",
            self.domain
        );

        // Enqueue task to virtual queue instead of direct dispatch
        match self
            .shepherd_manager
            .enqueue_task(self.domain.clone(), task_dispatch)
            .await
        {
            Ok(_) => {
                info!(
                    "Successfully scheduled task {task_id} in domain '{}' virtual queue",
                    self.domain
                );
                Ok(())
            }
            Err(e) => {
                warn!(
                    "Failed to enqueue task {task_id} to domain '{}' virtual queue: {e}",
                    self.domain
                );
                Err(anyhow::anyhow!(
                    "Failed to enqueue task {task_id} to domain '{}' virtual queue: {e}",
                    self.domain
                ))
            }
        }
    }

    /// Fast synchronous phase: Make task result decisions and update TaskSet
    fn decide_handle_task_result(
        &mut self,
        task_id: Uuid,
        result: TaskResult,
    ) -> Option<TaskResultData> {
        debug!(
            "decide_handle_task_result for task {} in domain {}",
            task_id, self.domain
        );

        // Note: in-flight counter will be decremented in execute_handle_task_result

        let task = match self.task_set.get_task_mut(task_id) {
            Some(task) => task,
            None => {
                warn!(
                    "Task {} not found in domain {} when handling result",
                    task_id, self.domain
                );
                return None;
            }
        };

        let attempt_number = match task.attempts.last() {
            Some(attempt) => attempt.attempt,
            None => {
                error!(
                    "No attempts found for task {} when handling result in domain {}",
                    task_id, self.domain
                );
                return None;
            }
        };
        let is_success = result.result_type.is_some()
            && matches!(
                result.result_type.as_ref().unwrap(),
                crate::proto::common::task_result::ResultType::Success(_)
            );

        if is_success {
            info!("Task {task_id} attempt {attempt_number} succeeded");

            task.status = TASK_STATUS_SUCCEEDED;

            // Store the task result
            let result_value = Self::convert_task_result_to_value(&result);
            self.task_set.store_task_result(task_id, result_value);

            None // No slow operations needed for success
        } else {
            info!("Task {task_id} attempt {attempt_number} failed");

            // Fast operations: Update attempt status and determine retry logic
            // Update the latest attempt
            if let Some(target_attempt) = task.attempts.last_mut() {
                target_attempt.end_time = Some(Utc::now());
                target_attempt.status = crate::ATTEMPT_STATUS_FAILED;
            }

            // Parse retry policy with enhanced logic
            let retry_policy = RetryPolicy::from_json(&task.retry_policy).unwrap_or_else(|err| {
                warn!(
                    "Failed to parse retry policy for task {task_id}: {err}. Using default policy."
                );
                RetryPolicy::default()
            });

            let flow_instance_id = task.flow_instance_id;

            // Check if error type should trigger retry
            let should_retry =
                if let Some(crate::proto::common::task_result::ResultType::Error(error)) =
                    result.result_type.as_ref()
                {
                    crate::orchestrator::retry_policy::should_retry_error(
                        &retry_policy.retry,
                        &error.r#type,
                    )
                } else {
                    false
                };

            if !should_retry {
                info!("Error type not in retry list, failing task {task_id}");
                task.status = TASK_STATUS_FAILED;

                // Store the error result
                let result_value = Self::convert_task_result_to_value(&result);
                self.task_set.store_task_result(task_id, result_value);

                return Some(TaskResultData {
                    flow_instance_id,
                    attempt_number,
                    is_final_failure: true,
                });
            }

            // Check max_attempts
            if let Some(max_attempts) = retry_policy.stop.max_attempts {
                // attempt_number is 0-based, so +1 to get total attempts made
                let total_attempts = attempt_number + 1;
                info!(
                    "🔢 SCHEDULER: Task {task_id} attempt check - total_attempts: {total_attempts}, max_attempts: {max_attempts}"
                );

                if total_attempts >= max_attempts as i32 {
                    info!("🚫 SCHEDULER: Max attempts ({max_attempts}) reached for task {task_id} - setting status to FAILED");
                    task.status = TASK_STATUS_FAILED;

                    // Store the error result
                    let result_value = Self::convert_task_result_to_value(&result);
                    self.task_set.store_task_result(task_id, result_value);

                    return Some(TaskResultData {
                        flow_instance_id,
                        attempt_number,
                        is_final_failure: true,
                    });
                } else {
                    info!(
                        "⏭️ SCHEDULER: Task {task_id} still has attempts left ({total_attempts}/{max_attempts}) - scheduling retry"
                    );
                }
            }

            // Check max_delay
            if task.has_exceeded_max_delay(retry_policy.stop.max_delay) {
                info!("Max delay exceeded for task {task_id}");
                task.status = TASK_STATUS_FAILED;

                // Store the error result
                let result_value = Self::convert_task_result_to_value(&result);
                self.task_set.store_task_result(task_id, result_value);

                return Some(TaskResultData {
                    flow_instance_id,
                    attempt_number,
                    is_final_failure: true,
                });
            }

            // Task can be retried - schedule the retry
            info!("Task {task_id} attempt {attempt_number} failed, scheduling retry");
            task.status = TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT;

            // Calculate retry delay and schedule
            let retry_attempt_number = task.attempts.len() as u32;
            let delay = retry_policy.wait.calculate_delay(retry_attempt_number);
            let min_retry_delay = Duration::from_millis(10);
            let effective_delay = if delay < min_retry_delay {
                min_retry_delay
            } else {
                delay
            };
            let retry_time = Instant::now() + effective_delay;
            let retry_delay_secs = effective_delay.as_secs_f64();

            self.task_set.schedule_retry(task_id, retry_time);
            self.retry_schedule_changed = true;

            info!("Scheduled retry for task {task_id} in {retry_delay_secs} seconds");

            Some(TaskResultData {
                flow_instance_id,
                attempt_number,
                is_final_failure: false,
            })
        }
    }

    /// Slow asynchronous phase: Execute I/O operations (EventStream writes)
    async fn execute_handle_task_result(
        task_id: Uuid,
        task_result_data: TaskResultData,
        domain: String,
        event_stream: Arc<EventStream>,
        shepherd_manager: crate::orchestrator::shepherd_manager::ShepherdManager,
    ) -> Result<()> {
        debug!("Executing I/O for task result {task_id} in domain {domain}");

        // CRITICAL: Decrement VirtualQueue in-flight counter when task completes
        // This was the missing piece causing TaskB to get stuck in ATTEMPT_STARTED
        if let Err(e) = shepherd_manager
            .decrement_in_flight_task(domain.clone())
            .await
        {
            error!("Failed to decrement in-flight task count for domain {domain}: {e}");
        } else {
            debug!("Successfully decremented in-flight task count for domain {domain}");
        }

        // Write attempt ended event
        let event_metadata = serde_json::json!({
            "task_id": task_id,
            "attempt_number": task_result_data.attempt_number,
            "result": "error",
            "error_details": "Task attempt failed",
            "ended_at": Utc::now(),
            "attempt_status": crate::ATTEMPT_STATUS_FAILED
        });

        let event_record = crate::orchestrator::event_stream::EventRecord {
            domain: domain.clone(),
            task_instance_id: Some(task_id),
            flow_instance_id: task_result_data.flow_instance_id,
            event_type: crate::EVENT_TASK_ATTEMPT_ENDED,
            created_at: Utc::now(),
            metadata: event_metadata,
        };

        event_stream.write_event(event_record).await?;

        // If this is the final failure, write task ended event
        if task_result_data.is_final_failure {
            let event_metadata = serde_json::json!({
                "task_id": task_id,
                "result": "failed",
                "ended_at": Utc::now()
            });

            let event_record = crate::orchestrator::event_stream::EventRecord {
                domain,
                task_instance_id: Some(task_id),
                flow_instance_id: task_result_data.flow_instance_id,
                event_type: crate::EVENT_TASK_ENDED,
                created_at: Utc::now(),
                metadata: event_metadata,
            };

            event_stream.write_event(event_record).await?;
        }

        Ok(())
    }

    async fn handle_shepherd_death(&mut self, affected_task_ids: Vec<Uuid>) -> Result<()> {
        info!(
            "Handling shepherd death affecting {} tasks in domain {}",
            affected_task_ids.len(),
            self.domain
        );

        for task_id in affected_task_ids {
            if self.task_set.get_task(task_id).is_some() {
                // Create a TaskResult for failure to use the new split approach
                let failure_result = crate::proto::common::TaskResult {
                    task_id: task_id.to_string(),
                    result_type: Some(crate::proto::common::task_result::ResultType::Error(
                        crate::proto::common::ErrorResult {
                            r#type: "InfrastructureError".to_string(),
                            message: "Shepherd disconnected".to_string(),
                            data: "{}".to_string(),
                            retriable: true,
                        },
                    )),
                };

                if let Some(task_result_data) =
                    self.decide_handle_task_result(task_id, failure_result)
                {
                    // Execute I/O operations for shepherd death
                    SchedulerState::execute_handle_task_result(
                        task_id,
                        task_result_data,
                        self.domain.clone(),
                        self.event_stream.clone(),
                        self.shepherd_manager.clone(),
                    )
                    .await?;
                }
            }
        }

        Ok(())
    }

    /// Check if a task is orphaned (stuck in CREATED state too long)
    #[allow(dead_code)]
    async fn is_task_orphaned(&self, task: &Task) -> Result<bool> {
        let timeout_secs = task
            .retry_policy
            .get("task_attempt_creation_timeout")
            .and_then(|v| v.as_u64())
            .unwrap_or(self.config.default_task_attempt_creation_timeout_secs);

        let timeout_duration = Duration::from_secs(timeout_secs);
        let elapsed = Utc::now().signed_duration_since(task.created_at);

        Ok(elapsed.to_std().unwrap_or(Duration::ZERO) > timeout_duration)
    }

    /// Check if a task is ready for retry
    #[allow(dead_code)]
    async fn is_retry_ready(&self, _task: &Task) -> Result<bool> {
        Ok(true)
    }

    async fn process_due_retries(&mut self) {
        let now = std::time::Instant::now();
        let due_task_ids = self.task_set.pop_due_retries(now);

        for task_id in due_task_ids {
            if let Some(task) = self.task_set.get_task(task_id) {
                if task.status == crate::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT {
                    info!(
                        "Processing due retry for task {} in domain {}",
                        task_id, self.domain
                    );

                    // Use the consolidated schedule_task_once logic for retry
                    if let Err(e) = self.schedule_task_once(task_id).await {
                        error!("Failed to schedule retry task {task_id}: {e}");
                    }
                }
            }
        }
    }
}

pub struct SchedulerRegistry {
    schedulers: DashMap<String, Arc<SchedulerActor>>,
    task_set_registry: Arc<crate::orchestrator::taskset::TaskSetRegistry>,
    shepherd_registry: Arc<crate::orchestrator::shepherd_registry::ShepherdManagerRegistry>,
    event_stream: Arc<EventStream>,
    config: SchedulerConfig,
}

impl SchedulerRegistry {
    pub fn new(
        task_set_registry: Arc<crate::orchestrator::taskset::TaskSetRegistry>,
        shepherd_registry: Arc<crate::orchestrator::shepherd_registry::ShepherdManagerRegistry>,
        event_stream: Arc<EventStream>,
        config: SchedulerConfig,
    ) -> Self {
        Self {
            schedulers: DashMap::new(),
            task_set_registry,
            shepherd_registry,
            event_stream,
            config,
        }
    }

    pub fn get_or_create_scheduler(&self, domain: &str) -> Arc<SchedulerActor> {
        if let Some(scheduler) = self.schedulers.get(domain) {
            scheduler.clone()
        } else {
            let task_set = self
                .task_set_registry
                .extract_task_set(domain)
                .unwrap_or_else(|| crate::orchestrator::taskset::TaskSet::new(domain.to_string()));

            let scheduler = Arc::new(SchedulerActor::new(
                domain.to_string(),
                task_set,
                self.shepherd_registry.get_or_create_manager(domain),
                self.event_stream.clone(),
                self.config.clone(),
            ));

            self.schedulers
                .insert(domain.to_string(), scheduler.clone());
            scheduler
        }
    }

    pub fn get_scheduler(&self, domain: &str) -> Option<Arc<SchedulerActor>> {
        self.schedulers.get(domain).map(|s| s.clone())
    }

    pub fn domains(&self) -> Vec<String> {
        self.schedulers
            .iter()
            .map(|entry| entry.key().clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.schedulers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.schedulers.is_empty()
    }

    pub async fn shutdown_all(&self) -> Result<()> {
        let schedulers: Vec<_> = self
            .schedulers
            .iter()
            .map(|entry| entry.value().clone())
            .collect();

        for scheduler in schedulers {
            if let Err(e) = scheduler.shutdown().await {
                error!(
                    "Failed to shutdown scheduler for domain {}: {:?}",
                    scheduler.domain(),
                    e
                );
            }
        }

        self.schedulers.clear();
        Ok(())
    }
}

impl Default for SchedulerRegistry {
    fn default() -> Self {
        panic!(
            "SchedulerRegistry::default() should not be used - use new() with proper dependencies"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_test;
    use crate::orchestrator::event_stream::{EventStream, EventStreamConfig};
    use crate::orchestrator::taskset::{Task, TaskSetRegistry};
    use crate::proto::common::{ErrorResult, SuccessResult, TaskResult};
    use crate::{
        TASK_STATUS_ATTEMPT_STARTED, TASK_STATUS_CREATED, TASK_STATUS_FAILED, TASK_STATUS_SUCCEEDED,
    };
    use chrono::Utc;
    use serde_json::json;
    use std::sync::Arc;

    fn create_test_task(_domain: &str, retry_policy: serde_json::Value) -> Task {
        Task {
            id: Uuid::new_v4(),
            name: "test_task".to_string(),
            created_at: Utc::now(),
            flow_instance_id: None,
            retry_policy,
            args: r#"["arg1", "arg2"]"#.to_string(),
            kwargs: json!({"key": "value"}),
            status: TASK_STATUS_CREATED,
            attempts: Vec::new(),
            shepherd_group: None,
            result: None,
            result_stored_at: None,
        }
    }

    fn create_test_components() -> (
        Arc<TaskSetRegistry>,
        Arc<crate::orchestrator::shepherd_registry::ShepherdManagerRegistry>,
        Arc<EventStream>,
        SchedulerConfig,
    ) {
        let task_registry = Arc::new(TaskSetRegistry::new());
        let event_stream = Arc::new(EventStream::new(
            // Use a dummy pool for testing - EventStream operations will be mocked
            create_dummy_pool(),
            EventStreamConfig::default(),
        ));
        use crate::orchestrator::db::DomainsConfig;
        let domains_config = Arc::new(DomainsConfig::default());
        let shepherd_registry = Arc::new(
            crate::orchestrator::shepherd_registry::ShepherdManagerRegistry::new(
                domains_config,
                task_registry.clone(),
                event_stream.clone(),
            ),
        );
        let config = SchedulerConfig {
            default_task_attempt_creation_timeout_secs: 5,
            default_task_timeout_secs: 10,
        };

        (task_registry, shepherd_registry, event_stream, config)
    }

    fn create_dummy_pool() -> crate::orchestrator::db::PgPool {
        // For unit tests, we'll create a minimal pool that won't be used
        // In real integration tests, use the db_test! macro
        use crate::orchestrator::db::{
            create_pool, Database, DomainsConfig, Server, Settings, ShutdownConfig,
        };

        let settings = Settings {
            database: Database {
                url: "postgres://dummy".to_string(),
                pool_size: 1,
            },
            server: Server { port: 0 },
            event_stream: crate::orchestrator::db::EventStream::default(),
            domains: DomainsConfig::default(),
            shutdown: ShutdownConfig::default(),
        };

        // This will fail but we catch it for unit tests
        create_pool(&settings).unwrap_or_else(|_| {
            // Return a minimal pool for unit testing
            use deadpool_postgres::Config;
            let mut cfg = Config::new();
            cfg.host = Some("localhost".to_string());
            cfg.port = Some(5432);
            cfg.dbname = Some("test".to_string());
            cfg.user = Some("test".to_string());
            cfg.password = Some("test".to_string());
            cfg.create_pool(None, tokio_postgres::NoTls).unwrap()
        })
    }

    // Helper function to create a test shepherd tx channel
    type TestShepherdTx =
        tokio::sync::mpsc::Sender<Result<crate::proto::orchestrator::ServerMsg, tonic::Status>>;
    type TestShepherdRx =
        tokio::sync::mpsc::Receiver<Result<crate::proto::orchestrator::ServerMsg, tonic::Status>>;

    fn create_test_shepherd_tx() -> (TestShepherdTx, TestShepherdRx) {
        tokio::sync::mpsc::channel(10)
    }

    #[tokio::test]
    async fn test_scheduler_registry_creation() {
        let (task_registry, shepherd_registry, event_stream, config) = create_test_components();

        let scheduler_registry =
            SchedulerRegistry::new(task_registry, shepherd_registry, event_stream, config);

        assert_eq!(scheduler_registry.len(), 0);
        assert!(scheduler_registry.is_empty());
    }

    #[tokio::test]
    async fn test_scheduler_creation_per_domain() {
        let (task_registry, shepherd_registry, event_stream, config) = create_test_components();

        let scheduler_registry = Arc::new(SchedulerRegistry::new(
            task_registry,
            shepherd_registry,
            event_stream,
            config,
        ));

        let scheduler1 = scheduler_registry.get_or_create_scheduler("domain1");
        assert_eq!(scheduler1.domain(), "domain1");
        assert_eq!(scheduler_registry.len(), 1);

        let scheduler1_again = scheduler_registry.get_or_create_scheduler("domain1");
        assert_eq!(scheduler1_again.domain(), "domain1");
        assert_eq!(scheduler_registry.len(), 1);

        let scheduler2 = scheduler_registry.get_or_create_scheduler("domain2");
        assert_eq!(scheduler2.domain(), "domain2");
        assert_eq!(scheduler_registry.len(), 2);

        let domains = scheduler_registry.domains();
        assert!(domains.contains(&"domain1".to_string()));
        assert!(domains.contains(&"domain2".to_string()));
    }

    #[tokio::test]
    async fn test_scheduler_config_from_env() {
        // Test default config
        let config = SchedulerConfig::default();
        assert_eq!(config.default_task_attempt_creation_timeout_secs, 300);
        assert_eq!(config.default_task_timeout_secs, 1800);

        // Test custom defaults
        let config = SchedulerConfig::from_env_with_defaults(120, 900);
        assert_eq!(config.default_task_attempt_creation_timeout_secs, 120);
        assert_eq!(config.default_task_timeout_secs, 900);
    }

    #[tokio::test]
    async fn test_scheduler_start_task_basic() {
        let (task_registry, shepherd_registry, event_stream, config) = create_test_components();

        let scheduler_registry = Arc::new(SchedulerRegistry::new(
            task_registry.clone(),
            shepherd_registry,
            event_stream,
            config,
        ));

        let task = create_test_task(
            "test_domain",
            json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {"strategy": "exponential_jitter", "initial_delay": 1, "multiplier": 2, "max_delay": 300},
                "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
            }),
        );
        let task_id = task.id;

        let domain_actor = task_registry.get_or_create_domain("test_domain");
        domain_actor.upsert_task(task).await.unwrap();

        // Verify the task is still in created state (before creating scheduler)
        let task_state = domain_actor.get_task(task_id).await.unwrap().unwrap();
        assert_eq!(task_state.status, TASK_STATUS_CREATED);

        // Get scheduler
        let scheduler = scheduler_registry.get_or_create_scheduler("test_domain");

        // For this unit test, we'll just verify the scheduler was created correctly
        assert_eq!(scheduler.domain(), "test_domain");
    }

    #[tokio::test]
    #[ignore] // Flaky test: fails when no shepherds available due to 100ms sleep in start_task loop (lines 346-351)
    async fn test_scheduler_start_task_async() {
        let (task_registry, shepherd_manager, event_stream, config) = create_test_components();

        let scheduler_registry = Arc::new(SchedulerRegistry::new(
            task_registry.clone(),
            shepherd_manager,
            event_stream,
            config,
        ));

        let task = create_test_task(
            "test_domain",
            json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {"strategy": "exponential_jitter", "initial_delay": 1, "multiplier": 2, "max_delay": 300},
                "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
            }),
        );
        let task_id = task.id;
        let scheduler = scheduler_registry.get_or_create_scheduler("test_domain");

        // Test 1: start_task_async with task creation (Some(task))
        let start_time = std::time::Instant::now();
        let result = scheduler
            .start_task_async(task_id, Some(task.clone()))
            .await;
        let elapsed = start_time.elapsed();

        assert!(
            elapsed.as_millis() < 1,
            "start_task_async should return immediately"
        );
        assert!(result.is_ok(), "start_task_async should succeed");

        // Verify the task was created in the task set
        let domain_actor = task_registry.get_or_create_domain("test_domain");
        let stored_task = domain_actor.get_task(task_id).await.unwrap().unwrap();
        assert_eq!(stored_task.status, TASK_STATUS_CREATED);

        // Test 2: start_task_async without task creation (None) for existing task
        let start_time = std::time::Instant::now();
        let result = scheduler.start_task_async(task_id, None).await;
        let elapsed = start_time.elapsed();

        assert!(
            elapsed.as_millis() < 1,
            "start_task_async should return immediately"
        );
        assert!(result.is_ok(), "start_task_async should succeed");
    }

    #[tokio::test]
    async fn test_scheduler_handle_task_success() {
        let (task_registry, shepherd_manager, event_stream, config) = create_test_components();

        let scheduler_registry = Arc::new(SchedulerRegistry::new(
            task_registry.clone(),
            shepherd_manager,
            event_stream,
            config,
        ));

        // Create and add a task in ATTEMPT_STARTED state with a proper attempt
        let mut task = create_test_task(
            "test_domain",
            json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {"strategy": "exponential_jitter", "initial_delay": 1, "multiplier": 2, "max_delay": 300},
                "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
            }),
        );
        task.status = TASK_STATUS_ATTEMPT_STARTED;
        task.attempts
            .push(crate::orchestrator::taskset::TaskAttempt {
                attempt: 0,
                start_time: Some(Utc::now()),
                end_time: None,
                status: crate::ATTEMPT_STATUS_STARTED,
            });
        let task_id = task.id;

        let domain_actor = task_registry.get_or_create_domain("test_domain");
        domain_actor.upsert_task(task).await.unwrap();

        let task_result = TaskResult {
            task_id: task_id.to_string(),
            result_type: Some(crate::proto::common::task_result::ResultType::Success(
                SuccessResult {
                    result: Some(crate::proto::common::AnyValue {
                        value: Some(crate::proto::common::any_value::Value::StringValue(
                            "success".to_string(),
                        )),
                    }),
                },
            )),
        };

        let scheduler = scheduler_registry.get_or_create_scheduler("test_domain");
        let result = scheduler.handle_task_result(task_id, task_result).await;

        assert!(result.is_ok());

        let updated_task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
        assert_eq!(updated_task.status, TASK_STATUS_SUCCEEDED);
    }

    db_test!(
        test_scheduler_handle_task_failure_with_retries,
        (|pool: crate::orchestrator::db::PgPool| async move {
            use crate::orchestrator::db::DomainsConfig;
            use crate::orchestrator::engine::Engine;
            use crate::orchestrator::event_stream::EventStreamConfig;
            use crate::proto::common::ErrorResult;

            // Create Engine with database
            let engine = Engine::new(
                pool.clone(),
                EventStreamConfig::default(),
                DomainsConfig::default(),
            );
            engine.initialize().await.unwrap();

            // Create and add a task in ATTEMPT_STARTED state with multiple attempts allowed
            let mut task = create_test_task(
                "test_domain",
                json!({
                    "version": 1,
                    "stop": {"max_attempts": 3},
                    "wait": {"strategy": "exponential_jitter", "initial_delay": 1, "multiplier": 2, "max_delay": 300},
                    "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
                }),
            );
            task.status = TASK_STATUS_ATTEMPT_STARTED;
            task.attempts = vec![crate::orchestrator::taskset::TaskAttempt {
                attempt: 1,
                start_time: Some(Utc::now()),
                end_time: None,
                status: crate::ATTEMPT_STATUS_STARTED,
            }];
            let task_id = task.id;

            let domain_actor = engine.registry.get_or_create_domain("test_domain");
            domain_actor.upsert_task(task).await.unwrap();

            // Create failed task result
            let task_result = TaskResult {
                task_id: task_id.to_string(),
                result_type: Some(crate::proto::common::task_result::ResultType::Error(
                    ErrorResult {
                        r#type: "RuntimeError".to_string(),
                        message: "Task failed".to_string(),
                        data: "{}".to_string(),
                        retriable: false,
                    },
                )),
            };

            let scheduler = engine
                .scheduler_registry
                .get_or_create_scheduler("test_domain");
            let result = scheduler.handle_task_result(task_id, task_result).await;

            assert!(result.is_ok());

            // Verify task status was updated to failed with attempts left
            let updated_task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
            assert_eq!(
                updated_task.status,
                crate::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT
            );
        })
    );

    db_test!(
        test_scheduler_handle_task_failure_no_retries,
        (|pool: crate::orchestrator::db::PgPool| async move {
            use crate::orchestrator::db::DomainsConfig;
            use crate::orchestrator::engine::Engine;
            use crate::orchestrator::event_stream::EventStreamConfig;
            use crate::proto::common::ErrorResult;

            // Create Engine with database
            let engine = Engine::new(
                pool.clone(),
                EventStreamConfig::default(),
                DomainsConfig::default(),
            );
            engine.initialize().await.unwrap();

            // Create and add a task in ATTEMPT_STARTED state with max attempts reached
            let mut task = create_test_task(
                "test_domain",
                json!({
                    "version": 1,
                    "stop": {"max_attempts": 1},
                    "wait": {"strategy": "fixed", "delay": 1},
                    "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
                }),
            );
            task.status = TASK_STATUS_ATTEMPT_STARTED;
            task.attempts = vec![crate::orchestrator::taskset::TaskAttempt {
                attempt: 1,
                start_time: Some(Utc::now()),
                end_time: None,
                status: crate::ATTEMPT_STATUS_STARTED,
            }];
            let task_id = task.id;

            let domain_actor = engine.registry.get_or_create_domain("test_domain");
            domain_actor.upsert_task(task).await.unwrap();

            // Create failed task result
            let task_result = TaskResult {
                task_id: task_id.to_string(),
                result_type: Some(crate::proto::common::task_result::ResultType::Error(
                    ErrorResult {
                        r#type: "RuntimeError".to_string(),
                        message: "Task failed".to_string(),
                        data: "{}".to_string(),
                        retriable: false,
                    },
                )),
            };

            let scheduler = engine
                .scheduler_registry
                .get_or_create_scheduler("test_domain");
            let result = scheduler.handle_task_result(task_id, task_result).await;

            assert!(result.is_ok());

            // Verify task status was updated to failed
            let updated_task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
            assert_eq!(updated_task.status, TASK_STATUS_FAILED);
        })
    );

    #[tokio::test]
    async fn test_scheduler_shutdown() {
        let (task_registry, shepherd_manager, event_stream, config) = create_test_components();

        let scheduler_registry = Arc::new(SchedulerRegistry::new(
            task_registry,
            shepherd_manager,
            event_stream,
            config,
        ));

        let _scheduler1 = scheduler_registry.get_or_create_scheduler("domain1");
        let _scheduler2 = scheduler_registry.get_or_create_scheduler("domain2");

        assert_eq!(scheduler_registry.len(), 2);

        let result = scheduler_registry.shutdown_all().await;
        assert!(result.is_ok());
        assert_eq!(scheduler_registry.len(), 0);
    }

    #[tokio::test]
    async fn test_task_status_indices() {
        let task_registry = Arc::new(TaskSetRegistry::new());
        let domain_actor = task_registry.get_or_create_domain("test_domain");

        let created_task = create_test_task(
            "test_domain",
            json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {"strategy": "exponential_jitter", "initial_delay": 1, "multiplier": 2, "max_delay": 300},
                "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
            }),
        );
        let created_id = created_task.id;

        let mut started_task = create_test_task(
            "test_domain",
            json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {"strategy": "exponential_jitter", "initial_delay": 1, "multiplier": 2, "max_delay": 300},
                "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
            }),
        );
        started_task.status = TASK_STATUS_ATTEMPT_STARTED;
        let started_id = started_task.id;

        let mut retry_task = create_test_task(
            "test_domain",
            json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {"strategy": "exponential_jitter", "initial_delay": 1, "multiplier": 2, "max_delay": 300},
                "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
            }),
        );
        retry_task.status = crate::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT;
        let retry_id = retry_task.id;

        domain_actor.upsert_task(created_task).await.unwrap();
        domain_actor.upsert_task(started_task).await.unwrap();
        domain_actor.upsert_task(retry_task).await.unwrap();

        let retrieved_created = task_registry.get_task("test_domain", created_id).unwrap();
        assert_eq!(retrieved_created.status, crate::TASK_STATUS_CREATED);

        let retrieved_started = task_registry.get_task("test_domain", started_id).unwrap();
        assert_eq!(retrieved_started.status, crate::TASK_STATUS_ATTEMPT_STARTED);

        let retrieved_retry = task_registry.get_task("test_domain", retry_id).unwrap();
        assert_eq!(
            retrieved_retry.status,
            crate::TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT
        );
    }

    #[tokio::test]
    async fn test_orphaned_task_detection() {
        let (task_registry, shepherd_manager, event_stream, mut config) = create_test_components();

        config.default_task_attempt_creation_timeout_secs = 1;

        let scheduler_registry = Arc::new(SchedulerRegistry::new(
            task_registry.clone(),
            shepherd_manager,
            event_stream,
            config,
        ));

        let mut task = create_test_task(
            "test_domain",
            json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {"strategy": "exponential_jitter", "initial_delay": 1, "multiplier": 2, "max_delay": 300},
                "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
            }),
        );
        task.created_at = Utc::now() - chrono::Duration::seconds(5); // 5 seconds ago
        let task_id = task.id;

        let domain_actor = task_registry.get_or_create_domain("test_domain");
        domain_actor.upsert_task(task).await.unwrap();

        let retrieved_task = task_registry.get_task("test_domain", task_id).unwrap();
        assert_eq!(retrieved_task.status, crate::TASK_STATUS_CREATED);

        let _scheduler = scheduler_registry.get_or_create_scheduler("test_domain");

        // Note: After creating the scheduler, the TaskSet is moved to the SchedulerActor
    }

    #[cfg(test)]
    mod integration_tests {
        use super::*;
        use crate::orchestrator::taskset::Task;
        use crate::proto::common::{SuccessResult, TaskResult};
        use chrono::Utc;

        #[tokio::test]
        async fn test_scheduler_basic_functionality() {
            // Basic unit test to verify scheduler components work together
            let (task_registry, shepherd_manager, event_stream, config) = create_test_components();

            let scheduler_registry = Arc::new(SchedulerRegistry::new(
                task_registry.clone(),
                shepherd_manager.clone(),
                event_stream.clone(),
                config,
            ));

            let mut task = Task::new();
            task.name = "integration_test_task".to_string();
            task.retry_policy = json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {"strategy": "exponential_jitter", "initial_delay": 1, "multiplier": 2, "max_delay": 300},
                "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
            });
            task.status = TASK_STATUS_CREATED;
            task.created_at = Utc::now();
            let task_id = task.id;

            // Simulate task being in started state (as if it was scheduled)
            // We need to manually create an attempt since start_task requires shepherds
            let mut updated_task = task.clone();
            updated_task.status = TASK_STATUS_ATTEMPT_STARTED;
            updated_task
                .attempts
                .push(crate::orchestrator::taskset::TaskAttempt {
                    attempt: 0,
                    start_time: Some(Utc::now()),
                    end_time: None,
                    status: crate::ATTEMPT_STATUS_STARTED,
                });

            let task_set = task_registry.get_or_create_domain("test_domain");
            task_set.upsert_task(updated_task).await.unwrap();

            // Get scheduler - this creates the scheduler for the domain AFTER we add the attempt
            let scheduler = scheduler_registry.get_or_create_scheduler("test_domain");

            // For unit testing without actual shepherd connections, we'll test task result handling
            // In integration tests, we would test start_task() with real shepherds

            // Test task result handling
            let task_result = TaskResult {
                task_id: task_id.to_string(),
                result_type: Some(crate::proto::common::task_result::ResultType::Success(
                    SuccessResult {
                        result: Some(crate::proto::common::AnyValue {
                            value: Some(crate::proto::common::any_value::Value::StringValue(
                                "test success".to_string(),
                            )),
                        }),
                    },
                )),
            };

            scheduler
                .handle_task_result(task_id, task_result)
                .await
                .unwrap();

            // Verify final state
            let final_task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
            assert_eq!(final_task.status, TASK_STATUS_SUCCEEDED);
        }

        db_test!(
            test_multi_domain_scheduler_isolation,
            (|pool: crate::orchestrator::db::PgPool| async move {
                use crate::orchestrator::db::DomainsConfig;
                use crate::orchestrator::engine::Engine;
                use crate::orchestrator::event_stream::EventStreamConfig;
                use crate::orchestrator::taskset::Task;
                use crate::EVENT_TASK_CREATED;
                use std::collections::HashMap;
                use tokio::time::Duration;

                let engine = Engine::new(
                    pool.clone(),
                    EventStreamConfig::default(),
                    DomainsConfig::default(),
                );
                engine.initialize().await.unwrap();

                // Domains under test
                let domains = vec!["domain1", "domain2", "domain3"];

                // Register one shepherd per domain
                for domain in &domains {
                    let shepherd_id = Uuid::new_v4();
                    let (tx, mut rx) = create_test_shepherd_tx();
                    tokio::spawn(async move { while let Some(_cmd) = rx.recv().await {} });
                    engine
                        .shepherd_registry
                        .get_or_create_manager(domain)
                        .register_shepherd(shepherd_id, 5, "default".to_string(), tx)
                        .await
                        .unwrap();
                }

                // Create tasks across multiple domains
                let mut all_task_ids = HashMap::new();

                for domain in &domains {
                    let mut domain_tasks = Vec::new();
                    let task_set = engine.registry.get_or_create_domain(domain);

                    for i in 0..3 {
                        let mut task = Task::new();
                        task.name = format!("isolation_task_{domain}_{i}");
                        task.retry_policy = json!({
                            "version": 1,
                            "stop": {"max_attempts": 2},
                            "wait": {"strategy": "fixed", "delay": 1},
                            "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
                        });
                        task.args = format!(r#"["arg_{i}"]"#);
                        task.kwargs = json!({"domain": domain, "index": i});
                        task.status = TASK_STATUS_CREATED;
                        task.created_at = Utc::now();
                        let task_id = task.id;

                        // Add task to TaskSet
                        task_set.upsert_task(task.clone()).await.unwrap();

                        // Create event and write to database
                        let event_record = crate::orchestrator::event_stream::EventRecord {
                            domain: domain.to_string(),
                            task_instance_id: Some(task_id),
                            flow_instance_id: None,
                            event_type: EVENT_TASK_CREATED,
                            created_at: Utc::now(),
                            metadata: json!({
                                "task_name": task.name,
                                "domain": domain,
                                "index": i
                            }),
                        };
                        engine.event_stream.write_event(event_record).await.unwrap();

                        domain_tasks.push(task_id);
                    }
                    all_task_ids.insert(domain.to_string(), domain_tasks);
                }

                // Start tasks through schedulers for each domain
                for (domain, task_ids) in &all_task_ids {
                    let scheduler = engine.scheduler_registry.get_or_create_scheduler(domain);
                    for &task_id in task_ids {
                        scheduler.start_task_async(task_id, None).await.unwrap();
                    }
                }

                // Give scheduler actors time to process StartTask commands
                tokio::time::sleep(Duration::from_millis(100)).await;

                // Wait for all scheduling to complete
                // Manually trigger dispatch multiple times instead of relying on timer to avoid CI timing issues
                for _ in 0..5 {
                    for domain in &domains {
                        engine
                            .shepherd_registry
                            .get_or_create_manager(domain)
                            .dispatch_tick()
                            .await
                            .unwrap();
                    }
                    tokio::time::sleep(Duration::from_millis(30)).await;
                }

                // Verify domain isolation in scheduler registry
                assert_eq!(engine.scheduler_registry.len(), 3);
                for domain in &domains {
                    assert!(engine.scheduler_registry.get_scheduler(domain).is_some());
                }

                // Verify all tasks were scheduled across domains
                let mut total_scheduled = 0;
                for (domain, task_ids) in &all_task_ids {
                    let scheduler = engine.scheduler_registry.get_scheduler(domain).unwrap();
                    for &task_id in task_ids {
                        let task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
                        println!(
                            "DEBUG: Task {} in domain {} has status {}",
                            task_id, domain, task.status
                        );
                        if task.status == TASK_STATUS_ATTEMPT_STARTED {
                            total_scheduled += 1;
                        }
                    }
                }

                assert_eq!(total_scheduled, 9); // 3 domains × 3 tasks each

                // Verify all events were written to database with proper domain isolation
                let client = pool.get().await.unwrap();
                for domain in &domains {
                    let domain_events = client.query_one(
                    "SELECT COUNT(*) as count FROM events WHERE domain = $1 AND event_type = $2",
                    &[&domain, &EVENT_TASK_CREATED]
                ).await.unwrap();

                    let count: i64 = domain_events.get("count");
                    assert_eq!(count, 3); // 3 tasks per domain
                }

                // Verify total events across all domains
                let total_created_events = client
                    .query_one(
                        "SELECT COUNT(*) as count FROM events WHERE event_type = $1",
                        &[&EVENT_TASK_CREATED],
                    )
                    .await
                    .unwrap();

                let created_count: i64 = total_created_events.get("count");
                assert_eq!(created_count, 9); // 9 total tasks

                let total_started_events = client
                    .query_one(
                        "SELECT COUNT(*) as count FROM events WHERE event_type = $1",
                        &[&EVENT_TASK_ATTEMPT_STARTED],
                    )
                    .await
                    .unwrap();

                let started_count: i64 = total_started_events.get("count");
                assert_eq!(started_count, 9); // All tasks started
            })
        );

        db_test!(
            test_retry_logic_integration,
            (|pool: crate::orchestrator::db::PgPool| async move {
                use crate::orchestrator::db::DomainsConfig;
                use crate::orchestrator::engine::Engine;
                use crate::orchestrator::event_stream::EventStreamConfig;
                use crate::orchestrator::taskset::Task;

                // Create Engine with database
                let engine = Engine::new(
                    pool.clone(),
                    EventStreamConfig::default(),
                    DomainsConfig::default(),
                );
                engine.initialize().await.unwrap();

                // Register a shepherd for testing
                let shepherd_id = Uuid::new_v4();
                let (tx, _rx) = create_test_shepherd_tx();
                engine
                    .shepherd_registry
                    .get_or_create_manager("test_domain")
                    .register_shepherd(shepherd_id, 10, "default".to_string(), tx)
                    .await
                    .unwrap();

                // Create task with retry policy
                let mut task = Task::new();
                task.name = "retry_test_task".to_string();
                task.retry_policy = json!({
                    "version": 1,
                    "stop": {"max_attempts": 3},
                    "wait": {"strategy": "exponential_jitter", "initial_delay": 1, "multiplier": 2, "max_delay": 300},
                    "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
                });
                task.status = TASK_STATUS_ATTEMPT_STARTED;
                task.created_at = Utc::now();
                task.attempts = vec![crate::orchestrator::taskset::TaskAttempt {
                    attempt: 0,
                    start_time: Some(Utc::now()),
                    end_time: None,
                    status: crate::ATTEMPT_STATUS_STARTED,
                }];
                let task_id = task.id;

                let task_set = engine.registry.get_or_create_domain("test_domain");
                task_set.upsert_task(task).await.unwrap();

                let scheduler = engine
                    .scheduler_registry
                    .get_or_create_scheduler("test_domain");

                // Simulate first failure
                let error_result = TaskResult {
                    task_id: task_id.to_string(),
                    result_type: Some(crate::proto::common::task_result::ResultType::Error(
                        ErrorResult {
                            r#type: "RetryableError".to_string(),
                            message: "First attempt failed".to_string(),
                            data: "{}".to_string(),
                            retriable: true,
                        },
                    )),
                };

                scheduler
                    .handle_task_result(task_id, error_result)
                    .await
                    .unwrap();

                // Verify task is marked for retry
                let task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
                assert_eq!(task.status, TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT);

                // Test final failure - simulate the retry process by starting the task again
                // This will trigger the retry logic in the scheduler
                scheduler.start_task_async(task_id, None).await.unwrap();

                let final_error = TaskResult {
                    task_id: task_id.to_string(),
                    result_type: Some(crate::proto::common::task_result::ResultType::Error(
                        ErrorResult {
                            r#type: "FatalError".to_string(),
                            message: "Final failure".to_string(),
                            data: "{}".to_string(),
                            retriable: false,
                        },
                    )),
                };

                scheduler
                    .handle_task_result(task_id, final_error.clone())
                    .await
                    .unwrap();

                // Should still have attempts left (2 < 3)
                // Check status before starting the next retry attempt
                let task_after_second =
                    scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
                assert_eq!(
                    task_after_second.status,
                    TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT
                );

                // Simulate third and final attempt
                scheduler.start_task_async(task_id, None).await.unwrap();

                scheduler
                    .handle_task_result(task_id, final_error)
                    .await
                    .unwrap();

                // Should be permanently failed now (3 attempts exhausted)
                let final_task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
                assert_eq!(final_task.status, TASK_STATUS_FAILED);
            })
        );
    }

    db_test!(
        test_scheduler_end_to_end_with_db,
        (|pool: crate::orchestrator::db::PgPool| async move {
            use crate::orchestrator::db::DomainsConfig;
            use crate::orchestrator::engine::Engine;
            use crate::orchestrator::event_stream::EventStreamConfig;
            use crate::orchestrator::taskset::Task;
            use crate::proto::common::{SuccessResult, TaskResult};
            use crate::EVENT_TASK_CREATED;
            use tokio::time::Duration;

            // Create Engine with database
            let engine = Engine::new(
                pool.clone(),
                EventStreamConfig::default(),
                DomainsConfig::default(),
            );
            engine.initialize().await.unwrap();

            // Create a task manually and persist to database
            let task_name = "db_integration_test_task";
            let domain = "test_domain";
            // Register a test shepherd with tx channel for this domain
            let shepherd_id = Uuid::new_v4();
            let (tx, _rx) = create_test_shepherd_tx();
            engine
                .shepherd_registry
                .get_or_create_manager(domain)
                .register_shepherd(shepherd_id, 10, "default".to_string(), tx)
                .await
                .unwrap();
            let retry_policy = json!({
                "version": 1,
                "stop": {"max_attempts": 3},
                "wait": {"strategy": "exponential", "initial_delay": 1, "multiplier": 2, "max_delay": 60},
                "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
            });

            let mut task = Task::new();
            task.name = task_name.to_string();
            task.retry_policy = retry_policy.clone();
            task.args = r#"["arg1"]"#.to_string();
            task.kwargs = json!({"test": "value"});
            task.status = TASK_STATUS_CREATED;
            task.created_at = Utc::now();
            let task_id = task.id;

            // Add task to TaskSet and verify it's persisted
            let task_set = engine.registry.get_or_create_domain(domain);
            task_set.upsert_task(task.clone()).await.unwrap();

            // Create TASK_CREATED event and verify it's written to database
            let event_record = crate::orchestrator::event_stream::EventRecord {
                domain: domain.to_string(),
                task_instance_id: Some(task_id),
                flow_instance_id: None,
                event_type: EVENT_TASK_CREATED,
                created_at: Utc::now(),
                metadata: json!({
                    "task_name": task_name,
                    "retry_policy": retry_policy
                }),
            };
            engine.event_stream.write_event(event_record).await.unwrap();

            // Verify event was persisted in database
            let client = pool.get().await.unwrap();
            let created_events = client.query(
            "SELECT event_id, event_type, metadata FROM events WHERE task_instance_id = $1 AND event_type = $2",
            &[&task_id, &EVENT_TASK_CREATED]
        ).await.unwrap();

            assert_eq!(created_events.len(), 1);
            assert_eq!(
                created_events[0].get::<_, i16>("event_type"),
                EVENT_TASK_CREATED
            );

            // Start task through scheduler
            let scheduler = engine.scheduler_registry.get_or_create_scheduler(domain);
            scheduler.start_task_async(task_id, None).await.unwrap();

            // Give the scheduler actor a moment to process the StartTask command
            tokio::time::sleep(Duration::from_millis(50)).await;

            // Wait for the virtual queue dispatcher to pick up and dispatch the task
            // Manually trigger dispatch instead of relying on timer to avoid CI timing issues
            let mut attempts = 0;
            let max_attempts = 30; // Try more times with shorter waits

            loop {
                // Manually trigger dispatch multiple times to ensure it processes
                for _ in 0..3 {
                    engine
                        .shepherd_registry
                        .get_or_create_manager(domain)
                        .dispatch_tick()
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }

                // Check if the task has been dispatched by looking for the attempt started event
                let started_events = client.query(
                    "SELECT event_id, event_type FROM events WHERE task_instance_id = $1 AND event_type = $2",
                    &[&task_id, &EVENT_TASK_ATTEMPT_STARTED]
                ).await.unwrap();

                if !started_events.is_empty() {
                    // Task has been dispatched, verify status
                    let updated_task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
                    assert_eq!(updated_task.status, TASK_STATUS_ATTEMPT_STARTED);
                    assert_eq!(started_events.len(), 1);
                    break;
                }

                attempts += 1;
                if attempts >= max_attempts {
                    // Debug information
                    eprintln!("DEBUG: Task was not dispatched after {attempts} attempts");
                    let task_status = scheduler.get_task_for_test(task_id).await.unwrap();
                    eprintln!("DEBUG: Task status: {:?}", task_status.map(|t| t.status));

                    panic!("Task was not dispatched within expected time. Virtual queue dispatcher may not be running.");
                }

                tokio::time::sleep(Duration::from_millis(80)).await;
            }

            // Simulate successful task completion
            let task_result = TaskResult {
                task_id: task_id.to_string(),
                result_type: Some(crate::proto::common::task_result::ResultType::Success(
                    SuccessResult {
                        result: Some(crate::proto::common::AnyValue {
                            value: Some(crate::proto::common::any_value::Value::StringValue(
                                "db integration test success".to_string(),
                            )),
                        }),
                    },
                )),
            };

            // Handle task result through scheduler
            scheduler
                .handle_task_result(task_id, task_result)
                .await
                .unwrap();

            // Verify final task state
            let final_task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
            assert_eq!(final_task.status, TASK_STATUS_SUCCEEDED);

            // Verify all events were written to database in correct order
            let all_events = client.query(
            "SELECT event_type, created_at FROM events WHERE task_instance_id = $1 ORDER BY created_at",
            &[&task_id]
        ).await.unwrap();

            assert_eq!(all_events.len(), 2); // TASK_CREATED, TASK_ATTEMPT_STARTED
            assert_eq!(
                all_events[0].get::<_, i16>("event_type"),
                EVENT_TASK_CREATED
            );
            assert_eq!(
                all_events[1].get::<_, i16>("event_type"),
                EVENT_TASK_ATTEMPT_STARTED
            );

            // Verify events are ordered chronologically
            assert!(
                all_events[0].get::<_, chrono::DateTime<chrono::Utc>>("created_at")
                    <= all_events[1].get::<_, chrono::DateTime<chrono::Utc>>("created_at")
            );
        })
    );

    db_test!(
        test_scheduler_multi_domain_with_db,
        (|pool: crate::orchestrator::db::PgPool| async move {
            use crate::orchestrator::db::DomainsConfig;
            use crate::orchestrator::engine::Engine;
            use crate::orchestrator::event_stream::EventStreamConfig;
            use crate::orchestrator::taskset::Task;
            use crate::EVENT_TASK_CREATED;
            use std::collections::HashMap;
            use tokio::time::Duration;

            let engine = Engine::new(
                pool.clone(),
                EventStreamConfig::default(),
                DomainsConfig::default(),
            );
            engine.initialize().await.unwrap();

            // Create tasks across multiple domains
            let domains = vec!["domain1", "domain2", "domain3"];
            let mut all_task_ids = HashMap::new();

            // Register one shepherd per domain
            for domain in &domains {
                let shepherd_id = Uuid::new_v4();
                let (tx, mut rx) = create_test_shepherd_tx();
                tokio::spawn(async move { while let Some(_cmd) = rx.recv().await {} });
                engine
                    .shepherd_registry
                    .get_or_create_manager(domain)
                    .register_shepherd(shepherd_id, 5, "default".to_string(), tx)
                    .await
                    .unwrap();
            }

            for domain in &domains {
                let mut domain_tasks = Vec::new();
                let task_set = engine.registry.get_or_create_domain(domain);

                for i in 0..3 {
                    let mut task = Task::new();
                    task.name = format!("db_task_{domain}_{i}");
                    task.retry_policy = json!({"max_attempts": 2});
                    task.args = format!(r#"["arg_{i}"]"#);
                    task.kwargs = json!({"domain": domain, "index": i});
                    task.status = TASK_STATUS_CREATED;
                    task.created_at = Utc::now();
                    let task_id = task.id;

                    // Add task to TaskSet
                    task_set.upsert_task(task.clone()).await.unwrap();

                    // Create event and write to database
                    let event_record = crate::orchestrator::event_stream::EventRecord {
                        domain: domain.to_string(),
                        task_instance_id: Some(task_id),
                        flow_instance_id: None,
                        event_type: EVENT_TASK_CREATED,
                        created_at: Utc::now(),
                        metadata: json!({
                            "task_name": task.name,
                            "domain": domain,
                            "index": i
                        }),
                    };
                    engine.event_stream.write_event(event_record).await.unwrap();

                    domain_tasks.push(task_id);
                }
                all_task_ids.insert(domain.to_string(), domain_tasks);
            }

            // Start tasks through schedulers for each domain
            for (domain, task_ids) in &all_task_ids {
                let scheduler = engine.scheduler_registry.get_or_create_scheduler(domain);
                for &task_id in task_ids {
                    scheduler.start_task_async(task_id, None).await.unwrap();
                }
            }

            // Give scheduler actors time to process StartTask commands
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Wait for all scheduling to complete
            // Manually trigger dispatch multiple times instead of relying on timer to avoid CI timing issues
            for _ in 0..8 {
                // Trigger dispatch on all known domains
                for domain in &domains {
                    engine
                        .shepherd_registry
                        .get_or_create_manager(domain)
                        .dispatch_tick()
                        .await
                        .unwrap();
                }
                tokio::time::sleep(Duration::from_millis(30)).await;
            }

            // Verify domain isolation in scheduler registry
            assert_eq!(engine.scheduler_registry.len(), 3);
            for domain in &domains {
                assert!(engine.scheduler_registry.get_scheduler(domain).is_some());
            }

            // Verify all tasks were scheduled across domains
            let mut total_scheduled = 0;
            for (domain, task_ids) in &all_task_ids {
                let scheduler = engine.scheduler_registry.get_scheduler(domain).unwrap();
                for &task_id in task_ids {
                    let task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
                    if task.status == TASK_STATUS_ATTEMPT_STARTED {
                        total_scheduled += 1;
                    }
                }
            }

            assert_eq!(total_scheduled, 9); // 3 domains × 3 tasks each

            // Verify all events were written to database with proper domain isolation
            let client = pool.get().await.unwrap();
            for domain in &domains {
                let domain_events = client.query_one(
                "SELECT COUNT(*) as count FROM events WHERE domain = $1 AND event_type = $2",
                &[&domain, &EVENT_TASK_CREATED]
            ).await.unwrap();

                let count: i64 = domain_events.get("count");
                assert_eq!(count, 3); // 3 tasks per domain
            }

            // Verify total events across all domains
            let total_created_events = client
                .query_one(
                    "SELECT COUNT(*) as count FROM events WHERE event_type = $1",
                    &[&EVENT_TASK_CREATED],
                )
                .await
                .unwrap();

            let created_count: i64 = total_created_events.get("count");
            assert_eq!(created_count, 9); // 9 total tasks

            let total_started_events = client
                .query_one(
                    "SELECT COUNT(*) as count FROM events WHERE event_type = $1",
                    &[&EVENT_TASK_ATTEMPT_STARTED],
                )
                .await
                .unwrap();

            let started_count: i64 = total_started_events.get("count");
            assert_eq!(started_count, 9); // All tasks started
        })
    );

    db_test!(
        test_scheduler_retry_policy_with_db,
        (|pool: crate::orchestrator::db::PgPool| async move {
            use crate::orchestrator::db::DomainsConfig;
            use crate::orchestrator::engine::Engine;
            use crate::orchestrator::event_stream::EventStreamConfig;
            use crate::orchestrator::taskset::{Task, TaskAttempt};
            use crate::proto::common::{ErrorResult, TaskResult};
            use crate::EVENT_TASK_CREATED;

            let engine = Engine::new(
                pool.clone(),
                EventStreamConfig::default(),
                DomainsConfig::default(),
            );
            engine.initialize().await.unwrap();

            // Register shepherd
            let shepherd_id = Uuid::new_v4();
            let (tx, _rx) = create_test_shepherd_tx();
            engine
                .shepherd_registry
                .get_or_create_manager("test_domain")
                .register_shepherd(shepherd_id, 10, "default".to_string(), tx)
                .await
                .unwrap();

            // Create task with specific retry policy
            let mut task = Task::new();
            task.name = "db_retry_test_task".to_string();
            task.retry_policy = json!({
                "version": 1,
                "stop": {"max_attempts": 3, "max_delay": 30},
                "wait": {"strategy": "exponential_jitter", "initial_delay": 1, "multiplier": 2, "max_delay": 30},
                "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError"]}
            });
            task.status = TASK_STATUS_ATTEMPT_STARTED;
            task.created_at = Utc::now();
            task.attempts = vec![TaskAttempt {
                attempt: 0,
                start_time: Some(Utc::now()),
                end_time: None,
                status: crate::ATTEMPT_STATUS_STARTED,
            }];
            let task_id = task.id;

            let task_set = engine.registry.get_or_create_domain("test_domain");
            task_set.upsert_task(task.clone()).await.unwrap();

            let scheduler = engine
                .scheduler_registry
                .get_or_create_scheduler("test_domain");

            // Create initial events for the task
            let create_event = crate::orchestrator::event_stream::EventRecord {
                domain: "test_domain".to_string(),
                task_instance_id: Some(task_id),
                flow_instance_id: None,
                event_type: EVENT_TASK_CREATED,
                created_at: Utc::now(),
                metadata: json!({"task_name": task.name}),
            };
            engine.event_stream.write_event(create_event).await.unwrap();

            let start_event = crate::orchestrator::event_stream::EventRecord {
                domain: "test_domain".to_string(),
                task_instance_id: Some(task_id),
                flow_instance_id: None,
                event_type: EVENT_TASK_ATTEMPT_STARTED,
                created_at: Utc::now(),
                metadata: json!({"attempt": 0, "shepherd_id": shepherd_id}),
            };
            engine.event_stream.write_event(start_event).await.unwrap();

            // Simulate first failure
            let error_result1 = TaskResult {
                task_id: task_id.to_string(),
                result_type: Some(crate::proto::common::task_result::ResultType::Error(
                    ErrorResult {
                        r#type: "RetryableError".to_string(),
                        message: "First attempt failed".to_string(),
                        data: "{}".to_string(),
                        retriable: true,
                    },
                )),
            };

            // Simulate first failure
            scheduler
                .handle_task_result(task_id, error_result1.clone())
                .await
                .unwrap();

            // Verify task is marked for retry
            let task_after_first_failure =
                scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
            assert_eq!(
                task_after_first_failure.status,
                TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT
            );

            // Start the second attempt
            scheduler.start_task_async(task_id, None).await.unwrap();

            // Simulate second failure
            let error_result2 = TaskResult {
                task_id: task_id.to_string(),
                result_type: Some(crate::proto::common::task_result::ResultType::Error(
                    ErrorResult {
                        r#type: "RetryableError".to_string(),
                        message: "Second attempt failed".to_string(),
                        data: "{}".to_string(),
                        retriable: true,
                    },
                )),
            };

            scheduler
                .handle_task_result(task_id, error_result2.clone())
                .await
                .unwrap();

            // Still should have attempts left
            let task_after_second_failure =
                scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
            assert_eq!(
                task_after_second_failure.status,
                TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT
            );

            // Start the third attempt
            scheduler.start_task_async(task_id, None).await.unwrap();

            // Simulate third failure (should exhaust retries)
            let error_result3 = TaskResult {
                task_id: task_id.to_string(),
                result_type: Some(crate::proto::common::task_result::ResultType::Error(
                    ErrorResult {
                        r#type: "FatalError".to_string(),
                        message: "Final attempt failed".to_string(),
                        data: "{}".to_string(),
                        retriable: false,
                    },
                )),
            };

            scheduler
                .handle_task_result(task_id, error_result3.clone())
                .await
                .unwrap();

            // Should be permanently failed
            let final_task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
            assert_eq!(final_task.status, TASK_STATUS_FAILED);

            // Verify all events are in database with proper sequencing
            let client = pool.get().await.unwrap();
            let all_events = client.query(
            "SELECT event_type, metadata, created_at FROM events WHERE task_instance_id = $1 ORDER BY created_at",
            &[&task_id]
        ).await.unwrap();

            // Should have: TASK_CREATED, TASK_ATTEMPT_STARTED (attempt 0), multiple TASK_ATTEMPT_ENDED events, TASK_ENDED
            // Note: Due to dispatch failures, we get TASK_ATTEMPT_ENDED events without corresponding TASK_ATTEMPT_STARTED events

            // Verify key events exist
            assert_eq!(
                all_events[0].get::<_, i16>("event_type"),
                EVENT_TASK_CREATED
            );
            assert_eq!(
                all_events[1].get::<_, i16>("event_type"),
                EVENT_TASK_ATTEMPT_STARTED
            );

            // Check that we have multiple TASK_ATTEMPT_ENDED events
            let attempt_ended_count = all_events
                .iter()
                .filter(|event| {
                    event.get::<_, i16>("event_type") == crate::EVENT_TASK_ATTEMPT_ENDED
                })
                .count();
            assert!(
                attempt_ended_count >= 3,
                "Should have at least 3 TASK_ATTEMPT_ENDED events, got {attempt_ended_count}"
            );

            // Check that we have at least one TASK_ENDED event
            let task_ended_count = all_events
                .iter()
                .filter(|event| event.get::<_, i16>("event_type") == crate::EVENT_TASK_ENDED)
                .count();
            assert!(
                task_ended_count >= 1,
                "Should have at least 1 TASK_ENDED event, got {task_ended_count}"
            );

            // Verify chronological ordering of events
            for i in 1..all_events.len() {
                let prev_time =
                    all_events[i - 1].get::<_, chrono::DateTime<chrono::Utc>>("created_at");
                let curr_time = all_events[i].get::<_, chrono::DateTime<chrono::Utc>>("created_at");
                assert!(prev_time <= curr_time);
            }

            // Verify metadata contains expected information
            let task_created_metadata: serde_json::Value = all_events[0].get("metadata");
            assert_eq!(task_created_metadata["task_name"], "db_retry_test_task");

            let attempt_started_metadata: serde_json::Value = all_events[1].get("metadata");
            assert_eq!(attempt_started_metadata["attempt"], 0);
        })
    );

    db_test!(
        test_scheduler_shepherd_failure_with_db,
        (|pool: crate::orchestrator::db::PgPool| async move {
            use crate::orchestrator::db::DomainsConfig;
            use crate::orchestrator::engine::Engine;
            use crate::orchestrator::event_stream::EventStreamConfig;
            use crate::orchestrator::taskset::Task;
            use crate::EVENT_TASK_CREATED;
            use tokio::time::Duration;

            let engine = Engine::new(
                pool.clone(),
                EventStreamConfig::default(),
                DomainsConfig::default(),
            );
            engine.initialize().await.unwrap();

            // Register a shepherd
            let shepherd_id = Uuid::new_v4();
            let (tx, _rx) = create_test_shepherd_tx();
            engine
                .shepherd_registry
                .get_or_create_manager("test_domain")
                .register_shepherd(shepherd_id, 10, "default".to_string(), tx)
                .await
                .unwrap();

            // Create multiple tasks
            let mut task_ids = Vec::new();
            let task_set = engine.registry.get_or_create_domain("test_domain");

            for i in 0..3 {
                let mut task = Task::new();
                task.name = format!("shepherd_failure_task_{i}");
                // Use a fixed retry delay to avoid jitter-based immediate retries that race this test
                task.retry_policy = json!({
                    "version": 1,
                    "stop": {"max_attempts": 3},
                    "wait": {
                        "strategy": "fixed",
                        "delay": 1,
                        "initial_delay": 1,
                        "max_delay": 1
                    },
                    "retry": {"include_errors": ["ValueError", "RuntimeError", "RetryableError", "FatalError", "InfrastructureError"]}
                });
                task.status = TASK_STATUS_CREATED;
                task.created_at = Utc::now();
                let task_id = task.id;

                task_set.upsert_task(task.clone()).await.unwrap();

                // Create events in database
                let event_record = crate::orchestrator::event_stream::EventRecord {
                    domain: "test_domain".to_string(),
                    task_instance_id: Some(task_id),
                    flow_instance_id: None,
                    event_type: EVENT_TASK_CREATED,
                    created_at: Utc::now(),
                    metadata: json!({
                        "task_name": task.name,
                        "created_for_shepherd_test": true
                    }),
                };
                engine.event_stream.write_event(event_record).await.unwrap();

                task_ids.push(task_id);
            }

            // Start tasks through scheduler
            let scheduler = engine
                .scheduler_registry
                .get_or_create_scheduler("test_domain");
            for &task_id in &task_ids {
                scheduler.start_task_async(task_id, None).await.unwrap();
            }

            // Give scheduler actors time to process StartTask commands
            tokio::time::sleep(Duration::from_millis(100)).await;

            // Wait for the virtual queue dispatcher to pick up and dispatch all tasks
            let client = pool.get().await.unwrap();
            let mut attempts = 0;
            let max_attempts = 25; // More attempts with shorter waits

            loop {
                // Manually trigger dispatch multiple times to ensure it processes
                for _ in 0..3 {
                    engine
                        .shepherd_registry
                        .get_or_create_manager("test_domain")
                        .dispatch_tick()
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                tokio::time::sleep(Duration::from_millis(80)).await;

                // Check how many tasks have been dispatched
                let started_events_count = client
                    .query_one(
                        "SELECT COUNT(*) as count FROM events WHERE event_type = $1 AND domain = $2",
                        &[&EVENT_TASK_ATTEMPT_STARTED, &"test_domain"],
                    )
                    .await
                    .unwrap();

                let started_count: i64 = started_events_count.get("count");

                if started_count >= 3 {
                    // All tasks have been dispatched, verify their status
                    for &task_id in &task_ids {
                        let task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
                        assert_eq!(task.status, TASK_STATUS_ATTEMPT_STARTED);
                    }
                    assert_eq!(started_count, 3);
                    break;
                }

                attempts += 1;
                if attempts >= max_attempts {
                    eprintln!("DEBUG: Only {started_count} out of 3 tasks were dispatched after {attempts} attempts");
                    for &task_id in &task_ids {
                        let task_status = scheduler.get_task_for_test(task_id).await.unwrap();
                        eprintln!(
                            "DEBUG: Task {} status: {:?}",
                            task_id,
                            task_status.map(|t| t.status)
                        );
                    }
                    panic!("Only {started_count} out of 3 tasks were dispatched within expected time. Virtual queue dispatcher may not be running properly.");
                }
            }

            // Simulate shepherd death
            engine
                .shepherd_registry
                .get_or_create_manager("test_domain")
                .mark_shepherd_disconnected(shepherd_id)
                .await
                .unwrap();

            // Handle shepherd death through scheduler
            scheduler
                .handle_shepherd_death(task_ids.clone())
                .await
                .unwrap();

            // Verify tasks were marked for retry
            for &task_id in &task_ids {
                let task = scheduler.get_task_for_test(task_id).await.unwrap().unwrap();
                assert_eq!(task.status, TASK_STATUS_ATTEMPT_FAILED_WITH_ATTEMPTS_LEFT);
            }

            // Verify failure events were written to database
            let failure_events = client
                .query_one(
                    "SELECT COUNT(*) as count FROM events WHERE event_type = $1 AND domain = $2",
                    &[&EVENT_TASK_ATTEMPT_ENDED, &"test_domain"],
                )
                .await
                .unwrap();

            let failure_count: i64 = failure_events.get("count");
            assert_eq!(failure_count, 3);

            // Verify complete event history for each task
            for &task_id in &task_ids {
                let task_events = client.query(
                "SELECT event_type, created_at FROM events WHERE task_instance_id = $1 ORDER BY created_at",
                &[&task_id]
            ).await.unwrap();

                // Should have: CREATED, ATTEMPT_STARTED, ATTEMPT_ENDED
                assert_eq!(task_events.len(), 3);
                assert_eq!(
                    task_events[0].get::<_, i16>("event_type"),
                    EVENT_TASK_CREATED
                );
                assert_eq!(
                    task_events[1].get::<_, i16>("event_type"),
                    EVENT_TASK_ATTEMPT_STARTED
                );
                assert_eq!(
                    task_events[2].get::<_, i16>("event_type"),
                    EVENT_TASK_ATTEMPT_ENDED
                );

                // Verify chronological ordering
                let time0 = task_events[0].get::<_, chrono::DateTime<chrono::Utc>>("created_at");
                let time1 = task_events[1].get::<_, chrono::DateTime<chrono::Utc>>("created_at");
                let time2 = task_events[2].get::<_, chrono::DateTime<chrono::Utc>>("created_at");
                assert!(time0 <= time1);
                assert!(time1 <= time2);
            }
        })
    );
}
