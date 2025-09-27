//! Public API compatibility tests for the single-task worker client

use azolla_client::client::{Client, ClientConfig};
use azolla_client::convert::FromJsonValue;
use azolla_client::error::{AzollaError, TaskError};
use azolla_client::retry_policy::RetryPolicy;
use azolla_client::task::{BoxedTask, Task, TaskResult};
use azolla_client::worker::{Worker, WorkerInvocation};
use azolla_client::*;
use serde_json::json;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;

struct SampleTask;

impl Task for SampleTask {
    type Args = ();

    fn name(&self) -> &'static str {
        "sample_task"
    }

    fn execute(&self, _args: Self::Args) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
        Box::pin(async move { Ok(json!({"status": "ok"})) })
    }
}

/// Test that core public APIs remain importable and usable
#[test]
fn test_public_api_imports() {
    let _config = ClientConfig::default();
    let _client_builder = Client::builder();
    let _retry_policy = RetryPolicy::default();
    let _worker_builder = Worker::builder();

    let _task_error = TaskError::invalid_args("test");
    let _azolla_error = AzollaError::InvalidConfig("test".to_string());

    let conversion = <i32 as FromJsonValue>::try_from(json!(123));
    assert!(conversion.is_ok());
}

/// Test that the crate version constant is exposed
#[test]
fn test_version_constant() {
    assert!(!VERSION.is_empty());
    assert!(VERSION.chars().any(|c| c.is_ascii_digit()));
}

/// Test public type construction and field access
#[test]
fn test_public_type_construction() {
    let client_config = ClientConfig {
        endpoint: "http://localhost:9000".to_string(),
        domain: "test".to_string(),
        timeout: Duration::from_secs(5),
    };
    assert_eq!(client_config.endpoint, "http://localhost:9000");
    assert_eq!(client_config.domain, "test");

    let invocation = WorkerInvocation::from_json(
        "7b4c20d0-0d40-4f97-8c3f-1a602d52b521",
        "sample_task",
        "[]",
        "{}",
        "http://127.0.0.1:50052",
    )
    .expect("Invocation should parse");
    assert_eq!(invocation.task_name, "sample_task");
}

/// Test trait implementations on public types
#[test]
fn test_public_trait_implementations() {
    let config = ClientConfig::default();
    let debug_repr = format!("{config:?}");
    assert!(!debug_repr.is_empty());

    let clone = config.clone();
    assert_eq!(config.endpoint, clone.endpoint);

    let task_error = TaskError::execution_failed("error");
    let azolla_error = AzollaError::TaskFailed(task_error);
    let display = format!("{azolla_error}");
    assert!(!display.is_empty());
}

/// Test builder patterns remain available
#[test]
fn test_builder_patterns() {
    let client_future = Client::builder()
        .endpoint("http://localhost:9000")
        .domain("test")
        .build();
    assert!(tokio_test::block_on(client_future).is_err());

    let retry_policy = RetryPolicy::builder()
        .max_attempts(3)
        .initial_delay(Duration::from_millis(50))
        .build();
    assert_eq!(retry_policy.stop.max_attempts, Some(3));

    let worker = Worker::builder().register_task(SampleTask).build();
    assert_eq!(worker.task_count(), 1);
}

/// Test crate re-exports remain stable
#[test]
fn test_reexports() {
    use azolla_client::{AzollaError, Client, RetryPolicy, TaskError, Worker};

    let _ = Client::builder();
    let _ = Worker::builder();
    let _ = RetryPolicy::default();
    let _ = TaskError::invalid_args("test");
    let _ = AzollaError::InvalidConfig("test".to_string());
}

/// Test Task trait object support remains available
#[test]
fn test_boxed_task_usage() {
    struct BoxedSampleTask;
    impl Task for BoxedSampleTask {
        type Args = ();
        fn name(&self) -> &'static str {
            "boxed_sample"
        }
        fn execute(
            &self,
            _args: Self::Args,
        ) -> Pin<Box<dyn Future<Output = TaskResult> + Send + '_>> {
            Box::pin(async move { Ok(json!({"status": "ok"})) })
        }
    }

    let boxed: Box<dyn BoxedTask> = Box::new(BoxedSampleTask);
    assert_eq!(boxed.name(), "boxed_sample");
}

/// Test that API naming conventions remain consistent
#[test]
fn test_api_conventions() {
    assert_eq!(
        std::any::type_name::<ClientConfig>(),
        "azolla_client::client::ClientConfig"
    );
    assert_eq!(
        std::any::type_name::<WorkerInvocation>(),
        "azolla_client::worker::WorkerInvocation"
    );
}
