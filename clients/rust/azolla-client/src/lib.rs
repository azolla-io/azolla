//! # Azolla Client Library
//!
//! A Rust client library for the Azolla distributed task processing platform.
//!
//! See [https://github.com/azolla-io/azolla](https://github.com/azolla-io/azolla) for documentation.

pub mod client;
pub mod convert;
pub mod error;
pub mod retry_policy;
pub mod task;
pub mod worker;

// Generated protobuf code
pub mod proto {
    pub mod orchestrator {
        tonic::include_proto!("azolla.orchestrator");
    }

    pub mod common {
        tonic::include_proto!("azolla.common");
    }

    pub mod shepherd {
        tonic::include_proto!("azolla.shepherd");
    }
}

// Re-export main public API
pub use client::{Client, ClientBuilder, TaskExecutionResult, TaskHandle, TaskSubmissionBuilder};
pub use convert::{ConversionError, FromJsonValue};
pub use error::{AzollaError, TaskError};
pub use retry_policy::{RetryPolicy, RetryPolicyBuilder, StopCondition, WaitStrategy};
pub use task::{BoxedTask, Task, TaskResult};
pub use worker::{TaskExecutionOutcome, Worker, WorkerBuilder, WorkerExecution, WorkerInvocation};

// Re-export proc macro (only when macros feature is enabled)
#[cfg(feature = "macros")]
pub use azolla_macros::azolla_task;

// Re-export common types for convenience
pub use serde_json::Value;

// Re-export the version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
