//! # Azolla Client Library
//!
//! A Rust client library for the Azolla distributed task processing platform.
//!
//! ## Features
//!
//! - **Type-safe task arguments** - Compile-time argument validation with automatic JSON conversion
//! - **Proc macro support** - Define tasks using `#[azolla_task]` attribute for zero boilerplate
//! - **Async/await ready** - Built on tokio with full async support
//! - **Worker registration** - Create workers that automatically discover and execute tasks
//! - **Retry policies** - Configurable retry behavior for failed tasks
//! - **Error handling** - Comprehensive error types with detailed messages
//!
//! ## Quick Start
//!
//! ### Defining Tasks
//!
//! ```no_run
//! use azolla_client::{azolla_task, TaskError};
//! use serde_json::{json, Value};
//!
//! #[azolla_task]
//! async fn greet_user(name: String, age: u32) -> Result<Value, TaskError> {
//!     Ok(json!({
//!         "greeting": format!("Hello {name}, you are {age} years old!"),
//!         "name": name,
//!         "age": age
//!     }))
//! }
//! ```
//!
//! ### Creating a Client
//!
//! ```no_run
//! use azolla_client::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::builder()
//!         .endpoint("http://localhost:52710")
//!         .domain("my-domain")
//!         .build()
//!         .await?;
//!     
//!     // Submit a task with type-safe arguments
//!     let task = client
//!         .submit_task("greet_user")
//!         .args(("Alice".to_string(), 25u32))?
//!         .submit()
//!         .await?;
//!         
//!     // Wait for completion
//!     let result = task.wait().await?;
//!     println!("Result: {:?}", result);
//!     
//!     Ok(())
//! }
//! ```
//!
//! ### Creating a Worker
//!
//! See README.md for complete examples of creating workers.

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
}

// Re-export main public API
pub use client::{Client, ClientBuilder, TaskHandle, TaskSubmissionBuilder};
pub use convert::{ConversionError, FromJsonValue};
pub use error::{AzollaError, TaskError};
pub use retry_policy::{RetryPolicy, RetryPolicyBuilder, StopCondition, WaitStrategy};
pub use task::{Task, TaskContext, TaskResult};
pub use worker::{Worker, WorkerBuilder};

// Re-export proc macro (only when macros feature is enabled)
#[cfg(feature = "macros")]
pub use azolla_macros::azolla_task;

// Re-export common types for convenience
pub use serde_json::Value;

// Re-export the version
pub const VERSION: &str = env!("CARGO_PKG_VERSION");
