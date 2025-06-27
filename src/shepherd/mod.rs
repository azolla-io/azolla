pub mod config;
pub mod stream_handler;
pub mod task_manager;
pub mod worker_service;

// Re-export commonly used types
pub use config::{load_config, ShepherdConfig};
pub use stream_handler::{IncomingTask, StreamEvent, StreamHandler};
pub use task_manager::{TaskInfo, TaskManager, TaskManagerStats, TaskStatus};
pub use worker_service::{start_worker_service, TaskResultMessage, WorkerService};
