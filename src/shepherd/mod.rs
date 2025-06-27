pub mod config;
pub mod worker_service;
pub mod stream_handler;
pub mod task_manager;

// Re-export commonly used types
pub use config::{ShepherdConfig, load_config};
pub use worker_service::{WorkerService, TaskResultMessage, start_worker_service};
pub use stream_handler::{StreamHandler, IncomingTask, StreamEvent};
pub use task_manager::{TaskManager, TaskInfo, TaskStatus, TaskManagerStats};