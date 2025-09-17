use anyhow::Result;
use log::{debug, error, info, warn};
use tokio::sync::mpsc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

use crate::proto::{common, shepherd};
use shepherd::worker_server::{Worker, WorkerServer};
use shepherd::*;

#[derive(Debug, Clone)]
pub struct TaskResultMessage {
    pub task_id: Uuid,
    pub result: common::TaskResult,
}
pub struct WorkerService {
    result_sender: mpsc::UnboundedSender<TaskResultMessage>,
}

impl WorkerService {
    pub fn new(result_sender: mpsc::UnboundedSender<TaskResultMessage>) -> Self {
        Self { result_sender }
    }

    pub fn into_server(self) -> WorkerServer<Self> {
        WorkerServer::new(self)
    }
}

#[tonic::async_trait]
impl Worker for WorkerService {
    async fn report_result(
        &self,
        request: Request<ReportResultRequest>,
    ) -> Result<Response<ReportResultResponse>, Status> {
        let req = request.into_inner();

        debug!("Received result report for task: {}", req.task_id);

        let task_id = Uuid::parse_str(&req.task_id)
            .map_err(|e| Status::invalid_argument(format!("Invalid task ID: {e}")))?;

        let result = req
            .result
            .ok_or_else(|| Status::invalid_argument("Missing task result"))?;

        if result.task_id != req.task_id {
            return Err(Status::invalid_argument(
                "Task ID mismatch between request and result",
            ));
        }

        if result.result_type.is_none() {
            return Err(Status::invalid_argument("Missing result type"));
        }

        let success = match &result.result_type {
            Some(common::task_result::ResultType::Success(_)) => {
                info!("Task {task_id} completed successfully");
                true
            }
            Some(common::task_result::ResultType::Error(error_result)) => {
                warn!(
                    "Task {task_id} failed: {} - {}",
                    error_result.r#type, error_result.message
                );
                false
            }
            None => {
                error!("Task {task_id} has invalid result type");
                return Err(Status::invalid_argument("Invalid result type"));
            }
        };

        let message = TaskResultMessage { task_id, result };

        if let Err(e) = self.result_sender.send(message) {
            error!("Failed to forward task result to stream handler: {e}");
            return Err(Status::internal("Failed to process task result"));
        }

        debug!("Successfully forwarded result for task {task_id} to stream handler");

        Ok(Response::new(ReportResultResponse {
            success: true,
            message: if success {
                "Task result received successfully".to_string()
            } else {
                "Task failure result received successfully".to_string()
            },
        }))
    }
}

pub async fn start_worker_service(
    port: u16,
    result_sender: mpsc::UnboundedSender<TaskResultMessage>,
) -> Result<()> {
    let addr = format!("127.0.0.1:{port}")
        .parse()
        .map_err(|e| anyhow::anyhow!("Invalid address format: {e}"))?;

    let worker_service = WorkerService::new(result_sender);
    let server = worker_service.into_server();

    info!("Starting worker service on {addr}");

    tonic::transport::Server::builder()
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
        .add_service(server)
        .serve(addr)
        .await
        .map_err(|e| anyhow::anyhow!("Worker service error: {e}"))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc;
    use tonic::Request;

    #[tokio::test]
    async fn test_report_result_success() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let service = WorkerService::new(tx);

        let task_id = Uuid::new_v4();
        let request = Request::new(ReportResultRequest {
            task_id: task_id.to_string(),
            result: Some(common::TaskResult {
                task_id: task_id.to_string(),
                result_type: Some(common::task_result::ResultType::Success(
                    common::SuccessResult {
                        result: Some(common::AnyValue {
                            value: Some(common::any_value::Value::StringValue(
                                "test result".to_string(),
                            )),
                        }),
                    },
                )),
            }),
        });

        let response = service.report_result(request).await.unwrap();
        let response_inner = response.into_inner();

        assert!(response_inner.success);
        assert!(response_inner.message.contains("successfully"));

        // Verify the message was sent
        let received = rx.try_recv().unwrap();
        assert_eq!(received.task_id, task_id);
    }

    #[tokio::test]
    async fn test_report_result_error() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let service = WorkerService::new(tx);

        let task_id = Uuid::new_v4();
        let request = Request::new(ReportResultRequest {
            task_id: task_id.to_string(),
            result: Some(common::TaskResult {
                task_id: task_id.to_string(),
                result_type: Some(common::task_result::ResultType::Error(
                    common::ErrorResult {
                        r#type: "TestError".to_string(),
                        message: "Test error message".to_string(),
                        data: "{}".to_string(),
                        retriable: true,
                    },
                )),
            }),
        });

        let response = service.report_result(request).await.unwrap();
        let response_inner = response.into_inner();

        assert!(response_inner.success);
        assert!(response_inner.message.contains("failure"));

        // Verify the message was sent
        let received = rx.try_recv().unwrap();
        assert_eq!(received.task_id, task_id);
    }

    #[tokio::test]
    async fn test_report_result_invalid_task_id() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = WorkerService::new(tx);

        let request = Request::new(ReportResultRequest {
            task_id: "invalid-uuid".to_string(),
            result: Some(common::TaskResult {
                task_id: "invalid-uuid".to_string(),
                result_type: Some(common::task_result::ResultType::Success(
                    common::SuccessResult { result: None },
                )),
            }),
        });

        let result = service.report_result(request).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_report_result_missing_result() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = WorkerService::new(tx);

        let task_id = Uuid::new_v4();
        let request = Request::new(ReportResultRequest {
            task_id: task_id.to_string(),
            result: None,
        });

        let result = service.report_result(request).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn test_report_result_task_id_mismatch() {
        let (tx, _rx) = mpsc::unbounded_channel();
        let service = WorkerService::new(tx);

        let task_id = Uuid::new_v4();
        let different_id = Uuid::new_v4();

        let request = Request::new(ReportResultRequest {
            task_id: task_id.to_string(),
            result: Some(common::TaskResult {
                task_id: different_id.to_string(),
                result_type: Some(common::task_result::ResultType::Success(
                    common::SuccessResult { result: None },
                )),
            }),
        });

        let result = service.report_result(request).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}
