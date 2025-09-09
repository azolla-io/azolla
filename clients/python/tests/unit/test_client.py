"""Unit tests for client functionality."""
import pytest
import json
from unittest.mock import AsyncMock, MagicMock

from azolla import Client, ClientConfig, TaskHandle, azolla_task
from azolla.types import TaskStatus
from azolla.exceptions import ConnectionError, SerializationError
from azolla._grpc import orchestrator_pb2

# Define test task at module level
@azolla_task
async def sample_client_task(message: str, count: int = 1) -> dict:
    """Test task for client testing."""
    return {"message": message, "count": count}

class TestClientConfig:
    """Test client configuration."""
    
    def test_client_config_defaults(self) -> None:
        """Test default client configuration values."""
        config = ClientConfig()
        assert config.endpoint == "http://localhost:52710"
        assert config.domain == "default"
        assert config.timeout == 30.0
        assert config.max_message_size == 4 * 1024 * 1024
    
    def test_client_config_custom_values(self) -> None:
        """Test client configuration with custom values."""
        config = ClientConfig(
            endpoint="http://production:8080",
            domain="prod",
            timeout=60.0,
            max_message_size=8 * 1024 * 1024
        )
        assert config.endpoint == "http://production:8080"
        assert config.domain == "prod"
        assert config.timeout == 60.0
        assert config.max_message_size == 8 * 1024 * 1024

class TestClient:
    """Test Client functionality."""
    
    def test_client_creation(self) -> None:
        """Test client creation with configuration."""
        config = ClientConfig(endpoint="http://test:9090")
        client = Client(config)
        assert client._config.endpoint == "http://test:9090"
        assert client._channel is None
        assert client._stub is None
    
    async def test_client_connection_setup(self, mock_client: Client) -> None:
        """Test that client connection is set up properly."""
        await mock_client._ensure_connection()
        assert mock_client._channel is not None
        assert mock_client._stub is not None
    
    async def test_task_submission_builder_pattern(self, mock_client: Client) -> None:
        """Test task submission using builder pattern."""
        # Test builder creation
        builder = mock_client.submit_task("test_task")
        assert builder._task_name == "test_task"
        assert builder._args is None
        
        # Test builder configuration
        builder = (builder
                  .args({"message": "hello", "count": 5})
                  .shepherd_group("test-group"))
        
        assert builder._args == {"message": "hello", "count": 5}
        assert builder._shepherd_group == "test-group"
    
    async def test_task_submission_with_decorated_function(self, mock_client: Client) -> None:
        """Test submitting a decorated function as a task."""
        builder = mock_client.submit_task(sample_client_task, {"message": "test", "count": 3})
        
        # Should extract task name from decorated function
        assert builder._task_name == "sample_client_task"
        assert builder._args == {"message": "test", "count": 3}
    
    async def test_successful_task_submission(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test successful task submission and handle creation."""
        # Configure mock to return task ID
        mock_grpc_stub.CreateTask.return_value = orchestrator_pb2.CreateTaskResponse(
            task_id="test-task-456"
        )
        
        handle = await (mock_client
                       .submit_task("test_task")
                       .args({"key": "value"})
                       .submit())
        
        assert isinstance(handle, TaskHandle)
        assert handle.task_id == "test-task-456"
        
        # Verify gRPC call was made correctly
        mock_grpc_stub.CreateTask.assert_called_once()
        call_args = mock_grpc_stub.CreateTask.call_args[0][0]
        assert call_args.name == "test_task"
        assert call_args.domain == "default"
        assert json.loads(call_args.args) == [{"key": "value"}]
    
    async def test_task_submission_argument_serialization(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test that task arguments are serialized correctly."""
        # Test with dict
        await (mock_client.submit_task("test").args({"a": 1, "b": 2}).submit())
        call_args = mock_grpc_stub.CreateTask.call_args[0][0]
        assert json.loads(call_args.args) == [{"a": 1, "b": 2}]
        
        # Test with list
        await (mock_client.submit_task("test").args([1, 2, 3]).submit())
        call_args = mock_grpc_stub.CreateTask.call_args[0][0]
        assert json.loads(call_args.args) == [1, 2, 3]
        
        # Test with single value
        await (mock_client.submit_task("test").args("single").submit())
        call_args = mock_grpc_stub.CreateTask.call_args[0][0]
        assert json.loads(call_args.args) == ["single"]
    
    async def test_task_submission_with_retry_policy(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test task submission with retry policy."""
        from azolla.retry import RetryPolicy, ExponentialBackoff
        
        retry_policy = RetryPolicy(
            max_attempts=5,
            backoff=ExponentialBackoff(initial=2.0)
        )
        
        await (mock_client
              .submit_task("test")
              .retry_policy(retry_policy)
              .submit())
        
        call_args = mock_grpc_stub.CreateTask.call_args[0][0]
        assert call_args.retry_policy != ""
        
        # Should be valid JSON
        retry_data = json.loads(call_args.retry_policy)
        assert retry_data["max_attempts"] == 5

class TestTaskHandle:
    """Test TaskHandle functionality."""
    
    async def test_task_handle_creation(self, mock_client: Client) -> None:
        """Test TaskHandle creation."""
        handle = TaskHandle("test-task-789", mock_client)
        assert handle.task_id == "test-task-789"
        assert handle._client == mock_client
    
    async def test_successful_task_result(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test getting successful task result."""
        # Configure mock to return completed task
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status="completed",
            result='{"status": "success", "value": 42}'
        )
        
        handle = TaskHandle("test-task", mock_client)
        result = await handle.try_result()
        
        assert result is not None
        assert result.success is True
        assert result.status == TaskStatus.COMPLETED
        assert result.value == {"status": "success", "value": 42}
    
    async def test_failed_task_result(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test getting failed task result."""
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status="failed",
            error="Task execution failed with error"
        )
        
        handle = TaskHandle("test-task", mock_client)
        result = await handle.try_result()
        
        assert result is not None
        assert result.failed is True
        assert result.status == TaskStatus.FAILED
        assert result.error == "Task execution failed with error"
    
    async def test_pending_task_result(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test getting result for pending task."""
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status="running"
        )
        
        handle = TaskHandle("test-task", mock_client)
        result = await handle.try_result()
        
        assert result is None  # Still running
    
    async def test_task_wait_with_timeout(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test task wait with timeout."""
        # Mock task that never completes
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status="running"
        )
        
        handle = TaskHandle("test-task", mock_client)
        result = await handle.wait(timeout=0.1)  # Very short timeout
        
        assert result.failed is True
        assert "timeout" in result.error.lower()

class TestClientBuilder:
    """Test ClientBuilder functionality."""
    
    def test_client_builder_configuration(self) -> None:
        """Test client builder configuration."""
        builder = Client.builder()
        builder = (builder
                  .endpoint("http://custom:9999")
                  .domain("custom-domain")
                  .timeout(45.0)
                  .max_message_size(16 * 1024 * 1024))
        
        assert builder._config.endpoint == "http://custom:9999"
        assert builder._config.domain == "custom-domain"
        assert builder._config.timeout == 45.0
        assert builder._config.max_message_size == 16 * 1024 * 1024