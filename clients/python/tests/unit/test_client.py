"""Unit tests for client functionality."""

import json

from azolla import Client, ClientConfig, TaskHandle, azolla_task
from azolla._grpc import common_pb2, orchestrator_pb2


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
            max_message_size=8 * 1024 * 1024,
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
        builder = builder.args({"message": "hello", "count": 5}).shepherd_group("test-group")

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

        handle = await mock_client.submit_task("test_task").args({"key": "value"}).submit()

        assert isinstance(handle, TaskHandle)
        assert handle.task_id == "test-task-456"

        # Verify gRPC call was made correctly
        mock_grpc_stub.CreateTask.assert_called_once()
        call_args = mock_grpc_stub.CreateTask.call_args[0][0]
        assert call_args.name == "test_task"
        assert call_args.domain == "default"
        assert json.loads(call_args.args) == [{"key": "value"}]

    async def test_task_submission_argument_serialization(
        self, mock_client: Client, mock_grpc_stub
    ) -> None:
        """Test that task arguments are serialized correctly."""
        # Test with dict
        await mock_client.submit_task("test").args({"a": 1, "b": 2}).submit()
        call_args = mock_grpc_stub.CreateTask.call_args[0][0]
        assert json.loads(call_args.args) == [{"a": 1, "b": 2}]

        # Test with list
        await mock_client.submit_task("test").args([1, 2, 3]).submit()
        call_args = mock_grpc_stub.CreateTask.call_args[0][0]
        assert json.loads(call_args.args) == [1, 2, 3]

        # Test with single value
        await mock_client.submit_task("test").args("single").submit()
        call_args = mock_grpc_stub.CreateTask.call_args[0][0]
        assert json.loads(call_args.args) == ["single"]

    async def test_task_submission_with_retry_policy(
        self, mock_client: Client, mock_grpc_stub
    ) -> None:
        """Test task submission with retry policy."""
        from azolla.retry import ExponentialBackoff, RetryPolicy

        retry_policy = RetryPolicy(max_attempts=5, backoff=ExponentialBackoff(initial=2.0))

        await mock_client.submit_task("test").retry_policy(retry_policy).submit()

        call_args = mock_grpc_stub.CreateTask.call_args[0][0]
        assert call_args.HasField("retry_policy")
        assert call_args.retry_policy.stop.max_attempts == 5


class TestTaskHandle:
    """Test TaskHandle functionality."""

    async def test_task_handle_creation(self, mock_client: Client) -> None:
        """Test TaskHandle creation."""
        handle = TaskHandle("test-task-789", mock_client)
        assert handle.task_id == "test-task-789"
        assert handle._client == mock_client

    async def test_successful_task_result(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test getting successful task result."""
        # Configure mock to return completed task with new structure
        success_result = common_pb2.SuccessResult(
            result=common_pb2.AnyValue(json_value='{"status": "success", "value": 42}')
        )
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            success=success_result,
        )

        handle = TaskHandle("test-task", mock_client)
        result = await handle.try_result()

        assert result is not None
        assert result.task_id == "test-task"
        assert result.value == {"status": "success", "value": 42}

    async def test_failed_task_result(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test getting failed task result."""
        # Configure mock to return failed task with new structure
        error_result = common_pb2.ErrorResult(
            type="ValueError",
            message="Task execution failed with error",
            data="{}",
            retriable=True,
        )
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            error=error_result,
        )

        handle = TaskHandle("test-task", mock_client)

        result = await handle.try_result()

        assert result is not None
        assert result.success is False
        assert result.error is not None
        assert result.error.error_type == "ValueError"
        assert result.error.message == "Task execution failed with error"
        assert result.error.retriable is True

    async def test_pending_task_result(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test getting result for pending task."""
        # Mock task that's still running (timeout status in try_result means not ready)
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_TIMEOUT
        )

        handle = TaskHandle("test-task", mock_client)
        result = await handle.try_result()

        assert result is None  # Still running

    async def test_task_wait_with_timeout(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test task wait with timeout."""
        # Mock task that never completes
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_TIMEOUT
        )

        handle = TaskHandle("test-task", mock_client)

        # Expect TaskWaitTimeoutError to be raised
        from azolla.exceptions import TaskWaitTimeoutError

        try:
            await handle.wait(timeout=0.1)  # Very short timeout
            raise AssertionError("Expected TaskWaitTimeoutError to be raised")
        except TaskWaitTimeoutError as e:
            assert "timeout exceeded" in str(e)


class TestTaskResultRetrieval:
    """Test comprehensive task result retrieval scenarios."""

    async def test_success_result_string_value(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test getting successful task result with string value."""
        # Configure mock to return string result
        success_result = common_pb2.SuccessResult(
            result=common_pb2.AnyValue(string_value="Hello, World!")
        )
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            success=success_result,
        )

        handle = TaskHandle("test-task", mock_client)
        result = await handle.try_result()

        assert result is not None
        assert result.task_id == "test-task"
        assert result.value == "Hello, World!"

    async def test_success_result_numeric_values(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test getting successful task result with numeric values."""
        # Test integer result
        success_result = common_pb2.SuccessResult(result=common_pb2.AnyValue(int_value=42))
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            success=success_result,
        )

        handle = TaskHandle("test-task-int", mock_client)
        result = await handle.try_result()

        assert result is not None
        assert result.value == 42

        # Test float result
        success_result = common_pb2.SuccessResult(result=common_pb2.AnyValue(double_value=3.14159))
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            success=success_result,
        )

        handle = TaskHandle("test-task-float", mock_client)
        result = await handle.try_result()

        assert result is not None
        assert abs(result.value - 3.14159) < 1e-5

    async def test_success_result_boolean_values(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test getting successful task result with boolean values."""
        # Test true result
        success_result = common_pb2.SuccessResult(result=common_pb2.AnyValue(bool_value=True))
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            success=success_result,
        )

        handle = TaskHandle("test-task-true", mock_client)
        result = await handle.try_result()

        assert result is not None
        assert result.value is True

        # Test false result
        success_result = common_pb2.SuccessResult(result=common_pb2.AnyValue(bool_value=False))
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            success=success_result,
        )

        handle = TaskHandle("test-task-false", mock_client)
        result = await handle.try_result()

        assert result is not None
        assert result.value is False

    async def test_success_result_complex_objects(
        self, mock_client: Client, mock_grpc_stub
    ) -> None:
        """Test getting successful task result with complex dict objects."""
        complex_data = {
            "user_id": 12345,
            "username": "alice",
            "active": True,
            "settings": {"theme": "dark", "notifications": False},
            "tags": ["admin", "developer"],
        }

        success_result = common_pb2.SuccessResult(
            result=common_pb2.AnyValue(json_value=json.dumps(complex_data))
        )
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            success=success_result,
        )

        handle = TaskHandle("test-task-complex", mock_client)
        result = await handle.try_result()

        assert result is not None
        assert result.value == complex_data
        assert result.value["user_id"] == 12345
        assert result.value["username"] == "alice"
        assert result.value["settings"]["theme"] == "dark"
        assert "admin" in result.value["tags"]

    async def test_success_result_none_values(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test getting successful task result with None/null values."""
        success_result = common_pb2.SuccessResult(result=common_pb2.AnyValue(json_value="null"))
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            success=success_result,
        )

        handle = TaskHandle("test-task-null", mock_client)
        result = await handle.try_result()

        assert result is not None
        assert result.value is None

    async def test_success_result_list_values(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test getting successful task result with list/array values."""
        list_data = [1, "two", True, None, {"nested": "object"}]

        success_result = common_pb2.SuccessResult(
            result=common_pb2.AnyValue(json_value=json.dumps(list_data))
        )
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            success=success_result,
        )

        handle = TaskHandle("test-task-list", mock_client)
        result = await handle.try_result()

        assert result is not None
        assert result.value == list_data
        assert len(result.value) == 5
        assert result.value[0] == 1
        assert result.value[1] == "two"
        assert result.value[2] is True
        assert result.value[3] is None
        assert result.value[4]["nested"] == "object"

    async def test_validation_error_with_detailed_data(
        self, mock_client: Client, mock_grpc_stub
    ) -> None:
        """Test ValidationError with detailed error data."""
        error_data = {"field": "count", "value": -5, "expected": "positive integer"}

        error_result = common_pb2.ErrorResult(
            type="ValidationError",
            message="Invalid argument: expected positive integer, got -5",
            data=json.dumps(error_data),
            retriable=False,
        )
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            error=error_result,
        )

        handle = TaskHandle("test-task", mock_client)

        result = await handle.try_result()

        assert result is not None
        assert result.success is False
        assert result.error is not None
        assert result.error.error_type == "ValidationError"
        assert "Invalid argument" in result.error.message
        assert result.error.retriable is False
        assert result.error.data == error_data

    async def test_type_error_with_detailed_data(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test TypeError with detailed error data."""
        error_data = {"expected_type": "string", "actual_type": "integer", "value": 42}

        error_result = common_pb2.ErrorResult(
            type="TypeError",
            message="Expected string, got integer",
            data=json.dumps(error_data),
            retriable=False,
        )
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            error=error_result,
        )

        handle = TaskHandle("test-task", mock_client)

        result = await handle.try_result()

        assert result is not None
        assert result.success is False
        assert result.error is not None
        assert result.error.error_type == "TypeError"
        assert "Expected string" in result.error.message
        assert result.error.retriable is False
        assert result.error.data == error_data

    async def test_runtime_error_retryable(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test RuntimeError that is retryable."""
        error_data = {"timeout_seconds": 30, "retry_count": 2, "database": "user_db"}

        error_result = common_pb2.ErrorResult(
            type="RuntimeError",
            message="Database connection timeout",
            data=json.dumps(error_data),
            retriable=True,
        )
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            error=error_result,
        )

        handle = TaskHandle("test-task", mock_client)

        result = await handle.try_result()

        assert result is not None
        assert result.success is False
        assert result.error is not None
        assert result.error.error_type == "RuntimeError"
        assert "Database connection" in result.error.message
        assert result.error.retriable is True
        assert result.error.data == error_data

    async def test_resource_error_retryable(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test ResourceError that is retryable."""
        error_data = {
            "requested_memory_mb": 1024,
            "available_memory_mb": 512,
            "resource_type": "memory",
        }

        error_result = common_pb2.ErrorResult(
            type="ResourceError",
            message="Insufficient memory available",
            data=json.dumps(error_data),
            retriable=True,
        )
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            error=error_result,
        )

        handle = TaskHandle("test-task", mock_client)

        result = await handle.try_result()

        assert result is not None
        assert result.success is False
        assert result.error is not None
        assert result.error.error_type == "ResourceError"
        assert "Insufficient memory" in result.error.message
        assert result.error.retriable is True
        assert result.error.data == error_data

    async def test_timeout_error_retryable(self, mock_client: Client, mock_grpc_stub) -> None:
        """Test TimeoutError that is retryable."""
        error_data = {
            "timeout_seconds": 300,
            "elapsed_seconds": 305,
            "stage": "data_processing",
        }

        error_result = common_pb2.ErrorResult(
            type="TimeoutError",
            message="Task execution exceeded 300 seconds",
            data=json.dumps(error_data),
            retriable=True,
        )
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            error=error_result,
        )

        handle = TaskHandle("test-task", mock_client)

        result = await handle.try_result()

        assert result is not None
        assert result.success is False
        assert result.error is not None
        assert result.error.error_type == "TimeoutError"
        assert "exceeded 300 seconds" in result.error.message
        assert result.error.retriable is True
        assert result.error.data == error_data

    async def test_custom_business_error_with_complex_data(
        self, mock_client: Client, mock_grpc_stub
    ) -> None:
        """Test custom business logic error with complex error data."""
        error_data = {
            "order_id": "ORD-12345",
            "payment_method": "expired_card",
            "valid_methods": ["credit_card", "paypal", "bank_transfer"],
            "user_id": 98765,
            "timestamp": "2024-01-15T10:30:00Z",
            "metadata": {"customer_tier": "premium", "retry_allowed": False},
        }

        error_result = common_pb2.ErrorResult(
            type="BusinessLogicError",
            message="Order cannot be processed: invalid payment method",
            data=json.dumps(error_data),
            retriable=False,
        )
        mock_grpc_stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
            status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
            error=error_result,
        )

        handle = TaskHandle("test-task", mock_client)

        result = await handle.try_result()

        assert result is not None
        assert result.success is False
        assert result.error is not None
        assert result.error.error_type == "BusinessLogicError"
        assert "Order cannot be processed" in result.error.message
        assert result.error.retriable is False
        assert result.error.data == error_data


class TestClientBuilder:
    """Test ClientBuilder functionality."""

    def test_client_builder_configuration(self) -> None:
        """Test client builder configuration."""
        builder = Client.builder()
        builder = (
            builder.endpoint("http://custom:9999")
            .domain("custom-domain")
            .timeout(45.0)
            .max_message_size(16 * 1024 * 1024)
        )

        assert builder._config.endpoint == "http://custom:9999"
        assert builder._config.domain == "custom-domain"
        assert builder._config.timeout == 45.0
        assert builder._config.max_message_size == 16 * 1024 * 1024
