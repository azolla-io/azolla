"""Client implementation for Azolla task submission."""

import asyncio
import json
from collections.abc import Awaitable
from datetime import datetime
from typing import Any, Callable, Optional, Union

import grpc
from pydantic import BaseModel, Field

from azolla._grpc import common_pb2, orchestrator_pb2, orchestrator_pb2_grpc
from azolla.exceptions import (
    ConnectionError,
    SerializationError,
    TaskError,
    TaskInternalError,
    TaskNotFoundError,
    TaskWaitTimeoutError,
)
from azolla.retry import ExponentialBackoff, FixedBackoff, LinearBackoff, RetryPolicy
from azolla.retry import RetryPolicy as PyRetryPolicy
from azolla.types import ErrorInfo, TaskResult


def _convert_any_value_to_json(any_value: Any) -> Any:
    """Convert protobuf AnyValue to Python object."""
    if any_value.HasField("string_value"):
        return any_value.string_value
    elif any_value.HasField("int_value"):
        return any_value.int_value
    elif any_value.HasField("double_value"):
        return any_value.double_value
    elif any_value.HasField("bool_value"):
        return any_value.bool_value
    elif any_value.HasField("json_value"):
        try:
            return json.loads(any_value.json_value)
        except json.JSONDecodeError:
            return any_value.json_value
    else:
        return None


class ClientConfig(BaseModel):
    """Configuration for the Azolla client."""

    endpoint: str = Field(default="http://localhost:52710")
    domain: str = Field(default="default")
    timeout: float = Field(default=30.0, gt=0)
    max_message_size: int = Field(default=4 * 1024 * 1024)  # 4MB


class TaskSubmissionBuilder:
    """Builder for task submissions with retry policies."""

    def __init__(self, client: "Client", task_name: str) -> None:
        self._client = client
        self._task_name = task_name
        self._args: Any = None
        self._retry_policy: Optional[RetryPolicy] = None
        self._shepherd_group: Optional[str] = None
        self._flow_instance_id: Optional[str] = None

    def args(self, args: Any) -> "TaskSubmissionBuilder":
        """set task arguments."""
        self._args = args
        return self

    def retry_policy(self, policy: RetryPolicy) -> "TaskSubmissionBuilder":
        """set retry policy for this task."""
        self._retry_policy = policy
        return self

    def with_retry(self, policy: RetryPolicy) -> "TaskSubmissionBuilder":
        """set retry policy for this task (alias for retry_policy)."""
        return self.retry_policy(policy)

    def shepherd_group(self, group: str) -> "TaskSubmissionBuilder":
        """set shepherd group for targeted execution."""
        self._shepherd_group = group
        return self

    def flow_instance_id(self, flow_id: str) -> "TaskSubmissionBuilder":
        """set flow instance ID if task is part of a flow."""
        self._flow_instance_id = flow_id
        return self

    async def submit(self) -> "TaskHandle":
        """Submit the task and get a handle."""
        # Ensure connection is established
        await self._client._ensure_connection()

        try:
            # Serialize arguments
            if self._args is None:
                # Preserve explicit null argument but wrap in an array so the
                # orchestrator can deserialize consistently
                args_json = "[null]"
            elif isinstance(self._args, (list, tuple)):
                args_json = json.dumps(list(self._args))
            elif isinstance(self._args, dict):
                args_json = json.dumps([self._args])
            else:
                args_json = json.dumps([self._args])

            retry_policy_msg: Optional[common_pb2.RetryPolicy] = None
            if self._retry_policy:
                retry_policy_msg = self._to_proto_retry_policy(self._retry_policy)

            # Create gRPC request
            request_kwargs: dict[str, Any] = {
                "name": self._task_name,
                "domain": self._client._config.domain,
                "args": args_json,
                "kwargs": "{}",  # Not used in Python client
                "flow_instance_id": self._flow_instance_id,
                "shepherd_group": self._shepherd_group,
            }

            if retry_policy_msg is not None:
                request_kwargs["retry_policy"] = retry_policy_msg

            request = orchestrator_pb2.CreateTaskRequest(**request_kwargs)

            # Submit task
            if self._client._stub is None:
                raise ConnectionError("Client not connected")
            response = await self._client._stub.CreateTask(
                request, timeout=self._client._config.timeout
            )

            return TaskHandle(task_id=response.task_id, client=self._client)

        except grpc.RpcError as e:
            raise ConnectionError(f"Failed to submit task: {e.details()}") from e
        except (json.JSONDecodeError, TypeError) as e:
            raise SerializationError(f"Failed to serialize task arguments: {e}") from e

    @staticmethod
    def _to_proto_retry_policy(policy: PyRetryPolicy) -> common_pb2.RetryPolicy:
        """Convert Python RetryPolicy to orchestrator RetryPolicy proto."""
        wait = common_pb2.RetryPolicyWait()
        backoff = policy.backoff
        if isinstance(backoff, ExponentialBackoff):
            if backoff.jitter:
                wait.exponential_jitter.CopyFrom(
                    common_pb2.RetryPolicyExponentialJitterWait(
                        initial_delay=backoff.initial,
                        multiplier=backoff.multiplier,
                        max_delay=backoff.max_delay,
                    )
                )
            else:
                wait.exponential.CopyFrom(
                    common_pb2.RetryPolicyExponentialWait(
                        initial_delay=backoff.initial,
                        multiplier=backoff.multiplier,
                        max_delay=backoff.max_delay,
                    )
                )
        elif isinstance(backoff, FixedBackoff):
            wait.fixed.CopyFrom(common_pb2.RetryPolicyFixedWait(delay=backoff.delay))
        elif isinstance(backoff, LinearBackoff):
            wait.exponential.CopyFrom(
                common_pb2.RetryPolicyExponentialWait(
                    initial_delay=backoff.initial,
                    multiplier=1.0,
                    max_delay=backoff.max_delay,
                )
            )
        else:
            wait.exponential_jitter.CopyFrom(
                common_pb2.RetryPolicyExponentialJitterWait(
                    initial_delay=1.0,
                    multiplier=2.0,
                    max_delay=300.0,
                )
            )

        include_errors: list[str] = []
        for item in policy.retry_on:
            if isinstance(item, str):
                include_errors.append(item)
            elif isinstance(item, type):
                include_errors.append(item.__name__)
            else:
                include_errors.append(str(item))
        retry = common_pb2.RetryPolicyRetry(
            include_errors=include_errors,
            exclude_errors=list(policy.stop_on_codes),
        )

        stop = common_pb2.RetryPolicyStop(max_attempts=policy.max_attempts)

        return common_pb2.RetryPolicy(
            version=1,
            stop=stop,
            wait=wait,
            retry=retry,
        )


class TaskHandle:
    """Handle to a submitted task."""

    def __init__(self, task_id: str, client: "Client") -> None:
        self.task_id = task_id
        self._client = client

    async def wait(self, timeout: Optional[float] = None) -> TaskResult[Any]:
        """Wait for task completion with exponential backoff polling.

        Returns a ``TaskResult`` regardless of success or failure. Connection-level
        issues and task metadata problems still surface as exceptions.
        """
        start_time = datetime.now()
        poll_interval = 0.1  # Start with 100ms
        max_poll_interval = 5.0  # Max 5 seconds

        while True:
            # Check if we've exceeded timeout
            if timeout and (datetime.now() - start_time).total_seconds() > timeout:
                raise TaskWaitTimeoutError(f"Task wait timeout exceeded after {timeout} seconds")

            try:
                result = await self.try_result()
                if result is not None:
                    return result

                # Task still running, wait before next poll
                await asyncio.sleep(poll_interval)
                poll_interval = min(poll_interval * 1.5, max_poll_interval)

            except (TaskNotFoundError, TaskInternalError):
                # Re-raise these without wrapping
                raise
            except ConnectionError:
                # Re-raise connection errors without wrapping
                raise
            except Exception as e:
                # Wrap unexpected exceptions in a generic TaskError
                raise TaskError(f"Unexpected error while waiting for task: {e}") from e

    async def try_result(self) -> Optional[TaskResult[Any]]:
        """Try to get result without blocking.

        Returns ``None`` if the task is still running, a ``TaskResult`` with
        ``success=True`` when the task completed successfully, or a ``TaskResult``
        with structured ``ErrorInfo`` for task-level failures. Connection issues
        and orchestrator errors continue to raise exceptions.
        """
        # Ensure connection is established
        await self._client._ensure_connection()

        try:
            request = orchestrator_pb2.WaitForTaskRequest(
                task_id=self.task_id,
                domain=self._client._config.domain,
                timeout_ms=0,  # Non-blocking call
            )

            if self._client._stub is None:
                raise ConnectionError("Client not connected")
            response = await self._client._stub.WaitForTask(
                request, timeout=self._client._config.timeout
            )
            # Additional diagnostics can be added here when investigating wait behaviour

            # Check status code using enum values
            if response.status_code == orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED:
                if response.HasField("success"):
                    # Extract result value from SuccessResult
                    result_value = None
                    if response.success.result:
                        result_value = _convert_any_value_to_json(response.success.result)
                    return TaskResult(task_id=self.task_id, success=True, value=result_value)
                elif response.HasField("error"):
                    # Task completed with error - return structured failure result
                    error_data: Optional[dict[str, Any]] = None
                    if response.error.data:
                        try:
                            error_data = json.loads(response.error.data)
                        except json.JSONDecodeError:
                            error_data = {"raw_data": response.error.data}

                    return TaskResult(
                        task_id=self.task_id,
                        success=False,
                        value=None,
                        error=ErrorInfo(
                            message=response.error.message,
                            error_type=response.error.type or "TaskError",
                            retriable=response.error.retriable,
                            data=error_data,
                        ),
                    )
                else:
                    raise TaskInternalError("Task completed but no result provided")

            elif response.status_code == orchestrator_pb2.WAIT_FOR_TASK_STATUS_TASK_NOT_FOUND:
                raise TaskNotFoundError(f"Task {self.task_id} not found")

            elif response.status_code == orchestrator_pb2.WAIT_FOR_TASK_STATUS_TIMEOUT:
                return None  # Still running (timeout in non-blocking call means not ready)

            elif response.status_code == orchestrator_pb2.WAIT_FOR_TASK_STATUS_INTERNAL_ERROR:
                raise TaskInternalError("Internal server error")

            else:
                return None  # Unspecified status, assume still running

        except grpc.RpcError as e:
            raise ConnectionError(f"Failed to get task result: {e.details()}") from e


class Client:
    """Main client for interacting with Azolla orchestrator."""

    def __init__(
        self,
        config: Optional[ClientConfig] = None,
        orchestrator_endpoint: Optional[str] = None,
        domain: Optional[str] = None,
        timeout: Optional[float] = None,
        **kwargs: Any,
    ) -> None:
        # Support both documented API and config-based API
        if config is not None:
            self._config = config
        elif orchestrator_endpoint is not None:
            # Create config from documented constructor parameters
            config_params: dict[str, Any] = {"endpoint": orchestrator_endpoint}
            if domain is not None:
                config_params["domain"] = domain
            if timeout is not None:
                config_params["timeout"] = timeout
            config_params.update(kwargs)
            self._config = ClientConfig(**config_params)
        else:
            raise ValueError("Either 'config' or 'orchestrator_endpoint' must be provided")

        self._channel: Optional[grpc.aio.Channel] = None
        self._stub: Optional[Any] = None

    @classmethod
    async def connect(cls, endpoint: str, **kwargs: Any) -> "Client":
        """Connect to Azolla orchestrator with default config."""
        config = ClientConfig(endpoint=endpoint, **kwargs)
        client = cls(config)
        await client._ensure_connection()
        return client

    @staticmethod
    def builder() -> "ClientBuilder":
        """Create a client builder."""
        return ClientBuilder()

    async def _ensure_connection(self) -> None:
        """Ensure gRPC connection is established."""
        if self._channel is None:
            # Parse endpoint to remove http:// prefix if present
            endpoint = self._config.endpoint
            if endpoint.startswith("http://"):
                endpoint = endpoint[7:]
            elif endpoint.startswith("https://"):
                # TLS not yet supported for Python client gRPC
                raise ValueError(
                    "HTTPS endpoints are not supported yet. Use plaintext host:port or http://"
                )

            self._channel = grpc.aio.insecure_channel(
                endpoint,
                options=[
                    ("grpc.max_send_message_length", self._config.max_message_size),
                    ("grpc.max_receive_message_length", self._config.max_message_size),
                    # Keep-alive settings to prevent connection drops
                    ("grpc.keepalive_time_ms", 30000),  # Send keep-alive every 30s
                    (
                        "grpc.keepalive_timeout_ms",
                        10000,
                    ),  # Wait 10s for keep-alive response
                    (
                        "grpc.keepalive_permit_without_calls",
                        True,
                    ),  # Allow keep-alive without active calls
                    ("grpc.http2.max_pings_without_data", 0),  # Unlimited pings
                    (
                        "grpc.http2.min_time_between_pings_ms",
                        10000,
                    ),  # Min 10s between pings
                    (
                        "grpc.http2.min_ping_interval_without_data_ms",
                        300000,
                    ),  # 5min without data
                ],
            )
            self._stub = orchestrator_pb2_grpc.ClientServiceStub(self._channel)  # type: ignore[no-untyped-call]

    def submit_task(
        self, task: Union[str, Callable[..., Awaitable[Any]]], args: Any = None
    ) -> TaskSubmissionBuilder:
        """Submit a task for execution."""
        if isinstance(task, str):
            task_name = task
        elif hasattr(task, "__azolla_task_instance__"):
            # Decorated function
            task_name = task.__azolla_task_instance__.name()
        elif hasattr(task, "name"):
            # Task instance
            task_name = task.name()
        else:
            # Try to get name from function
            task_name = getattr(task, "__name__", str(task))

        builder = TaskSubmissionBuilder(self, task_name)
        if args is not None:
            builder = builder.args(args)
        return builder

    async def close(self) -> None:
        """Close the gRPC connection."""
        if self._channel:
            await self._channel.close()
            self._channel = None
            self._stub = None

    async def __aenter__(self) -> "Client":
        """Async context manager entry."""
        await self._ensure_connection()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Async context manager exit."""
        await self.close()


class ClientBuilder:
    """Builder for client configuration."""

    def __init__(self) -> None:
        self._config = ClientConfig()

    def endpoint(self, endpoint: str) -> "ClientBuilder":
        """set orchestrator endpoint."""
        self._config.endpoint = endpoint
        return self

    def domain(self, domain: str) -> "ClientBuilder":
        """set domain."""
        self._config.domain = domain
        return self

    def timeout(self, timeout: float) -> "ClientBuilder":
        """set request timeout."""
        self._config.timeout = timeout
        return self

    def max_message_size(self, size: int) -> "ClientBuilder":
        """set maximum gRPC message size."""
        self._config.max_message_size = size
        return self

    async def build(self) -> Client:
        """Build and connect the client."""
        client = Client(self._config)
        await client._ensure_connection()
        return client
