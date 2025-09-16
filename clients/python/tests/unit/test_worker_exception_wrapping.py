"""Tests for worker exception wrapping functionality."""

from typing import Any

import pytest

from azolla import Task, Worker, WorkerConfig
from azolla._grpc import common_pb2
from azolla.exceptions import TaskError, ValidationError


class AlwaysFailTask(Task):
    """Test task that always raises a non-TaskError exception."""

    def name(self) -> str:
        return "always_fail"

    async def execute(self, args: Any) -> str:
        raise ValueError("This is a test ValueError")


class AlwaysFailWithTaskErrorTask(Task):
    """Test task that always raises a TaskError."""

    def name(self) -> str:
        return "always_fail_task_error"

    async def execute(self, args: Any) -> str:
        raise ValidationError("This is a test ValidationError")


class AlwaysSucceedTask(Task):
    """Test task that always succeeds."""

    def name(self) -> str:
        return "always_succeed"

    async def execute(self, args: Any) -> str:
        return "success"


@pytest.mark.asyncio
async def test_non_task_error_wrapped_in_task_error() -> None:
    """Test that non-TaskError exceptions are wrapped in TaskError."""
    worker = Worker(WorkerConfig())

    # Register test task
    worker.register_task(AlwaysFailTask())

    # Create test task protobuf
    proto_task = common_pb2.Task(task_id="test-123", name="always_fail", args="[]")

    # Execute task
    result = await worker._execute_task(proto_task)

    # Verify the result
    assert result.task_id == "test-123"
    assert result.HasField("error")
    assert not result.HasField("success")

    error = result.error
    assert error.type == "ValueError"  # Original exception type
    assert "This is a test ValueError" in error.message
    assert error.retriable is True  # Default for wrapped exceptions
    assert error.data == "{}"


@pytest.mark.asyncio
async def test_task_error_preserved_as_is() -> None:
    """Test that TaskError and its subclasses are preserved without wrapping."""
    worker = Worker(WorkerConfig())

    # Register test task
    worker.register_task(AlwaysFailWithTaskErrorTask())

    # Create test task protobuf
    proto_task = common_pb2.Task(task_id="test-456", name="always_fail_task_error", args="[]")

    # Execute task
    result = await worker._execute_task(proto_task)

    # Verify the result
    assert result.task_id == "test-456"
    assert result.HasField("error")
    assert not result.HasField("success")

    error = result.error
    assert error.type == "ValidationError"  # TaskError subclass type
    assert "This is a test ValidationError" in error.message
    assert error.retriable is False  # ValidationError is not retryable
    assert error.data == "{}"


@pytest.mark.asyncio
async def test_successful_task_execution() -> None:
    """Test that successful task execution works correctly."""
    worker = Worker(WorkerConfig())

    # Register test task
    worker.register_task(AlwaysSucceedTask())

    # Create test task protobuf
    proto_task = common_pb2.Task(task_id="test-789", name="always_succeed", args="[]")

    # Execute task
    result = await worker._execute_task(proto_task)

    # Verify the result
    assert result.task_id == "test-789"
    assert not result.HasField("error")
    assert result.HasField("success")

    success = result.success
    assert success.result.json_value == '"success"'


@pytest.mark.asyncio
async def test_different_exception_types_wrapped_correctly() -> None:
    """Test that different exception types are wrapped with correct type information."""

    class CustomExceptionTask(Task):
        def __init__(self, exception_type: type[Exception], message: str):
            self.exception_type = exception_type
            self.exception_message = message

        def name(self) -> str:
            return "custom_exception"

        async def execute(self, args: Any) -> str:
            raise self.exception_type(self.exception_message)

    worker = Worker(WorkerConfig())

    # Test different exception types
    test_cases = [
        (ValueError, "ValueError message"),
        (TypeError, "TypeError message"),
        (RuntimeError, "RuntimeError message"),
        (KeyError, "KeyError message"),
    ]

    for exception_type, message in test_cases:
        # Register task with specific exception
        task = CustomExceptionTask(exception_type, message)
        worker.register_task(task)

        # Create test task protobuf
        proto_task = common_pb2.Task(
            task_id=f"test-{exception_type.__name__}",
            name="custom_exception",
            args="[]",
        )

        # Execute task
        result = await worker._execute_task(proto_task)

        # Verify the result
        assert result.HasField("error")
        error = result.error
        assert error.type == exception_type.__name__
        assert message in error.message
        assert error.retriable is True

        # Clear registry for next test
        worker._task_registry._tasks.clear()


@pytest.mark.asyncio
async def test_task_error_retryable_flag_preserved() -> None:
    """Test that TaskError retryable flag is correctly preserved."""

    class NonRetryableTaskErrorTask(Task):
        def name(self) -> str:
            return "non_retryable"

        async def execute(self, args: Any) -> str:
            raise TaskError("Non-retryable error", retryable=False)

    class RetryableTaskErrorTask(Task):
        def name(self) -> str:
            return "retryable"

        async def execute(self, args: Any) -> str:
            raise TaskError("Retryable error", retryable=True)

    worker = Worker(WorkerConfig())

    # Test non-retryable TaskError
    worker.register_task(NonRetryableTaskErrorTask())
    proto_task = common_pb2.Task(task_id="test-non-retryable", name="non_retryable", args="[]")
    result = await worker._execute_task(proto_task)
    assert result.error.retriable is False

    # Test retryable TaskError
    worker.register_task(RetryableTaskErrorTask())
    proto_task = common_pb2.Task(task_id="test-retryable", name="retryable", args="[]")
    result = await worker._execute_task(proto_task)
    assert result.error.retriable is True
