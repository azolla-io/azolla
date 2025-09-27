"""Tests for worker exception handling in the single-run worker runtime."""

from __future__ import annotations

from typing import Any

import pytest

from azolla import Task, Worker, WorkerInvocation
from azolla.exceptions import TaskError, ValidationError
from azolla.worker import TaskExecutionOutcome


class AlwaysFailTask(Task):
    def name(self) -> str:
        return "always_fail"

    async def execute(self, args: Any) -> str:
        raise ValueError("This is a test ValueError")


class AlwaysFailWithTaskErrorTask(Task):
    def name(self) -> str:
        return "always_fail_task_error"

    async def execute(self, args: Any) -> str:
        raise ValidationError("This is a test ValidationError")


class AlwaysSucceedTask(Task):
    def name(self) -> str:
        return "always_succeed"

    async def execute(self, args: Any) -> str:
        return "success"


@pytest.mark.asyncio
async def test_non_task_error_wrapped_in_task_error() -> None:
    worker = Worker.builder().register_task(AlwaysFailTask()).build()
    invocation = WorkerInvocation(
        task_id="test-123",
        task_name="always_fail",
        args=[],
        kwargs={},
        shepherd_endpoint="http://127.0.0.1:0",
    )

    execution = await worker.execute(invocation)
    assert execution.outcome is TaskExecutionOutcome.FAILED
    assert execution.task_result.HasField("error")
    error = execution.task_result.error
    assert error.type == "ValueError"
    assert "test ValueError" in error.message
    assert error.retriable is True


@pytest.mark.asyncio
async def test_task_error_preserved_as_is() -> None:
    worker = Worker.builder().register_task(AlwaysFailWithTaskErrorTask()).build()
    invocation = WorkerInvocation(
        task_id="test-456",
        task_name="always_fail_task_error",
        args=[],
        kwargs={},
        shepherd_endpoint="http://127.0.0.1:0",
    )

    execution = await worker.execute(invocation)
    assert execution.outcome is TaskExecutionOutcome.FAILED
    error = execution.task_result.error
    assert error.type == "ValidationError"
    assert "test ValidationError" in error.message
    assert error.retriable is False


@pytest.mark.asyncio
async def test_successful_task_execution() -> None:
    worker = Worker.builder().register_task(AlwaysSucceedTask()).build()
    invocation = WorkerInvocation(
        task_id="test-789",
        task_name="always_succeed",
        args=[],
        kwargs={},
        shepherd_endpoint="http://127.0.0.1:0",
    )

    execution = await worker.execute(invocation)
    assert execution.outcome is TaskExecutionOutcome.SUCCESS
    assert execution.task_result.HasField("success")
    assert execution.task_result.success.result.json_value == '"success"'


@pytest.mark.asyncio
async def test_different_exception_types_wrapped_correctly() -> None:
    class CustomExceptionTask(Task):
        def __init__(self, exception_type: type[Exception], message: str):
            self.exception_type = exception_type
            self.exception_message = message

        def name(self) -> str:
            return "custom_exception"

        async def execute(self, args: Any) -> str:
            raise self.exception_type(self.exception_message)

    test_cases = [
        (ValueError, "ValueError message"),
        (TypeError, "TypeError message"),
        (RuntimeError, "RuntimeError message"),
        (KeyError, "KeyError message"),
    ]

    for exception_type, message in test_cases:
        worker = (
            Worker.builder().register_task(CustomExceptionTask(exception_type, message)).build()
        )
        invocation = WorkerInvocation(
            task_id=f"test-{exception_type.__name__}",
            task_name="custom_exception",
            args=[],
            kwargs={},
            shepherd_endpoint="http://127.0.0.1:0",
        )
        execution = await worker.execute(invocation)
        assert execution.outcome is TaskExecutionOutcome.FAILED
        error = execution.task_result.error
        assert error.type == exception_type.__name__
        assert message in error.message
        assert error.retriable is True


@pytest.mark.asyncio
async def test_task_error_retryable_flag_preserved() -> None:
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

    worker = Worker.builder().register_task(NonRetryableTaskErrorTask()).build()
    invocation = WorkerInvocation(
        task_id="test-non-retryable",
        task_name="non_retryable",
        args=[],
        kwargs={},
        shepherd_endpoint="http://127.0.0.1:0",
    )
    execution = await worker.execute(invocation)
    assert execution.task_result.error.retriable is False

    worker = Worker.builder().register_task(RetryableTaskErrorTask()).build()
    invocation = WorkerInvocation(
        task_id="test-retryable",
        task_name="retryable",
        args=[],
        kwargs={},
        shepherd_endpoint="http://127.0.0.1:0",
    )
    execution = await worker.execute(invocation)
    assert execution.task_result.error.retriable is True
