"""Single-invocation worker runtime for Azolla tasks."""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from enum import Enum
from typing import Any

from grpc import aio as grpc_aio

from azolla._grpc import common_pb2, shepherd_pb2, shepherd_pb2_grpc
from azolla.exceptions import TaskError, WorkerError
from azolla.task import Task

logger = logging.getLogger(__name__)


@dataclass
class WorkerInvocation:
    """Invocation context provided by the shepherd."""

    task_id: str
    task_name: str
    args: list[Any]
    kwargs: dict[str, Any]
    shepherd_endpoint: str

    @classmethod
    def from_json(
        cls,
        task_id: str,
        task_name: str,
        args_json: str,
        kwargs_json: str,
        shepherd_endpoint: str,
    ) -> WorkerInvocation:
        try:
            raw_args = json.loads(args_json) if args_json else []
        except json.JSONDecodeError as exc:
            raise WorkerError(f"Invalid args JSON: {exc}") from exc

        if not isinstance(raw_args, list):
            raw_args = [raw_args]

        try:
            raw_kwargs = json.loads(kwargs_json) if kwargs_json else {}
        except json.JSONDecodeError as exc:
            raise WorkerError(f"Invalid kwargs JSON: {exc}") from exc

        if not isinstance(raw_kwargs, dict):
            raise WorkerError("Kwargs JSON must encode an object")

        return cls(
            task_id=task_id,
            task_name=task_name,
            args=raw_args,
            kwargs=raw_kwargs,
            shepherd_endpoint=shepherd_endpoint,
        )


class TaskExecutionOutcome(Enum):
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class WorkerExecution:
    """Outcome of executing a worker invocation."""

    task_id: str
    outcome: TaskExecutionOutcome
    task_result: common_pb2.TaskResult
    value: Any | None = None
    error: TaskError | None = None

    def is_success(self) -> bool:
        return self.outcome is TaskExecutionOutcome.SUCCESS


class TaskRegistry:
    """Registry of task implementations."""

    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}

    def register(self, task: Any) -> None:
        if hasattr(task, "__azolla_task_instance__"):
            task_instance = task.__azolla_task_instance__
        elif isinstance(task, Task):
            task_instance = task
        else:  # pragma: no cover - defensive
            raise ValueError(f"Invalid task type: {type(task)!r}")

        name = task_instance.name()
        self._tasks[name] = task_instance
        logger.debug("Registered task '%s' as %s", name, task_instance.__class__.__name__)

    def get(self, name: str) -> Task | None:
        return self._tasks.get(name)

    def names(self) -> list[str]:
        return list(self._tasks.keys())

    def count(self) -> int:
        return len(self._tasks)


class Worker:
    """Worker that executes exactly one task per process."""

    def __init__(self) -> None:
        self._registry = TaskRegistry()

    @classmethod
    def builder(cls) -> WorkerBuilder:
        return WorkerBuilder()

    def register_task(self, task: Any) -> None:
        self._registry.register(task)

    def task_count(self) -> int:
        return self._registry.count()

    async def execute(self, invocation: WorkerInvocation) -> WorkerExecution:
        logger.info("Executing task %s (%s)", invocation.task_name, invocation.task_id)

        task = self._registry.get(invocation.task_name)
        if task is None:
            logger.warning(
                "No implementation registered for task '%s'. Available: %s",
                invocation.task_name,
                self._registry.names(),
            )
            error = TaskError(
                f"No implementation found for task: {invocation.task_name}",
                error_type="TaskNotFound",
                retryable=False,
            )
            result = _build_error_result(invocation.task_id, error)
            return WorkerExecution(
                task_id=invocation.task_id,
                outcome=TaskExecutionOutcome.FAILED,
                task_result=result,
                error=error,
            )

        raw_args: list[Any] = list(invocation.args)
        if invocation.kwargs:
            # When kwargs are present, append them so Task.parse_args can map appropriately.
            raw_args.append(invocation.kwargs)

        try:
            value = await task._execute_with_casting(raw_args)
        except TaskError as exc:
            logger.exception("Task %s failed", invocation.task_id)
            result = _build_error_result(invocation.task_id, exc)
            return WorkerExecution(
                task_id=invocation.task_id,
                outcome=TaskExecutionOutcome.FAILED,
                task_result=result,
                error=exc,
            )
        except Exception as exc:  # pragma: no cover - defensive
            logger.exception("Task %s raised unexpected error", invocation.task_id)
            wrapped = TaskError(
                message=str(exc),
                error_type=exc.__class__.__name__,
                retryable=True,
            )
            result = _build_error_result(invocation.task_id, wrapped)
            return WorkerExecution(
                task_id=invocation.task_id,
                outcome=TaskExecutionOutcome.FAILED,
                task_result=result,
                error=wrapped,
            )

        result = _build_success_result(invocation.task_id, value)
        return WorkerExecution(
            task_id=invocation.task_id,
            outcome=TaskExecutionOutcome.SUCCESS,
            task_result=result,
            value=value,
        )

    async def run_invocation(self, invocation: WorkerInvocation) -> WorkerExecution:
        execution = await self.execute(invocation)
        await self._report_result(invocation, execution.task_result)
        return execution

    async def _report_result(
        self, invocation: WorkerInvocation, task_result: common_pb2.TaskResult
    ) -> None:
        logger.info(
            "Reporting result for task %s to %s", invocation.task_id, invocation.shepherd_endpoint
        )

        endpoint = invocation.shepherd_endpoint
        if endpoint.startswith("http://"):
            endpoint = endpoint[len("http://") :]
        elif endpoint.startswith("https://"):
            endpoint = endpoint[len("https://") :]

        channel = grpc_aio.insecure_channel(endpoint)
        try:
            stub = shepherd_pb2_grpc.WorkerStub(channel)  # type: ignore[no-untyped-call]
            request = shepherd_pb2.ReportResultRequest(
                task_id=invocation.task_id,
                result=task_result,
            )
            response = await stub.ReportResult(request)
            if not response.success:
                raise WorkerError(
                    f"Shepherd rejected result: {response.message or 'unknown error'}"
                )
        finally:
            await channel.close()


class WorkerBuilder:
    """Builder that collects tasks before instantiating a worker."""

    def __init__(self) -> None:
        self._tasks: list[Any] = []

    def register_task(self, task: Any) -> WorkerBuilder:
        self._tasks.append(task)
        return self

    def build(self) -> Worker:
        worker = Worker()
        for task in self._tasks:
            worker.register_task(task)
        return worker


def _build_success_result(task_id: str, value: Any) -> common_pb2.TaskResult:
    json_value = json.dumps(value) if value is not None else "null"
    return common_pb2.TaskResult(
        task_id=task_id,
        success=common_pb2.SuccessResult(result=common_pb2.AnyValue(json_value=json_value)),
    )


def _build_error_result(task_id: str, error: TaskError) -> common_pb2.TaskResult:
    payload: dict[str, Any] = {}
    if error.error_code:
        payload["code"] = error.error_code
    if error.extra_data:
        payload.update(error.extra_data)

    data_json = json.dumps(payload) if payload else "{}"

    return common_pb2.TaskResult(
        task_id=task_id,
        error=common_pb2.ErrorResult(
            type=error.error_type or error.__class__.__name__,
            message=error.message,
            data=data_json,
            retriable=error.retryable,
        ),
    )


__all__ = [
    "TaskExecutionOutcome",
    "Worker",
    "WorkerBuilder",
    "WorkerExecution",
    "WorkerInvocation",
]
