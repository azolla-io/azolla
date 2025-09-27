"""Azolla Python Client Library.

A modern, type-safe Python client for Azolla distributed task processing.
"""

from azolla._version import __version__, __version_info__
from azolla.client import Client, ClientConfig, TaskHandle
from azolla.exceptions import (
    AzollaConnectionError,
    AzollaError,
    ConnectionError,
    ResourceError,
    SerializationError,
    TaskError,
    TaskTimeoutError,
    TaskValidationError,
    ValidationError,
    WorkerError,
)
from azolla.retry import ExponentialBackoff, FixedBackoff, LinearBackoff, RetryPolicy
from azolla.task import Task, azolla_task
from azolla.types import TaskResult, TaskStatus
from azolla.worker import (
    TaskExecutionOutcome,
    Worker,
    WorkerExecution,
    WorkerInvocation,
)

__all__ = [
    "AzollaConnectionError",
    "AzollaError",
    "Client",
    "ClientConfig",
    "ConnectionError",
    "ExponentialBackoff",
    "FixedBackoff",
    "LinearBackoff",
    "ResourceError",
    "RetryPolicy",
    "SerializationError",
    "Task",
    "TaskError",
    "TaskExecutionOutcome",
    "TaskHandle",
    "TaskResult",
    "TaskStatus",
    "TaskTimeoutError",
    "TaskValidationError",
    "ValidationError",
    "Worker",
    "WorkerError",
    "WorkerExecution",
    "WorkerInvocation",
    "__version__",
    "__version_info__",
    "azolla_task",
]
