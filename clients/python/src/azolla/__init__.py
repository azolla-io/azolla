"""Azolla Python Client Library.

A modern, type-safe Python client for Azolla distributed task processing.
"""

from azolla._version import __version__, __version_info__
from azolla.client import Client, ClientConfig, TaskHandle
from azolla.exceptions import (
    AzollaError,
    ConnectionError,
    ResourceError,
    SerializationError,
    TaskError,
    TimeoutError,
    ValidationError,
    WorkerError,
)
from azolla.retry import ExponentialBackoff, FixedBackoff, LinearBackoff, RetryPolicy
from azolla.task import Task, azolla_task
from azolla.types import TaskContext, TaskResult, TaskStatus
from azolla.worker import Worker, WorkerConfig

__all__ = [
    "__version__",
    "__version_info__",
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
    "TaskContext",
    "TaskError",
    "TaskHandle",
    "TaskResult",
    "TaskStatus",
    "TimeoutError",
    "ValidationError",
    "Worker",
    "WorkerConfig",
    "WorkerError",
    "azolla_task",
]
