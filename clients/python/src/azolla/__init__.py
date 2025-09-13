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
    # Version info
    "__version__",
    "__version_info__",
    # Core classes
    "Client",
    "ClientConfig",
    "Task",
    "TaskContext",
    "TaskHandle",
    "Worker",
    "WorkerConfig",
    # Decorators
    "azolla_task",
    # Exceptions
    "AzollaError",
    "ConnectionError",
    "ResourceError",
    "SerializationError",
    "TaskError",
    "TimeoutError",
    "ValidationError",
    "WorkerError",
    # Retry policies
    "ExponentialBackoff",
    "FixedBackoff",
    "LinearBackoff",
    "RetryPolicy",
    # Types
    "TaskResult",
    "TaskStatus",
]