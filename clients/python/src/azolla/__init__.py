"""Azolla Python Client Library.

A modern, type-safe Python client for Azolla distributed task processing.
"""

from azolla._version import __version__, __version_info__
from azolla.client import Client, ClientConfig, TaskHandle
from azolla.worker import Worker, WorkerConfig
from azolla.task import Task, azolla_task
from azolla.types import TaskContext, TaskResult, TaskStatus
from azolla.exceptions import (
    AzollaError,
    TaskError,
    ValidationError,
    TimeoutError,
    ResourceError,
    ConnectionError,
    SerializationError,
    WorkerError,
)
from azolla.retry import RetryPolicy, ExponentialBackoff, LinearBackoff, FixedBackoff

__all__ = [
    # Version info
    "__version__",
    "__version_info__",
    # Core classes
    "Client",
    "ClientConfig",
    "TaskHandle",
    "Worker",
    "WorkerConfig",
    "Task",
    "TaskContext",
    # Decorators
    "azolla_task",
    # Exceptions
    "AzollaError",
    "TaskError",
    "ValidationError", 
    "TimeoutError",
    "ResourceError",
    "ConnectionError",
    "SerializationError",
    "WorkerError",
    # Retry policies
    "RetryPolicy",
    "ExponentialBackoff",
    "LinearBackoff",
    "FixedBackoff",
    # Types
    "TaskResult",
    "TaskStatus",
]