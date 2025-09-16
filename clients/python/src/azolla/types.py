"""Type definitions for Azolla client library."""

from enum import Enum
from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class TaskStatus(str, Enum):
    """Task execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ErrorInfo(BaseModel):
    """Structured error information for failed tasks."""

    message: str
    error_type: str
    retriable: Optional[bool] = None
    data: Optional[dict[str, Any]] = None


class TaskResult(BaseModel, Generic[T]):
    """Represents the result of task execution, successful or failed."""

    task_id: str
    success: bool
    value: Optional[T] = None
    error: Optional[ErrorInfo] = None


class TaskContext(BaseModel):
    """Task execution context."""

    task_id: str
    attempt_number: int
    max_attempts: Optional[int] = None

    def is_final_attempt(self) -> bool:
        """Check if this is the final retry attempt."""
        return self.max_attempts is not None and self.attempt_number >= self.max_attempts
