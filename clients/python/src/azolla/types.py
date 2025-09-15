"""Type definitions for Azolla client library."""

from enum import Enum
from typing import Generic, Optional, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class TaskStatus(str, Enum):
    """Task execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class TaskResult(BaseModel, Generic[T]):
    """Represents the result of successful task execution."""

    task_id: str
    value: T


class TaskContext(BaseModel):
    """Task execution context."""

    task_id: str
    attempt_number: int
    max_attempts: Optional[int] = None

    def is_final_attempt(self) -> bool:
        """Check if this is the final retry attempt."""
        return (
            self.max_attempts is not None and self.attempt_number >= self.max_attempts
        )
