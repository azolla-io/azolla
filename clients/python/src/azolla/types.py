"""Type definitions for Azolla client library."""
from typing import Any, Optional, Generic, TypeVar
from enum import Enum
from pydantic import BaseModel

T = TypeVar('T')

class TaskStatus(str, Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running" 
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

class TaskResult(BaseModel, Generic[T]):
    """Represents the result of task execution."""
    task_id: str
    status: TaskStatus
    value: Optional[T] = None
    error: Optional[str] = None
    error_code: Optional[str] = None
    error_type: Optional[str] = None
    execution_time: Optional[float] = None
    attempt_number: int = 1
    max_attempts: Optional[int] = None
    
    @property
    def success(self) -> bool:
        """Check if task completed successfully."""
        return self.status == TaskStatus.COMPLETED
        
    @property
    def failed(self) -> bool:
        """Check if task failed."""
        return self.status == TaskStatus.FAILED

class TaskContext(BaseModel):
    """Task execution context."""
    task_id: str
    attempt_number: int
    max_attempts: Optional[int] = None
    
    def is_final_attempt(self) -> bool:
        """Check if this is the final retry attempt."""
        return self.max_attempts is not None and self.attempt_number >= self.max_attempts