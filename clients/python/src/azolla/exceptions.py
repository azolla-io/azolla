"""Exception hierarchy for Azolla client library."""
from typing import Any, Optional, Dict

class AzollaError(Exception):
    """Base exception for all Azolla-related errors."""
    def __init__(self, message: str, **extra_data: Any) -> None:
        super().__init__(message)
        self.message = message
        self.extra_data = extra_data

class ConnectionError(AzollaError):
    """Raised when connection to orchestrator fails."""
    pass

class TaskError(AzollaError):
    """Base exception for task execution errors."""
    def __init__(
        self, 
        message: str, 
        error_code: str = "TASK_ERROR",
        error_type: Optional[str] = None,
        retryable: bool = True,
        **extra_data: Any
    ) -> None:
        super().__init__(message, **extra_data)
        self.error_code = error_code  
        self.error_type = error_type or self.__class__.__name__
        self.retryable = retryable
        
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "message": self.message,
            "error_code": self.error_code,
            "error_type": self.error_type,
            "retryable": self.retryable,
            **self.extra_data
        }

class ValidationError(TaskError):
    """Raised when task arguments are invalid."""
    def __init__(self, message: str, **extra_data: Any) -> None:
        super().__init__(message, error_code="VALIDATION_ERROR", retryable=False, **extra_data)

class TimeoutError(TaskError):
    """Raised when task execution times out."""
    def __init__(self, message: str, **extra_data: Any) -> None:
        super().__init__(message, error_code="TIMEOUT_ERROR", retryable=True, **extra_data)

class ResourceError(TaskError):
    """Raised when required resources are unavailable."""
    def __init__(self, message: str, **extra_data: Any) -> None:
        super().__init__(message, error_code="RESOURCE_ERROR", retryable=True, **extra_data)

class SerializationError(AzollaError):
    """Raised when serialization/deserialization fails."""
    pass

class WorkerError(AzollaError):
    """Raised when worker encounters an error."""
    pass