"""Exception hierarchy for Azolla client library."""

from typing import Any, Optional


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
        **extra_data: Any,
    ) -> None:
        super().__init__(message, **extra_data)
        self.error_code = error_code
        self.error_type = error_type or self.__class__.__name__
        self.retryable = retryable

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "message": self.message,
            "error_code": self.error_code,
            "error_type": self.error_type,
            "retryable": self.retryable,
            **self.extra_data,
        }


class TaskValidationError(TaskError):
    """Raised when task arguments are invalid."""

    def __init__(self, message: str, **extra_data: Any) -> None:
        super().__init__(message, error_code="VALIDATION_ERROR", retryable=False, **extra_data)


class TaskTimeoutError(TaskError):
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


# Preferred aliases to avoid confusion with Python built-ins in user code.
class AzollaConnectionError(ConnectionError):
    """Alias for ConnectionError to avoid shadowing built-in names."""

    pass


class ValidationError(TaskValidationError):
    """Alias for TaskValidationError for backward compatibility."""

    pass


class TaskNotFoundError(AzollaError):
    """Raised when a task is not found."""

    pass


class TaskWaitTimeoutError(AzollaError):
    """Raised when waiting for task completion times out."""

    pass


class TaskInternalError(AzollaError):
    """Raised when an internal server error occurs."""

    pass
