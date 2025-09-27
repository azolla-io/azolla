"""Tests for exception alias names to avoid confusion with built-ins."""


def test_exception_aliases_exist_and_subclass() -> None:
    # Import deferred to avoid ImportError at module import time pre-fix
    from azolla.exceptions import (
        AzollaConnectionError,
        ConnectionError,
        TaskTimeoutError,
    )

    assert issubclass(AzollaConnectionError, ConnectionError)
    # TaskTimeoutError should be a subclass of TaskError
    from azolla.exceptions import TaskError

    assert issubclass(TaskTimeoutError, TaskError)
