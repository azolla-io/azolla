"""Tests for exception alias names to avoid confusion with built-ins."""


def test_exception_aliases_exist_and_subclass() -> None:
    # Import deferred to avoid ImportError at module import time pre-fix
    from azolla.exceptions import (
        AzollaConnectionError,
        ConnectionError,
        TaskTimeoutError,
        TimeoutError,
    )

    assert issubclass(AzollaConnectionError, ConnectionError)
    assert issubclass(TaskTimeoutError, TimeoutError)
