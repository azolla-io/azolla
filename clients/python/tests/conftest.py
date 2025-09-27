"""Shared test fixtures and configuration."""

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from azolla import Client, ClientConfig
from azolla._grpc import common_pb2, orchestrator_pb2


@pytest.fixture
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture
async def mock_grpc_stub():
    """Provide a mock gRPC stub for testing."""
    stub = AsyncMock()
    # Mock successful task creation
    stub.CreateTask.return_value = orchestrator_pb2.CreateTaskResponse(task_id="test-task-123")

    # Mock task completion with new structure
    success_result = common_pb2.SuccessResult(
        result=common_pb2.AnyValue(json_value='{"status": "success", "message": "Task completed"}')
    )
    stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
        status_code=orchestrator_pb2.WAIT_FOR_TASK_STATUS_COMPLETED,
        success=success_result,
    )

    return stub


@pytest.fixture
async def mock_client(mock_grpc_stub) -> Client:
    """Provide a test client with mocked gRPC stub."""
    config = ClientConfig(endpoint="localhost:52710")
    client = Client(config)

    # Replace the stub with our mock
    client._stub = mock_grpc_stub
    client._channel = MagicMock()

    return client


@pytest.fixture
def sample_task_args() -> dict[str, Any]:
    """Provide sample task arguments for testing."""
    return {"name": "test", "count": 42, "enabled": True, "metadata": {"key": "value"}}
