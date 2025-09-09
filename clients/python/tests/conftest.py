"""Shared test fixtures and configuration."""
import pytest
import asyncio
from typing import AsyncGenerator, Dict, Any
from unittest.mock import AsyncMock, MagicMock

from azolla import Client, Worker, ClientConfig, WorkerConfig
from azolla._grpc import orchestrator_pb2

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
    stub.CreateTask.return_value = orchestrator_pb2.CreateTaskResponse(
        task_id="test-task-123"
    )
    
    # Mock task completion
    stub.WaitForTask.return_value = orchestrator_pb2.WaitForTaskResponse(
        status="completed",
        result='{"status": "success", "message": "Task completed"}'
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
def worker_config() -> WorkerConfig:
    """Provide a test worker configuration."""
    return WorkerConfig(
        orchestrator_endpoint="localhost:52710",
        domain="test-domain",
        shepherd_group="test-workers",
        max_concurrency=5,
        heartbeat_interval=1.0
    )

@pytest.fixture
def sample_task_args() -> Dict[str, Any]:
    """Provide sample task arguments for testing."""
    return {
        "name": "test",
        "count": 42,
        "enabled": True,
        "metadata": {"key": "value"}
    }