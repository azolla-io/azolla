"""Tests for worker ready signaling behavior."""

import pytest

from azolla import Worker, WorkerConfig
from azolla._grpc import orchestrator_pb2


class _FakeStream:
    def __init__(self, messages):
        self._messages = messages
        self._index = 0

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._index >= len(self._messages):
            raise StopAsyncIteration
        msg = self._messages[self._index]
        self._index += 1
        return msg


@pytest.mark.asyncio
async def test_ready_signal_set_on_first_server_message() -> None:
    worker = Worker(WorkerConfig())
    assert not worker._ready_event.is_set()

    msg = orchestrator_pb2.ServerMsg(ping=orchestrator_pb2.Ping(timestamp=0))
    fake_stream = _FakeStream([msg])

    await worker._read_responses(fake_stream)
    assert worker._ready_event.is_set()
