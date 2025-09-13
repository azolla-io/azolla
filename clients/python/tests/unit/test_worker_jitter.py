"""Tests for worker reconnect jitter bounds in start() loop."""

import asyncio
from typing import Any

import pytest

from azolla import Worker, WorkerConfig


@pytest.mark.asyncio
async def test_reconnect_delay_never_negative(monkeypatch: pytest.MonkeyPatch) -> None:
    # Force conditions that produce negative jitter in current implementation
    worker = Worker(WorkerConfig(reconnect_delay=20.0, max_reconnect_delay=60.0))

    # Make connection always fail quickly
    async def fail_run_connection(self: Worker) -> None:  # type: ignore[override]
        raise RuntimeError("boom")

    monkeypatch.setattr(Worker, "_run_connection", fail_run_connection)

    # Make loop time return 0 so jitter = 1.0 - 0.1*delay -> negative for delay >= 10
    class _FakeLoop:
        def time(self) -> float:
            return 0.0

    monkeypatch.setattr(asyncio, "get_event_loop", lambda: _FakeLoop())

    # Capture the timeout passed to wait_for
    captured: list[float] = []

    async def fake_wait_for(awaitable: Any, timeout: float) -> Any:  # type: ignore[override]
        captured.append(timeout)
        # First call: simulate timeout to trigger backoff calculation
        if len(captured) == 1:
            raise asyncio.TimeoutError
        # Second call: capture updated delay then stop the loop
        worker.shutdown()
        raise asyncio.TimeoutError

    monkeypatch.setattr(asyncio, "wait_for", fake_wait_for)

    # Run start; our fake wait_for triggers shutdown after capture
    await worker.start()

    assert len(captured) >= 2, "wait_for should have been invoked twice"
    # Expected behavior after backoff update: non-negative timeout.
    assert captured[1] >= 0.0
